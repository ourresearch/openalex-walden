"""Edge autocomplete index build (oxjob #1529; prototypes and measurements in oxjob #1504).

One adaptive prefix tree per entity type, written to openalex.autocomplete.nodes_<type> as (p, v, h) rows that the
loader (notebooks/autocomplete_edge/LoadAutocompleteEdge) copies into Cloudflare Workers KV for the Worker
openalex-autocomplete-edge (github.com/ourresearch/openalex-autocomplete-edge, which also holds the ranking code that
reads these nodes: src/rank.js, src/core.js, src/entcore.js).

Pure Python, no Spark import: every function returns SQL, so the same code runs in the walden notebooks (spark.sql)
and from a desk session against a SQL warehouse.

Tree design (#1504 findings/edge-index.md § 1): a key with <= N matching entries is a LEAF holding all of them (the
browser filters later keystrokes itself); a busier key is HEAVY and keeps the top K entities by popularity, the top KS
whose label starts with the prefix, the top 10 near-exact labels (at most 3 characters longer, prefixes of 3+
characters) and every label that IS the prefix (#1529: "ML", "ACA", which a top-K cut dropped). A key is stored when it
is 1 character long or its parent is heavy.

  KV key       <t>:<build>:<p>        t = type letter (TYPES), build = the loader's build id, p = prefix
  value        {"l":0|1,"n":count,"e":[entries]}
  entries      small types  [id, label, display|0, works, pop*100, hint|0, cited, ext|0]   (one per matching label)
               awards       [id, title|0, number, funder tag, outputs, start year, pop*100, placeholder 0|1]
               authors      [id, display, works, pop*100, hint|0, cited, orcid|0]
  prefixes     small types and awards: word-start suffixes of the normalised label + ' ' (a complete word), <= CAP chars
               authors: each display-name token + ' ' (<= 24 chars), and pairs anchor|q under tokens held by > N
               authors (anchor = the exact token, q = a prefix of another token of the same name, <= 16 chars)

pop = log10(1 + cited) + 0.5 * log10(1 + works); keywords 1.75 * log10(1 + works) (their ranking uses works only, as
today's /autocomplete/keywords does, #1529 step 1.4); awards log10(1 + outputs) + (year - 1900) / 1000.
"""

SCHEMA = "openalex.autocomplete"
N, K, KS, CAP, ACAP = 500, 50, 30, 32, 24

# type -> (letter, source table whose update triggers the type's refresh)
TYPES = {
    "keywords": ("k", "openalex.common.keywords_api"),
    "sources": ("s", "openalex.sources.sources_api"),
    "institutions": ("i", "openalex.institutions.institutions_api"),
    "funders": ("f", "openalex.funders.funders_api"),
    "publishers": ("p", "openalex.publishers.publishers_api"),
    "concepts": ("c", "openalex.common.concepts_api"),
    "topics": ("t", "openalex.common.topics_api"),
    "awards": ("g", "openalex.awards.awards_api"),
    "authors": ("a", "openalex.authors.openalex_authors"),
}
SMALL = ["keywords", "sources", "institutions", "funders", "publishers", "concepts", "topics"]

STOP = ["of", "the", "and", "in", "for", "a", "an", "to", "on", "with", "by", "at", "from", "de", "la", "et", "des", "der",
        "und", "du", "le", "les", "el", "y", "en", "von", "zu", "da", "do", "e", "o", "di", "del", "della", "its", "as", "or"]
STOPSQL = "array(" + ",".join(f"'{s}'" for s in STOP) + ")"

# collective or placeholder author names (#1504: "PLOS ONE Staff" ranked under "plos"), checked on the display name
# against the 2+-work authors on 2026-10-03: "Unknown" (280 profiles), "Anonymous" (1,216), "Editorial Office",
# "Various Authors", "Maine Campus Staff"... "Staff" counts only after an organisational word or an all-caps token,
# because it is also a surname (Anne Cathrine Staff, Ilene Staff).
def junk_author_sql(col):
    named = (r"(?i)(^(the )?editors?$|editorial (board|office|team|staff)|^anonymous|^unknown|^(n/a|na|none|null|undefined)$|"
             r"^various|erratum|corrigendum|retraction notice|^no authors?( listed)?$|^staff$|"
             r"\\b(news|campus|reporter|editorial|cent(er|re)|library|magazine|daily|tribune|times|network|journal|press|"
             r"university|college|school|medical|society|planet|computer)\\b.*\\bstaff$)")
    return f"({col} RLIKE '{named}' OR {col} RLIKE '\\\\b[A-Z]{{2,}}\\\\b.*\\\\b[Ss]taff$')"

# ---------------------------------------------------------------- normalisation (must match src/norm.js)
_MULTI = [("ß", "ss"), ("æ", "ae"), ("œ", "oe"), ("þ", "th"), ("ĳ", "ij")]
_GREEK = {"α": "alpha", "β": "beta", "γ": "gamma", "δ": "delta", "ε": "epsilon", "ζ": "zeta", "η": "eta",
          "θ": "theta", "ι": "iota", "κ": "kappa", "λ": "lambda", "μ": "mu", "ν": "nu", "ξ": "xi", "π": "pi",
          "ρ": "rho", "σ": "sigma", "ς": "sigma", "τ": "tau", "υ": "upsilon", "φ": "phi", "χ": "chi", "ψ": "psi",
          "ω": "omega"}
_SPECIAL1 = {"ø": "o", "ł": "l", "đ": "d", "ð": "d", "ı": "i", "ħ": "h", "ŧ": "t", "ŀ": "l", "ƒ": "f"}


def _fold_map():
    import unicodedata
    src, dst = [], []
    for cp in list(range(0xC0, 0x250)) + list(range(0x1E00, 0x1F00)):
        lc = chr(cp).lower()
        if len(lc) != 1 or lc in src:
            continue
        if lc in _SPECIAL1:
            b = _SPECIAL1[lc]
        else:
            d = unicodedata.normalize("NFKD", lc)
            b = "".join(x for x in d if not unicodedata.combining(x))
        if len(b) == 1 and b.isascii() and b.isalnum() and b != lc:
            src.append(lc)
            dst.append(b)
    return "".join(src), "".join(dst)


MAP_FROM, MAP_TO = _fold_map()


def norm_sql(col, greek=False):
    """SQL for the shared normalisation: lowercase; drop apostrophes; fold ß æ œ þ ĳ, accents and Latin diacritics;
    optionally spell Greek letters out; every run of non letters/digits -> one space; trim."""
    e = f"lower({col})"
    e = f"regexp_replace({e}, '[’\\'`´ʼ]', '')"
    for a, b in _MULTI:
        e = f"replace({e}, '{a}', '{b}')"
    if greek:
        for a, b in _GREEK.items():
            e = f"replace({e}, '{a}', ' {b} ')"
    e = f"regexp_replace({e}, '\\\\p{{M}}', '')"
    e = f"translate({e}, '{MAP_FROM}', '{MAP_TO}')"
    return f"trim(regexp_replace({e}, '[^\\\\p{{L}}\\\\p{{N}}]+', ' '))"


# ---------------------------------------------------------------- SQL helpers
def js(x):
    """JSON string literal of the SQL expression x."""
    return f"substr(to_json(array({x})), 2, length(to_json(array({x}))) - 2)"


def trunc(x, n):
    """x cut to at most n chars at a word boundary, with an ellipsis."""
    return (f"IF({x} IS NULL OR trim({x}) = '', NULL, IF(length({x}) <= {n}, {x}, "
            f"concat(regexp_replace(substr({x}, 1, {n} - 1), '\\\\s+\\\\S*$', ''), '…')))")


def arr(*xs):
    return "array_distinct(filter(concat(" + ", ".join(f"coalesce({x}, array())" for x in xs) + "), z -> z IS NOT NULL AND trim(z) <> ''))"


POP = "(log10(1 + coalesce({c}, 0)) + 0.5 * log10(1 + coalesce({w}, 0)))"
CNAME = ("CASE upper({g}.country_code) WHEN 'US' THEN 'USA' WHEN 'GB' THEN 'UK' "
         "ELSE coalesce(cn.display_name, {g}.country_code) END")


def stg(typ, step):
    return f"{SCHEMA}.stg_{typ}_{step}"


def nodes_table(typ):
    return f"{SCHEMA}.nodes_{typ}"


# ---------------------------------------------------------------- small types
def _entities_sql(typ):
    """one row per entity: eid, display, works, cited, hint, ext, labels"""
    if typ == "keywords":
        return f"""SELECT substr(id, 31) AS eid, display_name AS display, works_count AS works, cited_by_count AS cited,
       {trunc('description', 60)} AS hint, CAST(NULL AS STRING) AS ext,
       {arr('array(display_name)', 'display_name_alternatives')} AS labels
FROM openalex.common.keywords_api WHERE display_name IS NOT NULL"""
    if typ == "sources":
        return f"""SELECT CAST(s.id AS STRING) AS eid, s.display_name AS display, s.works_count AS works, s.cited_by_count AS cited,
       coalesce(nullif(s.host_organization_name, ''), 'host organization unknown') AS hint, s.issn_l AS ext,
       {arr('array(s.display_name)', 's.alternate_titles', 'array(j.abbreviated_title)', 's.issn', "transform(s.issn, z -> replace(z, '-', ''))")} AS labels
FROM openalex.sources.sources_api s LEFT JOIN (SELECT journal_id, max(abbreviated_title) AS abbreviated_title
  FROM openalex.mid.journal WHERE abbreviated_title IS NOT NULL GROUP BY journal_id) j ON j.journal_id = s.id
WHERE s.display_name IS NOT NULL"""
    if typ == "institutions":
        return f"""SELECT CAST(i.id AS STRING) AS eid, i.display_name AS display, i.works_count AS works, i.cited_by_count AS cited,
       nullif(concat_ws(', ', i.geo.city, {CNAME.format(g='i.geo')}), '') AS hint, regexp_replace(i.ror, '^https://ror.org/', '') AS ext,
       {arr('array(i.display_name)', 'i.display_name_acronyms', 'i.display_name_alternatives')} AS labels
FROM openalex.institutions.institutions_api i LEFT JOIN openalex.common.countries_api cn ON cn.country_code = upper(i.geo.country_code)
WHERE i.display_name IS NOT NULL AND coalesce(i.status, 'active') = 'active'"""
    if typ == "funders":
        return f"""SELECT CAST(id AS STRING) AS eid, display_name AS display, works_count AS works, cited_by_count AS cited,
       {trunc('description', 60)} AS hint, regexp_replace(ids.ror, '^https://ror.org/', '') AS ext,
       {arr('array(display_name)', 'alternate_titles')} AS labels
FROM openalex.funders.funders_api WHERE display_name IS NOT NULL"""
    if typ == "concepts":
        return f"""SELECT CAST(id AS STRING) AS eid, display_name AS display, works_count AS works, cited_by_count AS cited,
       {trunc('description', 60)} AS hint, regexp_extract(coalesce(wikidata, ''), '(Q[0-9]+)$', 1) AS ext, array(display_name) AS labels
FROM openalex.common.concepts_api WHERE display_name IS NOT NULL"""
    if typ == "publishers":
        return f"""SELECT CAST(id AS STRING) AS eid, display_name AS display, works_count AS works, cited_by_count AS cited,
       CAST(NULL AS STRING) AS hint, regexp_extract(coalesce(ids.wikidata, ''), '(Q[0-9]+)$', 1) AS ext,
       {arr('array(display_name)', 'alternate_titles')} AS labels
FROM openalex.publishers.publishers_api WHERE display_name IS NOT NULL"""
    if typ == "topics":
        return f"""SELECT CAST(id AS STRING) AS eid, display_name AS display, works_count AS works, cited_by_count AS cited,
       {trunc('description', 160)} AS hint, ids.wikipedia AS ext, {arr('array(display_name)', 'keywords')} AS labels
FROM openalex.common.topics_api WHERE display_name IS NOT NULL"""
    raise ValueError(typ)


def small_steps(typ):
    """[(name, sql)] building nodes_<typ> for a small type"""
    greek = typ in ("keywords", "topics")
    pop = "1.75 * log10(1 + coalesce(works, 0))" if typ == "keywords" else POP.format(c="cited", w="works")
    return [
        ("ent", f"CREATE OR REPLACE TABLE {stg(typ, 'ent')} AS\n{_entities_sql(typ)}"),
        # one row per (entity, label): normalised label and the entry JSON
        ("lab", f"""CREATE OR REPLACE TABLE {stg(typ, 'lab')} AS
WITH x AS (SELECT eid, display, works, cited, hint, ext, {pop} AS pop, lid, label
           FROM {stg(typ, 'ent')} LATERAL VIEW posexplode(labels) z AS lid, label)
SELECT eid, lid, pop, {norm_sql('label', greek=greek)} AS nlabel,
       concat('[', {js('eid')}, ',', {js('label')}, ',', IF(label = display, '0', {js('display')}), ',', coalesce(works, 0), ',',
              CAST(round(100 * pop) AS INT), ',', IF(hint IS NULL, '0', {js('hint')}), ',', coalesce(cited, 0), ',',
              IF(ext IS NULL OR ext = '', '0', {js('ext')}), ']') AS e
FROM x"""),
        # (entity, label, prefix of a word-start suffix); st = the prefix starts at the label's first word
        ("sfx", f"""CREATE OR REPLACE TABLE {stg(typ, 'sfx')} AS
WITH s AS (SELECT eid, lid, split(nlabel, ' ') AS w FROM {stg(typ, 'lab')} WHERE nlabel <> ''),
sf AS (SELECT eid, lid, i = 1 AS st, array_join(slice(w, i, size(w)), ' ') || ' ' AS sx FROM s LATERAL VIEW explode(sequence(1, size(w))) z AS i)
SELECT eid, lid, substr(sx, 1, L) AS p, max(st) AS st FROM sf LATERAL VIEW explode(sequence(1, least(length(sx), {CAP}))) z AS L
GROUP BY eid, lid, substr(sx, 1, L)"""),
        ("cnt", f"CREATE OR REPLACE TABLE {stg(typ, 'cnt')} AS SELECT p, count(*) AS cnt FROM {stg(typ, 'sfx')} GROUP BY p"),
        ("keys", f"""CREATE OR REPLACE TABLE {stg(typ, 'keys')} AS
SELECT c.p, c.cnt, c.cnt <= {N} AS leaf
FROM {stg(typ, 'cnt')} c LEFT JOIN {stg(typ, 'cnt')} par ON par.p = substr(c.p, 1, length(c.p) - 1)
WHERE length(c.p) = 1 OR par.cnt > {N}"""),
        ("mem", f"""CREATE OR REPLACE TABLE {stg(typ, 'mem')} AS
WITH m AS (SELECT x.p, x.eid, x.lid, x.st, length(l.nlabel) <= length(rtrim(x.p)) + 3 AS near, l.nlabel = rtrim(x.p) AS exact,
                  k.leaf, k.cnt, l.pop, l.e
           FROM {stg(typ, 'sfx')} x JOIN {stg(typ, 'keys')} k ON k.p = x.p JOIN {stg(typ, 'lab')} l ON l.eid = x.eid AND l.lid = x.lid),
r AS (SELECT m.*, dense_rank() OVER (PARTITION BY m.p ORDER BY m.pop DESC, m.eid) AS er,
             dense_rank() OVER (PARTITION BY m.p, m.st ORDER BY m.pop DESC, m.eid) AS es,
             dense_rank() OVER (PARTITION BY m.p, m.st AND m.near ORDER BY m.pop DESC, m.eid) AS en,
             row_number() OVER (PARTITION BY m.p ORDER BY m.pop DESC, m.eid, m.lid) AS rn FROM m)
SELECT p, leaf, cnt, rn, e FROM r
WHERE leaf OR er <= {K} OR (st AND es <= {KS}) OR (st AND near AND length(p) >= 3 AND en <= 10) OR (st AND exact)"""),
        ("nodes", _nodes_sql(typ, f"SELECT p, leaf, cnt, rn, e FROM {stg(typ, 'mem')}")),
    ]


def _nodes_sql(typ, mem_select):
    return f"""CREATE OR REPLACE TABLE {nodes_table(typ)} AS
WITH g AS (SELECT p, concat('{{"l":', IF(first(leaf), '1', '0'), ',"n":', first(cnt), ',"e":[',
                         array_join(transform(array_sort(collect_list(struct(rn, e))), z -> z.e), ','), ']}}') AS v,
                  count(*) AS ne
           FROM ({mem_select}) GROUP BY p)
SELECT p, v, xxhash64(v) AS h, octet_length(v) AS bytes, ne, current_timestamp() AS built_at FROM g"""


# ---------------------------------------------------------------- awards
_NUMN = norm_sql("num")
_GROUPS = (f"split(regexp_replace(regexp_replace({_NUMN}, '(\\\\p{{L}})(\\\\p{{N}})', '$1 $2'), '(\\\\p{{N}})(\\\\p{{L}})', '$1 $2'), ' ')")
# display: junk punctuation off the ends and '?' (mojibake for a space or dash) out: "SFB1032," -> "SFB1032", "SFB?1083" -> "SFB1083"
_NUMDISP = "regexp_replace(regexp_replace(trim(num), '^[^\\\\p{L}\\\\p{N}(\\\\[]+|[^\\\\p{L}\\\\p{N})\\\\]]+$', ''), '\\\\?', '')"


def award_steps():
    typ = "awards"
    return [
        # one acronym per funder: the shortest all-caps single-token alias (2 to 10 chars); tag = acronym, else the name
        ("fun", f"""CREATE OR REPLACE TABLE {stg(typ, 'fun')} AS
WITH a AS (SELECT id, alias FROM openalex.funders.funders_api LATERAL VIEW explode(
             concat(array(display_name), coalesce(alternate_titles, array()))) z AS alias
           WHERE alias RLIKE '^[A-Z][A-Z0-9&]{{1,9}}$'),
r AS (SELECT id, alias, row_number() OVER (PARTITION BY id ORDER BY length(alias), alias) AS rn FROM a)
SELECT f.id AS fid, r.alias AS acr, lower(r.alias) AS nacr,
       coalesce(r.alias, IF(length(f.display_name) <= 32, f.display_name, concat(substr(f.display_name, 1, 31), '…'))) AS tag
FROM openalex.funders.funders_api f LEFT JOIN r ON r.id = f.id AND r.rn = 1"""),
        # placeholder numbers rank below real grants: no run of 4+ digits ("R01-grant", "R01 NS", "DE-AC02-"), or a
        # programme-level code cited by a huge number of outputs ("HORIZON2020", 191K)
        ("ent", f"""CREATE OR REPLACE TABLE {stg(typ, 'ent')} AS
WITH a AS (SELECT a.id, nullif(trim(a.display_name), '') AS title, nullif(trim(a.funder_award_id), '') AS num, a.funder_id,
                  coalesce(a.funded_outputs_count, 0) AS outs, a.start_year AS yr FROM openalex.awards.awards_api a),
g AS (SELECT a.*, IF(num IS NULL, array(), filter({_GROUPS}, z -> z <> '')) AS grp,
             IF(num IS NULL, NULL, nullif({_NUMDISP}, '')) AS numd FROM a)
SELECT g.id AS eid, g.title, coalesce(g.numd, g.num) AS num, g.outs, g.yr, f.nacr, coalesce(f.tag, '') AS tag,
       log10(1 + g.outs) + (coalesce(g.yr, 1900) - 1900) / 1000 AS pop,
       g.num IS NOT NULL AND g.title IS NULL AND (NOT g.num RLIKE '[0-9]{{4,}}' OR g.outs >= 20000) AS placeholder,
       slice(filter(transform(sequence(1, greatest(size(g.grp), 1)), i -> IF(size(g.grp) = 0, NULL, array_join(slice(g.grp, i, size(g.grp)), ''))),
                    z -> z IS NOT NULL AND (length(z) >= 4 OR z = array_join(g.grp, ''))), 1, 5) AS nv,
       IF(g.title IS NULL, NULL, {norm_sql('g.title')}) AS ntitle
FROM g LEFT JOIN {stg(typ, 'fun')} f ON f.fid = g.funder_id
WHERE g.title IS NOT NULL OR g.num IS NOT NULL"""),
        # labels: (eid, nlabel, starts) -- starts = how many word starts to index (titles 12; the rest only from the start)
        ("lab", f"""CREATE OR REPLACE TABLE {stg(typ, 'lab')} AS
SELECT eid, substr(ntitle, 1, 200) AS nlabel, 12 AS starts FROM {stg(typ, 'ent')} WHERE ntitle IS NOT NULL AND ntitle <> ''
UNION ALL SELECT eid, substr(v, 1, 40), 1 FROM {stg(typ, 'ent')} LATERAL VIEW explode(nv) z AS v
UNION ALL SELECT eid, concat(nacr, ' ', substr(nv[0], 1, 40)), 1 FROM {stg(typ, 'ent')} WHERE nacr IS NOT NULL AND size(nv) > 0
UNION ALL SELECT eid, concat(nacr, ' ', substr(nv[1], 1, 40)), 1 FROM {stg(typ, 'ent')} WHERE nacr IS NOT NULL AND size(nv) > 1
UNION ALL SELECT eid, concat(nacr, ' ', substr(ntitle, 1, 200)), 1 FROM {stg(typ, 'ent')} WHERE nacr IS NOT NULL AND ntitle IS NOT NULL AND ntitle <> ''"""),
        ("sfx", f"""CREATE OR REPLACE TABLE {stg(typ, 'sfx')} AS
WITH s AS (SELECT eid, starts, split(nlabel, ' ') AS w FROM {stg(typ, 'lab')} WHERE nlabel <> ''),
sf AS (SELECT eid, array_join(slice(w, i, size(w)), ' ') || ' ' AS sx FROM s
       LATERAL VIEW explode(sequence(1, least(size(w), starts))) z AS i WHERE i = 1 OR NOT array_contains({STOPSQL}, w[i - 1]))
SELECT DISTINCT eid, substr(sx, 1, L) AS p FROM sf LATERAL VIEW explode(sequence(1, least(length(sx), {ACAP}))) z AS L"""),
        ("cnt", f"CREATE OR REPLACE TABLE {stg(typ, 'cnt')} AS SELECT p, count(*) AS cnt FROM {stg(typ, 'sfx')} GROUP BY p"),
        ("keys", f"""CREATE OR REPLACE TABLE {stg(typ, 'keys')} AS
SELECT c.p, c.cnt, c.cnt <= {N} AS leaf
FROM {stg(typ, 'cnt')} c LEFT JOIN {stg(typ, 'cnt')} par ON par.p = substr(c.p, 1, length(c.p) - 1)
WHERE length(c.p) = 1 OR par.cnt > {N}"""),
        ("mem", f"""CREATE OR REPLACE TABLE {stg(typ, 'mem')} AS
WITH m AS (SELECT x.p, x.eid, k.leaf, k.cnt FROM {stg(typ, 'sfx')} x JOIN {stg(typ, 'keys')} k ON k.p = x.p),
e AS (SELECT eid, pop - IF(placeholder, 3, 0) AS rpop,
             concat('[', {js('CAST(eid AS STRING)')}, ',', IF(title IS NULL, '0', {js('substr(title, 1, 160)')}), ',',
                    IF(num IS NULL, '""', {js('substr(num, 1, 60)')}), ',', {js('tag')}, ',', outs, ',', coalesce(yr, 0), ',',
                    CAST(round(100 * pop) AS INT), ',', IF(placeholder, '1', '0'), ']') AS e FROM {stg(typ, 'ent')}),
r AS (SELECT m.p, m.leaf, m.cnt, e.e, row_number() OVER (PARTITION BY m.p ORDER BY e.rpop DESC, m.eid) AS rn
      FROM m JOIN e ON e.eid = m.eid)
SELECT p, leaf, cnt, rn, e FROM r WHERE leaf OR rn <= {K}"""),
        ("nodes", _nodes_sql(typ, f"SELECT p, leaf, cnt, rn, e FROM {stg(typ, 'mem')}")),
    ]


# ---------------------------------------------------------------- authors
def author_steps():
    typ = "authors"
    return [
        # one row per author with 2+ works (one-work authors, 52M more, stay with today's endpoint: the GUI falls back
        # to it when the edge has no rows); display-name tokens only (raw names are noisy, +40% pair level); no junk
        # collective names; no 7+-token or 60+-char names
        ("ent", f"""CREATE OR REPLACE TABLE {stg(typ, 'ent')} AS
WITH a AS (SELECT id AS eid, display_name AS display, {norm_sql('display_name')} AS nname, works_count AS works,
                  cited_by_count AS cited, regexp_replace(coalesce(orcid, ''), '^https?://orcid.org/', '') AS orcid,
                  get(last_known_institutions, 0) AS inst
           FROM openalex.authors.openalex_authors
           WHERE works_count >= 2 AND display_name IS NOT NULL AND length(display_name) <= 60
             AND NOT {junk_author_sql('display_name')})
SELECT a.eid, a.display, a.nname, a.works, a.cited, nullif(a.orcid, '') AS orcid,
       nullif(concat_ws(', ', a.inst.display_name, {CNAME.format(g='a.inst')}), '') AS hint,
       {POP.format(c='a.cited', w='a.works')} AS pop
FROM a LEFT JOIN openalex.common.countries_api cn ON cn.country_code = upper(a.inst.country_code)
WHERE a.nname <> '' AND size(split(a.nname, ' ')) <= 6"""),
        ("tok", f"""CREATE OR REPLACE TABLE {stg(typ, 'tok')} AS
SELECT eid, tok FROM {stg(typ, 'ent')} LATERAL VIEW explode(array_distinct(split(nname, ' '))) z AS tok WHERE tok <> ''"""),
        ("e", f"""CREATE OR REPLACE TABLE {stg(typ, 'e')} AS
SELECT eid, pop, concat('[', eid, ',', {js('display')}, ',', works, ',', CAST(round(100 * pop) AS INT), ',',
                        IF(hint IS NULL, '0', {js('hint')}), ',', coalesce(cited, 0), ',', IF(orcid IS NULL, '0', {js('orcid')}), ']') AS e
FROM {stg(typ, 'ent')}"""),
        # level 1: prefixes of (token || ' ') for tokens of 2+ chars
        ("p1", f"""CREATE OR REPLACE TABLE {stg(typ, 'p1')} AS
SELECT DISTINCT eid, substr(tok || ' ', 1, L) AS p FROM {stg(typ, 'tok')}
LATERAL VIEW explode(sequence(1, least(length(tok) + 1, 24))) z AS L WHERE length(tok) >= 2"""),
        ("c1", f"CREATE OR REPLACE TABLE {stg(typ, 'c1')} AS SELECT p, count(*) AS cnt FROM {stg(typ, 'p1')} GROUP BY p"),
        ("k1", f"""CREATE OR REPLACE TABLE {stg(typ, 'k1')} AS
SELECT c.p, c.cnt, c.cnt <= {N} AS leaf FROM {stg(typ, 'c1')} c LEFT JOIN {stg(typ, 'c1')} par ON par.p = substr(c.p, 1, length(c.p) - 1)
WHERE length(c.p) = 1 OR par.cnt > {N}"""),
        # level 2: anchor = an exact token held by > N authors; q = prefix of (another token || ' ')
        ("p2", f"""CREATE OR REPLACE TABLE {stg(typ, 'p2')} AS
WITH anc AS (SELECT substr(p, 1, length(p) - 1) AS anchor FROM {stg(typ, 'c1')} WHERE endswith(p, ' ') AND cnt > {N}),
ea AS (SELECT t.eid, t.tok AS anchor FROM {stg(typ, 'tok')} t JOIN anc ON anc.anchor = t.tok),
oth AS (SELECT ea.eid, ea.anchor, o.tok AS other FROM ea JOIN {stg(typ, 'tok')} o ON o.eid = ea.eid AND o.tok <> ea.anchor)
SELECT DISTINCT eid, anchor || '|' || substr(other || ' ', 1, L) AS p FROM oth
LATERAL VIEW explode(sequence(1, least(length(other) + 1, 16))) z AS L"""),
        ("c2", f"CREATE OR REPLACE TABLE {stg(typ, 'c2')} AS SELECT p, count(*) AS cnt FROM {stg(typ, 'p2')} GROUP BY p"),
        ("k2", f"""CREATE OR REPLACE TABLE {stg(typ, 'k2')} AS
SELECT c.p, c.cnt, c.cnt <= {N} AS leaf FROM {stg(typ, 'c2')} c LEFT JOIN {stg(typ, 'c2')} par ON par.p = substr(c.p, 1, length(c.p) - 1)
WHERE substr(c.p, instr(c.p, '|') + 1) RLIKE '^.$' OR par.cnt > {N}"""),
        ("mem", f"""CREATE OR REPLACE TABLE {stg(typ, 'mem')} AS
WITH m AS (SELECT x.p, x.eid, k.leaf, k.cnt FROM {stg(typ, 'p1')} x JOIN {stg(typ, 'k1')} k ON k.p = x.p
           UNION ALL
           SELECT x.p, x.eid, k.leaf, k.cnt FROM {stg(typ, 'p2')} x JOIN {stg(typ, 'k2')} k ON k.p = x.p),
r AS (SELECT m.p, m.leaf, m.cnt, e.e, row_number() OVER (PARTITION BY m.p ORDER BY e.pop DESC, m.eid) AS rn
      FROM m JOIN {stg(typ, 'e')} e ON e.eid = m.eid)
SELECT p, leaf, cnt, rn, e FROM r WHERE leaf OR rn <= {K}"""),
        ("nodes", _nodes_sql(typ, f"SELECT p, leaf, cnt, rn, e FROM {stg(typ, 'mem')}")),
    ]


def steps(typ):
    if typ in SMALL:
        return small_steps(typ)
    if typ == "awards":
        return award_steps()
    if typ == "authors":
        return author_steps()
    raise ValueError(typ)


def setup_sql():
    """the schema and the loader's bookkeeping tables (idempotent)"""
    return [
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA} COMMENT 'Edge autocomplete index (oxjob #1529): per-type trees for Workers KV'",
        # what Workers KV holds per type: one row per key of the live build
        f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.loaded (typ STRING, build STRING, p STRING, h BIGINT)
            CLUSTER BY (typ, p) COMMENT 'Keys Workers KV holds for each type''s live build, with each value''s xxhash64'""",
        # the live build per type, and builds waiting for their old keys to be deleted
        f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.builds (typ STRING, build STRING, state STRING, keys BIGINT,
            src_version BIGINT, created_at TIMESTAMP, flipped_at TIMESTAMP, deleted_at TIMESTAMP)
            COMMENT 'Builds per type: writing, live, retired (old keys deleted)'""",
        f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.refresh_runs (typ STRING, build STRING, mode STRING, src_table STRING,
            src_version BIGINT, src_committed_at TIMESTAMP, started_at TIMESTAMP, finished_at TIMESTAMP, minutes DOUBLE,
            keys_total BIGINT, keys_written BIGINT, keys_deleted BIGINT, bytes_written BIGINT, est_usd DOUBLE,
            do_rows BIGINT, status STRING, message STRING, job_run_id STRING)
            COMMENT 'One row per type per refresh (oxjob #1529): what was written to the edge, cost and duration'""",
    ]


# ---------------------------------------------------------------- loader
# Workers KV pricing (2026-10): writes, deletes and lists $5 per million; Durable Object rows written $1 per million.
KV_ACCOUNT = "a452eddbbe06eb7d02f4879cee70d29c"
KV_NAMESPACE = "ff3e53d8c9a64ca5a4cf0369bdcf5059"      # openalex-autocomplete
WORKER = "https://openalex-autocomplete-edge.our-research.workers.dev"
USD_KV_OP, USD_DO_ROW = 5e-6, 1e-6
PROPAGATION_WAIT_S = 180      # KV write propagation is up to ~60 s; #1504 read keys a minute after the last write and poisoned them
REBUILD_FRACTION = 0.5        # a delta touching more than half the keys is written as a new build instead (plan 2.3)


class KV:
    """Cloudflare Workers KV REST client for the loader: bulk put/delete with retries, list by prefix, get."""

    def __init__(self, token, account=KV_ACCOUNT, namespace=KV_NAMESPACE):
        import requests
        self.s = requests.Session()
        self.s.headers.update({"Authorization": f"Bearer {token}"})
        self.base = f"https://api.cloudflare.com/client/v4/accounts/{account}/storage/kv/namespaces/{namespace}"

    def _call(self, method, path, **kw):
        import time
        for attempt in range(8):
            try:
                r = self.s.request(method, self.base + path, timeout=120, **kw)
                if r.status_code < 300:
                    return r
                if r.status_code not in (429, 500, 502, 503, 504):
                    raise RuntimeError(f"KV {method} {path} {r.status_code}: {r.text[:300]}")
            except OSError:  # requests' connection errors subclass OSError
                if attempt == 7:
                    raise
            time.sleep(min(60, 2 ** attempt))
        raise RuntimeError(f"KV {method} {path}: retries exhausted")

    def put_bulk(self, pairs):
        self._call("PUT", "/bulk", json=[{"key": k, "value": v} for k, v in pairs])

    def delete_bulk(self, keys):
        self._call("POST", "/bulk/delete", json=list(keys))

    def put(self, key, value):
        from urllib.parse import quote
        self._call("PUT", "/values/" + quote(key, safe=""), data=value.encode())

    def get(self, key):
        from urllib.parse import quote
        r = self.s.get(self.base + "/values/" + quote(key, safe=""), timeout=60)
        return r.text if r.status_code == 200 else None

    def count_prefix(self, prefix):
        n, cursor = 0, None
        while True:
            params = {"prefix": prefix, "limit": 1000, **({"cursor": cursor} if cursor else {})}
            d = self._call("GET", "/keys", params=params).json()
            n += len(d["result"])
            cursor = (d.get("result_info") or {}).get("cursor")
            if not cursor:
                return n


class Copies:
    """The Worker's Durable Object copies (src/copy.js): every KV write and delete goes to each live region too."""

    def __init__(self, admin_key, regions, worker=WORKER):
        import requests
        self.s = requests.Session()
        self.s.headers.update({"x-admin-key": admin_key or "", "content-type": "application/json"})
        self.regions, self.worker = [r for r in regions if r], worker

    def load(self, rows):
        import json, time
        for region in self.regions:
            for i in range(0, len(rows), 1000):   # a Durable Object batch stays small (CPU and request size)
                part = rows[i:i + 1000]
                for attempt in range(8):
                    r = self.s.post(f"{self.worker}/admin/copy-load?region={region}", data=json.dumps(part), timeout=300)
                    if r.status_code == 200:
                        break
                    if attempt == 7:
                        raise RuntimeError(f"copy {region} {r.status_code}: {r.text[:300]}")
                    time.sleep(min(60, 2 ** attempt))
        return len(rows) * len(self.regions)


def batches(rows, max_n=10000, max_bytes=40_000_000):
    """group (key, value) pairs into KV bulk requests: <= 10,000 pairs and well under the 100 MB body limit"""
    cur, size = [], 0
    for k, v in rows:
        b = len(k) + len(v or "") + 32
        if cur and (len(cur) >= max_n or size + b > max_bytes):
            yield cur
            cur, size = [], 0
        cur.append((k, v))
        size += b
    if cur:
        yield cur


def run_parallel(fn, items, threads=16, log=print, label="", every=50):
    """fn(item) -> keys done, over items with a bounded number in flight (never queue everything); progress lines"""
    import time
    from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
    t0, done, n_keys, pending = time.time(), 0, 0, set()
    with ThreadPoolExecutor(threads) as ex:
        for it in items:
            pending.add(ex.submit(fn, it))
            if len(pending) >= threads * 2:
                fin, pending = wait(pending, return_when=FIRST_COMPLETED)
                for f in fin:
                    n_keys += f.result()
                    done += 1
                    if done % every == 0:
                        log(f"{label}: {done} batches, {n_keys:,} keys, {n_keys / max(1e-9, time.time() - t0):,.0f} keys/s")
        for f in pending:
            n_keys += f.result()
    return n_keys


def refresh(typ, sql, rows, kv, copies, log=print, force_rebuild=False, job_run_id=""):
    """Copy nodes_<typ> into Workers KV (and the copies) by the release rule of plan 2.1:
      - no live build, a forced rebuild, or a delta over REBUILD_FRACTION of the keys: write a NEW build under its own
        prefix, wait PROPAGATION_WAIT_S, check the key count by listing and 100 random reads, then flip ver:<t>; the
        old build's keys are deleted on the type's next run (readers on the old build keep working meanwhile);
      - otherwise write only changed keys IN PLACE, longest first (children before parents), then delete removed keys.
    sql(q) -> list of row tuples; rows(q) -> an iterator of row tuples (streamed). Returns the refresh_runs row."""
    import datetime, time
    t, src = TYPES[typ]
    started = datetime.datetime.now(datetime.timezone.utc)
    nt = nodes_table(typ)
    sv = sql(f"DESCRIBE HISTORY {src} LIMIT 1")[0]
    src_version, src_at = sv[0], sv[1]
    keys_total = sql(f"SELECT count(*) FROM {nt}")[0][0]
    live = sql(f"SELECT build FROM {SCHEMA}.builds WHERE typ = '{typ}' AND state = 'live' ORDER BY created_at DESC LIMIT 1")
    live = live[0][0] if live else None
    put = lambda bt: (kv.put_bulk(bt), copies.load([list(x) for x in bt]), len(bt))[2]
    drop = lambda bt: (kv.delete_bulk([k for k, _ in bt]), copies.load([[k, None] for k, _ in bt]), len(bt))[2]
    # builds retired by an earlier flip: delete their keys now (readers moved over at least a run ago)
    deleted = 0
    for (old,) in sql(f"SELECT build FROM {SCHEMA}.builds WHERE typ = '{typ}' AND state = 'retiring'"):
        ks = rows(f"SELECT concat('{t}:{old}:', p) FROM {SCHEMA}.loaded WHERE typ = '{typ}' AND build = '{old}'")
        deleted += run_parallel(drop, batches((k, "") for (k,) in ks), log=log, label=f"{typ} delete build {old}")
        sql(f"DELETE FROM {SCHEMA}.loaded WHERE typ = '{typ}' AND build = '{old}'")
        sql(f"UPDATE {SCHEMA}.builds SET state = 'retired', deleted_at = current_timestamp() WHERE typ = '{typ}' AND build = '{old}'")
    mode = "rebuild"
    if live and not force_rebuild:
        changed = sql(f"""SELECT count(*) FROM {nt} n LEFT JOIN {SCHEMA}.loaded l ON l.typ = '{typ}' AND l.build = '{live}' AND l.p = n.p
                          WHERE l.h IS NULL OR l.h <> n.h""")[0][0]
        if changed <= REBUILD_FRACTION * max(keys_total, 1):
            mode = "delta"
    log(f"{typ}: {keys_total:,} keys, live build {live}, mode {mode}")
    written = removed = 0
    if mode == "delta":
        b = live
        q = (f"""SELECT concat('{t}:{b}:', n.p), n.v FROM {nt} n LEFT JOIN {SCHEMA}.loaded l
                 ON l.typ = '{typ}' AND l.build = '{b}' AND l.p = n.p WHERE l.h IS NULL OR l.h <> n.h ORDER BY length(n.p) DESC""")
        written = run_parallel(put, batches(rows(q)), log=log, label=f"{typ} write")
        gone = rows(f"""SELECT concat('{t}:{b}:', l.p) FROM {SCHEMA}.loaded l LEFT ANTI JOIN {nt} n ON n.p = l.p
                        WHERE l.typ = '{typ}' AND l.build = '{b}'""")
        removed = run_parallel(drop, batches((k, "") for (k,) in gone), log=log, label=f"{typ} delete")
        sql(f"DELETE FROM {SCHEMA}.loaded WHERE typ = '{typ}' AND build = '{b}'")
        sql(f"INSERT INTO {SCHEMA}.loaded SELECT '{typ}', '{b}', p, h FROM {nt}")
    else:
        prev = sql(f"SELECT max(CAST(build AS INT)) FROM {SCHEMA}.builds WHERE typ = '{typ}'")[0][0]
        b = str((prev or 0) + 1)
        sql(f"INSERT INTO {SCHEMA}.builds VALUES ('{typ}', '{b}', 'writing', {keys_total}, {src_version}, current_timestamp(), NULL, NULL)")
        written = run_parallel(put, batches(rows(f"SELECT concat('{t}:{b}:', p), v FROM {nt} ORDER BY length(p) DESC")),
                               log=log, label=f"{typ} write")
        log(f"{typ}: wrote {written:,} keys of build {b}; waiting {PROPAGATION_WAIT_S} s for propagation")
        time.sleep(PROPAGATION_WAIT_S)
        listed = kv.count_prefix(f"{t}:{b}:")
        if listed != keys_total:
            raise RuntimeError(f"{typ} build {b}: listed {listed:,} keys, expected {keys_total:,}; not flipping")
        sample = sql(f"SELECT p FROM {nt} ORDER BY rand() LIMIT 100")
        bad = [p for (p,) in sample if kv.get(f"{t}:{b}:{p}") is None]
        if bad:
            raise RuntimeError(f"{typ} build {b}: {len(bad)} of 100 sampled keys unreadable ({bad[:3]}); not flipping")
        sql(f"INSERT INTO {SCHEMA}.loaded SELECT '{typ}', '{b}', p, h FROM {nt}")
        kv.put(f"ver:{t}", b)
        copies.load([[f"ver:{t}", b]])
        sql(f"UPDATE {SCHEMA}.builds SET state = 'live', flipped_at = current_timestamp() WHERE typ = '{typ}' AND build = '{b}'")
        if live:
            sql(f"UPDATE {SCHEMA}.builds SET state = 'retiring' WHERE typ = '{typ}' AND build = '{live}'")
        log(f"{typ}: build {b} live (listed {listed:,}, 100 of 100 sampled keys readable)")
    finished = datetime.datetime.now(datetime.timezone.utc)
    ops = written + removed + deleted
    return dict(typ=typ, build=b, mode=mode, src_table=src, src_version=src_version, src_committed_at=src_at,
                started_at=started, finished_at=finished, minutes=round((finished - started).total_seconds() / 60, 2),
                keys_total=keys_total, keys_written=written, keys_deleted=removed + deleted,
                est_usd=round(ops * USD_KV_OP + ops * len(copies.regions) * USD_DO_ROW, 2),
                do_rows=ops * len(copies.regions), status="ok", message="", job_run_id=job_run_id)


def record_run(sql, run):
    cols = ["typ", "build", "mode", "src_table", "src_version", "src_committed_at", "started_at", "finished_at", "minutes",
            "keys_total", "keys_written", "keys_deleted", "bytes_written", "est_usd", "do_rows", "status", "message", "job_run_id"]

    def lit(v):
        if v is None:
            return "NULL"
        if isinstance(v, (int, float)):
            return str(v)
        if hasattr(v, "isoformat"):
            return f"TIMESTAMP '{v.isoformat()}'"
        return "'" + str(v).replace("'", "''")[:2000] + "'"
    sql(f"INSERT INTO {SCHEMA}.refresh_runs ({', '.join(cols)}) VALUES ({', '.join(lit(run.get(c)) for c in cols)})")
