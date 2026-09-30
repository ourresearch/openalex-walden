"""Keywords nightly (oxjob #1322): the closed-vocabulary normalisation that turns the student tagger's raw keyword strings into
work_keywords_v2 rows, shared by the notebooks in notebooks/keywords/nightly/.

NORMALISE_STEPS is GENERATED, not hand-written: it is the SQL that built the shipped vocabulary's per-work rows
(oxjobs keyword-tagger-optimization scratch/vocab/build_vocab.py STEPS with the production flags
`--boilerplate --synmap kw1322v3_synmap9 --min-lp -1.2 --floor 100`, then scratch/writepath/gen_build.py wk_select), rendered for the
2026-09-30 new-works catch-up and turned into templates by scratch/nightly/gen_walden_sql.py, where only table names became <<TOKENS>>.
Rendering the templates with the catch-up's table names gives that SQL back byte for byte (scratch/nightly/check_render.py).
Do not edit a rule here by hand: change build_vocab.py, rebuild, regenerate.

Per mention (raw string, log-prob lp, position): lp >= -1.2; raw -> kid0 (UDF) -> plural fold -> synonym map -> kid; fates malformed,
boilerplate (per source), loop, discipline_lazy, venue_name, purge (doc-type labels) / dual_use_redundant, country_no_evidence, else keep;
works of templated sources keep their first 2 kept mentions; then only kids already in the served vocabulary (keywords_v2, closed: nothing is
added), position = first mention, score = round(exp(max lp), 3). Works left with no keyword get no row (as in the corpus build).
"""

TAGGER_VERSION = "kw1322-student-v3fix"
MODEL = "/vol/models/student_qwen4b_v3fix"
QUANT = "fp8"

SERVED_TYPE = "array<struct<id:string,display_name:string,score:float>>"   # = typeof(openalex_works.keywords)
# the served vocabulary in the shape the templates read (keyword_id 'keywords/<kid>', kid, display_name)
VOCAB_FROM_KEYWORDS_V2 = "(SELECT concat('keywords/', kid) AS keyword_id, kid, display_name FROM openalex.common.keywords_v2)"

NORMALISE_STEPS = [
    ('strings0', r"""CREATE OR REPLACE TABLE <<STAGE>>strings0 AS
    SELECT raw, mentions, <<KID0>>(raw) AS kid0 FROM (SELECT raw, count(*) AS mentions FROM (SELECT s.id AS work_id, e.pos, trim(e.z.keywords) AS raw, e.z.lp AS lp FROM <<RAW>> s LATERAL VIEW posexplode(arrays_zip(s.keywords, s.lp)) e AS pos, z WHERE e.z.lp >= -1.2) GROUP BY raw)"""),
    ('strings', r"""CREATE OR REPLACE TABLE <<STAGE>>strings AS
    SELECT s.raw, s.mentions, s.kid0, COALESCE(sy.canon, f.kid, s.kid0) AS kid FROM <<STAGE>>strings0 s LEFT JOIN <<FOLD>> f ON f.kid0 = s.kid0 LEFT JOIN <<SYNMAP>> sy ON sy.kid = COALESCE(f.kid, s.kid0)"""),
    ('mentions_kept', r"""CREATE OR REPLACE TABLE <<STAGE>>mentions_kept AS
    WITH m AS (SELECT s.id AS work_id, e.pos, trim(e.z.keywords) AS raw, e.z.lp AS lp FROM <<RAW>> s LATERAL VIEW posexplode(arrays_zip(s.keywords, s.lp)) e AS pos, z WHERE e.z.lp >= -1.2),
    mk AS (SELECT m.work_id, m.pos, m.raw, m.lp, st.kid, st.kid0 FROM m JOIN <<STAGE>>strings st ON st.raw = m.raw WHERE st.kid IS NOT NULL),
    pk AS (SELECT kid, max(CAST(NOT dual_use AS INT)) AS hard, collect_set(CASE WHEN dual_use THEN named_struct('kind', kind, 'maps_to', maps_to) END) AS dual FROM <<PURGE>> GROUP BY kid),
    ck AS (SELECT kid, collect_set(named_struct('cc', cc, 'rx', rx)) AS cs FROM <<COUNTRY_KIDS>> GROUP BY kid),
    flagged AS (SELECT mk.*, pk.hard, pk.dual, ck.cs FROM mk LEFT JOIN pk ON pk.kid = mk.kid LEFT JOIN ck ON ck.kid = mk.kid),
    mal AS (SELECT work_id, pos FROM mk WHERE
                 raw RLIKE '\\x{FFFD}' OR NOT raw RLIKE '^[\\p{L}\\p{N}(\\["#.\']'
                 OR lower(raw) IN ('as','in','the','a','an','of','on','and','for','to','with','by','at','from','or','is')
                 OR raw RLIKE '^[0-9]{1,4}$' OR NOT raw RLIKE '\\p{L}'
                 OR length(raw) - length(replace(raw, '(', '')) <> length(raw) - length(replace(raw, ')', ''))
                 OR (raw RLIKE '(?i)^[0-9]+(st|nd|rd|th)[ -]century' AND CAST(regexp_extract(raw, '^([0-9]+)', 1) AS INT) > 21)),
    lp2 AS (SELECT work_id, pos, concat_ws(' ', slice(split(lower(raw), ' '), 1, 2)) AS pfx FROM mk WHERE size(split(raw, ' ')) >= 3),
    loops AS (SELECT work_id, pos FROM (SELECT *, count(*) OVER (PARTITION BY work_id, pfx) AS c, row_number() OVER (PARTITION BY work_id, pfx ORDER BY pos) AS r FROM lp2) WHERE c >= 6 AND r > 3),
    src AS (SELECT DISTINCT <<KID0>>(n) AS kid0 FROM openalex.sources.sources_api LATERAL VIEW explode(array(display_name, host_organization_name)) t AS n WHERE n IS NOT NULL),
    vcand AS (SELECT mk.work_id, mk.pos, mk.kid0 FROM mk JOIN src ON src.kid0 = mk.kid0),
    venue AS (SELECT v.work_id, v.pos FROM vcand v JOIN openalex.works.openalex_works w ON w.id = CAST(substr(v.work_id, 2) AS BIGINT)
              WHERE v.kid0 IN (<<KID0>>(w.primary_location.source.display_name), <<KID0>>(w.primary_location.source.host_organization_name))),
    wsrc AS (SELECT concat('W', CAST(id AS STRING)) AS work_id, primary_location.source.id AS src FROM openalex.works.openalex_works WHERE primary_location.source.id IN (SELECT DISTINCT src FROM <<BOILER>>)),
    boil AS (SELECT mk.work_id, mk.pos FROM mk JOIN wsrc ON wsrc.work_id = mk.work_id JOIN <<BOILER>> b ON b.src = wsrc.src AND b.kid = mk.kid WHERE mk.kid NOT IN ('large-helical-device')),
    
    disc AS (SELECT mk.work_id, mk.pos FROM mk JOIN openalex.works.openalex_works dw ON dw.id = CAST(substr(mk.work_id, 2) AS BIGINT)
             WHERE mk.kid IN ('medicine', 'biology', 'physics', 'chemistry', 'computer-science', 'engineering', 'psychology', 'economics', 'history', 'education', 'literature', 'literary-criticism', 'epidemiology', 'theology', 'geography', 'public-health', 'physiology', 'sociology', 'philosophy', 'anthropology', 'archaeology', 'linguistics', 'law', 'political-science', 'statistics', 'ecology', 'geology', 'astronomy', 'botany', 'zoology', 'genetics', 'immunology', 'microbiology', 'pharmacology', 'neuroscience', 'nursing', 'agriculture', 'management', 'business', 'art', 'music', 'religion', 'humanities', 'social-sciences', 'natural-sciences', 'life-sciences', 'earth-sciences', 'environmental-science', 'materials-science', 'biochemistry', 'molecular-biology', 'cell-biology', 'oncology', 'cardiology', 'psychiatry', 'pediatrics', 'surgery', 'dentistry', 'veterinary-medicine', 'architecture', 'finance', 'accounting', 'marketing', 'sciences', 'science', 'technology', 'health', 'research') AND COALESCE(dw.type, '') NOT IN ('book', 'reference-entry', 'libguides')
               AND instr(lower(COALESCE(dw.title, '')), replace(mk.kid, '-', ' ')) = 0),
    need AS (SELECT DISTINCT work_id FROM flagged WHERE hard = 0 OR cs IS NOT NULL),
    w AS (SELECT concat('W', CAST(w.id AS STRING)) AS work_id, w.type, COALESCE(w.title, '') AS title, COALESCE(w.abstract, '') AS abstract,
                 flatten(transform(w.authorships, x -> COALESCE(x.countries, array()))) AS inst_cc, sd.study_designs
          FROM openalex.works.openalex_works w LEFT JOIN openalex.works.works_study_design sd ON sd.work_id = w.id
          WHERE concat('W', CAST(w.id AS STRING)) IN (SELECT work_id FROM need)),
    judged AS (SELECT f.work_id, f.pos, f.raw, f.lp, f.kid,
                 CASE WHEN ml.pos IS NOT NULL THEN 'malformed' WHEN bo.pos IS NOT NULL THEN 'boilerplate'  WHEN lo.pos IS NOT NULL THEN 'loop' WHEN di.pos IS NOT NULL THEN 'discipline_lazy' WHEN ve.pos IS NOT NULL THEN 'venue_name'
                      WHEN f.hard = 1 THEN 'purge'
                      WHEN f.hard = 0 AND exists(f.dual, d -> (d.kind = 'work_type' AND d.maps_to = w.type) OR (d.kind = 'study_design' AND array_contains(COALESCE(w.study_designs, array()), d.maps_to))) THEN 'dual_use_redundant'
                      WHEN f.cs IS NOT NULL AND NOT exists(f.cs, c -> array_contains(w.inst_cc, c.cc) OR concat(w.title, ' ', w.abstract) RLIKE c.rx) THEN 'country_no_evidence'
                      ELSE 'keep' END AS fate
               FROM flagged f LEFT JOIN w ON w.work_id = f.work_id
               LEFT JOIN mal ml ON ml.work_id = f.work_id AND ml.pos = f.pos LEFT JOIN loops lo ON lo.work_id = f.work_id AND lo.pos = f.pos
               LEFT JOIN venue ve ON ve.work_id = f.work_id AND ve.pos = f.pos LEFT JOIN disc di ON di.work_id = f.work_id AND di.pos = f.pos LEFT JOIN boil bo ON bo.work_id = f.work_id AND bo.pos = f.pos )
    SELECT work_id, pos, raw, lp, kid, fate FROM judged"""),
    ('final', r"""CREATE OR REPLACE TABLE <<STAGE>>mentions_final AS
    WITH k AS (SELECT m.*, row_number() OVER (PARTITION BY m.work_id ORDER BY m.pos) AS rk FROM <<STAGE>>mentions_kept m WHERE m.fate = 'keep'),
    tw AS (SELECT concat('W', CAST(w.id AS STRING)) AS work_id FROM openalex.works.openalex_works w JOIN <<TEMPLATED>> t ON t.src = w.primary_location.source.id)
    SELECT k.work_id, k.pos, k.raw, k.lp, k.kid FROM k LEFT JOIN tw ON tw.work_id = k.work_id WHERE tw.work_id IS NULL OR k.rk <= 2"""),
    ('work_keywords', r"""CREATE OR REPLACE TABLE <<STAGE>>work_keywords AS
    WITH m AS (SELECT work_id, kid, min(pos) AS pos, max(lp) AS lp FROM <<STAGE>>mentions_final GROUP BY work_id, kid),
    v AS (SELECT m.work_id, m.kid, m.pos, m.lp, v.keyword_id, v.display_name FROM m JOIN <<VOCAB>> v ON v.kid = m.kid),
    r AS (SELECT *, row_number() OVER (PARTITION BY work_id ORDER BY pos) - 1 AS rnk, count(*) OVER (PARTITION BY work_id) AS n FROM v)
    SELECT work_id, keyword_id, display_name, CAST(rnk AS INT) AS position, round(exp(lp), 3) AS score FROM r"""),
    ('newworks', r"""CREATE OR REPLACE TABLE <<ROWS>>
COMMENT '<<COMMENT>>'
AS SELECT
  CAST(SUBSTRING(k.work_id, 2) AS BIGINT) AS work_id,
  -- ARRAY<STRUCT<id STRING, display_name STRING, score FLOAT>> in position order: the exact type of openalex_works.keywords
  TRANSFORM(
    ARRAY_SORT(COLLECT_LIST(NAMED_STRUCT(
      'position', k.position,
      'id', CONCAT('https://openalex.org/', k.keyword_id),
      'display_name', v.display_name,
      'score', CAST(k.score AS FLOAT)))),
    x -> NAMED_STRUCT('id', x.id, 'display_name', x.display_name, 'score', x.score)
  ) AS keywords,
  '<<TAGGER_VERSION>>' AS tagger_version,
  CURRENT_TIMESTAMP() AS updated_at
FROM <<STAGE>>work_keywords k
JOIN <<VOCAB>> v ON v.keyword_id = k.keyword_id
WHERE CAST(SUBSTRING(k.work_id, 2) AS BIGINT) NOT IN (SELECT work_id FROM <<TARGET>>)   -- append-only: new work_ids
GROUP BY 1"""),
]


TOKENS = ("RAW", "STAGE", "ROWS", "KID0", "FOLD", "SYNMAP", "PURGE", "COUNTRY_KIDS", "BOILER", "TEMPLATED", "VOCAB", "TARGET",
          "TAGGER_VERSION", "COMMENT")


def render(template, values):
    """Replace every <<TOKEN>>; fail on a missing or unknown token."""
    out = template
    for k in TOKENS:
        if f"<<{k}>>" in out:
            if k not in values:
                raise KeyError(f"no value for <<{k}>>")
            out = out.replace(f"<<{k}>>", values[k])
    if "<<" in out and ">>" in out.split("<<", 1)[1]:
        raise ValueError("unrendered token in: " + out.split("<<", 1)[1][:60])
    return out


def normalise_statements(values):
    """[(step, sql)] in run order. values: RAW (table with id 'W<n>', keywords ARRAY<STRING>, lp ARRAY<DOUBLE>), STAGE (prefix of the per-run
    tables), ROWS (output table, work_keywords_v2 shape), TARGET (served table: rows already there are left out), rule tables, VOCAB."""
    return [(name, render(sql, values)) for name, sql in NORMALISE_STEPS]


def check_statements(rows, target, queue, vocab_table="openalex.common.keywords_v2"):
    """Pre-append checks on the output rows; each returns one row, 'ok' says pass."""
    return {
        "shape": f"""SELECT COUNT(*) AS works, COUNT(DISTINCT work_id) AS distinct_works, COALESCE(MIN(SIZE(keywords)), 1) AS min_kw,
            COUNT_IF(work_id IS NULL OR keywords IS NULL OR EXISTS(keywords, x -> x.id IS NULL OR x.display_name IS NULL OR x.score IS NULL)) AS nulls,
            COUNT_IF(tagger_version <> '{TAGGER_VERSION}' OR tagger_version IS NULL) AS bad_version,
            COUNT_IF(size(keywords) <> size(array_distinct(transform(keywords, x -> x.id)))) AS dup_ids
          FROM {rows}""",
        "already_in_target": f"SELECT COUNT(*) AS n FROM {rows} r JOIN {target} t ON t.work_id = r.work_id",
        "not_in_queue": f"SELECT COUNT(*) AS n FROM {rows} r LEFT ANTI JOIN {queue} q ON q.id = r.work_id",
        "ids_not_in_vocab": f"""SELECT COUNT(*) AS n FROM (SELECT explode(keywords) AS k FROM {rows}) e
          LEFT ANTI JOIN {vocab_table} kv ON kv.id = e.k.id AND kv.display_name = e.k.display_name""",
        "types": f"""SELECT (SELECT typeof(keywords) FROM {rows} LIMIT 1) AS rows_type, (SELECT typeof(keywords) FROM {target} LIMIT 1) AS target_type,
          (SELECT concat_ws(',', typeof(work_id), typeof(tagger_version), typeof(updated_at)) FROM {rows} LIMIT 1) AS rows_cols,
          (SELECT concat_ws(',', typeof(work_id), typeof(tagger_version), typeof(updated_at)) FROM {target} LIMIT 1) AS target_cols""",
    }


def checks_pass(res):
    """res: {name: first row as dict}. Returns (ok, reasons)."""
    s, t = res["shape"], res["types"]
    bad = []
    if int(s["works"]) != int(s["distinct_works"]): bad.append("duplicate work_ids")
    if int(s["nulls"]): bad.append(f"{s['nulls']} rows with nulls")
    if int(s["bad_version"]): bad.append("wrong tagger_version")
    if int(s["dup_ids"]): bad.append("a keyword twice in one work")
    if int(s["min_kw"]) < 1: bad.append("empty keyword list")
    if int(res["already_in_target"]["n"]): bad.append(f"{res['already_in_target']['n']} work_ids already in the target")
    if int(res["not_in_queue"]["n"]): bad.append(f"{res['not_in_queue']['n']} work_ids not in the queue")
    if int(res["ids_not_in_vocab"]["n"]): bad.append(f"{res['ids_not_in_vocab']['n']} keyword ids / names not in the vocabulary")
    if int(s["works"]):   # no rows (every work ended empty) = nothing to type-check; an empty target has no row to compare with
        if t["rows_type"] != SERVED_TYPE: bad.append(f"rows type {t['rows_type']}")
        if t["target_type"] is not None and (t["rows_type"] != t["target_type"] or t["rows_cols"] != t["target_cols"]):
            bad.append(f"types differ: {t}")
    return (not bad), bad
