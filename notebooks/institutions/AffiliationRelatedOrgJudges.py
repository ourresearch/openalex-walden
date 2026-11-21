# Databricks notebook source
# MAGIC %md
# MAGIC ### Related-organization judges (oxjob #2493)
# MAGIC
# MAGIC **Rule** (`What "right" means`, affiliations charter): a string is matched only to what it names, and an organization
# MAGIC ROR lists as *related* to a university (its teaching hospital, university medical center, a partner institute) never
# MAGIC brings the university. The matcher's teacher was trained on a question that said the opposite, so the nightly still
# MAGIC links some universities to strings that name only their related hospital. The related-org guard (task 1 of this
# MAGIC job) removes the classes a name rule reads reliably. This task takes the rest, on strings answered in the last
# MAGIC `since_hours`:
# MAGIC
# MAGIC * **A, university and related organization both linked**, the name rule's held-out classes (`drop:absent`,
# MAGIC   `drop:hosp_compound`, `drop:hosp_romance`): drop the university where two judges agree the string does not name it.
# MAGIC * **B, university linked alone, a related organization among the matcher's candidates** (`scores`), the name rule
# MAGIC   (with the related organization's names masked) says the string doesn't name the university: a swap. Where two
# MAGIC   judges agree the string does not name the university AND two judges agree it names a related candidate, the
# MAGIC   candidate replaces the university ("McGill University Health Centre" -> MUHC, not McGill). No swap, no change.
# MAGIC
# MAGIC **Judges:** Claude Sonnet 5.5, then Claude Opus 5.5 on Sonnet's verdicts that would change the answer, each blind to
# MAGIC the other, one institution card (names, place, ROR parents, units, related) and up to 20 strings per prompt; the
# MAGIC rubric is the institution QC judge of record (oxjob #1387). Only agreement acts. Bars (blind GPT-6 Astra): route A
# MAGIC 499/500 on 98,614 held-out pairs (patch layer 12, oxjob #2493); route B swaps on strings answered since 10 Nov:
# MAGIC university removed 152/152 and related organization added 249/250 over two draws.
# MAGIC
# MAGIC **Writes** (unless `dry_run`): MERGE into `affiliation_matcher_answers` only where the answer still equals the
# MAGIC record's `before_ids`; one `oxjob1395_patches` row per string (layer 12, `related_org_judges_daily_2493`); every
# MAGIC judged pair into `affiliation_related_org_judged`, so a pair is judged once per answer. No MV refresh: Institutions
# MAGIC Frontfill does it in the End 2 End. **Revert:** pause the task; undo a day by MERGEing `before_ids` back from its
# MAGIC patches rows where `institution_ids = after_ids`.

# COMMAND ----------

import json
import random
import re

dbutils.widgets.text("since_hours", "72")
dbutils.widgets.text("dry_run", "false")
dbutils.widgets.text("max_pairs", "20000")
dbutils.widgets.text("max_usd", "40")
dbutils.widgets.text("tag", "")  # suffix for a dry run's tables, so they never touch the live ones

SINCE_H = int(dbutils.widgets.get("since_hours"))
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
MAX_PAIRS = int(dbutils.widgets.get("max_pairs"))
MAX_USD = float(dbutils.widgets.get("max_usd"))
TAG = re.sub(r"[^a-z0-9_]", "", dbutils.widgets.get("tag").strip().lower())
SFX = f"_{TAG}" if TAG else ""
if DRY and not TAG:
    TAG, SFX = "dry", "_dry"

SONNET, OPUS = "databricks-claude-sonnet-5-5", "databricks-claude-opus-5-5"
PRICE = {SONNET: (2.0, 10.0), OPUS: (4.0, 20.0)}  # $ per 1M tokens in, out (list price)
A = "openalex.institutions.affiliation_matcher_answers"
MV = "openalex.institutions.raw_affiliation_strings_institutions_mv"
P = "openalex.institutions.oxjob1395_patches"
API = "openalex.institutions.institutions_api"
STATE = f"openalex.institutions.affiliation_related_org_judged{SFX}"
PAIRS = f"openalex.institutions.affiliation_related_org_judge_pairs{SFX}"
PROMPTS = f"openalex.institutions.affiliation_related_org_judge_prompts{SFX}"
REPLIES = f"openalex.institutions.affiliation_related_org_judge_replies{SFX}"
REC = f"openalex.institutions.affiliation_related_org_judges_today{SFX}"
LAYER, LAYER_NAME = 12, "related_org_judges_daily_2493"
PER = 20
print(f"since_hours={SINCE_H} dry_run={DRY} tag={TAG!r} max_pairs={MAX_PAIRS} max_usd={MAX_USD}")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {STATE} (raw_affiliation_string STRING, u_id BIGINT, live_ids ARRAY<BIGINT>, route STRING,
  side STRING, inst_id BIGINT, sonnet STRING, opus STRING, acted BOOLEAN, judged_at TIMESTAMP)""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Pairs: (string, university) in route A or B, not judged before for this answer
# MAGIC The CTEs from `inst` to `v` mirror `affiliation_related_org_guard.sql` (same UDF, same masks); they differ in `a`
# MAGIC (any answer of the window, with its candidates) and `c` (the related organization linked, or only a candidate).

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {PAIRS} AS
WITH inst AS (SELECT id, type, lineage FROM {API}),
ai AS (SELECT i.id AS a_id, CAST(regexp_extract(x.id, 'I([0-9]+)$', 1) AS BIGINT) AS b_id, x.relationship AS rel
       FROM {API} i LATERAL VIEW explode(i.associated_institutions) t AS x),
relp AS (SELECT a_id AS u_id, b_id AS h_id FROM ai WHERE rel = 'related' UNION SELECT b_id, a_id FROM ai WHERE rel = 'related'),
pc AS (SELECT a_id, b_id FROM ai WHERE rel IN ('parent', 'child')),
rel AS (SELECT r.u_id, r.h_id, h.type AS h_type FROM relp r
        JOIN inst u ON u.id = r.u_id AND u.type = 'education' JOIN inst h ON h.id = r.h_id AND h.type <> 'education'
        LEFT ANTI JOIN pc ON (pc.a_id = r.u_id AND pc.b_id = r.h_id) OR (pc.a_id = r.h_id AND pc.b_id = r.u_id)
        WHERE NOT array_contains(h.lineage, concat('https://openalex.org/I', r.u_id))
          AND NOT array_contains(u.lineage, concat('https://openalex.org/I', r.h_id))),
a AS (SELECT raw_affiliation_string AS s, institution_ids AS ids, scores FROM {A}
      WHERE run_at >= current_timestamp() - make_interval(0, 0, 0, 0, {SINCE_H}, 0, 0) AND size(institution_ids) >= 1),
x AS (SELECT a.s, a.ids, rel.u_id, rel.h_id, rel.h_type, array_contains(a.ids, rel.h_id) AS h_linked,
             coalesce(a.scores IS NOT NULL AND map_contains_key(a.scores, rel.h_id), false) AS h_cand
      FROM (SELECT s, ids, scores, explode(ids) AS iid FROM a) a JOIN rel ON rel.u_id = a.iid),
c0 AS (SELECT s, ids AS live_ids, u_id, max(CAST(h_linked AS INT)) = 1 AS any_linked,
              collect_set(CASE WHEN h_linked OR h_cand THEN h_type END) AS h_types,
              collect_set(CASE WHEN NOT h_linked AND h_cand THEN h_id END) AS h_cand_ids
       FROM x GROUP BY s, ids, u_id HAVING max(CAST(h_linked AS INT)) = 1 OR max(CAST(h_cand AS INT)) = 1),
c AS (SELECT s, CASE WHEN any_linked THEN live_ids ELSE array_union(live_ids, h_cand_ids) END AS ids, live_ids, u_id, h_types,
             CASE WHEN any_linked THEN 'A' ELSE 'B' END AS route, CASE WHEN any_linked THEN array() ELSE h_cand_ids END AS h_cand_ids
      FROM c0),
nm0 AS (SELECT id, norm, kind FROM openalex.institutions.oxjob1395_names_v1),
live AS (SELECT i.id, lower(n) AS norm, k AS kind FROM {API} i
         LATERAL VIEW explode(array_union(array(i.display_name), coalesce(i.display_name_alternatives, array()))) t1 AS n
         LATERAL VIEW explode(array('alt')) t2 AS k WHERE i.id NOT IN (SELECT DISTINCT id FROM nm0)
         UNION ALL SELECT i.id, lower(n), 'acronym' FROM {API} i
         LATERAL VIEW explode(coalesce(i.display_name_acronyms, array())) t AS n WHERE i.id NOT IN (SELECT DISTINCT id FROM nm0)),
nm AS (SELECT id, collect_set(CASE WHEN kind <> 'acronym' THEN norm END) AS names, collect_set(CASE WHEN kind = 'acronym' THEN norm END) AS acr
       FROM (SELECT * FROM nm0 UNION ALL SELECT * FROM live) GROUP BY id),
dsc AS (SELECT DISTINCT CAST(regexp_extract(l, 'I([0-9]+)$', 1) AS BIGINT) AS anc, i.id AS d FROM {API} i LATERAL VIEW explode(i.lineage) t AS l),
dn AS (SELECT x.anc AS u_id, slice(collect_set(n.norm), 1, 400) AS desc_names
       FROM (SELECT * FROM dsc WHERE anc <> d AND anc IN (SELECT DISTINCT u_id FROM c)) x
       JOIN nm0 n ON n.id = x.d AND n.kind <> 'acronym' GROUP BY 1),
o AS (SELECT q.s, q.u_id, flatten(collect_list(concat(coalesce(nm.names, array()), coalesce(nm.acr, array())))) AS mask_names
      FROM (SELECT s, u_id, explode(ids) AS oid FROM c) q
      LEFT ANTI JOIN (SELECT * FROM dsc WHERE anc <> d) d2 ON d2.anc = q.u_id AND d2.d = q.oid
      JOIN nm ON nm.id = q.oid WHERE q.oid <> q.u_id GROUP BY 1, 2),
cur AS (SELECT raw_affiliation_string, curated_add_ids FROM {MV} WHERE size(curated_add_ids) > 0),
pat AS (SELECT raw_affiliation_string, flatten(collect_list(add_ids)) AS patch_adds FROM {P} GROUP BY 1),
v AS (SELECT c.s, c.u_id, c.live_ids, c.route, c.h_cand_ids,
             openalex.institutions.affiliation_names_u(c.s, nm.names, nm.acr, coalesce(dn.desc_names, array()), coalesce(o.mask_names, array()),
                 array(ui.geo.city, ui.geo.region, ui.geo.country)) AS verdict
      FROM c JOIN nm ON nm.id = c.u_id AND size(nm.names) > 0
      LEFT JOIN {API} ui ON ui.id = c.u_id
      LEFT JOIN dn ON dn.u_id = c.u_id LEFT JOIN o ON o.s = c.s AND o.u_id = c.u_id
      LEFT JOIN cur ON cur.raw_affiliation_string = c.s LEFT JOIN pat ON pat.raw_affiliation_string = c.s
      WHERE NOT coalesce(array_contains(cur.curated_add_ids, c.u_id), false) AND NOT coalesce(array_contains(pat.patch_adds, c.u_id), false))
SELECT v.s AS raw_affiliation_string, v.u_id, v.live_ids, v.route, v.h_cand_ids, v.verdict FROM v
LEFT ANTI JOIN {STATE} st ON st.raw_affiliation_string = v.s AND st.u_id = v.u_id AND st.live_ids <=> v.live_ids AND st.side = 'u'
WHERE (v.route = 'A' AND v.verdict IN ('drop:absent', 'drop:hosp_compound', 'drop:hosp_romance'))
   OR (v.route = 'B' AND v.verdict LIKE 'drop:%')""")
counts = spark.sql(f"SELECT route, verdict, count(*) n FROM {PAIRS} GROUP BY 1, 2 ORDER BY 3 DESC").collect()
for r in counts:
    print(r.route, r.verdict, r.n)
n_pairs = sum(r.n for r in counts)
print("pairs to judge:", n_pairs)
if n_pairs > MAX_PAIRS:
    raise Exception(f"related-org judges: {n_pairs} pairs, over max_pairs {MAX_PAIRS}; check before judging (oxjob #2493)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Cards and prompts (the institution QC judge's rubric, verdict letters)

# COMMAND ----------

SYSTEM = """You check institution assignments for OpenAlex, an open index of scholarly works. Each work's authors have raw affiliation strings copied from the paper, and OpenAlex links each string to the institution records it names. You get one institution record (a card) and a numbered list of affiliation strings. For each string, decide whether it names this institution as an affiliation.

Verdicts:
- names_it: the string names this institution itself: its name, a translation or transliteration, an acronym that plainly refers to it in context, or a former name when the former organisation has no record of its own. A department, lab or other part written with the institution's name counts ("Dept. of Physics, University of Oxford" names University of Oxford). A joint unit that lists it as a partner counts ("UMR 7590, CNRS, Sorbonne Université" names both CNRS and Sorbonne Université).
- names_unit_of_it: the string does not name this institution itself, but names an organisation that belongs to it: a school, faculty, hospital, institute, laboratory, campus, or a predecessor that merged into it. Use the card's list of units and parents; membership follows ROR, so organisations the card lists as "related" (affiliated hospitals, partners) do NOT belong to it. For a unit not on the card, count it only when it is plainly part of this institution (a faculty or department of it, "Université X, Faculté de Médecine"), not a partner or affiliate.
- no: the string does not name this institution or any unit of it. This includes: a different organisation with a similar or identical name (another city, state or country: University of Georgia in Athens, USA is not the University of Georgia in Tbilisi); this institution's parent or a sibling named alone; an affiliated but separate organisation; only a place (a city, street, postcode or country never names an institution); a funder, acknowledgement, email domain or publisher boilerplate; or nothing identifiable.
- unsure: you cannot tell (a generic name with no anchor such as "Children's Hospital" or "Institute of Physics", an acronym that could be several organisations, a garbled string).

Further rules:
1. A name inside a longer name is not a separate mention: "John Brown University" does not name Brown University; "Korea University Guro Hospital" names that hospital (a unit of Korea University only if it belongs to it).
2. "University Department of X, <City>" or "Faculté de X, <City>" names the city's single obvious university; nothing if the city has several.
3. "Key Laboratory of X (Ministry of Education)" names the host university only, not the ministry.
4. Companies often have one record per country ("Pfizer (France)"). A country-specific company record is named only by strings from that country (or when the country is not stated and it is the main record).
5. Script and language do not matter.
6. Judge only what the string says. Do not assume an author's affiliation from their name, a topic or your memory of the paper.

Reply with one line per string, in order, "<string number> <letter>", where the letter is I (names_it), U (names_unit_of_it), N (no) or ? (unsure). No other text."""
LINE = re.compile(r"(\d+)\s+([IUN?])(?=\s|$)")


def build_cards(ids):
    if not ids:
        return {}
    spark.createDataFrame([(int(i),) for i in ids], "id BIGINT").createOrReplaceTempView("card_ids")
    rows = spark.sql(f"""
WITH i AS (SELECT * FROM {API}),
base AS (SELECT id, display_name, to_json(display_name_alternatives) alts, to_json(display_name_acronyms) acr, geo.city city,
                geo.region region, geo.country country, type, lineage FROM i WHERE id IN (SELECT id FROM card_ids)),
ai AS (SELECT b.id, x.display_name dn, x.relationship rel FROM i b LATERAL VIEW explode(b.associated_institutions) t AS x
       WHERE b.id IN (SELECT id FROM card_ids)),
anc AS (SELECT x.id, i2.display_name dn FROM (SELECT b.id, l FROM base b LATERAL VIEW explode(b.lineage) t AS l) x
        JOIN i i2 ON i2.id = CAST(regexp_extract(x.l, 'I([0-9]+)$', 1) AS BIGINT) WHERE i2.id <> x.id),
dsc AS (SELECT CAST(regexp_extract(l, 'I([0-9]+)$', 1) AS BIGINT) AS anc_id, i.display_name dn, i.works_count wc
        FROM i LATERAL VIEW explode(i.lineage) t AS l),
dsc2 AS (SELECT d.anc_id id, d.dn, d.wc FROM dsc d WHERE d.anc_id IN (SELECT id FROM card_ids))
SELECT b.id, b.display_name, b.alts, b.acr, b.city, b.region, b.country, b.type,
       to_json((SELECT collect_set(dn) FROM ai WHERE ai.id = b.id AND ai.rel = 'parent')) parents_ror,
       to_json((SELECT collect_set(dn) FROM anc WHERE anc.id = b.id)) parents_lin,
       to_json((SELECT collect_set(dn) FROM ai WHERE ai.id = b.id AND ai.rel = 'child')) kids_ror,
       to_json((SELECT array_sort(collect_list(named_struct('w', -coalesce(wc, 0), 'n', dn))) FROM dsc2 WHERE dsc2.id = b.id AND dsc2.dn <> b.display_name)) kids_lin,
       to_json((SELECT collect_set(dn) FROM ai WHERE ai.id = b.id AND ai.rel = 'related')) related
FROM base b""").collect()
    out = {}
    for r in rows:
        j = lambda v: json.loads(v) if v else []
        lin = [x["n"] for x in j(r.kids_lin)]
        out[int(r.id)] = dict(
            name=r.display_name, alts=[x for x in j(r.alts) if x != r.display_name], acr=j(r.acr),
            city=r.city, region=r.region, country=r.country, type=r.type,
            parents=list(dict.fromkeys(j(r.parents_ror) + j(r.parents_lin))),
            units=list(dict.fromkeys(lin + sorted(j(r.kids_ror)))), related=j(r.related))
    return out


def card(c):
    lines = [f"Name: {c['name']}"]
    if c.get("alts"):
        lines.append("Other names: " + "; ".join(c["alts"][:12]))
    if c.get("acr"):
        lines.append("Acronyms: " + ", ".join(c["acr"][:8]))
    lines.append("Location: " + (", ".join(x for x in (c.get("city"), c.get("region"), c.get("country")) if x) or "-"))
    lines.append(f"Type: {c.get('type') or '-'}")
    if c.get("parents"):
        lines.append("It is part of (ROR parent / OpenAlex lineage): " + "; ".join(c["parents"]))
    units = c.get("units") or []
    if units:
        more = f" (and {len(units) - 30} more)" if len(units) > 30 else ""
        lines.append(f"Units that belong to it (ROR children / OpenAlex lineage, {len(units)} total, largest first): "
                     + "; ".join(units[:30]) + more)
    if c.get("related"):
        lines.append("Related but NOT part of it (ROR 'related'): " + "; ".join(c["related"][:10]))
    return "\n".join(lines)


def judge(stage, endpoint, items, cards, seed):
    """items: [(inst_id, string)] -> {(inst_id, string): letter or None}. One ai_query pass, <= 20 strings per card."""
    by = {}
    for i, s in items:
        by.setdefault(i, set()).add(s)
    rng = random.Random(seed)
    rows = []
    for i in sorted(by):
        if i not in cards:
            continue
        ss = sorted(by[i])
        rng.shuffle(ss)
        for k in range(0, len(ss), PER):
            chunk = ss[k:k + PER]
            prompt = (SYSTEM + "\n\nInstitution card:\n" + card(cards[i]) + "\n\nAffiliation strings:\n"
                      + "\n".join(f"{n + 1}. {' '.join(s.split())[:1500]}" for n, s in enumerate(chunk)))
            rows.append((f"{stage}_{i}_{k // PER}", int(i), json.dumps(chunk, ensure_ascii=False), prompt))
    if not rows:
        return {}
    pin, pout = PRICE[endpoint]
    est = sum(len(r[3]) for r in rows) / 3.5 * pin / 1e6 + len(rows) * 400 * pout / 1e6
    print(f"{stage}: {len(items)} pairs in {len(rows)} prompts, est ${est:.2f}")
    spent[0] += est
    if spent[0] > MAX_USD:
        raise Exception(f"related-org judges: estimated spend ${spent[0]:.2f} over max_usd {MAX_USD} (oxjob #2493)")
    spark.createDataFrame(rows, "batch_id STRING, inst_id BIGINT, items STRING, prompt STRING").write.mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(PROMPTS)
    spark.sql(f"""CREATE OR REPLACE TABLE {REPLIES} AS
        SELECT batch_id, inst_id, items, r.result AS response, r.errorMessage AS error
        FROM (SELECT *, ai_query('{endpoint}', prompt, modelParameters => named_struct('max_tokens', 4000), failOnError => false) AS r
              FROM {PROMPTS})""")
    out, bad = {}, 0
    for r in spark.table(REPLIES).collect():
        chunk = json.loads(r["items"])
        got = {int(n): v for n, v in LINE.findall(r["response"] or "")}
        ok = set(got) == set(range(1, len(chunk) + 1))
        bad += not ok
        for n, s in enumerate(chunk):
            out[(int(r["inst_id"]), s)] = got.get(n + 1) if ok else None
    print(f"{stage}: {len(rows) - bad}/{len(rows)} prompts parsed")
    return out


spent = [0.0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Judge the university side (both routes), then the related candidates of route B's drops

# COMMAND ----------

pairs = [r.asDict() for r in spark.table(PAIRS).collect()]
need = {p["u_id"] for p in pairs} | {h for p in pairs for h in (p["h_cand_ids"] or [])}
cards = build_cards(sorted(need))
u_items = [(p["u_id"], p["raw_affiliation_string"]) for p in pairs]
son = judge("u_sonnet", SONNET, u_items, cards, 2493)
opu = judge("u_opus", OPUS, [k for k in u_items if son.get(k) == "N"], cards, 24932)
drop_u = {k for k in u_items if son.get(k) == "N" and opu.get(k) == "N"}
h_items = sorted({(h, p["raw_affiliation_string"]) for p in pairs if p["route"] == "B"
                  and (p["u_id"], p["raw_affiliation_string"]) in drop_u for h in (p["h_cand_ids"] or [])})
hson = judge("h_sonnet", SONNET, h_items, cards, 2494)
hopu = judge("h_opus", OPUS, [k for k in h_items if hson.get(k) == "I"], cards, 24942)
add_h = {k for k in h_items if hson.get(k) == "I" and hopu.get(k) == "I"}
print(f"university drops agreed: {len(drop_u)} of {len(u_items)}; related adds agreed: {len(add_h)} of {len(h_items)}; "
      f"estimated spend ${spent[0]:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Record: per string, remove the agreed universities and add the agreed related organizations; never empty

# COMMAND ----------

by_s = {}
for p in pairs:
    by_s.setdefault(p["raw_affiliation_string"], {"live": list(p["live_ids"]), "rm": set(), "add": set(), "cls": set()})
for p in pairs:
    s, u = p["raw_affiliation_string"], p["u_id"]
    if (u, s) not in drop_u:
        continue
    d = by_s[s]
    added = [h for h in (p["h_cand_ids"] or []) if (h, s) in add_h]
    # Route B acts only as a swap: the university goes only when a related organization both judges say the string names
    # comes in. Plain route-B removals judged 87/98 right (strings naming a unit of the university with no record of its
    # own); swaps 152/152 on two draws (oxjob #2493 ACCEPTANCE Test 2).
    if p["route"] == "B" and not added:
        continue
    d["rm"].add(u)
    d["add"].update(added)
    d["cls"].add(f"{p['route']}:{p['verdict'][5:]}")
rec = []
for s, d in by_s.items():
    if not d["rm"]:
        continue
    after = [i for i in d["live"] if i not in d["rm"]] + sorted(h for h in d["add"] if h not in d["live"])
    if not after:
        continue  # never empty a string: the university stays
    rec.append((s, sorted(d["add"] - set(d["live"])), sorted(d["rm"]), d["live"], after, "+".join(sorted(d["cls"]))))
spark.createDataFrame(rec, "raw_affiliation_string STRING, add_ids ARRAY<BIGINT>, remove_ids ARRAY<BIGINT>, before_ids ARRAY<BIGINT>, "
                           "after_ids ARRAY<BIGINT>, cls STRING").createOrReplaceTempView("rec0")
spark.sql(f"""CREATE OR REPLACE TABLE {REC} AS
WITH cc AS (SELECT r.raw_affiliation_string, collect_set(i.country_code) cc FROM (SELECT raw_affiliation_string, explode(after_ids) id FROM rec0) r
            JOIN {API} i ON i.id = r.id WHERE i.country_code IS NOT NULL GROUP BY 1)
SELECT r.raw_affiliation_string, xxhash64(r.raw_affiliation_string) AS sid, concat('related_org_judges:', r.cls) AS basis,
       r.add_ids, r.remove_ids, r.before_ids, a.countries AS before_countries, r.after_ids, coalesce(cc.cc, array()) AS after_countries
FROM rec0 r JOIN {A} a ON a.raw_affiliation_string = r.raw_affiliation_string AND a.institution_ids <=> r.before_ids
LEFT JOIN cc ON cc.raw_affiliation_string = r.raw_affiliation_string""")
for r in spark.sql(f"SELECT basis, count(*) strings, sum(size(remove_ids)) removes, sum(size(add_ids)) adds FROM {REC} GROUP BY 1 ORDER BY 2 DESC").collect():
    print(r.basis, r.strings, r.removes, r.adds)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Write (skipped on a dry run) and remember every judged pair

# COMMAND ----------

acted = {(u, r[0]) for r in rec for u in r[2]}
route_of = {(p["u_id"], p["raw_affiliation_string"]): p["route"] for p in pairs}
state_rows = [(s, u, [int(x) for x in by_s[s]["live"]], route_of[(u, s)], "u", u, son.get((u, s)), opu.get((u, s)), (u, s) in acted)
              for u, s in u_items]
u_of = {}
for p in pairs:
    for h in p["h_cand_ids"] or []:
        u_of[(h, p["raw_affiliation_string"])] = p["u_id"]
state_rows += [(s, u_of[(h, s)], [int(x) for x in by_s[s]["live"]], "B", "h", h, hson.get((h, s)), hopu.get((h, s)), (h, s) in add_h)
               for h, s in h_items]
if not DRY:
    spark.sql(f"""MERGE INTO {A} t USING {REC} r ON t.raw_affiliation_string = r.raw_affiliation_string
        WHEN MATCHED AND t.institution_ids <=> r.before_ids THEN UPDATE SET t.institution_ids = r.after_ids, t.countries = r.after_countries""")
    spark.sql(f"""INSERT INTO {P} SELECT {LAYER}, '{LAYER_NAME}', current_timestamp(), r.raw_affiliation_string, r.sid, r.basis,
        r.add_ids, r.remove_ids, r.before_ids, r.before_countries, r.after_ids, r.after_countries
        FROM {REC} r JOIN {A} a ON a.raw_affiliation_string = r.raw_affiliation_string WHERE a.institution_ids <=> r.after_ids""")
if state_rows:
    spark.createDataFrame(state_rows, "raw_affiliation_string STRING, u_id BIGINT, live_ids ARRAY<BIGINT>, route STRING, side STRING, "
                                      "inst_id BIGINT, sonnet STRING, opus STRING, acted BOOLEAN") \
        .selectExpr("*", "current_timestamp() AS judged_at").write.mode("append").saveAsTable(STATE)
n = spark.sql(f"SELECT count(*) n FROM {REC}").collect()[0].n
print(f"{'dry run: would write' if DRY else 'wrote'} {n} strings; judged {len(u_items)} university pairs and {len(h_items)} related pairs; "
      f"estimated spend ${spent[0]:.2f}")
