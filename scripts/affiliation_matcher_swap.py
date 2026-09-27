#!/usr/bin/env python3
"""Swap walden's affiliation-string institution ids to the new matcher (oxjob #1386).

The new matcher's answers live in `openalex.institutions.affiliation_matcher_answers` (one row per
raw affiliation string). `raw_affiliation_strings_institutions_mv` reads them first and falls back to
the legacy model + rules where a string has no row; definition and rules in
notebooks/institutions/raw_affiliation_strings_institutions_mv.sql. So the swap is a data load, and
emptying the answers table reverts it at the next refresh.

Dry run (writes only oxjob1386_* scratch tables):
    scripts/affiliation_matcher_swap.py standin                  # today's ids as a stand-in answers table
    scripts/affiliation_matcher_swap.py without-bot              # scratch copy of ras_curations_without_bot
    scripts/affiliation_matcher_swap.py candidate --answers openalex.institutions.oxjob1385_answers_v1
    scripts/affiliation_matcher_swap.py diff                     # blast radius: strings, works, institutions
    scripts/affiliation_matcher_swap.py check-mv-unchanged       # new definition + empty answers == live MV

Production (Jason's yes + a charter write-log row before and after, every time):
    scripts/affiliation_matcher_swap.py create-answers-table     # empty table; changes nothing
    scripts/affiliation_matcher_swap.py define-mv                # CREATE OR REPLACE the MV from the .sql file
    scripts/affiliation_matcher_swap.py load --from openalex.institutions.oxjob1385_answers_v1
    scripts/affiliation_matcher_swap.py revert                   # empty the answers table

Needs the `databricks` CLI authenticated on this machine. Warehouse: --warehouse, default serverless.
"""
import argparse
import json
import pathlib
import subprocess
import sys
import time

REPO = pathlib.Path(__file__).resolve().parent.parent
MV_SQL = REPO / "notebooks/institutions/raw_affiliation_strings_institutions_mv.sql"
SYNC_NB = REPO / "notebooks/end2end/SyncRasCurations.ipynb"

MV = "openalex.institutions.raw_affiliation_strings_institutions_mv"
ANSWERS = "openalex.institutions.affiliation_matcher_answers"
WITHOUT_BOT = "openalex.institutions.ras_curations_without_bot"
LOOKUP = "openalex.institutions.affiliation_strings_lookup"
WORKS_COUNTS = "openalex.institutions.affiliation_string_works_counts"
SEATS = "openalex.works.work_author_affiliations_mv"
INSTITUTIONS = "openalex.institutions.institutions"

SCRATCH = "openalex.institutions.oxjob1386_"
STANDIN = SCRATCH + "standin_answers"
SCRATCH_WITHOUT_BOT = SCRATCH + "ras_curations_without_bot"
CANDIDATE = SCRATCH + "candidate_mv"
DIFF_STRINGS = SCRATCH + "diff_strings"
DIFF_SEATS = SCRATCH + "diff_seats"

DEFAULT_WAREHOUSE = "69a583ace3bdc8d0"  # Serverless Medium SQL

ANSWERS_DDL = f"""
CREATE TABLE IF NOT EXISTS {ANSWERS} (
  raw_affiliation_string STRING NOT NULL COMMENT 'key of affiliation_strings_lookup; one row per string',
  institution_ids ARRAY<BIGINT> NOT NULL COMMENT 'the matcher''s answer; [] = names no institution',
  countries ARRAY<STRING> COMMENT 'ISO codes; [] falls back to the lookup''s countries',
  scores MAP<BIGINT, DOUBLE> COMMENT 'chooser p per candidate id, when the run kept them',
  decider STRING COMMENT 'jev | student | no_jev',
  tier STRING COMMENT 'corpus-run tier or nightly',
  matcher_version STRING,
  run_at TIMESTAMP
)
CLUSTER BY (raw_affiliation_string)
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.enableRowTracking = true)
COMMENT 'oxjob #1386: new affiliation matcher answers; raw_affiliation_strings_institutions_mv reads these before the legacy model + rules'
"""

LEGACY_BASE = """FILTER(
        CASE
          WHEN asl.institution_ids_override != array() THEN asl.institution_ids_override
          WHEN SIZE(asl.institution_ids) > 0 AND asl.institution_ids[0] IS NULL THEN array()
          ELSE COALESCE(asl.institution_ids, array())
        END,
        x -> x IS NOT NULL AND x != -1)"""


def sql(statement, warehouse, max_wait_s=3 * 3600):
    body = json.dumps({"warehouse_id": warehouse, "statement": statement, "wait_timeout": "50s",
                       "on_wait_timeout": "CONTINUE"})
    out = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--json", body],
                         capture_output=True, text=True)
    try:
        d = json.loads(out.stdout)
    except json.JSONDecodeError:
        sys.exit(f"databricks api failed: {out.stdout[:300]} {out.stderr[:300]}")
    sid = d.get("statement_id")
    t0 = time.time()
    while d.get("status", {}).get("state") not in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
        if time.time() - t0 > max_wait_s:
            sys.exit(f"statement {sid} still running after {max_wait_s} s; cancel with "
                     f"databricks api post /api/2.0/sql/statements/{sid}/cancel")
        time.sleep(10)
        o = subprocess.run(["databricks", "api", "get", f"/api/2.0/sql/statements/{sid}"],
                           capture_output=True, text=True)
        d = json.loads(o.stdout)
    st = d.get("status", {})
    if st.get("state") != "SUCCEEDED":
        sys.exit(f"SQL {st.get('state')} ({sid}): {(st.get('error') or {}).get('message', '')[:600]}")
    cols = [c["name"] for c in d.get("manifest", {}).get("schema", {}).get("columns", [])]
    return cols, d.get("result", {}).get("data_array", []) or []


def show(cols, rows):
    if not rows:
        print("(no rows)")
        return
    w = [max(len(str(c)), *(len(str(r[i])) for r in rows)) for i, c in enumerate(cols)]
    print("  ".join(str(c).ljust(w[i]) for i, c in enumerate(cols)))
    for r in rows:
        print("  ".join(str(v).ljust(w[i]) for i, v in enumerate(r)))


def run(label, statement, wh):
    t = time.time()
    print(f"-- {label} ...", flush=True)
    cols, rows = sql(statement, wh)
    print(f"-- {label}: done in {time.time() - t:.0f} s", flush=True)
    return cols, rows


def mv_select(answers=ANSWERS, without_bot=WITHOUT_BOT):
    """The MV's SELECT from the .sql file, with the answers and bot-free curation tables swapped."""
    text = MV_SQL.read_text()
    body = text[text.index("\nAS\nSELECT") + len("\nAS\n"):]
    for old, new in ((ANSWERS, answers), (WITHOUT_BOT, without_bot)):
        assert body.count(old) == 1, f"{old} expected once in {MV_SQL.name}"
        body = body.replace(old, new)
    return body


def without_bot_merge(target):
    """SyncRasCurations' bot-free MERGE (both statements), pointed at `target`."""
    nb = json.loads(SYNC_NB.read_text())
    cells = ["".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"]
    [cell] = [c for c in cells if f"MERGE INTO {WITHOUT_BOT}" in c]
    cell = cell.replace("%sql\n", "", 1).replace(WITHOUT_BOT, target)
    create, merge = [s.strip() for s in cell.split(";\n") if s.strip()]
    return create, merge


def cmd_standin(a):
    run("stand-in answers (legacy ids for every string with works)", f"""
CREATE OR REPLACE TABLE {STANDIN} CLUSTER BY (raw_affiliation_string) AS
SELECT
  asl.raw_affiliation_string,
  {LEGACY_BASE} AS institution_ids,
  CAST(array() AS ARRAY<STRING>) AS countries,
  CAST(NULL AS MAP<BIGINT, DOUBLE>) AS scores,
  'standin' AS decider,
  'standin' AS tier,
  'legacy-copy' AS matcher_version,
  CURRENT_TIMESTAMP() AS run_at
FROM {LOOKUP} asl
JOIN {WORKS_COUNTS} c ON c.raw_aff_string = asl.raw_affiliation_string AND c.works_count >= 1""", a.warehouse)
    show(*sql(f"SELECT COUNT(*) AS rows, COUNT(DISTINCT raw_affiliation_string) AS strings FROM {STANDIN}", a.warehouse))


def cmd_without_bot(a):
    create, merge = without_bot_merge(SCRATCH_WITHOUT_BOT)
    run("create scratch bot-free curations", create.replace("CREATE TABLE IF NOT EXISTS", "CREATE OR REPLACE TABLE"), a.warehouse)
    run("merge scratch bot-free curations", merge, a.warehouse)
    show(*sql(f"""SELECT COUNT(*) AS strings, SUM(SIZE(curated_add_ids)) AS adds,
                  SUM(SIZE(curated_remove_ids)) AS removes FROM {SCRATCH_WITHOUT_BOT}""", a.warehouse))


def cmd_candidate(a):
    run(f"candidate MV from {a.answers}", f"""
CREATE OR REPLACE TABLE {CANDIDATE} CLUSTER BY (raw_affiliation_string)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported') AS
{mv_select(a.answers, a.without_bot)}""", a.warehouse)
    show(*sql(f"SELECT COUNT(*) AS rows, COUNT(DISTINCT raw_affiliation_string) AS strings, "
              f"COUNT_IF(source = 'matcher') AS matcher_strings FROM {CANDIDATE}", a.warehouse))


def cmd_check_mv_unchanged(a):
    """New definition with an empty answers table must reproduce the live MV on every column."""
    empty = SCRATCH + "empty_answers"
    run("empty answers table", ANSWERS_DDL.replace(ANSWERS, empty).replace("CREATE TABLE IF NOT EXISTS", "CREATE OR REPLACE TABLE"), a.warehouse)
    cols = ["institution_ids", "countries", "source", "model_institution_ids", "institution_ids_override",
            "curated_add_ids", "curated_remove_ids", "created_datetime", "updated_datetime"]
    h = "XXHASH64(" + ", ".join(f"TO_JSON(NAMED_STRUCT('v', {c}))" for c in cols) + ")"
    cols_, rows = run("compare against the live MV", f"""
WITH n AS (SELECT raw_affiliation_string, {h} AS h FROM ({mv_select(empty, SCRATCH_WITHOUT_BOT)})),
     o AS (SELECT raw_affiliation_string, {h} AS h FROM {MV})
SELECT COUNT(*) AS rows,
       COUNT_IF(o.raw_affiliation_string IS NULL) AS only_new,
       COUNT_IF(n.raw_affiliation_string IS NULL) AS only_live,
       COUNT_IF(o.h <> n.h) AS differ
FROM n FULL OUTER JOIN o ON n.raw_affiliation_string = o.raw_affiliation_string""", a.warehouse)
    show(cols_, rows)
    r = dict(zip(cols_, rows[0]))
    if int(r["only_new"]) or int(r["only_live"]) or int(r["differ"]):
        print("NOT identical: rows added since the last MV refresh show up as only_new; anything else is a bug.")
    else:
        print("identical")


def cmd_diff(a):
    wh = a.warehouse
    run("changed strings", f"""
CREATE OR REPLACE TABLE {DIFF_STRINGS} CLUSTER BY (raw_affiliation_string) AS
SELECT o.raw_affiliation_string,
       ARRAY_SORT(o.institution_ids) AS old_ids,
       ARRAY_SORT(n.institution_ids) AS new_ids,
       COALESCE(c.works_count, 0) AS works_count
FROM {MV} o
JOIN {a.candidate} n ON n.raw_affiliation_string = o.raw_affiliation_string
LEFT JOIN {WORKS_COUNTS} c ON c.raw_aff_string = o.raw_affiliation_string
WHERE NOT (ARRAY_SORT(COALESCE(o.institution_ids, array())) <=> ARRAY_SORT(COALESCE(n.institution_ids, array())))""", wh)
    # Seats (work, author) that carry a changed string; old/new = the ids their authorship shows
    # (authorships keep only ids present in the institutions table, CreateWorkAuthorships cell 4).
    run("changed seats", f"""
CREATE OR REPLACE TABLE {DIFF_SEATS} CLUSTER BY (work_id) AS
WITH touched AS (
  SELECT DISTINCT s.work_id, s.author_sequence
  FROM {SEATS} s JOIN {DIFF_STRINGS} d ON d.raw_affiliation_string = s.raw_affiliation_string
),
strs AS (
  SELECT s.work_id, s.author_sequence, s.raw_affiliation_string
  FROM {SEATS} s JOIN touched t ON t.work_id = s.work_id AND t.author_sequence = s.author_sequence
  WHERE s.raw_affiliation_string IS NOT NULL
),
ids AS (
  SELECT st.work_id, st.author_sequence, 'old' AS side, i AS institution_id
  FROM strs st JOIN {MV} o ON o.raw_affiliation_string = st.raw_affiliation_string
  LATERAL VIEW EXPLODE(o.institution_ids) e AS i
  UNION ALL
  SELECT st.work_id, st.author_sequence, 'new' AS side, i AS institution_id
  FROM strs st JOIN {a.candidate} n ON n.raw_affiliation_string = st.raw_affiliation_string
  LATERAL VIEW EXPLODE(n.institution_ids) e AS i
),
known AS (
  SELECT ids.* FROM ids JOIN {INSTITUTIONS} inst ON inst.id = ids.institution_id
)
SELECT t.work_id, t.author_sequence,
       ARRAY_SORT(COALESCE(COLLECT_SET(CASE WHEN k.side = 'old' THEN k.institution_id END), array())) AS old_set,
       ARRAY_SORT(COALESCE(COLLECT_SET(CASE WHEN k.side = 'new' THEN k.institution_id END), array())) AS new_set
FROM touched t LEFT JOIN known k ON k.work_id = t.work_id AND k.author_sequence = t.author_sequence
GROUP BY t.work_id, t.author_sequence""", wh)

    print("\n== Strings whose ids change, by works tier")
    show(*sql(f"""
SELECT CASE WHEN works_count >= 100 THEN '>=100' WHEN works_count >= 10 THEN '10-99'
            WHEN works_count >= 2 THEN '2-9' WHEN works_count = 1 THEN '1' ELSE '0' END AS tier,
       COUNT(*) AS strings_changed,
       COUNT_IF(SIZE(old_ids) > 0 AND SIZE(new_ids) = 0) AS to_none,
       COUNT_IF(SIZE(old_ids) = 0 AND SIZE(new_ids) > 0) AS from_none,
       SUM(works_count) AS affiliations
FROM {DIFF_STRINGS} GROUP BY 1 ORDER BY MIN(works_count) DESC""", wh))

    print("\n== Works (what Guardrails check 1 counts; check 4 counts the net loss of works with any institution)")
    show(*sql(f"""
WITH w AS (
  SELECT work_id,
         MAX(CASE WHEN NOT (old_set <=> new_set) THEN 1 ELSE 0 END) AS changed,
         MAX(SIZE(old_set)) AS old_any, MAX(SIZE(new_set)) AS new_any
  FROM {DIFF_SEATS} GROUP BY work_id
)
SELECT COUNT_IF(changed = 1) AS works_changed,
       COUNT_IF(changed = 1 AND old_any > 0 AND new_any = 0) AS touched_works_losing_all_seats_ids,
       COUNT_IF(changed = 1 AND old_any = 0 AND new_any > 0) AS touched_works_gaining_first_ids
FROM w""", wh))
    print("(losing/gaining are over the touched seats only: a work whose other authors keep ids still has institutions.)")

    print("\n== Works with any institution: live vs candidate (the Guardrails check 4 quantity)")
    show(*sql(f"""
WITH w AS (
  SELECT d.work_id,
         MAX(SIZE(d.old_set)) AS touched_old, MAX(SIZE(d.new_set)) AS touched_new
  FROM {DIFF_SEATS} d GROUP BY d.work_id
),
other AS (
  -- does the work have ids on a seat the swap does not touch?
  SELECT DISTINCT s.work_id
  FROM {SEATS} s
  JOIN w ON w.work_id = s.work_id
  LEFT ANTI JOIN {DIFF_SEATS} d ON d.work_id = s.work_id AND d.author_sequence = s.author_sequence
  JOIN {MV} o ON o.raw_affiliation_string = s.raw_affiliation_string
  WHERE EXISTS(o.institution_ids, i -> i IS NOT NULL)
)
SELECT COUNT_IF(touched_old > 0 AND touched_new = 0 AND other.work_id IS NULL) AS works_losing_all_institutions,
       COUNT_IF(touched_old = 0 AND touched_new > 0 AND other.work_id IS NULL) AS works_gaining_first_institution
FROM w LEFT JOIN other ON other.work_id = w.work_id""", wh))

    print(f"\n== Top {a.top} institutions by works lost and gained")
    base = f"""
WITH e AS (
  SELECT work_id, i AS institution_id, 'old' AS side FROM {DIFF_SEATS} LATERAL VIEW EXPLODE(old_set) x AS i
  UNION ALL
  SELECT work_id, i, 'new' FROM {DIFF_SEATS} LATERAL VIEW EXPLODE(new_set) x AS i
),
per AS (
  SELECT institution_id, work_id,
         MAX(CASE WHEN side = 'old' THEN 1 ELSE 0 END) AS had, MAX(CASE WHEN side = 'new' THEN 1 ELSE 0 END) AS has
  FROM e GROUP BY institution_id, work_id
),
net AS (
  SELECT institution_id, COUNT_IF(had = 1 AND has = 0) AS works_lost, COUNT_IF(had = 0 AND has = 1) AS works_gained
  FROM per GROUP BY institution_id
)
SELECT net.institution_id, inst.display_name, inst.iso3166_code AS country,
       net.works_lost, net.works_gained, net.works_gained - net.works_lost AS net
FROM net LEFT JOIN {INSTITUTIONS} inst ON inst.id = net.institution_id"""
    print("-- losing most")
    show(*sql(base + f"\nORDER BY net ASC LIMIT {a.top}", wh))
    print("-- gaining most")
    show(*sql(base + f"\nORDER BY net DESC LIMIT {a.top}", wh))


def cmd_rehearse_mv(a):
    """Build the new definition as a real scratch MV (reads --answers and the scratch bot-free curations): does it
    compile as an MV, how long is a full build, is the next refresh incremental."""
    name = SCRATCH + "mv_rehearsal"
    run(f"CREATE MV {name}", f"""
CREATE OR REPLACE MATERIALIZED VIEW {name}
CLUSTER BY (raw_affiliation_string)
AS
{mv_select(a.answers, a.without_bot)}""", a.warehouse)
    run(f"REFRESH MV {name}", f"REFRESH MATERIALIZED VIEW {name}", a.warehouse)
    show(*sql(f"DESCRIBE TABLE EXTENDED {name}", a.warehouse))


def cmd_bot_agreement(a):
    """Of the curation bot's winning adds and removes, how many does the candidate agree with?"""
    show(*sql(f"""
WITH bot AS (
  SELECT entity_id AS s, CAST(REGEXP_REPLACE(value, '^https?://openalex\\\\.org/I', '') AS BIGINT) AS i,
         MAX_BY(action, STRUCT(created, id)) AS action,
         MAX_BY(user_id = 'user-5UKz4XUnsuZY', STRUCT(created, id)) AS bot_wins
  FROM openalex_users.public.curations
  WHERE entity = 'ras' AND property = 'institution_ids' AND action IN ('add', 'remove')
    AND value RLIKE '^https?://openalex\\\\.org/I[0-9]+$'
  GROUP BY entity_id, value
)
SELECT b.action, COUNT(*) AS bot_pairs,
       COUNT_IF(ARRAY_CONTAINS(n.institution_ids, b.i)) AS candidate_has_id,
       COUNT_IF(n.source = 'matcher') AS answered_by_matcher
FROM bot b JOIN {a.candidate} n ON n.raw_affiliation_string = b.s
WHERE b.bot_wins
GROUP BY b.action""", a.warehouse))


def cmd_create_answers_table(a):
    run(f"create {ANSWERS}", ANSWERS_DDL, a.warehouse)


def cmd_define_mv(a):
    for t in (ANSWERS, WITHOUT_BOT):  # the definition joins both; SyncRasCurations creates the second
        sql(f"DESCRIBE TABLE {t}", a.warehouse)
    run(f"CREATE OR REPLACE {MV}", MV_SQL.read_text(), a.warehouse)


def cmd_load(a):
    wh = a.warehouse
    cols, rows = sql(f"""SELECT COUNT(*) AS rows, COUNT(DISTINCT raw_affiliation_string) AS strings,
                         COUNT_IF(raw_affiliation_string IS NULL OR institution_ids IS NULL) AS nulls FROM {a.source}""", wh)
    show(cols, rows)
    r = dict(zip(cols, rows[0]))
    if r["rows"] != r["strings"] or int(r["nulls"]):
        sys.exit("refusing: the source must have one row per string and no NULL keys or ids")
    src_cols = {row[0] for row in sql(f"DESCRIBE TABLE {a.source}", wh)[1]}
    def col(name, default):
        return name if name in src_cols else f"{default} AS {name}"
    run(f"INSERT OVERWRITE {ANSWERS} FROM {a.source}", f"""
INSERT OVERWRITE {ANSWERS}
SELECT raw_affiliation_string,
       institution_ids,
       {col('countries', 'CAST(array() AS ARRAY<STRING>)')},
       {col('scores', 'CAST(NULL AS MAP<BIGINT, DOUBLE>)')},
       {col('decider', 'CAST(NULL AS STRING)')},
       {col('tier', 'CAST(NULL AS STRING)')},
       {col('matcher_version', repr(a.matcher_version))},
       {col('run_at', 'CURRENT_TIMESTAMP()')}
FROM {a.source}""", wh)
    show(*sql(f"SELECT COUNT(*) AS rows, COUNT_IF(SIZE(institution_ids) = 0) AS names_none FROM {ANSWERS}", wh))


def cmd_revert(a):
    run(f"TRUNCATE {ANSWERS}", f"TRUNCATE TABLE {ANSWERS}", a.warehouse)
    print(f"Next refresh of {MV} returns the legacy ids for every string.")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--warehouse", default=DEFAULT_WAREHOUSE)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("standin").set_defaults(f=cmd_standin)
    sub.add_parser("without-bot").set_defaults(f=cmd_without_bot)
    p = sub.add_parser("candidate")
    p.add_argument("--answers", default=STANDIN)
    p.add_argument("--without-bot", default=SCRATCH_WITHOUT_BOT)
    p.set_defaults(f=cmd_candidate)
    p = sub.add_parser("diff")
    p.add_argument("--candidate", default=CANDIDATE)
    p.add_argument("--top", type=int, default=50)
    p.set_defaults(f=cmd_diff)
    sub.add_parser("check-mv-unchanged").set_defaults(f=cmd_check_mv_unchanged)
    p = sub.add_parser("rehearse-mv")
    p.add_argument("--answers", default=STANDIN)
    p.add_argument("--without-bot", default=SCRATCH_WITHOUT_BOT)
    p.set_defaults(f=cmd_rehearse_mv)
    p = sub.add_parser("bot-agreement")
    p.add_argument("--candidate", default=CANDIDATE)
    p.set_defaults(f=cmd_bot_agreement)
    sub.add_parser("create-answers-table").set_defaults(f=cmd_create_answers_table)
    sub.add_parser("define-mv").set_defaults(f=cmd_define_mv)
    p = sub.add_parser("load")
    p.add_argument("--from", dest="source", required=True)
    p.add_argument("--matcher-version", default="v1")
    p.set_defaults(f=cmd_load)
    sub.add_parser("revert").set_defaults(f=cmd_revert)
    a = ap.parse_args()
    a.f(a)


if __name__ == "__main__":
    main()
