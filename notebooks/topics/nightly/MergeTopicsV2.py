# Databricks notebook source
# MAGIC %md
# MAGIC # Topics nightly 3/3: append the new works to work_topics_v2 (oxjob #1531)
# MAGIC
# MAGIC Refuses (appends nothing, fails the task) when this build's ledger rows in `<state_prefix>raw` do not equal the queue, when the
# MAGIC not-classifiable share is over `max_nc_share` (0.30; the corpus is 9.6%), or when any topic id is not in `openalex.common.topics`.
# MAGIC Then builds the served rows with the PLAN § 2b builder (keep identical to it: top 3 after dropping the not-classifiable class,
# MAGIC score = ROUND(prob, 4) as FLOAT, ids / names from the vocabulary tables, `[]` for not-classifiable works) into
# MAGIC `<state_prefix>append_rows`, checks them (one row per work, the served struct type, sizes, nothing unresolved, none already
# MAGIC served) and INSERTs the new work_ids only into `target` (append-only, the #1312 rule; existing rows are never touched).
# MAGIC
# MAGIC Catalogue records get no topic: a queued work whose `primary_location.source.id` (read from `works_table`) is in
# MAGIC `catalogue_sources_table` (source_id = full 'https://openalex.org/S…' id) and whose `type` is 'dataset' or 'other' is served with
# MAGIC `topics = []` and `catalogue_record = true`; `not_classifiable` stays the model's own flag and the ledger keeps the raw scores, so the
# MAGIC rule can be reversed. An empty `catalogue_sources_table` disables the rule (every row `catalogue_record = false`).
# MAGIC One line per finished build goes into `<state_prefix>runs`. A repair after a successful INSERT finds every row already served
# MAGIC with the same topics and only records the run. `CreateWorksEnriched` merges the new rows into `openalex_works` the same night.

# COMMAND ----------

import datetime as dt
import json

for name, default in [("state_prefix", "openalex.works.work_topics_v2_"), ("target", "openalex.works.work_topics_v2"), ("max_nc_share", "0.30"),
                      ("works_table", "openalex.works.openalex_works_base"),
                      ("catalogue_sources_table", "openalex.works.work_topics_v2_catalogue_sources"), ("dry_run", "false")]:
    dbutils.widgets.text(name, default)
P = dbutils.widgets.get("state_prefix").strip()
TARGET = dbutils.widgets.get("target").strip()
MAX_NC = float(dbutils.widgets.get("max_nc_share"))
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
WORKS = dbutils.widgets.get("works_table").strip()
CAT = dbutils.widgets.get("catalogue_sources_table").strip()
for t in (P + "x", TARGET, WORKS) + ((CAT,) if CAT else ()):
    assert t.startswith("openalex.") and t.replace(".", "").replace("_", "").isalnum(), f"bad table name {t!r}"
QUEUE, RAW, ROWS, RUNS = f"{P}queue", f"{P}raw", f"{P}append_rows", f"{P}runs"
SOURCE = "q8b_2m"
TOPICS_TYPE = ("ARRAY<STRUCT<id: STRING, display_name: STRING, score: FLOAT, subfield: STRUCT<id: STRING, display_name: STRING>, "
               "field: STRUCT<id: STRING, display_name: STRING>, domain: STRUCT<id: STRING, display_name: STRING>>>")
SERVED_TYPE = ("array<struct<id:string,display_name:string,score:float,subfield:struct<id:string,display_name:string>,"
               "field:struct<id:string,display_name:string>,domain:struct<id:string,display_name:string>>>")   # = typeof(openalex_works.topics)


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


def task_value(task, key, default):
    try:
        return dbutils.jobs.taskValues.get(taskKey=task, key=key, default=default, debugValue=default)
    except Exception:
        return default


def one(sql):
    return spark.sql(sql).collect()[0]

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {RUNS} (build_id STRING, queued BIGINT, scored BIGINT, appended BIGINT, not_classifiable BIGINT,
  catalogue_records BIGINT, usd_est DOUBLE, modal_wall_s DOUBLE, target STRING, finished_at TIMESTAMP)
  COMMENT 'Topics nightly (oxjob #1531): one row per finished build'""")
q = one(f"SELECT count(*) AS n, max(build_id) AS b FROM {QUEUE}")
n, build_id = int(q.n), q.b
if n == 0:
    if not DRY:
        spark.sql(f"INSERT INTO {RUNS} VALUES (NULL, 0, 0, 0, 0, 0, 0, 0, '{TARGET}', current_timestamp())")
    dbutils.notebook.exit("empty queue: nothing to append")

# Gate 1: the ledger holds exactly this build's queue, the NC share is sane, every topic id resolves.
g = one(f"""SELECT count(*) AS rows, count(DISTINCT r.work_id) AS ids,
    (SELECT count(*) FROM {QUEUE} q LEFT ANTI JOIN (SELECT work_id FROM {RAW} WHERE build_id = '{build_id}') r ON r.work_id = q.work_id) AS missing,
    count_if(q.work_id IS NULL) AS extra,
    count_if(r.not_classifiable) AS nc,
    count_if(exists(r.topic_ids, x -> x IS NULL OR (x <> -1 AND NOT array_contains(t.ids, x)))) AS unresolved
  FROM {RAW} r LEFT JOIN {QUEUE} q ON q.work_id = r.work_id
  CROSS JOIN (SELECT collect_set(CAST(topic_id AS INT)) AS ids FROM openalex.common.topics) t
  WHERE r.build_id = '{build_id}'""")
log(f"gate 1 for build {build_id}: {g.asDict()} (queue {n:,})")
bad = []
if not (g.rows == g.ids == n and g.missing == 0 and g.extra == 0):
    bad.append(f"ledger rows {g.rows:,} / ids {g.ids:,} / missing {g.missing:,} / extra {g.extra:,} != queue {n:,}")
if g.rows and g.nc / g.rows > MAX_NC:
    bad.append(f"not-classifiable share {g.nc / g.rows:.3f} > {MAX_NC}")
if g.unresolved:
    bad.append(f"{g.unresolved:,} works carry a topic id not in openalex.common.topics")
if bad:
    raise RuntimeError("refusing to append, NOTHING appended: " + "; ".join(bad))

# COMMAND ----------

# The catalogue-record rule's input: queued works from a catalogue source with type dataset / other.
if CAT:
    cols = {c.name for c in spark.table(CAT).schema} if spark.catalog.tableExists(CAT) else set()
    if not {"source_id", "display_name", "note"} <= cols:
        raise RuntimeError(f"{CAT} missing or without source_id / display_name / note; pass catalogue_sources_table='' to disable the rule")
    n_cat_sources = spark.table(CAT).count()
    cat_cte = f"""SELECT DISTINCT w.id AS work_id FROM {WORKS} w
      JOIN {CAT} c ON c.source_id = w.primary_location.source.id
      WHERE w.type IN ('dataset', 'other') AND w.id IN (SELECT work_id FROM {QUEUE})"""
    log(f"catalogue rule ON: {n_cat_sources} sources in {CAT}")
else:
    cat_cte = "SELECT CAST(NULL AS BIGINT) AS work_id WHERE false"
    log("catalogue rule OFF (catalogue_sources_table is empty)")

# The PLAN § 2b builder over this build's ledger rows (keep identical to it), plus the catalogue-record rule.
spark.sql(f"""
CREATE OR REPLACE TABLE {ROWS} COMMENT 'Topics nightly (oxjob #1531): served rows of the last build, staged and checked before the append' AS
WITH tm AS (   -- 4,516 rows -> one MAP literal, so the build is a narrow per-row transform (no shuffle)
  SELECT map_from_entries(collect_list(struct(t.topic_id, named_struct(
      'display_name', t.display_name,
      'subfield', named_struct('id', concat('https://openalex.org/subfields/', s.subfield_id), 'display_name', s.display_name),
      'field',    named_struct('id', concat('https://openalex.org/fields/', f.field_id), 'display_name', f.display_name),
      'domain',   named_struct('id', concat('https://openalex.org/domains/', d.domain_id), 'display_name', d.display_name))))) AS m
  FROM openalex.common.topics t
  JOIN openalex.common.subfields s USING (subfield_id)
  JOIN openalex.common.fields f USING (field_id)
  JOIN openalex.common.domains d USING (domain_id)
),
sc AS (
  SELECT * FROM {RAW} WHERE build_id = '{build_id}'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY work_id ORDER BY shard DESC) = 1
),
cat AS ({cat_cte})
SELECT sc.work_id,
  CASE WHEN sc.not_classifiable OR cat.work_id IS NOT NULL
    THEN CAST(ARRAY() AS {TOPICS_TYPE})
    ELSE TRANSFORM(
      slice(filter(arrays_zip(sc.topic_ids, sc.probs), x -> x.topic_ids <> -1), 1, 3),   -- skip the not-classifiable class
      x -> named_struct('id', concat('https://openalex.org/T', x.topic_ids),
                        'display_name', tm.m[x.topic_ids].display_name,
                        'score', CAST(ROUND(x.probs, 4) AS FLOAT),
                        'subfield', tm.m[x.topic_ids].subfield,
                        'field', tm.m[x.topic_ids].field,
                        'domain', tm.m[x.topic_ids].domain))
  END AS topics,
  sc.not_classifiable, cat.work_id IS NOT NULL AS catalogue_record,
  '{SOURCE}' AS source, current_timestamp() AS created_datetime, current_timestamp() AS updated_datetime
FROM sc CROSS JOIN tm LEFT JOIN cat ON cat.work_id = sc.work_id
""")

# Gate 2: the staged rows.
c = one(f"""SELECT count(*) AS n, count(DISTINCT work_id) AS ids,
    count_if(work_id IS NULL OR topics IS NULL OR not_classifiable IS NULL OR catalogue_record IS NULL) AS nulls,
    count_if(not_classifiable) AS nc, count_if(catalogue_record) AS catalogue, count_if(catalogue_record AND NOT not_classifiable) AS catalogue_classified,
    count_if(NOT not_classifiable AND NOT catalogue_record AND size(topics) <> 3) AS bad_size,
    count_if((not_classifiable OR catalogue_record) AND size(topics) <> 0) AS bad_nc,
    count_if(exists(topics, t -> t.display_name IS NULL OR t.subfield.id IS NULL OR t.field.id IS NULL OR t.domain.id IS NULL OR t.score IS NULL)) AS unresolved,
    (SELECT count(*) FROM {ROWS} r JOIN {TARGET} v ON v.work_id = r.work_id) AS already_in_target,
    max(typeof(topics)) AS typ FROM {ROWS}""")
log(f"gate 2: {c.asDict()}")
if DRY:
    dbutils.notebook.exit("dry run: staged rows checked, nothing appended")
bad = []
if not (c.n == c.ids == n and c.nulls == 0): bad.append(f"staged rows {c.n:,} / ids {c.ids:,} / nulls {c.nulls:,} != queue {n:,}")
if c.bad_size or c.bad_nc or c.unresolved: bad.append(f"bad_size {c.bad_size:,} bad_nc {c.bad_nc:,} unresolved {c.unresolved:,}")
if c.typ != SERVED_TYPE: bad.append(f"topics type {c.typ} != served {SERVED_TYPE}")
if c.already_in_target and c.already_in_target != c.n: bad.append(f"{c.already_in_target:,} of {c.n:,} works already in {TARGET} (another writer?)")
if bad:
    raise RuntimeError("refusing to append, NOTHING appended: " + "; ".join(bad))

# COMMAND ----------

if c.already_in_target == c.n:
    # a repair of this task after its INSERT went through: accept only if the served rows are these rows
    same = one(f"SELECT count(*) AS n FROM {ROWS} r JOIN {TARGET} v ON v.work_id = r.work_id AND to_json(v.topics) = to_json(r.topics)").n
    if same != c.n:
        raise RuntimeError(f"all {c.n:,} work_ids are already in {TARGET} but only {same:,} with these topics; NOTHING appended")
    log(f"all {c.n:,} rows of build {build_id} are already in {TARGET} (an earlier attempt appended them); recording the run only")
    appended = 0
else:
    before = spark.table(TARGET).count()
    spark.sql(f"""INSERT INTO {TARGET} (work_id, topics, not_classifiable, catalogue_record, source, created_datetime, updated_datetime)
    SELECT r.work_id, r.topics, r.not_classifiable, r.catalogue_record, r.source, r.created_datetime, r.updated_datetime
    FROM {ROWS} r LEFT ANTI JOIN {TARGET} v ON v.work_id = r.work_id""")
    after = spark.table(TARGET).count()
    appended = after - before
    log(f"appended {appended:,} rows to {TARGET} ({before:,} -> {after:,})")
    if appended != c.n:
        raise RuntimeError(f"expected {c.n:,} new rows in {TARGET}, got {appended:,} (another writer?)")

spark.sql(f"""INSERT INTO {RUNS} VALUES ('{build_id}', {n}, {g.rows}, {appended}, {c.nc}, {c.catalogue},
  {float(task_value('topics_score_modal', 'usd_est', 0.0))}, {float(task_value('topics_score_modal', 'modal_wall_s', 0.0))}, '{TARGET}', current_timestamp())""")
log(f"build {build_id}: {n:,} queued, {appended:,} appended ({c.nc:,} not classifiable, {c.nc / n:.2%}; {c.catalogue:,} catalogue records "
    f"served with no topic, {c.catalogue_classified:,} of them classified by the model)")
print(json.dumps({"build_id": build_id, "queued": n, "appended": appended, "nc": c.nc, "catalogue_records": c.catalogue}))
