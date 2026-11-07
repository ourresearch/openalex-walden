# Databricks notebook source
# MAGIC %md
# MAGIC # Topics nightly 3/3: append the new works to work_topics_v2, update the re-scored ones (oxjobs #1531, #2085)
# MAGIC
# MAGIC Refuses (appends nothing, fails the task) when this build's ledger rows in `<state_prefix>raw` do not equal the queue, when the
# MAGIC not-classifiable share among queued works WITH an abstract is over `max_nc_share` (0.30; 0.2-6.1% over the 15 builds to 19 Oct),
# MAGIC when the share over ALL queued works is over `max_nc_share_all` (0.70), or when any topic id is not in `openalex.common.topics`.
# MAGIC The abstract share is the model-health signal. The all-works share swings 3-37% with how many title-only records the queue holds
# MAGIC (19 Oct: 16,159 new PNNL EMSL DataCite records, "Data for EMSL Project N from <month>", rightly not-classifiable, took it to
# MAGIC 36.5% and failed the End 2 End while the abstract share was 1.0%); its ceiling still catches a model that fails on everything.
# MAGIC Then builds the served rows with the PLAN § 2b builder (keep identical to it: top 3 after dropping the not-classifiable class,
# MAGIC score = ROUND(prob, 4) as FLOAT, ids / names from the vocabulary tables, `[]` for not-classifiable works) into
# MAGIC `<state_prefix>append_rows`, checks them (one row per work, the served struct type, sizes, nothing unresolved, no row already
# MAGIC served except the queue's re-scores) and MERGEs them into `target`: new work_ids are inserted; re-scored works (`rescore`, text
# MAGIC changed since scoring, oxjob #2085) get their row updated (topics, flags, `text_key`, `updated_datetime`; `created_datetime`
# MAGIC kept). Every row a re-score replaces is first copied into `<state_prefix>replaced` with the build id, so a build can be undone
# MAGIC (`MERGE ... USING (SELECT * FROM <state_prefix>replaced WHERE build_id = ...)`). No other existing row is ever touched.
# MAGIC
# MAGIC Catalogue records get no topic: a queued work whose `primary_location.source.id` (read from `works_table`) is in
# MAGIC `catalogue_sources_table` (source_id = full 'https://openalex.org/S…' id) and whose `type` is 'dataset' or 'other' is served with
# MAGIC `topics = []` and `catalogue_record = true`; `not_classifiable` stays the model's own flag and the ledger keeps the raw scores, so the
# MAGIC rule can be reversed. An empty `catalogue_sources_table` disables the rule (every row `catalogue_record = false`): the default,
# MAGIC by Jason's decision 2026-10-04 ("tag it all").
# MAGIC One line per finished build goes into `<state_prefix>runs`. A repair after a successful MERGE finds every row already served
# MAGIC with the same topics; the MERGE is idempotent. `CreateWorksEnriched` merges work_topics_v2 into `openalex_works` the same night.

# COMMAND ----------

import datetime as dt
import json

for name, default in [("state_prefix", "openalex.works.work_topics_v2_"), ("target", "openalex.works.work_topics_v2"), ("max_nc_share", "0.30"),
                      ("max_nc_share_all", "0.70"), ("works_table", "openalex.works.openalex_works_base"),
                      ("catalogue_sources_table", ""), ("dry_run", "false")]:
    dbutils.widgets.text(name, default)
P = dbutils.widgets.get("state_prefix").strip()
TARGET = dbutils.widgets.get("target").strip()
MAX_NC = float(dbutils.widgets.get("max_nc_share"))            # among queued works with an abstract
MAX_NC_ALL = float(dbutils.widgets.get("max_nc_share_all"))    # among all queued works
DRY = dbutils.widgets.get("dry_run").strip().lower() == "true"
WORKS = dbutils.widgets.get("works_table").strip()
CAT = dbutils.widgets.get("catalogue_sources_table").strip()
for t in (P + "x", TARGET, WORKS) + ((CAT,) if CAT else ()):
    assert t.startswith("openalex.") and t.replace(".", "").replace("_", "").isalnum(), f"bad table name {t!r}"
QUEUE, RAW, ROWS, RUNS, REPLACED = f"{P}queue", f"{P}raw", f"{P}append_rows", f"{P}runs", f"{P}replaced"
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
if "rescored" not in {c.name for c in spark.table(RUNS).schema}:
    spark.sql(f"ALTER TABLE {RUNS} ADD COLUMNS (rescored BIGINT COMMENT 'rows updated because the work text changed (oxjob #2085)')")
if "text_key" not in {c.name for c in spark.table(TARGET).schema}:
    spark.sql(f"ALTER TABLE {TARGET} ADD COLUMNS (text_key BIGINT COMMENT 'xxhash64(title, abstract[:4000]) the row was scored on (oxjob #2085)')")
RUN_COLS = "build_id, queued, scored, appended, not_classifiable, catalogue_records, usd_est, modal_wall_s, target, finished_at, rescored"
q = one(f"SELECT count(*) AS n, max(build_id) AS b FROM {QUEUE}")
n, build_id = int(q.n), q.b
if n == 0:
    if not DRY:
        spark.sql(f"INSERT INTO {RUNS} ({RUN_COLS}) VALUES (NULL, 0, 0, 0, 0, 0, 0, 0, '{TARGET}', current_timestamp(), 0)")
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
# The model-health share: queued works that carry an abstract (title-only records are often rightly not classifiable).
a = one(f"""SELECT count_if(w.has_abs) AS n_abs, count_if(w.has_abs AND r.not_classifiable) AS nc_abs
  FROM {RAW} r LEFT JOIN (SELECT id, max(abstract_inverted_index IS NOT NULL) AS has_abs FROM {WORKS}
                          WHERE id IN (SELECT work_id FROM {RAW} WHERE build_id = '{build_id}') GROUP BY id) w ON w.id = r.work_id
  WHERE r.build_id = '{build_id}'""")
log(f"gate 1 abstract share: {a.asDict()}")
bad = []
if not (g.rows == g.ids == n and g.missing == 0 and g.extra == 0):
    bad.append(f"ledger rows {g.rows:,} / ids {g.ids:,} / missing {g.missing:,} / extra {g.extra:,} != queue {n:,}")
if a.n_abs and a.nc_abs / a.n_abs > MAX_NC:
    bad.append(f"not-classifiable share among {a.n_abs:,} works with an abstract {a.nc_abs / a.n_abs:.3f} > {MAX_NC}")
if g.rows and g.nc / g.rows > MAX_NC_ALL:
    bad.append(f"not-classifiable share over all works {g.nc / g.rows:.3f} > {MAX_NC_ALL}")
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
# A queue built before oxjob #2085 has no text_key / rescore: its works are all new and get a NULL key (never re-queued).
qcols = {c.name for c in spark.table(QUEUE).schema}
queue_cols = (f"SELECT work_id, text_key, rescore FROM {QUEUE}" if {"text_key", "rescore"} <= qcols
              else f"SELECT work_id, CAST(NULL AS BIGINT) AS text_key, false AS rescore FROM {QUEUE}")
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
cat AS ({cat_cte}),
q AS ({queue_cols})
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
  '{SOURCE}' AS source, current_timestamp() AS created_datetime, current_timestamp() AS updated_datetime,
  q.text_key, COALESCE(q.rescore, false) AS rescore
FROM sc CROSS JOIN tm LEFT JOIN cat ON cat.work_id = sc.work_id LEFT JOIN q ON q.work_id = sc.work_id
""")

# Gate 2: the staged rows.
c = one(f"""SELECT count(*) AS n, count(DISTINCT work_id) AS ids,
    count_if(work_id IS NULL OR topics IS NULL OR not_classifiable IS NULL OR catalogue_record IS NULL) AS nulls,
    count_if(not_classifiable) AS nc, count_if(catalogue_record) AS catalogue, count_if(catalogue_record AND NOT not_classifiable) AS catalogue_classified,
    count_if(NOT not_classifiable AND NOT catalogue_record AND size(topics) <> 3) AS bad_size,
    count_if((not_classifiable OR catalogue_record) AND size(topics) <> 0) AS bad_nc,
    count_if(exists(topics, t -> t.display_name IS NULL OR t.subfield.id IS NULL OR t.field.id IS NULL OR t.domain.id IS NULL OR t.score IS NULL)) AS unresolved,
    (SELECT count(*) FROM {ROWS} r JOIN {TARGET} v ON v.work_id = r.work_id WHERE NOT r.rescore) AS already_in_target,
    count_if(rescore) AS rescore,
    (SELECT count(*) FROM {ROWS} r LEFT ANTI JOIN {TARGET} v ON v.work_id = r.work_id WHERE r.rescore) AS rescore_not_served,
    max(typeof(topics)) AS typ FROM {ROWS}""")
log(f"gate 2: {c.asDict()}")
if DRY:
    dbutils.notebook.exit("dry run: staged rows checked, nothing appended")
bad = []
if not (c.n == c.ids == n and c.nulls == 0): bad.append(f"staged rows {c.n:,} / ids {c.ids:,} / nulls {c.nulls:,} != queue {n:,}")
if c.bad_size or c.bad_nc or c.unresolved: bad.append(f"bad_size {c.bad_size:,} bad_nc {c.bad_nc:,} unresolved {c.unresolved:,}")
if c.typ != SERVED_TYPE: bad.append(f"topics type {c.typ} != served {SERVED_TYPE}")
n_new = c.n - c.rescore
if c.already_in_target and c.already_in_target != n_new: bad.append(f"{c.already_in_target:,} of {n_new:,} new works already in {TARGET} (another writer?)")
if c.rescore_not_served: bad.append(f"{c.rescore_not_served:,} re-scored works have no row in {TARGET} (deleted meanwhile?)")
if bad:
    raise RuntimeError("refusing to append, NOTHING appended: " + "; ".join(bad))

# COMMAND ----------

repair = bool(n_new) and c.already_in_target == n_new
if repair:
    # a repair of this task after its MERGE went through: accept only if the new works' served rows are these rows
    same = one(f"SELECT count(*) AS n FROM {ROWS} r JOIN {TARGET} v ON v.work_id = r.work_id AND to_json(v.topics) = to_json(r.topics) WHERE NOT r.rescore").n
    if same != n_new:
        raise RuntimeError(f"all {n_new:,} new work_ids are already in {TARGET} but only {same:,} with these topics; NOTHING written")
    log(f"all {n_new:,} new rows of build {build_id} are already in {TARGET} (an earlier attempt wrote them); the MERGE below is a no-op for them")

# Keep every row a re-score replaces (once per build: a repair finds them saved), then one MERGE: update re-scores, insert new works.
spark.sql(f"""CREATE TABLE IF NOT EXISTS {REPLACED} (build_id STRING, replaced_at TIMESTAMP, work_id BIGINT, topics {TOPICS_TYPE},
  not_classifiable BOOLEAN, catalogue_record BOOLEAN, source STRING, created_datetime TIMESTAMP, updated_datetime TIMESTAMP, text_key BIGINT)
  COMMENT 'Topics nightly (oxjob #2085): served rows replaced by a re-score, one per (build, work); restore a build by MERGE from here'""")
spark.sql(f"""INSERT INTO {REPLACED}
SELECT '{build_id}', current_timestamp(), v.work_id, v.topics, v.not_classifiable, v.catalogue_record, v.source, v.created_datetime,
       v.updated_datetime, v.text_key
FROM {TARGET} v JOIN (SELECT work_id FROM {ROWS} WHERE rescore) r ON r.work_id = v.work_id
LEFT ANTI JOIN (SELECT work_id FROM {REPLACED} WHERE build_id = '{build_id}') d ON d.work_id = v.work_id""")
saved = one(f"SELECT count(*) AS n FROM {REPLACED} WHERE build_id = '{build_id}'").n
if saved != c.rescore:
    raise RuntimeError(f"{REPLACED} holds {saved:,} rows for build {build_id}, expected {c.rescore:,}; NOTHING written")
before = spark.table(TARGET).count()
spark.sql(f"""MERGE INTO {TARGET} v USING {ROWS} r ON v.work_id = r.work_id
WHEN MATCHED THEN UPDATE SET v.topics = r.topics, v.not_classifiable = r.not_classifiable, v.catalogue_record = r.catalogue_record,
  v.source = r.source, v.text_key = r.text_key, v.updated_datetime = r.updated_datetime
WHEN NOT MATCHED THEN INSERT (work_id, topics, not_classifiable, catalogue_record, source, created_datetime, updated_datetime, text_key)
  VALUES (r.work_id, r.topics, r.not_classifiable, r.catalogue_record, r.source, r.created_datetime, r.updated_datetime, r.text_key)""")
after = spark.table(TARGET).count()
appended = after - before
served = one(f"""SELECT count(*) AS n FROM {ROWS} r JOIN {TARGET} v ON v.work_id = r.work_id
  AND to_json(v.topics) = to_json(r.topics) AND v.text_key <=> r.text_key""").n
log(f"merged build {build_id} into {TARGET}: {appended:,} new rows ({before:,} -> {after:,}), {c.rescore:,} re-scored rows updated; "
    f"{served:,} of {c.n:,} rows now served as staged")
if appended != (0 if repair else n_new) or served != c.n:
    raise RuntimeError(f"expected {n_new:,} new rows and {c.n:,} served as staged; got {appended:,} / {served:,} (another writer?)")

spark.sql(f"""INSERT INTO {RUNS} ({RUN_COLS}) VALUES ('{build_id}', {n}, {g.rows}, {appended}, {c.nc}, {c.catalogue},
  {float(task_value('topics_score_modal', 'usd_est', 0.0))}, {float(task_value('topics_score_modal', 'modal_wall_s', 0.0))}, '{TARGET}',
  current_timestamp(), {c.rescore})""")
log(f"build {build_id}: {n:,} queued, {appended:,} appended, {c.rescore:,} re-scored ({c.nc:,} not classifiable, {c.nc / n:.2%}; {c.catalogue:,} catalogue records "
    f"served with no topic, {c.catalogue_classified:,} of them classified by the model)")
print(json.dumps({"build_id": build_id, "queued": n, "appended": appended, "rescored": c.rescore, "nc": c.nc, "catalogue_records": c.catalogue}))
