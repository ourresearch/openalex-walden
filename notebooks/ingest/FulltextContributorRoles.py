# Databricks notebook source
# MAGIC %md
# MAGIC # CRediT roles from full-text contribution statements (oxjob #2450)
# MAGIC
# MAGIC Many papers print an "Author contributions" statement written in CRediT's own terms ("Conceptualization, J.Z.
# MAGIC and Y.H.; Software, Y.H." or "Jialin Zhou: Conceptualization, Investigation"). This notebook reads those
# MAGIC statements out of the full text OpenAlex holds (`openalex.works.work_fulltext`) and keeps each author's roles.
# MAGIC Only CRediT's 14 terms count; free-form sentences and roles outside CRediT are skipped. Each name or set of
# MAGIC initials is matched to the work's byline and used only when exactly one author fits it; a statement whose names
# MAGIC mostly don't fit the byline is dropped whole (the stored text may belong to another paper). The parser is
# MAGIC `utils/credit_statements.py` (pure Python, tested in `utils/tests/test_credit_statements.py`).
# MAGIC
# MAGIC Writes ONLY:
# MAGIC - `openalex.works.fulltext_contributor_roles`: one row per work whose statement was read, `authors` shaped like
# MAGIC   `openalex.crossref.crossref_contributor_roles.authors` (position = 0-based seat on the byline read, name = that
# MAGIC   seat's raw_author_name, roles = CRediT slugs). CreateWorksEnriched uses it after Crossref and PubMed Central.
# MAGIC - `openalex.works.fulltext_contributor_roles_runs`: one row per run (restart point for the daily delta).
# MAGIC
# MAGIC Modes: `daily` reads works whose full text changed since the last run's newest `work_fulltext.updated_at`
# MAGIC (minus an hour), with or without a statement; `all` reads every work (first fill, or a parser change: bump
# MAGIC `PARSER_VERSION`) and drops rows of works no longer read. A work whose new read yields no roles loses its row; a
# MAGIC parser error never removes one, and more than 1% errors stops the run before the write.

# COMMAND ----------

import os
import sys
import time

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "credit_statements.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "credit_statements.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils import credit_statements  # noqa: E402

import pandas as pd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (ArrayType, IntegerType, LongType, StringType, StructField,  # noqa: E402
                               StructType, TimestampType)

dbutils.widgets.text("target", "openalex.works.fulltext_contributor_roles", "target table")
dbutils.widgets.text("mode", "daily", "daily | all")
dbutils.widgets.text("sample_pct", "", "testing: read only this TABLESAMPLE percent of work_fulltext")
dbutils.widgets.text("reuse_candidates", "false", "true: parse the existing _candidates table (built elsewhere, e.g. on a SQL warehouse)")

TARGET = dbutils.widgets.get("target").strip()
RUNS = TARGET + "_runs"
CAND = TARGET + "_candidates"
MODE = dbutils.widgets.get("mode").strip()
SAMPLE = dbutils.widgets.get("sample_pct").strip()
REUSE = dbutils.widgets.get("reuse_candidates").strip().lower() == "true"
assert MODE in ("daily", "all"), MODE
T0 = time.time()
RUN_ID = time.strftime("%Y%m%dT%H%M%S")
print(f"parser {credit_statements.PARSER_VERSION}, mode {MODE}, target {TARGET}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
  work_id BIGINT NOT NULL,
  authors ARRAY<STRUCT<position: INT, given: STRING, family: STRING, name: STRING, orcid: STRING, sequence: STRING,
                       roles: ARRAY<STRING>>> COMMENT 'byline seats with roles; name = the raw_author_name read',
  n_authors_with_roles INT,
  layout STRING COMMENT 'term_first | name_first',
  parser_version STRING,
  fulltext_key STRING COMMENT 'work_fulltext.key the statement was read from',
  fulltext_updated_at TIMESTAMP,
  updated_at TIMESTAMP)
COMMENT 'oxjob #2450: per-author CRediT roles read from full-text contribution statements in CRediT terms'
CLUSTER BY (work_id)""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RUNS} (
  run_id STRING, mode STRING, parser_version STRING, n_candidates BIGINT, n_ok BIGINT, n_removed BIGINT,
  source_max_updated_at TIMESTAMP, seconds DOUBLE, finished_at TIMESTAMP)""")

# COMMAND ----------

# The works to read: a CRediT writing term somewhere in the text. The window holds the statement (it sits near that
# term); the byline is the published work's.
SIGNAL = "original draft|review (and|&) editing|review ?& ?editing|reviewing (and|&) editing"
if MODE == "daily":
    wm = spark.sql(f"SELECT max(source_max_updated_at) AS wm FROM {RUNS} WHERE n_candidates IS NOT NULL").first().wm
    where = "TRUE" if wm is None else f"updated_at > TIMESTAMP '{wm}' - INTERVAL 1 HOUR"
else:
    where = "TRUE"
sample = f"TABLESAMPLE ({float(SAMPLE)} PERCENT) REPEATABLE (2450)" if SAMPLE else ""
if not REUSE:
    spark.sql(f"""
CREATE OR REPLACE TABLE {CAND} AS
WITH ft AS (
  SELECT work_id, key, updated_at, fulltext, regexp_instr(lower(fulltext), '{SIGNAL}') AS p
  FROM openalex.works.work_fulltext {sample}
  WHERE {where}
),
-- daily: every changed work, so one whose new text has no statement loses its old roles (win NULL)
hit AS (SELECT * FROM ft WHERE p > 0 OR {str(MODE == "daily").upper()})
SELECT h.work_id, h.key, h.updated_at,
       CASE WHEN h.p > 0 THEN substring(h.fulltext, greatest(h.p - 3000, 1), 7000) END AS win,
       transform(w.authorships, a -> a.raw_author_name) AS byline
FROM hit h JOIN openalex.works.openalex_works w ON w.id = h.work_id""")
src = spark.sql(f"SELECT count(*) AS n, max(updated_at) AS mx FROM {CAND}").first()
n_cand, src_max = int(src.n), src.mx
print(f"{n_cand:,} candidate works ({time.time() - T0:.0f} s); newest full text {src_max}")

# COMMAND ----------

OUT = StructType([
    StructField("work_id", LongType()), StructField("status", StringType()), StructField("layout", StringType()),
    StructField("authors", ArrayType(StructType([
        StructField("position", IntegerType()), StructField("given", StringType()), StructField("family", StringType()),
        StructField("name", StringType()), StructField("orcid", StringType()), StructField("sequence", StringType()),
        StructField("roles", ArrayType(StringType()))]))),
    StructField("n_authors_with_roles", IntegerType()), StructField("fulltext_key", StringType()),
    StructField("fulltext_updated_at", TimestampType())])


def parse_partition(batches):
    """Runs on executors. A statement that can't be read (any status but ok, or an exception) is a row with no
    authors, so it can remove an older read."""
    import sys as _sys
    if REPO_ROOT not in _sys.path:
        _sys.path.insert(0, REPO_ROOT)
    from utils import credit_statements as _cs
    cols = [f.name for f in OUT.fields]
    for pdf in batches:
        rows = []
        for rec in pdf.to_dict("records"):
            byline = [b or "" for b in (rec["byline"] if rec["byline"] is not None else [])]
            try:
                res = _cs.parse(rec["win"] or "", byline)
                status, layout, roles = res.status, res.layout, res.roles
            except Exception as e:  # noqa: BLE001 - one bad text never stops the run
                status, layout, roles = "error:" + type(e).__name__, None, {}
            authors = [{"position": int(i), "given": None, "family": None, "name": byline[i], "orcid": None,
                        "sequence": None, "roles": list(v)} for i, v in sorted(roles.items())] if status == "ok" else None
            rows.append({"work_id": int(rec["work_id"]), "status": status, "layout": layout or None,
                         "authors": authors, "n_authors_with_roles": len(authors) if authors else 0,
                         "fulltext_key": rec["key"], "fulltext_updated_at": rec["updated_at"]})
        yield pd.DataFrame(rows, columns=cols).astype(object).where(lambda d: d.notna(), None)


parsed_tbl = TARGET + "_parsed"
n_tasks = max(spark.sparkContext.defaultParallelism * 4, n_cand // 5000 + 1)
# one row per work: the MERGE below must never see a work twice (work_fulltext is keyed by work_id; this is a guard)
(spark.table(CAND).dropDuplicates(["work_id"]).repartition(n_tasks).mapInPandas(parse_partition, schema=OUT)
 .withColumn("parser_version", F.lit(credit_statements.PARSER_VERSION))
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(parsed_tbl))
display(spark.sql(f"SELECT status, layout, count(*) AS n FROM {parsed_tbl} GROUP BY ALL ORDER BY n DESC"))

# COMMAND ----------

n_err = spark.sql(f"SELECT count_if(status LIKE 'error%') AS e FROM {parsed_tbl}").first().e
if n_cand and n_err / n_cand > 0.01:
    raise RuntimeError(f"{n_err:,} of {n_cand:,} reads raised errors: parser bug; nothing written")

# ok reads replace the stored row; a work read again without roles loses its row; an error keeps the old row;
# mode all also drops works it no longer reads
drop_unread = "WHEN NOT MATCHED BY SOURCE THEN DELETE" if (MODE == "all" and not SAMPLE) else ""
merge = spark.sql(f"""
MERGE INTO {TARGET} AS t
USING {parsed_tbl} AS s
ON t.work_id = s.work_id
WHEN MATCHED AND s.status = 'ok' THEN UPDATE SET
  t.authors = s.authors, t.n_authors_with_roles = s.n_authors_with_roles, t.layout = s.layout,
  t.parser_version = s.parser_version, t.fulltext_key = s.fulltext_key, t.fulltext_updated_at = s.fulltext_updated_at,
  t.updated_at = current_timestamp()
WHEN MATCHED AND s.status != 'ok' AND s.status NOT LIKE 'error%' THEN DELETE
WHEN NOT MATCHED AND s.status = 'ok' THEN INSERT
  (work_id, authors, n_authors_with_roles, layout, parser_version, fulltext_key, fulltext_updated_at, updated_at)
  VALUES (s.work_id, s.authors, s.n_authors_with_roles, s.layout, s.parser_version, s.fulltext_key,
          s.fulltext_updated_at, current_timestamp())
{drop_unread}""").first()
n_ok = spark.sql(f"SELECT count(*) AS n FROM {parsed_tbl} WHERE status = 'ok'").first().n
n_removed = int(merge["num_deleted_rows"]) if "num_deleted_rows" in merge.asDict() else None
secs = time.time() - T0
spark.createDataFrame([(RUN_ID, MODE, credit_statements.PARSER_VERSION, n_cand, int(n_ok), n_removed, src_max, secs)],
                      "run_id string, mode string, parser_version string, n_candidates bigint, n_ok bigint, "
                      "n_removed bigint, source_max_updated_at timestamp, seconds double") \
    .withColumn("finished_at", F.current_timestamp()).write.mode("append").saveAsTable(RUNS)
print(f"done: {n_cand:,} read, {n_ok:,} with roles, {n_removed} removed, {secs / 60:.1f} min")
