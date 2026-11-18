# Databricks notebook source
# MAGIC %md
# MAGIC # Crossref CRediT contributor roles (oxjob #2075)
# MAGIC
# MAGIC Crossref's REST API gives every author a `role` list since about 21 May 2026: `{"vocabulary": "crossref",
# MAGIC "role": "author"}` by default, plus `{"vocabulary": "credit", "role": "conceptualization"}` etc. when the
# MAGIC publisher deposits CRediT roles (MDPI since Sep 2026). Walden's Crossref pipeline reads the raw files with a schema
# MAGIC frozen before that field existed, so the roles are dropped there (`crossref_items` has no `author.role` and an
# MAGIC empty `_rescued_data`).
# MAGIC
# MAGIC This notebook reads only the fields it needs from the same raw files (`s3://openalex-ingest/crossref/`) and
# MAGIC keeps, per DOI, the newest record's authors with their CRediT roles. It never touches the Crossref pipeline.
# MAGIC
# MAGIC Writes ONLY:
# MAGIC - `openalex.crossref.crossref_contributor_roles`: one row per DOI that has carried CRediT roles in any record
# MAGIC   read; the newest record (by Crossref's `indexed` time) wins, so a DOI whose newest record drops its roles keeps
# MAGIC   a row with `authors` NULL and `n_authors_with_roles` 0.
# MAGIC - `openalex.crossref.crossref_contributor_roles_progress`: one row per finished chunk (restart-safe).
# MAGIC
# MAGIC Nothing else reads these tables until the authorship schema for roles is approved (#2075, standing rule 77).
# MAGIC
# MAGIC Modes: the daily run re-reads the last `days_back` days of `updates/` and `new-works/` (idempotent: a record
# MAGIC replaces the stored one only when it is newer). A backfill sets `from_date` / `to_date` and reads in chunks of
# MAGIC `chunk_days`, skipping chunks already done; `extra_paths` adds one-off folders (replays, re-pulls).
# MAGIC
# MAGIC Roles are taken as deposited, never inferred: only vocabulary `credit`, only the 14 CRediT role slugs.

# COMMAND ----------

import datetime as dt
import time

from pyspark.sql import functions as F

dbutils.widgets.text("target", "openalex.crossref.crossref_contributor_roles", "target table")
dbutils.widgets.text("days_back", "3", "daily mode: re-read this many days back from today (UTC)")
dbutils.widgets.text("from_date", "", "backfill: first folder date (YYYY-MM-DD); empty = daily mode")
dbutils.widgets.text("to_date", "", "backfill: last folder date (YYYY-MM-DD); empty = today")
dbutils.widgets.text("chunk_days", "4", "backfill: folder days per chunk (one MERGE + one progress row each)")
dbutils.widgets.text("extra_paths", "", "comma-separated extra S3 folders read as one more chunk (replays, re-pulls)")
dbutils.widgets.text("max_minutes", "600", "stop starting new chunks after this long")

TARGET = dbutils.widgets.get("target").strip()
PROGRESS = TARGET + "_progress"
DAYS_BACK = int(dbutils.widgets.get("days_back"))
FROM_DATE = dbutils.widgets.get("from_date").strip()
TO_DATE = dbutils.widgets.get("to_date").strip()
CHUNK_DAYS = int(dbutils.widgets.get("chunk_days"))
EXTRA = [p.strip().rstrip("/") + "/" for p in dbutils.widgets.get("extra_paths").split(",") if p.strip()]
MAX_MINUTES = float(dbutils.widgets.get("max_minutes"))
BASE = "s3://openalex-ingest/crossref"
T0 = time.time()

# The 14 CRediT roles (ANSI/NISO Z39.104-2022), as Crossref spells them (the slugs of credit.niso.org's role URLs);
# the same list as utils/pmc_jats.py CREDIT_ROLES.
CREDIT_ROLES = [
    "conceptualization", "data-curation", "formal-analysis", "funding-acquisition", "investigation", "methodology",
    "project-administration", "resources", "software", "supervision", "validation", "visualization",
    "writing-original-draft", "writing-review-editing",
]

# Only what this notebook reads; Spark skips every other field while parsing.
RAW_SCHEMA = (
    "items ARRAY<STRUCT<DOI: STRING, member: STRING, publisher: STRING, type: STRING, "
    "indexed: STRUCT<`date-time`: STRING>, "
    "author: ARRAY<STRUCT<given: STRING, family: STRING, name: STRING, ORCID: STRING, sequence: STRING, "
    "role: ARRAY<STRUCT<role: STRING, vocabulary: STRING>>>>>>"
)

spark.sql(f"""CREATE TABLE IF NOT EXISTS {TARGET} (
  doi STRING COMMENT 'Lower-cased DOI (Crossref native id)',
  indexed_at TIMESTAMP COMMENT 'Crossref indexed time of the record kept (newest wins)',
  member STRING, publisher STRING, type STRING,
  authors ARRAY<STRUCT<position: INT, given: STRING, family: STRING, name: STRING, orcid: STRING, sequence: STRING,
                       roles: ARRAY<STRING>>> COMMENT 'Crossref author list in deposit order; roles = CRediT slugs',
  n_authors_with_roles INT,
  source_file STRING,
  updated_at TIMESTAMP
) USING DELTA CLUSTER BY (doi)
COMMENT 'CRediT contributor roles from Crossref deposits, newest record per DOI (oxjob #2075). Internal until the authorship schema is approved.'""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {PROGRESS} (
  run_id STRING, chunk STRING, n_paths INT, n_records BIGINT, n_dois BIGINT, n_dois_with_roles BIGINT,
  seconds DOUBLE, done_at TIMESTAMP
) USING DELTA COMMENT 'One row per finished chunk of CrossrefContributorRoles (oxjob #2075)'""")

# COMMAND ----------


def exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:  # noqa: BLE001 - a folder the feed never wrote (a gap day)
        return False


def day_paths(day):
    return [p for p in (f"{BASE}/updates/{day:%Y/%m/%d}/", f"{BASE}/new-works/{day:%Y/%m/%d}/") if exists(p)]


today = dt.datetime.now(dt.timezone.utc).date()
if FROM_DATE:
    start = dt.date.fromisoformat(FROM_DATE)
    end = dt.date.fromisoformat(TO_DATE) if TO_DATE else today
    days = [start + dt.timedelta(days=i) for i in range((end - start).days + 1)]
    chunks = [(f"{days[i]:%Y-%m-%d}..{days[min(i + CHUNK_DAYS, len(days)) - 1]:%Y-%m-%d}", days[i:i + CHUNK_DAYS])
              for i in range(0, len(days), CHUNK_DAYS)]
    done = {r.chunk for r in spark.table(PROGRESS).select("chunk").distinct().collect()}
    todo = [(label, ds) for label, ds in chunks if label not in done]
    print(f"backfill {start}..{end}: {len(chunks)} chunks, {len(chunks) - len(todo)} already done")
else:
    days = [today - dt.timedelta(days=i) for i in range(DAYS_BACK, -1, -1)]
    todo = [(f"daily:{days[0]:%Y-%m-%d}..{days[-1]:%Y-%m-%d}", days)]
plans = [(label, [p for d in ds for p in day_paths(d)]) for label, ds in todo]
if EXTRA:
    plans.append(("extra:" + ",".join(EXTRA), [p for p in EXTRA if exists(p)]))

# COMMAND ----------

credit_sql = ", ".join(f"'{r}'" for r in CREDIT_ROLES)
authors_expr = f"""transform(r.author, (a, i) -> named_struct(
    'position', i, 'given', a.given, 'family', a.family, 'name', a.name,
    'orcid', nullif(upper(regexp_extract(a.ORCID, '(\\\\d{{4}}-\\\\d{{4}}-\\\\d{{4}}-\\\\d{{3}}[\\\\dXx])', 1)), ''),
    'sequence', a.sequence,
    'roles', array_distinct(filter(
        transform(filter(a.role, x -> lower(x.vocabulary) = 'credit'), x -> lower(trim(x.role))),
        x -> x IN ({credit_sql})))))"""


def read_chunk(paths):
    """Newest record per DOI in these folders; authors kept only when a CRediT role is present."""
    recs = (spark.read.schema(RAW_SCHEMA).option("multiline", "true").option("recursiveFileLookup", "true")
            .json(paths)
            .select(F.explode("items").alias("r"), F.col("_metadata.file_path").alias("source_file"))
            .select(F.lower(F.trim("r.DOI")).alias("doi"),
                    F.to_timestamp(F.col("r.indexed.`date-time`")).alias("indexed_at"),
                    F.col("r.member").alias("member"), F.col("r.publisher").alias("publisher"),
                    F.col("r.type").alias("type"), F.expr(authors_expr).alias("authors"), "source_file")
            .filter("doi IS NOT NULL AND doi != ''")
            .withColumn("n_authors_with_roles", F.expr("coalesce(size(filter(authors, a -> size(a.roles) > 0)), 0)"))
            # role-less records only need to say "this DOI's newest record has no roles": drop the author list
            .withColumn("authors", F.when(F.col("n_authors_with_roles") > 0, F.col("authors"))))
    return (recs.groupBy("doi")
            .agg(F.count("*").alias("_n"),
                 F.max_by(F.struct("indexed_at", "member", "publisher", "type", "authors", "n_authors_with_roles",
                                   "source_file"), "indexed_at").alias("s"))
            .select("doi", "_n", "s.*"))


run_id = time.strftime("%Y%m%dT%H%M%S")
for n, (label, paths) in enumerate(plans, 1):
    if (time.time() - T0) / 60 > MAX_MINUTES:
        print(f"stopping before chunk {label}: {MAX_MINUTES} minutes used")
        break
    if not paths:
        print(f"chunk {label}: no folders, skipped")
        continue
    t = time.time()
    latest = read_chunk(paths).cache()
    stats = latest.agg(F.sum("_n").alias("records"), F.count("*").alias("dois"),
                       F.count_if(F.col("n_authors_with_roles") > 0).alias("with_roles")).first()
    latest.drop("_n").withColumn("updated_at", F.current_timestamp()).createOrReplaceTempView("chunk_latest")
    spark.sql(f"""
      MERGE INTO {TARGET} t USING chunk_latest s ON t.doi = s.doi
      WHEN MATCHED AND s.indexed_at > t.indexed_at THEN UPDATE SET *
      WHEN NOT MATCHED AND s.n_authors_with_roles > 0 THEN INSERT *
    """)
    latest.unpersist()
    secs = time.time() - t
    spark.createDataFrame(
        [(run_id, label, len(paths), int(stats.records or 0), int(stats.dois or 0), int(stats.with_roles or 0), secs)],
        "run_id string, chunk string, n_paths int, n_records bigint, n_dois bigint, n_dois_with_roles bigint, seconds double",
    ).withColumn("done_at", F.current_timestamp()).write.mode("append").saveAsTable(PROGRESS)
    left = len(plans) - n
    print(f"chunk {n}/{len(plans)} {label}: {len(paths)} folders, {int(stats.records or 0):,} records, "
          f"{int(stats.dois or 0):,} DOIs, {int(stats.with_roles or 0):,} with CRediT roles, {secs:.0f}s; "
          f"ETA {left * (time.time() - T0) / n / 60:.0f} min")

# COMMAND ----------

s = spark.sql(f"""SELECT count(*) n, count_if(n_authors_with_roles > 0) with_roles,
  count(DISTINCT CASE WHEN n_authors_with_roles > 0 THEN member END) members FROM {TARGET}""").first()
print(f"{TARGET}: {s.n:,} DOIs, {s.with_roles:,} with CRediT roles now, from {s.members} Crossref members "
      f"({(time.time() - T0) / 60:.0f} min total)")
