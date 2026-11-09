# Databricks notebook source
# MAGIC %md
# MAGIC # Repository description fill (oxjob #1800)
# MAGIC
# MAGIC Many repository records put a short note first ("Orientador: ...", "107 p.", a deposit notice) and the work's
# MAGIC abstract in a later `dc:description`. The parsers take the first, so ~0.4M works show no abstract although one of
# MAGIC their repository records holds it. The live parser can't choose per record (its rule failed its bar where works
# MAGIC show nothing, oxjobs #1800 EXPLORE § 38), so the choice is made here, once, by a record reader:
# MAGIC
# MAGIC - candidate: rule desc-v2 (first description blank, under 100 characters or a deposit notice; the first later one
# MAGIC   of 200+ characters that doesn't open like a notice and isn't a "Published as part of" line), on both repository
# MAGIC   paths, for works that showed no abstract when the source table was built;
# MAGIC - reader: Jev, card `repo-desc-v1` (the work's title + the candidate), accepted at p_abstract >= 0.95; per work the
# MAGIC   record with the highest p_abstract;
# MAGIC - bar (pre-registered, fresh 1% bucket, Opus 5.5 judge): live path on the chooser endpoints 100/100, backfill on
# MAGIC   them 99/100, backfill elsewhere 96/100 own abstract. The live path elsewhere failed (93/100) and is left out.
# MAGIC   oxjobs #1800 EXPLORE § 41.
# MAGIC
# MAGIC `CreateWorksBase` unions the fill into the legacy abstracts_backfill fallback (priority 997, after every location),
# MAGIC so it shows only where a work has no other abstract: no work loses or switches one. A work with any
# MAGIC abstracts_backfill row is left out, so the union keeps one row per work.
# MAGIC
# MAGIC The text goes through the shipped `udf_abstract_features`, the call `apply_initial_processing` makes on every
# MAGIC repository abstract, so the stored abstract and its inverted index are what a repository location would carry,
# MAGIC from byte-identical text (#191.1). A row carries both or neither.
# MAGIC
# MAGIC **MANUAL TRIGGER ONLY; built once.** New records keep the ingest parse. Re-running replaces the rows (INSERT
# MAGIC OVERWRITE; the table is never dropped, because CreateWorksBase reads it every night).
# MAGIC **Revert:** `TRUNCATE TABLE openalex.abstracts.repo_description_fill` (the next nightly takes the abstracts back
# MAGIC out), then drop the CreateWorksBase join before ever dropping the table.

# COMMAND ----------

# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.40-py3-none-any.whl
# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from openalex.dlt.transform import udf_abstract_features

dbutils.widgets.text("source_table", "", "accepted choices: work_id, native_id, path, endpoint_id, p_abstract, text")
SOURCE = dbutils.widgets.get("source_table").strip()
assert SOURCE, "source_table is required"
FILTER_COMMIT = "repo-desc-v1 >= 0.95 on rule desc-v2 (oxjobs #1800 EXPLORE § 41)"  # stored on every row
TARGET = "openalex.abstracts.repo_description_fill"
LEGACY = "openalex.abstracts.abstracts_backfill"  # CreateWorksBase unions the fill into it: no work may be in both
REPEAT_MIN = 5
print(f"source {SOURCE} -> {TARGET} ({FILTER_COMMIT})")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
  work_id BIGINT NOT NULL,
  fill_abstract STRING,
  fill_abstract_inverted_index STRING,
  native_id STRING,
  path STRING,
  endpoint_id STRING,
  p_abstract DOUBLE,
  filter_commit STRING,
  built_at TIMESTAMP
)
COMMENT 'oxjob #1800: the later dc:description of a repository record, accepted by a Jev record reader, as the abstract of a work that has no other. Unioned by CreateWorksBase into its abstracts_backfill fallback (priority 997).'
""")

# COMMAND ----------

src = (spark.table(SOURCE).withColumn("work_id", F.col("work_id").cast("bigint"))  # before the window: one row per key
       .join(spark.table(LEGACY).select("work_id"), "work_id", "left_anti"))  # one row per work in the union
n_src = src.count()
# One row per work: the highest p_abstract, then native_id, path, endpoint (the order the bar drew by).
one = (
    src.withColumn("_r", F.row_number().over(Window.partitionBy("work_id").orderBy(
        F.col("p_abstract").desc(), "native_id", "path", "endpoint_id")))
    .filter("_r = 1").drop("_r")
)
rows = (
    one.withColumn("_f", udf_abstract_features(F.col("text")))
    .select(
        "work_id",
        F.col("_f.abstract").alias("fill_abstract"),
        F.col("_f.abstract_inverted_index").alias("fill_abstract_inverted_index"),
        "native_id", "path", "endpoint_id",
        F.col("p_abstract").cast("double").alias("p_abstract"),
        F.lit(FILTER_COMMIT).alias("filter_commit"),
        F.current_timestamp().alias("built_at"),
    )
    # Both or neither (#191.1): CreateWorksBase offers the pair only when both are present.
    .filter(F.col("fill_abstract").isNotNull() & F.col("fill_abstract_inverted_index").isNotNull())
    # A text on REPEAT_MIN+ works is a collection-wide description (an alumnae survey on 2,047 people's records, an
    # atlas's note on each map sheet), not each work's abstract: of 50 such works 18 were their own (EXPLORE § 41).
    .withColumn("_n", F.count("*").over(Window.partitionBy(F.xxhash64("fill_abstract"))))
    .filter(F.col("_n") < REPEAT_MIN).drop("_n")
)
rows.createOrReplaceTempView("_fill_rows")
spark.sql(f"INSERT OVERWRITE {TARGET} SELECT * FROM _fill_rows")

# COMMAND ----------

t = spark.table(TARGET)
n, n_works = t.agg(F.count("*"), F.countDistinct("work_id")).first()
print(f"source rows {n_src:,}; fill rows {n:,}; distinct works {n_works:,}")
assert n == n_works, "more than one fill row for a work: CreateWorksBase would duplicate works"
n_legacy = t.join(spark.table(LEGACY).select("work_id"), "work_id", "left_semi").count()
assert n_legacy == 0, f"{n_legacy} fill works also have an abstracts_backfill row: CreateWorksBase would duplicate them"
display(t.groupBy("path").agg(F.count("*").alias("works"), F.avg(F.length("fill_abstract")).alias("avg_len")))
