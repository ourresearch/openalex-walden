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
# MAGIC **Replace mode (oxjob #1800 lead 2, 11 Nov 2026).** ~0.36M works SHOW a record's short first description (a
# MAGIC cataloguing note: "Thesis (Ph. D.)--...", "Orientador: ...") while a later one holds the abstract. `mode=replace` writes
# MAGIC those works' accepted later descriptions with `replace_hash` = xxhash64 of the text the work shows (as CreateWorksBase
# MAGIC writes it); CreateWorksBase shows the fill instead only while the work still shows exactly that text. Bar
# MAGIC pre-registered per stratum (own abstract and losses), EXPLORE § 45. Each mode rewrites only its own rows
# MAGIC (`INSERT INTO ... REPLACE WHERE`, one atomic commit).
# MAGIC
# MAGIC **Opening mode (oxjob #2162, 12 Nov 2026).** A publisher-marked opening (T&F books' "first-paragraph", Cambridge Core's
# MAGIC "extract") that describes the work stays its abstract (Jason, landing-pages charter § 6 2026-11-11 20:19). Parseland
# MAGIC no longer emits marked openings (parseland-lib 5ad0605), so `mode=opening` writes the ones a Jev reader
# MAGIC (`opening-desc-v1`, p >= 0.15) keeps, for works with no legacy backfill row, as rows with `path = 'landing_page_opening'`.
# MAGIC It rewrites only those rows; `fill` mode leaves them alone. Revert: `DELETE FROM openalex.abstracts.repo_description_fill
# MAGIC WHERE path = 'landing_page_opening'`.
# MAGIC
# MAGIC **MANUAL TRIGGER ONLY; built once per mode.** New records keep the ingest parse. Re-running a mode replaces that mode's
# MAGIC rows; the table is never dropped, because CreateWorksBase reads it every night.
# MAGIC **Revert:** `TRUNCATE TABLE openalex.abstracts.repo_description_fill` (the next nightly takes the abstracts back
# MAGIC out), or only lead 2: `DELETE FROM openalex.abstracts.repo_description_fill WHERE replace_hash IS NOT NULL`; then drop
# MAGIC the CreateWorksBase join before ever dropping the table.

# COMMAND ----------

# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.40-py3-none-any.whl
# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from openalex.dlt.transform import udf_abstract_features

dbutils.widgets.text("source_table", "", "accepted choices: work_id, native_id, path, endpoint_id, p_abstract, text[, replace_hash]")
dbutils.widgets.dropdown("mode", "fill", ["fill", "replace", "opening"],
                         "fill: works with no abstract; replace: works showing the stub; opening: #2162 kept openings")
SOURCE = dbutils.widgets.get("source_table").strip()
MODE = dbutils.widgets.get("mode")
assert SOURCE, "source_table is required"
FILTER_COMMIT = {  # stored on every row
    "fill": "repo-desc-v1 >= 0.95 on rule desc-v2 (oxjobs #1800 EXPLORE § 41)",
    "replace": "repo-desc-v1 >= 0.95 on rule desc-v2, replacing the shown short first description (oxjobs #1800 EXPLORE § 45)",
    "opening": "opening-desc-v1 >= 0.15 on a page-marked opening parseland-lib 5ad0605 drops (oxjobs #2162 EXPLORE § 5)",
}[MODE]
OPENING = "coalesce(path, '') = 'landing_page_opening'"  # oxjob #2162's rows
MODE_ROWS = {  # the rows this mode owns
    "fill": f"replace_hash IS NULL AND NOT {OPENING}",
    "replace": "replace_hash IS NOT NULL",
    "opening": OPENING,
}[MODE]
TARGET = "openalex.abstracts.repo_description_fill"
LEGACY = "openalex.abstracts.abstracts_backfill"  # CreateWorksBase unions the fill into it: no work may be in both
REPEAT_MIN = 5
print(f"mode {MODE}: source {SOURCE} -> {TARGET} ({FILTER_COMMIT})")

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
  built_at TIMESTAMP,
  replace_hash BIGINT
)
COMMENT 'oxjob #1800: the later dc:description of a repository record, accepted by a Jev record reader, as the abstract of a work that has no other. Unioned by CreateWorksBase into its abstracts_backfill fallback (priority 997).'
""")
if "replace_hash" not in spark.table(TARGET).columns:  # the table predates replace mode (built 9 Nov)
    spark.sql(f"ALTER TABLE {TARGET} ADD COLUMNS (replace_hash BIGINT COMMENT "
              "'oxjob #1800 lead 2: xxhash64 of the short first description the work showed; CreateWorksBase shows the fill "
              "instead while the work still shows exactly that text. NULL = fill only where the work has no abstract.')")

# COMMAND ----------

src = (spark.table(SOURCE).withColumn("work_id", F.col("work_id").cast("bigint"))  # before the window: one row per key
       .join(spark.table(LEGACY).select("work_id"), "work_id", "left_anti"))  # one row per work in the union
if MODE == "opening":  # one row per work: works another mode already holds are left out
    src = (src.withColumn("replace_hash", F.lit(None).cast("bigint")).withColumn("path", F.lit("landing_page_opening"))
           .join(spark.table(TARGET).filter(f"NOT {OPENING}").select("work_id"), "work_id", "left_anti"))
elif MODE == "fill":
    src = (src.withColumn("replace_hash", F.lit(None).cast("bigint"))
           # one row per work: works #2162's opening rows hold are left out
           .join(spark.table(TARGET).filter(OPENING).select("work_id"), "work_id", "left_anti"))
else:
    assert "replace_hash" in src.columns, "replace mode needs replace_hash in the source"
    src = (src.withColumn("replace_hash", F.col("replace_hash").cast("bigint")).filter("replace_hash IS NOT NULL")
           # one row per work: works the other mode already holds are left out
           .join(spark.table(TARGET).filter("replace_hash IS NULL").select("work_id"), "work_id", "left_anti"))
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
        "replace_hash",
    )
    # Both or neither (#191.1): CreateWorksBase offers the pair only when both are present.
    .filter(F.col("fill_abstract").isNotNull() & F.col("fill_abstract_inverted_index").isNotNull())
    # A text on REPEAT_MIN+ works is a collection-wide description (an alumnae survey on 2,047 people's records, an
    # atlas's note on each map sheet), not each work's abstract: of 50 such works 18 were their own (EXPLORE § 41).
    .withColumn("_n", F.count("*").over(Window.partitionBy(F.xxhash64("fill_abstract"))))
    .filter(F.col("_n") < REPEAT_MIN).drop("_n")
)
rows.createOrReplaceTempView("_fill_rows")
spark.sql(f"INSERT INTO {TARGET} REPLACE WHERE {MODE_ROWS} SELECT * FROM _fill_rows")  # this mode's rows only, one commit

# COMMAND ----------

t = spark.table(TARGET)
n, n_works = t.agg(F.count("*"), F.countDistinct("work_id")).first()
print(f"source rows {n_src:,}; fill rows {n:,}; distinct works {n_works:,}")
assert n == n_works, "more than one fill row for a work: CreateWorksBase would duplicate works"
n_legacy = t.join(spark.table(LEGACY).select("work_id"), "work_id", "left_semi").count()
assert n_legacy == 0, f"{n_legacy} fill works also have an abstracts_backfill row: CreateWorksBase would duplicate them"
display(t.groupBy("path", F.col("replace_hash").isNotNull().alias("replace")).agg(
    F.count("*").alias("works"), F.avg(F.length("fill_abstract")).alias("avg_len")))
