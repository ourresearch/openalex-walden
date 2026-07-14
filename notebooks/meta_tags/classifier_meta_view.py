# Databricks notebook source
# Classifier Meta View (wave-2 derived layer, work-type classifier)
# raw landing_page_meta_tags (ALL tags) -> the classifier's 8-family tx_meta + tx_page_title.
# Contract: eval/wave2_reference_extractor.py — KEEP copied VERBATIM and run as the SAME
# Python regex via pandas UDF (Casey: NOT an RLIKE translation; the classifier's rules were
# measured against these exact bytes). Byte-compat vs the reference is checked by
# eval/wave2_bytecompat_derived.py after every deriver change.
# Full overwrite each run — reparse-from-stored is the cheap operation (~minutes); no resume
# machinery needed. Casey 2026-07-13: adding a tag family later = edit KEEP, bump
# DERIVER_VERSION, rerun.

import re

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

# COMMAND ----------

dbutils.widgets.text("source_table", "openalex_dev.rohan_lab.landing_page_meta_tags_dev")
dbutils.widgets.text("target_table", "openalex_dev.rohan_lab.classifier_meta_view_dev")

source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")

DERIVER_VERSION = "keep8-2026-07-13"   # bump on ANY change to KEEP; ingest version rides along per-row

# COMMAND ----------

# KEEP — copied VERBATIM from eval/wave2_reference_extractor.py (the byte-compat contract).
# re.X: whitespace/newlines inside the pattern are ignored, so the line-wrap is cosmetic.
KEEP = re.compile(
    r"""["'](?:citation_[a-z_]+|bepress_citation_[a-z_]+|dc\.type[a-z.]*|dcterms\.type|
        DC\.Type|article-?type|og:type|prism\.contenttype|eprints\.type)["']""",
    re.I | re.X)


@F.pandas_udf(ArrayType(StringType()))
def keep8_udf_vectorized(meta_tags: pd.Series) -> pd.Series:
    """Filter each row's raw tag array to the 8 families; tags pass through byte-unmodified."""
    return meta_tags.apply(
        lambda tags: [t for t in (tags if tags is not None else []) if KEEP.search(t)])

# COMMAND ----------

# one row per WORK (native_id, namespace) — consumers LEFT JOIN per work; per-file rows would
# fan out works whose page was stored more than once (280 dup native_ids measured in the 1M).
# Newest fetched_at wins (tie: max file_key); chosen file_key kept as provenance. The raw
# table keeps every file — per-file granularity lives there.
# Two-phase dedupe instead of a window: a window would shuffle all ~300M rows WITH their
# meta_tags arrays through the sorter (OOM'd a 256GB node); the aggregate only shuffles
# keys+timestamps, then one join moves each winning payload once.
src = (
    spark.read.table(source_table)
    .filter(F.col("status") == "ok")
    .filter(F.col("native_id").isNotNull()))
winners = (
    src.groupBy("native_id", "native_id_namespace")
    .agg(F.max(F.struct("fetched_at", "file_key")).alias("w"))
    .select("native_id", "native_id_namespace",
            F.col("w.fetched_at").alias("fetched_at"),
            F.col("w.file_key").alias("file_key")))
latest = (
    src.join(winners, ["native_id", "native_id_namespace", "fetched_at", "file_key"])
    .dropDuplicates(["native_id", "native_id_namespace"]))

out = latest.select(
    "file_key", "native_id", "native_id_namespace",
    keep8_udf_vectorized("meta_tags").alias("tx_meta"),
    F.col("page_title").alias("tx_page_title"),
    "extractor_version",
    F.lit(DERIVER_VERSION).alias("deriver_version"),
    F.current_timestamp().alias("derived_at"),
)

out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

# COMMAND ----------

t = spark.read.table(target_table)
print(f"rows: {t.count():,}")
t.select(
    F.avg(F.size("tx_meta")).alias("avg_kept_tags"),
    F.avg((F.size("tx_meta") > 0).cast("int")).alias("any_kept_frac"),
    F.avg(F.col("tx_page_title").isNotNull().cast("int")).alias("title_frac"),
).show()
t.groupBy("native_id_namespace").count().show()
