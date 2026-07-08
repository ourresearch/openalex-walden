# Databricks notebook source
# MAGIC %md
# MAGIC # Build native_id → repository_id Mapping from S3
# MAGIC
# MAGIC Reads gzipped XML files from `s3a://openalex-ingest/repositories/` to build a
# MAGIC lookup table mapping `native_id` to `repository_id` (from S3 folder path).
# MAGIC
# MAGIC **Approach**: Per-folder processing with binary file reads + regex (NOT Spark XML).
# MAGIC - Read files as raw binary via `spark.read.format("binaryFile")`
# MAGIC - Decompress gzip + regex `<ns0:identifier>` in a UDF — 10x faster than XML parser
# MAGIC - One folder at a time: checkpoint after each, progress after each
# MAGIC - Sorted small-first so we see output within the first minute

# COMMAND ----------

import time
import gzip
import re
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

OUTPUT_TABLE = "openalex.repo.native_id_to_repo_id"
S3_BASE = "s3a://openalex-ingest/repositories"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: List all repository folders and sort small-first

# COMMAND ----------

entries = dbutils.fs.ls(f"{S3_BASE}/")
repo_ids = [e.name.rstrip("/") for e in entries if e.name.rstrip("/")]
print(f"Found {len(repo_ids)} repository folders")

# Sort by folder size (small first) so we get fast initial feedback
folder_sizes = []
for e in entries:
    name = e.name.rstrip("/")
    if name:
        folder_sizes.append((name, e.size))
folder_sizes.sort(key=lambda x: x[1])
repo_ids = [name for name, _ in folder_sizes]
print(f"Sorted small-first. Smallest: {folder_sizes[0] if folder_sizes else 'none'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Resume support — skip already-written folders

# COMMAND ----------

try:
    done_ids = set(
        row.repository_id for row in
        spark.sql(f"SELECT DISTINCT repository_id FROM {OUTPUT_TABLE}").collect()
    )
    print(f"Already written: {len(done_ids)} repository_ids — resuming")
except Exception:
    done_ids = set()
    print("Output table doesn't exist yet — starting fresh")

remaining = [rid for rid in repo_ids if rid not in done_ids]
print(f"Remaining: {len(remaining)} to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: UDF to extract native_ids from gzipped XML via regex

# COMMAND ----------

@F.udf(ArrayType(StringType()))
def extract_native_ids(content):
    """Extract <ns0:identifier> values from gzipped XML bytes. No XML parsing needed."""
    if content is None:
        return []
    try:
        text = gzip.decompress(bytes(content)).decode("utf-8", errors="replace")
        return re.findall(r"<(?:ns0:)?identifier>([^<]+)</(?:ns0:)?identifier>", text)
    except Exception:
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Process per-folder with progress

# COMMAND ----------

total_rows = 0
job_start = time.time()
errors = []
batch_times = []  # for rolling rate

for i, repo_id in enumerate(remaining, 1):
    folder_start = time.time()
    s3_path = f"{S3_BASE}/{repo_id}/"

    try:
        ids_df = (spark.read
            .format("binaryFile")
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.xml.gz")
            .load(s3_path)
            .select(F.explode(extract_native_ids(F.col("content"))).alias("native_id"))
            .withColumn("repository_id", F.lit(repo_id))
            .dropDuplicates(["native_id"])
        )

        ids_df.write.mode("append").saveAsTable(OUTPUT_TABLE)

        # Count what we wrote (fast — single partition)
        folder_count = spark.sql(
            f"SELECT COUNT(*) FROM {OUTPUT_TABLE} WHERE repository_id = '{repo_id}'"
        ).collect()[0][0]

    except Exception as e:
        folder_elapsed = time.time() - folder_start
        errors.append((repo_id, str(e)[:200]))
        print(f"[{i}/{len(remaining)}] {repo_id}: ERROR ({folder_elapsed:.0f}s) — {str(e)[:150]}")
        continue

    total_rows += folder_count
    folder_elapsed = time.time() - folder_start
    elapsed = time.time() - job_start

    # Rolling rate (last 20 folders)
    batch_times.append((folder_elapsed, folder_count))
    if len(batch_times) > 20:
        batch_times.pop(0)
    recent_time = sum(t for t, _ in batch_times)
    recent_rows = sum(r for _, r in batch_times)
    rate = recent_rows / recent_time if recent_time > 0 else 0

    remaining_count = len(remaining) - i
    avg_folder_time = elapsed / i
    eta_seconds = avg_folder_time * remaining_count
    eta_h, eta_rem = divmod(int(eta_seconds), 3600)
    eta_m, _ = divmod(eta_rem, 60)

    print(
        f"[{i}/{len(remaining)}] {repo_id}: "
        f"+{folder_count:,} rows ({folder_elapsed:.0f}s) | "
        f"Total: {total_rows:,} | "
        f"Rate: {rate:,.0f} rows/s | "
        f"ETA: {eta_h}h{eta_m:02d}m | "
        f"Elapsed: {int(elapsed)}s"
    )

elapsed = time.time() - job_start
print(f"\nDone! {total_rows:,} rows across {len(remaining) - len(errors)} folders in {int(elapsed)}s")
if errors:
    print(f"\n{len(errors)} folders failed:")
    for rid, err in errors[:20]:
        print(f"  {rid}: {err}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_rows, COUNT(DISTINCT repository_id) as repo_ids
# MAGIC FROM openalex.repo.native_id_to_repo_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) as total_null_in_repo_works,
# MAGIC   SUM(CASE WHEN m.repository_id IS NOT NULL THEN 1 ELSE 0 END) as fixable,
# MAGIC   ROUND(SUM(CASE WHEN m.repository_id IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as pct_fixable
# MAGIC FROM openalex.repo.repo_works rw
# MAGIC LEFT JOIN openalex.repo.native_id_to_repo_id m ON rw.native_id = m.native_id
# MAGIC WHERE rw.repository_id IS NULL
