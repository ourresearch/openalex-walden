# Databricks notebook source
# MAGIC %md
# MAGIC # Build the related-works vector table (one-time, oxjob #1648)
# MAGIC
# MAGIC Writes `openalex.vector_search.work_vectors_ivf`: every work's Qwen3 vector as float16 (2 KB), tagged with its IVF list
# MAGIC (131,072 lists from the 27 Oct GPU backfill, spherical k-means on the same vectors), partitioned by list group
# MAGIC (`lg = list_id DIV 128`, 1,024 groups) and sorted by list inside each file. `RelatedWorksIncremental` scans this table
# MAGIC each night to find the nearest works for new and re-embedded works, and appends their vectors to it.
# MAGIC
# MAGIC Input: the backfill export `openalex.transient.oxjob1648_all` (460,789,597 works, vectors as of 2026-10-27 16:20 UTC)
# MAGIC and the backfill's saved clustering in S3 (`centroids.npy`, and `assign.npy` = list per row, by row position in the
# MAGIC export: files in path order, rows in file order). Nothing here recomputes the clustering.
# MAGIC
# MAGIC Re-running replaces the table (CREATE OR REPLACE): only do that for a full rebuild, then re-run the nightly.

# COMMAND ----------

dbutils.widgets.text("export_table", "openalex.transient.oxjob1648_all")
dbutils.widgets.text("clustering_s3", "s3://openalex-sandbox/oxjob1648/out/r1/")
dbutils.widgets.text("volume", "/Volumes/openalex/vector_search/related_works_ivf")
dbutils.widgets.text("target", "openalex.vector_search.work_vectors_ivf")
dbutils.widgets.text("vec_at", "2026-10-27 16:20:00")
dbutils.widgets.text("check_rows", "200000")

EXPORT = dbutils.widgets.get("export_table")
CLUS = dbutils.widgets.get("clustering_s3").rstrip("/") + "/"
VOL = dbutils.widgets.get("volume").rstrip("/")
TARGET = dbutils.widgets.get("target")
VEC_AT = dbutils.widgets.get("vec_at")
CHECK_ROWS = int(dbutils.widgets.get("check_rows"))
NLIST, LISTS_PER_GROUP, D = 131072, 128, 1024

# COMMAND ----------

# 1. The saved clustering into the volume (the nightly reads centroids.npy from there).
import os, time
import numpy as np

spark.sql("CREATE VOLUME IF NOT EXISTS openalex.vector_search.related_works_ivf COMMENT 'oxjob #1648: IVF clustering + nightly scratch for related works'")
for f in ("centroids.npy", "assign.npy"):
    if not os.path.exists(f"{VOL}/{f}"):
        t = time.time()
        dbutils.fs.cp(CLUS + f, f"{VOL}/{f}")
        print(f"copied {f} in {time.time() - t:.0f} s")
cent = np.load(f"{VOL}/centroids.npy")
assign = np.load(f"{VOL}/assign.npy", mmap_mode="r")
assert cent.shape == (NLIST, D), cent.shape
print("centroids", cent.shape, "assign", assign.shape, "norms", np.linalg.norm(cent[:5], axis=1))

# COMMAND ----------

# 2. Row positions exactly as the GPU run counted them: live files sorted by path, rows in file order.
from pyspark.sql import functions as F

hist = spark.sql(f"DESCRIBE HISTORY {EXPORT}").select("version").collect()
assert [r.version for r in hist] == [0], f"{EXPORT} changed since the GPU run (versions {hist}): positions would not match"
per_file = (spark.read.table(EXPORT).select(F.col("_metadata.file_path").alias("file_path"))
            .groupBy("file_path").count().collect())
paths = sorted(per_file, key=lambda r: r.file_path.rsplit("/", 1)[1])
offs, a = {}, 0
for r in paths:
    offs[r.file_path] = a
    a += r["count"]
assert a == assign.shape[0], f"{a} rows vs {assign.shape[0]} positions"
print(f"{len(paths)} files, {a:,} rows")
offs_df = spark.createDataFrame([(k, v) for k, v in offs.items()], "file_path string, off bigint")

# COMMAND ----------

# 3. Attach list ids (assign[pos]) on the executors, no join: the volume file is memory-mapped by each Python worker.
import pyarrow as pa

ASSIGN_PATH = f"{VOL}/assign.npy"

def attach_list(batches):
    import numpy as np, pyarrow as pa
    asg = np.load(ASSIGN_PATH, mmap_mode="r")
    for b in batches:
        pos = b.column("pos").to_numpy()
        lid = np.asarray(asg[pos], dtype=np.int32)
        yield pa.RecordBatch.from_arrays(
            [pa.array(lid // 128, pa.int32()), pa.array(lid, pa.int32()), b.column("work_id"), b.column("v"),
             b.column("type"), b.column("is_retracted")],
            names=["lg", "list_id", "work_id", "v", "type", "is_retracted"])

out_schema = "lg int, list_id int, work_id bigint, v binary, type string, is_retracted boolean"
src = (spark.read.table(EXPORT)
       .select("work_id", "v", "type", "is_retracted",
               F.col("_metadata.file_path").alias("file_path"), F.col("_metadata.row_index").alias("row_index"))
       .join(F.broadcast(offs_df), "file_path")
       .select((F.col("off") + F.col("row_index")).alias("pos"), "work_id", "v", "type", "is_retracted"))
tagged = src.mapInArrow(attach_list, out_schema)

# COMMAND ----------

# 4. Check the mapping before writing: each sampled vector's list must be its nearest centroid (the GPU run assigned with
#    fp16 copies of the centroids, so a few near-ties may differ; a position slip would put ~0% on top).
#    Only 24 files spread over the position range are read (file pruning on _metadata.file_path), not the whole export.
check_files = [paths[i].file_path for i in np.linspace(0, len(paths) - 1, 24).astype(int)]
n_check = sum(r["count"] for r in paths if r.file_path in set(check_files))
sample = (spark.read.table(EXPORT)
          .select("work_id", "v", "type", "is_retracted",
                  F.col("_metadata.file_path").alias("file_path"), F.col("_metadata.row_index").alias("row_index"))
          .filter(F.col("file_path").isin(check_files))
          .join(F.broadcast(offs_df), "file_path")
          .select((F.col("off") + F.col("row_index")).alias("pos"), "work_id", "v", "type", "is_retracted")
          .mapInArrow(attach_list, out_schema)
          .sample(fraction=min(1.0, CHECK_ROWS / n_check), seed=1648).select("list_id", "v").toPandas())
V = np.frombuffer(b"".join(sample.v.values), dtype=np.float16).reshape(-1, D).astype(np.float32)
top = np.concatenate([np.argmax(V[i:i + 4096] @ cent.T, axis=1) for i in range(0, len(V), 4096)])
agree = float((top == sample.list_id.values).mean())
print(f"mapping check: {len(V):,} sampled rows, assigned list = nearest centroid on {agree:.4%}")
assert agree >= 0.98, "positions do not line up with assign.npy: STOP"

# COMMAND ----------

# 5. Write: one shuffle by list group, sorted by list inside each file.
t = time.time()
(tagged.withColumn("vec_at", F.lit(VEC_AT).cast("timestamp"))
 .repartition(1024, "lg").sortWithinPartitions("lg", "list_id")
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("lg")
 .saveAsTable(TARGET))
spark.sql(f"""COMMENT ON TABLE {TARGET} IS 'oxjob #1648: every work''s Qwen3 vector (float16, 2 KB) with its IVF list (131,072 lists,
lg = list_id DIV 128). Built by BuildRelatedWorksIvf from the 27 Oct backfill export; RelatedWorksIncremental appends new and
re-embedded works each night (a re-embedded work can have more than one row: vec_at says which is newest).'""")
print(f"written in {(time.time() - t) / 60:.1f} min")
r = spark.sql(f"""SELECT COUNT(*) n, COUNT(DISTINCT work_id) d, COUNT(DISTINCT list_id) lists, MIN(list_id) lo, MAX(list_id) hi,
  COUNT_IF(lg <> list_id DIV 128) bad_lg, COUNT_IF(length(v) <> 2048) bad_v FROM {TARGET}""").first()
print(r)
assert r.n == a and r.d == a and r.bad_lg == 0 and r.bad_v == 0 and r.lists == NLIST
