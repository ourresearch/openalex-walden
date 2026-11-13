# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5: load a Modal GPU batch's output from S3 into a Delta table (scripts/modal_s3_batch)
# MAGIC Reads `<s3>/out/shard=S/part-0.parquet` a batch (`batch_size` shards) at a time, checks it against the source table
# MAGIC (same row count, no duplicate key, no bad value), then appends it with an idempotent write id
# MAGIC (`txnAppId = <write_id_prefix><batch>`, `txnVersion = 1`): a batch already committed under that id is skipped by Delta,
# MAGIC never duplicated, so a rerun, or a second route writing the same batches with the same ids, is safe. A progress row per
# MAGIC batch records who loaded it. Runbook: docs/runbooks/modal-gpu-batch-via-s3.md

# COMMAND ----------

dbutils.widgets.text("source", "")          # table with the key and an INT shard column (what was exported)
dbutils.widgets.text("key", "text_hash")
dbutils.widgets.text("s3", "")              # s3://bucket/prefix
dbutils.widgets.text("dest", "")            # Delta table to append to (must exist)
dbutils.widgets.text("select", "*")         # columns for dest, e.g. "CAST(text_hash AS BIGINT) AS text_hash, text, v, current_timestamp() AS embedded_at"
dbutils.widgets.text("bad", "false")        # a row is bad if this is true, e.g. "v IS NULL OR length(v) != 2048"
dbutils.widgets.text("batch_size", "8")
dbutils.widgets.text("batches", "0-31")
dbutils.widgets.text("write_id_prefix", "") # e.g. "oxjob2000_refemb_b"; one id per batch, never reused for other data
dbutils.widgets.text("progress", "")        # progress table, created if missing
dbutils.widgets.text("run", "modal")
dbutils.widgets.text("wait_minutes", "0")   # >0: keep polling for batches Modal has not finished yet
import json
import time

W = {k: dbutils.widgets.get(k) for k in ("source", "key", "s3", "dest", "select", "bad", "batch_size", "batches", "write_id_prefix",
                                          "progress", "run", "wait_minutes")}
assert W["source"] and W["s3"] and W["dest"] and W["write_id_prefix"] and W["progress"], "set source, s3, dest, write_id_prefix, progress"
B = int(W["batch_size"])
spark.sql(f"""CREATE TABLE IF NOT EXISTS {W['progress']} (batch INT, shard_from INT, shard_to INT, rows BIGINT, seconds DOUBLE,
  run STRING, cluster STRING, ts TIMESTAMP)""")
cluster = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "?")


def parse_ranges(arg):
    out = []
    for part in arg.split(","):
        a, _, b = part.partition("-")
        out += list(range(int(a), int(b or a) + 1))
    return out


result = {"loaded": [], "already": [], "not_ready": [], "bad": []}


def load_one(b):
    if spark.sql(f"SELECT 1 FROM {W['progress']} WHERE batch = {b} LIMIT 1").count():
        return "already"
    s0, s1 = B * b, B * b + B
    paths = [f"{W['s3']}/out/shard={s}/part-0.parquet" for s in range(s0, s1)]
    try:
        for p in paths:
            dbutils.fs.ls(p)
    except Exception:
        return "not_ready"
    t = time.time()
    df = spark.read.parquet(*paths)
    st = df.selectExpr("count(*) n", f"count(DISTINCT {W['key']}) d", f"count_if({W['bad']}) bad").first()
    want = spark.table(W["source"]).where(f"shard >= {s0} AND shard < {s1}").count()
    if st.n != want or st.d != want or st.bad:
        result["bad"].append({"batch": b, "rows": st.n, "distinct": st.d, "bad": st.bad, "source_rows": want})
        return "bad"
    df.createOrReplaceTempView("modal_out")
    (spark.sql(f"SELECT {W['select']} FROM modal_out")
     .write.format("delta").mode("append").option("txnAppId", f"{W['write_id_prefix']}{b}").option("txnVersion", 1)
     .saveAsTable(W["dest"]))
    spark.sql(f"INSERT INTO {W['progress']} VALUES ({b}, {s0}, {s1}, {want}, {time.time() - t}, '{W['run']}', '{cluster}', current_timestamp())")
    return "loaded"


deadline = time.time() + 60 * float(W["wait_minutes"])
pending = parse_ranges(W["batches"])
while pending:
    left = []
    for b in pending:
        r = load_one(b)
        if r == "not_ready":
            left.append(b)
        elif r != "bad":
            result[r].append(b)
    pending = left
    if not pending or time.time() > deadline:
        break
    time.sleep(60)
result["not_ready"] = pending
if result["bad"]:
    raise RuntimeError(f"batches failed their checks (nothing written for them): {json.dumps(result['bad'])}")
dbutils.notebook.exit(json.dumps(result))
