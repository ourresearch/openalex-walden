# Databricks notebook source
# MAGIC %md
# MAGIC # #981 gzip rescue (one-off, branch only; Casey's go 21 Nov)
# MAGIC
# MAGIC ~3,000 linked "PDFs" are gzip-compressed (the Feb 2025 harvester stored the compressed body). Each is now excluded.
# MAGIC For each: download, gunzip, keep it only if the result is a whole PDF (`%PDF-` head, `%%EOF` in the tail, not valid
# MAGIC UTF-8), store it like every writer does (`utils/pdf_store.store_pdf`, reuses bytes we already hold) and write the
# MAGIC `taxicab_results` row the TaxiCab notebook would (url = the record's URL), so Parse PDFs links it to the record.
# MAGIC Revert: DELETE the taxicab_results rows with resolved_url LIKE 'oxjob981-gzip-rescue:%' before Parse PDFs.

# COMMAND ----------

import datetime, gzip, json, os, sys, zlib
import boto3
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote
from pyspark.sql import types as T

dbutils.widgets.text("dry_run", "true")
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"
TARGETS = json.load(open("/Volumes/openalex/oxjobs/data/oxjob981/gzip_targets.json"))
s3 = boto3.client("s3", region_name="auto", endpoint_url=dbutils.secrets.get("cloudflare", "r2-endpoint"),
                  aws_access_key_id=dbutils.secrets.get("cloudflare", "r2-access-key-id"),
                  aws_secret_access_key=dbutils.secrets.get("cloudflare", "r2-secret-access-key"),
                  config=Config(max_pool_connections=16, retries={"max_attempts": 6}))
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from utils.pdf_store import md5_table, store_pdf  # noqa: E402
table = md5_table(dbutils)


def whole_pdf(b):
    if not b[:1024].lstrip().startswith(b"%PDF-"):
        return "not_pdf"
    if b"%%EOF" not in b[-4096:]:
        return "no_eof"
    try:
        b.decode("utf-8"); return "utf8_decoded"
    except UnicodeDecodeError:
        return "ok"


def one(t):
    r = {"old_id": t["id"], "url": t["url"]}
    try:
        raw = s3.get_object(Bucket="openalex-pdfs", Key=f"{t['id']}.pdf")["Body"].read()
        try:
            body = gzip.decompress(raw)
        except Exception:
            body = zlib.decompress(raw, 16 + zlib.MAX_WBITS)
        r["verdict"] = whole_pdf(body)
        r["bytes"] = len(body)
        if r["verdict"] == "ok" and not DRY_RUN:
            now = datetime.datetime.utcnow()
            new_id, reused = store_pdf(s3, table, body, {
                "url": quote(t["url"]), "resolved_url": quote(t["url"]), "created_date": now.isoformat(),
                "content_type": "pdf", "native_id": t["native_id"] or "", "native_id_namespace": t["native_id_namespace"] or "",
                "source": "oxjob981_gzip_rescue"})
            r.update(new_id=new_id, reused=reused,
                     row=(new_id, t["url"], f"oxjob981-gzip-rescue:{t['id']}", 200, "pdf", t["native_id"], t["native_id_namespace"],
                          f"s3://openalex-pdfs/{new_id}.pdf", False, now, now, None))
    except Exception as e:
        r["verdict"] = f"error {type(e).__name__}"
    return r


with ThreadPoolExecutor(16) as ex:
    res = list(ex.map(one, TARGETS))
from collections import Counter
rows = [r.pop("row") for r in res if "row" in r]
schema = T.StructType([
    T.StructField("taxicab_id", T.StringType()), T.StructField("url", T.StringType()),
    T.StructField("resolved_url", T.StringType()), T.StructField("status_code", T.IntegerType()),
    T.StructField("content_type", T.StringType()), T.StructField("native_id", T.StringType()),
    T.StructField("native_id_namespace", T.StringType()), T.StructField("s3_path", T.StringType()),
    T.StructField("is_soft_block", T.BooleanType()), T.StructField("created_date", T.TimestampType()),
    T.StructField("processed_date", T.TimestampType()), T.StructField("error", T.StringType()),
])
if rows and not DRY_RUN:
    spark.createDataFrame(rows, schema).write.mode("append").format("delta").saveAsTable("openalex.taxicab.taxicab_results")
with open("/Volumes/openalex/oxjobs/data/oxjob981/gzip_rescue_result.json", "w") as f:
    json.dump(res, f)
dbutils.notebook.exit(json.dumps({"dry_run": DRY_RUN, "verdicts": dict(Counter(r["verdict"] for r in res)), "registered": len(rows)}))
