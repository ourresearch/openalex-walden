# Databricks notebook source
# MAGIC %pip install /Volumes/openalex/default/libraries/openalex_dlt_utils-0.3.34-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC # PMC open data feed (oxjob #1793)
# MAGIC
# MAGIC Reads PubMed Central's public bucket `s3://pmc-oa-opendata` (NLM; ~8M article versions, us-east-1, unsigned)
# MAGIC instead of scraping PMC pages. The bucket's daily S3 inventory (`inventory-reports/.../metadata/<date>/`) lists
# MAGIC every `metadata/PMC<id>.<v>.json` with its ETag; a key whose (key, ETag) we haven't read yet is fetched:
# MAGIC its metadata JSON, then the article's JATS up to `</front>` (`utils/pmc_jats.py`).
# MAGIC
# MAGIC Writes ONLY its own tables in `openalex.pubmed_central` (nothing else reads them until the merge step in
# MAGIC `CreateCrossrefSuperAuthorships` ships):
# MAGIC - `pmc_oa_inventory`: the newest inventory (overwritten each run).
# MAGIC - `pmc_oa_articles`: one row per article version read (append-only; newest `fetched_at` per key wins).
# MAGIC - `pmc_oa_progress`: one row per finished chunk (restart-safe: a rerun anti-joins on (key, etag, parser_version)).
# MAGIC - `pmc_oa_authors`: one row per DOI, the authors of its newest readable version with Walden's `author_key`
# MAGIC   (`udf_last_name_only`), shaped like `locations_parsed.authors` (rebuilt each run).
# MAGIC
# MAGIC Why authors only (EXPLORE § 1-3 of #1793): the bucket adds a corresponding author to ~1.2M works (its flags
# MAGIC agree with ours 99.1%); abstracts, licences and author lists add ~0.
# MAGIC
# MAGIC Batch-job rules: progress per chunk with rate and ETA, errors kept as rows (`error` column), append per chunk,
# MAGIC checkpoint per chunk, stops starting chunks after `max_minutes`. Credit: data from PMC (NLM).

# COMMAND ----------

import gzip
import io
import json
import math
import os
import re
import sys
import time

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "pmc_jats.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "pmc_jats.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils import pmc_jats  # noqa: E402

import pandas as pd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (ArrayType, BooleanType, IntegerType, StringType, StructField,  # noqa: E402
                               StructType, TimestampType)

dbutils.widgets.text("schema", "openalex.pubmed_central", "target schema")
dbutils.widgets.text("inventory_date", "", "inventory folder (e.g. 2026-10-25T01-00Z); empty = newest")
dbutils.widgets.text("only_pmcids_sql", "", "optional SQL returning a `pmcid` column ('PMC123'): read only these (pilots, priority backfills)")
dbutils.widgets.text("max_articles", "0", "stop after this many article versions this run (0 = no cap)")
dbutils.widgets.text("max_minutes", "170", "stop starting new chunks after this long")
dbutils.widgets.text("chunk_size", "200000", "article versions per chunk (one append + one progress row each)")
dbutils.widgets.text("threads", "16", "HTTP threads per Spark task")
dbutils.widgets.text("rows_per_task", "2000", "article versions per Spark task")
dbutils.widgets.text("dry_run", "false", "true = refresh the inventory and report the to-do count, fetch nothing")

SCHEMA = dbutils.widgets.get("schema").strip()
INVENTORY_DATE = dbutils.widgets.get("inventory_date").strip()
ONLY_SQL = dbutils.widgets.get("only_pmcids_sql").strip()
MAX_ARTICLES = int(dbutils.widgets.get("max_articles"))
MAX_MINUTES = float(dbutils.widgets.get("max_minutes"))
CHUNK = int(dbutils.widgets.get("chunk_size"))
THREADS = int(dbutils.widgets.get("threads"))
ROWS_PER_TASK = int(dbutils.widgets.get("rows_per_task"))
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"

INVENTORY = f"{SCHEMA}.pmc_oa_inventory"
ARTICLES = f"{SCHEMA}.pmc_oa_articles"
PROGRESS = f"{SCHEMA}.pmc_oa_progress"
AUTHORS = f"{SCHEMA}.pmc_oa_authors"
PARSER_VERSION = pmc_jats.PARSER_VERSION
BUCKET = pmc_jats.BUCKET_HTTPS
T0 = time.time()

AUTHOR_TYPE = StructType([
    StructField("given", StringType()), StructField("family", StringType()), StructField("name", StringType()),
    StructField("orcid", StringType()), StructField("affiliations", ArrayType(StringType())),
    StructField("is_corresponding", BooleanType()),
])
ARTICLE_SCHEMA = StructType([
    StructField("key", StringType()), StructField("etag", StringType()), StructField("last_modified", StringType()),
    StructField("pmcid", StringType()), StructField("version", IntegerType()), StructField("pmid", StringType()),
    StructField("doi", StringType()), StructField("title", StringType()),
    StructField("is_pmc_openaccess", BooleanType()), StructField("is_manuscript", BooleanType()),
    StructField("is_retracted", BooleanType()), StructField("is_historical_ocr", BooleanType()),
    StructField("license_code", StringType()), StructField("xml_url", StringType()), StructField("pdf_url", StringType()),
    StructField("article_type", StringType()), StructField("has_abstract", BooleanType()),
    StructField("corresp_notes", BooleanType()), StructField("n_affs", IntegerType()),
    StructField("authors", ArrayType(AUTHOR_TYPE)),
    StructField("parser_version", StringType()), StructField("fetched_at", TimestampType()),
    StructField("error", StringType()),
])

spark.sql(f"""CREATE TABLE IF NOT EXISTS {ARTICLES} ({', '.join(f'`{f.name}` {f.dataType.simpleString()}' for f in ARTICLE_SCHEMA.fields)})
USING DELTA COMMENT 'PMC open-data article versions read from s3://pmc-oa-opendata (oxjob #1793). Append-only: newest fetched_at per key wins. Data: PMC (NLM).'""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {PROGRESS} (
  run_id STRING, chunk_no INT, n_todo INT, n_ok INT, n_failed INT, seconds DOUBLE, parser_version STRING, done_at TIMESTAMP
) USING DELTA COMMENT 'One row per finished chunk of PmcOpenData (oxjob #1793)'""")

# COMMAND ----------

# The newest S3 inventory of metadata keys (CSV.gz: bucket, key, last_modified, etag).

def http_get(url):
    return pmc_jats.http_get(url, timeout=60)


prefix = "inventory-reports/pmc-oa-opendata/metadata/"
if not INVENTORY_DATE:
    listing = http_get(f"{BUCKET}/?list-type=2&delimiter=/&prefix={prefix}&max-keys=1000").decode()
    dates = sorted(p for p in re.findall(r"<Prefix>" + re.escape(prefix) + r"([0-9T\-Z]+)/</Prefix>", listing))
    assert dates, "no inventory folders found"
    INVENTORY_DATE = dates[-1]
manifest = json.loads(http_get(f"{BUCKET}/{prefix}{INVENTORY_DATE}/manifest.json"))
assert manifest.get("fileSchema", "").replace(" ", "").lower() == "bucket,key,lastmodifieddate,etag", manifest.get("fileSchema")
frames = []
for f in manifest["files"]:
    raw = gzip.decompress(http_get(f"{BUCKET}/{f['key']}"))
    frames.append(pd.read_csv(io.BytesIO(raw), header=None, names=["bucket", "key", "last_modified", "etag"],
                              dtype=str, keep_default_na=False))
inv = pd.concat(frames, ignore_index=True)
inv = inv[inv["key"].str.match(r"^metadata/PMC\d+\.\d+\.json$")]
inv["pmcid"] = inv["key"].str.extract(r"^metadata/(PMC\d+)\.")[0]
inv["version"] = inv["key"].str.extract(r"\.(\d+)\.json$")[0].astype(int)
inv["inventory_date"] = INVENTORY_DATE
inv = inv.drop(columns=["bucket"])
print(f"inventory {INVENTORY_DATE}: {len(inv):,} article versions, {inv['pmcid'].nunique():,} PMCIDs "
      f"({len(manifest['files'])} files, {time.time() - T0:.0f}s)")
spark.createDataFrame(inv).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(INVENTORY)
del inv, frames

# COMMAND ----------

# To do: inventory keys whose (key, etag) has no successful read at this parser version.
done = spark.sql(f"""SELECT key, etag FROM {ARTICLES} WHERE error IS NULL AND parser_version = '{PARSER_VERSION}'""")
todo = spark.table(INVENTORY).join(done, ["key", "etag"], "left_anti")
if ONLY_SQL:
    todo = todo.join(spark.sql(ONLY_SQL).select("pmcid").distinct(), "pmcid", "left_semi")
todo = todo.select("key", "etag", "last_modified", "pmcid", "version")
if MAX_ARTICLES:
    todo = todo.limit(MAX_ARTICLES)
todo = todo.cache()
n_todo = todo.count()
n_chunks = math.ceil(n_todo / CHUNK)
print(f"to do: {n_todo:,} article versions in {n_chunks} chunks of <= {CHUNK:,}"
      + (f" (restricted by only_pmcids_sql)" if ONLY_SQL else ""))
if DRY_RUN or n_todo == 0:
    dbutils.notebook.exit(f"dry run: {n_todo} to do" if DRY_RUN else "nothing to do")

# COMMAND ----------

def read_partition(batches):
    """Runs on executors: metadata JSON + JATS front for each key, THREADS at a time. Every failure is a row."""
    import datetime as _dt
    import json as _json
    import sys as _sys
    from concurrent.futures import ThreadPoolExecutor
    if REPO_ROOT not in _sys.path:
        _sys.path.insert(0, REPO_ROOT)
    from utils import pmc_jats as _pj

    cols = [f.name for f in ARTICLE_SCHEMA.fields]

    def one(rec):
        row = {"key": rec["key"], "etag": rec["etag"], "last_modified": rec["last_modified"], "pmcid": rec["pmcid"],
               "version": int(rec["version"]), "parser_version": _pj.PARSER_VERSION,
               "fetched_at": _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)}
        try:
            md = _json.loads(_pj.http_get(f"{_pj.BUCKET_HTTPS}/{rec['key']}"))
            row.update(pmid=str(md["pmid"]) if md.get("pmid") else None, doi=md.get("doi") or None, title=(md.get("title") or "")[:2000] or None,
                       is_pmc_openaccess=md.get("is_pmc_openaccess"), is_manuscript=md.get("is_manuscript"),
                       is_retracted=md.get("is_retracted"), is_historical_ocr=md.get("is_historical_ocr"),
                       license_code=md.get("license_code"), xml_url=md.get("xml_url"), pdf_url=md.get("pdf_url"))
            if md.get("xml_url"):
                parsed = _pj.parse_front(_pj.fetch_front(_pj.https_url(md["xml_url"])))
                row.update(article_type=parsed["article_type"], has_abstract=parsed["has_abstract"],
                           corresp_notes=parsed["corresp_notes"], n_affs=parsed["n_affs"], authors=parsed["authors"])
        except Exception as e:  # noqa: BLE001 - kept as a row, retried next run
            row["error"] = repr(e)[:500]
        return row

    with ThreadPoolExecutor(THREADS) as pool:
        for pdf in batches:
            rows = list(pool.map(one, pdf.to_dict("records")))
            # missing keys and NaN -> None: Spark's array converter can't iterate a NaN (a row without JATS)
            out = pd.DataFrame(rows, columns=cols).astype(object).where(lambda d: d.notna(), None)
            out["version"] = out["version"].astype("int32")
            out["n_affs"] = out["n_affs"].astype("Int32")
            yield out


run_id = f"{time.strftime('%Y%m%dT%H%M%S')}-{INVENTORY_DATE}"
todo = todo.withColumn("_chunk", F.pmod(F.xxhash64("key"), F.lit(n_chunks)))
n_tasks_min = spark.sparkContext.defaultParallelism
total_ok = total_failed = 0
for chunk_no in range(n_chunks):
    if (time.time() - T0) / 60 > MAX_MINUTES:
        print(f"stopping before chunk {chunk_no}: {MAX_MINUTES} minutes used")
        break
    t = time.time()
    part = todo.filter(F.col("_chunk") == chunk_no).drop("_chunk")
    n_part = part.count()
    out = (part.repartition(max(n_tasks_min, n_part // ROWS_PER_TASK))
           .mapInPandas(read_partition, schema=ARTICLE_SCHEMA))
    out.write.mode("append").saveAsTable(ARTICLES)
    # this chunk's rows: fetched since it began, same key hash (never re-runs the to-do plan, which the write changed)
    stats = (spark.table(ARTICLES)
             .filter(F.col("fetched_at") >= F.lit(time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(t - 5))).cast("timestamp"))
             .filter(F.pmod(F.xxhash64("key"), F.lit(n_chunks)) == chunk_no)
             .agg(F.count_if(F.col("error").isNull()).alias("ok"), F.count_if(F.col("error").isNotNull()).alias("failed"))
             .first())
    ok, failed = int(stats.ok), int(stats.failed)
    total_ok += ok
    total_failed += failed
    secs = time.time() - t
    spark.createDataFrame([(run_id, chunk_no, n_part, ok, failed, secs, PARSER_VERSION)],
                          "run_id string, chunk_no int, n_todo int, n_ok int, n_failed int, seconds double, parser_version string") \
        .withColumn("done_at", F.current_timestamp()).write.mode("append").saveAsTable(PROGRESS)
    rate = (total_ok + total_failed) / max(1.0, time.time() - T0)
    left = n_todo - total_ok - total_failed
    print(f"chunk {chunk_no + 1}/{n_chunks}: {ok:,} ok, {failed:,} failed in {secs:.0f}s "
          f"({n_part / max(secs, 1):.0f}/s); run total {total_ok:,} ok / {total_failed:,} failed; "
          f"ETA {left / max(rate, 1e-9) / 60:.0f} min")
    # Systemic failures only: a few dozen versions whose metadata names an XML the bucket lacks (404) stay error rows,
    # retried each run, and are often a day's whole to-do.
    if n_part >= 1000 and failed > 0.2 * n_part:
        raise RuntimeError(f"chunk {chunk_no}: {failed}/{n_part} failed; first errors in {ARTICLES} (error IS NOT NULL)")

# COMMAND ----------

# pmc_oa_authors: one row per DOI from its newest readable version; authors shaped like locations_parsed.authors,
# author_key from the same UDF every Walden source uses, so CreateCrossrefSuperAuthorships can match on it.
from openalex.dlt.normalize import normalize_doi_spark_col, udf_last_name_only  # noqa: E402

latest = spark.sql(f"""
  SELECT * FROM {ARTICLES}
  WHERE error IS NULL AND doi IS NOT NULL AND size(authors) > 0 AND NOT coalesce(is_retracted, false)
  QUALIFY row_number() OVER (PARTITION BY key ORDER BY fetched_at DESC) = 1
""").withColumn("doi", normalize_doi_spark_col(F.col("doi"))).filter("doi != ''")
per_doi = (latest
           .withColumn("_rank", F.expr("""row_number() OVER (PARTITION BY doi ORDER BY
                coalesce(is_manuscript, false) ASC, version DESC, pmcid DESC)"""))
           .filter("_rank = 1"))
authors_df = per_doi.select(
    "doi", "pmcid", "version", "is_manuscript", "license_code",
    F.expr("""transform(authors, a -> named_struct(
        'given', a.given, 'family', a.family, 'name', a.name, 'orcid', a.orcid,
        'affiliations', transform(a.affiliations, x -> named_struct('name', x, 'department', CAST(NULL AS STRING), 'ror_id', CAST(NULL AS STRING))),
        'is_corresponding', a.is_corresponding))""").alias("authors"),
    "fetched_at",
).withColumn("authors", udf_last_name_only(F.col("authors"))).withColumns({
    "authors_exist": F.expr("concat_ws('', authors[0].given, authors[0].family, authors[0].name) != ''"),
    "affiliations_exist": F.expr("exists(authors, a -> nvl(size(a.affiliations), 0) > 0)"),
    "is_corresponding_exists": F.expr("exists(authors, a -> a.is_corresponding = true)"),
})
authors_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(AUTHORS)
s = spark.sql(f"""SELECT count(*) n, count_if(is_corresponding_exists) corr, count_if(affiliations_exist) aff FROM {AUTHORS}""").first()
print(f"{AUTHORS}: {s.n:,} DOIs, {s.corr:,} with a corresponding author, {s.aff:,} with affiliations "
      f"({(time.time() - T0) / 60:.0f} min total)")
