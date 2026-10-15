# Databricks notebook source
# MAGIC %md
# MAGIC # Build sitemaps for openalex.org record pages (oxjob #1689)
# MAGIC
# MAGIC Nightly, after the End 2 End publish path (ES sync) so every listed page exists.
# MAGIC
# MAGIC 1. **Records**: every indexable record of each entity, by THE quality rule in `utils/sitemaps.py`
# MAGIC    (the same rule as the website's `noindex`, openalex-gui `src/seo/indexable.mjs`), with its group and
# MAGIC    a fingerprint of what its page shows.
# MAGIC 2. **lastmod**: joined to last night's state (`openalex.sitemaps.page_state`): the true time the page's
# MAGIC    content last changed (rule in `utils/sitemaps.py`), never the build time. Written to `<state>_next`.
# MAGIC 3. **Files**: groups packed into files of <= 50,000 URLs by id; a rolling `changed` group (new records first,
# MAGIC    then the newest changes, last `changed_days`, capped). Executors write gzipped files to the bucket's
# MAGIC    `runs/<run_id>/` prefix.
# MAGIC 4. **Checks**: counts per group equal the state table's, every file within the limits. Any problem
# MAGIC    stops the run here: nothing under `live/` has changed yet.
# MAGIC 5. **Publish** (`mode=publish` only): copy changed files to `live/`, then the group indexes and the flat
# MAGIC    `sitemap.xml`, then delete stale `live/` files; swap the state table; queue IndexNow URLs for
# MAGIC    `IndexNowPing`; log the run.
# MAGIC
# MAGIC `mode=dry_run` (default) does 1-4 into `dryrun/<run_id>/` and `<state>_dryrun` (diffed against the
# MAGIC published state, read-only), and publishes nothing. Served at https://openalex.org/sitemap.xml by the openalex-gui Nitro middleware
# MAGIC `server/middleware/02.sitemaps.ts` (bucket `live/` prefix).

# COMMAND ----------

# MAGIC %pip install boto3
# MAGIC %restart_python

# COMMAND ----------

import json
import os
import sys
import time
from datetime import datetime, timezone

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "sitemaps.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "sitemaps.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils import sitemaps as sm  # noqa: E402

from pyspark.sql import Row  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

spark.conf.set("spark.sql.session.timeZone", "UTC")

dbutils.widgets.dropdown("mode", "dry_run", ["dry_run", "publish"])
dbutils.widgets.text("state_table", "openalex.sitemaps.page_state")
dbutils.widgets.text("bucket", "openalex-sitemaps")
dbutils.widgets.text("r2_account_id", "a452eddbbe06eb7d02f4879cee70d29c")
dbutils.widgets.text("changed_days", "7")
dbutils.widgets.text("changed_max_urls", "1000000")
dbutils.widgets.text("require_es_sync_today", "true")
# Testing only: keep ids with pmod(id, N) = 0 in every entity ("" = everything). A sample can't be published.
dbutils.widgets.text("sample_mod", "")

MODE = dbutils.widgets.get("mode")
PUBLISH = MODE == "publish"
SAMPLE_MOD = dbutils.widgets.get("sample_mod").strip()
if PUBLISH and SAMPLE_MOD:
    raise ValueError("sample_mod is for dry runs only")
STATE = dbutils.widgets.get("state_table")            # last published state; a dry run only reads it
NEXT = STATE + ("_next" if PUBLISH else "_dryrun")
QUEUE = "openalex.sitemaps.indexnow_queue"
RUNS = "openalex.sitemaps.runs"
BUCKET = dbutils.widgets.get("bucket")
CHANGED_DAYS = int(dbutils.widgets.get("changed_days"))
CHANGED_MAX = int(dbutils.widgets.get("changed_max_urls"))
RUN_TS = datetime.now(timezone.utc).replace(microsecond=0)
RUN_ID = RUN_TS.strftime("%Y%m%dT%H%M%SZ")
RUN_TS_LIT = RUN_TS.strftime("%Y-%m-%d %H:%M:%S")
STAGE = f"{'runs' if PUBLISH else 'dryrun'}/{RUN_ID}"
LIVE = "live"
T0 = time.time()


def log(msg):
    print(f"[{time.time() - T0:7.0f}s] {msg}", flush=True)


R2 = {
    "endpoint_url": f"https://{dbutils.widgets.get('r2_account_id')}.r2.cloudflarestorage.com",
    "aws_access_key_id": dbutils.secrets.get(scope="cloudflare", key="r2-access-key-id"),
    "aws_secret_access_key": dbutils.secrets.get(scope="cloudflare", key="r2-secret-access-key"),
    "region_name": "auto",
}


def s3():
    import boto3
    return boto3.client("s3", **R2)


log(f"run {RUN_ID} mode={MODE} state={STATE} stage=r2://{BUCKET}/{STAGE}/ sample_mod={SAMPLE_MOD or '-'}")

# COMMAND ----------

# MAGIC %md ## 0. Gate: today's ES sync has run (so every page we list exists)

# COMMAND ----------

if dbutils.widgets.get("require_es_sync_today").lower() == "true":
    last = spark.sql("SELECT max(last_sync_started_at) AS t FROM openalex.works.elastic_sync_state "
                     "WHERE index_name = 'works'").collect()[0].t
    if last is None or last.date() < RUN_TS.date():
        raise RuntimeError(f"works ES sync last started {last}; not today ({RUN_TS.date()}). End 2 End not "
                           "published yet: rerun later, or set require_es_sync_today=false knowingly.")
    log(f"works ES sync started {last} UTC: OK")

# COMMAND ----------

# MAGIC %md ## 1-2. Current records and their lastmod → `<state>_next`

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS openalex.sitemaps")
where = f"pmod(id, {int(SAMPLE_MOD)}) = 0" if SAMPLE_MOD else ""
spark.sql("CREATE OR REPLACE TEMP VIEW sm_current AS\n" +
          "\nUNION ALL\n".join(sm.current_records_sql(e, where) for e in sm.ENTITIES))

seed = not spark.catalog.tableExists(STATE)
if not seed:
    spark.sql(f"CREATE OR REPLACE TEMP VIEW sm_prev AS SELECT * FROM {STATE}" + (f" WHERE {where}" if where else ""))
spark.sql(f"CREATE OR REPLACE TABLE {NEXT} CLUSTER BY (entity, id) AS\n" +
          sm.next_state_sql("sm_current", None if seed else "sm_prev", RUN_TS_LIT))
counts = {r.grp: r.n for r in spark.sql(f"SELECT grp, count(*) AS n FROM {NEXT} GROUP BY grp").collect()}
changes = {r.change: r.n for r in spark.sql(f"SELECT change, count(*) AS n FROM {NEXT} GROUP BY change").collect()}
removed = 0 if seed else spark.sql(f"SELECT count(*) AS n FROM ({sm.removed_sql('sm_current', 'sm_prev')})").collect()[0].n
log(f"{'SEED run (no previous state)' if seed else 'incremental'}: {sum(counts.values()):,} records; "
    f"by group {json.dumps(counts)}; changes {json.dumps(changes)}; removed {removed:,}")
for g in counts:
    assert g in sm.ALL_GROUPS, f"unknown group {g}"

# COMMAND ----------

# MAGIC %md ## 3. Pack into files and write them to the stage prefix

# COMMAND ----------

st = spark.table(NEXT)
hist = (st.groupBy("grp", F.floor(F.col("id") / sm.BUCKET_WIDTH).alias("bucket")).count()
        .collect())
assign = sm.pack_buckets((r.grp, r.bucket, r["count"]) for r in hist)
assign_df = spark.createDataFrame([Row(grp=g, bucket=b, file_no=n) for (g, b), n in assign.items()])
n_main_files = len(set((g, n) for (g, _), n in assign.items()))
log(f"{len(hist):,} id buckets packed into {n_main_files:,} files")

main_rows = (st.withColumn("bucket", F.floor(F.col("id") / sm.BUCKET_WIDTH))
             .join(F.broadcast(assign_df), ["grp", "bucket"])
             .select("grp", "file_no", "entity", "id", F.expr(sm.LASTMOD_W3C_SQL).alias("lm"),
                     F.col("id").alias("ord")))
changed = spark.sql(sm.changed_sql(NEXT, RUN_TS_LIT, CHANGED_DAYS, CHANGED_MAX))
changed_rows = (changed.withColumn("ord", F.row_number().over(  # <= changed_max_urls rows: one window is fine
                    Window.orderBy("prio", F.desc("lastmod"), "entity", "id")))
                .select(F.lit(sm.CHANGED_GROUP).alias("grp"),
                        (F.floor((F.col("ord") - 1) / sm.MAX_URLS_PER_FILE) + 1).alias("file_no"),
                        "entity", "id", F.expr(sm.LASTMOD_W3C_SQL).alias("lm"), "ord"))
counts[sm.CHANGED_GROUP] = changed.count()
log(f"changed group: {counts[sm.CHANGED_GROUP]:,} URLs (last {CHANGED_DAYS} days, cap {CHANGED_MAX:,})")

all_rows = main_rows.unionByName(changed_rows)
n_files = n_main_files + -(-counts[sm.CHANGED_GROUP] // sm.MAX_URLS_PER_FILE)
R2_CONF, BUCKET_B, STAGE_B = R2, BUCKET, STAGE


def write_partition(rows):
    """Rows arrive sorted by (grp, file_no, ord); one file per (grp, file_no)."""
    import boto3
    client = boto3.client("s3", **R2_CONF)
    cur_key, buf = None, []

    def flush():
        g, n = cur_key
        gz, stat = sm.build_file(g, n, buf)
        client.put_object(Bucket=BUCKET_B, Key=f"{STAGE_B}/{stat.name}", Body=gz,
                          ContentType="application/gzip", CacheControl="public, max-age=3600")
        return Row(group=stat.group, n=stat.n, name=stat.name, urls=stat.urls, lastmod=stat.lastmod,
                   bytes_xml=stat.bytes_xml, bytes_gz=stat.bytes_gz, md5=stat.md5,
                   first_id=stat.first_id, last_id=stat.last_id)

    for r in rows:
        key = (r.grp, r.file_no)
        if key != cur_key and buf:
            yield flush()
            buf = []
        cur_key = key
        buf.append((r.entity, r.id, r.lm))
    if buf:
        yield flush()


stats_rows = (all_rows.repartition(max(64, n_files // 4), "grp", "file_no")
              .sortWithinPartitions("grp", "file_no", "ord")
              .rdd.mapPartitions(write_partition).collect())
stats = [sm.FileStat(r.group, r.n, r.name, r.urls, r.lastmod, r.bytes_xml, r.bytes_gz, r.md5, r.first_id, r.last_id)
         for r in stats_rows]
log(f"wrote {len(stats):,} files, {sum(s.urls for s in stats):,} URLs, "
    f"{sum(s.bytes_gz for s in stats) / 1e9:.2f} GB gzipped ({sum(s.bytes_xml for s in stats) / 1e9:.2f} GB XML) "
    f"to r2://{BUCKET}/{STAGE}/")

# COMMAND ----------

# MAGIC %md ## 4. Checks (any problem stops here; `live/` untouched)

# COMMAND ----------

problems = sm.validate_published_set(stats, counts)
if len(stats) != n_files:
    problems.append(f"{len(stats)} files written, {n_files} planned")
if not SAMPLE_MOD:
    floor = {"works-abstract": 150_000_000, "works-cited": 30_000_000, "authors-orcid": 8_000_000,
             "authors-multi": 30_000_000, "sources": 200_000, "institutions": 100_000, "topics": 4_000}
    problems += [f"group {g}: {counts.get(g, 0):,} URLs, under the floor {f:,} (input table broken?)"
                 for g, f in floor.items() if counts.get(g, 0) < f]
indexes = sm.build_indexes(stats)
if problems:
    raise RuntimeError("NOT PUBLISHED:\n  " + "\n  ".join(problems))
client = s3()
for name, data in indexes.items():
    client.put_object(Bucket=BUCKET, Key=f"{STAGE}/{name}", Body=data, ContentType="application/xml; charset=utf-8")
client.put_object(Bucket=BUCKET, Key=f"{STAGE}/manifest.json", ContentType="application/json",
                  Body=json.dumps({"run_id": RUN_ID, "mode": MODE, "seed": seed, "counts": counts,
                                   "changes": changes, "removed": removed,
                                   "files": [s.__dict__ for s in stats]}).encode())
log(f"checks OK; {len(indexes)} indexes written to the stage")

# COMMAND ----------

# MAGIC %md ## 5. Publish (mode=publish only)

# COMMAND ----------

if not PUBLISH:
    log(f"dry run: nothing published. Files: r2://{BUCKET}/{STAGE}/ ; state: {NEXT}")
    dbutils.notebook.exit(json.dumps({"run_id": RUN_ID, "mode": MODE, "files": len(stats), "counts": counts}))

from concurrent.futures import ThreadPoolExecutor  # noqa: E402

live = {}
for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"{LIVE}/"):
    for o in page.get("Contents", []):
        live[o["Key"][len(LIVE) + 1:]] = o["ETag"].strip('"')
to_copy = [s.name for s in stats if live.get(s.name) != s.md5]
log(f"{len(to_copy):,} of {len(stats):,} files changed since the live set; copying")


def _copy(name):
    client.copy_object(Bucket=BUCKET, Key=f"{LIVE}/{name}", CopySource={"Bucket": BUCKET, "Key": f"{STAGE}/{name}"},
                       MetadataDirective="COPY")


with ThreadPoolExecutor(32) as ex:
    list(ex.map(_copy, to_copy))
# Indexes last, so a crawler never sees an index entry before its file; the flat top index very last.
for name in sorted(indexes, key=lambda n: n == sm.TOP_INDEX_NAME):
    client.put_object(Bucket=BUCKET, Key=f"{LIVE}/{name}", Body=indexes[name],
                      ContentType="application/xml; charset=utf-8", CacheControl="public, max-age=3600")
keep = {s.name for s in stats} | set(indexes)
stale = [k for k in live if k not in keep and k.startswith("sitemap")]
for chunk in sm.chunks(stale, 1000):
    client.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": f"{LIVE}/{k}"} for k in chunk]})
log(f"published: {len(to_copy):,} files copied, {len(indexes)} indexes, {len(stale):,} stale files deleted")

# IndexNow queue (read by IndexNowPing), then the state swap, then the run log. A failure before the swap
# leaves the old state, so the next run recomputes the same diffs (and may queue some URLs twice: harmless).
spark.sql(f"""CREATE TABLE IF NOT EXISTS {QUEUE} (run_id STRING, url STRING, kind STRING, priority INT,
              lastmod TIMESTAMP) CLUSTER BY (run_id)""")
queued = 0
if not seed:  # the seed run marks every record new: nothing to ping
    url_sql = ("concat('https://openalex.org/', entity, '/', CASE entity "
               + " ".join(f"WHEN '{e.name}' THEN '{e.prefix}'" for e in sm.ENTITIES) + " END, id)")
    spark.sql(f"""INSERT INTO {QUEUE}
      SELECT '{RUN_ID}', {url_sql}, change, CASE change WHEN 'new' THEN 0 ELSE 1 END, lastmod
      FROM {NEXT} WHERE change IN ('new', 'updated')
      UNION ALL
      SELECT '{RUN_ID}', {url_sql}, 'removed', 2, TIMESTAMP'{RUN_TS_LIT}'
      FROM ({sm.removed_sql('sm_current', 'sm_prev')})""")
    queued = spark.sql(f"SELECT count(*) AS n FROM {QUEUE} WHERE run_id = '{RUN_ID}'").collect()[0].n
spark.sql(f"CREATE OR REPLACE TABLE {STATE} CLUSTER BY (entity, id) AS SELECT * FROM {NEXT}")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {RUNS} (run_id STRING, run_ts TIMESTAMP, mode STRING, seed BOOLEAN,
              counts STRING, changes STRING, removed BIGINT, files INT, bytes_gz BIGINT, copied INT,
              indexnow_queued BIGINT, seconds DOUBLE)""")
spark.createDataFrame([Row(run_id=RUN_ID, run_ts=RUN_TS.replace(tzinfo=None), mode=MODE, seed=seed,
                           counts=json.dumps(counts), changes=json.dumps(changes), removed=int(removed),
                           files=len(stats), bytes_gz=sum(s.bytes_gz for s in stats), copied=len(to_copy),
                           indexnow_queued=int(queued), seconds=time.time() - T0)]).write.mode("append").saveAsTable(RUNS)

# Stage prefixes older than the last 3 runs go.
runs = sorted({o["Key"].split("/")[1] for page in client.get_paginator("list_objects_v2").paginate(
    Bucket=BUCKET, Prefix="runs/") for o in page.get("Contents", [])})
for old in runs[:-3]:
    keys = [o["Key"] for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"runs/{old}/")
            for o in page.get("Contents", [])]
    for chunk in sm.chunks(keys, 1000):
        client.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in chunk]})
log(f"done: state swapped, {queued:,} IndexNow URLs queued for run {RUN_ID}")
dbutils.notebook.exit(json.dumps({"run_id": RUN_ID, "mode": MODE, "files": len(stats), "queued": queued}))
