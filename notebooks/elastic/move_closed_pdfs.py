# Databricks notebook source
# MAGIC %md
# MAGIC # Closed PDFs upkeep (oxjob #2430)
# MAGIC
# MAGIC Rule (Casey + Jason, 19 Nov 2026): a PDF whose locations the works API shows closed lives in the private R2 bucket
# MAGIC `openalex-pdfs-closed` (its GROBID XML in `openalex-grobid-xml-closed`), with a row in `openalex.pdf.closed_pdfs`
# MAGIC saying how it was linked and since when; it moves back the night its location turns open. Runs before the manifest
# MAGIC export, so the manifest, the content API (D1) and `has_content` never name a file that isn't in the client bucket.
# MAGIC
# MAGIC Order: (1) new closed files get a pending row (the manifest and D1 pairs skip them from now on); (2) reopened files
# MAGIC are copied back and stamped `moved_back_at`; (3) D1 rows of every touched work are re-pointed (old rows saved to
# MAGIC `content_index_removed_rows`); (4) pending files are copied, checked and removed from the client buckets, then
# MAGIC stamped `moved_at`. A file that fails stays pending (still left out everywhere) and is retried the next night.

# COMMAND ----------

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

dbutils.widgets.text("dry_run", "false", "dry run: count only, change nothing")
dbutils.widgets.text("max_files", "300000", "stop before changing anything if more files than this would move")
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() in ("true", "1", "yes")
MAX_FILES = int(dbutils.widgets.get("max_files").strip() or "300000")

CF_ACCOUNT_ID = "a452eddbbe06eb7d02f4879cee70d29c"
D1_URL = (f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}"
          "/d1/database/c2e1cc17-1810-400b-a7c8-c5103ab366de/query")
D1_TOKEN = dbutils.secrets.get(scope="cloudflare", key="d1_api_token")
BUCKETS = {"pdf": ("openalex-pdfs", "openalex-pdfs-closed"), "xml": ("openalex-grobid-xml", "openalex-grobid-xml-closed")}
s3 = boto3.client(
    "s3", endpoint_url=f"https://{CF_ACCOUNT_ID}.r2.cloudflarestorage.com", region_name="auto",
    aws_access_key_id=dbutils.secrets.get(scope="cloudflare", key="closed-r2-access-key-id"),
    aws_secret_access_key=dbutils.secrets.get(scope="cloudflare", key="closed-r2-secret-access-key"),
    config=Config(max_pool_connections=64, read_timeout=300, retries={"max_attempts": 3, "mode": "standard"}))
spark.conf.set("spark.sql.session.timeZone", "UTC")

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "content_files.py")):  # as in export_content_manifest_to_r2
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "content_files.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils.content_files import primary_pair_sql  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which files turned closed, which turned open

# COMMAND ----------

# Every location the works API serves, read once (both rules below read it).
spark.sql("""
SELECT w.id AS work_id, loc.native_id, lower(loc.native_id) AS nid, coalesce(loc.is_oa, false) AS oa,
       loc.pdf_s3_id, loc.grobid_s3_id, loc.doi, coalesce(loc.landing_page_url, loc.pdf_url) AS url
FROM openalex.works.openalex_works w LATERAL VIEW explode(w.locations) t AS loc
""").persist().createOrReplaceTempView("api")

# A file is closed when no open location uses it or its record and a closed one does: linked to closed locations
# only (a #1006 dead host, a #1170 curation), or named only by a parse row that isn't open (#1105 pdf_removed).
new_df = spark.sql("""
WITH open_file AS (SELECT DISTINCT pdf_s3_id AS key FROM api WHERE oa AND pdf_s3_id IS NOT NULL),
open_rec AS (SELECT DISTINCT nid FROM api WHERE oa AND nid IS NOT NULL),
closed_rec AS (SELECT nid, min_by(struct(work_id, native_id, doi), work_id) AS r FROM api WHERE NOT oa GROUP BY nid),
held AS (SELECT key FROM openalex.pdf.closed_pdfs WHERE moved_back_at IS NULL
         UNION SELECT key FROM openalex.pdf.archived_files WHERE restored_at IS NULL),
linked AS (
  SELECT a.pdf_s3_id AS key, min_by(struct(a.work_id, a.native_id, a.doi, a.grobid_s3_id, a.url), a.work_id) AS l
  FROM api a LEFT ANTI JOIN open_file o ON o.key = a.pdf_s3_id
  WHERE NOT a.oa AND a.pdf_s3_id IS NOT NULL GROUP BY a.pdf_s3_id),
parsed AS (
  SELECT get(filter(ids, x -> x.namespace = 'docs.pdf'), 0).id AS key,
         max(get(filter(ids, x -> x.namespace = 'docs.parsed-pdf'), 0).id) AS xml,
         min(lower(coalesce(get(filter(ids, x -> x.namespace = 'pmh'), 0).id,
                            get(filter(ids, x -> x.namespace = 'doi'), 0).id))) AS nid,
         min(rm.applied_at) AS removed_at, bool_or(coalesce(p.is_oa, false)) AS any_open
  FROM openalex.pdf.pdf_works p LEFT JOIN openalex.pdf.pdf_removed rm ON rm.pdf_url = p.native_id
  GROUP BY 1 HAVING key IS NOT NULL AND NOT any_open),
not_open AS (
  SELECT p.key, p.xml, p.nid, p.removed_at, c.r
  FROM parsed p JOIN closed_rec c USING (nid)
  LEFT ANTI JOIN open_file o ON o.key = p.key LEFT ANTI JOIN open_rec r ON r.nid = p.nid)
SELECT l.key, l.l.work_id AS work_id, l.l.native_id AS native_id, l.l.doi AS doi, 'location' AS linked_via,
       CASE WHEN cur.native_id IS NOT NULL THEN 'location curated closed (#1170)'
            WHEN dl.kind IS NOT NULL THEN concat('host ', dl.host, ' is ', dl.kind, ' (#1006)')
            ELSE 'location closed in the works API' END AS reason,
       l.l.grobid_s3_id AS xml_key
FROM linked l
LEFT JOIN openalex.works.dead_link_hosts dl
  ON dl.host = regexp_extract(lower(l.l.url), '^[a-z]+://(?:www[.])?([^/:?#]+)', 1)
 AND (dl.url_rlike IS NULL OR lower(l.l.url) RLIKE dl.url_rlike)
LEFT JOIN (SELECT DISTINCT native_id, work_id FROM openalex.works.locations_mapped_curated
           WHERE array_contains(curated_fields, 'is_oa')) cur
  ON cur.native_id = l.l.native_id AND cur.work_id = l.l.work_id
LEFT ANTI JOIN held h ON h.key = l.key
UNION ALL
SELECT n.key, n.r.work_id, n.r.native_id, n.r.doi, 'parse row',
       CASE WHEN n.removed_at IS NOT NULL THEN 'publisher PDF URL not open (#1105 pdf_removed)'
            ELSE 'parse row not open (#757)' END,
       n.xml
FROM not_open n LEFT ANTI JOIN held h ON h.key = n.key LEFT ANTI JOIN linked lk ON lk.key = n.key
""")
new_df = new_df.dropDuplicates(["key"])  # a host listed twice in dead_link_hosts (url_rlike variants) joins twice
new_df.createOrReplaceTempView("new_closed")

# Back to the client bucket: the mirror of the rule above, so a file never flips back and forth. A file closed by
# its location returns when an open location uses it; one closed by its record (parse row, file metadata) also
# returns when that record has an open location. An XML follows its PDF.
reopen_df = spark.sql("""
WITH open_file AS (SELECT DISTINCT pdf_s3_id AS key FROM api WHERE oa AND pdf_s3_id IS NOT NULL),
open_rec AS (SELECT DISTINCT nid FROM api WHERE oa AND nid IS NOT NULL),
moved AS (SELECT * FROM openalex.pdf.closed_pdfs WHERE moved_back_at IS NULL AND moved_at IS NOT NULL),
back_pdf AS (
  SELECT m.key FROM moved m
  WHERE m.kind = 'pdf' AND (m.key IN (SELECT key FROM open_file)
        OR (m.linked_via <> 'location' AND lower(m.native_id) IN (SELECT nid FROM open_rec))))
SELECT m.key, m.kind, m.work_id FROM moved m
WHERE m.key IN (SELECT key FROM back_pdf) OR (m.kind = 'xml' AND m.pdf_key IN (SELECT key FROM back_pdf))
""")
new_rows = [r.asDict() for r in new_df.collect()]
reopen_rows = [r.asDict() for r in reopen_df.collect()]
n_new = len(new_rows) + sum(1 for r in new_rows if r["xml_key"])
n_pending = spark.sql("SELECT count(*) n FROM openalex.pdf.closed_pdfs WHERE moved_at IS NULL AND moved_back_at IS NULL").first()["n"]
print(f"new closed PDFs: {len(new_rows):,} (+ {n_new - len(new_rows):,} XMLs); already pending: {n_pending:,}; "
      f"to move back: {len(reopen_rows):,}")
if n_new + n_pending + len(reopen_rows) > MAX_FILES:
    raise Exception(f"{n_new + n_pending + len(reopen_rows):,} files would move, over max_files={MAX_FILES:,}: check the "
                    "night's changes, then rerun with a higher max_files (oxjob #2430). Nothing was changed.")
if DRY_RUN:
    dbutils.notebook.exit(f"dry run: {len(new_rows)} new closed PDFs, {len(reopen_rows)} to move back")

# COMMAND ----------

# MAGIC %md
# MAGIC ## (1) Pending rows for the new closed files

# COMMAND ----------

if new_rows:
    spark.sql("""
    INSERT INTO openalex.pdf.closed_pdfs
    SELECT key, 'pdf', CAST(NULL AS STRING), NULL, NULL, NULL, work_id, native_id,
           CASE WHEN native_id LIKE 'oai:%' THEN 'pmh' WHEN native_id LIKE '10.%' THEN 'doi' END, doi, linked_via, reason,
           current_date(), 'openalex-pdfs', 'openalex-pdfs-closed', CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), 2430
    FROM new_closed
    UNION ALL
    SELECT xml_key, 'xml', key, NULL, NULL, NULL, work_id, native_id,
           CASE WHEN native_id LIKE 'oai:%' THEN 'pmh' WHEN native_id LIKE '10.%' THEN 'doi' END, doi, linked_via, reason,
           current_date(), 'openalex-grobid-xml', 'openalex-grobid-xml-closed', CAST(NULL AS TIMESTAMP),
           CAST(NULL AS TIMESTAMP), 2430
    FROM new_closed WHERE xml_key IS NOT NULL
      AND xml_key NOT IN (SELECT key FROM openalex.pdf.closed_pdfs WHERE moved_back_at IS NULL)
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## (2) Move reopened files back

# COMMAND ----------


def copy_checked(src, dst, key, storage_class):
    """Copy key src -> dst and check it; returns (size, etag, last_modified) of the copy, or None."""
    try:
        h = s3.head_object(Bucket=src, Key=key)
        r = s3.copy_object(Bucket=dst, Key=key, CopySource={"Bucket": src, "Key": key}, StorageClass=storage_class)
        etag = r["CopyObjectResult"]["ETag"].strip('"')
        if etag != h["ETag"].strip('"') and s3.head_object(Bucket=dst, Key=key)["ContentLength"] != h["ContentLength"]:
            return None
        return h["ContentLength"], h["ETag"].strip('"'), h["LastModified"]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in ("NoSuchKey", "404"):
            print(f"{src}/{key}: {e}")
            return None
        try:  # already copied by an earlier night that stopped before the delete
            h = s3.head_object(Bucket=dst, Key=key)
            return h["ContentLength"], h["ETag"].strip('"'), h["LastModified"]
        except ClientError:
            print(f"{key}: in neither {src} nor {dst}")
            return None


def move(rows, kind_of, back):
    """Copy, check, delete from the source. Returns {key: (size, etag, last_modified)} of the files moved."""
    def one(r):
        src, dst = BUCKETS[kind_of(r)][::-1] if back else BUCKETS[kind_of(r)]
        got = copy_checked(src, dst, r["key"], "STANDARD" if back else "STANDARD_IA")
        if got is None:
            return None
        try:
            s3.delete_object(Bucket=src, Key=r["key"])
        except ClientError as e:
            print(f"delete {src}/{r['key']}: {e}")
            return None
        return r["key"], got
    with ThreadPoolExecutor(32) as ex:
        return dict(x for x in ex.map(one, rows) if x)


back = move(reopen_rows, lambda r: r["kind"], back=True)
if back:
    spark.createDataFrame([(k,) for k in back], "key string").createOrReplaceTempView("moved_back")
    spark.sql("""MERGE INTO openalex.pdf.closed_pdfs t USING moved_back b
                 ON t.key = b.key AND t.moved_back_at IS NULL WHEN MATCHED THEN UPDATE SET t.moved_back_at = current_timestamp()""")
print(f"moved back: {len(back):,} of {len(reopen_rows):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## (3) Re-point D1 for every touched work

# COMMAND ----------


def d1(sql, params=None):
    for attempt in range(9):
        r = requests.post(D1_URL, headers={"Authorization": f"Bearer {D1_TOKEN}"},
                          json={"sql": sql, **({"params": params} if params else {})}, timeout=120)
        if r.status_code not in (429, 500, 502, 503, 504) or attempt == 8:
            r.raise_for_status()
            return r.json()
        time.sleep(min(60, 2 ** attempt))


pending = spark.sql("SELECT key, kind, work_id FROM openalex.pdf.closed_pdfs WHERE moved_at IS NULL AND moved_back_at IS NULL")
pending.createOrReplaceTempView("pending")
works = sorted({r["work_id"] for r in pending.select("work_id").distinct().collect() if r["work_id"]}
               | {r["work_id"] for r in reopen_rows if r["work_id"]})
spark.createDataFrame([(w,) for w in works], "work_id bigint").createOrReplaceTempView("touched_works")
want = {r["work_id"]: (r["pdf_uuid"], r["grobid_uuid"])
        for r in spark.sql(primary_pair_sql("AND work_id IN (SELECT work_id FROM touched_works)")).collect()}
changes = []
for i in range(0, len(works), 500):
    batch = works[i:i + 500]
    have = d1(f"SELECT work_id, pdf_uuid, grobid_uuid FROM content_index WHERE work_id IN ({','.join(map(str, batch))})")
    have = {x["work_id"]: (x["pdf_uuid"], x["grobid_uuid"]) for x in have["result"][0]["results"]}
    for w in batch:
        if have.get(w) != want.get(w):
            changes.append((w, *(have.get(w) or (None, None)), "deleted" if w not in want else "replaced",
                            *(want.get(w) or (None, None))))
if changes:
    spark.createDataFrame(changes, "work_id bigint, pdf_uuid string, grobid_uuid string, action string, "
                          "new_pdf_uuid string, new_grobid_uuid string").createOrReplaceTempView("d1_changes")
    spark.sql("""INSERT INTO openalex.pdf.content_index_removed_rows
                 SELECT work_id, pdf_uuid, grobid_uuid, action, new_pdf_uuid, new_grobid_uuid, current_timestamp(), 2430
                 FROM d1_changes""")
    for i in range(0, len(changes), 500):  # change = (work_id, old pdf, old xml, action, new pdf, new xml)
        b = changes[i:i + 500]
        gone = [str(c[0]) for c in b if c[3] == "deleted"]
        if gone:
            d1(f"DELETE FROM content_index WHERE work_id IN ({','.join(gone)})")
        put = [c for c in b if c[3] != "deleted"]
        if put:
            d1("INSERT OR REPLACE INTO content_index (work_id, pdf_uuid, grobid_uuid) VALUES " + ",".join(
               f"({c[0]}, {repr(c[4]) if c[4] else 'NULL'}, {repr(c[5]) if c[5] else 'NULL'})" for c in put))
print(f"D1: {sum(c[3] == 'deleted' for c in changes):,} rows deleted, {sum(c[3] == 'replaced' for c in changes):,} "
      f"re-pointed, over {len(works):,} touched works")

# COMMAND ----------

# MAGIC %md
# MAGIC ## (4) Move the pending files out

# COMMAND ----------

pending_rows = [r.asDict() for r in pending.collect()]
moved = move(pending_rows, lambda r: r["kind"], back=False)
if moved:
    spark.createDataFrame([(k, *v) for k, v in moved.items()],
                          "key string, size bigint, etag string, last_modified timestamp").createOrReplaceTempView("moved_out")
    spark.sql("""MERGE INTO openalex.pdf.closed_pdfs t USING moved_out m
                 ON t.key = m.key AND t.moved_at IS NULL AND t.moved_back_at IS NULL
                 WHEN MATCHED THEN UPDATE SET t.moved_at = current_timestamp(), t.size = m.size, t.etag = m.etag,
                                              t.last_modified = m.last_modified""")
print(f"moved out: {len(moved):,} of {len(pending_rows):,} pending (the rest stay pending and left out; retried tomorrow)")
stuck = spark.sql("""SELECT count(*) n FROM openalex.pdf.closed_pdfs
                     WHERE moved_at IS NULL AND moved_back_at IS NULL AND closed_since < current_date() - 3""").first()["n"]
if stuck:  # fail (job email) rather than retry quietly forever
    raise Exception(f"{stuck:,} closed files have failed to move for more than 3 nights (oxjob #2430)")
