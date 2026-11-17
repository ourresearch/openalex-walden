# Databricks notebook source
# MAGIC %md
# MAGIC # Oxjob #780: replace the 11 damaged PDFs behind tickets 7704 and 22726 (one-off, branch only)
# MAGIC
# MAGIC 1. **Check**: HEAD and byte-check the 11 damaged files content.openalex.org serves today, plus the other stored
# MAGIC    PDFs of the same works (the file each work would fall back to once the damaged one is excluded).
# MAGIC 2. **PMC**: store the open-licence PMC PDF for the 7 works that have one, exactly as `pmc_pdf_sweep.py` does
# MAGIC    (md5 must equal PMC's, `%PDF-` header, `utils/pdf_store.store_pdf`, one `taxicab_results` row each).
# MAGIC 3. **Re-fetch**: POST the other 4 publisher URLs to TaxiCab (same URL as the damaged file's `pdf_works` key, so a
# MAGIC    good parse replaces that row), byte-check what it stored, and append a `taxicab_results` row only when the new
# MAGIC    file is a whole PDF. A damaged or walled result gets no row, so Parse PDFs never sees it.
# MAGIC
# MAGIC Revert: `DELETE FROM openalex.taxicab.taxicab_results WHERE taxicab_id IN (<ids this run printed>)` before the
# MAGIC 00:30 UTC Parse PDFs run; after it, add the ids to `openalex.pdf.invalid_pdfs`.

# COMMAND ----------

import datetime
import hashlib
import json
import os
import re
import sys
from urllib.parse import quote

import boto3
import requests
from botocore.config import Config
from pyspark.sql import types as T
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

dbutils.widgets.text("dry_run", "true", "true = check only, store nothing")
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"
dbutils.widgets.text("steps", "check,pmc,refetch", "which steps to run")
STEPS = {x.strip() for x in dbutils.widgets.get("steps").split(",") if x.strip()}

PDF_BUCKET = "openalex-pdfs"
PMC_BUCKET = "https://pmc-oa-opendata.s3.amazonaws.com/"
USER_AGENT = "OpenAlex PMC PDF sweep (https://openalex.org; team@ourresearch.org)"
TAXICAB = "http://harvester-load-balancer-366186003.us-east-1.elb.amazonaws.com/taxicab"
try:
    ORIGIN_HEADERS = {"X-OpenAlex-Origin-Key": dbutils.secrets.get("openalex-harvest", "origin-key")}
except Exception:
    ORIGIN_HEADERS = {}

# work -> the damaged file content.openalex.org serves (D1 primary pair, 17 Nov 2026) and its served size
DAMAGED = {
    "W2104639053": ("06382c7d-4b47-4e08-944f-b9a4bbe0e5ca", 1057824),
    "W2057055150": ("41928213-b320-43e2-b7b4-cbbd59f7297f", 1054869),
    "W2991253675": ("f39a312a-4dbd-46b8-8024-a029cd529f62", 537718),
    "W3201983205": ("2f634ca1-a1b2-47d9-87b0-cfa641b9e0bd", 665118),
    "W2748037493": ("5310897b-26b4-4f80-8128-77bef78fd2b4", 6257324),
    "W2014074083": ("23d2664b-1c01-4cbc-bdfa-926cee1b28e8", 249809),
    "W2281843965": ("4a5f3300-3c94-4560-8e70-b085246e36fd", 903919),
    "W2041575284": ("ad9afd28-b281-45bc-832d-befe15ab0eb5", 256180),
    "W2287302929": ("5fb46f3a-1ed0-4f44-9120-15c8aef1444c", 278110),
    "W2607011121": ("b97489d4-1d35-42c0-b715-fd5fc27e007e", 153220),
    "W2150730646": ("40339735-b7ae-4b79-bf9f-d8ebef94e443", 456013),
}
# other stored PDFs on the same works (fallback once the damaged file is excluded)
OTHERS = {
    "W2057055150": ["da6f02ad-f1ac-410e-b1a4-9b49cb5bf473"],  # CSIC repository, submittedVersion
    "W2991253675": ["10ab0a3c-9738-4c21-aec0-18dc6661727f"],  # ORA, submittedVersion
    "W2607011121": ["208cbe47-90dc-4ac7-824a-8afedd6c57ee", "1b730c50-b851-44cc-9a9d-ee1c85f836b9"],  # bioRxiv, ORA
}
PMC = [  # native_id, digits, bucket key, md5 (EXPLORE § 6, work/pmc_replacements_2026-11-16.jsonl)
    ("oai:pubmedcentral.nih.gov:7044756", "7044756", "PMC7044756.1/PMC7044756.1.pdf", "ba523a309f44d6b7849e5d4a61ca6785"),
    ("oai:pubmedcentral.nih.gov:8988461", "8988461", "PMC8988461.1/PMC8988461.1.pdf", "641f1198fbb3796e7d432329fe16dbfe"),
    ("oai:pubmedcentral.nih.gov:5589211", "5589211", "PMC5589211.1/PMC5589211.1.pdf", "88c22a24ed81290966fef28fe5cd9d08"),
    ("oai:pubmedcentral.nih.gov:3911435", "3911435", "PMC3911435.1/PMC3911435.1.pdf", "fb560532176c1e32a24556105af7b828"),
    ("oai:pubmedcentral.nih.gov:4763151", "4763151", "PMC4763151.1/PMC4763151.1.pdf", "ab40f73b0baa1b91298ea393f6786582"),
    ("oai:pubmedcentral.nih.gov:4742704", "4742704", "PMC4742704.1/PMC4742704.1.pdf", "2145a3521185d5fc3137c944e45c7ee7"),
    ("oai:pubmedcentral.nih.gov:5527411", "5527411", "PMC5527411.1/PMC5527411.1.pdf", "376eea1cc2dad377fd1da2700df5bd44"),
]
REFETCH = [  # work, the damaged file's pdf_works key (URL), bare location DOI
    ("W2104639053", "https://academic.oup.com/plcell/article-pdf/9/4/641/34749300/plcell_v9_4_641.pdf", "10.1105/tpc.9.4.641"),
    ("W2057055150", "http://www.cell.com/article/S0092867414013762/pdf", "10.1016/j.cell.2014.10.049"),
    ("W2041575284", "https://onlinelibrary.wiley.com/doi/pdfdirect/10.1002/hep.20155", "10.1002/hep.20155"),
    ("W2150730646", "https://onlinelibrary.wiley.com/doi/pdfdirect/10.1890/07-1359.1", "10.1890/07-1359.1"),
]

s3 = boto3.client(
    "s3",
    endpoint_url=dbutils.secrets.get("cloudflare", "r2-endpoint"),
    aws_access_key_id=dbutils.secrets.get("cloudflare", "r2-access-key-id"),
    aws_secret_access_key=dbutils.secrets.get("cloudflare", "r2-secret-access-key"),
    region_name="auto",
    config=Config(retries={"max_attempts": 6}),
)
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from utils.pdf_store import md5_table, store_pdf  # noqa: E402

# COMMAND ----------

# MAGIC %md ## Byte check (oxjob #780 `work/pdf_byte_check.py`, whole-file mode)

# COMMAND ----------

TAIL_BYTES = 4096
CAPS = (10 * 1024 * 1024, 26 * 1024 * 1024)
FORBIDDEN = frozenset([0xC0, 0xC1] + list(range(0xF5, 0x100)))


def _valid_utf8(b):
    for lo in range(0, 4):
        for hi in range(0, 4):
            seg = b[lo:len(b) - hi] if hi else b[lo:]
            try:
                seg.decode("utf-8")
                return True
            except UnicodeDecodeError as e:
                if e.start > 3 and e.end < len(seg) - 3:
                    return False
    return False


def byte_check(data):
    size = len(data)
    r = {"size": size}
    if size < 1024:
        return {**r, "label": "TINY"}
    if data[:2] == b"\x1f\x8b":
        return {**r, "label": "NOT_PDF_GZIP"}
    if b"%PDF-" not in data[:1024]:
        low = data[:2048].lower()
        return {**r, "label": "NOT_PDF_HTML" if (b"<html" in low or b"<!doctype" in low or b"<head" in low) else "NOT_PDF_OTHER"}
    n_high = sum(1 for x in data if x >= 0x80)
    decoded = n_high >= 64 and not any(x in FORBIDDEN for x in data) and _valid_utf8(data)
    tail = data[-TAIL_BYTES:]
    m = None
    for m in re.finditer(rb"startxref\s+(\d+)", tail):
        pass
    xref = int(m.group(1)) if m else None
    has_eof = b"%%EOF" in tail
    r.update(startxref=xref, has_eof=has_eof, high_bytes=n_high)
    if decoded:
        r["label"] = "UTF8_DECODED"
    elif xref is not None and xref >= size:
        r["label"] = "STARTXREF_PAST_EOF"
    elif not has_eof and size in CAPS:
        r["label"] = "SIZE_CAP"
    elif not has_eof:
        r["label"] = "NO_EOF"
    else:
        r["label"] = "OK"
    return r


def r2_file(pdf_id):
    """HEAD + GET one stored PDF: (head info, bytes)."""
    o = s3.get_object(Bucket=PDF_BUCKET, Key=f"{pdf_id}.pdf")
    body = o["Body"].read()
    meta = o.get("Metadata", {})
    return {"etag": o["ETag"].strip('"'), "native_id": meta.get("native_id"), "url": meta.get("url"),
            "created_date": meta.get("created_date")}, body


def first_text(body):
    """A crude look at page 1: the title-ish words GROBID would see are compressed, so just report the /Title."""
    m = re.search(rb"/Title\s*\(([^)]{0,200})\)", body[:200000])
    return m.group(1).decode("latin-1", "replace") if m else None

# COMMAND ----------

# MAGIC %md ## 1. Check the served files and the fallbacks

# COMMAND ----------

check_rows = []
for work, (pdf_id, served_size) in (DAMAGED.items() if "check" in STEPS else []):
    head, body = r2_file(pdf_id)
    r = {"work": work, "role": "served", "pdf_id": pdf_id, "served_size": served_size,
         "same_as_served": len(body) == served_size, **head, **byte_check(body)}
    check_rows.append(r)
    print(json.dumps(r))
    for other in OTHERS.get(work, []):
        try:
            head, body = r2_file(other)
            r = {"work": work, "role": "fallback", "pdf_id": other, **head, **byte_check(body), "title": first_text(body)}
        except Exception as e:
            r = {"work": work, "role": "fallback", "pdf_id": other, "error": f"{type(e).__name__}: {e}"[:300]}
        check_rows.append(r)
        print(json.dumps(r))

# COMMAND ----------

# MAGIC %md ## 2. PMC copies for the 7 works that have one

# COMMAND ----------

session = requests.Session()
session.headers["User-Agent"] = USER_AGENT
session.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1.5,
                                                        status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])))
pdf_md5 = None if DRY_RUN else md5_table(dbutils)
already = {r.native_id for r in spark.sql(
    "SELECT DISTINCT native_id FROM openalex.taxicab.taxicab_results WHERE native_id IN ({}) "
    "AND content_type LIKE '%pdf%' AND status_code = 200 AND s3_path IS NOT NULL".format(
        ",".join(f"'{p[0]}'" for p in PMC))).collect()}

tx_rows, report = [], []
for native_id, digits, key, md5 in (PMC if "pmc" in STEPS else []):
    pmc_url = f"https://pmc.ncbi.nlm.nih.gov/articles/PMC{digits}/pdf/"
    if native_id in already:
        report.append({"step": "pmc", "native_id": native_id, "result": "already stored, skipped"})
        continue
    r = session.get(PMC_BUCKET + key, timeout=300)
    body = r.content
    ok_md5 = hashlib.md5(body).hexdigest() == md5
    chk = byte_check(body) if r.status_code == 200 else {"label": f"HTTP {r.status_code}"}
    rep = {"step": "pmc", "native_id": native_id, "http": r.status_code, "md5_ok": ok_md5, **chk}
    if r.status_code == 200 and ok_md5 and chk["label"] == "OK" and not DRY_RUN:
        now = datetime.datetime.utcnow()
        pdf_uuid, reused = store_pdf(s3, pdf_md5, body, {
            "url": quote(pmc_url), "resolved_url": quote(PMC_BUCKET + key), "created_date": now.isoformat(),
            "content_type": "pdf", "native_id": native_id, "native_id_namespace": "pmh", "source": "pmc_pdf_sweep"})
        tx_rows.append((pdf_uuid, pmc_url, PMC_BUCKET + key, 200, "pdf", native_id, "pmh",
                        f"s3://{PDF_BUCKET}/{pdf_uuid}.pdf", False, now, now, None))
        rep.update(stored=pdf_uuid, reused=reused)
    report.append(rep)
    print(json.dumps(rep))

# COMMAND ----------

# MAGIC %md ## 3. Re-fetch the other 4 through TaxiCab

# COMMAND ----------

for work, url, doi in (REFETCH if "refetch" in STEPS else []):
    rep = {"step": "refetch", "work": work, "url": url}
    if DRY_RUN:
        report.append({**rep, "result": "dry run, not fetched"})
        continue
    try:
        resp = requests.post(TAXICAB, json={"url": url, "native_id": doi, "native_id_namespace": "doi"},
                             headers=ORIGIN_HEADERS, timeout=180)
        rep["http"] = resp.status_code
        d = resp.json()
    except Exception as e:
        rep["error"] = f"{type(e).__name__}: {e}"[:300]
        report.append(rep)
        print(json.dumps(rep))
        continue
    rep["body"] = resp.text[:300] if resp.status_code >= 400 else None
    rep.update({k: d.get(k) for k in ("id", "status_code", "resolved_url", "content_type", "s3_path", "is_soft_block")})
    if d.get("id") and d.get("s3_path") and "pdf" in (d.get("content_type") or ""):
        try:
            head, body = r2_file(d["id"])
            rep.update(byte_check(body))
            rep["title"] = first_text(body)
        except Exception as e:
            rep["r2_error"] = f"{type(e).__name__}: {e}"[:300]
    if rep.get("label") == "OK":
        now = datetime.datetime.utcnow()
        tx_rows.append((d["id"], url, d.get("resolved_url"), d.get("status_code"), d.get("content_type"),
                        d.get("native_id") or doi, d.get("native_id_namespace") or "doi", d["s3_path"],
                        bool(d.get("is_soft_block", False)), now, now, None))
        rep["taxicab_results_row"] = True
    report.append(rep)
    print(json.dumps(rep))

# COMMAND ----------

# MAGIC %md ## Write `taxicab_results` rows (stored, whole PDFs only)

# COMMAND ----------

schema = T.StructType([
    T.StructField("taxicab_id", T.StringType()), T.StructField("url", T.StringType()),
    T.StructField("resolved_url", T.StringType()), T.StructField("status_code", T.IntegerType()),
    T.StructField("content_type", T.StringType()), T.StructField("native_id", T.StringType()),
    T.StructField("native_id_namespace", T.StringType()), T.StructField("s3_path", T.StringType()),
    T.StructField("is_soft_block", T.BooleanType()), T.StructField("created_date", T.TimestampType()),
    T.StructField("processed_date", T.TimestampType()), T.StructField("error", T.StringType()),
])
if tx_rows and not DRY_RUN:
    spark.createDataFrame(tx_rows, schema).write.mode("append").format("delta").saveAsTable("openalex.taxicab.taxicab_results")
summary = {"dry_run": DRY_RUN, "checks": check_rows, "report": report,
           "taxicab_results_rows": [r[0] for r in tx_rows] if not DRY_RUN else []}
print(json.dumps({"taxicab_results_rows": summary["taxicab_results_rows"]}))
dbutils.notebook.exit(json.dumps(summary, default=str)[:900000])
