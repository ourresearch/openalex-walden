# Databricks notebook source
# MAGIC %md
# MAGIC # PubMed book records harvest (oxjob #1104)
# MAGIC
# MAGIC PubMed holds ~40K book records (`PubmedBookArticle`: NCBI Bookshelf parts such as StatPearls and GeneReviews
# MAGIC chapters, and whole books). NLM's baseline and update files, which openalex-ingest `pubmed.py` copies to
# MAGIC `s3://openalex-ingest/pubmed/`, carry none of them, so they never became works. This notebook fetches them
# MAGIC through NCBI E-utilities and writes gzipped `PubmedArticleSet` XML files to
# MAGIC `s3://openalex-ingest/pubmed_books/<run date>/`, which `PubMed.py` reads (`pubmed_book_items` ->
# MAGIC `pubmed_book_parsed` -> `pubmed_book_enriched` -> its own CDC flow into `pubmed_works`).
# MAGIC
# MAGIC - First run (no folder under the prefix yet): every book record, `"pubmed books"[filter]`.
# MAGIC - Later runs: records modified since the newest folder's date minus `overlap_days` (`[mdat]`), a handful a day.
# MAGIC
# MAGIC ESearch returns at most 10,000 ids per query, so ids are collected per PMID range, halving any range that holds
# MAGIC 9,999 or more. EFetch takes 200 ids per POST. 3 requests a second without an API key (NCBI's limit).
# MAGIC Batch-job rules: progress per file with rate and ETA; a file is written whole or not at all; a rerun on the same
# MAGIC day skips the files already in its folder.

# COMMAND ----------

import datetime
import gzip
import json
import os
import time
import urllib.parse
import urllib.request

dbutils.widgets.text("prefix", "s3://openalex-ingest/pubmed_books/", "output prefix")
dbutils.widgets.text("since", "", "fetch records modified since YYYY/MM/DD; empty = from the newest folder, 'all' = everything")
dbutils.widgets.text("overlap_days", "2", "days of overlap with the newest folder")
dbutils.widgets.text("per_file", "2000", "records per output file")
PREFIX = dbutils.widgets.get("prefix").rstrip("/") + "/"
SINCE = dbutils.widgets.get("since").strip()
OVERLAP = int(dbutils.widgets.get("overlap_days"))
PER_FILE = int(dbutils.widgets.get("per_file"))

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
TERM = '"pubmed books"[filter]'
PAUSE = 0.4  # 3 requests a second, NCBI's limit without an API key
UA = {"User-Agent": "openalex-ingest/1.0"}
HEAD = b'<?xml version="1.0" ?>\n<PubmedArticleSet>\n'
TAIL = b"\n</PubmedArticleSet>\n"

# COMMAND ----------


def call(util, params, post=False, tries=6):
    data = urllib.parse.urlencode(dict(params, tool="openalex-ingest")).encode()
    for i in range(tries):
        try:
            req = (urllib.request.Request(EUTILS + util, data=data, headers=UA) if post
                   else urllib.request.Request(EUTILS + util + "?" + data.decode(), headers=UA))
            with urllib.request.urlopen(req, timeout=180) as r:
                body = r.read()
            time.sleep(PAUSE)
            return body
        except Exception as e:  # 429, 5xx, timeouts: back off and retry
            print(f"  {util} retry {i + 1}: {e}")
            time.sleep(5 * (i + 1))
    raise RuntimeError(f"{util} failed after {tries} tries")


def esearch(term, retmax):
    r = json.loads(call("esearch.fcgi", {"db": "pubmed", "term": term, "retmax": retmax, "retmode": "json"}))["esearchresult"]
    return int(r["count"]), r.get("idlist", [])


def collect_ids(term, lo=1, hi=60_000_000):
    n, ids = esearch(f"{term} AND {lo}:{hi}[pmid]", 9999)
    if n < 9999:
        return ids
    mid = (lo + hi) // 2
    return collect_ids(term, lo, mid) + collect_ids(term, mid + 1, hi)


def folders():
    try:
        return sorted(f.name.rstrip("/") for f in dbutils.fs.ls(PREFIX) if f.name.endswith("/"))
    except Exception as e:  # the prefix doesn't exist before the first run
        if "FileNotFoundException" in str(e) or "does not exist" in str(e):
            return []
        raise


# COMMAND ----------

today = datetime.date.today().isoformat()
existing = folders()
if SINCE == "all" or (not SINCE and not existing):
    term = TERM
elif SINCE:
    term = f'{TERM} AND ("{SINCE}"[mdat] : "3000"[mdat])'
else:
    start = datetime.date.fromisoformat(existing[-1]) - datetime.timedelta(days=OVERLAP)
    term = f'{TERM} AND ("{start:%Y/%m/%d}"[mdat] : "3000"[mdat])'
total, _ = esearch(term, 0)
ids = sorted(set(collect_ids(term)), key=int)
print(f"query {term}: count {total}, collected {len(ids)}")
assert len(ids) == total, f"collected {len(ids)} of {total} ids: a PMID range held 9,999+ after splitting?"

out_dir = f"{PREFIX}{today}/"
done_files = set()
if today in existing:
    done_files = {f.name for f in dbutils.fs.ls(out_dir)}
local = "/local_disk0/tmp/pubmed_books" if os.path.isdir("/local_disk0") else "/tmp/pubmed_books"
os.makedirs(local, exist_ok=True)
t0, done = time.time(), 0
for f_i in range(0, len(ids), PER_FILE):
    chunk = ids[f_i:f_i + PER_FILE]
    name = f"pubmed_books_{today}_{f_i // PER_FILE + 1:04d}.xml.gz"
    if name in done_files:
        done += len(chunk)
        continue
    path, n_rec = os.path.join(local, name), 0
    with gzip.open(path, "wb") as g:
        g.write(HEAD)
        for b_i in range(0, len(chunk), 200):
            body = call("efetch.fcgi", {"db": "pubmed", "id": ",".join(chunk[b_i:b_i + 200]), "retmode": "xml"}, post=True)
            s, e = body.find(b"<PubmedArticleSet>"), body.rfind(b"</PubmedArticleSet>")
            if s < 0 or e < 0:
                raise RuntimeError(f"no PubmedArticleSet in the efetch reply for ids from {chunk[b_i]}")
            inner = body[s + len(b"<PubmedArticleSet>"):e]
            n_rec += inner.count(b"<PubmedBookArticle>")
            g.write(inner)
        g.write(TAIL)
    # written whole, then copied: the stream never sees a partial file
    dbutils.fs.cp(f"file:{path}", out_dir + name)
    os.remove(path)
    done += len(chunk)
    rate = done / max(time.time() - t0, 1)
    print(f"{name}: {n_rec} book records | {done}/{len(ids)} | {rate:.0f}/s | ETA {(len(ids) - done) / max(rate, 0.01) / 60:.1f} min")
print(f"done: {len(ids)} records into {out_dir}")
