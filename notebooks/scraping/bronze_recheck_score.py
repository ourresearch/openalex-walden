# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze re-check: score fresh fetches against the register (oxjob #1290)
# MAGIC
# MAGIC Reads every fresh `taxicab_results` row for this pass's URLs (`bronze_recheck_draw` drew them; the
# MAGIC TaxiCab Campaign guard fetches them; ordinary taxicab runs count too) and gives each one a verdict:
# MAGIC
# MAGIC | verdict | when |
# MAGIC |---|---|
# MAGIC | `open` | a PDF came back, or Chrome's PDF-viewer stub (a PDF the harvester saved as HTML) |
# MAGIC | `not_open` | the URL now lands on its host's verified paywall shape (`RULES`), with the host's own access block on the page where one is listed; or a 404/410 |
# MAGIC | `no_information` | anything else: errors, blocks, interstitials, HTML that matches no shape |
# MAGIC
# MAGIC Only `not_open` can close a work, and only once its register group is **cleared** in `gate_table`
# MAGIC (a person hand-checked fresh `not_open` rows of that group: oxjob #1290 ACCEPTANCE Test 2). Cleared
# MAGIC rows go to `openalex.pdf.pdf_removed`, which `PDF.py` reads on every rebuild, so writing there is the
# MAGIC commit point; `bronze_recheck_apply` then flips the existing `pdf_works` rows. Phrase markers never
# MAGIC close on their own (#1105): a marker here is only a second condition on a host-verified URL shape.
# MAGIC Register evidence: oxjob #1105 README (current hosts), oxjob #1290 EXPLORE § 4 (new hosts).

# COMMAND ----------

import gzip
import re
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import functions as F

dbutils.widgets.text("pass_id", "2026-10", "Pass id")
dbutils.widgets.text("batch_table", "openalex.taxicab.bronze_recheck_batch", "Batch table")
dbutils.widgets.text("results_table", "openalex.taxicab.bronze_recheck_results", "Results table (append-only)")
dbutils.widgets.text("gate_table", "openalex.taxicab.bronze_recheck_host_gate", "Groups cleared to close")
dbutils.widgets.text("fresh_evidence_days", "14", "Fetches this many days before the draw still count")
dbutils.widgets.text("max_bodies", "200000", "Max HTML bodies read from R2 per run")
dbutils.widgets.text("threads", "32", "R2 reader threads")
dbutils.widgets.text("removed_table", "openalex.pdf.pdf_removed", "Closures table (PDF.py reads it); tests point elsewhere")
dbutils.widgets.text("dry_run", "true", "true = print verdicts only, write nothing")

pass_id = dbutils.widgets.get("pass_id").strip()
batch_table = dbutils.widgets.get("batch_table").strip()
results_table = dbutils.widgets.get("results_table").strip()
gate_table = dbutils.widgets.get("gate_table").strip()
fresh_evidence_days = int(dbutils.widgets.get("fresh_evidence_days"))
max_bodies = int(dbutils.widgets.get("max_bodies"))
threads = int(dbutils.widgets.get("threads"))
removed_table = dbutils.widgets.get("removed_table").strip()
dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"

RULE_VERSION = "2026-10-25"
SOURCE = f"oxjob #1290 bronze re-check {pass_id}"

# COMMAND ----------

# The register: group -> list of (resolved-URL regex, required page regex or None).
# "AT_PDF_URL" = the fetch returned HTML at the PDF URL itself (no redirect, query string ignored).
# Current hosts: #1105 (hand-checked live by Casey, Sep 2026). New hosts: #1290 EXPLORE § 4 (25 stored
# pages each, Oct 2026). Wiley and IOP never appear: the harvester gets files there readers can't (#1291).
NATURE = r"^https?://www\.nature\.com/articles/[^/?#]+$"
SD_ABS = r"^https?://www\.sciencedirect\.com/science/article/abs/pii/"
ELS_SOCIETY_ABS = r"^https?://[a-z0-9.-]+/article/S?[0-9]{4}-[0-9]{3}[0-9X]\([0-9]{2}\)[0-9A-Z-]+/abstract"
ELS_ACCESS = r"Get Access|Purchase"
OUP_ABS = r"^https?://academic\.oup\.com/.*article-abstract"
# Silverchair's own access block on the article (ACS moved to Silverchair in 2026: its PDF links now redirect to
# /<journal>/article-abstract/...?redirectedFrom=PDF). The site menu also says "Purchase", so match the block only.
SILVERCHAIR_ACCESS = r"Pay-Per-View Access|You do not currently have access to this content"
RULES = {
    "oup": [(OUP_ABS, None)],
    "aip": [(r"^https?://pubs\.aip\.org/.*article-abstract", None)],
    "springer": [(r"^https?://link\.springer\.com/article/", None)],
    "aps": [(r"^https?://(journals|link)\.aps\.org/.*/abstract/", None)],  # PDFs resolve to journals.aps.org/<j>/abstract/
    "cambridge": [(r"^https?://www\.cambridge\.org/.*/article/abs/", None)],
    "sciencedirect": [(SD_ABS, None)],
    "nature": [(NATURE, None)],
    # Not OUP's shape for T&F journals that moved to OUP (Biosci Biotechnol Biochem): the harvester is redirected to
    # OUP's abstract, but a reader's browser gets T&F's PDF (reader check 25 Oct, 2 of 2).
    "tandf": [("AT_PDF_URL", None)],
    "sage": [(r"^https?://journals\.sagepub\.com/doi/abs/", r'class="meta-panel__access[^"]*"[^>]*>(?:\s|<[^>]+>)*Restricted access')],  # the article's own badge, not a teaser
    "aha": [(r"^https?://www\.ahajournals\.org/doi/abs/", r"get full access to this article")],
    "karger": [(r"^https?://(www\.)?karger\.com/[a-z]+/article-abstract/", r"Pay-Per-View|BUY THIS")],
    "degruyter": [(r"^https?://www\.degruyter(brill)?\.com/unauthorizedDownload/", None)],
    "iucr": [(r"^https?://journals\.iucr\.org/paper\?.*buy=y", r"Buy article")],
    "acs": [("AT_PDF_URL", r"please review the available access options"),
            (r"^https?://pubs\.acs\.org/[a-z0-9]+/article-abstract/", SILVERCHAIR_ACCESS)],
    "thieme": [(r"^https?://www\.thieme-connect\.(de|com)/products/ejournals/abstract/", r"Buy Article")],
    "elsevier_society": [(ELS_SOCIETY_ABS, ELS_ACCESS)],
    "elsevier_doiorg": [(SD_ABS, None), (ELS_SOCIETY_ABS, ELS_ACCESS)],
    # Pending a live check (EXPLORE § 4): scored, but leave them out of gate_table until checked.
    "ieee": [(r"^https?://ieeexplore\.ieee\.org/Xplore/login\.jsp\?.*authDecision=-203", None)],
    "acm": [(r"^https?://dl\.acm\.org/doi/abs/", r"Get Access")],
}
# Closed side only (as #1105 treated IOP): the harvester got the PDF AND the paywall page for the same URL
# (share of re-fetched paywall-shape URLs that also served a PDF, Oct 2025-Oct 2026: Sage 28%, IUCr 21%,
# Thieme 7%, Karger 6%, De Gruyter 4/8, IEEE 10/16). The paywall page offers a purchase or carries a
# "Restricted access" article badge, so the article isn't free to the public and the PDF came through access
# a reader doesn't have (#1291). Here a not_open in the pass wins over any PDF fetch.
CLOSED_SIDE_ONLY = {"sage", "iucr", "thieme", "karger", "degruyter", "ieee"}
NO_404_CLOSE = {"elsevier_doiorg"}  # a doi.org 404 says the DOI didn't resolve, not that the file is gone

CHROME_STUB = re.compile(r"chrome-extension://mhjfbmdgcfjbbpaeojofohoefgiehjai|type=\"application/pdf\"", re.I)
INTERSTITIAL = re.compile(r"please wait|one moment|bot check|are you a robot|just a moment|verify you are human|access denied"
                          r"|attention required|checking your browser|captcha|perfdrive|validate\.|validate user|where are you from"
                          r"|pardon our interruption|liblynx\.com/wayf|crawlprevention", re.I)


def strip_query(u):
    return re.sub(r"[?#].*$", "", u or "")


def shape_matches(grp, pdf_url, resolved_url):
    """Index of the first matching rule for this group, or None."""
    for i, (shape, _) in enumerate(RULES.get(grp, [])):
        if shape == "AT_PDF_URL":
            if not resolved_url or strip_query(resolved_url) == strip_query(pdf_url):
                return i
        elif re.search(shape, resolved_url or "", re.I):
            return i
    return None

# COMMAND ----------

# Fresh fetches of this pass's URLs that have not been scored yet.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {results_table} (
  pass_id STRING, work_id STRING, doi STRING, pdf_url STRING, grp STRING, stratum STRING,
  taxicab_id STRING, processed_date TIMESTAMP, status_code INT, content_type STRING, resolved_url STRING,
  is_soft_block BOOLEAN, error STRING, s3_path STRING, body_class STRING, title STRING, body_bytes INT,
  verdict STRING, reason STRING, rule_version STRING, cleared BOOLEAN, scored_at TIMESTAMP) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {gate_table} (
  grp STRING, rule_version STRING, cleared_at TIMESTAMP, cleared_by STRING, evidence STRING) USING DELTA""")

batch = spark.table(batch_table).filter(F.col("pass_id") == pass_id)
if batch.limit(1).count() == 0:
    dbutils.notebook.exit(f"no batch rows for pass {pass_id}")
since = batch.agg(F.min("drawn_at")).collect()[0][0]
bad = batch.filter(F.col("grp").isin("wiley", "iop") | ~F.col("grp").isin(list(RULES))).count()
assert bad == 0, f"{bad} batch rows outside the register; refusing to score"

batch.select("pdf_url").distinct().createOrReplaceTempView("batch_urls")
fresh = spark.sql(f"""
SELECT t.url AS pdf_url, t.taxicab_id, t.processed_date, t.status_code, t.content_type, t.resolved_url,
       t.is_soft_block, t.error, t.s3_path
FROM openalex.taxicab.taxicab_results t
WHERE t.processed_date >= timestamp'{since}' - INTERVAL {fresh_evidence_days} DAYS
  AND t.url IN (SELECT pdf_url FROM batch_urls)
""")
# keyed on (url, fetch time): failed fetches carry no taxicab_id
scored = spark.table(results_table).filter(F.col("pass_id") == pass_id).select("pdf_url", "processed_date")
todo = (fresh.join(scored, ["pdf_url", "processed_date"], "left_anti")
        .join(batch.select("pdf_url", "work_id", "doi", "grp", "stratum"), "pdf_url")
        .filter(F.col("processed_date").isNotNull()))

BLOCKED = ("coalesce(is_soft_block, false) OR status_code IN (202, 403, 429) "
           "OR coalesce(resolved_url, '') LIKE '%fbi.gov%'")


def shape_sql():
    """SQL twin of shape_matches(): index of the first matching rule per group, else NULL."""
    whens = []
    for grp, rules in RULES.items():
        for i, (shape, _) in enumerate(rules):
            if shape == "AT_PDF_URL":
                cond = ("(resolved_url IS NULL OR regexp_replace(resolved_url, '[?#].*$', '') = "
                        "regexp_replace(pdf_url, '[?#].*$', ''))")
            else:
                cond = "coalesce(resolved_url, '') RLIKE '(?i)" + shape.replace("\\", "\\\\").replace("'", "\\'") + "'"
            whens.append(f"WHEN grp = '{grp}' AND {cond} THEN {i}")
    return "CASE " + " ".join(whens) + " ELSE NULL END"


pre = todo.withColumn("shape_idx", F.expr(shape_sql())).withColumn("pre", F.expr(f"""
  CASE WHEN error IS NOT NULL THEN 'no_information:harvester error'
       WHEN {BLOCKED} THEN 'no_information:blocked'
       WHEN status_code IN (404, 410) AND grp NOT IN ({",".join("'" + g + "'" for g in NO_404_CLOSE)}) THEN 'not_open:404'
       WHEN status_code <> 200 THEN concat('no_information:http ', coalesce(cast(status_code AS STRING), 'none'))
       WHEN lower(coalesce(content_type, '')) LIKE '%pdf%' THEN 'open:pdf'
       WHEN lower(coalesce(content_type, '')) LIKE '%html%' AND shape_idx IS NULL THEN 'no_information:html, no register shape'
       WHEN lower(coalesce(content_type, '')) LIKE '%html%' THEN 'body'
       ELSE concat('no_information:content ', coalesce(content_type, 'none')) END"""))
# Materialize once (serverless has no .cache()); everything below reads the stage.
STAGE = f"{results_table}_stage"
pre.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STAGE)
pre = spark.table(STAGE)
counts = {r["pre"]: r["count"] for r in pre.groupBy("pre").count().collect()}
need_body = counts.get("body", 0)
print(f"pass {pass_id}: {sum(counts.values()):,} fresh fetches to score; {need_body:,} need a page body")
if need_body > max_bodies:
    # Score in parts: the oldest max_bodies pages now, the rest stay unscored for the next run.
    later = pre.filter("pre = 'body'").orderBy(F.desc("processed_date")).limit(need_body - max_bodies)
    pre = pre.join(later.select("pdf_url", "processed_date"), ["pdf_url", "processed_date"], "left_anti")
    print(f"  reading {max_bodies:,} bodies this run; {need_body - max_bodies:,} wait for the next run")

# COMMAND ----------

R2_ENDPOINT = dbutils.secrets.get("meta-tags-r2", "r2-endpoint")
if not R2_ENDPOINT.startswith("http"):
    R2_ENDPOINT = "https://" + R2_ENDPOINT
R2_KEY = dbutils.secrets.get("meta-tags-r2", "r2-access-key-id")
R2_SECRET = dbutils.secrets.get("meta-tags-r2", "r2-secret-access-key")
_client = None


def r2():
    global _client
    if _client is None:
        import boto3
        from botocore.config import Config
        _client = boto3.client("s3", endpoint_url=R2_ENDPOINT, aws_access_key_id=R2_KEY, aws_secret_access_key=R2_SECRET,
                               config=Config(retries={"max_attempts": 3}, connect_timeout=10, read_timeout=60,
                                             max_pool_connections=threads * 2))
    return _client


def read_body(s3_path):
    m = re.match(r"s3://([^/]+)/(.+)", s3_path or "")
    if not m:
        return None, "r2_no_path"
    try:
        b = r2().get_object(Bucket=m.group(1), Key=m.group(2))["Body"].read()
        if s3_path.endswith(".gz"):
            b = gzip.decompress(b)
        return b.decode("utf-8", "ignore"), None
    except Exception as e:
        return None, "r2_" + type(e).__name__


def judge(r):
    """Final verdict for one fresh fetch: (verdict, reason, body_class, title, body_bytes)."""
    i = shape_matches(r["grp"], r["pdf_url"], r["resolved_url"])
    if i is None or i != r["shape_idx"]:  # Python and SQL regexes must agree
        return "no_information", "shape check disagreed (python vs sql)", None, None, None
    body, err = read_body(r["s3_path"])
    if body is None:
        return "no_information", f"body unread ({err})", err, None, None
    tt = re.search(r"<title[^>]*>(.*?)</title>", body, re.I | re.S)
    title = re.sub(r"\s+", " ", tt.group(1)).strip()[:200] if tt else ""
    n = len(body)
    if n < 3000 and CHROME_STUB.search(body):
        return "open", "chrome pdf stub", "stub", title, n
    if INTERSTITIAL.search(title) or (n < 20000 and INTERSTITIAL.search(body)):
        return "no_information", "interstitial", "interstitial", title, n
    marker = RULES[r["grp"]][i][1]
    if marker and not re.search(marker, body, re.I):
        return "no_information", "register shape, no access block", "page", title, n
    return "not_open", "register shape", "page", title, n


COLS = ["pdf_url", "taxicab_id", "work_id", "doi", "grp", "stratum", "processed_date", "status_code", "content_type",
        "resolved_url", "is_soft_block", "error", "s3_path"]
body_rows = [r.asDict() for r in pre.filter("pre = 'body'").select(*COLS, "shape_idx").collect()]  # bounded by max_bodies
with ThreadPoolExecutor(threads) as ex:
    judged = list(ex.map(judge, body_rows))
read = spark.createDataFrame(
    [(r["taxicab_id"], v, why, bc, title, nbytes) for r, (v, why, bc, title, nbytes) in zip(body_rows, judged)],
    "taxicab_id STRING, b_verdict STRING, b_reason STRING, body_class STRING, title STRING, body_bytes INT")

cleared = [r["grp"] for r in spark.table(gate_table).filter(F.col("rule_version") == RULE_VERSION).collect()]
res = (pre.join(read, "taxicab_id", "left")
       .withColumn("verdict", F.when(F.col("pre") == "body", F.col("b_verdict")).otherwise(F.split("pre", ":").getItem(0)))
       .withColumn("reason", F.when(F.col("pre") == "body", F.col("b_reason")).otherwise(F.expr("substring_index(pre, ':', -1)")))
       .withColumn("pass_id", F.lit(pass_id)).withColumn("rule_version", F.lit(RULE_VERSION))
       .withColumn("cleared", F.col("grp").isin(cleared) if cleared else F.lit(False))
       .withColumn("scored_at", F.current_timestamp())
       .select(*spark.table(results_table).columns))
res.groupBy("grp", "verdict").count().orderBy("grp", "verdict").show(200, truncate=False)

# COMMAND ----------

if dry_run:
    dbutils.notebook.exit(f"dry run: {sum(counts.values()):,} fetches scored, nothing written")

res.write.mode("append").saveAsTable(results_table)

# Newest verdict per URL decides (closed-side-only groups: any not_open); only cleared groups reach pdf_removed.
# Cleared is read from gate_table now, not from the row's `cleared` (the gate as it stood at scoring), so
# rows scored before their group was cleared close at the first run after it.
spark.sql(f"""
MERGE INTO {removed_table} t
USING (
  SELECT pdf_url, doi, grp, resolved_url, status_code, reason, processed_date
  FROM (SELECT *, row_number() OVER (PARTITION BY pdf_url ORDER BY
                 (verdict = 'not_open' AND grp IN ({",".join("'" + g + "'" for g in CLOSED_SIDE_ONLY)})) DESC,
                 processed_date DESC) AS rn
        FROM {results_table} WHERE pass_id = '{pass_id}' AND verdict IN ('open', 'not_open'))
  WHERE rn = 1 AND verdict = 'not_open'
    AND grp IN (SELECT grp FROM {gate_table} WHERE rule_version = '{RULE_VERSION}')
) s ON t.pdf_url = s.pdf_url
WHEN NOT MATCHED THEN INSERT (pdf_url, doi, host, resolved_url, status_code, reason, checked_at, source, applied_at)
  VALUES (s.pdf_url, s.doi, s.grp, s.resolved_url, s.status_code, s.reason, s.processed_date, '{SOURCE}', NULL)
""")
pending = spark.sql(f"SELECT count(*) FROM {removed_table} WHERE source = '{SOURCE}' AND applied_at IS NULL").collect()[0][0]
held = spark.sql(f"""SELECT grp, count(DISTINCT pdf_url) FROM {results_table}
                     WHERE pass_id = '{pass_id}' AND verdict = 'not_open'
                       AND grp NOT IN (SELECT grp FROM {gate_table} WHERE rule_version = '{RULE_VERSION}')
                     GROUP BY grp""").collect()
print(f"pdf_removed: {pending:,} rows from {SOURCE} waiting for bronze_recheck_apply")
print(f"not_open held back (group not cleared): {[(r[0], r[1]) for r in held]}")
