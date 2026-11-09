"""PMC harvest gap check (oxjob #2111). Read-only except its own result table.

Every released PubMed Central article is free to read, and OpenAlex gets PMC copies only from PMC's OAI feed
(endpoint ac9de7698155b820de7, openalex-ingest repositories.py). In Nov 2026 411,651 released articles had never been
harvested: the harvester checkpointed past windows it had not read (PMC lists a window out of datestamp order) and died
on a record with an empty datestamp every other day. This check compares NCBI's own list of every PMC article
(PMC-ids.csv.gz) with the PMC OAI records in openalex.repo.repo_works, weekly.

Fails (non-zero exit, so the job fails and the failure reaches Sentry through the Databricks bridge) when more than
MAX_MISSING released articles, older than the newest RECENT_PMCIDS (the normal 1-2 day harvest + parse lag), have no
repo_works row. The missing PMCIDs are written to openalex.repo.pmc_harvest_gap (one row per id per run) and the failure
prints the one command that re-harvests them (openalex-ingest backfill_by_id.py). A gap is recoverable: PMC serves
every article on GetRecord, so this catches it while it can still be fixed.

Job: jobs/pmc_harvest_gap_check.yaml. Locally (Databricks Connect):
    DATABRICKS_CONFIG_PROFILE=dbc-ce570f73-0362 python monitoring/checks/pmc_harvest_gap_check.py
"""
import datetime as dt
import json
import os
import shutil
import sys
import urllib.request

NCBI_URL = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz"
VOLUME_FILE = "/Volumes/openalex/transient/pmcid1862_vol/pmc_gap_check/PMC-ids-latest.csv.gz"
RESULT_TABLE = "openalex.repo.pmc_harvest_gap"
MAX_MISSING = int(os.getenv("MAX_MISSING", "5000"))
RECENT_PMCIDS = int(os.getenv("RECENT_PMCIDS", "50000"))
# Task parameters override, e.g. ["--max-missing", "500000"] for a test run while a backfill is still landing.
_args = sys.argv[1:]
for _flag, _name in (("--max-missing", "MAX_MISSING"), ("--recent-pmcids", "RECENT_PMCIDS")):
    if _flag in _args:
        globals()[_name] = int(_args[_args.index(_flag) + 1])
PMC_ENDPOINT = "ac9de7698155b820de7"

profile = os.getenv("DATABRICKS_CONFIG_PROFILE")
if os.getenv("DATABRICKS_RUNTIME_VERSION"):            # job task on Databricks
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
else:                                                   # laptop, Databricks Connect
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.profile(profile or "dbc-ce570f73-0362").serverless(True).getOrCreate()

now = dt.datetime.now(dt.timezone.utc)
os.makedirs(os.path.dirname(VOLUME_FILE), exist_ok=True)
req = urllib.request.Request(NCBI_URL, headers={"User-Agent": "OpenAlex PMC harvest gap check (support@openalex.org)"})
with urllib.request.urlopen(req, timeout=600) as r, open(VOLUME_FILE, "wb") as f:
    ncbi_modified = r.headers.get("Last-Modified")
    shutil.copyfileobj(r, f, 1 << 20)

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW pmc_live AS
SELECT DISTINCT cast(regexp_extract(upper(trim(PMCID)), '^PMC([0-9]+)$', 1) AS bigint) AS k
FROM read_files('{VOLUME_FILE}', format => 'csv', header => true,
     schema => 'Journal_Title STRING, ISSN STRING, eISSN STRING, Year STRING, Volume STRING, Issue STRING, Page STRING, DOI STRING, PMCID STRING, PMID STRING, `Manuscript Id` STRING, `Release Date` STRING')
WHERE `Release Date` = 'live' AND upper(trim(PMCID)) RLIKE '^PMC[0-9]+$'
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW pmc_harvested AS
SELECT DISTINCT cast(regexp_extract(native_id, '([0-9]+)$', 1) AS bigint) AS k
FROM openalex.repo.repo_works WHERE native_id LIKE 'oai:pubmedcentral.nih.gov:%'
""")
live = spark.sql("SELECT count(*) FROM pmc_live").collect()[0][0]
cutoff = spark.sql(f"SELECT min(k) FROM (SELECT k FROM pmc_live ORDER BY k DESC LIMIT {RECENT_PMCIDS})").collect()[0][0]
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW pmc_missing AS
SELECT l.k FROM pmc_live l LEFT ANTI JOIN pmc_harvested h ON h.k = l.k WHERE l.k < {cutoff}
""")
missing = spark.sql("SELECT count(*) FROM pmc_missing").collect()[0][0]
buckets = [r.asDict() for r in spark.sql("""
SELECT floor(k / 100000) * 100000 AS pmcid_from, count(*) AS missing FROM pmc_missing GROUP BY 1 ORDER BY 2 DESC LIMIT 10
""").collect()]

spark.sql(f"CREATE TABLE IF NOT EXISTS {RESULT_TABLE} (run_at TIMESTAMP, pmc_digits STRING)")
spark.sql(f"DELETE FROM {RESULT_TABLE} WHERE run_at < current_timestamp() - INTERVAL 90 DAYS")
spark.sql(f"INSERT INTO {RESULT_TABLE} SELECT timestamp'{now:%Y-%m-%d %H:%M:%S}', cast(k AS STRING) FROM pmc_missing")

report = {"when_utc": f"{now:%Y-%m-%dT%H:%M:%SZ}", "ncbi_list_last_modified": ncbi_modified, "live_pmc_articles": live,
          "newest_pmcids_skipped": RECENT_PMCIDS, "skipped_from_pmcid": cutoff, "missing_from_repo_works": missing,
          "max_missing": MAX_MISSING, "top_buckets": buckets}
print(json.dumps(report, indent=1, default=str))
if missing > MAX_MISSING:
    msg = (f"PMC HARVEST GAP: {missing:,} released PMC articles (older than the newest {RECENT_PMCIDS:,} PMCIDs) are not in "
           f"openalex.repo.repo_works (line {MAX_MISSING:,}); their works read closed though PMC serves them free. Ids: "
           f"{RESULT_TABLE} WHERE run_at = '{now:%Y-%m-%d %H:%M:%S}'. Fix: export them one per line to "
           f"s3://openalex-ingest/backfill-ids/pmc-gap-YYYYMMDD.txt, then on Heroku app openalex-ingest: python backfill_by_id.py "
           f"--endpoint-id {PMC_ENDPOINT} --ids s3://openalex-ingest/backfill-ids/pmc-gap-YYYYMMDD.txt --id-prefix "
           f"oai:pubmedcentral.nih.gov: --per-sec 2. Then look for the harvester bug (oxjob #2111 README, Root cause).")
    print(msg)
    sys.exit(msg)
print(f"OK: {missing:,} released PMC articles missing from repo_works (line {MAX_MISSING:,})")
