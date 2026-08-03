"""oxjob #682 generalized wave runner.

Usage:
  python qa/oxjob682_wave.py <family> <pmod> <keys> <num_shards> [max_workers]
  e.g. python qa/oxjob682_wave.py springer 9 0,1,2,3 4 640

Enqueues the family's universe rows with PMOD(HASH(doi), <pmod>) IN (<keys>), launches
<num_shards> concurrent Parseland Reparse shard runs, polls to completion, truncates the
queue on all-SUCCESS. Aborts if the queue is not empty (another wave in flight).
"""
import datetime
import subprocess
import sys
import time

from databricks.sdk import WorkspaceClient

JOB_ID = 790908172942128
WAREHOUSE = "3996dc0a9b183ce3"

FAMILY_PREDICATES = {
    "elsevier": "c.publisher IN ('Elsevier BV', 'Elsevier')",
    "springer": "c.publisher LIKE 'Springer%' AND c.publisher <> 'Springer Publishing Company'",
    "wiley": "c.publisher LIKE '%Wiley%'",
    "informa": "c.publisher IN ('Informa UK Limited', 'Routledge', 'CRC Press')",
    "ieee": "c.publisher IN ('IEEE', 'Institute of Electrical and Electronics Engineers (IEEE)')",
    "oup": "c.publisher IN ('Oxford University Press (OUP)', 'Oxford University Press')",
    "sage": "c.publisher LIKE 'SAGE%'",
    "cup": "c.publisher IN ('Cambridge University Press (CUP)', 'Cambridge University Press')",
    # tier-3 combined waves (enqueue with pmod=1 keys=0; shard at run time via num_shards)
    "ovid_degruyter": "c.publisher IN ('Ovid Technologies (Wolters Kluwer Health)', 'Walter de Gruyter GmbH', 'De Gruyter')",
    "acs_mdpi_cup": "c.publisher IN ('American Chemical Society (ACS)', 'MDPI AG', 'Cambridge University Press (CUP)', 'Cambridge University Press')",
    "midsize_a": "c.publisher IN ('JSTOR', 'IOP Publishing', 'Royal Society of Chemistry (RSC)', 'BMJ')",
    "midsize_b": "c.publisher IN ('AIP Publishing', 'American Physical Society (APS)', 'Frontiers Media SA', 'American Psychological Association (APA)', 'American Medical Association (AMA)')",
    "midsize_c": "c.publisher IN ('SPIE', 'Emerald', 'American Association for Cancer Research (AACR)', 'University of Chicago Press', 'Project MUSE', 'ENCODE Data Coordination Center', 'ACM', 'Copernicus GmbH')",
}

family = sys.argv[1]
pmod = int(sys.argv[2])
keys = "(" + sys.argv[3] + ")"
num_shards = int(sys.argv[4])
max_workers = sys.argv[5] if len(sys.argv) > 5 else "640"
predicate = FAMILY_PREDICATES.get(family) if family not in ('remaining_doi', 'remaining_nondoi', 'backfill_pmh', 'excluded_dois') else None

w = WorkspaceClient(profile="DEFAULT")


def sql(statement, timeout="50s"):
    r = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=WAREHOUSE, wait_timeout=timeout
    )
    while r.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(10)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"SQL {r.status.state.value}: {r.status.error}")
    return r


def scalar(statement):
    for attempt in range(3):
        r = sql(statement)
        if r.result is not None and r.result.data_array:
            return int(r.result.data_array[0][0])
        time.sleep(30)
    raise RuntimeError(f"no inline result after 3 attempts: {statement[:80]}")


def ecs_running():
    out = subprocess.run(
        ["aws", "ecs", "describe-services", "--cluster", "parseland", "--services",
         "parseland", "--region", "us-east-1", "--query", "services[0].runningCount",
         "--output", "text"],
        capture_output=True, text=True,
    )
    return out.stdout.strip()


def run_state(run_id):
    run = w.jobs.get_run(run_id)
    state = run.status.state.value if run.status else "?"
    result = ""
    if state == "TERMINATED" and run.status.termination_details:
        result = run.status.termination_details.code.value
    return state, result


def now():
    return datetime.datetime.now(datetime.timezone.utc)


def log(msg):
    print(f"{now().strftime('%H:%M:%S')}Z {msg}", flush=True)


n = scalar("SELECT COUNT(*) FROM openalex.parseland.reparse_queue")
if n != 0:
    log(f"ABORT: queue not empty ({n} rows)")
    raise SystemExit(1)

log(f"enqueueing {family} PMOD-{pmod} keys {keys}...")
if family == "excluded_dois":
    # Re-test of the #508-era exclusions (book DOIs 978-/979-, SSRN 10.2139, Thieme):
    # parsers have been rebuilt since June — pilot first, then full run if clean.
    enqueue_sql = rf"""
INSERT INTO openalex.parseland.reparse_queue (native_id, native_id_namespace, created_date)
SELECT doi, 'doi', current_timestamp()
FROM (
  SELECT DISTINCT u.doi
  FROM (
    SELECT lower(regexp_replace(native_id, '^https?://(dx\\.)?doi\\.org/', '')) AS doi
    FROM openalex.landing_page.landing_page_works_backfill
    WHERE native_id LIKE '%doi.org/%' AND array_contains(ids.namespace, 'html.gz')
    UNION
    SELECT lower(native_id)
    FROM openalex.taxicab.taxicab_results
    WHERE taxicab_id IS NOT NULL AND content_type LIKE '%html%' AND native_id_namespace = 'doi'
  ) u
  LEFT ANTI JOIN (
    SELECT DISTINCT lower(native_id) AS doi
    FROM openalex.parseland.parsed_pages
    WHERE parsed_date >= '2026-07-27T20:40:00Z' AND parsed_date < '2027-01-01'
      AND native_id_namespace = 'doi'
  ) p ON u.doi = p.doi
  LEFT JOIN openalex.crossref.crossref_works c ON u.doi = lower(c.native_id)
  WHERE (u.doi LIKE '10.2139/%'
      OR u.doi LIKE '%/978-%'
      OR u.doi LIKE '%/979-%'
      OR COALESCE(c.publisher, '') = 'Georg Thieme Verlag KG')
    AND PMOD(HASH(u.doi), {pmod}) IN {keys}
)
"""
elif family == "backfill_pmh":
    # Repo-side backfill reachable only via the pmh bridge (parseland.ipynb arm (c)).
    # Enqueue the pmh id itself; the resolver joins it to ids[namespace='pmh'] and uses the
    # backfill record's own native_id as the url. Anti-join on (native_id, namespace) so
    # waves self-dedupe against everything already parsed this campaign.
    enqueue_sql = rf"""
INSERT INTO openalex.parseland.reparse_queue (native_id, native_id_namespace, created_date)
SELECT pmh, 'pmh', current_timestamp()
FROM (
  SELECT DISTINCT b.pmh
  FROM (
    SELECT lower(filter(ids, x -> x.namespace = 'pmh')[0].id) AS pmh
    FROM openalex.landing_page.landing_page_works_backfill
    WHERE native_id NOT LIKE '%doi.org/%' AND array_contains(ids.namespace, 'html.gz')
  ) b
  LEFT ANTI JOIN (
    SELECT DISTINCT lower(native_id) AS pmh
    FROM openalex.parseland.parsed_pages
    WHERE parsed_date >= '2026-07-27T20:40:00Z' AND parsed_date < '2027-01-01'
      AND native_id_namespace <> 'doi'
  ) p ON b.pmh = p.pmh
  WHERE b.pmh IS NOT NULL
    AND PMOD(HASH(b.pmh), {pmod}) IN {keys}
)
"""
elif family == "remaining_nondoi":
    # pmh/non-DOI phase: taxicab-scraped HTML only (backfill resolver is DOI-keyed).
    # Enqueue with the record's real namespace; anti-join on (native_id, namespace).
    enqueue_sql = rf"""
INSERT INTO openalex.parseland.reparse_queue (native_id, native_id_namespace, created_date)
SELECT native_id, native_id_namespace, current_timestamp()
FROM (
  SELECT DISTINCT t.native_id, t.native_id_namespace
  FROM openalex.taxicab.taxicab_results t
  LEFT ANTI JOIN (
    SELECT DISTINCT native_id, native_id_namespace
    FROM openalex.parseland.parsed_pages
    WHERE parsed_date >= '2026-07-27T20:40:00Z' AND parsed_date < '2027-01-01'
  ) p ON t.native_id = p.native_id AND t.native_id_namespace = p.native_id_namespace
  WHERE t.taxicab_id IS NOT NULL AND t.content_type LIKE '%html%'
    AND t.native_id_namespace <> 'doi'
    AND PMOD(HASH(t.native_id), {pmod}) IN {keys}
)
"""
elif family == "remaining_doi":
    # Long-tail sweep: everything in the DOI universe not parsed since campaign
    # start. No crossref requirement (keeps no-publisher + non-crossref DOIs);
    # crossref joined only to exclude Thieme. Anti-join makes waves self-deduping.
    enqueue_sql = rf"""
INSERT INTO openalex.parseland.reparse_queue (native_id, native_id_namespace, created_date)
SELECT doi, 'doi', current_timestamp()
FROM (
  SELECT DISTINCT u.doi
  FROM (
    SELECT lower(regexp_replace(native_id, '^https?://(dx\\.)?doi\\.org/', '')) AS doi
    FROM openalex.landing_page.landing_page_works_backfill
    WHERE native_id LIKE '%doi.org/%' AND array_contains(ids.namespace, 'html.gz')
    UNION
    SELECT lower(native_id)
    FROM openalex.taxicab.taxicab_results
    WHERE taxicab_id IS NOT NULL AND content_type LIKE '%html%' AND native_id_namespace = 'doi'
  ) u
  LEFT ANTI JOIN (
    SELECT DISTINCT lower(native_id) AS doi
    FROM openalex.parseland.parsed_pages
    WHERE parsed_date >= '2026-07-27T20:40:00Z' AND parsed_date < '2027-01-01'
      AND native_id_namespace = 'doi'
  ) p ON u.doi = p.doi
  LEFT JOIN openalex.crossref.crossref_works c ON u.doi = lower(c.native_id)
  WHERE u.doi NOT LIKE '10.2139/%'
    AND u.doi NOT LIKE '%/978-%'
    AND u.doi NOT LIKE '%/979-%'
    AND COALESCE(c.publisher, '') <> 'Georg Thieme Verlag KG'
    AND PMOD(HASH(u.doi), {pmod}) IN {keys}
)
"""
else:
    enqueue_sql = rf"""
INSERT INTO openalex.parseland.reparse_queue (native_id, native_id_namespace, created_date)
SELECT doi, 'doi', current_timestamp()
FROM (
  SELECT DISTINCT u.doi
  FROM (
    SELECT lower(regexp_replace(native_id, '^https?://(dx\\.)?doi\\.org/', '')) AS doi
    FROM openalex.landing_page.landing_page_works_backfill
    WHERE native_id LIKE '%doi.org/%' AND array_contains(ids.namespace, 'html.gz')
    UNION
    SELECT lower(native_id)
    FROM openalex.taxicab.taxicab_results
    WHERE taxicab_id IS NOT NULL AND content_type LIKE '%html%' AND native_id_namespace = 'doi'
  ) u
  JOIN openalex.crossref.crossref_works c ON u.doi = lower(c.native_id)
  WHERE {predicate}
    AND u.doi NOT LIKE '10.2139/%'
    AND u.doi NOT LIKE '%/978-%'
    AND u.doi NOT LIKE '%/979-%'
    AND PMOD(HASH(u.doi), {pmod}) IN {keys}
)
"""
sql(enqueue_sql, timeout="0s")
queued = scalar("SELECT COUNT(*) FROM openalex.parseland.reparse_queue")
log(f"queued {queued:,} records")

wave_start = now()
cutoff = wave_start.strftime("%Y-%m-%dT%H:%M:%SZ")
runs = {}
for shard in range(num_shards):
    r = w.jobs.run_now(
        job_id=JOB_ID,
        job_parameters={
            "reparse_queue_only": "true",
            "max_workers": max_workers,
            "num_shards": str(num_shards),
            "shard_id": str(shard),
        },
    )
    runs[shard] = r.run_id
    log(f"launched shard {shard}: run {r.run_id}")

poll = 0
last_n = 0
last_t = time.time()
while True:
    time.sleep(300)
    poll += 1
    states = {s: run_state(rid) for s, rid in runs.items()}
    done = {s: st for s, st in states.items() if st[0] in ("TERMINATED", "INTERNAL_ERROR")}
    n = scalar(
        f"SELECT COUNT(*) FROM openalex.parseland.parsed_pages WHERE parsed_date >= '{cutoff}'"
    )
    t = time.time()
    rate = (n - last_n) / (t - last_t) if t > last_t else 0
    last_n, last_t = n, t
    if poll % 2 == 0 or len(done) == len(runs) or (poll <= 4 and rate > 0):
        eta_min = (queued - n) / rate / 60 if rate > 0 else -1
        log(f"parsed={n:,}/{queued:,} rate={rate:.0f}/s ecs={ecs_running()} "
            f"done={len(done)}/{num_shards} eta={eta_min:.0f}m")
    for s, (st, res) in states.items():
        if st == "TERMINATED" and res and res != "SUCCESS":
            log(f"WARNING shard {s} run {runs[s]}: {res}")
    if len(done) == len(runs):
        results = {s: st[1] for s, st in done.items()}
        log(f"all shards finished: {results}")
        if all(v == "SUCCESS" for v in results.values()):
            sql("TRUNCATE TABLE openalex.parseland.reparse_queue")
            log("queue truncated (wave cleanup)")
        else:
            log("NOT truncating queue - failed shard(s) can be relaunched")
        total_min = (now() - wave_start).total_seconds() / 60
        log(f"WAVE DONE ({family} {keys}): {n:,} parsed in {total_min:.0f} min "
            f"(avg {n/(total_min*60):.0f}/s)")
        break
