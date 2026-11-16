# Databricks notebook source
# MAGIC %md
# MAGIC # BuildUnregisteredDois: DOIs doi.org says do not exist (oxjob #1656)
# MAGIC
# MAGIC Builds `openalex.works.unregistered_dois`, one row per lower-case DOI that a non-Crossref/DataCite record (MAG,
# MAGIC repository, PubMed, ...) carries and that names no work: the doi.org handle API answers "not found"
# MAGIC (`responseCode` 100), or the DOI is on DataCite's test prefix 10.5072 (every handle there points at
# MAGIC datacite.org/testprefix). `CreateLocationsMapped` hides such a DOI (`best_doi` NULL on that record), so it shows on
# MAGIC neither the record's location nor the work (`doi`, `ids.doi`). The record stays on its work; work ids never move.
# MAGIC
# MAGIC Why (#1616 sizing, V=17070, 10 Oct 2026): ~2.23M works showed a Crossref-prefix DOI doi.org and the Crossref API do not
# MAGIC know (10.4324 MAG chapters 156K, 10.5555 ACM pseudo-DOIs 71K, 10.1056 NEJM Journal Watch 50K, 10.1093 RePEc 40K,
# MAGIC 10.5209, OJS journals that never deposited), ~66K on unknown prefixes, 12K on 10.5072. Live re-measure 16 Nov 2026
# MAGIC (#1656 work/prefix_sample_2026-11-16.jsonl): of the sampled DOIs no Crossref/DataCite record holds, 10.4324 18/18,
# MAGIC 10.5555 196/196, 10.1056 44/45, 10.5209 89/89 answer 100; 10.5072 200/200 point at the test-prefix page.
# MAGIC
# MAGIC **Scope.** Candidates are DOIs held by no Crossref or DataCite record. Only prefixes whose registration agency
# MAGIC (doi.org/ra) is Crossref or DataCite, or that doi.org does not know, are checked per DOI: DOIs of agencies we do not
# MAGIC harvest (JaLC, mEDRA, KISTI, CNKI, ISTIC, Airiti, OP: ~3.24M, 92-100% real) are left alone (`include_other_ra=yes`
# MAGIC checks them too). Never pattern-only: `10.4324/<isbn>-NN` exists for real chapters and MAG also carries unregistered
# MAGIC ones in that form, so every DOI is checked.
# MAGIC
# MAGIC Modes (run in this order; each is safe to re-run):
# MAGIC - `candidates`  `openalex.transient.oxjob1656_candidates` from `locations_w_types` (the parse, before
# MAGIC                 CreateLocationsMapped's override, so a re-run rebuilds the same rows); prints counts by prefix.
# MAGIC - `prefixes`    doi.org/ra per distinct prefix -> `openalex.transient.oxjob1656_prefix_ra` (~10K calls, minutes).
# MAGIC - `check`       doi.org handle API per in-scope candidate not yet checked -> `openalex.transient.oxjob1656_handle_checks`,
# MAGIC                 `batch` DOIs per append, `threads` at once. Resumable: stop it any time, run it again.
# MAGIC                 ~100 checks/s at 16 threads from desk on 16 Nov (median 0.07-0.15 s each, no 429s).
# MAGIC - `publish`     `openalex.works.unregistered_dois` = every checked DOI with responseCode 100 + the 10.5072 candidates
# MAGIC                 + the seed rows in `seed_table` if it exists. Prints the before/after row count and the works it moves.
# MAGIC                 Refuses 04:00-12:00 UTC: the table is an End 2 End input.
# MAGIC - `verify`      the morning after a night: works and locations still serving a listed DOI (expect 0).
# MAGIC
# MAGIC Refresh: re-run `check` with `recheck_days=90` (re-checks rows older than that; a DOI that now resolves drops out at
# MAGIC `publish`), then `publish`. New repository records with an unknown DOI arrive daily; a quarterly refresh is enough.
# MAGIC
# MAGIC Revert: empty the table (`DELETE FROM openalex.works.unregistered_dois`), the next night serves the DOIs again. Full:
# MAGIC drop the `ud` join and the oxjob #1656 WHEN line in CreateLocationsMapped first, then the table.

# COMMAND ----------

dbutils.widgets.dropdown("mode", "candidates", ["candidates", "prefixes", "check", "publish", "verify"])
dbutils.widgets.text("threads", "16")
dbutils.widgets.text("batch", "50000")
dbutils.widgets.text("max_batches", "0")
dbutils.widgets.text("recheck_days", "0")
dbutils.widgets.text("include_other_ra", "no")
dbutils.widgets.text("seed_table", "openalex.transient.oxjob1656_seed")

MODE = dbutils.widgets.get("mode")
THREADS = int(dbutils.widgets.get("threads"))
BATCH = int(dbutils.widgets.get("batch"))
MAX_BATCHES = int(dbutils.widgets.get("max_batches"))
RECHECK_DAYS = int(dbutils.widgets.get("recheck_days"))
INCLUDE_OTHER_RA = dbutils.widgets.get("include_other_ra") == "yes"
SEED = dbutils.widgets.get("seed_table").strip()

CAND = "openalex.transient.oxjob1656_candidates"
PRA = "openalex.transient.oxjob1656_prefix_ra"
CHECKS = "openalex.transient.oxjob1656_handle_checks"
TARGET = "openalex.works.unregistered_dois"
LWT = "openalex.works.locations_w_types"
WORKS = "openalex.works.openalex_works"
UA = {"User-Agent": "OpenAlex walden BuildUnregisteredDois (oxjob 1656; mailto:support@openalex.org)"}

import datetime, json, time, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor


def show(sql, n=50):
    spark.sql(sql).show(n, truncate=False)


def table_exists(name):
    return spark.catalog.tableExists(name)


def fetch(url, timeout=30):
    """(http status, body text) with backoff on 429/5xx and network errors; (-1, error) after 5 tries."""
    last = ""
    for attempt in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
                return r.status, r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(min(60, 2 ** attempt * (5 if e.code == 429 else 1)))
                last = str(e.code)
                continue
            return e.code, e.read().decode("utf-8", "replace")[:500]
        except Exception as e:  # timeouts, resets
            last = str(e)[:200]
            time.sleep(2 ** attempt)
    return -1, last


def handle_check(doi):
    st, body = fetch("https://doi.org/api/handles/" + urllib.parse.quote(doi, safe="/"))
    rc, url_value = None, None
    try:
        j = json.loads(body)
        rc = j.get("responseCode")
        url_value = next((v["data"]["value"] for v in j.get("values", []) if v.get("type") == "URL"), None)
    except Exception:
        pass
    return (doi, st, rc, url_value, datetime.datetime.utcnow())

# COMMAND ----------

if MODE == "candidates":
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CAND}
    COMMENT 'oxjob #1656: DOIs a non-Crossref/DataCite record carries that no Crossref/DataCite record holds (BuildUnregisteredDois)'
    AS
    WITH carried AS (
      SELECT lower(l.best_doi) AS doi, l.provenance, r.work_id  -- the same key CreateLocationsMapped joins on
      FROM {LWT} l
      LEFT JOIN openalex.works.location_work_ids r
        ON l.provenance = r.provenance AND l.native_id_namespace = r.native_id_namespace AND l.native_id = r.native_id
      WHERE l.provenance NOT IN ('crossref', 'datacite') AND l.best_doi RLIKE '^10[.][0-9]+/.+'
    ),
    held AS (
      SELECT DISTINCT lower(native_id) AS doi FROM {LWT}
      WHERE provenance IN ('crossref', 'datacite') AND native_id_namespace = 'doi'
    )
    SELECT c.doi,
           substring_index(c.doi, '/', 1) AS prefix,
           count(*) AS n_records,
           count(DISTINCT c.work_id) AS n_works,
           array_sort(collect_set(c.provenance)) AS provenances
    FROM carried c LEFT ANTI JOIN held h ON c.doi = h.doi
    GROUP BY c.doi
    """)
    show(f"SELECT count(*) AS dois, sum(n_works) AS works, count(DISTINCT prefix) AS prefixes FROM {CAND}")
    show(f"SELECT prefix, count(*) AS dois, sum(n_works) AS works FROM {CAND} GROUP BY prefix ORDER BY dois DESC LIMIT 40", 40)

# COMMAND ----------

if MODE == "prefixes":
    done = set()
    if table_exists(PRA):
        done = {r.prefix for r in spark.sql(f"SELECT prefix FROM {PRA}").collect()}
    todo = [r.prefix for r in spark.sql(f"SELECT DISTINCT prefix FROM {CAND}").collect() if r.prefix not in done]
    print(f"{len(todo)} prefixes to look up ({len(done)} cached)")

    def ra(prefix):
        st, body = fetch("https://doi.org/ra/" + urllib.parse.quote(prefix, safe=""))
        try:
            return (prefix, json.loads(body)[0].get("RA") or json.loads(body)[0].get("status"), st)
        except Exception:
            return (prefix, None, st)

    with ThreadPoolExecutor(THREADS) as ex:
        rows = list(ex.map(ra, todo))
    if rows:
        df = spark.createDataFrame(rows, "prefix STRING, ra STRING, http INT")
        df.write.mode("append").saveAsTable(PRA)
    show(f"SELECT ra, count(*) AS prefixes FROM {PRA} GROUP BY ra ORDER BY prefixes DESC", 40)
    show(f"""SELECT p.ra, count(*) AS dois, sum(c.n_works) AS works FROM {CAND} c JOIN {PRA} p USING (prefix)
             GROUP BY p.ra ORDER BY dois DESC""", 40)

# COMMAND ----------

if MODE == "check":
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CHECKS}
                  (doi STRING, http INT, response_code INT, url_value STRING, checked_at TIMESTAMP)""")
    # In scope: Crossref / DataCite prefixes and prefixes doi.org does not know (no RA, 'DOI does not exist', 'Unknown',
    # an error). 10.5072 is decided by its prefix and never checked.
    ra_filter = "" if INCLUDE_OTHER_RA else """
        AND (p.ra IS NULL OR lower(p.ra) IN ('crossref', 'datacite') OR lower(p.ra) NOT IN
             ('jalc', 'medra', 'kisti', 'cnki', 'istic', 'airiti', 'op', 'eidr', 'public'))"""
    recheck = "" if RECHECK_DAYS <= 0 else f"OR k.checked_at < current_timestamp() - INTERVAL {RECHECK_DAYS} DAYS"
    todo_sql = f"""
        SELECT c.doi FROM {CAND} c
        LEFT JOIN {PRA} p USING (prefix)
        LEFT JOIN (SELECT doi, max(checked_at) AS checked_at FROM {CHECKS}
                   WHERE response_code IS NOT NULL GROUP BY doi) k USING (doi)  -- a failed call is checked again
        WHERE c.prefix <> '10.5072' {ra_filter}
          AND (k.doi IS NULL {recheck})
        ORDER BY xxhash64(c.doi)"""
    total = spark.sql(f"SELECT count(*) AS n FROM ({todo_sql})").collect()[0].n
    print(f"{total:,} DOIs to check; {THREADS} threads, {BATCH:,} per batch")
    n_batches = 0
    while True:
        dois = [r.doi for r in spark.sql(todo_sql + f" LIMIT {BATCH}").collect()]
        if not dois:
            break
        t0 = time.time()
        with ThreadPoolExecutor(THREADS) as ex:
            rows = list(ex.map(handle_check, dois))
        spark.createDataFrame(rows, "doi STRING, http INT, response_code INT, url_value STRING, checked_at TIMESTAMP") \
            .write.mode("append").saveAsTable(CHECKS)
        n_batches += 1
        rc = {}
        for r in rows:
            rc[r[2]] = rc.get(r[2], 0) + 1
        print(f"batch {n_batches}: {len(rows):,} in {time.time() - t0:,.0f} s; responseCode {rc}")
        if -1 in {r[1] for r in rows} and sum(1 for r in rows if r[1] == -1) > len(rows) // 10:
            raise Exception("over 10% of a batch failed to reach doi.org: stopping (re-run resumes)")
        if MAX_BATCHES and n_batches >= MAX_BATCHES:
            break
    show(f"""SELECT response_code, count(*) AS dois FROM
             (SELECT doi, max_by(response_code, checked_at) AS response_code FROM {CHECKS} GROUP BY doi)
             GROUP BY response_code ORDER BY dois DESC""")

# COMMAND ----------

if MODE == "publish":
    # unregistered_dois is an End 2 End input (CreateLocationsMapped): never rewrite it while a run may be reading it.
    if 4 <= datetime.datetime.utcnow().hour < 12:
        raise Exception("publish refuses 04:00-12:00 UTC (End 2 End window); run it after the night publishes")
    before = spark.sql(f"SELECT count(*) AS n FROM {TARGET}").collect()[0].n if table_exists(TARGET) else 0
    seed = f"UNION ALL SELECT lower(doi) AS doi, rule, checked_at FROM {SEED}" if SEED and table_exists(SEED) else ""
    spark.sql(f"""
    CREATE OR REPLACE TABLE {TARGET}
    COMMENT 'oxjob #1656: one row per DOI doi.org says does not exist (handle responseCode 100) or on the 10.5072 test prefix, carried by a non-Crossref/DataCite record; CreateLocationsMapped hides it (BuildUnregisteredDois)'
    AS
    WITH latest AS (
      SELECT doi, max_by(response_code, checked_at) AS response_code, max(checked_at) AS checked_at
      FROM {CHECKS} WHERE response_code IS NOT NULL GROUP BY doi  -- the latest answer doi.org gave
    ),
    rows AS (
      SELECT doi, 'handle_not_found' AS rule, checked_at FROM latest WHERE response_code = 100
      UNION ALL
      SELECT doi, 'test_prefix_10.5072' AS rule, current_timestamp() AS checked_at FROM {CAND} WHERE prefix = '10.5072'
      {seed}
    ),
    resolves AS (SELECT doi FROM latest WHERE response_code IN (1, 200))  -- a DOI that resolves now is never listed
    SELECT r.doi, min(r.rule) AS rule, max(r.checked_at) AS checked_at
    FROM rows r LEFT ANTI JOIN resolves s ON r.doi = s.doi
    GROUP BY r.doi
    """)
    show(f"SELECT count(*) AS rows, count(DISTINCT doi) AS dois, {before} AS rows_before FROM {TARGET}")
    show(f"SELECT rule, count(*) AS dois FROM {TARGET} GROUP BY rule")
    # Works the next night re-stamps (any location serving a listed DOI): size the pre-clear with this.
    show(f"""
    WITH ld AS (
      SELECT w.id, lower(regexp_replace(l.doi, '^https://doi.org/', '')) AS doi
      FROM {WORKS} w LATERAL VIEW explode(w.locations) x AS l WHERE l.doi IS NOT NULL
    )
    SELECT count(DISTINCT ld.id) AS works_to_stamp FROM ld JOIN {TARGET} u USING (doi)""")

# COMMAND ----------

if MODE == "verify":
    show(f"""
    WITH ld AS (
      SELECT w.id, w.doi AS work_doi, lower(regexp_replace(l.doi, '^https://doi.org/', '')) AS doi, l.id AS location_id
      FROM {WORKS} w LATERAL VIEW explode(w.locations) x AS l WHERE l.doi IS NOT NULL
    )
    SELECT count(*) AS locations_still_serving, count(DISTINCT ld.id) AS works_still_serving
    FROM ld JOIN {TARGET} u USING (doi)""")
    show(f"""SELECT count(*) AS works_doi_still_listed FROM {WORKS} w
             JOIN {TARGET} u ON lower(regexp_replace(w.doi, '^https://doi.org/', '')) = u.doi""")
    show(f"""SELECT id, doi, ids FROM {WORKS} WHERE id IN (3149911829, 2727676215, 2615268833)""")
