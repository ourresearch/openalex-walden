# Databricks notebook source
# MAGIC %md
# MAGIC # HAL affiliations from the HAL search API (oxjob #1402)
# MAGIC
# MAGIC HAL's OAI-PMH records carry no affiliations, so every HAL affiliation used to come from scraping hal.science.
# MAGIC Since spring 2026 HAL answers scrapers with an Anubis proof-of-work page, and new HAL records get no
# MAGIC affiliations at all. HAL's search API gives each author's primary structures; the structure reference
# MAGIC gives acronym, name, address and country, from which we rebuild the exact line HAL's pages showed:
# MAGIC `ACRONYM - Name (Address - Country)`. Those strings already have affiliation-matcher answers.
# MAGIC
# MAGIC Rows go to `openalex.landing_page.landing_page_works_backfill` (the LandingPage pipeline streams its appends),
# MAGIC keyed `https://hal.science/<halId>v<N>` with the record's PMH id, so they attach to the repository record.
# MAGIC Only HAL records whose work has no raw affiliation string on any author are touched. Each fetched PMH id is
# MAGIC logged in `openalex.landing_page.hal_api_fetch_log` and not retried for `refetch_days`.

# COMMAND ----------

# MAGIC %pip install pycountry

# COMMAND ----------

dbutils.widgets.text("max_ids", "300000")
dbutils.widgets.text("refetch_days", "30")
dbutils.widgets.text("dry_run", "false")
MAX_IDS = int(dbutils.widgets.get("max_ids"))
REFETCH_DAYS = int(dbutils.widgets.get("refetch_days"))
DRY_RUN = dbutils.widgets.get("dry_run").lower() == "true"

# COMMAND ----------

import json, time, datetime, urllib.request, urllib.parse
from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS openalex.landing_page.hal_api_fetch_log
  (pmh STRING, hal_id STRING, fetched_at TIMESTAMP, authors INT, authors_with_affiliations INT) USING DELTA""")

targets = spark.sql(f"""
WITH r AS (SELECT DISTINCT native_id AS pmh, REGEXP_EXTRACT(native_id, '^oai:HAL:(.+?)(v[0-9]+)?$', 1) AS hal_id
           FROM openalex.works.locations_parsed
           WHERE provenance IN ('repo', 'repo_backfill') AND native_id LIKE 'oai:HAL:%'),
lp AS (SELECT DISTINCT get(filter(ids, x -> x.namespace = 'pmh').id, 0) AS pmh
       FROM openalex.works.locations_parsed
       WHERE provenance = 'landing_page' AND affiliations_exist
         AND EXISTS(ids, x -> x.namespace = 'pmh' AND x.id LIKE 'oai:HAL:%')),
done AS (SELECT DISTINCT pmh FROM openalex.landing_page.hal_api_fetch_log
         WHERE fetched_at >= current_timestamp() - INTERVAL {REFETCH_DAYS} DAYS),
w AS (SELECT m.native_id AS pmh FROM openalex.works.location_work_ids m
      JOIN openalex.works.openalex_works ow ON ow.id = m.work_id
      WHERE m.provenance IN ('repo', 'repo_backfill') AND m.native_id LIKE 'oai:HAL:%'
        AND SIZE(ow.authorships) > 0 AND NOT EXISTS(ow.authorships, a -> SIZE(a.raw_affiliation_strings) > 0))
SELECT r.pmh, r.hal_id FROM r
JOIN w USING (pmh)
LEFT ANTI JOIN lp USING (pmh)
LEFT ANTI JOIN done USING (pmh)
WHERE r.hal_id <> ''
LIMIT {MAX_IDS}
""").collect()
pmh_by_hal = {}
for t in targets:
    pmh_by_hal.setdefault(t.hal_id, []).append(t.pmh)
print(f"{len(targets)} PMH ids, {len(pmh_by_hal)} HAL ids to fetch")

# COMMAND ----------

UA = {"User-Agent": "OpenAlex (mailto:team@ourresearch.org)", "Content-Type": "application/x-www-form-urlencoded"}

def post(url, params):
    body = urllib.parse.urlencode(params).encode()
    for attempt in range(8):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=body, headers=UA), timeout=300) as r:
                return json.load(r)["response"]["docs"]
        except Exception as e:  # noqa: BLE001
            err = e
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"HAL API failed: {err}")

def fetch_docs(ids):  # 200 per query: larger OR-lists hit HAL's query-size limit
    q = "halId_s:(" + " OR ".join('"%s"' % x for x in ids) + ")"
    return post("https://api.archives-ouvertes.fr/search/",
                {"q": q, "fl": "halId_s,authFullName_s,authIdHasPrimaryStructure_fs", "rows": 2000, "wt": "json"})

def fetch_structs(sids):
    q = "docid:(" + " OR ".join(sids) + ")"
    return post("https://api.archives-ouvertes.fr/ref/structure/",
                {"q": q, "fl": "docid,acronym_s,name_s,address_s,country_s", "rows": 2000, "wt": "json"})

hal_ids = sorted(pmh_by_hal)
with ThreadPoolExecutor(8) as ex:
    docs = [d for batch in ex.map(fetch_docs, [hal_ids[i:i + 200] for i in range(0, len(hal_ids), 200)]) for d in batch]
print(f"{len(docs)} HAL docs fetched")

def parse_primary(p):  # "<authid>_FacetSep_<Full Name>_JoinSep_<structid>_FacetSep_<struct name>"
    try:
        left, right = p.split("_JoinSep_", 1)
        return left.split("_FacetSep_", 1)[1], right.split("_FacetSep_", 1)[0]
    except (IndexError, ValueError):
        return None, None

sids = sorted({sid for d in docs for p in d.get("authIdHasPrimaryStructure_fs", []) for sid in [parse_primary(p)[1]] if sid and sid.isdigit()})
with ThreadPoolExecutor(8) as ex:
    S = {str(s["docid"]): s for batch in ex.map(fetch_structs, [sids[i:i + 200] for i in range(0, len(sids), 200)]) for s in batch}
print(f"{len(S)} of {len(sids)} structures fetched")

# COMMAND ----------

import pycountry

# HAL's own English country names where pycountry's differ
COUNTRY_FIX = {"kr": "South Korea", "kp": "North Korea", "ru": "Russia", "ir": "Iran", "tw": "Taiwan", "vn": "Vietnam",
               "bo": "Bolivia", "ve": "Venezuela", "tz": "Tanzania", "sy": "Syria", "la": "Laos", "md": "Moldova",
               "cz": "Czech Republic", "gb": "United Kingdom", "us": "United States", "ps": "Palestine",
               "cd": "Democratic Republic of the Congo", "cg": "Congo", "mk": "North Macedonia"}

def country(cc):
    if not cc:
        return None
    cc = cc.lower()
    if cc in COUNTRY_FIX:
        return COUNTRY_FIX[cc]
    c = pycountry.countries.get(alpha_2=cc.upper())
    return (getattr(c, "common_name", None) or c.name) if c else None

def aff_string(sid):
    s = S.get(sid)
    name = (s or {}).get("name_s", "").strip()
    if not name:
        return None
    head = f'{s["acronym_s"]} - {name}' if s.get("acronym_s") else name
    tail = " - ".join(x for x in [(s.get("address_s") or "").strip() or None, country(s.get("country_s"))] if x)
    return f"{head} ({tail})" if tail else head

now = datetime.datetime.utcnow()
rows, log = [], []
for d in docs:
    names = d.get("authFullName_s") or []
    by_name = {}
    for p in d.get("authIdHasPrimaryStructure_fs") or []:
        nm, sid = parse_primary(p)
        a = aff_string(sid) if sid else None
        if nm and a and a not in by_name.setdefault(nm, []):
            by_name[nm].append(a)
    authors = [{"name": nm, "is_corresponding": None,
                "affiliations": [{"name": a[:1000]} for a in by_name.get(nm, [])]} for nm in names]
    n_aff = sum(1 for a in authors if a["affiliations"])
    for pmh in pmh_by_hal.get(d["halId_s"], []):
        log.append((pmh, d["halId_s"], now, len(authors), n_aff))
        if not names or not n_aff:
            continue
        url = "https://hal.science/" + pmh.split("oai:HAL:", 1)[1]
        rows.append({"native_id": url, "native_id_namespace": "url", "authors": authors,
                     "ids": [{"id": url, "namespace": "url", "relationship": "self"},
                             {"id": pmh, "namespace": "pmh", "relationship": None}],
                     "version": None, "license": None, "abstract": None,
                     "urls": [{"url": url, "content_type": "html"}],
                     "is_oa": None, "updated_date": now, "created_date": now, "had_error": False})
print(f"{len(rows)} landing-page rows with affiliations; {len(log)} PMH ids logged")

# COMMAND ----------

target_schema = spark.table("openalex.landing_page.landing_page_works_backfill").schema
df = spark.createDataFrame(rows, schema=target_schema) if rows else None
log_df = spark.createDataFrame(log, "pmh STRING, hal_id STRING, fetched_at TIMESTAMP, authors INT, authors_with_affiliations INT")
if DRY_RUN:
    print("dry run: nothing written")
    if df is not None:
        display(df.limit(20))
else:
    if df is not None:
        df.write.mode("append").saveAsTable("openalex.landing_page.landing_page_works_backfill")
    log_df.write.mode("append").saveAsTable("openalex.landing_page.hal_api_fetch_log")
    print(f"appended {len(rows)} rows to landing_page_works_backfill")
