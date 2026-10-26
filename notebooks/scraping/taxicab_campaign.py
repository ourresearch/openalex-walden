# Databricks notebook source
# MAGIC %md
# MAGIC ### TaxiCab Campaign: re-scrapes with a pilot, a stop rule and a dollar cap (oxjob #1790)
# MAGIC
# MAGIC Rule (Jason, 2026-10-23): never spend a lot on a scrape and get nothing back. Every re-scrape
# MAGIC campaign (first user: the bronze re-check, #1290) runs through this guard instead of filling
# MAGIC `rescrape_queue` by hand.
# MAGIC
# MAGIC **Stateless tick**, every 20 min (pattern: Reparse Campaign Stager). State lives in tables, so a
# MAGIC tick can die anywhere and the next one carries on. For each campaign in `campaigns` with status
# MAGIC `active` (or `dry_run`), a tick does one step:
# MAGIC 1. its wave is still fetching → nothing, unless the campaign was paused or its cap lowered below the
# MAGIC    wave's estimate: then the wave is cancelled;
# MAGIC 2. its wave finished → score it (`campaign_results`), judge each host, maybe pause the campaign;
# MAGIC 3. no wave open → check the caps, plan the next wave, queue it, launch **TaxiCab Rescrape** on the
# MAGIC    campaign's own queue table with the campaign's own Zyte API key.
# MAGIC
# MAGIC **Pilot.** A host's first wave takes at most `pilot_first` (200) of its URLs; if it passes, its next
# MAGIC waves take it to `pilot_total` (1,000); only then does it get full waves (`host_wave_cap` per wave).
# MAGIC Waves always run at production concurrency (600 workers, 10 per host).
# MAGIC
# MAGIC **Stop rule** (a stopped host gets no more URLs from this campaign), judged after every wave. Words
# MAGIC follow the landing-page scoreboard (#1789): *stored* = HTTP 200, not soft-blocked; *useful* = a stored
# MAGIC PDF, or stored HTML where Parseland found authors or an abstract.
# MAGIC - *walled* (#1105's rule): blocked fetches ≥ 20 and above max(2%, the host's 7-day blocked share in the
# MAGIC   scoreboard funnel + 2 points). Blocked = the funnel's (soft block, 401, 403, 429) plus 202 and fbi.gov
# MAGIC   redirects, which are never pages; the extra two only make the rule stricter;
# MAGIC - *nothing back*: under `min_stored_share` (50%) of 100+ fetches stored or a clear answer (404/410);
# MAGIC - *nothing useful*: stored PDFs plus stored HTML × the host's 30-day useful-per-stored-HTML share in
# MAGIC   the funnel, plus answers, under `min_expected_useful_share` (20%) of 100+ fetches (shells and login
# MAGIC   pages served as 200: Parseland only runs later, so its history stands in);
# MAGIC - *too expensive*: estimated Zyte $ per 1,000 stored-or-answer fetches over `max_usd_per_1k_stored`
# MAGIC   ($15), unless the host is in `allow_hosts`.
# MAGIC A wave where 90%+ of 200+ fetches failed across 3+ hosts pauses the whole campaign instead (the
# MAGIC campaign key hit its Zyte limit, or the harvester is down); its rows go back to the pool.
# MAGIC
# MAGIC **Dollar caps, two layers.** (a) Zyte enforces each campaign key's own spending limit for the
# MAGIC billing cycle (Zyte dashboard → Spending Alerts → API key, "Pause processing requests"), so a campaign
# MAGIC can never spend the main key's money or stop new-content scraping. (b) This guard counts spend in
# MAGIC Zyte's dollars and pauses before Zyte's limit: it pauses the campaign at `cap_usd` for the billing
# MAGIC cycle (from the 2nd of the month), trims a wave to fit what is left, and pauses if steady use plus every
# MAGIC campaign (open waves included) would pass `zyte_total_cap_usd` ($9K; Zyte pauses ALL usage at $12K).
# MAGIC Spend per campaign per cycle is written to `campaign_spend` for the charter's daily check.
# MAGIC
# MAGIC **How spend is counted.** Each fetch gets a raw estimate: the scoreboard's price for the host it
# MAGIC reached (`openalex.monitoring.landing_page_zyte_prices`). That runs ~2x low for doi.org links (26 Oct
# MAGIC 2026, #1790 EXPLORE § 8): when Taxicab's free HEAD lookup of the DOI fails (Cloudflare publishers),
# MAGIC Zyte is asked for doi.org itself and bills doi.org's rate, often with browser rendering, though
# MAGIC `resolved_url` shows the publisher. So the guard scales the raw estimate by each Zyte key's measured
# MAGIC ratio, Zyte's billed $ ÷ raw estimate over its recent complete UTC days, from
# MAGIC `campaign_key_costs` (one row per key per day: written each tick from Zyte's Stats API when secret
# MAGIC `webscraper/zyte_stats_api_key` exists, else by hand from the dashboard: Stats → Usage, API Keys = the
# MAGIC key, Price). Spend = raw × ratio × `calibrated_margin` (1.1, for mix drift between readings); a key
# MAGIC with no reading yet falls back to raw × `cost_margin` (1.25), as before. The ratio never goes below 1.
# MAGIC Per-host "too expensive" stops still use the raw estimate × `cost_margin` (a key-wide ratio says
# MAGIC nothing about which host is dear).
# MAGIC
# MAGIC **Run a campaign:** put its URLs in a table with `url, native_id, native_id_namespace` (and an
# MAGIC optional int `priority`, higher first; `native_id` is a bare DOI for namespace `doi`), then
# MAGIC ```sql
# MAGIC INSERT INTO openalex.taxicab.campaigns (campaign, source_table, zyte_key, cap_usd, status, settings, created_at, updated_at)
# MAGIC VALUES ('bronze-2026-10', 'openalex.taxicab.bronze_recheck_urls', 'campaign-bronze', 2500, 'active', '{}', current_timestamp(), current_timestamp());
# MAGIC ```
# MAGIC and make sure the TaxiCab Campaign schedule is unpaused (`jobs/taxicab_campaign.yaml`). `settings` is
# MAGIC JSON overriding `DEFAULTS` below. Pause: `UPDATE ... SET status = 'paused'` (an open wave is cancelled
# MAGIC at the next tick). Resume after a cap pause: raise `cap_usd` (and the key's Zyte limit), set
# MAGIC `status = 'active'`.

# COMMAND ----------

import base64
import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

from databricks.sdk import WorkspaceClient
from pyspark.sql import Window
from pyspark.sql import functions as F

RESCRAPE_JOB_ID = 866093356324381   # TaxiCab Rescrape
DB = "openalex.taxicab"
# Shared with the landing-page scoreboard (#1789) so campaign numbers join its funnel:
MON = "openalex.monitoring"
HOST_FN = f"{MON}.landing_page_host"    # (url, resolved_url, native_id_namespace, native_id)
PRICES = f"{MON}.landing_page_zyte_prices"
FUNNEL = f"{MON}.landing_page_funnel"
BREAKER = f"{DB}.host_breaker"          # #1712's shared per-host breaker; skipped if absent
KEY_COSTS = f"{DB}.campaign_key_costs"  # Zyte's billed $ per campaign key per UTC day
ZYTE_ORG = 122441
ZYTE_STATS_URL = "https://zyte-api-stats.zyte.com/api/stats"   # docs.zyte.com/zyte-api/usage/stats
CALIBRATION_DAYS = 7                    # complete UTC days of Zyte readings a key's ratio is measured over

DEFAULTS = {
    "wave_size": 20000,
    "pilot_first": 200,
    "pilot_total": 1000,
    "host_wave_cap": 3000,           # a wave lasts about host_wave_cap x latency / max_per_host
    "max_per_host": 10,
    "harvest_workers": 600,
    "stop_min_blocked": 20,
    "stop_blocked_share": 0.02,
    "stop_over_baseline": 0.02,
    "min_fetches_judged": 100,
    "min_stored_share": 0.5,
    "min_expected_useful_share": 0.2,
    "max_usd_per_1k_stored": 15.0,
    "allow_hosts": [],
    "campaign_fail_share": 0.9,
    # $ per 1,000 fetches for a host missing from the price table; None = the table's '*' row
    # ($40 per 1M requests, repositories and small journals, #1290). Hosts priced above that are listed.
    "default_usd_per_1k": None,
    "cost_margin": 1.25,             # re-asks, recipes with extra calls, price drift (keys with no Zyte reading)
    "calibrated_margin": 1.1,        # on top of a key's measured Zyte ratio: mix drift between readings
    "zyte_total_cap_usd": 9000,
    "steady_usd_per_day": 100,       # non-campaign Zyte use; observed $51-80/day in Oct 2026
    "quiet_hours_utc": [3, 4, 5],    # no new waves while new-content runs peak (04:00 UTC) and End 2 End starts (05:00)
}

w = WorkspaceClient()


def q(sql):
    return spark.sql(sql)


def one(sql):
    return spark.sql(sql).collect()[0][0]


def sq(s):
    return "'" + str(s).replace("\\", "\\\\").replace("'", "\\'") + "'"


def now():
    return datetime.now(timezone.utc)


def cycle_start():
    """Start of the Zyte billing cycle (the 2nd of the month). Midnight UTC counts a little more spend
    than Zyte does, which errs on the safe side."""
    t = now()
    start = t.replace(day=2, hour=0, minute=0, second=0, microsecond=0)
    if t < start:
        start = (start - timedelta(days=3)).replace(day=2)
    return start


def ts(t):
    return t.strftime("%Y-%m-%d %H:%M:%S")


def price_key(url, resolved_url="NULL"):
    """The host Zyte bills, exactly as the scoreboard's funnel prices it (notebooks/monitoring/
    landing_pages/funnel.sql): the requested URL's host, or for a doi.org URL the resolved one, or doi.org
    when the free DOI lookup failed and Zyte followed it. (The argument order mirrors funnel.sql.)"""
    return (f"coalesce({MON}.landing_page_netloc_host({MON}.landing_page_fetched_netloc({resolved_url}, {url})), "
            f"'doi.org')")

# COMMAND ----------

q(f"""CREATE TABLE IF NOT EXISTS {DB}.campaigns (
  campaign STRING, source_table STRING, zyte_key STRING, cap_usd DOUBLE, status STRING,
  status_reason STRING, settings STRING, created_at TIMESTAMP, updated_at TIMESTAMP) USING DELTA""")
q(f"""CREATE TABLE IF NOT EXISTS {DB}.campaign_urls (
  campaign STRING, url STRING, native_id STRING, native_id_namespace STRING, host STRING,
  priority INT, shuffle DOUBLE, wave INT, attempts INT) USING DELTA""")
q(f"""CREATE TABLE IF NOT EXISTS {DB}.campaign_hosts (
  campaign STRING, host STRING, state STRING, reason STRING, updated_at TIMESTAMP) USING DELTA""")
q(f"""CREATE TABLE IF NOT EXISTS {DB}.campaign_waves (
  campaign STRING, wave INT, run_id BIGINT, status STRING, queued INT, est_usd DOUBLE,
  started_at TIMESTAMP, ended_at TIMESTAMP, landed INT, note STRING) USING DELTA""")
q(f"""CREATE TABLE IF NOT EXISTS {DB}.campaign_results (
  campaign STRING, wave INT, url STRING, host STRING, taxicab_id STRING, status_code INT,
  resolved_url STRING, content_type STRING, is_soft_block BOOLEAN, error STRING, processed_date TIMESTAMP,
  outcome STRING, est_usd DOUBLE) USING DELTA""")
q(f"""CREATE TABLE IF NOT EXISTS {DB}.campaign_spend (
  campaign STRING, zyte_key STRING, cycle_start TIMESTAMP, est_usd DOUBLE, cap_usd DOUBLE, fetches BIGINT,
  stored BIGINT, status STRING, updated_at TIMESTAMP) USING DELTA""")
# A reading counts for its day once read_at is after the day's end (a partial day is replaced later)
q(f"""CREATE TABLE IF NOT EXISTS {KEY_COSTS} (
  zyte_key STRING, day DATE, zyte_usd DOUBLE, billed_requests BIGINT, source STRING, read_at TIMESTAMP) USING DELTA""")

# COMMAND ----------

# Outcome of one taxicab_results row
BLOCKED = ("(coalesce(is_soft_block, false) OR status_code IN (202, 401, 403, 429) "
           "OR coalesce(resolved_url, '') LIKE '%fbi.gov%')")
OUTCOME = f"""CASE
  WHEN status_code = 400 AND coalesce(error, '') RLIKE 'got None' THEN 'ban'
  WHEN {BLOCKED} THEN 'blocked'
  WHEN status_code = 200 THEN 'stored'
  WHEN status_code IN (404, 410) THEN 'answer'
  ELSE 'failed' END"""
# Zyte doesn't bill bans; an unknown campaign key is refused before any Zyte call
FREE = "(outcome = 'ban' OR coalesce(error, '') LIKE '%unknown zyte_key%')"
RESULT_COLS = ["campaign", "wave", "url", "host", "taxicab_id", "status_code", "resolved_url",
               "content_type", "is_soft_block", "error", "processed_date", "outcome", "est_usd"]


def settings(c):
    s = dict(DEFAULTS)
    s.update(json.loads(c["settings"] or "{}"))
    return s


def queue_table(c):
    return f"{DB}.campaign_queue_" + re.sub(r"[^a-z0-9]+", "_", c["campaign"].lower()).strip("_")


def set_status(c, status, reason):
    q(f"""UPDATE {DB}.campaigns SET status = {sq(status)}, status_reason = {sq(reason)}, updated_at = current_timestamp()
          WHERE campaign = {sq(c['campaign'])}""")
    c["status"], c["status_reason"] = status, reason
    print(f"[{c['campaign']}] status -> {status}: {reason}")


# Newest price per billed host from the scoreboard's table, read once per run ('*' = unlisted hosts)
q(f"""CREATE OR REPLACE TEMP VIEW host_prices AS
      SELECT price_key, usd_per_million_requests * requests_per_fetch / 1000 AS usd_per_1k
      FROM (SELECT *, row_number() OVER (PARTITION BY price_key ORDER BY valid_from DESC) AS rn
            FROM {PRICES} WHERE valid_from <= current_date()) WHERE rn = 1""")
STAR_USD_PER_1K = float(one("SELECT usd_per_1k FROM host_prices WHERE price_key = '*'"))


def default_price(s):
    return STAR_USD_PER_1K if s["default_usd_per_1k"] is None else float(s["default_usd_per_1k"])

# COMMAND ----------

def load_urls(c):
    """First tick of a campaign: copy its source table into campaign_urls."""
    if q(f"SELECT 1 FROM {DB}.campaign_urls WHERE campaign = {sq(c['campaign'])} LIMIT 1").count():
        return
    src = spark.table(c["source_table"])
    priority = F.col("priority").cast("int") if "priority" in src.columns else F.lit(0)
    urls = (
        src.select("url", "native_id", "native_id_namespace", priority.alias("priority"))
        .filter(F.col("url").isNotNull())
        .dropDuplicates(["url"])
        # the scoreboard's host key: registered domain; a doi.org link gets its DOI prefix's publisher
        .withColumn("host", F.expr(f"{HOST_FN}(url, NULL, native_id_namespace, native_id)"))
        .filter(F.col("host").isNotNull() & (F.col("host") != "localhost") & ~F.col("host").rlike(r"^[0-9.\[\]:]+$"))
        .select(F.lit(c["campaign"]).alias("campaign"), "url", "native_id", "native_id_namespace", "host",
                F.coalesce("priority", F.lit(0)).alias("priority"), F.rand().alias("shuffle"),
                F.lit(None).cast("int").alias("wave"), F.lit(0).alias("attempts"))
    )
    urls.write.mode("append").saveAsTable(f"{DB}.campaign_urls")
    n = one(f"SELECT count(*) FROM {DB}.campaign_urls WHERE campaign = {sq(c['campaign'])}")
    print(f"[{c['campaign']}] loaded {n:,} URLs from {c['source_table']}")


def cycle_totals():
    """Per campaign this billing cycle: estimated $ before margin, fetches, stored. One query."""
    rows = q(f"""SELECT campaign, coalesce(sum(est_usd), 0) AS usd, count(*) AS fetches,
                        coalesce(sum(CASE WHEN outcome = 'stored' THEN 1 ELSE 0 END), 0) AS stored
                 FROM {DB}.campaign_results WHERE processed_date >= '{ts(cycle_start())}' GROUP BY campaign""").collect()
    return {r.campaign: r for r in rows}


def read_zyte_stats():
    """Zyte's billed $ per campaign key per UTC day from its Stats API into KEY_COSTS. Needs the Zyte
    *dashboard* API key (not a Zyte API key) in secret webscraper/zyte_stats_api_key; without it the rows
    are written by hand from the dashboard and this does nothing."""
    try:
        api_key = dbutils.secrets.get("webscraper", "zyte_stats_api_key")
    except Exception:
        return
    auth = "Basic " + base64.b64encode(f"{api_key}:".encode()).decode()
    keys = [r.zyte_key for r in q(f"SELECT DISTINCT zyte_key FROM {DB}.campaigns WHERE coalesce(zyte_key, '') <> ''").collect()]
    start = (now() - timedelta(days=CALIBRATION_DAYS + 2)).strftime("%Y-%m-%dT00:00:00Z")
    rows = []
    for key in keys:
        query = urllib.parse.urlencode({"organization_id": ZYTE_ORG, "start_time": start, "groupby_time": "day",
                                        "end_time": now().strftime("%Y-%m-%dT%H:%M:%SZ"), "apikey_labels": key})
        req = urllib.request.Request(f"{ZYTE_STATS_URL}?{query}", headers={"Authorization": auth})
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                results = json.load(resp).get("results", [])
        except Exception as e:  # never print the request: it carries the key
            print(f"WARNING: Zyte Stats API read for {key} failed ({type(e).__name__}); using the readings on file")
            continue
        for r in results:
            billed = sum(sc.get("count") or 0 for sc in r.get("status_codes") or [] if sc.get("code") == 200)
            rows.append((key, r["day"][:10], float(r["cost_microusd_total"]) / 1e6, billed))
    if rows:
        spark.createDataFrame(rows, "zyte_key STRING, day STRING, zyte_usd DOUBLE, billed_requests BIGINT") \
            .createOrReplaceTempView("zyte_readings")
        q(f"""MERGE INTO {KEY_COSTS} t
              USING (SELECT zyte_key, CAST(day AS DATE) AS day, zyte_usd, billed_requests FROM zyte_readings) s
              ON t.zyte_key = s.zyte_key AND t.day = s.day
              WHEN MATCHED THEN UPDATE SET zyte_usd = s.zyte_usd, billed_requests = s.billed_requests,
                source = 'zyte stats api', read_at = current_timestamp()
              WHEN NOT MATCHED THEN INSERT (zyte_key, day, zyte_usd, billed_requests, source, read_at)
                VALUES (s.zyte_key, s.day, s.zyte_usd, s.billed_requests, 'zyte stats api', current_timestamp())""")


def key_ratios():
    """Per Zyte key: (Zyte's billed $ ÷ the raw estimate, days) over its last CALIBRATION_DAYS complete UTC
    days that have a full-day reading and no wave still unscored; keys with under $1 of raw estimate on
    such days are left out (no ratio). Never below 1: the guard doesn't count less than its price table."""
    rows = q(f"""
      WITH k AS (
        SELECT zyte_key, day, zyte_usd FROM {KEY_COSTS} WHERE read_at >= date_add(day, 1) AND day < current_date()),
      open_ AS (   -- a wave's rows reach campaign_results only when it is scored
        SELECT c.zyte_key, min(CAST(w.started_at AS DATE)) AS first_open
        FROM {DB}.campaign_waves w JOIN {DB}.campaigns c USING (campaign)
        WHERE w.status IN ('launching', 'fetching') GROUP BY 1),
      raw AS (
        SELECT c.zyte_key, CAST(r.processed_date AS DATE) AS day, sum(r.est_usd) AS raw_usd
        FROM {DB}.campaign_results r JOIN {DB}.campaigns c USING (campaign)
        WHERE r.processed_date >= date_sub(current_date(), {CALIBRATION_DAYS + 40}) GROUP BY 1, 2),
      j AS (
        SELECT k.zyte_key, k.zyte_usd, raw.raw_usd, row_number() OVER (PARTITION BY k.zyte_key ORDER BY k.day DESC) AS rn
        FROM k JOIN raw USING (zyte_key, day) LEFT JOIN open_ o USING (zyte_key)
        WHERE (o.first_open IS NULL OR k.day < o.first_open) AND raw.raw_usd > 0)
      SELECT zyte_key, sum(zyte_usd) AS usd, sum(raw_usd) AS raw, count(*) AS n_days
      FROM j WHERE rn <= {CALIBRATION_DAYS} GROUP BY zyte_key HAVING sum(raw_usd) >= 1""").collect()
    return {r.zyte_key: (max(1.0, r.usd / r.raw), int(r.n_days)) for r in rows}


RATIOS = {}         # zyte_key -> (ratio, days); set once per run in the last cell
CAMPAIGN_KEYS = {}  # campaign -> zyte_key


def cost_factor(zyte_key, s):
    """Raw estimate -> Zyte dollars for spend on this key."""
    ratio = RATIOS.get(zyte_key)
    return ratio[0] * s["calibrated_margin"] if ratio else s["cost_margin"]


def spent(c, s, totals):
    r = totals.get(c["campaign"])
    return (float(r.usd) if r else 0.0) * cost_factor(c["zyte_key"], s)


def write_spend(c, s, totals):
    r = totals.get(c["campaign"])
    q(f"""MERGE INTO {DB}.campaign_spend t
          USING (SELECT {sq(c['campaign'])} AS campaign, {sq(c['zyte_key'] or '')} AS zyte_key,
                        TIMESTAMP '{ts(cycle_start())}' AS cycle_start, {spent(c, s, totals)} AS est_usd,
                        {float(c['cap_usd'])} AS cap_usd, {int(r.fetches) if r else 0} AS fetches,
                        {int(r.stored) if r else 0} AS stored, {sq(c['status'])} AS status,
                        current_timestamp() AS updated_at) s
          ON t.campaign = s.campaign AND t.cycle_start = s.cycle_start
          WHEN MATCHED THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *""")


def total_zyte_estimate(s, totals):
    """Steady (non-campaign) use so far this cycle, plus every campaign's scored spend and open waves."""
    days = (now() - cycle_start()).total_seconds() / 86400
    open_waves = one(f"SELECT coalesce(sum(est_usd), 0) FROM {DB}.campaign_waves WHERE status IN ('launching', 'fetching')")
    scored = sum(float(r.usd) * cost_factor(CAMPAIGN_KEYS.get(name), s) for name, r in totals.items())
    return s["steady_usd_per_day"] * days + scored + float(open_waves)

# COMMAND ----------

def unlaunch(c, wv):
    """Undo a wave that never reached TaxiCab Rescrape: URLs back in the pool, queue and wave row gone."""
    camp = sq(c["campaign"])
    q(f"UPDATE {DB}.campaign_urls SET wave = NULL WHERE campaign = {camp} AND wave = {wv}")
    q(f"TRUNCATE TABLE {queue_table(c)}")
    q(f"DELETE FROM {DB}.campaign_waves WHERE campaign = {camp} AND wave = {wv}")


def score_wave(c, s, wv, started_at):
    """Write the wave's results (each URL's newest taxicab_results row since the wave started); send
    unfetched URLs back to the pool (unfetched twice: skipped)."""
    camp = sq(c["campaign"])
    latest = (
        spark.table(f"{DB}.taxicab_results")
        .filter(F.col("processed_date") >= F.lit(started_at - timedelta(minutes=5)))
        .withColumn("rn", F.row_number().over(Window.partitionBy("url").orderBy(F.desc("processed_date"))))
        .filter("rn = 1")
        .select("url", "taxicab_id", "status_code", "resolved_url", "content_type", "is_soft_block", "error",
                "processed_date")
    )
    (spark.table(f"{DB}.campaign_urls").filter((F.col("campaign") == c["campaign"]) & (F.col("wave") == wv))
     .select("campaign", "wave", "url", "host")
     .join(latest, "url")
     .withColumn("outcome", F.expr(OUTCOME))
     .withColumn("price_key", F.expr(price_key("url", "resolved_url")))
     .join(spark.table("host_prices"), "price_key", "left")
     .withColumn("est_usd", F.when(F.expr(FREE), F.lit(0.0))
                 .otherwise(F.coalesce("usd_per_1k", F.lit(default_price(s))) / 1000))
     .select(*RESULT_COLS)
     .write.mode("append").saveAsTable(f"{DB}.campaign_results"))
    q(f"""MERGE INTO {DB}.campaign_urls u
          USING (SELECT url FROM {DB}.campaign_urls WHERE campaign = {camp} AND wave = {wv}
                 EXCEPT SELECT url FROM {DB}.campaign_results WHERE campaign = {camp} AND wave = {wv}) m
          ON u.campaign = {camp} AND u.url = m.url
          WHEN MATCHED THEN UPDATE SET wave = CASE WHEN u.attempts >= 1 THEN -1 ELSE NULL END, attempts = u.attempts + 1""")
    q(f"TRUNCATE TABLE {queue_table(c)}")
    if wv % 25 == 0:   # every wave's MERGEs leave deletion vectors across campaign_urls
        q(f"OPTIMIZE {DB}.campaign_urls")
    return one(f"SELECT count(*) FROM {DB}.campaign_results WHERE campaign = {camp} AND wave = {wv}")


def broad_failure(c, s, wv):
    r = q(f"""SELECT count(*) AS n, count(DISTINCT host) AS hosts,
                     coalesce(sum(CASE WHEN outcome IN ('failed', 'ban') THEN 1 ELSE 0 END), 0) AS bad
              FROM {DB}.campaign_results WHERE campaign = {sq(c['campaign'])} AND wave = {wv}""").collect()[0]
    if r.n >= 200 and r.hosts >= 3 and r.bad >= s["campaign_fail_share"] * r.n:
        return f"wave {wv}: {r.bad:,} of {r.n:,} fetches failed across {r.hosts} hosts (campaign key at its Zyte limit, or harvester down)"
    return None


def requeue_failed_wave(c, wv):
    camp = sq(c["campaign"])
    q(f"""MERGE INTO {DB}.campaign_urls u
          USING (SELECT url FROM {DB}.campaign_results WHERE campaign = {camp} AND wave = {wv}
                 AND outcome IN ('failed', 'ban')) m
          ON u.campaign = {camp} AND u.url = m.url
          WHEN MATCHED THEN UPDATE SET wave = NULL""")
    q(f"DELETE FROM {DB}.campaign_results WHERE campaign = {camp} AND wave = {wv} AND outcome IN ('failed', 'ban')")


def judge_hosts(c, s, wv):
    """Stop rule and pilot progress for every host in the wave."""
    camp = sq(c["campaign"])
    good = "outcome IN ('stored', 'answer')"
    # expected useful: stored PDFs count in full, stored HTML at the host's useful-per-stored-HTML share
    expected = ("CASE WHEN outcome = 'answer' OR (outcome = 'stored' AND content_type = 'pdf') THEN 1.0 "
                "WHEN outcome = 'stored' THEN coalesce(r_html, 1.0) ELSE 0.0 END")
    stats = q(f"""
      WITH f AS (   -- the scoreboard funnel (#1789), first attempts
        SELECT host,
          sum(CASE WHEN fetch_date >= date_sub(current_date(), 7) THEN fail_soft_block + fail_403 + fail_429 END)
            / sum(CASE WHEN fetch_date >= date_sub(current_date(), 7) THEN fetches END) AS b7,
          CASE WHEN sum(stored_html) >= 100 THEN (sum(useful) - sum(stored_pdf)) / sum(stored_html) END AS r_html
        FROM {FUNNEL} WHERE attempt = 'first' AND fetch_date >= date_sub(current_date(), 30)
          AND host IN (SELECT DISTINCT host FROM {DB}.campaign_results WHERE campaign = {camp} AND wave = {wv})
        GROUP BY host),
      res AS (SELECT r.*, r.wave = {wv} AS this_wave, f.b7, f.r_html
              FROM {DB}.campaign_results r LEFT JOIN f ON f.host = r.host WHERE r.campaign = {camp}),
      by_host AS (
        SELECT host, count_if(this_wave) AS wn, count_if(this_wave AND outcome = 'blocked') AS wb,
               count_if(this_wave AND {good}) AS wg, sum(CASE WHEN this_wave THEN {expected} ELSE 0 END) AS we,
               count(*) AS cn, count_if({good}) AS cg, sum({expected}) AS ce, sum(est_usd) AS usd,
               coalesce(max(b7), 0) AS b7, max(r_html) AS r_html
        FROM res GROUP BY host HAVING count_if(this_wave) > 0),
      left_ AS (
        SELECT host, count(*) AS left_n FROM {DB}.campaign_urls WHERE campaign = {camp} AND wave IS NULL GROUP BY host)
      SELECT b.*, coalesce(l.left_n, 0) AS left_n, coalesce(h.state, 'pilot1') AS state
      FROM by_host b
      LEFT JOIN left_ l ON l.host = b.host
      LEFT JOIN {DB}.campaign_hosts h ON h.campaign = {camp} AND h.host = b.host""").collect()

    updates = []
    for r in stats:
        reason = None
        limit = max(s["stop_blocked_share"], r.b7 + s["stop_over_baseline"])
        if r.wb >= s["stop_min_blocked"] and r.wb > limit * r.wn:
            reason = f"walled: {r.wb}/{r.wn} blocked in wave {wv} (7-day baseline {r.b7:.0%})"
        # judge this wave alone when it is big enough, else everything so far
        n, g, e = (r.wn, r.wg, r.we) if r.wn >= s["min_fetches_judged"] else (r.cn, r.cg, r.ce)
        if not reason and n >= s["min_fetches_judged"] and g < s["min_stored_share"] * n:
            reason = f"nothing back: {g}/{n} fetches stored or a clear 404"
        # without funnel history r_html counts as 1, which "nothing back" already covers
        if not reason and r.r_html is not None and n >= s["min_fetches_judged"] and e < s["min_expected_useful_share"] * n:
            reason = (f"nothing useful: ~{e:.0f}/{n} fetches expected useful (host's 30-day useful share of "
                      f"stored HTML {r.r_html:.0%})")
        usd_per_1k = r.usd * s["cost_margin"] / r.cg * 1000 if r.cg else 0
        if (not reason and r.host not in s["allow_hosts"] and r.cn >= s["min_fetches_judged"]
                and usd_per_1k > s["max_usd_per_1k_stored"]):
            reason = f"too expensive: ${usd_per_1k:.2f} per 1,000 stored or 404"
        if reason:
            state = "stopped"
            print(f"[{c['campaign']}] STOP {r.host}: {reason}")
        elif r.state == "pilot1" and (r.cn >= s["pilot_first"] or r.left_n == 0):
            state, reason = "pilot2", f"pilot 1 passed: {r.cg}/{r.cn} stored or 404"
        elif r.state == "pilot2" and (r.cn >= s["pilot_total"] or r.left_n == 0):
            state, reason = "admitted", f"pilot passed: {r.cg}/{r.cn} stored or 404"
        else:
            continue
        updates.append((r.host, state, reason))

    if updates:
        spark.createDataFrame(updates, "host STRING, state STRING, reason STRING").createOrReplaceTempView("host_updates")
        q(f"""MERGE INTO {DB}.campaign_hosts h
              USING host_updates u ON h.campaign = {camp} AND h.host = u.host
              WHEN MATCHED THEN UPDATE SET state = u.state, reason = u.reason, updated_at = current_timestamp()
              WHEN NOT MATCHED THEN INSERT (campaign, host, state, reason, updated_at)
                VALUES ({camp}, u.host, u.state, u.reason, current_timestamp())""")

# COMMAND ----------

def breaker_hosts():
    """Hosts #1712's shared breaker has open (walled). Contract to confirm with #1712: host, state."""
    if not spark.catalog.tableExists(BREAKER):
        return []
    try:
        return [r.host for r in q(f"SELECT DISTINCT host FROM {BREAKER} WHERE state = 'open'").collect()]
    except Exception as e:  # a schema we don't know yet: the campaign's own stop rule still protects
        print(f"WARNING: could not read {BREAKER} ({e}); relying on the campaign stop rule only")
        return []


def plan_wave(c, s, budget_usd):
    """Next wave: per-host quotas by pilot stage, round-robin by host, trimmed to the budget."""
    camp = sq(c["campaign"])
    skip = breaker_hosts()
    skip_sql = f"AND u.host NOT IN ({', '.join(sq(h) for h in skip)})" if skip else ""
    planned = q(f"""
      WITH done AS (SELECT host, count(*) AS n FROM {DB}.campaign_results WHERE campaign = {camp} GROUP BY host),
      pool AS (
        SELECT u.url, u.native_id, u.native_id_namespace, u.host, coalesce(h.state, 'pilot1') AS state,
               coalesce(d.n, 0) AS done_n,
               row_number() OVER (PARTITION BY u.host ORDER BY u.priority DESC, u.shuffle) AS rn
        FROM {DB}.campaign_urls u
        LEFT JOIN {DB}.campaign_hosts h ON h.campaign = u.campaign AND h.host = u.host
        LEFT JOIN done d ON d.host = u.host
        WHERE u.campaign = {camp} AND u.wave IS NULL AND coalesce(h.state, 'pilot1') <> 'stopped' {skip_sql}),
      quota AS (
        SELECT *, CASE state WHEN 'pilot1' THEN greatest({s['pilot_first']} - done_n, 0)
                             WHEN 'pilot2' THEN greatest({s['pilot_total']} - done_n, 0)
                             ELSE {s['host_wave_cap']} END AS q
        FROM pool
        -- a constant bound lets Spark keep only each host's top rows before the shuffle
        WHERE rn <= {max(s['pilot_first'], s['pilot_total'], s['host_wave_cap'])}),
      picked AS (
        SELECT quota.*, coalesce(p.usd_per_1k, {default_price(s)}) / 1000 * {s['cost_margin']} AS est_usd
        FROM quota LEFT JOIN host_prices p ON p.price_key = {price_key('quota.url')}
        WHERE rn <= q ORDER BY rn, host LIMIT {int(s['wave_size'])}),
      costed AS (
        SELECT *, sum(est_usd) OVER (ORDER BY rn, host ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_usd
        FROM picked)
      SELECT url, native_id, native_id_namespace, host, state, rn, est_usd FROM costed WHERE cum_usd <= {budget_usd}""")
    # Materialized on the driver (at most wave_size rows): a view would re-read campaign_urls after
    # launch_wave marks the wave and come back empty.
    spark.createDataFrame(planned.collect(), planned.schema).createOrReplaceTempView("plan")
    return q("SELECT count(*) AS n, coalesce(sum(est_usd), 0) AS usd, count(DISTINCT host) AS hosts FROM plan").collect()[0]


def launch_wave(c, s, plan):
    camp = sq(c["campaign"])
    qt = queue_table(c)
    wv = int(one(f"SELECT coalesce(max(wave), 0) + 1 FROM {DB}.campaign_waves WHERE campaign = {camp}"))
    q(f"CREATE TABLE IF NOT EXISTS {qt} (native_id STRING, native_id_namespace STRING, created_date TIMESTAMP, pdf_url STRING) USING DELTA")
    if one(f"SELECT count(*) FROM {qt}"):
        raise RuntimeError(f"{qt} still holds rows with no open wave; inspect before launching")
    # shortcut: a MERGE of ~20K random urls rewrites much of campaign_urls (deletion vectors keep it
    # cheap, OPTIMIZE every 25 waves); move wave membership to an append-only table if it isn't
    q(f"""MERGE INTO {DB}.campaign_urls u USING plan p ON u.campaign = {camp} AND u.url = p.url
          WHEN MATCHED THEN UPDATE SET wave = {wv}""")
    # the notebook shuffles and interleaves by host, so insert order doesn't matter
    q(f"INSERT INTO {qt} SELECT native_id, native_id_namespace, current_timestamp(), url FROM plan")
    q(f"""INSERT INTO {DB}.campaign_waves VALUES ({camp}, {wv}, NULL, 'launching', {plan.n}, {plan.usd},
          current_timestamp(), NULL, NULL, {sq(f'{plan.hosts} hosts')})""")
    params = {"queue_table": qt, "rescrape_queue_only": "true", "url_limit": str(plan.n + 100),
              "harvest_workers": str(s["harvest_workers"]), "max_per_host": str(s["max_per_host"]),
              # prefix: doi.org links are capped per DOI prefix, not unlimited
              "lookback_days": "3", "doi_host_key": "prefix"}
    if c["zyte_key"]:
        params["zyte_key"] = c["zyte_key"]
    try:
        run = w.jobs.run_now(job_id=RESCRAPE_JOB_ID, job_parameters=params)
    except Exception as e:   # e.g. TaxiCab Rescrape already at max_concurrent_runs: undo, try next tick
        unlaunch(c, wv)
        print(f"[{c['campaign']}] could not launch wave {wv}: {e}")
        return
    q(f"""UPDATE {DB}.campaign_waves SET run_id = {run.run_id}, status = 'fetching', started_at = current_timestamp()
          WHERE campaign = {camp} AND wave = {wv}""")
    print(f"[{c['campaign']}] wave {wv}: {plan.n:,} URLs on {plan.hosts} hosts, est ${plan.usd:,.2f}, TaxiCab Rescrape run {run.run_id}")

# COMMAND ----------

RUNNING = {"PENDING", "QUEUED", "RUNNING", "TERMINATING", "BLOCKED", "WAITING_FOR_RETRY"}


def find_run(qt, since):
    """A TaxiCab Rescrape run started since `since` on queue table `qt` (a tick died after launching)."""
    for r in w.jobs.list_runs(job_id=RESCRAPE_JOB_ID, start_time_from=int(since.timestamp() * 1000) - 60000):
        full = w.jobs.get_run(r.run_id)
        if any(p.name == "queue_table" and p.value == qt for p in (full.job_parameters or [])):
            return full.run_id
    return None


def recover_launching(c):
    camp = sq(c["campaign"])
    for wv in q(f"SELECT * FROM {DB}.campaign_waves WHERE campaign = {camp} AND status = 'launching'").collect():
        run_id = find_run(queue_table(c), wv.started_at.replace(tzinfo=timezone.utc))
        if run_id:
            q(f"UPDATE {DB}.campaign_waves SET run_id = {run_id}, status = 'fetching' WHERE campaign = {camp} AND wave = {wv.wave}")
            print(f"[{c['campaign']}] wave {wv.wave}: adopted run {run_id} left by a tick that died")
        else:
            unlaunch(c, wv.wave)
            print(f"[{c['campaign']}] wave {wv.wave}: never launched; URLs back in the pool")


def advance(c, s, totals):
    """Pause, finish or launch the next wave; the caller writes campaign_spend afterwards."""
    camp = sq(c["campaign"])
    if c["status"] not in ("active", "dry_run"):
        return
    if now().hour in s["quiet_hours_utc"]:
        print(f"[{c['campaign']}] quiet hour (UTC {now().hour}): no new wave")
        return
    so_far, total = spent(c, s, totals), total_zyte_estimate(s, totals)
    budget = min(c["cap_usd"] - so_far, s["zyte_total_cap_usd"] - total)
    ratio = RATIOS.get(c["zyte_key"])
    basis = (f"Zyte ratio x{ratio[0]:.2f} over {ratio[1]} day(s) x {s['calibrated_margin']}" if ratio
             else f"no Zyte reading for {c['zyte_key']}: estimate x {s['cost_margin']}")
    print(f"[{c['campaign']}] cycle spend ~${so_far:,.2f} of ${c['cap_usd']:,.0f} ({basis}); "
          f"all Zyte ~${total:,.0f} of ${s['zyte_total_cap_usd']:,.0f}")
    if budget < 0.01:
        which = "campaign cap" if c["cap_usd"] - so_far < 0.01 else "total Zyte cap"
        set_status(c, "paused", f"{which} reached (campaign ~${so_far:,.2f}, all Zyte ~${total:,.0f})")
        return
    plan = plan_wave(c, s, budget)
    if plan.n == 0:
        left = one(f"""SELECT count(*) FROM {DB}.campaign_urls u
                       LEFT JOIN {DB}.campaign_hosts h ON h.campaign = u.campaign AND h.host = u.host
                       WHERE u.campaign = {camp} AND u.wave IS NULL AND coalesce(h.state, '') <> 'stopped'""")
        if left == 0:
            set_status(c, "complete", "every URL fetched, skipped or on a stopped host")
        else:
            set_status(c, "paused", f"next wave doesn't fit the ${budget:,.2f} left ({left:,} URLs waiting)")
        return
    if c["status"] == "dry_run":
        print(f"[{c['campaign']}] DRY RUN: next wave {plan.n:,} URLs on {plan.hosts} hosts, est ${plan.usd:,.2f} (budget ${budget:,.2f})")
        q("SELECT host, state, count(*) AS n, round(sum(est_usd), 2) AS usd FROM plan GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20").show(truncate=False)
        return
    launch_wave(c, s, plan)


def tick(c):
    s = settings(c)
    camp = sq(c["campaign"])
    load_urls(c)
    recover_launching(c)

    open_wave = q(f"SELECT * FROM {DB}.campaign_waves WHERE campaign = {camp} AND status = 'fetching'").collect()
    if open_wave:
        wv = open_wave[0]
        state = w.jobs.get_run(wv.run_id).state.life_cycle_state.value
        if state in RUNNING:
            if c["status"] not in ("active", "dry_run"):
                w.jobs.cancel_run(wv.run_id)
                print(f"[{c['campaign']}] is {c['status']}: cancelled wave {wv.wave} (run {wv.run_id}); scored next tick")
            elif spent(c, s, cycle_totals()) + wv.est_usd > c["cap_usd"] + 0.01:
                # The wave was trimmed to fit the cap when it launched, so only a cap lowered since then
                # lands here; landed rows can't be counted before the run's end-of-run flush.
                w.jobs.cancel_run(wv.run_id)
                set_status(c, "paused", f"cap ${c['cap_usd']:,.2f} is below spend plus wave {wv.wave}'s estimate; wave cancelled")
            else:
                print(f"[{c['campaign']}] wave {wv.wave} fetching (run {wv.run_id}, {state})")
            return
        landed_n = score_wave(c, s, wv.wave, wv.started_at.replace(tzinfo=timezone.utc))
        failure = broad_failure(c, s, wv.wave)
        if failure:
            requeue_failed_wave(c, wv.wave)
            q(f"""UPDATE {DB}.campaign_waves SET status = 'failed', ended_at = current_timestamp(), landed = {landed_n},
                  note = {sq(failure)} WHERE campaign = {camp} AND wave = {wv.wave}""")
            set_status(c, "paused", failure)
        else:
            q(f"""UPDATE {DB}.campaign_waves SET status = 'scored', ended_at = current_timestamp(), landed = {landed_n}
                  WHERE campaign = {camp} AND wave = {wv.wave}""")
            judge_hosts(c, s, wv.wave)
            print(f"[{c['campaign']}] wave {wv.wave} scored: {landed_n:,} fetches landed (run ended {state})")

    totals = cycle_totals()
    advance(c, s, totals)
    write_spend(c, s, totals)

# COMMAND ----------

dbutils.widgets.text("campaign", "", "Only this campaign (empty = every active one)")
only = dbutils.widgets.get("campaign").strip()
where = f"AND campaign = {sq(only)}" if only else ""
# paused campaigns still get ticks while a wave is open, so it is cancelled and scored
campaigns = [r.asDict() for r in q(f"""SELECT * FROM {DB}.campaigns
    WHERE (status IN ('active', 'dry_run')
           OR campaign IN (SELECT campaign FROM {DB}.campaign_waves WHERE status IN ('launching', 'fetching'))) {where}""").collect()]
if not campaigns:
    dbutils.notebook.exit("no active campaign")
read_zyte_stats()
RATIOS = key_ratios()
CAMPAIGN_KEYS = {r.campaign: r.zyte_key for r in q(f"SELECT campaign, zyte_key FROM {DB}.campaigns").collect()}
print("Zyte ratio per key (billed $ / raw estimate):", {k: f"x{v[0]:.2f} over {v[1]} day(s)" for k, v in RATIOS.items()} or "none yet")
for c in campaigns:
    tick(c)
