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
# MAGIC 1. its wave is still fetching → check spend so far; cancel the wave if the campaign cap is hit;
# MAGIC 2. its wave finished → score it (`campaign_results`), judge each host, maybe pause the campaign;
# MAGIC 3. no wave open → check the caps, plan the next wave, queue it, launch **TaxiCab Rescrape** on the
# MAGIC    campaign's own queue table with the campaign's own Zyte API key.
# MAGIC
# MAGIC **Pilot.** A host's first wave takes at most `pilot_first` (200) of its URLs; if it passes, its next
# MAGIC waves take it to `pilot_total` (1,000); only then does it get full waves (`host_wave_cap` per wave).
# MAGIC Waves always run at production concurrency (600 workers, 10 per host).
# MAGIC
# MAGIC **Stop rule** (a stopped host gets no more URLs from this campaign), judged after every wave:
# MAGIC - *walled* (#1105's rule): blocked fetches (soft block, 202, 403, 429, fbi.gov redirect) ≥ 20 and
# MAGIC   above max(2%, the host's 7-day blocked share in `taxicab_results` + 2 points);
# MAGIC - *nothing back*: under `min_useful_share` (50%) of 100+ fetches came back useful (stored page) or
# MAGIC   as a clear answer (404/410);
# MAGIC - *too expensive*: estimated Zyte $ per 1,000 useful-or-answer fetches over `max_usd_per_1k_useful`
# MAGIC   ($15), unless the host is in `allow_hosts`.
# MAGIC A wave where 90%+ of 200+ fetches failed across 3+ hosts pauses the whole campaign instead (the
# MAGIC campaign key hit its Zyte limit, or the harvester is down); its rows go back to the pool.
# MAGIC
# MAGIC **Dollar caps, two layers.** (a) Zyte enforces each campaign key's own spending limit for the
# MAGIC billing cycle (Zyte dashboard → Spending Alerts → API key, "Pause processing requests"), so a campaign
# MAGIC can never spend the main key's money or stop new-content scraping. (b) This guard estimates spend
# MAGIC (fetches × per-domain price from `zyte_prices`, × `cost_margin`) and pauses before Zyte's limit:
# MAGIC it pauses the campaign at `cap_usd` for the billing cycle (from the 2nd of the month), trims a wave
# MAGIC to fit what is left, and pauses everything if all campaigns plus steady use would pass
# MAGIC `zyte_total_cap_usd` ($9K; Zyte pauses ALL usage at $12K). Spend per campaign per cycle is written
# MAGIC to `campaign_spend` for the charter's daily check.
# MAGIC
# MAGIC **Run a campaign:** put its URLs in a table with `url, native_id, native_id_namespace` (and an
# MAGIC optional int `priority`, higher first; `native_id` is a bare DOI for namespace `doi`), then
# MAGIC ```sql
# MAGIC INSERT INTO openalex.taxicab.campaigns (campaign, source_table, zyte_key, cap_usd, status, settings, created_at, updated_at)
# MAGIC VALUES ('bronze-2026-10', 'openalex.taxicab.bronze_recheck_urls', 'campaign-bronze', 2500, 'active', '{}', current_timestamp(), current_timestamp());
# MAGIC ```
# MAGIC and make sure the TaxiCab Campaign schedule is unpaused (`jobs/taxicab_campaign.yaml`). `settings` is
# MAGIC JSON overriding `DEFAULTS` below. Pause: `UPDATE ... SET status = 'paused'`. Resume after a cap
# MAGIC pause: raise `cap_usd` (and the key's Zyte limit), set `status = 'active'`.

# COMMAND ----------

import json
import re
from datetime import datetime, timedelta, timezone

from databricks.sdk import WorkspaceClient
from pyspark.sql import Window
from pyspark.sql import functions as F

RESCRAPE_JOB_ID = 866093356324381   # TaxiCab Rescrape
DB = "openalex.taxicab"
BREAKER = f"{DB}.host_breaker"      # #1712's shared per-host breaker; skipped if absent

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
    "min_useful_share": 0.5,
    "max_usd_per_1k_useful": 15.0,
    "allow_hosts": [],
    "campaign_fail_share": 0.9,
    # $ per 1,000 fetches for a domain missing from zyte_prices: #1290 measured the bronze long tail
    # at $0.19-0.29 per 1,000 works (EXPLORE § 1)
    "default_usd_per_1k": 0.30,
    "cost_margin": 1.25,             # re-asks, recipes with extra calls, price drift
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


def cycle_start(t=None):
    """Start of the Zyte billing cycle (the 2nd of the month). Midnight UTC counts a little more spend
    than Zyte does, which errs on the safe side."""
    t = t or now()
    start = t.replace(day=2, hour=0, minute=0, second=0, microsecond=0)
    if t < start:
        start = (start - timedelta(days=3)).replace(day=2)
    return start


def ts(t):
    return t.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

q(f"""CREATE TABLE IF NOT EXISTS {DB}.campaigns (
  campaign STRING, source_table STRING, zyte_key STRING, cap_usd DOUBLE, status STRING,
  status_reason STRING, settings STRING, created_at TIMESTAMP, updated_at TIMESTAMP) USING DELTA""")
q(f"""CREATE TABLE IF NOT EXISTS {DB}.campaign_urls (
  campaign STRING, url STRING, native_id STRING, native_id_namespace STRING, host STRING, domain STRING,
  priority INT, shuffle DOUBLE, wave INT, attempts INT) USING DELTA""")
q(f"""CREATE TABLE IF NOT EXISTS {DB}.campaign_hosts (
  campaign STRING, host STRING, state STRING, reason STRING, updated_at TIMESTAMP) USING DELTA""")
q(f"""CREATE TABLE IF NOT EXISTS {DB}.campaign_waves (
  campaign STRING, wave INT, run_id BIGINT, status STRING, queued INT, est_usd DOUBLE,
  started_at TIMESTAMP, ended_at TIMESTAMP, landed INT, note STRING) USING DELTA""")
q(f"""CREATE TABLE IF NOT EXISTS {DB}.campaign_results (
  campaign STRING, wave INT, url STRING, host STRING, domain STRING, taxicab_id STRING, status_code INT,
  resolved_url STRING, content_type STRING, is_soft_block BOOLEAN, error STRING, processed_date TIMESTAMP,
  outcome STRING, est_usd DOUBLE) USING DELTA""")
q(f"""CREATE TABLE IF NOT EXISTS {DB}.campaign_spend (
  campaign STRING, zyte_key STRING, cycle_start TIMESTAMP, est_usd DOUBLE, cap_usd DOUBLE, fetches BIGINT,
  useful BIGINT, status STRING, updated_at TIMESTAMP) USING DELTA""")
q(f"""CREATE TABLE IF NOT EXISTS {DB}.zyte_prices (
  domain STRING, usd_per_1k_fetches DOUBLE, source STRING, measured_through DATE) USING DELTA""")

# COMMAND ----------

def host_col(url_col):
    return F.lower(F.expr(f"try_parse_url({url_col}, 'HOST')"))


def domain_col(host):
    """Registered domain, which is what Zyte prices by: last two labels, three under a ccTLD with a
    short second level (co.uk, ac.jp, go.jp)."""
    d2 = F.regexp_extract(host, r"([^.]+\.[^.]+)$", 1)
    d3 = F.regexp_extract(host, r"([^.]+\.[^.]+\.[^.]+)$", 1)
    tld = F.regexp_extract(host, r"\.([^.]+)$", 1)
    sld = F.regexp_extract(host, r"([^.]+)\.[^.]+$", 1)
    return F.when((F.length(tld) == 2) & (F.length(sld) <= 3) & (d3 != ""), d3).otherwise(d2)


# Outcome of one taxicab_results row (columns unqualified)
BLOCKED = ("(coalesce(is_soft_block, false) OR status_code IN (202, 403, 429) "
           "OR coalesce(resolved_url, '') LIKE '%fbi.gov%')")
OUTCOME = f"""CASE
  WHEN processed_date IS NULL THEN NULL
  WHEN status_code = 400 AND coalesce(error, '') RLIKE 'got None' THEN 'ban'
  WHEN {BLOCKED} THEN 'blocked'
  WHEN status_code = 200 AND error IS NULL THEN 'useful'
  WHEN status_code IN (404, 410) THEN 'answer'
  ELSE 'failed' END"""
# Zyte doesn't bill bans; an unknown campaign key is refused before any Zyte call
FREE = "(outcome = 'ban' OR coalesce(error, '') LIKE '%unknown zyte_key%')"
RESULT_COLS = ["campaign", "wave", "url", "host", "domain", "taxicab_id", "status_code", "resolved_url",
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


def prices():
    return spark.table(f"{DB}.zyte_prices").select("domain", "usd_per_1k_fetches")

# COMMAND ----------

def load_urls(c):
    """First tick of a campaign: copy its source table into campaign_urls."""
    if one(f"SELECT count(*) FROM {DB}.campaign_urls WHERE campaign = {sq(c['campaign'])}"):
        return
    src = spark.table(c["source_table"])
    priority = F.col("priority").cast("int") if "priority" in src.columns else F.lit(0)
    urls = (
        src.select("url", "native_id", "native_id_namespace", priority.alias("priority"))
        .filter(F.col("url").isNotNull())
        .dropDuplicates(["url"])
        .withColumn("host", host_col("url"))
        .filter(F.col("host").isNotNull() & (F.col("host") != "localhost") & ~F.col("host").rlike(r"^[0-9.\[\]:]+$"))
        .withColumn("domain", domain_col(F.col("host")))
        .select(F.lit(c["campaign"]).alias("campaign"), "url", "native_id", "native_id_namespace", "host", "domain",
                F.coalesce("priority", F.lit(0)).alias("priority"), F.rand().alias("shuffle"),
                F.lit(None).cast("int").alias("wave"), F.lit(0).alias("attempts"))
    )
    urls.write.mode("append").saveAsTable(f"{DB}.campaign_urls")
    n = one(f"SELECT count(*) FROM {DB}.campaign_urls WHERE campaign = {sq(c['campaign'])}")
    print(f"[{c['campaign']}] loaded {n:,} URLs from {c['source_table']}")


def wave_rows(c, wv, since):
    """The wave's URLs joined to their newest taxicab_results row since the wave started (no write)."""
    s = settings(c)
    latest = (
        spark.table(f"{DB}.taxicab_results")
        .filter(F.col("processed_date") >= F.lit(since))
        .withColumn("rn", F.row_number().over(Window.partitionBy("url").orderBy(F.desc("processed_date"))))
        .filter("rn = 1").drop("rn")
    )
    u = (spark.table(f"{DB}.campaign_urls").filter((F.col("campaign") == c["campaign"]) & (F.col("wave") == wv))
         .select("campaign", "wave", "url", "host", F.col("domain").alias("url_domain")))
    t = latest.select("url", "taxicab_id", "status_code", "resolved_url", "content_type", "is_soft_block", "error",
                      "processed_date")
    # doi.org links are resolved by the harvester for free; Zyte bills the publisher's domain
    price_domain = F.when(F.col("url_domain") == "doi.org", domain_col(host_col("resolved_url"))).otherwise(F.col("url_domain"))
    return (
        u.join(t, "url", "left")
        .withColumn("domain", price_domain)
        .withColumn("outcome", F.expr(OUTCOME))
        .join(prices(), "domain", "left")
        .withColumn("est_usd", F.when(F.col("outcome").isNull() | F.expr(FREE), F.lit(0.0))
                    .otherwise(F.coalesce("usd_per_1k_fetches", F.lit(s["default_usd_per_1k"])) / 1000))
        .select(*RESULT_COLS)
    )


def cycle_spend(c):
    """Estimated $ this billing cycle (margin included), from scored waves."""
    s = settings(c)
    raw = one(f"""SELECT coalesce(sum(est_usd), 0) FROM {DB}.campaign_results
                  WHERE campaign = {sq(c['campaign'])} AND processed_date >= '{ts(cycle_start())}'""")
    return float(raw) * s["cost_margin"]


def write_spend(c):
    s = settings(c)
    r = q(f"""SELECT coalesce(sum(est_usd), 0) AS usd, count(*) AS n, coalesce(sum(CASE WHEN outcome = 'useful' THEN 1 ELSE 0 END), 0) AS useful
              FROM {DB}.campaign_results WHERE campaign = {sq(c['campaign'])} AND processed_date >= '{ts(cycle_start())}'""").collect()[0]
    q(f"""MERGE INTO {DB}.campaign_spend t
          USING (SELECT {sq(c['campaign'])} AS campaign, {sq(c['zyte_key'] or '')} AS zyte_key,
                        TIMESTAMP '{ts(cycle_start())}' AS cycle_start, {float(r.usd) * s['cost_margin']} AS est_usd,
                        {float(c['cap_usd'])} AS cap_usd, {int(r.n)} AS fetches, {int(r.useful)} AS useful,
                        {sq(c['status'])} AS status, current_timestamp() AS updated_at) s
          ON t.campaign = s.campaign AND t.cycle_start = s.cycle_start
          WHEN MATCHED THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *""")


def total_zyte_estimate(s):
    """Steady (non-campaign) use so far this cycle plus every campaign's estimate."""
    days = (now() - cycle_start()).total_seconds() / 86400
    campaigns = one(f"""SELECT coalesce(sum(est_usd), 0) FROM {DB}.campaign_results
                        WHERE processed_date >= '{ts(cycle_start())}'""")
    return s["steady_usd_per_day"] * days + float(campaigns) * s["cost_margin"]

# COMMAND ----------

def score_wave(c, wv, started_at):
    """Write the wave's results; send unfetched URLs back to the pool (twice unfetched: skipped)."""
    rows = wave_rows(c, wv, started_at - timedelta(minutes=5))
    rows.filter("outcome IS NOT NULL").write.mode("append").saveAsTable(f"{DB}.campaign_results")
    q(f"""MERGE INTO {DB}.campaign_urls u
          USING (SELECT url FROM {DB}.campaign_urls WHERE campaign = {sq(c['campaign'])} AND wave = {wv}
                 EXCEPT SELECT url FROM {DB}.campaign_results WHERE campaign = {sq(c['campaign'])} AND wave = {wv}) m
          ON u.campaign = {sq(c['campaign'])} AND u.url = m.url
          WHEN MATCHED THEN UPDATE SET wave = CASE WHEN u.attempts >= 1 THEN -1 ELSE NULL END, attempts = u.attempts + 1""")
    q(f"TRUNCATE TABLE {queue_table(c)}")
    return one(f"SELECT count(*) FROM {DB}.campaign_results WHERE campaign = {sq(c['campaign'])} AND wave = {wv}")


def broad_failure(c, wv):
    s = settings(c)
    r = q(f"""SELECT count(*) AS n, count(DISTINCT host) AS hosts,
                     coalesce(sum(CASE WHEN outcome IN ('failed', 'ban') THEN 1 ELSE 0 END), 0) AS bad
              FROM {DB}.campaign_results WHERE campaign = {sq(c['campaign'])} AND wave = {wv}""").collect()[0]
    if r.n >= 200 and r.hosts >= 3 and r.bad >= s["campaign_fail_share"] * r.n:
        return f"wave {wv}: {r.bad:,} of {r.n:,} fetches failed across {r.hosts} hosts (campaign key at its Zyte limit, or harvester down)"
    return None


def requeue_failed_wave(c, wv):
    q(f"""MERGE INTO {DB}.campaign_urls u
          USING (SELECT url FROM {DB}.campaign_results WHERE campaign = {sq(c['campaign'])} AND wave = {wv}
                 AND outcome IN ('failed', 'ban')) m
          ON u.campaign = {sq(c['campaign'])} AND u.url = m.url
          WHEN MATCHED THEN UPDATE SET wave = NULL""")
    q(f"DELETE FROM {DB}.campaign_results WHERE campaign = {sq(c['campaign'])} AND wave = {wv} AND outcome IN ('failed', 'ban')")


def judge_hosts(c, wv):
    """Stop rule and pilot progress for every host in the wave."""
    s = settings(c)
    camp = sq(c["campaign"])
    hosts_in_wave = f"SELECT DISTINCT host FROM {DB}.campaign_results WHERE campaign = {camp} AND wave = {wv}"
    baseline = (
        spark.table(f"{DB}.taxicab_results")
        .filter(F.col("processed_date") >= F.lit(now() - timedelta(days=7)))
        .withColumn("host", host_col("url"))
        .join(spark.sql(hosts_in_wave), "host", "left_semi")
        .groupBy("host").agg(F.avg(F.expr(BLOCKED).cast("double")).alias("b7"))
    )
    baseline.createOrReplaceTempView("baseline")
    stats = q(f"""
      WITH wv AS (
        SELECT host, count(*) AS n, sum(CASE WHEN outcome = 'blocked' THEN 1 ELSE 0 END) AS blocked
        FROM {DB}.campaign_results WHERE campaign = {camp} AND wave = {wv} GROUP BY host),
      cum AS (
        SELECT host, count(*) AS n, sum(CASE WHEN outcome IN ('useful', 'answer') THEN 1 ELSE 0 END) AS good,
               sum(est_usd) AS usd
        FROM {DB}.campaign_results WHERE campaign = {camp} GROUP BY host),
      wvgood AS (
        SELECT host, sum(CASE WHEN outcome IN ('useful', 'answer') THEN 1 ELSE 0 END) AS good
        FROM {DB}.campaign_results WHERE campaign = {camp} AND wave = {wv} GROUP BY host),
      left_ AS (
        SELECT host, count(*) AS left_n FROM {DB}.campaign_urls WHERE campaign = {camp} AND wave IS NULL GROUP BY host)
      SELECT wv.host, wv.n AS wn, wv.blocked AS wb, g.good AS wg, cum.n AS cn, cum.good AS cg, cum.usd AS usd,
             coalesce(b.b7, 0) AS b7, coalesce(l.left_n, 0) AS left_n, coalesce(h.state, 'pilot1') AS state
      FROM wv
      JOIN cum ON cum.host = wv.host
      JOIN wvgood g ON g.host = wv.host
      LEFT JOIN baseline b ON b.host = wv.host
      LEFT JOIN left_ l ON l.host = wv.host
      LEFT JOIN {DB}.campaign_hosts h ON h.campaign = {camp} AND h.host = wv.host""").collect()

    updates, stops = [], []
    for r in stats:
        reason = None
        limit = max(s["stop_blocked_share"], r.b7 + s["stop_over_baseline"])
        if r.wb >= s["stop_min_blocked"] and r.wb > limit * r.wn:
            reason = f"walled: {r.wb}/{r.wn} blocked in wave {wv} (7-day baseline {r.b7:.0%})"
        n, good = (r.wn, r.wg) if r.wn >= s["min_fetches_judged"] else (r.cn, r.cg)
        if not reason and n >= s["min_fetches_judged"] and good < s["min_useful_share"] * n:
            reason = f"nothing back: {good}/{n} fetches useful or a clear 404"
        if (not reason and r.host not in s["allow_hosts"] and r.cn >= s["min_fetches_judged"] and r.cg
                and r.usd * s["cost_margin"] / r.cg * 1000 > s["max_usd_per_1k_useful"]):
            reason = f"too expensive: ${r.usd * s['cost_margin'] / r.cg * 1000:.2f} per 1,000 useful"
        if reason:
            state = "stopped"
            stops.append((r.host, reason))
        elif r.state == "pilot1" and (r.cn >= s["pilot_first"] or r.left_n == 0):
            state, reason = "pilot2", f"pilot 1 passed: {r.cg}/{r.cn} useful or 404"
        elif r.state == "pilot2" and (r.cn >= s["pilot_total"] or r.left_n == 0):
            state, reason = "admitted", f"pilot passed: {r.cg}/{r.cn} useful or 404"
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
    for host, reason in stops:
        print(f"[{c['campaign']}] STOP {host}: {reason}")
    return stats

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


def plan_wave(c, budget_usd):
    """Next wave: per-host quotas by pilot stage, round-robin by host, trimmed to the budget."""
    s = settings(c)
    camp = sq(c["campaign"])
    skip = breaker_hosts()
    skip_sql = f"AND u.host NOT IN ({', '.join(sq(h) for h in skip)})" if skip else ""
    planned = q(f"""
      WITH done AS (SELECT host, count(*) AS n FROM {DB}.campaign_results WHERE campaign = {camp} GROUP BY host),
      pool AS (
        SELECT u.url, u.native_id, u.native_id_namespace, u.host, u.domain, coalesce(h.state, 'pilot1') AS state,
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
        FROM pool),
      picked AS (
        SELECT quota.*, coalesce(p.usd_per_1k_fetches, {s['default_usd_per_1k']}) / 1000 * {s['cost_margin']} AS est_usd
        FROM quota LEFT JOIN {DB}.zyte_prices p ON p.domain = quota.domain
        WHERE rn <= q ORDER BY rn, host LIMIT {int(s['wave_size'])}),
      costed AS (
        SELECT *, sum(est_usd) OVER (ORDER BY rn, host ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_usd
        FROM picked)
      SELECT url, native_id, native_id_namespace, host, state, rn, est_usd FROM costed WHERE cum_usd <= {budget_usd}""")
    # Materialized on the driver (at most wave_size rows): a view would re-read campaign_urls after
    # launch_wave marks the wave and come back empty.
    rows = planned.collect()
    spark.createDataFrame(rows, planned.schema).createOrReplaceTempView("plan")
    return q("SELECT count(*) AS n, coalesce(sum(est_usd), 0) AS usd, count(DISTINCT host) AS hosts FROM plan").collect()[0]


def launch_wave(c, plan):
    s = settings(c)
    camp = sq(c["campaign"])
    qt = queue_table(c)
    wv = int(one(f"SELECT coalesce(max(wave), 0) + 1 FROM {DB}.campaign_waves WHERE campaign = {camp}"))
    q(f"CREATE TABLE IF NOT EXISTS {qt} (native_id STRING, native_id_namespace STRING, created_date TIMESTAMP, pdf_url STRING) USING DELTA")
    if one(f"SELECT count(*) FROM {qt}"):
        raise RuntimeError(f"{qt} still holds rows with no open wave; inspect before launching")
    # shortcut: a MERGE of ~20K random urls rewrites much of campaign_urls (deletion vectors keep it
    # cheap); fine at a few waves an hour, move wave membership to an append-only table if it isn't
    q(f"""MERGE INTO {DB}.campaign_urls u USING plan p ON u.campaign = {camp} AND u.url = p.url
          WHEN MATCHED THEN UPDATE SET wave = {wv}""")
    # the notebook shuffles and interleaves by host, so insert order doesn't matter
    q(f"INSERT INTO {qt} SELECT native_id, native_id_namespace, current_timestamp(), url FROM plan")
    q(f"""INSERT INTO {DB}.campaign_waves VALUES ({camp}, {wv}, NULL, 'launching', {plan.n}, {plan.usd},
          current_timestamp(), NULL, NULL, {sq(f'{plan.hosts} hosts')})""")
    params = {"queue_table": qt, "rescrape_queue_only": "true", "url_limit": str(plan.n + 100),
              "harvest_workers": str(s["harvest_workers"]), "max_per_host": str(s["max_per_host"]),
              "lookback_days": "3", "doi_host_key": "host"}
    if c["zyte_key"]:
        params["zyte_key"] = c["zyte_key"]
    try:
        run = w.jobs.run_now(job_id=RESCRAPE_JOB_ID, job_parameters=params)
    except Exception as e:   # e.g. TaxiCab Rescrape already at max_concurrent_runs: undo, try next tick
        q(f"UPDATE {DB}.campaign_urls SET wave = NULL WHERE campaign = {camp} AND wave = {wv}")
        q(f"TRUNCATE TABLE {qt}")
        q(f"DELETE FROM {DB}.campaign_waves WHERE campaign = {camp} AND wave = {wv}")
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
            q(f"UPDATE {DB}.campaign_urls SET wave = NULL WHERE campaign = {camp} AND wave = {wv.wave}")
            q(f"TRUNCATE TABLE {queue_table(c)}")
            q(f"DELETE FROM {DB}.campaign_waves WHERE campaign = {camp} AND wave = {wv.wave}")
            print(f"[{c['campaign']}] wave {wv.wave}: never launched; URLs back in the pool")


def tick(c):
    s = settings(c)
    camp = sq(c["campaign"])
    load_urls(c)
    recover_launching(c)

    open_wave = q(f"SELECT * FROM {DB}.campaign_waves WHERE campaign = {camp} AND status = 'fetching'").collect()
    if open_wave:
        wv = open_wave[0]
        started = wv.started_at.replace(tzinfo=timezone.utc)
        state = w.jobs.get_run(wv.run_id).state.life_cycle_state.value
        if state in RUNNING:
            if c["status"] not in ("active", "dry_run"):
                w.jobs.cancel_run(wv.run_id)
                print(f"[{c['campaign']}] is {c['status']}: cancelled wave {wv.wave} (run {wv.run_id}); scored next tick")
                return
            landed = wave_rows(c, wv.wave, started - timedelta(minutes=5)).filter("outcome IS NOT NULL")
            so_far = float(landed.agg(F.coalesce(F.sum("est_usd"), F.lit(0.0))).collect()[0][0]) * s["cost_margin"]
            spent = cycle_spend(c) + so_far
            print(f"[{c['campaign']}] wave {wv.wave} fetching (run {wv.run_id}, {state}): ~${so_far:,.2f} so far, cycle ~${spent:,.2f} of ${c['cap_usd']:,.0f}")
            if spent >= c["cap_usd"]:
                w.jobs.cancel_run(wv.run_id)
                set_status(c, "paused", f"cap ${c['cap_usd']:,.0f} reached mid-wave {wv.wave} (~${spent:,.2f}); wave cancelled")
            return
        landed_n = score_wave(c, wv.wave, started)
        failure = broad_failure(c, wv.wave)
        status, note = ("failed", failure) if failure else ("scored", None)
        if failure:
            requeue_failed_wave(c, wv.wave)
        q(f"""UPDATE {DB}.campaign_waves SET status = {sq(status)}, ended_at = current_timestamp(), landed = {landed_n},
              note = coalesce({sq(note) if note else 'NULL'}, note) WHERE campaign = {camp} AND wave = {wv.wave}""")
        if failure:
            set_status(c, "paused", failure)
        else:
            judge_hosts(c, wv.wave)
            print(f"[{c['campaign']}] wave {wv.wave} scored: {landed_n:,} fetches landed (run ended {state})")

    write_spend(c)
    if c["status"] not in ("active", "dry_run"):
        return
    if now().hour in s["quiet_hours_utc"]:
        print(f"[{c['campaign']}] quiet hour (UTC {now().hour}): no new wave")
        return

    spent, total = cycle_spend(c), total_zyte_estimate(s)
    budget = min(c["cap_usd"] - spent, s["zyte_total_cap_usd"] - total)
    print(f"[{c['campaign']}] cycle spend ~${spent:,.2f} of ${c['cap_usd']:,.0f}; all Zyte ~${total:,.0f} of ${s['zyte_total_cap_usd']:,.0f}")
    if budget <= 0:
        which = "campaign cap" if c["cap_usd"] - spent <= 0 else "total Zyte cap"
        set_status(c, "paused", f"{which} reached (campaign ~${spent:,.2f}, all Zyte ~${total:,.0f})")
        write_spend(c)
        return
    plan = plan_wave(c, budget)
    if plan.n == 0:
        left = one(f"""SELECT count(*) FROM {DB}.campaign_urls u
                       LEFT JOIN {DB}.campaign_hosts h ON h.campaign = u.campaign AND h.host = u.host
                       WHERE u.campaign = {camp} AND u.wave IS NULL AND coalesce(h.state, '') <> 'stopped'""")
        if left == 0:
            set_status(c, "complete", "every URL fetched, skipped or on a stopped host")
        else:
            set_status(c, "paused", f"next wave doesn't fit the ${budget:,.2f} left ({left:,} URLs waiting)")
        write_spend(c)
        return
    if c["status"] == "dry_run":
        print(f"[{c['campaign']}] DRY RUN: next wave {plan.n:,} URLs on {plan.hosts} hosts, est ${plan.usd:,.2f} (budget ${budget:,.2f})")
        q("SELECT host, state, count(*) AS n, round(sum(est_usd), 2) AS usd FROM plan GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20").show(truncate=False)
        return
    launch_wave(c, plan)

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
for c in campaigns:
    tick(c)
