# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze re-check: draw one pass (oxjob #1290)
# MAGIC
# MAGIC A bronze work rests on a publisher PDF that taxicab fetched once; nothing turns it off when the
# MAGIC publisher puts it behind a paywall. Once a quarter we re-fetch those PDF URLs on the hosts where a
# MAGIC paywall has a verified shape (the register, `bronze_recheck_score`) and close the ones that now land
# MAGIC on it. #1105 did the one-time catch-up (498K closed, Sep 2026); this is the repeating version.
# MAGIC
# MAGIC This notebook only **draws** the pass: it picks the URLs and writes them where the fetch side can
# MAGIC take them. It fetches nothing and changes no work.
# MAGIC
# MAGIC * `batch_table`: one row per URL in this pass, with its register group, its stratum (what taxicab
# MAGIC   last saw) and a priority. The scorer reads it.
# MAGIC * `source_table`: `url, native_id, native_id_namespace, priority`, the input of the **TaxiCab
# MAGIC   Campaign** guard (#1790), which runs the fetches: per-host pilot, stop rule, #1712's host breaker,
# MAGIC   its own Zyte key and a dollar cap. Start the campaign by hand once the guards are live:
# MAGIC   ```sql
# MAGIC   INSERT INTO openalex.taxicab.campaigns (campaign, source_table, zyte_key, cap_usd, status, settings, created_at, updated_at)
# MAGIC   VALUES ('bronze-2026-10', 'openalex.taxicab.bronze_recheck_urls', 'campaign-bronze', 300, 'active',
# MAGIC           '{"host_wave_cap": 6000, "max_per_host": 20}', current_timestamp(), current_timestamp());
# MAGIC   ```
# MAGIC
# MAGIC **Which URLs.** Bronze works whose only OA location is a journal's publisher PDF with no licence
# MAGIC (#1105's cohort), with a DOI, on a register group in `groups`. A URL is re-fetched when taxicab never
# MAGIC fetched it, or its newest fetch failed, was a 404, was HTML more than `fresh_evidence_days` ago, or was
# MAGIC a PDF more than `refetch_after_days` ago. A PDF served within `refetch_after_days` counts as this
# MAGIC quarter's check (skipped); HTML fetched within `fresh_evidence_days` is scored as it stands (stratum
# MAGIC `html_fresh`, no fetch). Wiley and IOP sit out: the harvester gets files there that readers can't
# MAGIC (#1291).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("pass_id", "2026-10", "Pass id (one per quarterly pass)")
dbutils.widgets.text("batch_table", "openalex.taxicab.bronze_recheck_batch", "Batch table (all passes)")
dbutils.widgets.text("source_table", "openalex.taxicab.bronze_recheck_urls", "Campaign source table (this pass only)")
dbutils.widgets.text("groups", "oup,cambridge,springer,sciencedirect,tandf,nature,aip,aps,sage,elsevier_doiorg,elsevier_society,acs,aha,karger,degruyter,thieme,iucr",
                     "Register groups in this pass (ieee, acm only after their live check)")
dbutils.widgets.text("refetch_after_days", "90", "Re-fetch a URL last served as a PDF this many days ago")
dbutils.widgets.text("fresh_evidence_days", "14", "Stored HTML this recent is scored without a fetch")
dbutils.widgets.text("dry_run", "true", "true = print counts only, write nothing")

pass_id = dbutils.widgets.get("pass_id").strip()
batch_table = dbutils.widgets.get("batch_table").strip()
source_table = dbutils.widgets.get("source_table").strip()
groups = [g.strip() for g in dbutils.widgets.get("groups").split(",") if g.strip()]
refetch_after_days = int(dbutils.widgets.get("refetch_after_days"))
fresh_evidence_days = int(dbutils.widgets.get("fresh_evidence_days"))
dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"

# COMMAND ----------

# Register group of a cohort PDF URL. Keep in step with GROUP_RULES in bronze_recheck_score.
GROUP_SQL = r"""
CASE WHEN host = 'academic.oup.com' THEN 'oup'
     WHEN host = 'link.springer.com' THEN 'springer'
     WHEN host = 'link.aps.org' THEN 'aps'
     WHEN host = 'pubs.aip.org' THEN 'aip'
     WHEN host = 'www.cambridge.org' THEN 'cambridge'
     WHEN host = 'www.nature.com' THEN 'nature'
     WHEN host = 'www.sciencedirect.com' THEN 'sciencedirect'
     WHEN host = 'www.tandfonline.com' THEN 'tandf'
     WHEN host = 'journals.sagepub.com' THEN 'sage'
     WHEN host = 'www.ahajournals.org' THEN 'aha'
     WHEN host IN ('www.karger.com', 'karger.com') THEN 'karger'
     WHEN host = 'www.degruyter.com' THEN 'degruyter'
     WHEN host = 'journals.iucr.org' THEN 'iucr'
     WHEN host = 'pubs.acs.org' THEN 'acs'
     WHEN host RLIKE '^www\\.thieme-connect\\.(de|com)$' THEN 'thieme'
     WHEN host IN ('doi.org', 'dx.doi.org') AND doi LIKE '10.1016/%' THEN 'elsevier_doiorg'
     WHEN host <> 'www.cell.com'
          AND pdf_url RLIKE '/(article/S?[0-9]{4}-[0-9]{3}[0-9X]\\([0-9]{2}\\)[0-9A-Z-]+/pdf|action/showPdf\\?pii=)' THEN 'elsevier_society'
     WHEN host = 'ieeexplore.ieee.org' THEN 'ieee'
     WHEN host = 'dl.acm.org' THEN 'acm'
     ELSE NULL END"""

PRIORITY = {"html_old": 40, "never": 30, "err": 20, "http404": 15, "pdf_old": 10, "html_fresh": 0}

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW recheck_draw AS
WITH cohort AS (
  SELECT id AS work_id, lower(regexp_replace(doi, '^https?://(dx\\.)?doi\\.org/', '')) AS doi, loc.pdf_url,
         lower(regexp_extract(loc.pdf_url, '^https?://([^/:]+)', 1)) AS host
  FROM (SELECT id, doi, oa_locs[0] AS loc, size(oa_locs) AS n_oa
        FROM (SELECT id, doi, filter(locations, l -> l.is_oa) AS oa_locs FROM openalex.works.openalex_works
              WHERE open_access.oa_status = 'bronze' AND doi IS NOT NULL))
  WHERE n_oa = 1 AND loc.pdf_url IS NOT NULL AND loc.license IS NULL AND loc.source.type = 'journal'
    AND loc.pdf_url LIKE 'http%'
),
grouped AS (SELECT *, {GROUP_SQL} AS grp FROM cohort),
inscope AS (
  SELECT g.* FROM grouped g
  LEFT ANTI JOIN (SELECT DISTINCT pdf_url FROM openalex.pdf.pdf_removed) r ON r.pdf_url = g.pdf_url
  WHERE g.grp IN ({",".join("'" + x + "'" for x in groups)})
),
newest AS (
  SELECT url, processed_date, status_code, content_type, resolved_url, is_soft_block, error,
         row_number() OVER (PARTITION BY url ORDER BY processed_date DESC) AS rn
  FROM openalex.taxicab.taxicab_results
  WHERE url IN (SELECT pdf_url FROM inscope)
)
SELECT '{pass_id}' AS pass_id, i.work_id, i.doi, i.pdf_url, i.host, i.grp,
       n.processed_date AS prev_fetched, n.status_code AS prev_status, n.content_type AS prev_content_type,
       n.resolved_url AS prev_resolved_url,
       CASE WHEN n.url IS NULL THEN 'never'
            WHEN n.error IS NULL AND n.status_code = 200 AND NOT coalesce(n.is_soft_block, false)
                 AND lower(n.content_type) LIKE '%html%'
                 THEN CASE WHEN n.processed_date >= current_timestamp() - INTERVAL {fresh_evidence_days} DAYS
                           THEN 'html_fresh' ELSE 'html_old' END
            WHEN n.error IS NULL AND n.status_code = 200 AND lower(n.content_type) LIKE '%pdf%'
                 THEN CASE WHEN n.processed_date >= current_timestamp() - INTERVAL {refetch_after_days} DAYS
                           THEN 'pdf_recent' ELSE 'pdf_old' END
            WHEN n.status_code IN (404, 410) THEN 'http404'
            ELSE 'err' END AS stratum,
       current_timestamp() AS drawn_at
FROM inscope i LEFT JOIN newest n ON n.url = i.pdf_url AND n.rn = 1
""")

prio = "CASE stratum " + " ".join(f"WHEN '{k}' THEN {v}" for k, v in PRIORITY.items()) + " ELSE NULL END"
draw = (spark.table("recheck_draw").withColumn("priority", F.expr(prio)).filter("stratum <> 'pdf_recent'")
        .select("pass_id", "work_id", "doi", "pdf_url", "host", "grp", "prev_fetched", "prev_status",
                "prev_content_type", "prev_resolved_url", "stratum", "drawn_at", "priority"))

# COMMAND ----------

# Write the draw once (serverless has no .cache()) and summarize from what was written.
# A dry run writes only a scratch copy in openalex.transient.
if dry_run:
    target = "openalex.transient.bronze_recheck_draw_dryrun"
    draw.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
else:
    target = batch_table
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {batch_table} (
      pass_id STRING, work_id STRING, doi STRING, pdf_url STRING, host STRING, grp STRING,
      prev_fetched TIMESTAMP, prev_status INT, prev_content_type STRING, prev_resolved_url STRING,
      stratum STRING, drawn_at TIMESTAMP, priority INT) USING DELTA""")
    if spark.sql(f"SELECT count(*) FROM {batch_table} WHERE pass_id = '{pass_id}'").collect()[0][0]:
        dbutils.notebook.exit(f"pass {pass_id} is already drawn in {batch_table}; pick a new pass_id")
    draw.write.mode("append").saveAsTable(batch_table)

drawn = spark.table(target).filter(F.col("pass_id") == pass_id)
(drawn.groupBy("grp").pivot("stratum", list(PRIORITY)).count().na.fill(0)
      .withColumn("fetch", F.col("never") + F.col("err") + F.col("http404") + F.col("pdf_old") + F.col("html_old"))
      .orderBy(F.desc("fetch")).show(50, truncate=False))
n_all = drawn.count()
n_fetch = drawn.filter("stratum <> 'html_fresh'").count()
print(f"pass {pass_id}: {n_all:,} URLs drawn into {target} ({n_fetch:,} to fetch, {n_all - n_fetch:,} scored as stored)")
if dry_run:
    dbutils.notebook.exit(f"dry run: drawn into {target} only; no batch, no campaign source")

# The campaign fetches everything except html_fresh (already fresh evidence).
(drawn.filter("stratum <> 'html_fresh'")
      .select(F.col("pdf_url").alias("url"), F.col("doi").alias("native_id"),
              F.lit("doi").alias("native_id_namespace"), "priority")
      .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(source_table))
print(f"wrote {source_table}: {spark.table(source_table).count():,} URLs for the campaign")
