# Databricks notebook source
# MAGIC %md
# MAGIC # Match Locations To Works 4/6: XGBoost (oxjob #1668)
# MAGIC
# MAGIC Scores every (seed, candidate) pair with `openalex.works.location_work_matcher@challenger` (raw score, pinned training
# MAGIC categories). Hard rules first: pairs on a vetoed type boundary (`location_work_match_type_veto`, Casey 2026-10-15: matching
# MAGIC trusts the record's type), undeclared Kaggle versions (both DOIs `10.34740/kaggle/dsv/`) and a study registration with a
# MAGIC work that is not one unless both are the same registration (MergeIdenticalKeyWorks' #2063 rule: a shared DOI or OSF id;
# MAGIC Casey 2026-11-11, oxjob #1668: the matcher was joining OSF projects to their registrations) and a notice paired with the
# MAGIC work it is about (`notice_link_pairs`, oxjob #2169: retractions, corrections, errata, expressions of concern and comments
# MAGIC are separate works, and Crossref types most of them journal-article with the article's own title) are never scored. Writes
# MAGIC `location_work_match_xgb`: every scored pair with its raw score and its rank within the record. A record's top score
# MAGIC < 0.01 mints; >= 0.01 goes to Qwen. Scores in Spark through the model's UDF (a 705K-record night ran the driver out of
# MAGIC memory when the pairs went through pandas, 2026-10-30).

# COMMAND ----------

import json
import time

import mlflow
from pyspark.sql import Window, functions as F

NAME = "openalex.works.location_work_matcher"
T = "openalex.works.location_work_match_"
CATS = ["prov_a", "type_a", "type_b", "version_a", "prov_b", "version_b"]
KEEP = ["part", "seed_id", "cid", "decision", "stratum_prov", "weight", "raw_type_a", "dc_client"]
OSF = r"osf\\.io/(?:v2/registrations/)?([a-z0-9]{5})(?![a-z0-9])"   # MergeIdenticalKeyWorks' OSF guid
T0 = time.time()

# Registration pairs that are not the same registration: the record's DOIs and OSF ids (its keys and ids) against those of
# the work's locations, matched by explode + join (array set functions gave run-varying answers in big queries)
spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW sr_hold AS
  WITH p AS (
    SELECT seed_id, cid FROM {T}features
    WHERE (coalesce(type_a, '') = 'study-registration') <> (coalesce(type_b, '') = 'study-registration')
  ),
  rk AS (
    SELECT DISTINCT seed_id, k FROM (
      SELECT seed_id, CASE WHEN k0 LIKE 'd:%' THEN k0 END AS k FROM {T}fr LATERAL VIEW explode(keys) e AS k0
      UNION ALL SELECT seed_id, concat('o:', regexp_extract(k0, '{OSF}', 1)) FROM {T}fr LATERAL VIEW explode(keys) e AS k0
      UNION ALL SELECT seed_id, concat('d:', regexp_replace(lower(trim(i.id)), '^https?://(dx\\\\.)?doi\\\\.org/', ''))
        FROM {T}fr LATERAL VIEW explode(ids) e AS i WHERE lower(i.namespace) = 'doi'
    ) WHERE k IS NOT NULL AND k NOT IN ('d:', 'o:') AND seed_id IN (SELECT seed_id FROM p)
  ),
  wk AS (
    SELECT DISTINCT cid, k FROM (
      SELECT cid, concat('d:', regexp_replace(lower(l.doi), '^https?://(dx\\\\.)?doi\\\\.org/', '')) AS k
        FROM {T}fw LATERAL VIEW explode(locations) e AS l
      UNION ALL SELECT cid, concat('o:', regexp_extract(lower(coalesce(l.doi, '')), '{OSF}', 1)) FROM {T}fw LATERAL VIEW explode(locations) e AS l
      UNION ALL SELECT cid, concat('o:', regexp_extract(lower(coalesce(l.landing_page_url, '')), '{OSF}', 1))
        FROM {T}fw LATERAL VIEW explode(locations) e AS l
    ) WHERE k IS NOT NULL AND k NOT IN ('d:', 'o:') AND cid IN (SELECT cid FROM p)
  ),
  same AS (SELECT DISTINCT p.seed_id, p.cid FROM p JOIN rk ON rk.seed_id = p.seed_id JOIN wk ON wk.cid = p.cid AND wk.k = rk.k)
  SELECT seed_id, cid FROM p LEFT ANTI JOIN same USING (seed_id, cid)
""")
# A notice and the work it is about (oxjob #2169): the record's own DOI / PMID declares it a notice of a DOI / PMID the candidate
# holds (its ids, its locations' doi.org and PubMed pages). Same table as MapWorkIds' identifier veto and MergeIdenticalKeyWorks.
spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW notice_hold AS
  WITH rk AS (
    SELECT DISTINCT seed_id, CASE WHEN k LIKE 'd:%' THEN 'doi' ELSE 'pmid' END AS ns,
           CASE WHEN k LIKE 'd:%' THEN regexp_replace(substr(k, 3), '[^a-zA-Z0-9\\\\./-]', '') ELSE substr(k, 3) END AS id
    FROM {T}fr LATERAL VIEW explode(keys) e AS k
    WHERE k LIKE 'd:%' OR k LIKE 'p:%'
  ),
  tk AS (
    SELECT DISTINCT rk.seed_id, n.target_ns, n.target_id
    FROM rk JOIN openalex.works.notice_link_pairs n ON n.notice_ns = rk.ns AND n.notice_id = rk.id
  ),
  wk AS (
    SELECT DISTINCT cid,
           CASE WHEN k LIKE 'd:%' THEN 'doi' ELSE 'pmid' END AS ns,
           CASE WHEN k LIKE 'd:%' THEN regexp_replace(substr(k, 3), '[^a-zA-Z0-9\\\\./-]', '')
                WHEN k LIKE 'p:%' THEN substr(k, 3)
                ELSE regexp_extract(k, '^u:pubmed\\\\.ncbi\\\\.nlm\\\\.nih\\\\.gov/([0-9]+)$', 1) END AS id
    FROM {T}fw LATERAL VIEW explode(keys) e AS k
    WHERE (k LIKE 'd:%' OR k LIKE 'p:%' OR k RLIKE '^u:pubmed\\\\.ncbi\\\\.nlm\\\\.nih\\\\.gov/[0-9]+$')
      AND cid IN (SELECT DISTINCT f.cid FROM {T}features f LEFT SEMI JOIN tk ON tk.seed_id = f.seed_id)
  )
  SELECT DISTINCT f.seed_id, f.cid
  FROM {T}features f
  JOIN tk ON tk.seed_id = f.seed_id
  JOIN wk ON wk.cid = f.cid AND wk.ns = tk.target_ns AND wk.id = tk.target_id
""")
pairs = spark.sql(f"""
  WITH kag AS (
    SELECT DISTINCT r.id AS seed_id, w.id AS cid FROM {T}ftok r JOIN {T}ftok w
      ON r.side = 'r' AND w.side = 'w' AND r.kind = 'do' AND w.kind = 'do'
     AND r.t LIKE '10.34740/kaggle/dsv/%' AND w.t LIKE '10.34740/kaggle/dsv/%'
  )
  SELECT f.* FROM {T}features f
  LEFT ANTI JOIN {T}type_veto v ON v.type_a = f.type_a AND v.type_b = f.type_b
  LEFT ANTI JOIN kag k ON k.seed_id = f.seed_id AND k.cid = f.cid
  LEFT ANTI JOIN sr_hold s ON s.seed_id = f.seed_id AND s.cid = f.cid
  LEFT ANTI JOIN notice_hold nh ON nh.seed_id = f.seed_id AND nh.cid = f.cid
""")
n_all = spark.table(f"{T}features").count()
n_sr = spark.table("sr_hold").count()
n_notice = spark.table("notice_hold").count()

mlflow.set_registry_uri("databricks-uc")
mv = mlflow.MlflowClient().get_model_version_by_alias(NAME, "challenger")
score = mlflow.pyfunc.spark_udf(spark, f"models:/{NAME}/{mv.version}", result_type="double", env_manager="local")
feats = [c for c in pairs.columns if c not in KEEP]
inp = F.struct(*[(F.col(c).cast("string") if c in CATS else F.col(c).cast("double")).alias(c) for c in feats])
out = (pairs.withColumn("p_raw", score(inp))
       .withColumn("rank", F.row_number().over(Window.partitionBy("seed_id").orderBy(F.desc("p_raw"), "cid")))
       .select("seed_id", "cid", "p_raw", "rank", "is_bound", "ch_emb", "decision", "stratum_prov", "weight",
               F.lit(int(mv.version)).alias("model_version")))
out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{T}xgb")
s = spark.sql(f"""SELECT count(*) AS scored, count_if(rank = 1) AS records, count_if(rank = 1 AND p_raw >= 0.9156) AS top_ge_tau_bind,
  count_if(rank = 1 AND p_raw < 0.01) AS top_lt_tau_mint FROM {T}xgb""").collect()[0]
summary = {"pairs": n_all, "scored": s.scored, "vetoed": n_all - s.scored, "registration_held": n_sr, "notice_held": n_notice,
           "records": s.records,
           "top_ge_tau_bind": s.top_ge_tau_bind, "top_lt_tau_mint": s.top_lt_tau_mint, "model": f"{NAME} v{mv.version}"}
print(summary)
dbutils.jobs.taskValues.set(key="seconds", value=round(time.time() - T0, 1))
dbutils.notebook.exit(json.dumps(summary))
