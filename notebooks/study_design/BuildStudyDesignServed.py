# Databricks notebook source
# MAGIC %md
# MAGIC # Build the served study-design table (oxjob #1312)
# MAGIC
# MAGIC `works_study_design`: one row per work with at least one study-design value.
# MAGIC
# MAGIC Provenance rule (Jason, 2026-09-22): where a **MEDLINE-indexed** PubMed
# MAGIC record carries a study-characteristics tag (MeSH V03), PubMed's values are
# MAGIC served; otherwise the tagger's. Both are stored so disagreement on the live
# MAGIC corpus is measurable. Parents are implied (RCT ⇒ Clinical Trial;
# MAGIC Meta-Analysis ⇒ Systematic Review). Publication formats (Editorial, Letter,
# MAGIC Review, Guideline …) are `type`'s business and never appear here.
# MAGIC
# MAGIC Full rebuild every run (CREATE OR REPLACE): ~41M PubMed rows + the tagger
# MAGIC table. NOT wired into CreateWorksEnriched / ES yet (oxjob #1312 step 6);
# MAGIC nothing downstream reads it until then.

# COMMAND ----------

import os
import sys
import time

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if not os.path.exists(os.path.join(REPO_ROOT, "utils", "study_design.py")):
    for cand in ("/Workspace/Repos", "/Workspace/Shared"):
        for dirpath, dirnames, filenames in os.walk(cand):
            if "study_design.py" in filenames and dirpath.endswith("utils"):
                REPO_ROOT = os.path.dirname(dirpath)
                break
sys.path.insert(0, REPO_ROOT)
from utils import study_design as sd  # noqa: E402

dbutils.widgets.text("schema", "openalex.works", "target schema")
SCHEMA = dbutils.widgets.get("schema").strip()
TAGGER = f"{SCHEMA}.works_study_design_tagger"
SERVED = f"{SCHEMA}.works_study_design"

CANON = ", ".join(f"'{sd.VALUE_ID[c]}'" for c in sd.CLASSES)   # canonical value order
RCT, CT, MA, SR = (sd.VALUE_ID[c] for c in ("rct", "clinical_trial", "meta_analysis", "systematic_review"))

# COMMAND ----------

t0 = time.time()
spark.sql(f"""
CREATE OR REPLACE TABLE {SERVED}
USING DELTA CLUSTER BY (work_id)
COMMENT 'Study design per work (oxjob #1312): PubMed (MEDLINE V03 tags) where present, else automated tagging'
AS
WITH pm AS (
  SELECT pmid, types FROM (
    SELECT pmid, MedlineCitation._Status AS status,
           transform(MedlineCitation.Article.PublicationTypeList.PublicationType, x -> x._VALUE) AS types,
           row_number() OVER (PARTITION BY pmid ORDER BY revised_date DESC NULLS LAST) AS rn
    FROM openalex.pubmed.pubmed_exploded)
  WHERE rn = 1 AND status = 'MEDLINE'
),
pm_mapped AS (
  SELECT pmid, filter(transform(types, t -> {sd.sql_pubmed_case()}), v -> v IS NOT NULL) AS mapped FROM pm
),
pm_vals AS (
  SELECT pmid,
         filter(array({CANON}), v -> array_contains(mapped, v)
                                     OR (v = '{CT}' AND array_contains(mapped, '{RCT}'))
                                     OR (v = '{SR}' AND array_contains(mapped, '{MA}'))) AS pubmed_values
  FROM pm_mapped WHERE size(mapped) > 0
),
w_pm AS (
  SELECT w.id AS work_id, p.pubmed_values
  FROM openalex.works.openalex_works w
  JOIN pm_vals p ON p.pmid = CAST(regexp_extract(w.ids['pmid'], '(\\\\d+)$', 1) AS BIGINT)
  WHERE NOT w.is_xpac AND w.ids['pmid'] IS NOT NULL
),
tag AS (
  SELECT work_id, tagger_values, tagger_version, updated_at AS tagged_at FROM (
    SELECT *, row_number() OVER (PARTITION BY work_id ORDER BY updated_at DESC) AS rn
    FROM {TAGGER} WHERE tagger_version IN ({sd.sql_versions()}))
  WHERE rn = 1
)
SELECT coalesce(t.work_id, p.work_id) AS work_id,
       CASE WHEN p.pubmed_values IS NOT NULL THEN p.pubmed_values ELSE t.tagger_values END AS study_designs,
       CASE WHEN p.pubmed_values IS NOT NULL THEN 'pubmed' ELSE 'tagger' END AS source,
       p.pubmed_values,
       t.tagger_values,
       t.tagger_version,
       t.tagged_at,
       current_timestamp() AS updated_at
FROM tag t FULL OUTER JOIN w_pm p ON t.work_id = p.work_id
WHERE size(CASE WHEN p.pubmed_values IS NOT NULL THEN p.pubmed_values ELSE t.tagger_values END) > 0
""")
print(f"{SERVED} rebuilt ({time.time() - t0:.0f}s)")

# COMMAND ----------

display(spark.sql(f"SELECT source, count(*) AS works FROM {SERVED} GROUP BY source"))
display(spark.sql(f"""
SELECT v AS value, source, count(*) AS works
FROM {SERVED} LATERAL VIEW explode(study_designs) AS v
GROUP BY v, source ORDER BY v, source
"""))

# COMMAND ----------

# PubMed vs tagger on works that have both (the disagreement measure Jason asked for, 2026-09-22).
display(spark.sql(f"""
SELECT v AS value,
       sum(CASE WHEN array_contains(pubmed_values, v) AND array_contains(tagger_values, v) THEN 1 ELSE 0 END) AS both,
       sum(CASE WHEN array_contains(pubmed_values, v) AND NOT array_contains(tagger_values, v) THEN 1 ELSE 0 END) AS pubmed_only,
       sum(CASE WHEN NOT array_contains(pubmed_values, v) AND array_contains(tagger_values, v) THEN 1 ELSE 0 END) AS tagger_only
FROM {SERVED} LATERAL VIEW explode(array({CANON})) AS v
WHERE pubmed_values IS NOT NULL AND tagger_values IS NOT NULL
GROUP BY v ORDER BY v
"""))
