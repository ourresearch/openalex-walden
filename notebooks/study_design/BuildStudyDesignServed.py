# Databricks notebook source
# MAGIC %md
# MAGIC # Build the served study-design table (oxjob #1312)
# MAGIC
# MAGIC `works_study_design`: one row per work with at least one study-design value.
# MAGIC
# MAGIC Provenance rule (Jason, 2026-09-26; replaces "PubMed wins" of 2026-09-22):
# MAGIC the tagger's values are served wherever the tagger ran (works with an
# MAGIC abstract); PubMed's MEDLINE V03 tags only where it did not. A judged sample
# MAGIC showed PubMed right 279/280 where the two agree but only 21-43% where they
# MAGIC disagree (oxjob #1312 EXPLORE § 11). Both are stored so disagreement on the
# MAGIC live corpus stays measurable. Only PubMed's vocabulary is served: the tagger's
# MAGIC other-primary-research is kept in `tagger_values` but never in
# MAGIC `study_designs` (Jason, 2026-09-25; oxjob #1362). Served values apply the stricter served
# MAGIC thresholds (`sd.SERVED_THRESHOLDS`, 2026-09-28) to the stored scores; `tagger_values` stays as tagged. Parents are implied (RCT ⇒ Clinical Trial;
# MAGIC Meta-Analysis ⇒ Systematic Review). Publication formats (Editorial, Letter,
# MAGIC Review, Guideline …) are `type`'s business and never appear here.
# MAGIC
# MAGIC Full rebuild every run (CREATE OR REPLACE): ~41M PubMed rows + the tagger
# MAGIC table. CreateWorksEnriched merges it into `openalex_works.study_designs`.

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

CANON = ", ".join(f"'{sd.VALUE_ID[c]}'" for c in sd.SERVED_CLASSES)   # served values, canonical order
RCT, CT, MA, SR = (sd.VALUE_ID[c] for c in ("rct", "clinical_trial", "meta_analysis", "systematic_review"))

# COMMAND ----------

t0 = time.time()
spark.sql(f"""
CREATE OR REPLACE TABLE {SERVED}
USING DELTA CLUSTER BY (work_id)
COMMENT 'Study design per work (oxjob #1312): automated tagging wherever it ran (works with an abstract), else PubMed (MEDLINE V03 tags)'
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
  -- tagger_values keeps the tagger's full output (incl. other-primary-research, for oxjob #1362); study_designs
  -- serves PubMed's vocabulary only (served_values)
  -- served_values applies the stricter served thresholds to the stored scores (sd.SERVED_THRESHOLDS, oxjob #1312 step 12)
  SELECT work_id, tagger_values, {sd.sql_served_values()} AS served_values,
         tagger_version, updated_at AS tagged_at FROM (
    SELECT *, row_number() OVER (PARTITION BY work_id ORDER BY updated_at DESC) AS rn
    FROM {TAGGER} WHERE tagger_version IN ({sd.sql_versions()}))
  WHERE rn = 1
)
SELECT coalesce(t.work_id, p.work_id) AS work_id,
       CASE WHEN t.tagger_values IS NOT NULL THEN t.served_values ELSE p.pubmed_values END AS study_designs,
       CASE WHEN t.tagger_values IS NOT NULL THEN 'tagger' ELSE 'pubmed' END AS source,
       p.pubmed_values,
       t.tagger_values,
       t.tagger_version,
       t.tagged_at,
       current_timestamp() AS updated_at
FROM tag t FULL OUTER JOIN w_pm p ON t.work_id = p.work_id
WHERE size(CASE WHEN t.tagger_values IS NOT NULL THEN t.served_values ELSE p.pubmed_values END) > 0
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
