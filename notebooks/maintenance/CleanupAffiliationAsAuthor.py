# Databricks notebook source
# Cleanup Affiliation-as-Author Entries
#
# This notebook cleans up existing work_authors records that contain affiliation-as-author entries
# (institution names incorrectly appearing as author entries).
#
# Prerequisites:
# - The fix in notebooks/ingest/Crossref.py must be deployed first
# - The Crossref DLT pipeline should have processed new/updated records with the fix
#
# Related Issue: qa/issues/open/crossref-author-affiliation-interleaving/

# COMMAND ----------

# Institution keywords regex pattern (must match the one in Crossref.py)
INSTITUTION_KEYWORDS_PATTERN = (
    r"(?i)\\b("
    r"University|Institute|College|Hospital|Department|School|Center|Centre|"
    r"Laboratory|Faculty|Academy|"
    r"Universiteit|Universidade|Università|Uniwersytet|Üniversitesi|Universite|"
    r"Hochschule|Fakultät|Klinikum|Krankenhaus|Politecnico|Politechnika|"
    r"Inc|LLC|Ltd|Corp|Corporation|Company|GmbH|Consortium|Association|"
    r"Collaboration|Committee|Council|Organization|Organisation|"
    r"Clinic|Medical|Research|Museum|Library|Foundation|Polytechnic"
    r")\\b"
)

# COMMAND ----------

# Step 1: Identify Affected Work IDs

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW affected_work_ids AS
SELECT DISTINCT w.id as work_id
FROM openalex.works.openalex_works_base w
WHERE w.provenance = 'crossref'
  AND EXISTS(
    SELECT 1 FROM explode(w.authors) a
    WHERE a.family RLIKE '{INSTITUTION_KEYWORDS_PATTERN}'
      OR a.given RLIKE '{INSTITUTION_KEYWORDS_PATTERN}'
      OR (a.name RLIKE '{INSTITUTION_KEYWORDS_PATTERN}'
          AND COALESCE(a.given, '') = ''
          AND COALESCE(a.family, '') = '')
  )
""")

# COMMAND ----------

# Log count of affected works
affected_count = spark.sql("SELECT COUNT(*) as affected_work_count FROM affected_work_ids")
display(affected_count)

# COMMAND ----------

# Step 2: Preview records that will be deleted

records_to_delete = spark.sql("""
SELECT COUNT(*) as records_to_delete
FROM openalex.works.work_authors
WHERE work_id IN (SELECT work_id FROM affected_work_ids)
""")
display(records_to_delete)

# COMMAND ----------

# Step 2b: Delete existing records for affected works
# UNCOMMENT THE LINE BELOW TO EXECUTE THE DELETE

# spark.sql("DELETE FROM openalex.works.work_authors WHERE work_id IN (SELECT work_id FROM affected_work_ids)")

# COMMAND ----------

# Verify deletion: should return 0 after delete is executed

remaining = spark.sql("""
SELECT COUNT(*) as remaining_records
FROM openalex.works.work_authors
WHERE work_id IN (SELECT work_id FROM affected_work_ids)
""")
display(remaining)

# COMMAND ----------

# Step 3: Trigger Reprocessing
# Choose ONE of the following options:
# - Option A: Touch updated_date on affected works (recommended for targeted reprocessing)
# - Option B: Reset max_updated_date variable (for full reprocessing)

# COMMAND ----------

# Option A: Touch updated_date on affected works
# UNCOMMENT THE LINE BELOW TO EXECUTE

# spark.sql("UPDATE openalex.works.openalex_works_base SET updated_date = current_date() WHERE id IN (SELECT work_id FROM affected_work_ids)")

# COMMAND ----------

# Option B: Get min created_date for reference (if using max_updated_date reset approach)

min_date = spark.sql("""
SELECT MIN(created_date) as min_created_date
FROM openalex.works.openalex_works_base
WHERE id IN (SELECT work_id FROM affected_work_ids)
""")
display(min_date)

# COMMAND ----------

# Step 4: Re-run UpdateWorkAuthors
# After triggering reprocessing (Step 3), run the existing notebook:
# notebooks/end2end/UpdateWorkAuthors.ipynb

# COMMAND ----------

# Step 5: Refresh Materialized Views
# UNCOMMENT THE LINE BELOW TO EXECUTE

# spark.sql("REFRESH MATERIALIZED VIEW openalex.works.work_author_affiliations_mv")

# COMMAND ----------

# Step 6: Verification - Check crossref_works (source table)

verification_crossref = spark.sql("""
SELECT publisher, COUNT(*) as remaining_affiliation_as_author
FROM openalex.crossref.crossref_works
WHERE publisher NOT IN ('IEEE', 'Elsevier', 'Springer Berlin Heidelberg', 'Informa UK Limited')
  AND EXISTS(SELECT 1 FROM explode(authors) a
             WHERE a.family RLIKE '(?i)\\\\b(University|Institute|College|Hospital)\\\\b'
                OR a.given RLIKE '(?i)\\\\b(University|Institute|College|Hospital)\\\\b'
                OR (a.name RLIKE '(?i)\\\\b(University|Institute|College|Hospital)\\\\b'
                    AND COALESCE(a.given, '') = ''
                    AND COALESCE(a.family, '') = ''))
GROUP BY publisher
ORDER BY remaining_affiliation_as_author DESC
LIMIT 20
""")
display(verification_crossref)

# COMMAND ----------

# Verify fix propagated to openalex_works for known affected DOIs

verification_dois = spark.sql("""
SELECT native_id, authors
FROM openalex.works.openalex_works
WHERE native_id IN ('10.26907/esd.17.3.14', '10.21002/jaki.2024.11')
""")
display(verification_dois)

# COMMAND ----------

# Overall count of remaining affiliation-as-author entries in openalex_works

verification_total = spark.sql("""
SELECT COUNT(*) as remaining_affiliation_as_author
FROM openalex.works.openalex_works
WHERE provenance = 'crossref'
  AND EXISTS(SELECT 1 FROM explode(authors) a
             WHERE a.family RLIKE '(?i)\\\\b(University|Institute|College|Hospital)\\\\b'
                OR a.given RLIKE '(?i)\\\\b(University|Institute|College|Hospital)\\\\b'
                OR (a.name RLIKE '(?i)\\\\b(University|Institute|College|Hospital)\\\\b'
                    AND COALESCE(a.given, '') = ''
                    AND COALESCE(a.family, '') = ''))
""")
display(verification_total)

# COMMAND ----------

# Step 7: Continue Pipeline
# After verification, run the rest of the end2end pipeline normally:
# - UpdateWorkAuthorships
# - Any other downstream notebooks