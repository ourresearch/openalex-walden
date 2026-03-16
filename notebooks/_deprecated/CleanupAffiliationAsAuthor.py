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

# Version of crossref_works BEFORE the fix was applied (use DESCRIBE HISTORY to find this)
CROSSREF_WORKS_PRE_FIX_VERSION = 3180

# Institution keywords regex pattern (must match the logic in Crossref.py)
# Uses a hybrid approach to avoid false positives from surnames:
# - Long institution keywords: NO word boundaries (catches concatenated forms like "KazanUniversity")
# - Short corporate keywords: WITH word boundaries (avoids false positives like "Vincent" containing "Inc")
# - School/Center: Special handling to avoid surnames like Schooler, Centerwall

# Pattern for keywords EXCLUDING School and Center (used for family field)
INSTITUTION_KEYWORDS_PATTERN_NO_SCHOOL_CENTER = (
    r"(?i)("
    r"University|Institute|College|Hospital|Department|Centre|"
    r"Laboratory|Faculty|Academy|"
    r"Universiteit|Universidade|Università|Uniwersytet|Üniversitesi|Universite|"
    r"Hochschule|Fakultät|Klinikum|Krankenhaus|Politecnico|Politechnika|"
    r"Consortium|Association|Collaboration|Committee|Council|Organization|Organisation|"
    r"Clinic|Museum|Library|Foundation|Polytechnic"
    r")"
    r"|"
    r"\\b(Inc|LLC|Ltd|Corp|Corporation|Company|GmbH|Medical|Research)\\b"
)

# Full pattern including School and Center (used for given field and name-only pattern)
INSTITUTION_KEYWORDS_PATTERN = (
    r"(?i)("
    r"University|Institute|College|Hospital|Department|School|Center|Centre|"
    r"Laboratory|Faculty|Academy|"
    r"Universiteit|Universidade|Università|Uniwersytet|Üniversitesi|Universite|"
    r"Hochschule|Fakultät|Klinikum|Krankenhaus|Politecnico|Politechnika|"
    r"Consortium|Association|Collaboration|Committee|Council|Organization|Organisation|"
    r"Clinic|Museum|Library|Foundation|Polytechnic"
    r")"
    r"|"
    r"\\b(Inc|LLC|Ltd|Corp|Corporation|Company|GmbH|Medical|Research)\\b"
)

# Excluded publishers (must match the list in Crossref.py)
AFFILIATION_AS_AUTHOR_EXCLUDED_PUBLISHERS = [
    "ACM",
    "Acoustical Society of America (ASA)",
    "AIP Publishing",
    "American Chemical Society (ACS)",
    "American Geophysical Union (AGU)",
    "American Institute of Aeronautics & Astronautics",
    "American Society of Civil Engineers (ASCE)",
    "American Society of Mechanical Engineers",
    "Association for Computing Machinery (ACM)",
    "ASTM International",
    "Atlantis Press",
    "Bentham Science Publishers Ltd.",
    "BRILL",
    "CAIRN",
    "Cambridge University Press",
    "Center for Open Science",
    "CRC Press",
    "De Gruyter",
    "Duke University Press",
    "Edward Elgar Publishing",
    "Egyptian Knowledge Bank",
    "Egypts Presidential Specialized Council",
    "Elsevier",
    "Emerald",
    "ENCODE Data Coordination Center",
    "Georg Thieme Verlag KG",
    "H1 Connect",
    "Hans Publishers",
    "IEEE",
    "IGI Global",
    "Inderscience Publishers",
    "Informa UK Limited",
    "Institute of Electrical and Electronics Engineers (IEEE)",
    "Institution of Engineering and Technology (IET)",
    "International Union of Crystallography (IUCr)",
    "IUCN",
    "Japan Society of Mechanical Engineers",
    "Nomos Verlagsgesellschaft mbH & Co. KG",
    "OpenEdition",
    "Oxford University Press",
    "PERSEE Program",
    "Project MUSE",
    "Research Square Platform LLC",
    "Routledge",
    "Royal Society of Chemistry (RSC)",
    "SAE International",
    "Sciencedomain International",
    "Scientific Research Publishing, Inc.",
    "SPIE",
    "Springer Berlin Heidelberg",
    "Springer International Publishing",
    "Springer Nature Singapore",
    "Springer Nature Switzerland",
    "The Conversation",
    "The Electrochemical Society",
    "The Royal Society",
    "Trans Tech Publications, Ltd.",
    "transcript Verlag",
    "Universidade de Sao Paulo",
    "University of Chicago Press",
    "VS Verlag fur Sozialwissenschaften",
    "Walter de Gruyter GmbH",
    "World Scientific Pub Co Pte Lt",
]

# COMMAND ----------

# Step 1: Identify Affected Native IDs from pre-fix version of crossref_works
# Uses the exact same detection logic as the fix in Crossref.py
# Special handling for School and Center keywords in family field to avoid
# false positives from surnames like Schooler, Schooling, Centerwall

excluded_publishers_sql = ", ".join([f"'{p}'" for p in AFFILIATION_AS_AUTHOR_EXCLUDED_PUBLISHERS])

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW affected_native_ids AS
SELECT DISTINCT native_id
FROM openalex.crossref.crossref_works VERSION AS OF {CROSSREF_WORKS_PRE_FIX_VERSION}
WHERE publisher NOT IN ({excluded_publishers_sql})
  AND size(filter(authors, a ->
      -- Pattern 1a: Non-School/Center keywords in family
      a.family RLIKE '{INSTITUTION_KEYWORDS_PATTERN_NO_SCHOOL_CENTER}'
      -- Pattern 1b: School in family - only if exactly "School" or contains "Schoolof"
      OR a.family RLIKE '(?i)^School$'
      OR a.family RLIKE '(?i)Schoolof'
      -- Pattern 1c: Center in family - only if ends with "Center" or contains "Centerof"
      OR a.family RLIKE '(?i)Center$'
      OR a.family RLIKE '(?i)Centerof'
      -- Pattern 1d: Any institution keyword in given field (use full pattern)
      OR a.given RLIKE '{INSTITUTION_KEYWORDS_PATTERN}'
      -- Pattern 2: Institution in name field when given/family are empty
      OR (a.name RLIKE '{INSTITUTION_KEYWORDS_PATTERN}'
          AND COALESCE(a.given, '') = ''
          AND COALESCE(a.family, '') = '')
  )) > 0
""")

# COMMAND ----------

# Log count of affected native_ids
affected_native_count = spark.sql("SELECT COUNT(*) as affected_native_id_count FROM affected_native_ids")
display(affected_native_count)

# COMMAND ----------

# Step 2: Map native_ids to work_ids using locations_mapped

spark.sql("""
CREATE OR REPLACE TEMP VIEW affected_work_ids AS
SELECT DISTINCT lm.work_id
FROM affected_native_ids ani
JOIN openalex.works.locations_mapped lm
  ON ani.native_id = lm.native_id
  AND lm.provenance = 'crossref'
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