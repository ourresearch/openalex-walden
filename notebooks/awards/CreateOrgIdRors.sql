-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### French lab codes (RNSR) to ROR, from scanR's public mapping (oxjob #123.5, ZD 27257)
-- MAGIC
-- MAGIC Eric Jeangirard (French research ministry, scanR) keeps `rnsr_ror.json` current at a fixed URL: each RNSR code (the lab
-- MAGIC code ANR records on its grants) -> `{rors, type}`: `self` (the lab's own ROR), `institution` (a lab with none: its
-- MAGIC supervising institutions' RORs), `ambiguous`, `none`. Hand-checked 23 Nov: 30 of 30 random codes right.
-- MAGIC
-- MAGIC This notebook never reads that URL. It reads the dated copy `scripts/local/scanr_mappings_to_s3.py` pinned in S3 (the
-- MAGIC folder in the next cell; the script pins scanR's PIC file beside it, which nothing loads yet) and rewrites
-- MAGIC `openalex.awards.org_id_rors`, one row per (code, ROR), `self` and `institution` rows only.
-- MAGIC **Reader:** Nightly Grants (config key `org_id_rors`, `lib/org_id_rors.py`): an ANR grant's RNSR code adds the lab's RORs
-- MAGIC to its institutions, next to the name's match.
-- MAGIC
-- MAGIC **Refresh:** run the script (it validates and pins a new dated folder, never overwriting one), change the pinned folder
-- MAGIC below (both lines that carry it) in a reviewed commit, run this notebook; the next Nightly reads it.
-- MAGIC Undo: drop `org_id_rors` from `nightly_grants/config.prod.json` (the Nightly then reads none), then
-- MAGIC `DROP TABLE openalex.awards.org_id_rors`; or `RESTORE TABLE ... TO VERSION AS OF <previous>` to undo only a refresh.

-- COMMAND ----------

-- PINNED: the dated folder, written twice below (the file and the source column). Change both together, and only with a new
-- run of scripts/local/scanr_mappings_to_s3.py.
CREATE OR REPLACE TEMP VIEW org_id_rors_load AS
WITH rnsr AS (
  SELECT explode(from_json(value, 'map<string,struct<rors:array<string>,type:string>>')) AS (org_id, m)
  FROM read_files('s3://openalex-ingest/awards/mappings/scanr/2026-11-13/rnsr_ror.json', format => 'text', wholeText => true)
)
SELECT 'rnsr' id_type, org_id, concat('https://ror.org/', r) ror, m.type mapping_type,
  's3://openalex-ingest/awards/mappings/scanr/2026-11-13/' source
FROM rnsr LATERAL VIEW explode(m.rors) e AS r WHERE m.type IN ('self', 'institution');

-- COMMAND ----------

-- Stop before writing if the pinned file is not what the Nightly's checks (ORG_ID_RORS_*) accept, or looks truncated.
SELECT assert_true(count(*) = 0, 'ORG_ID_RORS_SHAPE') FROM org_id_rors_load
WHERE NOT coalesce(org_id RLIKE '^[0-9]{9}[A-Z]$' AND ror RLIKE '^https://ror.org/0[0-9a-z]{6}[0-9]{2}$', false);
SELECT assert_true(count(*) = 0, 'ORG_ID_RORS_UNIQUE') FROM (SELECT id_type, org_id, ror FROM org_id_rors_load GROUP BY ALL HAVING count(*) > 1);
-- 13 Nov file: 10,920 rows (2,256 self, 8,664 institution). A refresh that loses a fifth is a broken download, not an update.
SELECT assert_true(count(*) >= 8500, 'ORG_ID_RORS_TOO_FEW') FROM org_id_rors_load;

-- COMMAND ----------

CREATE OR REPLACE TABLE openalex.awards.org_id_rors
COMMENT 'scanR RNSR lab code -> ROR, from the pinned S3 copy; read by Nightly Grants (config org_id_rors). oxjob #123.5'
AS SELECT id_type, org_id, ror, mapping_type, source FROM org_id_rors_load;

SELECT id_type, mapping_type, count(*) rows, count(DISTINCT org_id) ids, count(DISTINCT ror) rors
FROM openalex.awards.org_id_rors GROUP BY ALL ORDER BY ALL;
