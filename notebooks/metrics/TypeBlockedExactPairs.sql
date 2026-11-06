-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Type-blocked exact pairs (oxjob #1722)
-- MAGIC
-- MAGIC A nightly log of the records Map Work Ids kept off the title rule's work (`cmp` model_mints / other_work) although
-- MAGIC the two are an exact match and their types are a vetoed pair. Exact = folded title of 20+ characters equal, first-author
-- MAGIC surname equal, years within 1 (oxjob #2026 s6). The matcher is unchanged (Casey 2026-10-15: matching trusts the
-- MAGIC record's type; a wrong type is fixed at the source). Each row says what both sides send, so a dictionary gap reads
-- MAGIC straight off it: on the 11-05 live night, 1,725 of 2,528 rows were Southern Cross's Esploro feed ('Conference or
-- MAGIC Workshop Item') against its old feed's 'unpublished_paper', which had no dictionary row and fell to article.
-- MAGIC
-- MAGIC Types are as of the run, so a source fix shows as its host leaving the log, and a rerun of an earlier night after a
-- MAGIC fix shows which of its pairs now agree on type (minted copies stay on their own work until a periodic merge).
-- MAGIC
-- MAGIC Reads: location_work_match_decisions / _runs / _type_veto, locations_mapped, locations_w_types, openalex_works.
-- MAGIC Writes only `openalex.monitoring.type_blocked_exact_pairs`. Idempotent per night (delete-then-insert).

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE log_night DATE

-- COMMAND ----------

-- blank = the latest night the live matcher finished
SET VARIABLE log_night = COALESCE(TRY_CAST(NULLIF(TRIM(:night), '') AS DATE),
  (SELECT max(night) FROM openalex.works.location_work_match_runs WHERE mode = 'live' AND status = 'done'))

-- COMMAND ----------

-- tripwire: the night has a finished live run, and a scheduled run (blank night) is not reading a stale one
SELECT assert_true(EXISTS (SELECT 1 FROM openalex.works.location_work_match_runs WHERE night = log_night AND mode = 'live' AND status = 'done')
    AND (TRIM(:night) <> '' OR log_night >= current_date() - 1),
  'no finished live matcher run for the night') AS live_run_done

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS openalex.monitoring.type_blocked_exact_pairs (
  night DATE NOT NULL,                 -- the matcher's night (location_work_match_decisions.night)
  provenance STRING NOT NULL, native_id_namespace STRING NOT NULL, native_id STRING NOT NULL,   -- the incoming record
  host STRING,                         -- its OAI repository host, else its provenance
  record_raw_type STRING, record_type STRING,
  title_rule_work BIGINT NOT NULL,     -- the work the title rule picked (nightly_work)
  work_host STRING, work_raw_type STRING, work_type STRING,   -- the work's primary location, which sets the work's type
  cmp STRING,
  computed_at TIMESTAMP NOT NULL
) USING DELTA
CLUSTER BY (night)

-- COMMAND ----------

DELETE FROM openalex.monitoring.type_blocked_exact_pairs WHERE night = log_night

-- COMMAND ----------

INSERT INTO openalex.monitoring.type_blocked_exact_pairs
WITH d AS (
  SELECT night, provenance, native_id_namespace, native_id, nightly_work, cmp,
    CASE WHEN native_id_namespace = 'pmh' THEN regexp_extract(native_id, '^oai:([^:]+):', 1) ELSE prov END AS host
  FROM openalex.works.location_work_match_decisions
  WHERE night = log_night AND mode = 'live' AND nightly_decision = 'title_bind' AND cmp IN ('model_mints', 'other_work')
),
r AS (
  SELECT d.*, l.title AS rtitle, year(l.published_date) AS ryr, t.raw_type AS rraw, t.type AS rtype,
    lower(element_at(split(trim(regexp_replace(coalesce(nullif(get(l.authors, 0).family, ''),
      CASE WHEN get(l.authors, 0).name LIKE '%,%' THEN split_part(get(l.authors, 0).name, ',', 1) ELSE get(l.authors, 0).name END),
      '[^\\p{L} ]', '')), ' +'), -1)) AS rsur
  FROM d
  JOIN openalex.works.locations_mapped l
    ON l.provenance = d.provenance AND l.native_id_namespace = d.native_id_namespace AND l.native_id = d.native_id
  JOIN openalex.works.locations_w_types t
    ON t.provenance = d.provenance AND t.native_id_namespace = d.native_id_namespace AND t.native_id = d.native_id
  QUALIFY row_number() OVER (PARTITION BY d.provenance, d.native_id_namespace, d.native_id
                             ORDER BY l.openalex_updated_dt DESC, l.work_id DESC) = 1
)
SELECT r.night, r.provenance, r.native_id_namespace, r.native_id, r.host, r.rraw, r.rtype, r.nightly_work,
  coalesce(nullif(regexp_extract(w.primary_location.native_id, '^oai:([^:]+):', 1), ''), w.primary_location.provenance) AS work_host,
  w.primary_location.raw_type, w.type, r.cmp, current_timestamp()
FROM r
JOIN openalex.works.openalex_works w ON w.id = r.nightly_work
JOIN openalex.works.location_work_match_type_veto v ON v.type_a = r.rtype AND v.type_b = w.type
WHERE length(regexp_replace(lower(r.rtitle), '[^\\p{L}\\p{N}]', '')) >= 20
  AND regexp_replace(lower(r.rtitle), '[^\\p{L}\\p{N}]', '') = regexp_replace(lower(w.title), '[^\\p{L}\\p{N}]', '')
  AND abs(r.ryr - w.publication_year) <= 1 AND length(r.rsur) >= 2
  AND r.rsur = lower(element_at(split(trim(regexp_replace(get(w.authorships, 0).author.display_name, '[^\\p{L} ]', '')), ' +'), -1))
