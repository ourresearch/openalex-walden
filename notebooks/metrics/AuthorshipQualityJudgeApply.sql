-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Authorship Quality Judge — apply (oxjob #640 Tier 2)
-- MAGIC
-- MAGIC Task 3 of Authorship Daily Metrics. Runs the `ai_query` judge over the
-- MAGIC prompt tables materialized (and cost-gated) by task 2
-- MAGIC (`AuthorshipQualityJudge`), then rolls verdicts into the tall metrics
-- MAGIC table. Runs on a SQL WAREHOUSE deliberately: cluster-side `ai_query`
-- MAGIC (DBR 16.4) injects a `temperature` parameter that the opus-4-8
-- MAGIC reasoning endpoint rejects with BAD_REQUEST (2026-07-25 night-1
-- MAGIC failure); the warehouse ai_query path does not, and is the path the
-- MAGIC 2026-07-25 backtest validated.
-- MAGIC
-- MAGIC Dates use current_date() (UTC): tasks 2 and 3 run in the same 22:30 UTC
-- MAGIC window. A cross-midnight manual rerun would mislabel the sample date —
-- MAGIC rerun the whole job instead.
-- MAGIC
-- MAGIC Re-runs on the same UTC date are idempotent: each arm deletes its rows
-- MAGIC for the date before inserting (a rerun re-judges and re-pays — 2x runs
-- MAGIC on 2026-07-26 duplicated every verdict before this guard existed).

-- COMMAND ----------

DELETE FROM openalex.authors.authorship_daily_quality_sample
WHERE sample_date = current_date() AND arm = 'armA'

-- COMMAND ----------

INSERT INTO openalex.authors.authorship_daily_quality_sample
  (sample_date, arm, work_id, author_sequence, match_tier, assigned_author_id,
   cand_author_ids, verdict, confidence, model, prompt_chars, judged_at)
SELECT current_date(), 'armA', work_id, author_sequence, tier, existing_author_id,
       NULL,
       get_json_object(out, '$.verdict'),
       get_json_object(out, '$.confidence'),
       'databricks-claude-opus-4-8', LENGTH(prompt), current_timestamp()
FROM (
  SELECT *, ai_query(
    'databricks-claude-opus-4-8', prompt,
    responseFormat => '{"type": "json_schema", "json_schema": {"name": "verdict", "schema": {"type": "object", "properties": {"verdict": {"type": "string", "enum": ["same_person", "different_person", "cannot_determine"]}, "confidence": {"type": "string", "enum": ["high", "medium", "low"]}}, "required": ["verdict", "confidence"]}, "strict": true}}'
  ) AS out
  FROM openalex.authors.judge_prompts_arm_a
)

-- COMMAND ----------

DELETE FROM openalex.authors.authorship_daily_quality_sample
WHERE sample_date = current_date() AND arm = 'armB'

-- COMMAND ----------

INSERT INTO openalex.authors.authorship_daily_quality_sample
  (sample_date, arm, work_id, author_sequence, match_tier, assigned_author_id,
   cand_author_ids, verdict, confidence, model, prompt_chars, judged_at)
SELECT current_date(), 'armB', work_id, author_sequence, NULL, NULL,
       cand_author_ids,
       get_json_object(out, '$.verdict'),
       get_json_object(out, '$.confidence'),
       'databricks-claude-opus-4-8', LENGTH(prompt), current_timestamp()
FROM (
  SELECT *, ai_query(
    'databricks-claude-opus-4-8', prompt,
    responseFormat => '{"type": "json_schema", "json_schema": {"name": "verdict", "schema": {"type": "object", "properties": {"verdict": {"type": "string", "enum": ["candidate_1", "candidate_2", "candidate_3", "candidate_4", "candidate_5", "none_correct", "cannot_determine"]}, "confidence": {"type": "string", "enum": ["high", "medium", "low"]}}, "required": ["verdict", "confidence"]}, "strict": true}}'
  ) AS out
  FROM openalex.authors.judge_prompts_arm_b
)

-- COMMAND ----------

DELETE FROM openalex.authors.authorship_daily_quality_sample
WHERE sample_date = current_date() AND arm = 'orcid_collision'

-- COMMAND ----------

INSERT INTO openalex.authors.authorship_daily_quality_sample
  (sample_date, arm, work_id, author_sequence, match_tier, assigned_author_id,
   cand_author_ids, verdict, confidence, model, prompt_chars, judged_at)
SELECT current_date(), 'orcid_collision', NULL, NULL, NULL, minted.id,
       CAST(older.id AS STRING), 'collision', 'high', NULL, NULL, current_timestamp()
FROM openalex.authors.authors minted
JOIN openalex.authors.authors older
  ON minted.orcid = older.orcid AND older.id < minted.id
WHERE DATE(minted.created_date) = current_date()
  AND minted.orcid IS NOT NULL

-- COMMAND ----------

DELETE FROM openalex.authors.authorship_daily_metrics
WHERE snapshot_date = current_date()
  AND metric IN ('judge_arm_a', 'judge_arm_b', 'orcid_mint_collisions', 'judge_cost_cents')

-- COMMAND ----------

INSERT INTO openalex.authors.authorship_daily_metrics
  (snapshot_date, snapshot_version, metric, dimension, value, computed_at)
SELECT current_date(), NULL, 'judge_arm_a', CONCAT(match_tier, '|', verdict), COUNT(*), current_timestamp()
FROM openalex.authors.authorship_daily_quality_sample
WHERE sample_date = current_date() AND arm = 'armA'
GROUP BY match_tier, verdict

-- COMMAND ----------

INSERT INTO openalex.authors.authorship_daily_metrics
  (snapshot_date, snapshot_version, metric, dimension, value, computed_at)
SELECT current_date(), NULL, 'judge_arm_b',
       CASE WHEN verdict LIKE 'candidate%' THEN 'candidate_pick' ELSE verdict END,
       COUNT(*), current_timestamp()
FROM openalex.authors.authorship_daily_quality_sample
WHERE sample_date = current_date() AND arm = 'armB'
GROUP BY CASE WHEN verdict LIKE 'candidate%' THEN 'candidate_pick' ELSE verdict END

-- COMMAND ----------

INSERT INTO openalex.authors.authorship_daily_metrics
  (snapshot_date, snapshot_version, metric, dimension, value, computed_at)
SELECT current_date(), NULL, 'orcid_mint_collisions', NULL,
       COUNT(DISTINCT assigned_author_id), current_timestamp()
FROM openalex.authors.authorship_daily_quality_sample
WHERE sample_date = current_date() AND arm = 'orcid_collision'

-- COMMAND ----------

INSERT INTO openalex.authors.authorship_daily_metrics
  (snapshot_date, snapshot_version, metric, dimension, value, computed_at)
SELECT current_date(), NULL, 'judge_cost_cents', NULL,
       CAST((
         ((SELECT COALESCE(SUM(LENGTH(prompt)), 0) FROM openalex.authors.judge_prompts_arm_a)
          + (SELECT COALESCE(SUM(LENGTH(prompt)), 0) FROM openalex.authors.judge_prompts_arm_b)) / 4.0 * 15.0
         + ((SELECT COUNT(*) FROM openalex.authors.judge_prompts_arm_a)
            + (SELECT COUNT(*) FROM openalex.authors.judge_prompts_arm_b)) * 20.0 * 75.0
       ) / 1e6 * 100 AS BIGINT),
       current_timestamp()
