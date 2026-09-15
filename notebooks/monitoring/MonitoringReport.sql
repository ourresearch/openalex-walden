-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Monitoring — daily report (oxjob #1116)
-- MAGIC
-- MAGIC Runs on a SQL WAREHOUSE (cluster-side `ai_query` on DBR 16.4 injects a `temperature`
-- MAGIC parameter the opus endpoint rejects — same reason as `AuthorshipQualityJudgeApply`).
-- MAGIC
-- MAGIC Input: today's findings across every component (status, value, baseline, deviation,
-- MAGIC the check's failure modes) plus the last 7 reports so the narrative has continuity.
-- MAGIC Output: one row in `openalex.monitoring.reports` with three sections — anomalies,
-- MAGIC failures, opportunities — and a one-line headline. Status is never decided here; the
-- MAGIC engine already did that. This step only explains.
-- MAGIC
-- MAGIC Idempotent per (report_date): delete-then-insert. Cost: one prompt of a few KB — cents.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS openalex.monitoring.reports (
  report_date   DATE NOT NULL,
  headline      STRING,
  anomalies     STRING,
  failures      STRING,
  opportunities STRING,
  all_clear     BOOLEAN,
  n_critical    INT,
  n_watch       INT,
  n_checks      INT,
  findings_json STRING,   -- what the model saw (non-ok rows), for audit
  model         STRING,
  prompt_chars  INT,
  created_at    TIMESTAMP NOT NULL
) USING DELTA

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW monitoring_today AS
SELECT f.*, c.failure_modes
FROM openalex.monitoring.findings f
LEFT JOIN openalex.monitoring.checks c
  ON c.component = f.component AND c.check_id = f.check_id
WHERE f.snapshot_date = current_date()

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW monitoring_prompt AS
WITH counts AS (
  SELECT COUNT(*) AS n_checks,
         SUM(CASE WHEN status = 'critical' THEN 1 ELSE 0 END) AS n_critical,
         SUM(CASE WHEN status = 'watch' THEN 1 ELSE 0 END) AS n_watch
  FROM monitoring_today
),
flagged AS (
  SELECT to_json(collect_list(named_struct(
           'component', component, 'section', section, 'check', check_id, 'title', title,
           'dimension', dimension, 'status', status, 'value', ROUND(value, 4),
           'baseline', ROUND(baseline, 4), 'deviation_mad', ROUND(deviation, 1), 'detail', detail,
           'page', page, 'known_failure_modes', failure_modes))) AS j
  FROM monitoring_today WHERE status IN ('critical', 'watch')
),
quiet AS (
  SELECT to_json(collect_list(named_struct('component', component, 'check', check_id,
           'dimension', dimension, 'status', status))) AS j
  FROM monitoring_today WHERE status IN ('no_data', 'insufficient_n')
),
history AS (
  SELECT concat_ws('\n', collect_list(concat(report_date, ': ', headline, '\n  anomalies: ', anomalies,
           '\n  failures: ', failures, '\n  opportunities: ', opportunities))) AS h
  FROM (SELECT * FROM openalex.monitoring.reports
        WHERE report_date >= current_date() - INTERVAL 7 DAYS ORDER BY report_date)
),
sections AS (
  SELECT concat_ws('\n', collect_list(concat('- [', component, '] ', section))) AS s
  FROM (SELECT DISTINCT component, section FROM monitoring_today)
)
SELECT
  counts.n_checks, counts.n_critical, counts.n_watch,
  COALESCE(flagged.j, '[]') AS findings_json,
  concat(
    'You are the OpenAlex Walden daily monitor. Every night, deterministic checks ',
    'score each component of the system against a written checklist. Your job is to READ the ',
    'results and write the morning report. You do not decide status; the checks did. You explain, ',
    'connect today to the recent history, and point at what a human should look at first.\n\n',
    'Write four fields as JSON: headline (one line, <= 120 chars), anomalies (things that deviated ',
    'from baseline and what they most likely mean, grouped, most severe first; name component, check, ',
    'value vs baseline), failures (things that did not run or did not produce data), opportunities ',
    '(improvements worth a job: a check to add, a threshold to tune, a root cause worth fixing — ',
    'only if the evidence supports it). Use plain prose, short paragraphs, no bullet spam. ',
    'If nothing is flagged, headline says all clear with the counts and the other fields say so in one line. ',
    'Never invent numbers not in the input. When a finding matches a known failure mode, say so.\n\n',
    'CHECKLIST SECTIONS IN SCOPE TODAY:\n', COALESCE(sections.s, '(none)'), '\n\n',
    'COUNTS: ', counts.n_checks, ' checks, ', counts.n_critical, ' critical, ', counts.n_watch, ' watch.\n\n',
    'FLAGGED FINDINGS (critical + watch):\n', COALESCE(flagged.j, '[]'), '\n\n',
    'CHECKS WITH NO DATA TODAY:\n', COALESCE(quiet.j, '[]'), '\n\n',
    'YOUR LAST 7 REPORTS:\n', COALESCE(history.h, '(none yet)')
  ) AS prompt
FROM counts, flagged, quiet, history, sections

-- COMMAND ----------

DELETE FROM openalex.monitoring.reports WHERE report_date = current_date()

-- COMMAND ----------

INSERT INTO openalex.monitoring.reports
  (report_date, headline, anomalies, failures, opportunities, all_clear, n_critical, n_watch, n_checks,
   findings_json, model, prompt_chars, created_at)
SELECT current_date(),
       get_json_object(out, '$.headline'),
       get_json_object(out, '$.anomalies'),
       get_json_object(out, '$.failures'),
       get_json_object(out, '$.opportunities'),
       n_critical = 0 AND n_watch = 0,
       n_critical, n_watch, n_checks, findings_json,
       'databricks-claude-opus-4-8', LENGTH(prompt), current_timestamp()
FROM (
  SELECT p.*, ai_query(
    'databricks-claude-opus-4-8', prompt,
    responseFormat => '{"type": "json_schema", "json_schema": {"name": "report", "schema": {"type": "object", "properties": {"headline": {"type": "string"}, "anomalies": {"type": "string"}, "failures": {"type": "string"}, "opportunities": {"type": "string"}}, "required": ["headline", "anomalies", "failures", "opportunities"]}, "strict": true}}'
  ) AS out
  FROM monitoring_prompt p
)

-- COMMAND ----------

SELECT report_date, headline, n_critical, n_watch, n_checks, prompt_chars
FROM openalex.monitoring.reports WHERE report_date = current_date()
