-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Monitoring — daily report (oxjob #1116)
-- MAGIC
-- MAGIC Runs on a SQL WAREHOUSE (cluster-side `ai_query` on DBR 16.4 injects a `temperature`
-- MAGIC parameter the opus endpoint rejects — same reason as `AuthorshipQualityJudgeApply`).
-- MAGIC
-- MAGIC Input: the night's findings across every component (status, value, baseline, deviation,
-- MAGIC the check's failure modes) plus the last 7 reports so the narrative has continuity.
-- MAGIC Output: one row in `openalex.monitoring.reports` with a one-line headline and three SHORT
-- MAGIC fields — anomalies, failures, opportunities. Status is never decided here; the engine
-- MAGIC already did that. This step only explains, briefly: the daily email already lists every
-- MAGIC flagged check, so the model's job is the two or three sentences a reader needs on top
-- MAGIC (Casey, 2026-09-17: "it needs to be more concise").
-- MAGIC
-- MAGIC The night is the job's `snapshot_date` parameter (blank = today UTC), same as the findings
-- MAGIC task, so a backfill or rerun narrates the right night.
-- MAGIC Idempotent per (report_date): delete-then-insert. Cost: one prompt of a few KB — cents.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE night_date DATE DEFAULT current_date()

-- COMMAND ----------

SET VARIABLE night_date = COALESCE(TRY_CAST(NULLIF(TRIM(:snapshot_date), '') AS DATE), current_date())

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
WHERE f.snapshot_date = night_date

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
  FROM (SELECT * FROM openalex.monitoring.reports r
        WHERE r.report_date >= night_date - INTERVAL 7 DAYS AND r.report_date < night_date ORDER BY r.report_date)
),
sections AS (
  SELECT concat_ws('\n', collect_list(concat('- [', component, '] ', section))) AS s
  FROM (SELECT DISTINCT component, section FROM monitoring_today)
)
SELECT
  counts.n_checks, counts.n_critical, counts.n_watch,
  COALESCE(flagged.j, '[]') AS findings_json,
  concat(
    'You are the OpenAlex Walden nightly monitor. Deterministic checks have already scored each ',
    'component against a written checklist; you do not decide status. The reader gets the full list ',
    'of flagged checks next to your text, so do NOT restate it. Write the few sentences they need on top: ',
    'what matters most tonight and why, in the language of the known failure modes.\n\n',
    'Write four fields as JSON, and keep every one SHORT:\n',
    '- headline: one line, at most 90 characters, the single most important thing tonight.\n',
    '- anomalies: at most 60 words. The one or two findings a human should look at first, each with its ',
    'value against baseline or line, and the matching known failure mode if there is one. Group findings ',
    'that are plainly one event. No diagnosis beyond the known failure modes; say "cause unknown" if so.\n',
    '- failures: at most 20 words on checks that did not run or had no data, or exactly "none".\n',
    '- opportunities: at most 25 words, one concrete suggestion only if tonight''s evidence clearly supports ',
    'it, or exactly "none". Never propose engineering changes at length.\n',
    'Plain prose, no bullets, no headings, no preamble. If nothing is flagged, headline says all clear ',
    'with the counts and the other fields are "none". Never invent numbers not in the input. ',
    'Several components may appear; name the component when more than one is flagged.\n\n',
    'CHECKLIST SECTIONS IN SCOPE TODAY:\n', COALESCE(sections.s, '(none)'), '\n\n',
    'COUNTS: ', counts.n_checks, ' checks, ', counts.n_critical, ' critical, ', counts.n_watch, ' watch.\n\n',
    'FLAGGED FINDINGS (critical + watch):\n', COALESCE(flagged.j, '[]'), '\n\n',
    'CHECKS WITH NO DATA TODAY:\n', COALESCE(quiet.j, '[]'), '\n\n',
    'YOUR LAST 7 REPORTS:\n', COALESCE(history.h, '(none yet)')
  ) AS prompt
FROM counts, flagged, quiet, history, sections

-- COMMAND ----------

DELETE FROM openalex.monitoring.reports WHERE report_date = night_date

-- COMMAND ----------

INSERT INTO openalex.monitoring.reports
  (report_date, headline, anomalies, failures, opportunities, all_clear, n_critical, n_watch, n_checks,
   findings_json, model, prompt_chars, created_at)
SELECT night_date,
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
FROM openalex.monitoring.reports WHERE report_date = night_date
