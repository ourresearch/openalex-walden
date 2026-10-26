-- Match_Locations_To_Works (oxjob #1668) 6/6: one decision per seed record for night __NIGHT__ (mode __MODE__), next to what
-- the nightly did. The model (strategy 2026-10-15): no candidate or XGBoost top < 0.01 -> mint; else bind Qwen's top when
-- p >= 0.887, else mint. Without Qwen (__QWEN_OK__ = false): bind XGBoost's top at >= 0.9156, else mint. A model bind only ever
-- names a work that existed before the run. cmp: same_bind / both_mint / other_work (both bind, different works) / model_mints
-- (the nightly binds, the model mints) / model_binds (the nightly mints or parks, the model binds). rule_pick = the model's work
-- is the title tier's own pick; emb_only = the model's work came only from the embedding neighbours.
CREATE TABLE IF NOT EXISTS openalex.works.location_work_match_decisions (
  night DATE, mode STRING, provenance STRING, native_id_namespace STRING, native_id STRING, seed_id BIGINT, prov STRING,
  nightly_decision STRING, nightly_work BIGINT, model_work BIGINT, layer STRING, xgb_work BIGINT, xgb_p DOUBLE,
  qwen_work BIGINT, qwen_p DOUBLE, cmp STRING, rule_pick INT, emb_only INT, model_version INT, adapter STRING,
  decided_at TIMESTAMP)
CLUSTER BY (night)
COMMENT 'Match Locations To Works (oxjob #1668): one row per record that reached the model, per night and mode; mode shadow = never applied'
;
DELETE FROM openalex.works.location_work_match_decisions WHERE night = DATE'__NIGHT__' AND mode = '__MODE__'
;
INSERT INTO openalex.works.location_work_match_decisions
WITH x AS (
  SELECT seed_id, max_by(cid, p_raw) AS x_cid, max(p_raw) AS x_p, max(model_version) AS model_version
  FROM openalex.works.location_work_match_xgb GROUP BY seed_id
),
q AS (
  SELECT seed_id, max_by(cid, p) AS q_cid, max(p) AS q_p, max(adapter) AS adapter
  FROM openalex.works.location_work_match_qwen GROUP BY seed_id
),
d AS (
  SELECT s.seed_id, s.provenance, s.native_id_namespace, s.native_id, s.prov, s.decision, s.work_id AS nightly_work,
    CASE WHEN s.decision = 'title_bind' THEN s.work_id END AS nightly_bind,
    x.x_cid, x.x_p, q.q_cid, q.q_p, x.model_version, q.adapter,
    CASE WHEN x.x_p IS NULL THEN 'no_candidates'
         WHEN x.x_p < 0.01 THEN 'xgb_mint'
         WHEN __QWEN_OK__ AND q.q_p IS NULL THEN 'qwen_missing'
         WHEN __QWEN_OK__ AND q.q_p >= 0.887 THEN 'qwen_bind'
         WHEN __QWEN_OK__ THEN 'qwen_mint'
         WHEN x.x_p >= 0.9156 THEN 'xgb_fallback_bind'
         ELSE 'xgb_fallback_mint' END AS layer
  FROM openalex.works.location_work_match_seeds s
  LEFT JOIN x USING (seed_id) LEFT JOIN q USING (seed_id)
),
m AS (
  SELECT d.*, CASE layer WHEN 'qwen_bind' THEN q_cid WHEN 'xgb_fallback_bind' THEN x_cid END AS model_work FROM d
)
SELECT DATE'__NIGHT__', '__MODE__', m.provenance, m.native_id_namespace, m.native_id, m.seed_id, m.prov, m.decision, m.nightly_work,
  m.model_work, m.layer, m.x_cid, m.x_p, m.q_cid, m.q_p,
  CASE WHEN m.nightly_bind IS NOT NULL AND m.model_work = m.nightly_bind THEN 'same_bind'
       WHEN m.nightly_bind IS NULL AND m.model_work IS NULL THEN 'both_mint'
       WHEN m.nightly_bind IS NOT NULL AND m.model_work IS NOT NULL THEN 'other_work'
       WHEN m.nightly_bind IS NOT NULL THEN 'model_mints'
       ELSE 'model_binds' END,
  CAST(xb.is_bound AS INT),
  CAST(e.channels IS NOT NULL AND array_contains(e.channels, 'emb') AND size(e.channels) = 1 AS INT),
  m.model_version, m.adapter, current_timestamp()
FROM m
LEFT JOIN openalex.works.location_work_match_xgb xb ON xb.seed_id = m.seed_id AND xb.cid = m.model_work
LEFT JOIN openalex.works.location_work_match_fp e ON e.seed_id = m.seed_id AND e.cid = m.model_work
;
SELECT cmp, layer, count(*) AS n FROM openalex.works.location_work_match_decisions
WHERE night = DATE'__NIGHT__' AND mode = '__MODE__' GROUP BY 1, 2 ORDER BY 1, 2
