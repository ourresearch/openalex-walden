-- The score load (worktype-student-v2, 2026-11-09, topics-shape re-cut): the Modal shards of one build (temp view
-- wt_shards, read by ScoreWorkTypeQueue from <volume_dir>/<build_id>/*.parquet) against the queue, then each text's scores
-- inserted once into the scores table. Statement 1, the checks (the notebook refuses, writing nothing, unless every count
-- but rows_ and ids is 0 and rows_ = ids = the queue's size):
--   missing / extra   a queued work with no shard row / a shard row for a work not queued
--   bad_hash          a shard row whose input_hash isn't the queued text's (the Modal app re-hashes every text)
--   bad_model         not the policy's model content id, or not its render version
--   bad_class         class_idx outside the model's classes; bad_logits: not one logit per class
--   bad_conf          confidence missing, outside [0, 1], or not a 4-dp value
SELECT
  (SELECT count(*) FROM wt_shards) AS rows_,
  (SELECT count(DISTINCT work_id) FROM wt_shards) AS ids,
  (SELECT count(*) FROM ${queue} q LEFT ANTI JOIN wt_shards r ON r.work_id = q.work_id) AS missing,
  (SELECT count(*) FROM wt_shards r LEFT ANTI JOIN ${queue} q ON q.work_id = r.work_id) AS extra,
  (SELECT count(*) FROM wt_shards r JOIN ${queue} q ON q.work_id = r.work_id WHERE r.input_hash IS DISTINCT FROM q.input_hash) AS bad_hash,
  (SELECT count(*) FROM wt_shards WHERE model_version IS DISTINCT FROM '${model_id}' OR render_version IS DISTINCT FROM '${render_version}') AS bad_model,
  (SELECT count(*) FROM wt_shards WHERE class_idx IS NULL OR class_idx < 0 OR class_idx >= ${n_classes}) AS bad_class,
  (SELECT count(*) FROM wt_shards WHERE logits IS NULL OR size(logits) <> ${n_classes}) AS bad_logits,
  (SELECT count(*) FROM wt_shards WHERE confidence IS NULL OR confidence < 0 OR confidence > 1
                                     OR CAST(confidence AS DECIMAL(5,4)) <> confidence) AS bad_conf;

-- Statement 2: insert once. A text already in the table (a repair after a load, or the same text queued twice) keeps its
-- scores; only its bookkeeping moves (it is the work's current text again).
MERGE INTO ${S}.work_type_scores AS s
USING (
  SELECT r.work_id, r.input_hash, r.model_version, r.render_version, r.class_idx,
         CAST(r.confidence AS DECIMAL(5,4)) AS confidence, r.logits, q.input_key, q.s0_doi, q.s0_names, q.s0_urls, q.build_id
  FROM wt_shards r JOIN ${queue} q ON q.work_id = r.work_id
) AS n
ON s.work_id = n.work_id AND s.input_hash = n.input_hash AND s.model_version = n.model_version
WHEN MATCHED THEN UPDATE SET s.input_key = n.input_key, s.seen_at = current_timestamp(),
  s.s0_doi = n.s0_doi, s.s0_names = n.s0_names, s.s0_urls = n.s0_urls
WHEN NOT MATCHED THEN INSERT (work_id, input_hash, model_version, render_version, class_idx, confidence, logits, input_key,
                              s0_doi, s0_names, s0_urls, scored_at, seen_at, build_id)
  VALUES (n.work_id, n.input_hash, n.model_version, n.render_version, n.class_idx, n.confidence, n.logits, n.input_key,
          n.s0_doi, n.s0_names, n.s0_urls, current_timestamp(), current_timestamp(), n.build_id);
