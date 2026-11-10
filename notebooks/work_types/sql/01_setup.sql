-- sql-template: rendered by lib/sql_templates.py, which fills every parameter before Spark runs it.
-- The one persistent table of the work-type path (topics-shape re-cut, worktype-student-v2, 2026-11-09): stage 2's scores,
-- the paid GPU output. One row per scored text: key (work_id, input_hash, model_version), written once by the score task
-- (ScoreWorkTypeQueue, from the Modal shards in the Volume); scores are never updated. The queue builder only moves the
-- bookkeeping of a row whose text comes back for its work (input_key, seen_at, the stage-0 signals), so the latest-seen
-- row of a work is its current one. Read by the queue builder (what changed) and the End 2 End cell (the decision).
-- Run once (setup), with ${S} = openalex.works, ${queue} = openalex.transient.work_type_queue, ${volume} = openalex.works.work_types.
-- Undo: DROP TABLE ${S}.work_type_scores (the GPU output goes with it); DROP TABLE ${queue}; DROP VOLUME ${volume}.
CREATE TABLE IF NOT EXISTS ${S}.work_type_scores (
  work_id        BIGINT        NOT NULL,
  input_hash     STRING        NOT NULL,   -- sha256('s2r3\n' + text)[:32]: the text the model scored
  model_version  STRING        NOT NULL,   -- the stage-2 model's content id (s2-<sha16>), never an alias
  render_version STRING        NOT NULL,
  class_idx      INT           NOT NULL,   -- index into the model's class list (the policy file's model content)
  confidence     DECIMAL(5,4)  NOT NULL,   -- the stored 4-dp confidence every decision reads
  logits         ARRAY<FLOAT>  NOT NULL,   -- one per class, in the model's class order
  input_key      BIGINT        NOT NULL,   -- the pack inputs' fingerprint (02_queue_keys.sql) the work last had with this text
  s0_doi         STRING,                   -- the pack's stage-0 signals (pack_assembly.stage0_signals)
  s0_names       ARRAY<STRING>,
  s0_urls        ARRAY<STRING>,
  scored_at      TIMESTAMP     NOT NULL,
  seen_at        TIMESTAMP     NOT NULL,   -- the last build that found this text current for the work
  build_id       STRING        NOT NULL    -- the build whose shards scored it
)
CLUSTER BY (work_id)
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');   -- the nightly touch MERGE hits keys spread over every file

ALTER TABLE ${S}.work_type_scores ADD CONSTRAINT wt_scores_conf CHECK (confidence BETWEEN 0 AND 1);

ALTER TABLE ${S}.work_type_scores ADD CONSTRAINT wt_scores_class CHECK (class_idx >= 0 AND class_idx < size(logits));

ALTER TABLE ${S}.work_type_scores ADD CONSTRAINT wt_scores_hash CHECK (length(input_hash) = 32);

-- The queue, created empty once so its grant to the Modal app's service principal is in place before the first build
-- (BuildWorkTypeQueue replaces it each build with CREATE OR REPLACE TABLE, which keeps the table and its grants).
CREATE TABLE IF NOT EXISTS ${queue} (
  shard          INT,
  work_id        BIGINT,
  input_hash     STRING,
  text           STRING,
  input_key      BIGINT,
  new            BOOLEAN,
  s0_doi         STRING,
  s0_names       ARRAY<STRING>,
  s0_urls        ARRAY<STRING>,
  render_version STRING,
  pack_version   STRING,
  model_version  STRING,
  build_id       STRING,
  queued_at      TIMESTAMP
)
CLUSTER BY (shard);

-- The Volume the Modal app writes its shard parquet to and the queue builder keeps its scratch in (a volume of its own,
-- so the app's service principal can write here and nowhere else).
CREATE VOLUME IF NOT EXISTS ${volume};
