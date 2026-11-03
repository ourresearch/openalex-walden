-- One written rule for "which Delta commits change no rows" (oxjob #1997, 3 Nov 2026).
--
-- Several jobs ask "what is this table's latest data version / when did its data last change?" from DESCRIBE HISTORY.
-- History mixes data commits (WRITE, MERGE, DELETE, UPDATE, CREATE OR REPLACE TABLE AS SELECT, CLONE, RESTORE,
-- TRUNCATE, STREAMING UPDATE) with maintenance commits that change no rows, and predictive optimization adds the
-- latter on its own schedule. Each reader kept its own list and they drifted: on 3 Nov, four lists, three naming
-- 'COMPUTE STATISTICS' (which Databricks never writes) and missing 'COMPUTE STATS' (which it does). Harm on record:
-- c3256527 and c99c4ee3 (autocomplete freshness alarm tripped by VACUUM, then by COMPUTE STATS: a false failure mail),
-- b6241b14 (vector matcher guard), 0eed17e6 and #1876 (Nightly Grants bound the wrong version).
--
-- Every reader filters with NOT openalex.works.is_maintenance_op(operation). Add an operation here, never in a reader.
-- Apply: run this file on any SQL warehouse (CREATE OR REPLACE is idempotent). Revert: DROP FUNCTION
-- openalex.works.is_maintenance_op, after reverting the readers that call it.
CREATE OR REPLACE FUNCTION openalex.works.is_maintenance_op(operation STRING)
RETURNS BOOLEAN
COMMENT 'oxjob #1997: TRUE for Delta history operations that change no rows (VACUUM, OPTIMIZE, stats, properties, constraints, layout). Readers asking for a table''s latest data version filter with NOT is_maintenance_op(operation). Source: openalex-walden notebooks/maintenance/is_maintenance_op.sql'
RETURN upper(trim(operation)) IN (
  'VACUUM START', 'VACUUM END', 'OPTIMIZE', 'REORG', 'FSCK',
  'COMPUTE STATS', 'COMPUTE STATISTICS', 'ANALYZE',
  'SET TBLPROPERTIES', 'UNSET TBLPROPERTIES', 'SET TABLE PROPERTIES', 'UPGRADE PROTOCOL',
  'SET COLUMN COMMENT', 'CHANGE COLUMN', 'ADD COLUMNS', 'ADD CONSTRAINT', 'DROP CONSTRAINT', 'CLUSTER BY'
);
