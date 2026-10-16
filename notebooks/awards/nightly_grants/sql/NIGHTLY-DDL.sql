-- One-time setup for the nightly grants job. {P} = registry prefix (prod: openalex.awards.)
CREATE TABLE IF NOT EXISTS {P}award_nightly_runs (run_id STRING NOT NULL, databricks_run_id STRING, started_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP, status STRING NOT NULL, details_json STRING, error STRING);
CREATE TABLE IF NOT EXISTS {P}award_nightly_lock (lock_name STRING NOT NULL, holder STRING, databricks_run_id STRING, acquired_at TIMESTAMP);
INSERT INTO {P}award_nightly_lock SELECT 'nightly',NULL,NULL,NULL WHERE NOT EXISTS (SELECT 1 FROM {P}award_nightly_lock WHERE lock_name='nightly');
-- carried state, written by each successful run (prod was seeded once from release n20260925 on 09-29; that release's tables are dropped)
CREATE TABLE IF NOT EXISTS {P}award_bindings_last (observation_key STRING, stable_id BIGINT, family STRING, source STRING);
CREATE TABLE IF NOT EXISTS {P}award_merge_doi_pairs (loser_id BIGINT, target_id BIGINT);
CREATE TABLE IF NOT EXISTS {P}award_state_transitions (run_id STRING NOT NULL, stable_id BIGINT NOT NULL, old_status STRING, status STRING NOT NULL,
  redirect_to BIGINT, recorded_at TIMESTAMP NOT NULL);
-- identity basis for funders: the version the deployed chain was pinned to (openalex.funders.funders v34 = current on 09-29).
-- Changing it is a reviewed step (a funder merge can collapse grants): re-create from the live table after a dev run shows the effect.
CREATE TABLE IF NOT EXISTS {P}award_funders_basis AS SELECT * FROM openalex.funders.funders VERSION AS OF 34;
-- ce1014 pinned link lists (list_legs.py): the night swaps this table in (CREATE OR REPLACE); created empty up front because
-- CreateWorksEnriched reads it (a SUPPRESSed link keeps its funder on the work) and may run before the first night that writes it.
CREATE TABLE IF NOT EXISTS {P}work_award_list_audit (action STRING, list_name STRING, list_version BIGINT, source STRING, work_id BIGINT,
  served_work_id BIGINT, list_award STRING, award_stable_id BIGINT, status STRING, detail STRING, reason STRING,
  award STRUCT<id:STRING,display_name:STRING,funder_award_id:STRING,funder_id:STRING,funder_display_name:STRING>, release_id STRING);
-- ce1014 data-only list activation (list_legs.bindings): append-only; Rohan's bind-list.sh line adds one row per activation/rollback.
-- Nightly binds it at run start; per list the newest row with active_from_night <= the run's PT start date wins; enabled=false = off.
CREATE TABLE IF NOT EXISTS {P}award_list_bindings (list_name STRING NOT NULL, action STRING NOT NULL, kind STRING, relation STRING NOT NULL,
  version BIGINT NOT NULL, expected_rows BIGINT NOT NULL, tolerance BIGINT, source STRING, columns_json STRING, filter_json STRING, priority INT,
  max_unresolved BIGINT, max_unmatched BIGINT, enabled BOOLEAN NOT NULL, active_from_night DATE NOT NULL, bound_at TIMESTAMP NOT NULL,
  bound_by STRING, note STRING) TBLPROPERTIES ('delta.appendOnly'='true');
