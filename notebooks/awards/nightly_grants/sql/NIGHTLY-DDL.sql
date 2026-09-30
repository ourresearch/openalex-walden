-- One-time setup for the nightly grants job. {P} = registry prefix (prod: openalex.awards.)
CREATE TABLE IF NOT EXISTS {P}award_nightly_runs (run_id STRING NOT NULL, databricks_run_id STRING, started_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP, status STRING NOT NULL, details_json STRING, error STRING);
CREATE TABLE IF NOT EXISTS {P}award_nightly_lock (lock_name STRING NOT NULL, holder STRING, databricks_run_id STRING, acquired_at TIMESTAMP);
INSERT INTO {P}award_nightly_lock SELECT 'nightly',NULL,NULL,NULL WHERE NOT EXISTS (SELECT 1 FROM {P}award_nightly_lock WHERE lock_name='nightly');
-- carried state, seeded once from the last accepted release (n20260925)
CREATE TABLE IF NOT EXISTS {P}award_bindings_last AS SELECT observation_key,stable_id,family,coalesce(payload.provenance,family) source FROM openalex.awards.n20260925_bindings_final;
CREATE TABLE IF NOT EXISTS {P}award_merge_doi_pairs AS SELECT loser_id,target_id FROM openalex.awards.n20260925_merge_doi_pairs;
CREATE TABLE IF NOT EXISTS {P}award_state_transitions (run_id STRING NOT NULL, stable_id BIGINT NOT NULL, old_status STRING, status STRING NOT NULL,
  redirect_to BIGINT, recorded_at TIMESTAMP NOT NULL);
-- identity basis for funders: the version the deployed chain was pinned to (openalex.funders.funders v34 = current on 09-29).
-- Changing it is a reviewed step (a funder merge can collapse grants): re-create from the live table after a dev run shows the effect.
CREATE TABLE IF NOT EXISTS {P}award_funders_basis AS SELECT * FROM openalex.funders.funders VERSION AS OF 34;
