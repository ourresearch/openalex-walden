-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- merged notebook cell 5
CREATE TABLE openalex.funders.funder_source_record (
  source STRING NOT NULL, source_key STRING NOT NULL, name STRING, alt_names ARRAY<STRING>, country STRING, ror_id STRING,
  fundref_ids ARRAY<STRING>, replaced_by ARRAY<STRING>, record_status STRING, is_funder BOOLEAN, homepage STRING, record_json STRING,
  fingerprint STRING, first_seen_at TIMESTAMP, fetched_at TIMESTAMP, changed_run_id STRING,
  outcome STRING, reason STRING, funder_id BIGINT, decided_run_id STRING, decided_fingerprint STRING) USING DELTA;
