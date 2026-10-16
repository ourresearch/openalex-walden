-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- merged notebook cell 6
CREATE TABLE openalex.funders.funder_decision (
  run_id STRING NOT NULL, decided_at TIMESTAMP, source STRING, source_key STRING, action STRING NOT NULL, funder_id BIGINT,
  ror_rid STRING, fundref STRING, display_name STRING, country STRING, evidence STRING) USING DELTA;
