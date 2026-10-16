-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- merged notebook cell 4
CREATE TABLE openalex.funders.funder_id (
  id_type STRING NOT NULL, id_value STRING NOT NULL, funder_id BIGINT NOT NULL, origin_funder_id BIGINT, is_primary BOOLEAN,
  source STRING, added_at TIMESTAMP, run_id STRING) USING DELTA;
