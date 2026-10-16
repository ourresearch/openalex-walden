-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- merged notebook cell 3
CREATE TABLE openalex.funders.funder (
  funder_id BIGINT NOT NULL, status STRING NOT NULL, merged_into_id BIGINT, ror_id STRING, ror_primary BOOLEAN, parent_id BIGINT,
  display_name STRING, alt_names ARRAY<STRING>, country_code STRING, homepage_url STRING, wikidata_id STRING, description STRING,
  image_url STRING, image_thumbnail_url STRING, created_date TIMESTAMP, updated_date TIMESTAMP, created_by STRING, run_id STRING,
  mint_key STRING) USING DELTA;
