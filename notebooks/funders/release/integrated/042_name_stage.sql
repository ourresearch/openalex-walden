-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- immutable release stage cell 1
-- CREATE (no REPLACE) makes this release immutable; use a fresh release id to restage.
CREATE TABLE openalex.funders.nm1013_integrated_catalogue_before AS SELECT * FROM openalex.funders.funders;
CREATE TABLE openalex.funders.nm1013_integrated_registry_before AS SELECT * FROM openalex.funders.funder;
CREATE TABLE openalex.funders.nm1013_integrated_identifiers_before AS SELECT * FROM openalex.funders.funder_id;
