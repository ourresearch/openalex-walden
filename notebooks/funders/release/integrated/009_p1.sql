-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- pinned served source
CREATE OR REPLACE TEMP VIEW registry_seed_funders AS SELECT * FROM openalex.funders.funders VERSION AS OF 41;
