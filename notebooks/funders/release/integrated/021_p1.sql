-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- seed check binding
CREATE OR REPLACE TEMP VIEW registry_prev_funder AS SELECT * FROM openalex.funders.funder;
