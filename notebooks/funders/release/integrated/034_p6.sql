-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- funders minus funders_v41_clone ALL 22 COLUMNS
SELECT count(*) differing_rows,assert_true(count(*)=0,'compat equality failed') ok FROM (SELECT * FROM openalex.funders.funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.funders_v41_clone) EXCEPT ALL SELECT * FROM openalex.funders.funders_v41_clone);
