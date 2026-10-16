-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- merged notebook cell 11
CREATE TABLE openalex.funders.f_ids_ever_issued USING DELTA AS
SELECT funder_id, 'live_v41' source, try_cast(substr(created_date, 1, 19) AS TIMESTAMP) issued_at FROM registry_seed_funders
UNION ALL
SELECT funder_id, 'deleted', deleted_at FROM openalex.funders.deleted_funders WHERE funder_id NOT IN (SELECT funder_id FROM registry_seed_funders)
UNION ALL
SELECT funder_id, created_by, created_date FROM openalex.funders.funder WHERE created_by LIKE 'mint%';
