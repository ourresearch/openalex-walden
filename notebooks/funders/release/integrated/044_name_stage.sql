-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- immutable release stage cell 3
-- name_twin_merges: mirrors only the reviewed merge rows in edits.csv.
-- All 59 reviewed merges are deferred; this release preserves every owner.
CREATE TABLE openalex.funders.nm1013_integrated_merges AS
SELECT CAST(NULL AS BIGINT) loser_id,CAST(NULL AS BIGINT) survivor_id WHERE false;
