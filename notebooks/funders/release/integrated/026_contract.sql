-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- entities
CREATE OR REPLACE VIEW openalex.funders.reader_entities AS
SELECT f.funder_id, CASE WHEN f.status='merged' THEN f.merged_into_id END merge_into_id,
       c.funder_id canonical_id, f.status, f.display_name, f.ror_id, coalesce(f.created_by='mint_ror',false) registry_created
FROM openalex.funders.funder f LEFT JOIN openalex.funders.funder c ON c.funder_id=coalesce(f.merged_into_id,f.funder_id) AND c.status='active'
WHERE f.status IN ('active','merged');
