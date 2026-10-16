-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- immutable release stage cell 6
CREATE TABLE openalex.funders.nm1013_integrated_lookup AS
SELECT l.*, 'openalex.funders.nm1013_integrated_' release_id,s.catalogue_signature FROM openalex.funders.nm1013_integrated_lookup_rows l
CROSS JOIN (SELECT concat(count(*), ':', cast(sum(cast(xxhash64(funder_id,display_name,alternate_titles,ror_id,doi,crossref_id,merge_into_id) AS DECIMAL(38,0))) AS STRING)) catalogue_signature FROM openalex.funders.nm1013_integrated_catalogue_after) s;
SELECT assert_true((SELECT count(*) FROM openalex.funders.nm1013_integrated_aliases)=204,'alias count'),
 assert_true((SELECT count(*) FROM (SELECT rule,name_key FROM openalex.funders.nm1013_integrated_lookup GROUP BY ALL HAVING count(*)<>1))=0,'ambiguous lookup');
SELECT assert_true(count(*)=0,'alias target absent from approved catalogue or lookup')
FROM openalex.funders.nm1013_integrated_aliases a
LEFT JOIN openalex.funders.nm1013_integrated_catalogue_after f ON f.funder_id=a.funder_id
LEFT JOIN openalex.funders.nm1013_integrated_lookup l ON l.rule=a.rule AND l.name_key=a.name_key AND l.funder_id=coalesce(f.merge_into_id,f.funder_id)
WHERE f.funder_id IS NULL OR l.funder_id IS NULL;
