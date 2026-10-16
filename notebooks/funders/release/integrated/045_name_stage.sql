-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- immutable release stage cell 4
SELECT assert_true(count(*)=0 AND count_if(l.funder_id IS NULL OR w.funder_id IS NULL OR l.merge_into_id IS NOT NULL OR w.merge_into_id IS NOT NULL OR (l.ror_id IS NOT NULL AND w.ror_id IS NOT NULL AND l.ror_id<>w.ror_id))=0,'merge preconditions changed')
FROM openalex.funders.nm1013_integrated_merges m
LEFT JOIN openalex.funders.nm1013_integrated_catalogue_before l ON l.funder_id=m.loser_id
LEFT JOIN openalex.funders.nm1013_integrated_catalogue_before w ON w.funder_id=m.survivor_id;
SELECT assert_true(count(*)=0,'incoming redirects require renewed merge review') FROM openalex.funders.nm1013_integrated_catalogue_before f JOIN openalex.funders.nm1013_integrated_merges m ON f.merge_into_id=m.loser_id;
CREATE TABLE openalex.funders.nm1013_integrated_catalogue_after AS
SELECT f.* EXCEPT(merge_into_id),coalesce(m.survivor_id,f.merge_into_id) merge_into_id
FROM openalex.funders.nm1013_integrated_catalogue_before f
LEFT JOIN openalex.funders.nm1013_integrated_merges m ON m.loser_id=f.funder_id;
CREATE TABLE openalex.funders.nm1013_integrated_identifiers AS
SELECT 'ror' id_type,ror_id id_value,funder_id,funder_id origin_funder_id
FROM openalex.funders.nm1013_integrated_catalogue_before WHERE ror_id IS NOT NULL;
CREATE TABLE openalex.funders.nm1013_integrated_registry_after AS SELECT f.funder_id AS funder_id,CASE WHEN m.loser_id IS NOT NULL THEN 'merged' ELSE f.status END AS status,coalesce(m.survivor_id,f.merged_into_id) AS merged_into_id,f.ror_id AS ror_id,f.ror_primary AS ror_primary,f.parent_id AS parent_id,f.display_name AS display_name,f.alt_names AS alt_names,f.country_code AS country_code,f.homepage_url AS homepage_url,f.wikidata_id AS wikidata_id,f.description AS description,f.image_url AS image_url,f.image_thumbnail_url AS image_thumbnail_url,f.created_date AS created_date,CASE WHEN m.loser_id IS NOT NULL THEN current_timestamp() ELSE f.updated_date END AS updated_date,f.created_by AS created_by,CASE WHEN m.loser_id IS NOT NULL THEN 'openalex.funders.nm1013_integrated_' ELSE f.run_id END AS run_id,f.mint_key AS mint_key FROM openalex.funders.nm1013_integrated_registry_before f LEFT JOIN openalex.funders.nm1013_integrated_merges m ON f.funder_id=m.loser_id OR f.merged_into_id=m.loser_id;
CREATE TABLE openalex.funders.nm1013_integrated_identifiers_after AS SELECT f.id_type AS id_type,f.id_value AS id_value,coalesce(m.survivor_id,f.funder_id) AS funder_id,f.origin_funder_id AS origin_funder_id,f.is_primary AS is_primary,f.source AS source,f.added_at AS added_at,CASE WHEN m.loser_id IS NOT NULL THEN 'openalex.funders.nm1013_integrated_' ELSE f.run_id END AS run_id FROM openalex.funders.nm1013_integrated_identifiers_before f LEFT JOIN openalex.funders.nm1013_integrated_merges m ON f.funder_id=m.loser_id AND f.id_type NOT IN ('name','ror_cleared');
SELECT assert_true(count(*)=0,'duplicate identifier key') FROM (SELECT id_type,id_value FROM openalex.funders.nm1013_integrated_identifiers_before GROUP BY ALL HAVING count(*)<>1);
