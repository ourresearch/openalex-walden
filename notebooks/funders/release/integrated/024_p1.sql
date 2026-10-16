-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- merged notebook cell 87
CREATE OR REPLACE TABLE openalex.funders.funders_compat AS
SELECT f.funder_id, CAST(get_json_object(v41.j, '$.crossref_id') AS BIGINT) crossref_id,
  get_json_object(v41.j, '$.location') location, f.display_name,
  CASE WHEN CASE WHEN v41.j IS NULL AND r.source_key IS NOT NULL THEN array_sort(array_distinct(concat(coalesce(f.alt_names, array()), coalesce(r.alt_names, array())))) ELSE f.alt_names END <=> from_json(get_json_object(v41.j, '$.alternate_titles'), 'array<string>') THEN get_json_object(v41.j, '$.alternate_titles') ELSE to_json(coalesce(CASE WHEN v41.j IS NULL AND r.source_key IS NOT NULL THEN array_sort(array_distinct(concat(coalesce(f.alt_names, array()), coalesce(r.alt_names, array())))) ELSE f.alt_names END, array())) END alternate_titles,
  get_json_object(v41.j, '$.uri') uri,
  coalesce(get_json_object(v41.j, '$.replaces'), CASE WHEN v41.j IS NULL THEN '[]' END) replaces,
  coalesce(get_json_object(v41.j, '$.replaced_by'), CASE WHEN v41.j IS NULL THEN '[]' END) replaced_by,
  get_json_object(v41.j, '$.tokens') tokens, CASE WHEN v41.j IS NOT NULL THEN get_json_object(v41.j, '$.ror_id') ELSE f.ror_id END ror_id,
  CASE WHEN f.status = 'merged' THEN f.merged_into_id END merge_into_id,
  CASE WHEN v41.j IS NOT NULL THEN CAST(get_json_object(v41.j, '$.merge_into_date') AS TIMESTAMP) WHEN f.status='merged' THEN f.updated_date END merge_into_date,
  coalesce(get_json_object(v41.j, '$.created_date'), CAST(f.created_date AS STRING)) created_date, f.wikidata_id,
  get_json_object(v41.j, '$.doi') doi,
  f.country_code, f.homepage_url, f.description, f.image_url, f.image_thumbnail_url, get_json_object(v41.j, '$.json_entity_hash') json_entity_hash, CASE WHEN v41.j IS NOT NULL THEN CAST(get_json_object(v41.j, '$.updated_date') AS TIMESTAMP) ELSE f.updated_date END updated_date
FROM openalex.funders.funder f
LEFT JOIN (SELECT CAST(source_key AS BIGINT) funder_id, record_json j FROM openalex.funders.funder_source_record WHERE source = 'openalex_v41') v41 ON v41.funder_id = f.funder_id
LEFT JOIN openalex.funders.funder_source_record r ON r.source = 'ror' AND r.source_key = lower(regexp_extract(f.ror_id, '([0-9a-z]{9})/*$', 1)) AND r.record_status <> 'gone'
WHERE f.status IN ('active', 'merged');
