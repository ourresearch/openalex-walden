-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- keys
CREATE OR REPLACE VIEW openalex.funders.resolution_keys AS
WITH acronym_keys AS (
 SELECT DISTINCT regexp_replace(trim(regexp_replace(lower(nm), r'[^\p{L}\p{N}]+', ' ')), '^the ', '') k FROM openalex.funders.funder f
 LATERAL VIEW explode(coalesce(f.alt_names,array())) a AS nm
 WHERE trim(nm) RLIKE '^[A-Z]{1,5}$'
), acronym_keeps AS (
 SELECT DISTINCT regexp_replace(trim(regexp_replace(lower(a.name), r'[^\p{L}\p{N}]+', ' ')), '^the ', '') keep_key,try_cast(regexp_extract(a.id,'F([0-9]+)$',1) AS BIGINT) keep_id
 FROM openalex.common.funder_names_keep a WHERE upper(trim(a.keep))='YES'
), candidates AS (
 SELECT i.id_type key_type, i.id_value key_value, e.canonical_id funder_id,
        coalesce(i.origin_funder_id,i.funder_id) origin_funder_id
 FROM openalex.funders.funder_id i JOIN openalex.funders.reader_entities e ON e.funder_id=i.funder_id
 WHERE (i.id_type IN ('fundref','name') OR (i.id_type='ror' AND i.is_primary)) AND e.canonical_id IS NOT NULL AND NOT (i.id_type='fundref' AND e.registry_created)
 UNION ALL
 -- Include all names, not just the unique index, so collisions cannot be hidden.
 SELECT 'name', regexp_replace(trim(regexp_replace(lower(nm), r'[^\p{L}\p{N}]+', ' ')), '^the ', ''), e.canonical_id, f.funder_id
 FROM openalex.funders.funder f JOIN openalex.funders.reader_entities e ON e.funder_id=f.funder_id
 LATERAL VIEW explode(concat(array(f.display_name), coalesce(f.alt_names,array()))) n AS nm
 WHERE e.canonical_id IS NOT NULL
 UNION ALL
 -- Preserve DataCite's exact-name conflict guard independently of normalized aliases.
 SELECT 'exact_name', lower(trim(nm)), e.canonical_id, f.funder_id
 FROM openalex.funders.funder f JOIN openalex.funders.reader_entities e ON e.funder_id=f.funder_id
 LATERAL VIEW explode(concat(array(f.display_name), coalesce(f.alt_names,array()))) n AS nm
 WHERE e.canonical_id IS NOT NULL
 UNION ALL
 SELECT 'name', regexp_replace(trim(regexp_replace(lower(a.name), r'[^\p{L}\p{N}]+', ' ')), '^the ', ''), e.canonical_id, e.funder_id
 FROM openalex.common.funder_names_keep a JOIN openalex.funders.reader_entities e
 ON e.funder_id=try_cast(regexp_extract(a.id,'F([0-9]+)$',1) AS BIGINT)
 WHERE upper(trim(a.keep))='YES' AND e.canonical_id IS NOT NULL
), scoped AS (
 SELECT key_type,key_value,funder_id,'all' name_policy FROM candidates
 UNION ALL
 SELECT c.key_type,c.key_value,c.funder_id,'datacite_name_v1' FROM candidates c
 LEFT SEMI JOIN openalex.funders.funder_match_policy p ON p.funder_id=c.origin_funder_id AND p.matcher='datacite_name_v1' AND p.eligible
 WHERE c.key_type IN ('name','exact_name')
)
SELECT key_type,key_value,name_policy,count(DISTINCT funder_id) owners,max(funder_id) funder_id
FROM scoped
LEFT JOIN acronym_keys excluded ON key_type IN ('name','exact_name') AND key_value=excluded.k
LEFT JOIN acronym_keeps kept ON key_value=kept.keep_key AND funder_id=kept.keep_id
WHERE key_value IS NOT NULL AND key_value<>'' AND (excluded.k IS NULL OR kept.keep_id IS NOT NULL)
AND NOT (key_type IN ('name','exact_name') AND (
 (key_value='robert bosch germany' AND funder_id=4320325119)
 OR (key_value='council of scientific and industrial research csir' AND funder_id=3825135565)
 OR (key_value='national health service' AND funder_id=3458447014)))
GROUP BY key_type,key_value,name_policy;
