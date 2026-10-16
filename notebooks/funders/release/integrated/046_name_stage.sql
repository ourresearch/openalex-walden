-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- immutable release stage cell 5
-- name_lookup: ROR/exact remain ahead of hierarchy, reviewed aliases, twins, programmes.
-- All-ROR names and broad normalization are ONLY used for pipe-delimited leaves.
-- No country/fuzzy matching and no nm1012_jev_* inputs: Jev requires a new gate.
CREATE TABLE openalex.funders.nm1013_integrated_lookup_rows AS
WITH f AS (SELECT f.*, CASE WHEN m.loser_id IS NOT NULL THEN f.funder_id
 ELSE coalesce(f.merge_into_id,f.funder_id) END canonical_id
 FROM openalex.funders.nm1013_integrated_catalogue_after f
 LEFT JOIN openalex.funders.nm1013_integrated_merges m ON m.loser_id=f.funder_id),
alias_variants AS (
 SELECT canonical_id,lower(trim(nm)) k FROM f
 LATERAL VIEW explode(array_union(array(display_name),coalesce(from_json(alternate_titles,'array<string>'),array()))) a AS nm
), aliases AS (SELECT k,array_sort(collect_set(canonical_id)) targets FROM alias_variants WHERE k<>'' GROUP BY k),
keep_variants AS (
 SELECT f.canonical_id,lower(trim(k.name)) k FROM openalex.common.funder_names_keep k
 JOIN f ON f.funder_id=try_cast(regexp_extract(k.id,'([0-9]+)$',1) AS BIGINT)
), keeps AS (SELECT k,array_sort(collect_set(canonical_id)) targets FROM keep_variants WHERE k<>'' GROUP BY k),
names AS (
 SELECT k,targets FROM keeps
 UNION ALL SELECT a.k,a.targets FROM aliases a LEFT ANTI JOIN keeps k ON a.k=k.k
),
prod_ror_variants AS (
 SELECT canonical_id,lower(regexp_extract(trim(ror_id),r'(?i)^(?:https?://ror\.org/)?([0-9a-z]{9})/?$',1)) k FROM f
), registry_ror_variants AS (
 SELECT coalesce(f.canonical_id,r.funder_id) canonical_id,lower(regexp_extract(trim(r.id_value),r'(?i)^(?:https?://ror\.org/)?([0-9a-z]{9})/?$',1)) k
 FROM openalex.funders.nm1013_integrated_identifiers r
 LEFT JOIN openalex.funders.nm1013_integrated_merges tm ON tm.loser_id=r.origin_funder_id
 LEFT JOIN f ON f.funder_id=CASE WHEN tm.loser_id IS NOT NULL THEN r.origin_funder_id ELSE r.funder_id END WHERE r.id_type='ror'
), ror_variants AS (
 SELECT * FROM registry_ror_variants
 UNION ALL
 SELECT p.* FROM prod_ror_variants p LEFT ANTI JOIN registry_ror_variants r ON p.k=r.k
), rors AS (SELECT k,array_sort(collect_set(canonical_id)) targets FROM ror_variants WHERE k<>'' GROUP BY k),
-- (i) Every ROR name type is retained for candidate construction. A single
-- globally unique target is required. Standalone all-ROR matching failed 99%.
ror_names AS (
 SELECT lower(trim(n.value)) k,target funder_id
 FROM openalex.institutions.ror x JOIN rors r
 ON lower(regexp_extract(x.id,'([0-9a-z]{9})/*$',1))=r.k
 LATERAL VIEW explode(x.names) a AS n LATERAL VIEW explode(r.targets) b AS target
 WHERE trim(n.value)<>''
), dictionary AS (
 SELECT k,target funder_id FROM names LATERAL VIEW explode(targets) e AS target
 UNION ALL SELECT k,funder_id FROM ror_names
), normalized_variants AS (
 -- (ii) This normalization is used for hierarchy leaves only in the accepted
 -- ladder. Applying it to arbitrary whole strings failed the 99% criterion.
 SELECT trim(regexp_replace(regexp_replace(regexp_replace(translate(lower(replace(replace(replace(k,'&amp;','&'),'&apos;',char(39)),'&#39;',char(39))), 'áàâäãåāăąçćčďđéèêëēėęěíìîïīıłñńňóòôöõøōőřśšşťúùûüūůűýÿžźż', 'aaaaaaaaacccddeeeeeeeeiiiiiilnnnoooooooorssstuuuuuuuyyzzz'), '&', ' and '), r'[^\p{L}\p{N}]+', ' '), r'(^| )the( |$)', ' ')) nk,funder_id FROM dictionary
), normalized_names AS (
 SELECT nk,collect_set(funder_id) targets FROM normalized_variants
 WHERE length(nk)>=4 GROUP BY nk
),
twin_names AS (
 SELECT n.k,collect_set(coalesce(m.survivor_id,t)) targets,count_if(m.loser_id IS NOT NULL) changed
 FROM (SELECT k,explode(targets) t FROM names) n
 LEFT JOIN openalex.funders.nm1013_integrated_merges m ON m.loser_id=t
 GROUP BY n.k
), unique_rules AS (
 SELECT 'exact' rule,k name_key,get(targets,0) funder_id FROM names WHERE size(targets)=1
 UNION ALL SELECT 'hierarchy',nk,get(targets,0) FROM normalized_names WHERE size(targets)=1
 UNION ALL SELECT rule,name_key,funder_id FROM openalex.funders.nm1013_integrated_aliases
 UNION ALL SELECT 'twin',k,get(targets,0) FROM twin_names WHERE size(targets)=1 AND changed>0
)
SELECT u.rule,u.name_key,coalesce(f.merge_into_id,f.funder_id) funder_id
FROM unique_rules u
JOIN openalex.funders.nm1013_integrated_catalogue_after f ON f.funder_id=u.funder_id
JOIN openalex.funders.nm1013_integrated_catalogue_after target ON target.funder_id=coalesce(f.merge_into_id,f.funder_id)
WHERE target.merge_into_id IS NULL;
