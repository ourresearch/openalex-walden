-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- crossref OR
CREATE OR REPLACE TEMP VIEW crossref_funder_references AS
SELECT lm.work_id,lower(lm.native_id) native_id,f.name funder_name,f.ror funder_ror,f.awards award_ids,
 lower(regexp_extract(trim(f.ror),r'(?i)^(?:https?://ror\.org/)?([0-9a-z]{9})/?$',1)) rk,
 sha2(to_json(named_struct('native_id',lower(lm.native_id),'name',f.name,'ror',f.ror,'awards',array_sort(f.awards))),256) reference_key
FROM openalex.works.locations_mapped lm
LATERAL VIEW explode(funders) e AS f
WHERE lm.provenance='crossref' AND lm.work_id IS NOT NULL;
