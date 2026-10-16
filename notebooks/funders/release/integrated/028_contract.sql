-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- resolver
CREATE OR REPLACE FUNCTION openalex.funders.resolve_funder(
 funder_name STRING, ror STRING, fundref STRING, internal_key BOOLEAN, requested_name_policy STRING)
RETURNS TABLE(funder_id BIGINT, reason STRING, ror_owners BIGINT, fundref_owners BIGINT,
 name_owners BIGINT, ror_funder_id BIGINT, fundref_funder_id BIGINT, name_funder_id BIGINT,
 exact_name_owners BIGINT, exact_name_funder_id BIGINT)
RETURN
WITH input AS (
 SELECT regexp_replace(trim(regexp_replace(lower(funder_name), r'[^\p{L}\p{N}]+', ' ')), '^the ', '') nk,
 lower(regexp_extract(trim(ror), r'(?i)^(?:https?://ror\.org/)?([0-9a-z]{9})/?$',1)) rk,
 regexp_extract(trim(fundref), r'(?i)^(?:(?:https?://(?:dx\.)?doi\.org/|doi:\s*)?10\.13039/)?([0-9]+)$',1) fk
), hits AS (
 SELECT r.owners rc,r.funder_id rf,d.owners dc,d.funder_id df,n.owners nc,n.funder_id nf,e.owners ec,e.funder_id ef
 FROM input x
 LEFT JOIN openalex.funders.resolution_keys r ON r.key_type='ror' AND r.name_policy='all' AND r.key_value=x.rk
 LEFT JOIN openalex.funders.resolution_keys d ON d.key_type='fundref' AND d.name_policy='all' AND d.key_value=x.fk AND internal_key
 LEFT JOIN openalex.funders.resolution_keys n ON n.key_type='name' AND n.name_policy=requested_name_policy AND n.key_value=x.nk
 LEFT JOIN openalex.funders.resolution_keys e ON e.key_type='exact_name' AND e.name_policy=requested_name_policy AND e.key_value=lower(trim(funder_name))
)
SELECT CASE WHEN rc=1 THEN rf WHEN dc=1 THEN df WHEN nc=1 THEN nf END,
 CASE WHEN rc=1 THEN 'ROR' WHEN dc=1 THEN 'FundRef internal' WHEN nc=1 THEN 'name'
      WHEN rc>1 OR dc>1 OR nc>1 THEN 'ambiguous' ELSE 'unresolved' END,
 rc,dc,nc,rf,df,nf,ec,ef FROM hits;
