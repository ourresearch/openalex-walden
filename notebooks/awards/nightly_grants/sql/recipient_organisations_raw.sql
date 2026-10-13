WITH country_aliases AS (
  -- Source spellings equivalent to the versioned country/award_country_lookup.csv ISO aliases.
  SELECT * FROM VALUES ('bolivia, plurinational state of','BO'),('bosnia-herzegovina','BA'),('congo, democratic republic of','CD'),('congo, the democratic republic of the','CD'),('cote d\'ivoire','CI'),('czech republic','CZ'),('côte d\'ivoire','CI'),('dominican republic','DO'),('korea, republic of','KR'),('palestine, state of','PS'),('palestinian territories','PS'),('taiwan, province of china','TW'),('tanzania, united republic of','TZ'),('trinidad and tobago','TT'),('türkiye','TR'),('venezuela, bolivarian republic of','VE'),('viet nam','VN') AS aliases(value,code)
), country_names AS (
  SELECT lower(trim(value)) value, min(upper(id)) code FROM openalex.common.countries
  LATERAL VIEW explode(array(id,alpha_3,display_name,full_name)) c AS value
  WHERE nullif(trim(value),'') IS NOT NULL GROUP BY 1 HAVING count(DISTINCT id)=1
), us AS (
  SELECT 'usaspending_usaid' provenance,4320306115L funder_id,award_id_fain funder_award_id,recipient_name name,recipient_country_code country FROM openalex.awards.usaid_awards_raw
    WHERE NOT coalesce(upper(trim(business_types_code))='P' OR upper(trim(business_types_description)) IN ('INDIVIDUAL','INDIVIDUALS'),false)
  UNION ALL SELECT 'usaspending_cdc',4320332162L,award_id_fain,recipient_name,recipient_country_code FROM openalex.awards.cdc_awards_raw
    WHERE NOT coalesce(upper(trim(business_types_code))='P' OR upper(trim(business_types_description)) IN ('INDIVIDUAL','INDIVIDUALS'),false)
  UNION ALL SELECT 'usaspending',4320306078L,award_id_fain,recipient_name,recipient_country_code FROM openalex.awards.dod_awards_raw
    WHERE NOT coalesce(upper(trim(business_types_code))='P' OR upper(trim(business_types_description)) IN ('INDIVIDUAL','INDIVIDUALS'),false)
  UNION ALL SELECT 'usaspending_usda',4320306114L,award_id_fain,recipient_name,recipient_country_code FROM openalex.awards.usda_awards_raw
    WHERE NOT coalesce(upper(trim(business_types_code))='P' OR upper(trim(business_types_description)) IN ('INDIVIDUAL','INDIVIDUALS'),false)
), iati AS (
  SELECT provenance,try_cast(openalex_funder_id AS BIGINT) funder_id,trim(funder_award_id) funder_award_id,reporting_org_ref,reporting_org_name,
    concat(coalesce(from_json(participating_orgs_json,'array<struct<role:string,ref:string,name:string,name_en:string,name_fr:string,name_es:string,percentage:string>>'),array()),
      coalesce(from_json(children_participating_orgs_json,'array<struct<role:string,ref:string,name:string,name_en:string,name_fr:string,name_es:string,percentage:string>>'),array())) orgs
  FROM openalex.awards.iati_raw
  WHERE ship='true' AND is_research='true' AND research_rule_ids IS NOT NULL
    AND NOT coalesce(try_cast(award_amount AS DOUBLE)<=1 OR lower(trim(title)) IN ('not in use','changed')
      OR lower(trim(title)) RLIKE '^(travel\\b|programme related costs)',false)
  UNION ALL
  SELECT 'idrc_iati',4320319949L,iati_identifier,CAST(NULL AS STRING),CAST(NULL AS STRING),
    from_json(participating_orgs_json,'array<struct<role:string,ref:string,name:string,name_en:string,name_fr:string,name_es:string,percentage:string>>')
  FROM openalex.awards.idrc_raw
), source_orgs AS (
  SELECT *, 'lead' role,CAST(NULL AS DOUBLE) share,CAST(NULL AS STRING) ror,'recipient_name' source_field FROM us
  UNION ALL
  SELECT 'gates_foundation',4320306137L,grant_id,grantee_name,grantee_country,'lead',NULL,NULL,'grantee_name' FROM openalex.awards.gates_raw
  UNION ALL
  SELECT 'wellcome_trust',4320311904L,grant_ref,recipient_org_name,recipient_orgcountry,'lead',NULL,
    CASE WHEN recipient_org_id RLIKE '^https://ror.org/[0-9a-z]{9}$' THEN recipient_org_id END,'recipient_org_name'
    FROM openalex.awards.wellcome_raw WHERE grant_ref NOT LIKE '360G-%'
  UNION ALL
  SELECT provenance,funder_id,funder_award_id,coalesce(o.name,o.name_en,o.name_fr,o.name_es),
    CASE WHEN o.ref RLIKE '^[A-Z]{2}-' THEN substring(o.ref,1,2) END,
    CASE o.role WHEN '2' THEN 'partner' WHEN '4' THEN 'implementing' END,
    try_cast(o.percentage AS DOUBLE)/100.0,
    CASE WHEN o.ref RLIKE '^https://ror.org/[0-9a-z]{9}$' THEN o.ref END,'participating-org'
  FROM iati LATERAL VIEW explode(orgs) e AS o WHERE o.role IN ('2','4')
    -- Implementers can also co-fund: retain role 4 even when the same organisation has role 1.
    -- Only remove the publisher/extending agency's self-accountability (role 2).
    AND (o.role='4' OR NOT (
      coalesce(nullif(trim(o.ref),'')=reporting_org_ref,false)
      OR coalesce(lower(trim(coalesce(o.name,o.name_en,o.name_fr,o.name_es)))=lower(trim(reporting_org_name)),false)
      OR coalesce(exists(orgs, f -> f.role='3' AND
        ((nullif(trim(o.ref),'') IS NOT NULL AND o.ref=f.ref)
         OR lower(trim(coalesce(o.name,o.name_en,o.name_fr,o.name_es)))=lower(trim(coalesce(f.name,f.name_en,f.name_fr,f.name_es))))),false)))
)
SELECT DISTINCT abs(xxhash64(concat(s.funder_id,':',lower(s.funder_award_id)))) % 9000000000 staging_id,
  s.funder_id,lower(s.funder_award_id) award_key,s.provenance,s.funder_award_id,
  'organisation' kind,coalesce(c.code,ca.code) country_code,CAST(NULL AS STRING) region_code,CAST(NULL AS STRING) region_vocabulary,
  'funder_reported' basis,s.share,CASE WHEN s.share IS NOT NULL THEN 'funder_reported' END share_basis,
  s.source_field,'funder' asserted_by,'af1570-organisations-v1' adapter_version,current_timestamp() asserted_at,
  trim(s.name) name,s.role,s.ror
FROM source_orgs s LEFT JOIN country_names c ON c.value=lower(trim(s.country))
LEFT JOIN country_aliases ca ON ca.value=lower(trim(s.country))
WHERE nullif(trim(s.name),'') IS NOT NULL AND nullif(trim(s.funder_award_id),'') IS NOT NULL
