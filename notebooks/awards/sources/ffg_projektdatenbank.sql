-- Mapping from CreateFFGAwards.ipynb (origin e22fdf7f): its awards-table SELECT over ffg_projektdatenbank_input (was the
-- staging table read from s3://openalex-ingest/awards/ffg/ffg_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320323031  -- FFG
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.project_id))))) % 9000000000 as id,
    COALESCE(NULLIF(TRIM(g.title), ''), NULLIF(TRIM(g.acronym), '')) as display_name,
    NULLIF(TRIM(g.abstract), '') as description,
    f.funder_id,
    LOWER(TRIM(g.project_id)) as funder_award_id,
    CAST(NULL AS DOUBLE) as amount,
    CAST(NULL AS STRING) as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'research' as funding_type,
    NULLIF(TRIM(g.programme), '') as funder_scheme,
    'ffg_projektdatenbank' as provenance,
    TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
    TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
    YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
    YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,
    CASE WHEN NULLIF(TRIM(g.lead_org), '') IS NOT NULL THEN named_struct(
        'given_name', CAST(NULL AS STRING),
        'family_name', CAST(NULL AS STRING),
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', TRIM(g.lead_org),
            'country', CASE UPPER(TRIM(g.lead_org_country))
                WHEN 'AUT' THEN 'AT' WHEN 'DEU' THEN 'DE' WHEN 'CHE' THEN 'CH' WHEN 'NLD' THEN 'NL'
                WHEN 'GBR' THEN 'GB' WHEN 'ITA' THEN 'IT' WHEN 'FRA' THEN 'FR' WHEN 'BEL' THEN 'BE'
                WHEN 'SWE' THEN 'SE' WHEN 'ESP' THEN 'ES' WHEN 'CZE' THEN 'CZ' WHEN 'SVN' THEN 'SI'
                WHEN 'HUN' THEN 'HU' WHEN 'SVK' THEN 'SK' WHEN 'POL' THEN 'PL' WHEN 'DNK' THEN 'DK'
                WHEN 'FIN' THEN 'FI' WHEN 'NOR' THEN 'NO' WHEN 'USA' THEN 'US' WHEN 'LUX' THEN 'LU'
            END,
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) END as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
         affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>)
        as co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
         affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>)
        as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.project_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM ffg_projektdatenbank_input g
CROSS JOIN src_funder f
WHERE g.project_id IS NOT NULL AND TRIM(g.project_id) != ''
  AND NOT (COALESCE(g.programme, '') RLIKE '(?i)ENIN|LADIN|EBIN|Emissionsfreie Busse und Infrastruktur')
)
