-- Mapping from CreateBCRFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over bcrf_researchers_input (was the
-- staging table read from s3://openalex-ingest/awards/bcrf/bcrf_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306260  -- Breast Cancer Research Foundation
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    REGEXP_REPLACE(TRIM(g.research_area), '[.]$', '') as display_name,
    NULLIF(TRIM(g.description), '') as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    CAST(NULL AS DOUBLE) as amount,
    CAST(NULL AS STRING) as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'research' as funding_type,
    CAST(NULL AS STRING) as funder_scheme,
    'bcrf_researchers' as provenance,
    TRY_TO_DATE(CONCAT(g.investigator_since, '-01-01'), 'yyyy-MM-dd') as start_date,
    CAST(NULL AS DATE) as end_date,
    TRY_CAST(g.investigator_since AS INT) as start_year,
    CAST(NULL AS INT) as end_year,
    named_struct(
        'given_name', NULLIF(TRIM(g.given_name), ''),
        'family_name', NULLIF(TRIM(g.family_name), ''),
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', NULLIF(TRIM(g.institution), ''),
            'country', NULLIF(TRIM(g.country), ''),
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM bcrf_researchers_input g
CROSS JOIN src_funder f
WHERE g.excluded = '0'
  AND g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
  AND NULLIF(TRIM(g.family_name), '') IS NOT NULL
)
