-- Mapping from CreateArthritisFoundationAwards.ipynb (origin e22fdf7f): its awards-table SELECT over arthritis_foundation_announcements_input (was the
-- staging table read from s3://openalex-ingest/awards/arthritis_foundation/arthritis_foundation_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306237  -- Arthritis Foundation
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    NULLIF(TRIM(g.description), '') as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    CASE WHEN TRY_CAST(g.amount AS DOUBLE) > 0 THEN TRY_CAST(g.amount AS DOUBLE) END as amount,
    CASE WHEN TRY_CAST(g.amount AS DOUBLE) > 0 THEN 'USD' END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    CASE WHEN g.scheme RLIKE '(?i)fellow' THEN 'fellowship' ELSE 'research' END as funding_type,
    NULLIF(TRIM(g.scheme), '') as funder_scheme,
    'arthritis_foundation_announcements' as provenance,
    TRY_TO_DATE(CONCAT(g.year, '-01-01'), 'yyyy-MM-dd') as start_date,
    CAST(NULL AS DATE) as end_date,
    TRY_CAST(g.year AS INT) as start_year,
    CAST(NULL AS INT) as end_year,
    named_struct(
        'given_name', NULLIF(TRIM(g.lead_given_name), ''),
        'family_name', NULLIF(TRIM(g.lead_family_name), ''),
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', NULLIF(TRIM(g.institution), ''),
            'country', CAST(NULL AS STRING),
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) as lead_investigator,
    CASE WHEN NULLIF(TRIM(g.co_lead_family_name), '') IS NOT NULL THEN
    named_struct(
        'given_name', NULLIF(TRIM(g.co_lead_given_name), ''),
        'family_name', NULLIF(TRIM(g.co_lead_family_name), ''),
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', NULLIF(TRIM(g.co_lead_institution), ''),
            'country', CAST(NULL AS STRING),
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) END as co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM arthritis_foundation_announcements_input g
CROSS JOIN src_funder f
WHERE g.excluded = '0'
  AND g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
  AND NULLIF(TRIM(g.title), '') IS NOT NULL
)
