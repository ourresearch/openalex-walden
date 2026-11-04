-- Mapping from CreateIMLSAwards.ipynb (origin e22fdf7f): its awards-table SELECT over imls_awarded_grants_input (was the
-- staging table read from s3://openalex-ingest/awards/imls/imls_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306122  -- Institute of Museum and Library Services
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.log_number))))) % 9000000000 as id,
    CONCAT(TRIM(g.program), ' — ', TRIM(g.institution), ' (FY', g.fiscal_year, ')') as display_name,
    NULLIF(TRIM(g.description), '') as description,
    f.funder_id,
    TRIM(g.log_number) as funder_award_id,
    CASE WHEN TRY_CAST(g.amount AS DOUBLE) > 0 THEN TRY_CAST(g.amount AS DOUBLE) END as amount,
    CASE WHEN TRY_CAST(g.amount AS DOUBLE) > 0 THEN 'USD' END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    g.funding_type as funding_type,
    NULLIF(TRIM(g.program), '') as funder_scheme,
    'imls_awarded_grants' as provenance,
    TRY_TO_DATE(CONCAT(g.fiscal_year, '-01-01'), 'yyyy-MM-dd') as start_date,
    CAST(NULL AS DATE) as end_date,
    TRY_CAST(g.fiscal_year AS INT) as start_year,
    CAST(NULL AS INT) as end_year,
    CASE WHEN NULLIF(TRIM(g.institution), '') IS NOT NULL THEN named_struct(
        'given_name', CAST(NULL AS STRING),
        'family_name', CAST(NULL AS STRING),
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', TRIM(g.institution),
            'country', CASE WHEN g.state IN ('FM', 'MH', 'PW') THEN g.state
                            WHEN g.state IS NOT NULL THEN 'US' END,
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) END as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.log_number))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM imls_awarded_grants_input g
CROSS JOIN src_funder f
WHERE g.log_number IS NOT NULL AND TRIM(g.log_number) != ''
)
