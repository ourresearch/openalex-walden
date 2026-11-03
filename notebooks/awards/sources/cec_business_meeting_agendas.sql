-- Mapping from CreateCECAwards.ipynb (origin e22fdf7f): its awards-table SELECT over cec_business_meeting_agendas_input (was the
-- staging table read from s3://openalex-ingest/awards/cec/cec_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320308020  -- California Energy Commission
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    NULLIF(TRIM(g.description), '') as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    NULLIF(TRY_CAST(g.amount AS DOUBLE), 0) as amount,
    CASE WHEN NULLIF(TRY_CAST(g.amount AS DOUBLE), 0) IS NOT NULL THEN 'USD' END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'grant' as funding_type,
    NULLIF(TRIM(g.programme), '') as funder_scheme,  -- from the agreement-number prefix
    'cec_business_meeting_agendas' as provenance,
    TRY_TO_DATE(g.meeting_date, 'yyyy-MM-dd') as start_date,
    CAST(NULL AS DATE) as end_date,
    YEAR(TRY_TO_DATE(g.meeting_date, 'yyyy-MM-dd')) as start_year,
    CAST(NULL AS INT) as end_year,
    CASE WHEN NULLIF(TRIM(g.recipient), '') IS NOT NULL THEN named_struct(
        'given_name', CAST(NULL AS STRING),
        'family_name', CAST(NULL AS STRING),
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', TRIM(g.recipient),
            'country', 'US',
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) END as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM cec_business_meeting_agendas_input g
CROSS JOIN src_funder f
WHERE g.in_scope = 'True'
  AND g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
)
