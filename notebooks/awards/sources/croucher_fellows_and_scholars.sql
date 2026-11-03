-- Mapping from CreateCroucherFoundationAwards.ipynb (origin e22fdf7f): its awards-table SELECT over croucher_fellows_and_scholars_input (was the
-- staging table read from s3://openalex-ingest/awards/croucher_foundation/croucher_foundation_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320904  -- Croucher Foundation
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    CONCAT(g.funder_scheme, ' ', g.award_year, ': ', TRIM(g.name)) as display_name,
    CAST(NULL AS STRING) as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    CAST(NULL AS DOUBLE) as amount,      -- not published per award
    CAST(NULL AS STRING) as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    g.funding_type as funding_type,
    g.funder_scheme as funder_scheme,
    'croucher_fellows_and_scholars' as provenance,
    CAST(NULL AS DATE) as start_date,
    CAST(NULL AS DATE) as end_date,
    TRY_CAST(g.award_year AS INT) as start_year,
    CAST(NULL AS INT) as end_year,
    CASE WHEN NULLIF(TRIM(g.family_name), '') IS NOT NULL THEN
        named_struct(
            'given_name', NULLIF(TRIM(g.given_name), ''),
            'family_name', NULLIF(TRIM(g.family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(g.institution), ''),
                'country', CAST(NULL AS STRING),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        )
    END as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
         affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CASE WHEN NULLIF(TRIM(g.family_name), '') IS NOT NULL THEN array(
        named_struct(
            'given_name', NULLIF(TRIM(g.given_name), ''),
            'family_name', NULLIF(TRIM(g.family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(g.institution), ''),
                'country', CAST(NULL AS STRING),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        )
    ) END as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM croucher_fellows_and_scholars_input g
CROSS JOIN src_funder f
WHERE NULLIF(TRIM(g.funder_award_id), '') IS NOT NULL
)
