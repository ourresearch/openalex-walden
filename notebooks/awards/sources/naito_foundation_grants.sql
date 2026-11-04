-- Mapping from CreateNaitoFoundationAwards.ipynb (origin e22fdf7f): its awards-table SELECT over naito_foundation_grants_input (was the
-- staging table read from s3://openalex-ingest/awards/naito_foundation/naito_foundation_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320309785  -- Naito Foundation
),

g AS (
    SELECT
        r.*,
        TRY_CAST(r.year AS INT) AS yr,
        NULLIF(TRIM(r.lead_given_name), '') AS given,
        NULLIF(TRIM(r.lead_family_name), '') AS family
    FROM naito_foundation_grants_input r
    WHERE r.funder_award_id IS NOT NULL AND TRIM(r.funder_award_id) != ''
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    CAST(NULL AS STRING) as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    TRY_CAST(g.amount_jpy AS DOUBLE) as amount,
    CASE WHEN TRY_CAST(g.amount_jpy AS DOUBLE) IS NOT NULL THEN 'JPY' END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    NULLIF(TRIM(g.funding_type), '') as funding_type,
    NULLIF(TRIM(g.programme), '') as funder_scheme,
    'naito_foundation_grants' as provenance,
    CASE WHEN g.yr IS NOT NULL THEN TRY_TO_DATE(CONCAT(g.yr, '-01-01'), 'yyyy-MM-dd') END as start_date,
    CAST(NULL AS DATE) as end_date,
    g.yr as start_year,
    CAST(NULL AS INT) as end_year,
    CASE WHEN g.family IS NOT NULL THEN named_struct(
        'given_name', g.given,
        'family_name', g.family,
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', NULLIF(CONCAT_WS(' ', NULLIF(TRIM(g.institution), ''), NULLIF(TRIM(g.department), '')), ''),
            'country', CAST(NULL AS STRING),
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
FROM g
CROSS JOIN src_funder f
)
