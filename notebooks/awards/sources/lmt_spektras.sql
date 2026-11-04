-- Mapping from CreateLMTAwards.ipynb (origin e22fdf7f): its awards-table SELECT over lmt_spektras_input (was the
-- staging table read from s3://openalex-ingest/awards/lmt/lmt_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320322689  -- Lietuvos Mokslo Taryba
),

g AS (
    SELECT
        r.*,
        CASE WHEN NULLIF(TRIM(r.lead_family_name), '') IS NOT NULL THEN named_struct(
            'given_name', NULLIF(TRIM(r.lead_given_name), ''),
            'family_name', NULLIF(TRIM(r.lead_family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(r.institution), ''),
                'country', CAST(NULL AS STRING),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        ) END AS lead
    FROM lmt_spektras_input r
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(NULLIF(TRIM(g.title), ''), 'Duomenys neteikiami') as display_name,  -- 'data not provided' placeholder
    CAST(NULL AS STRING) as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    CASE WHEN TRY_CAST(g.amount_total AS DOUBLE) > 0 THEN TRY_CAST(g.amount_total AS DOUBLE) END as amount,
    CASE WHEN TRY_CAST(g.amount_total AS DOUBLE) > 0 THEN 'EUR' END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'research' as funding_type,
    NULLIF(TRIM(g.activity), '') as funder_scheme,
    'lmt_spektras' as provenance,
    CAST(NULL AS DATE) as start_date,
    CAST(NULL AS DATE) as end_date,
    TRY_CAST(g.start_year AS INT) as start_year,
    TRY_CAST(g.end_year AS INT) as end_year,
    g.lead as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
         affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CASE WHEN g.lead IS NOT NULL THEN array(g.lead) END as investigators,
    COALESCE(g.landing_page_url, 'https://spektras.lmt.lt/') as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM g
CROSS JOIN src_funder f
WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
)
