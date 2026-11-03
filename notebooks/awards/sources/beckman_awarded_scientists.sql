-- Mapping from CreateBeckmanFoundationAwards.ipynb (origin e22fdf7f): its awards-table SELECT over beckman_awarded_scientists_input (was the
-- staging table read from s3://openalex-ingest/awards/beckman_foundation/beckman_foundation_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306253  -- Arnold and Mabel Beckman Foundation
),

parsed AS (
    SELECT
        g.*,
        TRY_CAST(g.award_year AS INT) AS yr,
        CASE WHEN NULLIF(TRIM(g.family_name), '') IS NOT NULL THEN named_struct(
            'given_name', NULLIF(TRIM(g.given_name), ''),
            'family_name', NULLIF(TRIM(g.family_name), ''),
            'orcid', CASE WHEN g.orcid RLIKE '^[0-9]{4}-' THEN CONCAT('https://orcid.org/', g.orcid) END,
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(g.institution_at_award), ''),
                'country', CAST(NULL AS STRING),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        ) END AS lead
    FROM beckman_awarded_scientists_input g
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    COALESCE(
        NULLIF(TRIM(g.research_title), ''),
        CONCAT_WS(' ', NULLIF(TRIM(g.program), ''), g.award_year) || ': ' ||
            CONCAT_WS(' ', NULLIF(TRIM(g.given_name), ''), NULLIF(TRIM(g.family_name), ''))
    ) as display_name,
    CAST(NULL AS STRING) as description,
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
    CASE
        WHEN LOWER(g.program) LIKE '%fellow%' THEN 'fellowship'
        WHEN LOWER(g.program) LIKE '%scholar%' THEN 'training'
        ELSE 'research'
    END as funding_type,
    NULLIF(TRIM(g.program), '') as funder_scheme,
    'beckman_awarded_scientists' as provenance,
    CASE WHEN g.yr IS NOT NULL THEN TRY_TO_DATE(CONCAT(g.award_year, '-01-01'), 'yyyy-MM-dd') END as start_date,
    CAST(NULL AS DATE) as end_date,
    g.yr as start_year,
    CAST(NULL AS INT) as end_year,
    g.lead as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CASE WHEN g.lead IS NOT NULL THEN array(g.lead) END as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM parsed g
CROSS JOIN src_funder f
WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
)
