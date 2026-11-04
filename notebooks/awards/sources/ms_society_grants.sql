-- Mapping from CreateMSSocietyAwards.ipynb (origin e22fdf7f): its awards-table SELECT over ms_society_grants_input (was the
-- staging table read from s3://openalex-ingest/awards/ms_society/ms_society_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320084  -- MS Society (UK)
),

people AS (
    SELECT
        funder_award_id,
        TRANSFORM(
            FILTER(
                from_json(people_json, 'ARRAY<STRUCT<given_name:STRING, family_name:STRING, title:STRING, orcid:STRING, institution:STRING, institution_ror:STRING>>'),
                p -> p.family_name IS NOT NULL
            ),
            p -> named_struct(
                'given_name', NULLIF(TRIM(p.given_name), ''),
                'family_name', NULLIF(TRIM(p.family_name), ''),
                'orcid', CASE WHEN p.orcid RLIKE '^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]$' THEN CONCAT('https://orcid.org/', p.orcid) END,
                'role_start', CAST(NULL AS DATE),
                'affiliation', named_struct(
                    'name', NULLIF(TRIM(p.institution), ''),
                    'country', CAST(NULL AS STRING),
                    'ids', CASE WHEN p.institution_ror IS NOT NULL
                                THEN array(named_struct('id', p.institution_ror, 'type', 'ror', 'asserted_by', 'funder'))
                                ELSE CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) END
                )
            )
        ) AS investigators
    FROM ms_society_grants_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    NULLIF(TRIM(g.abstract), '') as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    TRY_CAST(g.amount AS DOUBLE) as amount,
    CASE WHEN TRY_CAST(g.amount AS DOUBLE) IS NOT NULL THEN NULLIF(TRIM(g.currency), '') END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    g.funding_type as funding_type,
    NULLIF(TRIM(g.award_type), '') as funder_scheme,
    'ms_society_grants' as provenance,
    TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
    TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
    YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
    YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,
    try_element_at(p.investigators, 1) as lead_investigator,
    try_element_at(p.investigators, 2) as co_lead_investigator,
    CASE WHEN size(p.investigators) > 0 THEN p.investigators END as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM ms_society_grants_input g
JOIN people p ON p.funder_award_id = g.funder_award_id
CROSS JOIN src_funder f
WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
)
