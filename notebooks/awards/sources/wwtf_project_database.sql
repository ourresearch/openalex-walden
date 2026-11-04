-- Mapping from CreateWWTFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over wwtf_project_database_input (was the
-- staging table read from s3://openalex-ingest/awards/wwtf/wwtf_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321003  -- Vienna Science and Technology Fund
),

people AS (
    SELECT
        funder_award_id,
        TRANSFORM(
            from_json(people, 'ARRAY<STRUCT<role:STRING, name:STRING, given_name:STRING, family_name:STRING, institution:STRING>>'),
            p -> named_struct(
                'given_name', NULLIF(TRIM(p.given_name), ''),
                'family_name', NULLIF(TRIM(p.family_name), ''),
                'orcid', CAST(NULL AS STRING),
                'role_start', CAST(NULL AS DATE),
                'affiliation', named_struct(
                    'name', NULLIF(TRIM(p.institution), ''),
                    'country', CAST(NULL AS STRING),
                    'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
                )
            )
        ) AS investigators
    FROM wwtf_project_database_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    NULLIF(TRIM(g.description), '') as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    CASE WHEN g.funder_award_id = 'UIP25-BOKU' AND TRY_CAST(g.amount AS DOUBLE) > 10000000 THEN NULL
         ELSE TRY_CAST(g.amount AS DOUBLE) END as amount,
    CASE WHEN g.funder_award_id = 'UIP25-BOKU' AND TRY_CAST(g.amount AS DOUBLE) > 10000000 THEN NULL
         WHEN TRY_CAST(g.amount AS DOUBLE) IS NOT NULL THEN 'EUR' END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    CASE
        WHEN g.funder_award_id RLIKE '^UIP' THEN 'infrastructure'
        WHEN g.funder_award_id RLIKE '^(EI-)?VRG' THEN 'career_development'
        WHEN LOWER(g.call) LIKE '%fellowship%' THEN 'fellowship'
        WHEN LOWER(g.title) LIKE 'summer school%' OR g.funder_award_id RLIKE '^DCDH' THEN 'training'
        ELSE 'research'
    END as funding_type,
    NULLIF(TRIM(g.call), '') as funder_scheme,
    'wwtf_project_database' as provenance,
    TRY_TO_DATE(g.csv_start_date, 'yyyy-MM-dd') as start_date,
    TRY_TO_DATE(g.csv_end_date, 'yyyy-MM-dd') as end_date,
    YEAR(TRY_TO_DATE(g.csv_start_date, 'yyyy-MM-dd')) as start_year,
    YEAR(TRY_TO_DATE(g.csv_end_date, 'yyyy-MM-dd')) as end_year,
    try_element_at(p.investigators, 1) as lead_investigator,
    try_element_at(p.investigators, 2) as co_lead_investigator,
    CASE WHEN size(p.investigators) > 0 THEN p.investigators END as investigators,
    g.landing_page_url as landing_page_url,
    CASE WHEN g.grant_doi IS NOT NULL THEN CONCAT('https://doi.org/', LOWER(g.grant_doi)) END as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM wwtf_project_database_input g
JOIN people p ON p.funder_award_id = g.funder_award_id
CROSS JOIN src_funder f
WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
)
