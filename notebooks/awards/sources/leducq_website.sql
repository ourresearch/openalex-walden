-- Mapping from CreateLeducqAwards.ipynb (origin e22fdf7f): its awards-table SELECT over leducq_website_input (was the
-- staging table read from s3://openalex-ingest/awards/leducq/leducq_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320890  -- Fondation Leducq
),

people AS (
    SELECT
        funder_award_id,
        TRANSFORM(
            FILTER(
                from_json(people, 'array<struct<name:string,given_name:string,family_name:string,institution:string,country:string,role:string>>'),
                p -> p.family_name IS NOT NULL AND TRIM(p.family_name) != ''
            ),
            p -> named_struct(
                'given_name', NULLIF(TRIM(p.given_name), ''),
                'family_name', NULLIF(TRIM(p.family_name), ''),
                'orcid', CAST(NULL AS STRING),
                'role_start', CAST(NULL AS DATE),
                'affiliation', named_struct(
                    'name', NULLIF(TRIM(p.institution), ''),
                    'country', NULLIF(TRIM(p.country), ''),
                    'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
                )
            )
        ) AS investigators
    FROM leducq_website_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    NULLIF(TRIM(g.description), '') as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    TRY_CAST(g.amount AS DOUBLE) as amount,
    NULLIF(TRIM(g.currency), '') as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    CASE WHEN g.programme = 'Networks of Excellence' THEN 'research' ELSE 'infrastructure' END as funding_type,
    CASE WHEN g.programme = 'Networks of Excellence' THEN 'Transatlantic Networks of Excellence'
         ELSE g.programme END as funder_scheme,
    'leducq_website' as provenance,
    TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
    CAST(NULL AS DATE) as end_date,
    YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
    CAST(NULL AS INT) as end_year,
    try_element_at(p.investigators, 1) as lead_investigator,
    try_element_at(p.investigators, 2) as co_lead_investigator,
    CASE WHEN size(p.investigators) > 0 THEN p.investigators END as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM leducq_website_input g
JOIN people p ON p.funder_award_id = g.funder_award_id
CROSS JOIN src_funder f
WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
)
