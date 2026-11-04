-- Mapping from CreateNordForskAwards.ipynb (origin e22fdf7f): its awards-table SELECT over nordforsk_projects_input (was the
-- staging table read from s3://openalex-ingest/awards/nordforsk/nordforsk_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320322885  -- NordForsk
),

people AS (
    SELECT
        project_number,
        TRANSFORM(
            COALESCE(from_json(people_json, 'ARRAY<STRUCT<name:STRING, given_name:STRING, family_name:STRING, institution:STRING>>'), ARRAY()),
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
    FROM nordforsk_projects_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    NULLIF(TRIM(g.description), '') as description,
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
    'research' as funding_type,
    COALESCE(NULLIF(TRIM(g.programme), ''), NULLIF(TRIM(g.call), '')) as funder_scheme,
    'nordforsk_projects' as provenance,
    TRY_TO_DATE(CONCAT(TRY_CAST(g.start_year AS INT), '-01-01'), 'yyyy-MM-dd') as start_date,
    TRY_TO_DATE(CONCAT(TRY_CAST(g.end_year AS INT), '-12-31'), 'yyyy-MM-dd') as end_date,
    TRY_CAST(g.start_year AS INT) as start_year,
    TRY_CAST(g.end_year AS INT) as end_year,
    try_element_at(p.investigators, 1) as lead_investigator,
    try_element_at(p.investigators, 2) as co_lead_investigator,
    CASE WHEN size(p.investigators) > 0 THEN p.investigators END as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM nordforsk_projects_input g
JOIN people p ON p.project_number = g.project_number
CROSS JOIN src_funder f
WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
)
