-- Mapping from CreateTWCFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over twcf_project_database_input (was the
-- staging table read from s3://openalex-ingest/awards/twcf/twcf_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320327239  -- Templeton World Charity Foundation
),

people AS (
    SELECT
        twcf_number,
        TRANSFORM(people, p -> named_struct(
            'given_name', NULLIF(TRIM(p.given_name), ''),
            'family_name', NULLIF(TRIM(p.family_name), ''),
            'orcid', CASE WHEN p.orcid RLIKE '^[0-9]{4}-' THEN CONCAT('https://orcid.org/', p.orcid) END,
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(p.institution), ''),
                'country', CAST(NULL AS STRING),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        )) AS investigators
    FROM twcf_project_database_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    NULLIF(TRIM(g.description), '') as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    g.amount as amount,
    g.currency as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'research' as funding_type,
    COALESCE(NULLIF(TRIM(g.priority), ''), NULLIF(TRIM(g.core_funding_area), '')) as funder_scheme,
    'twcf_project_database' as provenance,
    TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
    TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
    YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
    YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,
    try_element_at(p.investigators, 1) as lead_investigator,
    try_element_at(p.investigators, 2) as co_lead_investigator,
    CASE WHEN size(p.investigators) > 0 THEN p.investigators END as investigators,
    g.landing_page_url as landing_page_url,
    CASE WHEN g.grant_doi IS NOT NULL THEN CONCAT('https://doi.org/', LOWER(g.grant_doi)) END as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM twcf_project_database_input g
JOIN people p ON p.twcf_number = g.twcf_number
CROSS JOIN src_funder f
WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
)
