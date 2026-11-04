-- Mapping from CreateRuffordAwards.ipynb (origin e22fdf7f): its awards-table SELECT over rufford_project_directory_input (was the
-- staging table read from s3://openalex-ingest/awards/rufford/rufford_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320309818  -- Rufford Foundation
),

lead AS (
    SELECT
        funder_award_id,
        CASE WHEN NULLIF(TRIM(lead_family_name), '') IS NOT NULL THEN named_struct(
            'given_name', NULLIF(TRIM(lead_given_name), ''),
            'family_name', TRIM(lead_family_name),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', CAST(NULL AS STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>)
        ) END AS lead_investigator
    FROM rufford_project_directory_input
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
    'grant' as funding_type,
    'Rufford Small Grants for Nature Conservation' as funder_scheme,
    'rufford_project_directory' as provenance,
    TRY_TO_DATE(g.award_date, 'yyyy-MM-dd') as start_date,
    CAST(NULL AS DATE) as end_date,
    YEAR(TRY_TO_DATE(g.award_date, 'yyyy-MM-dd')) as start_year,
    CAST(NULL AS INT) as end_year,
    l.lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CASE WHEN l.lead_investigator IS NOT NULL THEN array(l.lead_investigator) END as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM rufford_project_directory_input g
JOIN lead l ON l.funder_award_id = g.funder_award_id
CROSS JOIN src_funder f
WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
)
