-- Mapping from CreateAACRAwards.ipynb (origin e22fdf7f): its awards-table SELECT over aacr_funded_research_input (was the
-- staging table read from s3://openalex-ingest/awards/aacr/aacr_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306091  -- American Association for Cancer Research
),

parsed AS (
    SELECT
        g.*,
        TRY_CAST(g.year AS INT) AS yr,
        TRANSFORM(
            FILTER(from_json(g.people, 'array<struct<name:string,given_name:string,family_name:string,institution:string,country:string>>'),
                   p -> NULLIF(TRIM(p.family_name), '') IS NOT NULL),
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
    FROM aacr_funded_research_input g
    WHERE g.excluded IS NULL  -- "*Awarded by Pancreatic Cancer Action Network, administered by AACR"
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    COALESCE(
        NULLIF(TRIM(g.project_title), ''),
        CONCAT_WS(' ', NULLIF(TRIM(COALESCE(g.program_variant, g.program)), ''), g.year) || ': ' ||
            CONCAT_WS(' ', NULLIF(TRIM(g.given_name), ''), NULLIF(TRIM(g.family_name), ''))
    ) as display_name,
    NULLIF(TRIM(g.research_summary), '') as description,
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
        WHEN g.category IN ('research-training-and-fellowships', 'Research Fellowships')
             OR LOWER(g.program) LIKE '%fellow%' THEN 'fellowship'
        ELSE 'research'
    END as funding_type,
    NULLIF(TRIM(COALESCE(g.program_variant, g.program)), '') as funder_scheme,
    'aacr_funded_research' as provenance,
    CASE WHEN g.yr IS NOT NULL THEN TRY_TO_DATE(CONCAT(g.yr, '-01-01'), 'yyyy-MM-dd') END as start_date,
    CAST(NULL AS DATE) as end_date,
    g.yr as start_year,
    CAST(NULL AS INT) as end_year,
    try_element_at(g.investigators, 1) as lead_investigator,   -- safe on empty arrays (§2.3)
    try_element_at(g.investigators, 2) as co_lead_investigator,
    CASE WHEN size(g.investigators) > 0 THEN g.investigators END as investigators,
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
