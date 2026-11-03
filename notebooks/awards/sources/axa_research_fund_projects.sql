-- Mapping from CreateAXAResearchFundAwards.ipynb (origin e22fdf7f): its awards-table SELECT over axa_research_fund_projects_input (was the
-- staging table read from s3://openalex-ingest/awards/axa_research_fund/axa_research_fund_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321048  -- AXA Research Fund
),

people AS (
    SELECT
        funder_award_id,
        TRANSFORM(
            FILTER(
                from_json(people, 'array<struct<given_name:string,family_name:string,orcid:string,institution:string,department:string,country:string>>'),
                p -> p.family_name IS NOT NULL AND TRIM(p.family_name) != ''
            ),
            p -> named_struct(
                'given_name', NULLIF(TRIM(p.given_name), ''),
                'family_name', NULLIF(TRIM(p.family_name), ''),
                'orcid', CASE WHEN p.orcid RLIKE '^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]$'
                              THEN CONCAT('https://orcid.org/', p.orcid) END,
                'role_start', CAST(NULL AS DATE),
                'affiliation', named_struct(
                    'name', NULLIF(TRIM(p.institution), ''),
                    'country', NULLIF(TRIM(p.country), ''),
                    'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
                )
            )
        ) AS investigators
    FROM axa_research_fund_projects_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    COALESCE(NULLIF(TRIM(g.description), ''), NULLIF(TRIM(g.excerpt), '')) as description,
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
        WHEN lower(g.grant_type) LIKE 'post-doctoral%' THEN 'fellowship'
        WHEN lower(g.grant_type) IN ('ph.d', 'phd', 'ph.d.') THEN 'studentship'
        WHEN lower(g.grant_type) LIKE '%m_c_nat%' THEN 'grant'
        ELSE 'research'
    END as funding_type,
    NULLIF(TRIM(g.grant_type), '') as funder_scheme,
    'axa_research_fund_projects' as provenance,
    COALESCE(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd'), TRY_TO_DATE(SUBSTR(g.start_date, 1, 10), 'yyyy-MM-dd')) as start_date,
    CAST(NULL AS DATE) as end_date,
    YEAR(COALESCE(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd'), TRY_TO_DATE(SUBSTR(g.start_date, 1, 10), 'yyyy-MM-dd'))) as start_year,
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
FROM axa_research_fund_projects_input g
JOIN people p ON p.funder_award_id = g.funder_award_id
CROSS JOIN src_funder f
WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
)
