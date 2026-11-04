-- Mapping from CreateOntarioResearchFundingAwards.ipynb (origin e22fdf7f): its awards-table SELECT over ontario_research_funding_summary_input (was the
-- staging table read from s3://openalex-ingest/awards/ontario_research_funding/ontario_research_funding_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320314607  -- Government of Ontario
),
g AS (
    SELECT *,
        TRIM(funder_award_id) AS award_key,
        TRY_TO_DATE(approval_date, 'yyyy-MM-dd') AS start_dt
    FROM ontario_research_funding_summary_input
    WHERE funder_award_id IS NOT NULL AND TRIM(funder_award_id) != ''
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.award_key)))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    NULLIF(TRIM(g.description), '') as description,
    f.funder_id,
    g.award_key as funder_award_id,
    TRY_CAST(g.amount AS DOUBLE) as amount,
    'CAD' as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    CASE WHEN g.program IN ('PDF') THEN 'fellowship'
         WHEN g.program IN ('PDA') THEN 'prize'
         ELSE 'research' END as funding_type,
    CASE g.program
        WHEN 'PDF' THEN 'Post-Doctoral Fellowship Program'
        WHEN 'ISOP' THEN 'International Strategic Opportunities Program'
        WHEN 'PDA' THEN 'Premier''s Discovery Awards'
        ELSE g.program END as funder_scheme,
    'ontario_research_funding_summary' as provenance,
    g.start_dt as start_date,
    CAST(NULL AS DATE) as end_date,
    YEAR(g.start_dt) as start_year,
    CAST(NULL AS INT) as end_year,
    CASE WHEN NULLIF(TRIM(g.family_name), '') IS NOT NULL THEN named_struct(
        'given_name', NULLIF(TRIM(g.given_name), ''),
        'family_name', NULLIF(TRIM(g.family_name), ''),
        'orcid', CAST(NULL AS STRING),
        'role_start', g.start_dt,
        'affiliation', named_struct(
            'name', NULLIF(TRIM(g.institution), ''),
            'country', 'CA',
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) END as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) as investigators,
    CASE WHEN g.source_file LIKE '%historical%'
         THEN 'https://data.ontario.ca/dataset/ontario-research-funding-summary-historical'
         ELSE 'https://data.ontario.ca/dataset/ontario-research-funding-summary-current' END as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.award_key)))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM g
CROSS JOIN src_funder f
)
