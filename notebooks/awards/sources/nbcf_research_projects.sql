-- Mapping from CreateNBCFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over nbcf_research_projects_input (was the
-- staging table read from s3://openalex-ingest/awards/nbcf/nbcf_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320438  -- National Breast Cancer Foundation
),

g AS (
    SELECT
        *,
        TRY_CAST(award_year AS INT) AS yr,
        NULLIF(TRIM(lead_family_name), '') AS family
    FROM nbcf_research_projects_input
    WHERE funder_award_id IS NOT NULL AND TRIM(funder_award_id) != ''
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
    CASE WHEN g.grant_type = 'other' THEN 'grant' ELSE 'research' END as funding_type,
    CASE g.grant_type
        WHEN 'research-project-grant' THEN 'Research Project Grant'
        WHEN 'pink-sky-grants' THEN 'Pink Sky Grant'
        WHEN 'collaborative-research-accelerator-grant' THEN 'Collaborative Research Accelerator Grant'
        WHEN 'other' THEN 'Other'
        ELSE NULLIF(TRIM(g.grant_type), '')
    END as funder_scheme,
    'nbcf_research_projects' as provenance,
    CASE WHEN g.yr IS NOT NULL THEN TRY_TO_DATE(CONCAT(g.award_year, '-01-01'), 'yyyy-MM-dd') END as start_date,
    CAST(NULL AS DATE) as end_date,
    g.yr as start_year,
    CAST(NULL AS INT) as end_year,
    CASE WHEN g.family IS NOT NULL THEN named_struct(
        'given_name', NULLIF(TRIM(g.lead_given_name), ''),
        'family_name', g.family,
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', NULLIF(TRIM(g.institution), ''),
            'country', CAST(NULL AS STRING),
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) END as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM g
CROSS JOIN src_funder f
)
