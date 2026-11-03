-- Mapping from CreateCancerResearchSocietyAwards.ipynb (origin e22fdf7f): its awards-table SELECT over crs_operating_grant_results_input (was the
-- staging table read from s3://openalex-ingest/awards/cancer_research_society/cancer_research_society_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320311169  -- Cancer Research Society
),

g AS (
    SELECT
        *,
        TRY_CAST(competition_year AS INT) AS yr,
        NULLIF(TRIM(partner), '') AS partner_clean
    FROM crs_operating_grant_results_input
    WHERE funder_award_id IS NOT NULL AND TRIM(funder_award_id) != ''
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    CAST(NULL AS STRING) as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    TRY_CAST(g.amount AS DOUBLE) as amount,
    'CAD' as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'research' as funding_type,
    CASE WHEN g.partner_clean IS NOT NULL
         THEN CONCAT('Operating Grants (co-funded: ', g.partner_clean, ')')
         ELSE 'Operating Grants' END as funder_scheme,
    'crs_operating_grant_results' as provenance,
    CASE WHEN g.yr IS NOT NULL THEN TRY_TO_DATE(CONCAT(g.competition_year, '-01-01'), 'yyyy-MM-dd') END as start_date,
    CAST(NULL AS DATE) as end_date,
    g.yr as start_year,
    CAST(NULL AS INT) as end_year,
    CASE WHEN NULLIF(TRIM(g.lead_family_name), '') IS NOT NULL THEN named_struct(
        'given_name', NULLIF(TRIM(g.lead_given_name), ''),
        'family_name', NULLIF(TRIM(g.lead_family_name), ''),
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
    g.source_pdf as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM g
CROSS JOIN src_funder f
)
