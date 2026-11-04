-- Mapping from CreateIrishResearchCouncilAwards.ipynb (origin e22fdf7f): its awards-table SELECT over irc_awardees_database_input (was the
-- staging table read from s3://openalex-ingest/awards/irish_research_council/irish_research_council_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321056  -- Irish Research Council
),

irc_era AS (
    SELECT * FROM irc_awardees_database_input
    WHERE TRY_CAST(year AS INT) BETWEEN 2012 AND 2024
      AND funder_award_id IS NOT NULL AND TRIM(funder_award_id) != ''
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    CAST(NULL AS STRING) as description,
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
    CASE LOWER(TRIM(g.award_category))
        WHEN 'early career' THEN 'fellowship'
        WHEN 'principal investigator-led' THEN 'research'
        ELSE 'grant'
    END as funding_type,
    NULLIF(TRIM(g.award), '') as funder_scheme,
    'irc_awardees_database' as provenance,
    TRY_TO_DATE(CONCAT(g.year, '-01-01'), 'yyyy-MM-dd') as start_date,
    CAST(NULL AS DATE) as end_date,
    TRY_CAST(g.year AS INT) as start_year,
    CAST(NULL AS INT) as end_year,
    named_struct(
        'given_name', NULLIF(TRIM(g.given_name), ''),
        'family_name', NULLIF(TRIM(g.family_name), ''),
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', NULLIF(TRIM(g.institution), ''),
            'country', 'IE',
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
         affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>)
        as co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
         affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>)
        as investigators,
    'https://research.ie/awardees/' as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM irc_era g
CROSS JOIN src_funder f
)
