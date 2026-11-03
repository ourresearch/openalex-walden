-- Mapping from CreateAragonResearchGroupsAwards.ipynb (origin e22fdf7f): its awards-table SELECT over aragon_boa_research_groups_input (was the
-- staging table read from s3://openalex-ingest/awards/aragon_research_groups/aragon_research_groups_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320326208  -- Gobierno de Aragón
),
g AS (
    SELECT *,
        TRIM(funder_award_id) AS award_key,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS start_dt,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS end_dt
    FROM aragon_boa_research_groups_input
    WHERE funder_award_id IS NOT NULL AND TRIM(funder_award_id) != ''
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.award_key)))) % 9000000000 as id,
    NULLIF(TRIM(g.group_name), '') as display_name,
    CAST(NULL AS STRING) as description,
    f.funder_id,
    g.award_key as funder_award_id,
    TRY_CAST(g.amount AS DOUBLE) as amount,
    'EUR' as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'research' as funding_type,
    CONCAT('Grupos de investigación reconocidos por el Gobierno de Aragón ', g.period) as funder_scheme,
    'aragon_boa_research_groups' as provenance,
    g.start_dt as start_date,
    g.end_dt as end_date,
    YEAR(g.start_dt) as start_year,
    YEAR(g.end_dt) as end_year,
    CASE WHEN NULLIF(TRIM(g.pi_family_name), '') IS NOT NULL THEN named_struct(
        'given_name', NULLIF(TRIM(g.pi_given_name), ''),
        'family_name', NULLIF(TRIM(g.pi_family_name), ''),
        'orcid', CAST(NULL AS STRING),
        'role_start', g.start_dt,
        'affiliation', named_struct(
            'name', NULLIF(TRIM(g.institution), ''),
            'country', 'ES',
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) END as lead_investigator,
    CASE WHEN NULLIF(TRIM(g.co_pi_family_name), '') IS NOT NULL THEN named_struct(
        'given_name', NULLIF(TRIM(g.co_pi_given_name), ''),
        'family_name', NULLIF(TRIM(g.co_pi_family_name), ''),
        'orcid', CAST(NULL AS STRING),
        'role_start', g.start_dt,
        'affiliation', named_struct(
            'name', NULLIF(TRIM(g.institution), ''),
            'country', 'ES',
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) END as co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) as investigators,
    g.source_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.award_key)))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM g
CROSS JOIN src_funder f
)
