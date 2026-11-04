-- Mapping from CreateGaussCentreAwards.ipynb (origin e22fdf7f): its awards-table SELECT over gauss_centre_large_scale_input (was the
-- staging table read from s3://openalex-ingest/awards/gauss_centre/gauss_centre_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320331625  -- Gauss Centre for Supercomputing
),

g AS (
    SELECT r.*, TRIM(r.funder_award_id) AS award_id
    FROM gauss_centre_large_scale_input r
    WHERE r.funder_award_id IS NOT NULL AND TRIM(r.funder_award_id) != ''
),

lead AS (
    SELECT
        award_id,
        CASE WHEN NULLIF(TRIM(lead_family_name), '') IS NOT NULL THEN
        named_struct(
            'given_name', NULLIF(TRIM(lead_given_name), ''),
            'family_name', NULLIF(TRIM(lead_family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(institution), ''),
                'country', CAST(NULL AS STRING),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        ) END AS lead_investigator
    FROM g
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.award_id)))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    CONCAT_WS(' ',
        'In-kind allocation of supercomputing time (GCS Large-Scale Project' || chr(59) || ' no monetary award).',
        CONCAT(g.allocation_text, '.'),
        CASE WHEN NULLIF(TRIM(g.project_ids_all), '') IS NOT NULL THEN CONCAT('Project id(s): ', TRIM(g.project_ids_all), '.') END
    ) as description,
    f.funder_id,
    g.award_id as funder_award_id,
    CAST(NULL AS DOUBLE) as amount,
    CAST(NULL AS STRING) as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'grant' as funding_type,
    'GCS Large-Scale Projects' as funder_scheme,
    'gauss_centre_large_scale' as provenance,
    TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
    TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
    TRY_CAST(g.start_year AS INT) as start_year,
    TRY_CAST(g.end_year AS INT) as end_year,
    l.lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CASE WHEN l.lead_investigator IS NOT NULL THEN array(l.lead_investigator) END as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.award_id)))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM g
JOIN lead l ON l.award_id = g.award_id
CROSS JOIN src_funder f
)
