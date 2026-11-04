-- Mapping from CreateLZPAwards.ipynb (origin e22fdf7f): its awards-table SELECT over lzp_projects_input (was the
-- staging table read from s3://openalex-ingest/awards/lzp/lzp_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320323269  -- Latvian Council of Science
),
g AS (
    SELECT
        TRIM(funder_award_id) AS funder_award_id,
        NULLIF(TRIM(title_lv), '') AS title,
        NULLIF(TRIM(summary_lv), '') AS summary,
        TRY_CAST(amount AS DOUBLE) AS amount,
        COALESCE(NULLIF(TRIM(currency), ''), 'EUR') AS currency,
        NULLIF(TRIM(categories), '') AS scheme,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS start_d,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS end_d,
        CASE WHEN pi_family_name IS NOT NULL THEN named_struct(
            'given_name', NULLIF(TRIM(pi_given_name), ''),
            'family_name', NULLIF(TRIM(pi_family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(institution), ''),
                'country', 'LV',
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        ) END AS lead,
        url
    FROM lzp_projects_input
    WHERE funder_award_id IS NOT NULL AND TRIM(funder_award_id) != ''
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,
    g.title as display_name,
    g.summary as description,
    f.funder_id,
    g.funder_award_id,
    g.amount,
    g.currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'research' as funding_type,
    g.scheme as funder_scheme,
    'lzp_projects' as provenance,
    g.start_d as start_date,
    g.end_d as end_date,
    YEAR(g.start_d) as start_year,
    YEAR(g.end_d) as end_year,
    g.lead as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CASE WHEN g.lead IS NOT NULL THEN array(g.lead) END as investigators,
    g.url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM g
CROSS JOIN src_funder f
)
