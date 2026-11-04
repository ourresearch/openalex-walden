-- Mapping from CreateTakedaScienceFoundationAwards.ipynb (origin e22fdf7f): its awards-table SELECT over takeda_bopdb_input (was the
-- staging table read from s3://openalex-ingest/awards/takeda_science_foundation/takeda_science_foundation_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320309807  -- Takeda Science Foundation
),

g AS (
    SELECT
        r.*,
        TRY_CAST(r.fiscal_year AS INT) AS fy,
        NULLIF(TRIM(r.lead_given_name), '') AS given,
        NULLIF(TRIM(r.lead_family_name), '') AS family
    FROM takeda_bopdb_input r
    WHERE r.grant_no IS NOT NULL AND TRIM(r.grant_no) != ''
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.grant_no))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    NULLIF(TRIM(g.result_summary), '') as description,
    f.funder_id,
    TRIM(g.grant_no) as funder_award_id,
    CAST(NULL AS DOUBLE) as amount,
    CAST(NULL AS STRING) as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    CASE WHEN g.program_code IN ('11', '12', '19') THEN 'grant' ELSE 'research' END as funding_type,
    NULLIF(TRIM(g.program), '') as funder_scheme,
    'takeda_bopdb' as provenance,
    CASE WHEN g.fy IS NOT NULL THEN TRY_TO_DATE(CONCAT(g.fy, '-01-01'), 'yyyy-MM-dd') END as start_date,
    CAST(NULL AS DATE) as end_date,
    g.fy as start_year,
    CAST(NULL AS INT) as end_year,
    CASE WHEN g.family IS NOT NULL THEN named_struct(
        'given_name', g.given,
        'family_name', g.family,
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', CONCAT_WS(' ', NULLIF(TRIM(g.institution), ''), NULLIF(TRIM(g.department), '')),
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
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.grant_no))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM g
CROSS JOIN src_funder f
)
