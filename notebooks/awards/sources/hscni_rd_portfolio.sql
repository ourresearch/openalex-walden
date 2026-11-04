-- Mapping from CreateHSCNIPHAAwards.ipynb (origin e22fdf7f): its awards-table SELECT over hscni_rd_portfolio_input (was the
-- staging table read from s3://openalex-ingest/awards/hscni_pha/hscni_pha_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320853  -- Public Health Agency (Northern Ireland)
),

g AS (
    SELECT
        r.*,
        NULLIF(TRIM(r.lead_given_name), '') AS given,
        NULLIF(TRIM(r.lead_family_name), '') AS family,
        CASE WHEN UPPER(TRIM(r.title)) IN ('N/A', 'TBC', 'TBA', '') THEN NULL ELSE TRIM(r.title) END AS clean_title,
        TRY_CAST(r.start_year AS INT) AS sy,
        TRY_CAST(r.end_year AS INT) AS ey
    FROM hscni_rd_portfolio_input r
    WHERE r.funder_award_id IS NOT NULL AND TRIM(r.funder_award_id) != ''
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    COALESCE(g.clean_title,
             CONCAT_WS(' - ', NULLIF(TRIM(g.scheme), ''), CONCAT_WS(' ', g.given, g.family))) as display_name,
    NULLIF(TRIM(g.description), '') as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    TRY_CAST(g.amount AS DOUBLE) as amount,
    CASE WHEN TRY_CAST(g.amount AS DOUBLE) IS NOT NULL THEN 'GBP' END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    CASE WHEN g.page_code IN ('FEL', 'COC', 'GPA') OR LOWER(g.scheme) LIKE '%fellowship%' THEN 'fellowship'
         ELSE 'research' END as funding_type,
    NULLIF(TRIM(g.scheme), '') as funder_scheme,
    'hscni_rd_portfolio' as provenance,
    COALESCE(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd'), make_date(g.sy, 1, 1)) as start_date,
    COALESCE(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd'), make_date(g.ey, 12, 31)) as end_date,
    COALESCE(YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')), g.sy) as start_year,
    COALESCE(YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')), g.ey) as end_year,
    CASE WHEN g.family IS NOT NULL THEN named_struct(
        'given_name', g.given,
        'family_name', g.family,
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', NULLIF(TRIM(g.pi_affiliation), ''),
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
