-- Mapping from CreateDefraAwards.ipynb (origin e22fdf7f): its awards-table SELECT over defra_science_search_input (was the
-- staging table read from s3://openalex-ingest/awards/defra/defra_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320319995  -- Department for Environment, Food and Rural Affairs
),

g AS (
    SELECT
        r.*,
        TRY_TO_DATE(r.start_date, 'yyyy-MM-dd') AS sd,
        TRY_TO_DATE(r.end_date, 'yyyy-MM-dd') AS ed,
        NULLIF(TRIM(r.lead_contractor), '') AS contractor
    FROM defra_science_search_input r
    WHERE r.project_code IS NOT NULL AND TRIM(r.project_code) != ''
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.project_code))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    NULLIF(TRIM(g.description), '') as description,
    f.funder_id,
    TRIM(g.project_code) as funder_award_id,
    TRY_CAST(g.amount AS DOUBLE) as amount,
    CASE WHEN TRY_CAST(g.amount AS DOUBLE) IS NOT NULL THEN 'GBP' END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'research' as funding_type,
    CAST(NULL AS STRING) as funder_scheme,
    'defra_science_search' as provenance,
    g.sd as start_date,
    g.ed as end_date,
    YEAR(g.sd) as start_year,
    YEAR(g.ed) as end_year,
    CASE WHEN g.contractor IS NOT NULL THEN named_struct(
        'given_name', CAST(NULL AS STRING),
        'family_name', CAST(NULL AS STRING),
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', g.contractor,
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
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.project_code))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM g
CROSS JOIN src_funder f
)
