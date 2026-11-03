-- Mapping from CreateEnergimyndighetenAwards.ipynb (origin e22fdf7f): its awards-table SELECT over energimyndigheten_input (was the
-- staging table read from s3://openalex-ingest/awards/energimyndigheten/energimyndigheten_projects.parquet); created_date and updated_date are the run timestamp.
WITH
em_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320322711
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(REGEXP_REPLACE(TRIM(g.project_id), '_[A-Za-z]+$', ''))))) % 9000000000 as id,
        COALESCE(NULLIF(TRIM(g.title_english), ''), NULLIF(TRIM(g.title), '')) as display_name,
        COALESCE(NULLIF(TRIM(g.abstract_english), ''), NULLIF(TRIM(g.abstract), '')) as description,
        f.funder_id,
        REGEXP_REPLACE(TRIM(g.project_id), '_[A-Za-z]+$', '') as funder_award_id,
        NULLIF(TRY_CAST(g.amount AS DOUBLE), 0) as amount,
        'SEK' as currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,
        CASE
            WHEN LOWER(COALESCE(g.type_of_award, '')) LIKE '%fellow%' THEN 'fellowship'
            WHEN LOWER(COALESCE(g.type_of_award, '')) LIKE '%stipend%' THEN 'fellowship'
            WHEN LOWER(COALESCE(g.type_of_award, '')) LIKE '%infrastructure%' THEN 'infrastructure'
            WHEN LOWER(COALESCE(g.type_of_award, '')) LIKE '%project%' THEN 'research'
            ELSE 'grant'
        END as funding_type,
        NULLIF(TRIM(g.type_of_award), '') as funder_scheme,
        'energimyndigheten' as provenance,
        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,
        CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as lead_investigator,
        CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
        CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) as investigators,
        CONCAT('https://www.vr.se/swecris#/project/', TRIM(g.project_id)) as landing_page_url,
        CAST(NULL AS STRING) as doi,
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(REGEXP_REPLACE(TRIM(g.project_id), '_[A-Za-z]+$', ''))))) % 9000000000) as works_api_url,
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date
    FROM energimyndigheten_input g
    CROSS JOIN em_funder f
    WHERE g.project_id IS NOT NULL AND TRIM(g.project_id) != ''
)
