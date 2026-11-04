-- Mapping from CreateIADBScitechAwards.ipynb (origin e22fdf7f): its awards-table SELECT over iadb_project_search_scitech_input (was the
-- staging table read from s3://openalex-ingest/awards/iadb_scitech/iadb_scitech_projects.parquet); created_date and updated_date are the run timestamp.
WITH
iadb_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320307862
),

raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        CASE WHEN TRY_CAST(amount AS DOUBLE) IS NOT NULL THEN 'USD' ELSE NULL END AS parsed_currency,
        COALESCE(
            TRY_TO_DATE(approved_date, 'yyyy-MM-dd'),
            TRY_TO_DATE(signed_date, 'yyyy-MM-dd')
        ) AS parsed_start_date,
        TRY_CAST(source_year AS INT) AS parsed_start_year
    FROM iadb_project_search_scitech_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND display_name IS NOT NULL
      AND TRIM(display_name) != ''
),

mapped_awards AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000 as id,
        TRIM(g.display_name) as display_name,
        NULLIF(TRIM(g.description), '') as description,
        f.funder_id,
        g.native_award_id as funder_award_id,
        g.parsed_amount as amount,
        g.parsed_currency as currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,
        COALESCE(NULLIF(TRIM(g.funding_type), ''), 'research') as funding_type,
        COALESCE(NULLIF(TRIM(g.subsector_name), ''), NULLIF(TRIM(g.sector_name), ''), 'SCIENCE AND TECHNOLOGY') as funder_scheme,
        'iadb_project_search_scitech' as provenance,
        g.parsed_start_date as start_date,
        CAST(NULL AS DATE) as end_date,
        COALESCE(YEAR(g.parsed_start_date), g.parsed_start_year) as start_year,
        CAST(NULL AS INT) as end_year,
        CAST(NULL AS STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as lead_investigator,
        CAST(NULL AS STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as co_lead_investigator,
        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) as investigators,
        NULLIF(TRIM(g.landing_page_url), '') as landing_page_url,
        CAST(NULL AS STRING) as doi,
        CONCAT('https://api.openalex.org/works?filter=awards.id:G',
               CAST(abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000 AS STRING)) as works_api_url,
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date
    FROM raw_prepared g
    CROSS JOIN iadb_funder f
),

awards_transformed AS (
SELECT *
FROM mapped_awards
)
