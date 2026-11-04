-- Mapping from CreateHRSAAwards.ipynb (origin e22fdf7f): its awards-table SELECT over hrsa_data_warehouse_input (was the
-- staging table read from s3://openalex-ingest/awards/hrsa/hrsa_projects.parquet); created_date and updated_date are the run timestamp.
WITH
hrsa_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320332175
),

raw_prepared AS (
    SELECT
        *,
        CONCAT_WS(':', LOWER(TRIM(grant_number)), TRIM(award_year), source_row_hash) AS row_level_award_id,
        TRY_CAST(REGEXP_REPLACE(financial_assistance, '[^0-9.-]', '') AS DOUBLE) AS parsed_amount,
        TRY_TO_DATE(project_period_start_date, 'MM/dd/yyyy') AS parsed_start_date,
        TRY_TO_DATE(grant_project_period_end_date, 'MM/dd/yyyy') AS parsed_end_date
    FROM hrsa_data_warehouse_input
    WHERE grant_number IS NOT NULL
      AND TRIM(grant_number) != ''
      AND award_year IS NOT NULL
      AND TRIM(award_year) != ''
      AND source_row_hash IS NOT NULL
      AND TRIM(source_row_hash) != ''
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', g.row_level_award_id))) % 9000000000 as id,

        COALESCE(NULLIF(TRIM(g.grant_program_name), ''), NULLIF(TRIM(g.hrsa_program_area_name), ''), g.grant_number) as display_name,

        CASE
            WHEN g.abstract IS NULL OR TRIM(g.abstract) = '' OR LOWER(TRIM(g.abstract)) = 'no link' THEN NULL
            ELSE g.abstract
        END as description,

        f.funder_id,
        g.row_level_award_id as funder_award_id,

        g.parsed_amount as amount,
        'USD' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(COALESCE(g.grant_program_name, g.hrsa_program_area_name, '')) RLIKE 'training|traineeship|residency|workforce|scholarship|fellowship' THEN 'training'
            WHEN LOWER(COALESCE(g.grant_program_name, g.hrsa_program_area_name, '')) RLIKE 'cooperative' THEN 'grant'
            ELSE 'grant'
        END as funding_type,

        COALESCE(NULLIF(TRIM(g.grant_program_name), ''), NULLIF(TRIM(g.hrsa_program_area_name), '')) as funder_scheme,

        'hrsa_data_warehouse' as provenance,

        g.parsed_start_date as start_date,
        g.parsed_end_date as end_date,
        YEAR(g.parsed_start_date) as start_year,
        YEAR(g.parsed_end_date) as end_year,

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

        CAST(NULL AS STRING) as landing_page_url,
        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', g.row_level_award_id))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM raw_prepared g
    CROSS JOIN hrsa_funder f
)
