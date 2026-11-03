-- Mapping from CreateAASARISEAwards.ipynb (origin e22fdf7f): its awards-table SELECT over aas_arise_grantees_input (was the
-- staging table read from s3://openalex-ingest/awards/aas_arise/aas_arise_grantees.parquet); created_date and updated_date are the run timestamp.
WITH
aas_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320327323
),

raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS parsed_start_date,
        TRY_CAST(start_year AS INT) AS parsed_start_year
    FROM aas_arise_grantees_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND display_name IS NOT NULL
      AND TRIM(display_name) != ''
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000 as id,

        TRIM(g.display_name) as display_name,

        CASE
            WHEN g.description IS NULL OR TRIM(g.description) = '' THEN NULL
            ELSE TRIM(g.description)
        END as description,

        f.funder_id,
        g.native_award_id as funder_award_id,

        CAST(NULL AS DOUBLE) as amount,
        CAST(NULL AS STRING) as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        'grant' as funding_type,

        NULLIF(TRIM(g.funder_scheme), '') as funder_scheme,

        'aas_arise_grantees' as provenance,

        g.parsed_start_date as start_date,
        CAST(NULL AS DATE) as end_date,
        COALESCE(YEAR(g.parsed_start_date), g.parsed_start_year) as start_year,
        CAST(NULL AS INT) as end_year,

        struct(
            NULLIF(TRIM(g.lead_investigator_given_name), '') as given_name,
            NULLIF(TRIM(g.lead_investigator_family_name), '') as family_name,
            CAST(NULL AS STRING) as orcid,
            g.parsed_start_date as role_start,
            struct(
                NULLIF(TRIM(g.institution), '') as name,
                NULLIF(TRIM(g.country), '') as country,
                CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
            ) as affiliation
        ) as lead_investigator,

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

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM raw_prepared g
    CROSS JOIN aas_funder f
)
