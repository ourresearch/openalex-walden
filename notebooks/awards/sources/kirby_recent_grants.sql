-- Mapping from CreateKirbyAwards.ipynb (origin e22fdf7f): its awards-table SELECT over kirby_recent_grants_input (was the
-- staging table read from s3://openalex-ingest/awards/kirby/kirby_recent_grants.parquet); created_date and updated_date are the run timestamp.
WITH
kirby_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320314616
),

raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        CASE WHEN TRY_CAST(amount AS DOUBLE) IS NOT NULL THEN 'AUD' ELSE NULL END AS parsed_currency,
        TRY_CAST(source_year AS INT) AS parsed_year,
        CASE
            WHEN TRY_CAST(source_year AS INT) IS NOT NULL
            THEN TRY_TO_DATE(CONCAT(source_year, '-01-01'), 'yyyy-MM-dd')
            ELSE NULL
        END AS parsed_start_date
    FROM kirby_recent_grants_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND display_name IS NOT NULL
      AND TRIM(display_name) != ''
),

mapped_awards AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000 as id,

        TRIM(g.display_name) as display_name,

        CASE
            WHEN g.description IS NULL OR TRIM(g.description) = '' THEN NULL
            ELSE TRIM(g.description)
        END as description,

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

        'grant' as funding_type,

        NULLIF(TRIM(g.funder_scheme), '') as funder_scheme,

        'kirby_recent_grants' as provenance,

        g.parsed_start_date as start_date,
        CAST(NULL AS DATE) as end_date,
        CASE WHEN g.parsed_year > YEAR(current_date()) + 1 THEN NULL ELSE g.parsed_year END as start_year,
        CAST(NULL AS INT) as end_year,

        struct(
            CAST(NULL AS STRING) as given_name,
            CAST(NULL AS STRING) as family_name,
            CAST(NULL AS STRING) as orcid,
            g.parsed_start_date as role_start,
            struct(
                NULLIF(TRIM(g.recipient_name), '') as name,
                CAST(NULL AS STRING) as country,
                CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
            ) as affiliation
        ) as lead_investigator,

        CAST(NULL AS STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING,country:STRING,ids:ARRAY<STRUCT<id:STRING,type:STRING,asserted_by:STRING>>>
        >) as co_lead_investigator,

        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING,country:STRING,ids:ARRAY<STRUCT<id:STRING,type:STRING,asserted_by:STRING>>>
        >>) as investigators,

        NULLIF(TRIM(g.landing_page_url), '') as landing_page_url,
        CAST(NULL AS STRING) as doi,
        CONCAT('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000) as works_api_url,
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM raw_prepared g
    CROSS JOIN kirby_funder f
),

awards_transformed AS (
SELECT *
FROM mapped_awards
)
