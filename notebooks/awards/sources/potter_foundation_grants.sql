-- Mapping from CreatePotterFoundationAwards.ipynb (origin e22fdf7f): its awards-table SELECT over potter_foundation_grants_input (was the
-- staging table read from s3://openalex-ingest/awards/potter_foundation/potter_foundation_grants.parquet); created_date and updated_date are the run timestamp.
WITH
potter_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320314720
),

raw_typed AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        CASE WHEN TRY_CAST(amount AS DOUBLE) IS NOT NULL THEN currency ELSE NULL END AS parsed_currency,
        TRY_CAST(award_year AS INT) AS parsed_year,
        TRY_CAST(duration_months AS INT) AS parsed_duration_months,
        COALESCE(
            TRY_TO_DATE(award_date, 'yyyy-MM-dd'),
            CASE
                WHEN TRY_CAST(award_year AS INT) BETWEEN 1900 AND YEAR(current_date()) + 1
                THEN TRY_TO_DATE(CONCAT(award_year, '-01-01'), 'yyyy-MM-dd')
                ELSE NULL
            END
        ) AS parsed_start_date
    FROM potter_foundation_grants_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND display_name IS NOT NULL
      AND TRIM(display_name) != ''
),

raw_prepared AS (
    SELECT
        *,
        CASE
            WHEN parsed_start_date IS NOT NULL AND parsed_duration_months IS NOT NULL AND parsed_duration_months > 0
            THEN date_sub(add_months(parsed_start_date, parsed_duration_months), 1)
            ELSE NULL
        END AS parsed_end_date
    FROM raw_typed
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000 AS id,

        TRIM(g.display_name) AS display_name,

        CASE
            WHEN g.description IS NULL OR TRIM(g.description) = '' THEN NULL
            ELSE TRIM(g.description)
        END AS description,

        f.funder_id,
        g.native_award_id AS funder_award_id,

        g.parsed_amount AS amount,
        g.parsed_currency AS currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) AS id,
            f.display_name,
            f.ror_id,
            f.doi
        ) AS funder,

        CASE
            WHEN LOWER(TRIM(g.program_title)) = 'research' THEN 'research'
            ELSE 'grant'
        END AS funding_type,

        NULLIF(TRIM(g.program_title), '') AS funder_scheme,

        'potter_foundation_grants' AS provenance,

        CASE WHEN YEAR(g.parsed_start_date) > YEAR(current_date()) + 1 THEN NULL ELSE g.parsed_start_date END AS start_date,
        CASE WHEN YEAR(g.parsed_start_date) > YEAR(current_date()) + 1 THEN NULL ELSE g.parsed_end_date END AS end_date,
        CASE WHEN COALESCE(YEAR(g.parsed_start_date), g.parsed_year) > YEAR(current_date()) + 1 THEN NULL ELSE COALESCE(YEAR(g.parsed_start_date), g.parsed_year) END AS start_year,
        CASE WHEN COALESCE(YEAR(g.parsed_start_date), g.parsed_year) > YEAR(current_date()) + 1 THEN NULL ELSE YEAR(g.parsed_end_date) END AS end_year,

        struct(
            CAST(NULL AS STRING) AS given_name,
            CAST(NULL AS STRING) AS family_name,
            CAST(NULL AS STRING) AS orcid,
            g.parsed_start_date AS role_start,
            struct(
                NULLIF(TRIM(g.recipient_name), '') AS name,
                NULLIF(TRIM(g.recipient_country), '') AS country,
                CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
            ) AS affiliation
        ) AS lead_investigator,

        CAST(NULL AS STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) AS co_lead_investigator,

        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) AS investigators,

        NULLIF(TRIM(g.landing_page_url), '') AS landing_page_url,
        CAST(NULL AS STRING) AS doi,

        CONCAT('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000) AS works_api_url,

        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date

    FROM raw_prepared g
    CROSS JOIN potter_funder f
)
