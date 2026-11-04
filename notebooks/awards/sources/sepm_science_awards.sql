-- Mapping from CreateSEPMAwards.ipynb (origin e22fdf7f): its awards-table SELECT over sepm_science_awards_input (was the
-- staging table read from s3://openalex-ingest/awards/sepm/sepm_science_awards.parquet); created_date and updated_date are the run timestamp.
WITH
sepm_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320312534
),

raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_CAST(award_year AS INT) AS parsed_year,
        CASE
            WHEN TRY_CAST(award_year AS INT) BETWEEN 1900 AND YEAR(current_date()) + 1
            THEN TRY_TO_DATE(CONCAT(award_year, '-01-01'), 'yyyy-MM-dd')
            ELSE NULL
        END AS parsed_start_date
    FROM sepm_science_awards_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND display_name IS NOT NULL
      AND TRIM(display_name) != ''
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

        CAST(NULL AS DOUBLE) AS amount,
        CAST(NULL AS STRING) AS currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) AS id,
            f.display_name,
            f.ror_id,
            f.doi
        ) AS funder,

        'prize' AS funding_type,

        NULLIF(TRIM(g.award_name), '') AS funder_scheme,

        'sepm_science_awards' AS provenance,

        g.parsed_start_date AS start_date,
        CAST(NULL AS DATE) AS end_date,
        g.parsed_year AS start_year,
        CAST(NULL AS INT) AS end_year,

        struct(
            NULLIF(TRIM(g.recipient_given_name), '') AS given_name,
            NULLIF(TRIM(g.recipient_family_name), '') AS family_name,
            CAST(NULL AS STRING) AS orcid,
            g.parsed_start_date AS role_start,
            struct(
                NULLIF(TRIM(g.affiliation_name), '') AS name,
                NULLIF(TRIM(g.affiliation_country), '') AS country,
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
    CROSS JOIN sepm_funder f
)
