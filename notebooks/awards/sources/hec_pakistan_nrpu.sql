-- Mapping from CreateHECPakistanAwards.ipynb (origin e22fdf7f): its awards-table SELECT over hec_pakistan_nrpu_input (was the
-- staging table read from s3://openalex-ingest/awards/hec_pakistan/hec_pakistan_nrpu.parquet); created_date and updated_date are the run timestamp.
WITH
hec_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320322799
),

raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        NULLIF(TRIM(currency), '') AS parsed_currency,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS parsed_end_date,
        TRY_CAST(source_year AS INT) AS parsed_year
    FROM hec_pakistan_nrpu_input
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

        g.parsed_amount AS amount,
        g.parsed_currency AS currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) AS id,
            f.display_name,
            f.ror_id,
            f.doi
        ) AS funder,

        'grant' AS funding_type,

        COALESCE(NULLIF(TRIM(g.funder_scheme), ''), 'National Research Program for Universities (NRPU)') AS funder_scheme,

        'hec_pakistan_nrpu' AS provenance,

        g.parsed_start_date AS start_date,
        g.parsed_end_date AS end_date,
        COALESCE(YEAR(g.parsed_start_date), g.parsed_year) AS start_year,
        COALESCE(YEAR(g.parsed_end_date), g.parsed_year) AS end_year,

        CASE
            WHEN g.pi_name IS NULL OR TRIM(g.pi_name) = '' THEN CAST(NULL AS STRUCT<
                given_name:STRING,
                family_name:STRING,
                orcid:STRING,
                role_start:DATE,
                affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
            >)
            ELSE struct(
                NULLIF(TRIM(g.pi_given_name), '') AS given_name,
                NULLIF(TRIM(g.pi_family_name), '') AS family_name,
                CAST(NULL AS STRING) AS orcid,
                g.parsed_start_date AS role_start,
                struct(
                    NULLIF(TRIM(g.university), '') AS name,
                    'PK' AS country,
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
                ) AS affiliation
            )
        END AS lead_investigator,

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

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000) AS works_api_url,

        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date

    FROM raw_prepared g
    CROSS JOIN hec_funder f
)
