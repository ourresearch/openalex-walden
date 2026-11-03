-- Mapping from CreateCAPESCooperationAwards.ipynb (origin e22fdf7f): its awards-table SELECT over capes_cooperacao_internacional_input (was the
-- staging table read from s3://openalex-ingest/awards/capes_cooperation/capes_cooperation_projects.parquet); created_date and updated_date are the run timestamp.
WITH
capes_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321091
),

raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        CASE WHEN TRY_CAST(amount AS DOUBLE) IS NOT NULL THEN 'BRL' ELSE NULL END AS parsed_currency,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS parsed_end_date,
        TRY_CAST(source_year AS INT) AS parsed_source_year,
        TRY_CAST(source_end_year AS INT) AS parsed_end_year
    FROM capes_cooperacao_internacional_input
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

        'research' AS funding_type,

        COALESCE(NULLIF(TRIM(g.funder_scheme), ''), 'International cooperation') AS funder_scheme,

        'capes_cooperacao_internacional' AS provenance,

        g.parsed_start_date AS start_date,
        g.parsed_end_date AS end_date,
        COALESCE(YEAR(g.parsed_start_date), g.parsed_source_year) AS start_year,
        COALESCE(YEAR(g.parsed_end_date), g.parsed_end_year) AS end_year,

        CASE
            WHEN g.beneficiary_name IS NULL OR TRIM(g.beneficiary_name) = '' THEN NULL
            ELSE struct(
                NULLIF(TRIM(g.beneficiary_given_name), '') AS given_name,
                NULLIF(TRIM(g.beneficiary_family_name), '') AS family_name,
                CAST(NULL AS STRING) AS orcid,
                g.parsed_start_date AS role_start,
                struct(
                    NULLIF(TRIM(g.institution_name), '') AS name,
                    'BR' AS country,
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

        CONCAT('https://api.openalex.org/works?filter=awards.id:G',
               abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000) AS works_api_url,

        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date

    FROM raw_prepared g
    CROSS JOIN capes_funder f
)
