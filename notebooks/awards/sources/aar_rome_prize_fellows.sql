-- Mapping from CreateAARRomePrizeAwards.ipynb (origin e22fdf7f): its awards-table SELECT over aar_rome_prize_fellows_input (was the
-- staging table read from s3://openalex-ingest/awards/aar_rome_prize/aar_rome_prize_fellows.parquet); created_date and updated_date are the run timestamp.
WITH
aar_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320895
),

raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        NULLIF(TRIM(recipient_name), '') AS recipient_name_norm,
        NULLIF(TRIM(lead_given_name), '') AS lead_given_name_norm,
        NULLIF(TRIM(lead_family_name), '') AS lead_family_name_norm,
        NULLIF(TRIM(co_lead_given_name), '') AS co_lead_given_name_norm,
        NULLIF(TRIM(co_lead_family_name), '') AS co_lead_family_name_norm,
        NULLIF(TRIM(profession), '') AS profession_norm,
        COALESCE(NULLIF(TRIM(fellowship_name), ''), NULLIF(TRIM(source_discipline), '')) AS scheme_norm,
        NULLIF(TRIM(project_title), '') AS project_title_norm,
        NULLIF(TRIM(description), '') AS description_norm,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS parsed_end_date,
        TRY_CAST(source_year AS INT) AS parsed_source_year
    FROM aar_rome_prize_fellows_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND recipient_name IS NOT NULL
      AND TRIM(recipient_name) != ''
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000 AS id,

        COALESCE(
            g.project_title_norm,
            CONCAT('Rome Prize ', TRY_CAST(g.parsed_source_year AS STRING), ' - ', g.recipient_name_norm)
        ) AS display_name,

        g.description_norm AS description,

        f.funder_id,
        g.native_award_id AS funder_award_id,

        CAST(NULL AS DOUBLE) AS amount,
        CAST(NULL AS STRING) AS currency,

        struct(
            CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
            f.display_name,
            f.ror_id,
            f.doi
        ) AS funder,

        'fellowship' AS funding_type,

        COALESCE(g.scheme_norm, 'Rome Prize') AS funder_scheme,

        'aar_rome_prize_fellows' AS provenance,

        g.parsed_start_date AS start_date,
        g.parsed_end_date AS end_date,

        CASE
            WHEN COALESCE(YEAR(g.parsed_start_date), g.parsed_source_year) > YEAR(current_date()) + 1 THEN NULL
            ELSE COALESCE(YEAR(g.parsed_start_date), g.parsed_source_year)
        END AS start_year,
        CASE
            WHEN COALESCE(YEAR(g.parsed_start_date), g.parsed_source_year) > YEAR(current_date()) + 1 THEN NULL
            ELSE COALESCE(YEAR(g.parsed_end_date), g.parsed_source_year)
        END AS end_year,

        struct(
            g.lead_given_name_norm AS given_name,
            g.lead_family_name_norm AS family_name,
            CAST(NULL AS STRING) AS orcid,
            g.parsed_start_date AS role_start,
            struct(
                g.profession_norm AS name,
                CAST(NULL AS STRING) AS country,
                CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
            ) AS affiliation
        ) AS lead_investigator,

        CASE
            WHEN g.co_lead_given_name_norm IS NULL AND g.co_lead_family_name_norm IS NULL THEN CAST(NULL AS STRUCT<
                given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
                affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
            >)
            ELSE struct(
                g.co_lead_given_name_norm AS given_name,
                g.co_lead_family_name_norm AS family_name,
                CAST(NULL AS STRING) AS orcid,
                g.parsed_start_date AS role_start,
                struct(
                    g.profession_norm AS name,
                    CAST(NULL AS STRING) AS country,
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
                ) AS affiliation
            )
        END AS co_lead_investigator,

        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) AS investigators,

        NULLIF(TRIM(g.landing_page_url), '') AS landing_page_url,
        CAST(NULL AS STRING) AS doi,

        CONCAT('https://api.openalex.org/works?filter=awards.id:G',
               abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000) AS works_api_url,

        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date

    FROM raw_prepared g
    CROSS JOIN aar_funder f
)
