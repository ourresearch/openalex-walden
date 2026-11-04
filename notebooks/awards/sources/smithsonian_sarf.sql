-- Mapping from CreateSmithsonianSARFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over smithsonian_sarf_input (was the
-- staging table read from s3://openalex-ingest/awards/smithsonian_sarf/smithsonian_sarf_awards.parquet); created_date and updated_date are the run timestamp.
WITH
smithsonian_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 7230414656
),
raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_CAST(source_year AS INT) AS parsed_year,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS parsed_end_date
    FROM smithsonian_sarf_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND display_name IS NOT NULL
      AND TRIM(display_name) != ''
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000 AS id,

        TRIM(g.display_name) AS display_name,
        NULLIF(TRIM(g.description), '') AS description,

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

        'fellowship' AS funding_type,
        COALESCE(NULLIF(TRIM(g.funder_scheme), ''), 'Smithsonian Artist Research Fellowship') AS funder_scheme,

        'smithsonian_sarf' AS provenance,

        g.parsed_start_date AS start_date,
        g.parsed_end_date AS end_date,
        g.parsed_year AS start_year,
        g.parsed_year AS end_year,

        struct(
            NULLIF(TRIM(g.given_name), '') AS given_name,
            NULLIF(TRIM(g.family_name), '') AS family_name,
            CAST(NULL AS STRING) AS orcid,
            g.parsed_start_date AS role_start,
            struct(
                NULLIF(TRIM(g.host_unit), '') AS name,
                'US' AS country,
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

        g.landing_page_url AS landing_page_url,
        CAST(NULL AS STRING) AS doi,

        concat(
            'https://api.openalex.org/works?filter=awards.id:G',
            abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000
        ) AS works_api_url,

        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date
    FROM raw_prepared g
    CROSS JOIN smithsonian_funder f
)
