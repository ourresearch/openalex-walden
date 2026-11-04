-- Mapping from CreateTrygFondenAwards.ipynb (origin e22fdf7f): its awards-table SELECT over trygfonden_donations_api_input (was the
-- staging table read from s3://openalex-ingest/awards/trygfonden/trygfonden_donations.parquet); created_date and updated_date are the run timestamp.
WITH
trygfonden_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320324424
),

raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        CASE WHEN TRY_CAST(amount AS DOUBLE) IS NOT NULL THEN 'DKK' ELSE NULL END AS parsed_currency,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS parsed_end_date,
        TRY_CAST(published_year AS INT) AS parsed_year,
        CONCAT_WS(
            ' - ',
            NULLIF(TRIM(target_area), ''),
            NULLIF(TRIM(focus_area), ''),
            NULLIF(TRIM(type), '')
        ) AS source_scheme
    FROM trygfonden_donations_api_input
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

        g.parsed_amount as amount,
        g.parsed_currency as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(TRIM(g.type)) = 'forskningsprojekt' THEN 'research'
            ELSE 'grant'
        END as funding_type,

        NULLIF(TRIM(g.source_scheme), '') as funder_scheme,

        'trygfonden_donations_api' as provenance,

        g.parsed_start_date as start_date,
        g.parsed_end_date as end_date,
        COALESCE(YEAR(g.parsed_start_date), g.parsed_year) as start_year,
        COALESCE(YEAR(g.parsed_end_date), g.parsed_year) as end_year,

        struct(
            CAST(NULL AS STRING) as given_name,
            CAST(NULL AS STRING) as family_name,
            CAST(NULL AS STRING) as orcid,
            g.parsed_start_date as role_start,
            struct(
                NULLIF(TRIM(g.recipient_name), '') as name,
                CASE
                    WHEN g.council IN ('Syddanmark', 'Hovedstaden', 'Landsdækkende', 'Midtjylland', 'Sjælland', 'Nordjylland') THEN 'DK'
                    ELSE NULL
                END as country,
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
    CROSS JOIN trygfonden_funder f
)
