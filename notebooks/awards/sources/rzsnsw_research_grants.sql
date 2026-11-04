-- Mapping from CreateRZSNSWResearchGrantsAwards.ipynb (origin e22fdf7f): its awards-table SELECT over rzsnsw_research_grants_input (was the
-- staging table read from s3://openalex-ingest/awards/rzsnsw/rzsnsw_research_grants.parquet); created_date and updated_date are the run timestamp.
WITH
rzsnsw_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320331891
),

raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        NULLIF(TRIM(currency), '') AS parsed_currency,
        TRY_CAST(award_year AS INT) AS parsed_award_year,
        TRY_TO_DATE(CONCAT(award_year, '-01-01'), 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(CONCAT(award_year, '-12-31'), 'yyyy-MM-dd') AS parsed_end_date
    FROM rzsnsw_research_grants_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND display_name IS NOT NULL
      AND TRIM(display_name) != ''
      AND recipient_name IS NOT NULL
      AND TRIM(recipient_name) != ''
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

        NULLIF(TRIM(g.funding_type), '') as funding_type,
        NULLIF(TRIM(g.funder_scheme), '') as funder_scheme,

        'rzsnsw_research_grants' as provenance,

        CASE
            WHEN g.parsed_award_year > YEAR(current_date()) + 1 THEN NULL
            ELSE g.parsed_start_date
        END as start_date,
        CASE
            WHEN g.parsed_award_year > YEAR(current_date()) + 1 THEN NULL
            ELSE g.parsed_end_date
        END as end_date,
        CASE
            WHEN g.parsed_award_year > YEAR(current_date()) + 1 THEN NULL
            ELSE g.parsed_award_year
        END as start_year,
        CASE
            WHEN g.parsed_award_year > YEAR(current_date()) + 1 THEN NULL
            ELSE g.parsed_award_year
        END as end_year,

        struct(
            NULLIF(TRIM(g.given_name), '') as given_name,
            NULLIF(TRIM(g.family_name), '') as family_name,
            CAST(NULL AS STRING) as orcid,
            CASE
                WHEN g.parsed_award_year > YEAR(current_date()) + 1 THEN NULL
                ELSE g.parsed_start_date
            END as role_start,
            struct(
                NULLIF(TRIM(g.affiliation), '') as name,
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
        NULLIF(TRIM(g.doi), '') as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM raw_prepared g
    CROSS JOIN rzsnsw_funder f
)
