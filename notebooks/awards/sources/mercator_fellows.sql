-- Mapping from CreateMercatorFellowsAwards.ipynb (origin e22fdf7f): its awards-table SELECT over mercator_fellows_input (was the
-- staging table read from s3://openalex-ingest/awards/mercator/mercator_fellows.parquet); created_date and updated_date are the run timestamp.
WITH
mercator_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320327917
),
raw_prepared AS (
    SELECT
        *,
        TRY_CAST(start_year AS INT) AS parsed_start_year,
        TRY_CAST(end_year AS INT) AS parsed_end_year,
        TRY_TO_DATE(period_start, 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(period_end, 'yyyy-MM-dd') AS parsed_end_date
    FROM mercator_fellows_input
    WHERE fellow_name IS NOT NULL
      AND TRIM(fellow_name) != ''
      AND funder_award_id IS NOT NULL
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,

        g.fellow_name as display_name,
        g.description as description,

        f.funder_id,
        g.funder_award_id,

        CAST(NULL AS DOUBLE) as amount,
        CAST(NULL AS STRING) as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        'fellowship' as funding_type,
        COALESCE(NULLIF(TRIM(g.fellowship_program), ''), 'Stiftung Mercator Fellowship') as funder_scheme,

        'mercator_fellows' as provenance,

        g.parsed_start_date as start_date,
        g.parsed_end_date as end_date,
        g.parsed_start_year as start_year,
        g.parsed_end_year as end_year,

        struct(
            NULLIF(TRIM(g.given_name), '') as given_name,
            NULLIF(TRIM(g.family_name), '') as family_name,
            CAST(NULL AS STRING) as orcid,
            g.parsed_start_date as role_start,
            CAST(NULL AS STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>) as affiliation
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

        g.landing_page_url as landing_page_url,
        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G',
               abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date
    FROM raw_prepared g
    CROSS JOIN mercator_funder f
)
