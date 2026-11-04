-- Mapping from CreateMottAwards.ipynb (origin e22fdf7f): its awards-table SELECT over mott_grants_input (was the
-- staging table read from s3://openalex-ingest/awards/mott/mott_grants.parquet); created_date and updated_date are the run timestamp.
WITH
mott_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320307861
),

raw_prepared AS (
    SELECT
        *,
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        TRY_CAST(start_year AS INT) AS parsed_year,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS parsed_end_date
    FROM mott_grants_input
    WHERE title IS NOT NULL
      AND TRIM(title) != ''
      AND funder_award_id IS NOT NULL
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,

        g.title as display_name,
        g.description as description,

        f.funder_id,
        g.funder_award_id,

        g.parsed_amount as amount,
        'USD' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        'grant' as funding_type,

        CASE
            WHEN g.initiative IS NOT NULL AND TRIM(g.initiative) != '' AND g.program IS NOT NULL AND TRIM(g.program) != ''
                THEN CONCAT(TRIM(g.program), ' / ', TRIM(g.initiative))
            WHEN g.program IS NOT NULL AND TRIM(g.program) != ''
                THEN TRIM(g.program)
            ELSE 'Mott Foundation Grant'
        END as funder_scheme,

        'mott_grants' as provenance,

        g.parsed_start_date as start_date,
        g.parsed_end_date as end_date,
        g.parsed_year as start_year,
        YEAR(g.parsed_end_date) as end_year,

        struct(
            CAST(NULL AS STRING) as given_name,
            CAST(NULL AS STRING) as family_name,
            CAST(NULL AS STRING) as orcid,
            g.parsed_start_date as role_start,
            struct(
                NULLIF(TRIM(g.recipient), '') as name,
                'US' as country,  -- Mott funds primarily US recipients, some international (Civil Society program)
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

        g.landing_page_url as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM raw_prepared g
    CROSS JOIN mott_funder f
)
