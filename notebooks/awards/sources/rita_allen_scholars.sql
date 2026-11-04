-- Mapping from CreateRitaAllenAwards.ipynb (origin e22fdf7f): its awards-table SELECT over rita_allen_scholars_input (was the
-- staging table read from s3://openalex-ingest/awards/rita_allen/rita_allen_scholars.parquet); created_date and updated_date are the run timestamp.
WITH
rita_allen_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306590
),

raw_prepared AS (
    SELECT
        *,
        TRY_CAST(award_year AS INT) AS parsed_year,
        TRY_TO_DATE(CONCAT(award_year, '-01-01'), 'yyyy-MM-dd') AS parsed_start_date
    FROM rita_allen_scholars_input
    WHERE scholar_name IS NOT NULL
      AND TRIM(scholar_name) != ''
      AND funder_award_id IS NOT NULL
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,

        g.scholar_name as display_name,

        g.bio as description,

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

        COALESCE(
            CONCAT('Rita Allen Foundation ', NULLIF(TRIM(g.program), '')),
            'Rita Allen Foundation Scholar'
        ) as funder_scheme,

        'rita_allen_scholars' as provenance,

        g.parsed_start_date as start_date,
        CAST(NULL AS DATE) as end_date,  -- 5-year program but end date not published per-scholar
        g.parsed_year as start_year,
        CAST(NULL AS INT) as end_year,

        struct(
            NULLIF(TRIM(g.given_name), '') as given_name,
            NULLIF(TRIM(g.family_name), '') as family_name,
            CAST(NULL AS STRING) as orcid,
            g.parsed_start_date as role_start,
            struct(
                NULLIF(TRIM(g.institution), '') as name,
                CAST(NULL AS STRING) as country,  -- US-based mostly but not exposed structured
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

        g.scholar_year_url as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM raw_prepared g
    CROSS JOIN rita_allen_funder f
)
