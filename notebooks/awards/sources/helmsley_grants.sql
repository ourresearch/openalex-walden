-- Mapping from CreateHelmsleyAwards.ipynb (origin e22fdf7f): its awards-table SELECT over helmsley_grants_input (was the
-- staging table read from s3://openalex-ingest/awards/helmsley/helmsley_grants.parquet); created_date and updated_date are the run timestamp.
WITH
helmsley_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320309446
),

raw_prepared AS (
    SELECT
        *,
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        TRY_CAST(award_year AS INT) AS parsed_year,
        TRY_TO_DATE(award_date_iso, 'yyyy-MM-dd') AS parsed_start_date
    FROM helmsley_grants_input
    WHERE recipient IS NOT NULL
      AND TRIM(recipient) != ''
      AND funder_award_id IS NOT NULL
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,

        COALESCE(NULLIF(TRIM(g.project_title), ''), g.recipient) as display_name,

        g.project_title as description,

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

        COALESCE(NULLIF(TRIM(g.program), ''), 'Helmsley Charitable Trust Grant') as funder_scheme,

        'helmsley_grants' as provenance,

        g.parsed_start_date as start_date,
        CAST(NULL AS DATE) as end_date,
        g.parsed_year as start_year,
        CAST(NULL AS INT) as end_year,

        struct(
            CAST(NULL AS STRING) as given_name,
            CAST(NULL AS STRING) as family_name,
            CAST(NULL AS STRING) as orcid,
            g.parsed_start_date as role_start,
            struct(
                NULLIF(TRIM(g.recipient), '') as name,
                'US' as country,  -- some Helmsley grants are international (Sub-Saharan Africa, Israel), a more rigorous mapping would parse city from the listing pages
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
    CROSS JOIN helmsley_funder f
)
