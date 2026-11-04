-- Mapping from CreateISFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over isf_grant_search_input (was the
-- staging table read from s3://openalex-ingest/awards/isf/isf_grants.parquet); created_date and updated_date are the run timestamp.
WITH
isf_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320322252
),

raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        CASE WHEN TRY_CAST(amount AS DOUBLE) IS NOT NULL THEN 'ILS' ELSE NULL END AS parsed_currency,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS parsed_end_date,
        TRY_CAST(source_year AS INT) AS parsed_start_year,
        TRY_CAST(years AS INT) AS parsed_duration_years
    FROM isf_grant_search_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND display_name IS NOT NULL
      AND TRIM(display_name) != ''
),

mapped_awards AS (
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

        COALESCE(NULLIF(TRIM(g.funding_type), ''), 'grant') as funding_type,

        COALESCE(
            NULLIF(TRIM(g.grant_type_name), ''),
            NULLIF(TRIM(g.source_allocation_type_name), ''),
            'ISF grant'
        ) as funder_scheme,

        'isf_grant_search' as provenance,

        g.parsed_start_date as start_date,
        g.parsed_end_date as end_date,
        COALESCE(YEAR(g.parsed_start_date), g.parsed_start_year) as start_year,
        COALESCE(YEAR(g.parsed_end_date), g.parsed_start_year + g.parsed_duration_years - 1) as end_year,

        struct(
            NULLIF(TRIM(g.lead_investigator_given_name), '') as given_name,
            NULLIF(TRIM(g.lead_investigator_family_name), '') as family_name,
            CAST(NULL AS STRING) as orcid,
            g.parsed_start_date as role_start,
            struct(
                NULLIF(TRIM(g.institution), '') as name,
                'IL' as country,
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

        CONCAT('https://api.openalex.org/works?filter=awards.id:G',
               CAST(abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000 AS STRING)) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM raw_prepared g
    CROSS JOIN isf_funder f
),

awards_transformed AS (
SELECT *
FROM mapped_awards
)
