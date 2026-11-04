-- Mapping from CreateJSMFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over jsmf_input (was the
-- staging table read from s3://openalex-ingest/awards/jsmf/jsmf_grants.parquet); created_date and updated_date are the run timestamp.
WITH
jsmf_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306183  -- James S. McDonnell Foundation
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,
        g.title as display_name,
        CAST(NULL AS STRING) as description,
        f.funder_id,
        g.funder_award_id,
        TRY_CAST(g.amount AS DECIMAL(18,2)) as amount,
        'USD' as currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name, f.ror_id, f.doi
        ) as funder,
        'grant' as funding_type,
        CAST(NULL AS STRING) as funder_scheme,
        'jsmf' as provenance,
        TRY_TO_DATE(g.start_date_raw, 'yyyy-MM-dd') as start_date,
        CAST(NULL AS DATE) as end_date,
        CASE WHEN TRY_CAST(g.start_year AS INT) BETWEEN 1980 AND 2027 THEN TRY_CAST(g.start_year AS INT) ELSE NULL END as start_year,
        CAST(NULL AS INT) as end_year,
        CASE
            WHEN g.recipient IS NOT NULL THEN
                struct(
                    CAST(NULL AS STRING) as given_name,
                    CAST(NULL AS STRING) as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.recipient as name,
                        CAST(NULL AS STRING) as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            ELSE NULL
        END as lead_investigator,
        CAST(NULL AS STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as co_lead_investigator,
        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) as investigators,
        g.landing_page_url,
        CAST(NULL AS STRING) as doi,
        CAST(NULL AS STRING) as works_api_url,
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date
    FROM jsmf_input g
    CROSS JOIN jsmf_funder f
)
