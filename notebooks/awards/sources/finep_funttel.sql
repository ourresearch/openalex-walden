-- Mapping from CreateFINEPFUNTTELAwards.ipynb (origin e22fdf7f): its awards-table SELECT over finep_funttel_input (was the
-- staging table read from s3://openalex-ingest/awards/finep_funttel/finep_funttel_projects.parquet); created_date and updated_date are the run timestamp.
WITH
finep_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320322904
),

raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        CASE WHEN TRY_CAST(amount AS DOUBLE) IS NOT NULL THEN 'BRL' ELSE NULL END AS parsed_currency,
        TRY_TO_DATE(signed_date, 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS parsed_end_date,
        TRY_CAST(source_year AS INT) AS parsed_start_year
    FROM finep_funttel_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND display_name IS NOT NULL
      AND TRIM(display_name) != ''
),

mapped_awards AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000 as id,
        TRIM(g.display_name) as display_name,
        NULLIF(TRIM(g.description), '') as description,
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
        COALESCE(NULLIF(TRIM(g.funding_type), ''), 'research') as funding_type,
        COALESCE(NULLIF(TRIM(g.demand), ''), NULLIF(TRIM(g.product), ''), 'FUNTTEL') as funder_scheme,
        'finep_funttel' as provenance,
        g.parsed_start_date as start_date,
        g.parsed_end_date as end_date,
        COALESCE(YEAR(g.parsed_start_date), g.parsed_start_year) as start_year,
        COALESCE(YEAR(g.parsed_end_date), g.parsed_start_year) as end_year,
        struct(
            CAST(NULL AS STRING) as given_name,
            CAST(NULL AS STRING) as family_name,
            CAST(NULL AS STRING) as orcid,
            g.parsed_start_date as role_start,
            struct(
                NULLIF(TRIM(g.recipient_name), '') as name,
                'BR' as country,
                CASE
                    WHEN g.recipient_cnpj IS NOT NULL AND TRIM(g.recipient_cnpj) != '' THEN
                        ARRAY(struct(TRIM(g.recipient_cnpj) as id, 'cnpj' as type, 'source' as asserted_by))
                    ELSE CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
                END as ids
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
    CROSS JOIN finep_funder f
),

awards_transformed AS (
SELECT *
FROM mapped_awards
)
