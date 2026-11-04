-- Mapping from CreateUSAIDAwards.ipynb (origin e22fdf7f): its awards-table SELECT over usaspending_usaid_input (was the
-- staging table read from s3://openalex-ingest/awards/usaid/usaid_awards.parquet); created_date and updated_date are the run timestamp.
WITH
usaid_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306115  -- United States Agency for International Development
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.award_id_fain)))) % 9000000000 as id,

        COALESCE(g.prime_award_base_transaction_description, g.transaction_description) as display_name,

        COALESCE(g.prime_award_base_transaction_description, g.transaction_description) as description,

        f.funder_id,
        g.award_id_fain as funder_award_id,

        TRY_CAST(g.total_obligated_amount AS DOUBLE) as amount,
        'USD' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN g.assistance_type_code = '02' THEN 'grant'  -- Block Grant
            WHEN g.assistance_type_code = '03' THEN 'grant'  -- Formula Grant
            WHEN g.assistance_type_code = '04' THEN 'grant'  -- Project Grant
            WHEN g.assistance_type_code = '05' THEN 'grant'  -- Cooperative Agreement
            ELSE 'grant'
        END as funding_type,

        g.cfda_title as funder_scheme,

        'usaspending_usaid' as provenance,

        TRY_TO_DATE(g.period_of_performance_start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.period_of_performance_current_end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.period_of_performance_start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.period_of_performance_current_end_date, 'yyyy-MM-dd')) as end_year,

        CAST(NULL AS STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as lead_investigator,

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

        g.usaspending_permalink as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.award_id_fain)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM usaspending_usaid_input g
    CROSS JOIN usaid_funder f
    WHERE g.award_id_fain IS NOT NULL
      AND TRIM(g.award_id_fain) != ''
)
