-- Mapping from CreateKingFaisalPrizeAwards.ipynb (origin e22fdf7f): its awards-table SELECT over king_faisal_prize_input (was the
-- staging table read from s3://openalex-ingest/awards/king_faisal_prize/king_faisal_prize_laureates.parquet); created_date and updated_date are the run timestamp.
WITH
 king_faisal_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320323301
),
normalized AS (
    SELECT
        k.*,
        TRY_CAST(k.award_year AS INT) AS parsed_award_year,
        TRY_CAST(k.source_award_amount AS DOUBLE) AS parsed_amount,
        TRY_CAST(k.source_prize_amount_sar AS DOUBLE) AS parsed_total_amount,
        TRY_CAST(k.winner_count AS DOUBLE) AS parsed_winner_count
    FROM king_faisal_prize_input k
),
award_rows AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':king_faisal_prize:', LOWER(TRIM(k.funder_award_id))))) % 9000000000 AS id,
        CONCAT(k.prize_title, ' ', CAST(k.parsed_award_year AS STRING), ' - ', k.laureate_name) AS display_name,
        COALESCE(NULLIF(k.citation, ''), NULLIF(k.meta_description, ''), NULLIF(k.quote, ''), NULLIF(k.topic, '')) AS description,
        f.funder_id,
        k.funder_award_id AS funder_award_id,
        k.parsed_amount AS amount,
        NULLIF(k.currency, '') AS currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) AS id,
            f.display_name, f.ror_id, f.doi
        ) AS funder,
        'prize' AS funding_type,
        k.prize_title AS funder_scheme,
        'king_faisal_prize' AS provenance,
        TRY_TO_DATE(CONCAT(CAST(k.parsed_award_year AS STRING), '-01-01'), 'yyyy-MM-dd') AS start_date,
        TRY_TO_DATE(CONCAT(CAST(k.parsed_award_year AS STRING), '-12-31'), 'yyyy-MM-dd') AS end_date,
        k.parsed_award_year AS start_year,
        k.parsed_award_year AS end_year,
        struct(
            NULLIF(k.laureate_given_name, '') AS given_name,
            NULLIF(k.laureate_family_name, '') AS family_name,
            CAST(NULL AS STRING) AS orcid,
            CAST(NULL AS DATE) AS role_start,
            struct(
                CAST(NULL AS STRING) AS name,
                CAST(NULL AS STRING) AS country,
                CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
            ) AS affiliation
        ) AS lead_investigator,
        CAST(NULL AS STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) AS co_lead_investigator,
        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) AS investigators,
        NULLIF(k.landing_page_url, '') AS landing_page_url,
        CAST(NULL AS STRING) AS doi,
        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date
    FROM normalized k
    CROSS JOIN king_faisal_funder f
),

awards_transformed AS (
SELECT
    id,
    display_name,
    description,
    funder_id,
    funder_award_id,
    amount,
    currency,
    funder,
    funding_type,
    funder_scheme,
    provenance,
    start_date,
    end_date,
    start_year,
    end_year,
    lead_investigator,
    co_lead_investigator,
    investigators,
    landing_page_url,
    doi,
    concat('https://api.openalex.org/works?filter=awards.id:G', id) AS works_api_url,
    created_date,
    updated_date
FROM award_rows
)
