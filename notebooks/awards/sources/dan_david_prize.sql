-- Mapping from CreateDanDavidPrizeAwards.ipynb (origin e22fdf7f): its awards-table SELECT over dan_david_prize_input (was the
-- staging table read from s3://openalex-ingest/awards/dan_david_prize/dan_david_prize_winners.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320949
),
normalized AS (
    SELECT
        n.*,
        TRY_CAST(n.award_year AS INT) AS award_year_int,
        TRY_CAST(n.source_award_amount AS DOUBLE) AS amount_double
    FROM dan_david_prize_input n
),
awards AS (
    SELECT
        abs(xxhash64(CONCAT(
            TRY_CAST(f.funder_id AS STRING), ':dan-david-prize:', LOWER(n.funder_award_id)
        ))) % 9000000000 AS id,
        CONCAT('Dan David Prize ', TRY_CAST(n.award_year_int AS STRING), ' - ', n.laureate_name) AS display_name,
        COALESCE(NULLIF(n.profile_description, ''), NULLIF(n.research_topic, '')) AS description,
        f.funder_id,
        n.funder_award_id,
        n.amount_double AS amount,
        NULLIF(n.currency, '') AS currency,
        struct(
            CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
            f.display_name,
            f.ror_id,
            f.doi
        ) AS funder,
        'prize' AS funding_type,
        NULLIF(n.research_topic, '') AS funder_scheme,
        'dan_david_prize' AS provenance,
        TRY_TO_DATE(CONCAT(TRY_CAST(n.award_year_int AS STRING), '-01-01'), 'yyyy-MM-dd') AS start_date,
        TRY_TO_DATE(CONCAT(TRY_CAST(n.award_year_int AS STRING), '-12-31'), 'yyyy-MM-dd') AS end_date,
        n.award_year_int AS start_year,
        n.award_year_int AS end_year,
        struct(
            NULLIF(n.given_name, '') AS given_name,
            NULLIF(n.family_name, '') AS family_name,
            CAST(NULL AS STRING) AS orcid,
            CAST(NULL AS DATE) AS role_start,
            struct(
                NULLIF(n.affiliation, '') AS name,
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
        NULLIF(n.landing_page_url, '') AS landing_page_url,
        CAST(NULL AS STRING) AS doi,
        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date
    FROM normalized n
    CROSS JOIN funder_resolved f
    WHERE n.funder_award_id IS NOT NULL
      AND n.award_year_int IS NOT NULL
      AND n.laureate_name IS NOT NULL
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
    CONCAT('https://api.openalex.org/works?filter=awards.id:G', TRY_CAST(id AS STRING)) AS works_api_url,
    created_date,
    updated_date
FROM awards
)
