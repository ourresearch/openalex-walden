-- Mapping from CreateBluePlanetPrizeAwards.ipynb (origin e22fdf7f): its awards-table SELECT over blue_planet_prize_input (was the
-- staging table read from s3://openalex-ingest/awards/blue_planet_prize/blue_planet_prize_laureates.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320309996  -- Asahi Glass Foundation
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    n.display_name,
    n.description,
    f.funder_id,
    n.funder_award_id,
    TRY_CAST(n.amount AS DOUBLE) AS amount,
    n.currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    'prize' AS funding_type,
    'Blue Planet Prize' AS funder_scheme,
    'blue_planet_prize' AS provenance,
    TRY_TO_DATE(n.start_date, 'yyyy-MM-dd') AS start_date,
    TRY_TO_DATE(n.end_date,   'yyyy-MM-dd') AS end_date,
    TRY_CAST(SUBSTRING(n.start_date, 1, 4) AS INT) AS start_year,
    TRY_CAST(SUBSTRING(n.end_date,   1, 4) AS INT) AS end_year,
    CASE
        WHEN n.recipient_kind = 'individual' AND n.name_clean IS NOT NULL THEN struct(
            n.given_name AS given_name,
            n.family_name AS family_name,
            CAST(NULL AS STRING) AS orcid,
            TRY_TO_DATE(n.start_date, 'yyyy-MM-dd') AS role_start,
            struct(
                n.affiliation_name AS name,
                CAST(NULL AS STRING) AS country,
                CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
            ) AS affiliation
        )
        WHEN n.recipient_kind = 'organization' AND n.name_clean IS NOT NULL THEN struct(
            CAST(NULL AS STRING) AS given_name,
            CAST(NULL AS STRING) AS family_name,
            CAST(NULL AS STRING) AS orcid,
            TRY_TO_DATE(n.start_date, 'yyyy-MM-dd') AS role_start,
            struct(
                n.name_clean AS name,
                CAST(NULL AS STRING) AS country,
                CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
            ) AS affiliation
        )
        ELSE NULL
    END AS lead_investigator,
    CAST(NULL AS STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >) AS co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >>) AS investigators,
    n.landing_page_url AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(
               TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
           ))) % 9000000000 AS STRING)) AS works_api_url,
    n.declined,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM blue_planet_prize_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
  AND n.name_clean IS NOT NULL
)
