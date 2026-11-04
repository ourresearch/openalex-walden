-- Mapping from CreateWolfPrizeAwards.ipynb (origin e22fdf7f): its awards-table SELECT over wolf_prize_wp_input (was the
-- staging table read from s3://openalex-ingest/awards/wolf_prize/wolf_prize_winners.parquet); created_date and updated_date are the run timestamp.
WITH
 wolf_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320951
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':wolf:', LOWER(s.slug)))) % 9000000000 AS id,
    CONCAT('Wolf Prize in ', COALESCE(s.field, 'Various'), ' ', CAST(s.year AS STRING),
           ' — ', s.name) AS display_name,
    NULLIF(s.citation, '') AS description,
    f.funder_id,
    s.slug AS funder_award_id,
    CAST(NULL AS DOUBLE) AS amount,         -- per-laureate share not published
    'USD' AS currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) AS id,
        f.display_name, f.ror_id, f.doi
    ) AS funder,
    'prize' AS funding_type,
    s.field AS funder_scheme,
    'wolf_prize_wp' AS provenance,
    TRY_TO_DATE(CONCAT(CAST(s.year AS STRING), '-01-01'), 'yyyy-MM-dd') AS start_date,
    TRY_TO_DATE(CONCAT(CAST(s.year AS STRING), '-12-31'), 'yyyy-MM-dd') AS end_date,
    s.year AS start_year,
    s.year AS end_year,
    struct(
        s.given_name AS given_name,
        s.family_name AS family_name,
        CAST(NULL AS STRING) AS orcid,
        CAST(NULL AS DATE) AS role_start,
        struct(
            NULLIF(s.affiliation, '') AS name,
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
    s.url AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':wolf:', LOWER(s.slug)))) % 9000000000) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM wolf_prize_wp_input s
CROSS JOIN wolf_funder f
WHERE s.slug IS NOT NULL AND s.year IS NOT NULL
)
