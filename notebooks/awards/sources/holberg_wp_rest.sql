-- Mapping from CreateHolbergAwards.ipynb (origin e22fdf7f): its awards-table SELECT over holberg_wp_rest_input (was the
-- staging table read from s3://openalex-ingest/awards/holberg/holberg_laureates.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320323078  -- Universitetet i Bergen
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    CONCAT(n.prize_name, ' ', TRY_CAST(n.year AS STRING), ' \u2014 ', n.laureate_full_name) AS display_name,
    CASE
        WHEN TRY_CAST(n.declined AS BOOLEAN) = TRUE AND n.description IS NOT NULL
            THEN CONCAT('Declined the prize. ', n.description)
        WHEN TRY_CAST(n.declined AS BOOLEAN) = TRUE
            THEN 'Declined the prize.'
        ELSE n.description
    END AS description,
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
    n.prize_name AS funder_scheme,
    'holberg_wp_rest' AS provenance,
    TRY_TO_DATE(CONCAT(TRY_CAST(n.year AS STRING), '-06-01'), 'yyyy-MM-dd') AS start_date,
    TRY_TO_DATE(CONCAT(TRY_CAST(n.year AS STRING), '-06-01'), 'yyyy-MM-dd') AS end_date,
    TRY_CAST(n.year AS INT) AS start_year,
    TRY_CAST(n.year AS INT) AS end_year,
    struct(
        n.laureate_given_name AS given_name,
        n.laureate_family_name AS family_name,
        CAST(NULL AS STRING) AS orcid,
        CAST(NULL AS DATE) AS role_start,
        struct(
            CAST(NULL AS STRING) AS name,
            n.nationality AS country,
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
    n.landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(
               TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
           ))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM holberg_wp_rest_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
  AND n.year IS NOT NULL
  AND n.prize_slug IS NOT NULL
)
