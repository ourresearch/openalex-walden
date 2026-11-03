-- Mapping from CreateFieldsMedalAwards.ipynb (origin e22fdf7f): its awards-table SELECT over imu_fields_medal_input (was the
-- staging table read from s3://openalex-ingest/awards/fields_medal/fields_medal_laureates.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320877  -- International Mathematical Union
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        CAST(f.funder_id AS STRING), ':fields:', n.slug
    ))) % 9000000000 AS id,
    CONCAT('Fields Medal ', TRY_CAST(n.year AS STRING), ' — ', n.medalist_name) AS display_name,
    CASE
        WHEN n.declined AND n.citation IS NOT NULL THEN CONCAT('Declined the medal. ', n.citation)
        WHEN n.declined THEN 'Declined the medal.'
        ELSE n.citation
    END AS description,
    f.funder_id,
    CONCAT('fields-', n.slug) AS funder_award_id,
    CAST(NULL AS DOUBLE) AS amount,           -- non-monetary prize, waived per §6.7
    CAST(NULL AS STRING) AS currency,         -- no currency for non-monetary prize
    struct(
        CONCAT('https://openalex.org/F', CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    'prize' AS funding_type,
    'Fields Medal' AS funder_scheme,
    'imu_fields_medal' AS provenance,
    TRY_TO_DATE(CONCAT(TRY_CAST(n.year AS STRING), '-08-01'), 'yyyy-MM-dd') AS start_date,
    TRY_TO_DATE(CONCAT(TRY_CAST(n.year AS STRING), '-08-01'), 'yyyy-MM-dd') AS end_date,
    TRY_CAST(n.year AS INT) AS start_year,
    TRY_CAST(n.year AS INT) AS end_year,
    struct(
        n.given_name AS given_name,
        n.family_name AS family_name,
        CAST(NULL AS STRING) AS orcid,
        CAST(NULL AS DATE) AS role_start,
        struct(
            n.affiliation_when_awarded AS name,
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
    n.source_url AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           CAST(abs(xxhash64(CONCAT(
               CAST(f.funder_id AS STRING), ':fields:', n.slug
           ))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM imu_fields_medal_input n
CROSS JOIN funder_resolved f
WHERE n.slug IS NOT NULL AND n.year IS NOT NULL
)
