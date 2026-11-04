-- Mapping from CreateLaskerAwards.ipynb (origin e22fdf7f): its awards-table SELECT over lasker_wp_input (was the
-- staging table read from s3://openalex-ingest/awards/lasker/lasker_awards.parquet); created_date and updated_date are the run timestamp.
WITH
 lasker_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320311370
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':lasker:', CAST(s.wp_post_id AS STRING),
                        ':', CAST(s.laureate_term_id AS STRING)))) % 9000000000 AS id,
    CONCAT('Lasker ', s.award_name, ' Award ', CAST(s.year AS STRING),
           ' — ', s.laureate_name) AS display_name,
    NULLIF(s.achievement_title, '') AS description,
    f.funder_id,
    CONCAT(CAST(s.wp_post_id AS STRING), '-', CAST(s.laureate_term_id AS STRING)) AS funder_award_id,
    CAST(NULL AS DOUBLE) AS amount,
    'USD' AS currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) AS id,
        f.display_name, f.ror_id, f.doi
    ) AS funder,
    'prize' AS funding_type,
    s.award_name AS funder_scheme,           -- BASIC / CLINICAL / SPECIAL ACHIEVEMENT
    'lasker_wp' AS provenance,
    TRY_TO_DATE(CONCAT(CAST(s.year AS STRING), '-01-01'), 'yyyy-MM-dd') AS start_date,
    TRY_TO_DATE(CONCAT(CAST(s.year AS STRING), '-12-31'), 'yyyy-MM-dd') AS end_date,
    s.year AS start_year,
    s.year AS end_year,
    struct(
        s.laureate_given_name AS given_name,
        s.laureate_family_name AS family_name,
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
           abs(xxhash64(CONCAT(f.funder_id, ':lasker:', CAST(s.wp_post_id AS STRING),
                                ':', CAST(s.laureate_term_id AS STRING)))) % 9000000000) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM lasker_wp_input s
CROSS JOIN lasker_funder f
WHERE s.laureate_name IS NOT NULL AND s.year IS NOT NULL
)
