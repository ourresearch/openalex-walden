-- Mapping from CreateGairdnerAwards.ipynb (origin e22fdf7f): its awards-table SELECT over gairdner_sitefinity_input (was the
-- staging table read from s3://openalex-ingest/awards/gairdner/gairdner_awards.parquet); created_date and updated_date are the run timestamp.
WITH
 gairdner_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320313415
),
normalized AS (
    SELECT
        g.*,
        TRY_CAST(g.year AS INT) AS award_year
    FROM gairdner_sitefinity_input g
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':gairdner:', LOWER(TRIM(g.funder_award_id))))) % 9000000000 AS id,
    CONCAT(g.award_name, ' ', CAST(g.award_year AS STRING), ' - ', g.laureate_name) AS display_name,
    CASE
        WHEN TRY_CAST(g.declined AS BOOLEAN)
             AND COALESCE(NULLIF(g.citation, ''), NULLIF(g.description, ''), NULLIF(g.award_summary, '')) IS NOT NULL
            THEN CONCAT('Declined the prize. ', COALESCE(NULLIF(g.citation, ''), NULLIF(g.description, ''), NULLIF(g.award_summary, '')))
        WHEN TRY_CAST(g.declined AS BOOLEAN) THEN 'Declined the prize.'
        ELSE COALESCE(NULLIF(g.citation, ''), NULLIF(g.description, ''), NULLIF(g.award_summary, ''))
    END AS description,
    f.funder_id,
    g.funder_award_id AS funder_award_id,
    CAST(NULL AS DOUBLE) AS amount,         -- prize-pattern waiver: historical per-laureate amount not in source API
    'CAD' AS currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) AS id,
        f.display_name, f.ror_id, f.doi
    ) AS funder,
    'prize' AS funding_type,
    g.award_name AS funder_scheme,
    'gairdner_sitefinity' AS provenance,
    TRY_TO_DATE(CONCAT(CAST(g.award_year AS STRING), '-01-01'), 'yyyy-MM-dd') AS start_date,
    TRY_TO_DATE(CONCAT(CAST(g.award_year AS STRING), '-12-31'), 'yyyy-MM-dd') AS end_date,
    g.award_year AS start_year,
    g.award_year AS end_year,
    struct(
        NULLIF(g.laureate_given_name, '') AS given_name,
        NULLIF(g.laureate_family_name, '') AS family_name,
        CAST(NULL AS STRING) AS orcid,
        CAST(NULL AS DATE) AS role_start,
        struct(
            NULLIF(g.laureate_position_title, '') AS name,
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
    g.laureate_url AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':gairdner:', LOWER(TRIM(g.funder_award_id))))) % 9000000000) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM normalized g
CROSS JOIN gairdner_funder f
WHERE g.funder_award_id IS NOT NULL
  AND g.laureate_name IS NOT NULL
  AND g.award_name IS NOT NULL
  AND g.award_year IS NOT NULL
)
