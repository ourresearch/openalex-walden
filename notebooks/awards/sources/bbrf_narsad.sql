-- Mapping from CreateBBRFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over bbrf_narsad_input (was the
-- staging table read from s3://openalex-ingest/awards/bbrf/bbrf_grantees.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306147  -- Brain and Behavior Research Foundation
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    CONCAT('BBRF ', COALESCE(n.grant_type, 'Grant'),
           CASE WHEN n.grant_year IS NOT NULL THEN CONCAT(' (', n.grant_year, ')') ELSE '' END,
           ' - ', n.name) AS display_name,
    CASE
        WHEN n.grant_type IS NOT NULL AND n.illness IS NOT NULL AND n.illness <> ''
            THEN CONCAT(n.grant_type, ' awarded for ', n.illness, '-focused research.')
        WHEN n.grant_type IS NOT NULL
            THEN CONCAT(n.grant_type, '.')
        ELSE NULL
    END AS description,
    f.funder_id,
    n.funder_award_id,
    CASE
        WHEN LOWER(n.grant_type) = 'young investigator' THEN 70000.0
        WHEN LOWER(n.grant_type) = 'independent investigator' THEN 100000.0
        WHEN LOWER(n.grant_type) = 'distinguished investigator' THEN 100000.0
        ELSE NULL
    END AS amount,
    CASE
        WHEN LOWER(n.grant_type) RLIKE '^(young|independent|distinguished) investigator$' THEN 'USD'
        ELSE NULL
    END AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    CASE
        WHEN LOWER(n.grant_type) RLIKE 'young investigator|trailblazer|early.{0,5}career' THEN 'fellowship'
        WHEN LOWER(n.grant_type) RLIKE 'independent investigator|distinguished investigator|research' THEN 'research'
        WHEN LOWER(n.grant_type) RLIKE 'lieber|pardes|goldman|ruane|colvin|maltz|klerman|freedman|prize|award' THEN 'prize'
        ELSE 'research'
    END AS funding_type,
    n.grant_type AS funder_scheme,
    'bbrf_narsad' AS provenance,
    CAST(NULL AS DATE) AS start_date,
    CAST(NULL AS DATE) AS end_date,
    TRY_CAST(n.grant_year AS INT) AS start_year,
    CAST(NULL AS INT) AS end_year,
    CASE
        WHEN n.name IS NULL OR n.name = '' THEN NULL
        ELSE struct(
            n.given_name AS given_name,
            n.family_name AS family_name,
            CAST(NULL AS STRING) AS orcid,
            CAST(NULL AS DATE) AS role_start,
            struct(
                n.institution AS name,
                n.country AS country,
                CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
            ) AS affiliation
        )
    END AS lead_investigator,
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
FROM bbrf_narsad_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
  AND n.name IS NOT NULL
)
