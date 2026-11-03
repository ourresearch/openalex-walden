-- Mapping from CreateDamonRunyonAwards.ipynb (origin e22fdf7f): its awards-table SELECT over damon_runyon_drupal_input (was the
-- staging table read from s3://openalex-ingest/awards/damon_runyon/damon_runyon_fellows.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306271  -- Damon Runyon Cancer Research Foundation
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    CONCAT('Damon Runyon ', COALESCE(n.award_program, 'Award'), ' \u2014 ', n.scientist_full_name) AS display_name,
    CASE
        WHEN n.cancer_type IS NOT NULL AND n.research_area IS NOT NULL
            THEN CONCAT(n.award_type, ' working on ', n.cancer_type, ' (', n.research_area, ').',
                        CASE WHEN n.sponsor_mentor IS NOT NULL THEN CONCAT(' Mentor: ', n.sponsor_mentor, '.') ELSE '' END)
        WHEN n.cancer_type IS NOT NULL
            THEN CONCAT(n.award_type, ' working on ', n.cancer_type, '.',
                        CASE WHEN n.sponsor_mentor IS NOT NULL THEN CONCAT(' Mentor: ', n.sponsor_mentor, '.') ELSE '' END)
        WHEN n.award_type IS NOT NULL
            THEN n.award_type
        ELSE NULL
    END AS description,
    f.funder_id,
    n.funder_award_id,
    CAST(NULL AS DOUBLE) AS amount,
    CAST(NULL AS STRING) AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    CASE
        WHEN LOWER(n.award_type) RLIKE 'fellow' THEN 'fellowship'
        WHEN LOWER(n.award_type) RLIKE 'investigator|research|innovation|scholar' THEN 'research'
        WHEN LOWER(n.award_type) RLIKE 'clinical' THEN 'research'
        ELSE 'other'
    END AS funding_type,
    n.award_program AS funder_scheme,
    'damon_runyon_drupal' AS provenance,
    CAST(NULL AS DATE) AS start_date,
    CAST(NULL AS DATE) AS end_date,
    CAST(NULL AS INT) AS start_year,
    CAST(NULL AS INT) AS end_year,
    CASE
        WHEN n.scientist_full_name IS NULL OR n.scientist_full_name = '' THEN NULL
        ELSE struct(
            n.given_name AS given_name,
            n.family_name AS family_name,
            CAST(NULL AS STRING) AS orcid,
            CAST(NULL AS DATE) AS role_start,
            struct(
                n.institution AS name,
                CAST('US' AS STRING) AS country,  -- Damon Runyon funds US-based researchers
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
FROM damon_runyon_drupal_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
  AND n.scientist_full_name IS NOT NULL
)
