-- Mapping from CreateNuffieldAwards.ipynb (origin e22fdf7f): its awards-table SELECT over nuffield_wp_rest_input (was the
-- staging table read from s3://openalex-ingest/awards/nuffield/nuffield_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320319997  -- Nuffield Foundation
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    n.title AS display_name,
    n.description,
    f.funder_id,
    n.funder_award_id,
    TRY_CAST(n.amount AS DOUBLE) AS amount,
    n.currency,  -- 'GBP' from script, NULL when amount missing
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    'research' AS funding_type,
    n.theme AS funder_scheme,
    'nuffield_wp_rest' AS provenance,
    TRY_TO_DATE(n.start_date, 'yyyy-MM-dd') AS start_date,
    TRY_TO_DATE(n.end_date, 'yyyy-MM-dd') AS end_date,
    TRY_CAST(SUBSTRING(n.start_date, 1, 4) AS INT) AS start_year,
    TRY_CAST(SUBSTRING(n.end_date, 1, 4) AS INT) AS end_year,
    CASE
        WHEN n.lead_full_name IS NULL OR n.lead_full_name = '' THEN NULL
        ELSE struct(
            n.lead_given_name AS given_name,
            n.lead_family_name AS family_name,
            CAST(NULL AS STRING) AS orcid,
            CAST(NULL AS DATE) AS role_start,
            CAST(NULL AS STRUCT<name:STRING, country:STRING,
                ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>) AS affiliation
        )
    END AS lead_investigator,
    CASE
        WHEN n.co_investigators IS NULL OR n.co_investigators = '' THEN NULL
        ELSE struct(
            CAST(NULL AS STRING) AS given_name,
            try_element_at(split(n.co_investigators, '\\|'), 1) AS family_name,
            CAST(NULL AS STRING) AS orcid,
            CAST(NULL AS DATE) AS role_start,
            CAST(NULL AS STRUCT<name:STRING, country:STRING,
                ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>) AS affiliation
        )
    END AS co_lead_investigator,
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
FROM nuffield_wp_rest_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
  AND n.title IS NOT NULL
)
