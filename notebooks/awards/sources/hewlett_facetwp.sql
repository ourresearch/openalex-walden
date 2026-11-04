-- Mapping from CreateHewlettAwards.ipynb (origin e22fdf7f): its awards-table SELECT over hewlett_facetwp_input (was the
-- staging table read from s3://openalex-ingest/awards/hewlett/hewlett_grants.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320307873  -- William and Flora Hewlett Foundation
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    CONCAT('Hewlett ', n.program, ' \u2014 ', n.grantee) AS display_name,
    CASE
        WHEN n.projectTitle IS NOT NULL AND n.overview IS NOT NULL
            THEN CONCAT(n.projectTitle, '. ', n.overview)
        WHEN n.projectTitle IS NOT NULL THEN n.projectTitle
        ELSE n.overview
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
    CASE
        WHEN n.program IN ('Performing Arts','Special Projects','Effective Philanthropy','Gender Equity and Governance') THEN 'other'
        ELSE 'research'
    END AS funding_type,
    n.program AS funder_scheme,
    'hewlett_facetwp' AS provenance,
    TRY_TO_DATE(n.start_date, 'yyyy-MM-dd') AS start_date,
    TRY_TO_DATE(n.end_date, 'yyyy-MM-dd') AS end_date,
    TRY_CAST(SUBSTRING(n.start_date, 1, 4) AS INT) AS start_year,
    TRY_CAST(SUBSTRING(n.end_date, 1, 4) AS INT) AS end_year,
    CASE
        WHEN n.grantee IS NULL OR n.grantee = '' THEN NULL
        ELSE struct(
            CAST(NULL AS STRING) AS given_name,
            CAST(NULL AS STRING) AS family_name,
            CAST(NULL AS STRING) AS orcid,
            TRY_TO_DATE(n.start_date, 'yyyy-MM-dd') AS role_start,
            struct(
                n.grantee AS name,
                CAST('US' AS STRING) AS country,
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
    n.grantee_website AS landing_page_url,  -- grantee's own site, not Hewlett's grant detail page (Hewlett doesn't expose per-grant URLs)
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(
               TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
           ))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM hewlett_facetwp_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
  AND n.grantee IS NOT NULL
)
