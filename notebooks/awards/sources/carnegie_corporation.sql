-- Mapping from CreateCarnegieAwards.ipynb (origin e22fdf7f): its awards-table SELECT over carnegie_corporation_input (was the
-- staging table read from s3://openalex-ingest/awards/carnegie/carnegie_grants.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306125  -- Carnegie Corporation of New York
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    CONCAT('Carnegie ', COALESCE(n.program, 'Grant'), ' - ', n.grantee_org,
           CASE WHEN n.year IS NOT NULL THEN CONCAT(' (', n.year, ')') ELSE '' END) AS display_name,
    CAST(NULL AS STRING) AS description,
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
    'grant' AS funding_type,
    n.program AS funder_scheme,
    'carnegie_corporation' AS provenance,
    CAST(NULL AS DATE) AS start_date,
    CAST(NULL AS DATE) AS end_date,
    TRY_CAST(n.year AS INT) AS start_year,
    CAST(NULL AS INT) AS end_year,
    CASE
        WHEN n.grantee_org IS NULL OR n.grantee_org = '' THEN NULL
        ELSE struct(
            CAST(NULL AS STRING) AS given_name,
            CAST(NULL AS STRING) AS family_name,
            CAST(NULL AS STRING) AS orcid,
            CAST(NULL AS DATE) AS role_start,
            struct(
                n.grantee_org AS name,
                CAST(NULL AS STRING) AS country,  -- source doesn't expose grantee country per grant
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
FROM carnegie_corporation_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
  AND n.grantee_org IS NOT NULL
)
