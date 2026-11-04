-- Mapping from CreateKauffmanAwards.ipynb (origin e22fdf7f): its awards-table SELECT over kauffman_foundation_input (was the
-- staging table read from s3://openalex-ingest/awards/kauffman/kauffman_grants.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306140  -- Ewing Marion Kauffman Foundation
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    CONCAT('Kauffman ', COALESCE(n.grant_type, 'Grant'),
           ' - ', n.grantee_org,
           CASE WHEN n.start_year IS NOT NULL THEN CONCAT(' (', n.start_year, ')') ELSE '' END) AS display_name,
    n.description,
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
        WHEN LOWER(n.grant_type) RLIKE 'research' THEN 'research'
        ELSE 'grant'
    END AS funding_type,
    n.strategy AS funder_scheme,
    'kauffman_foundation' AS provenance,
    TRY_TO_DATE(n.approved_on, 'yyyy-MM-dd') AS start_date,
    CAST(NULL AS DATE) AS end_date,
    TRY_CAST(n.start_year AS INT) AS start_year,
    CAST(NULL AS INT) AS end_year,
    CASE
        WHEN n.grantee_org IS NULL OR n.grantee_org = '' THEN NULL
        ELSE struct(
            CAST(NULL AS STRING) AS given_name,
            CAST(NULL AS STRING) AS family_name,
            CAST(NULL AS STRING) AS orcid,
            TRY_TO_DATE(n.approved_on, 'yyyy-MM-dd') AS role_start,
            struct(
                n.grantee_org AS name,
                CAST('US' AS STRING) AS country,  -- Kauffman states taxonomy is US-only
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
FROM kauffman_foundation_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
  AND n.grantee_org IS NOT NULL
)
