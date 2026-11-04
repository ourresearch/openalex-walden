-- Mapping from CreateHiltonAwards.ipynb (origin e22fdf7f): its awards-table SELECT over hilton_foundation_input (was the
-- staging table read from s3://openalex-ingest/awards/hilton/hilton_grants.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306180  -- Conrad N. Hilton Foundation
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    CONCAT(COALESCE(n.program, 'Grant'), ' - ',
           COALESCE(n.grantee_org, n.title),
           CASE WHEN n.start_year IS NOT NULL THEN CONCAT(' (', n.start_year, ')') ELSE '' END) AS display_name,
    n.description,
    f.funder_id,
    n.funder_award_id,
    CASE WHEN TRY_CAST(n.amount AS DOUBLE) > 0 THEN TRY_CAST(n.amount AS DOUBLE) ELSE NULL END AS amount,
    CASE WHEN TRY_CAST(n.amount AS DOUBLE) > 0 THEN n.currency ELSE NULL END AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    'grant' AS funding_type,
    n.program AS funder_scheme,
    'hilton_foundation' AS provenance,
    CAST(NULL AS DATE) AS start_date,
    CAST(NULL AS DATE) AS end_date,
    TRY_CAST(n.start_year AS INT) AS start_year,
    TRY_CAST(n.end_year AS INT) AS end_year,
    CASE
        WHEN n.grantee_org IS NOT NULL THEN struct(
            CAST(NULL AS STRING) AS given_name,
            CAST(NULL AS STRING) AS family_name,
            CAST(NULL AS STRING) AS orcid,
            CAST(NULL AS DATE) AS role_start,
            struct(
                n.grantee_org AS name,
                CAST(NULL AS STRING) AS country,  -- Area Served is a region, not the grantee's country, never guessed
                CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
            ) AS affiliation
        )
        ELSE NULL
    END AS lead_investigator,
    CAST(NULL AS STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >) AS co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >>) AS investigators,
    n.landing_page_url AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(
               TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
           ))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM hilton_foundation_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
)
