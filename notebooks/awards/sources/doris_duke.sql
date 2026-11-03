-- Mapping from CreateDorisDukeAwards.ipynb (origin e22fdf7f): its awards-table SELECT over doris_duke_input (was the
-- staging table read from s3://openalex-ingest/awards/doris_duke/doris_duke.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306134  -- Doris Duke Charitable Foundation
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    CASE
        WHEN n.source_kind = 'organizational_grant'
            THEN CONCAT('Doris Duke Grant - ', n.grantee_org,
                        CASE WHEN n.year IS NOT NULL THEN CONCAT(' (', n.year, ')') ELSE '' END)
        ELSE CONCAT('Doris Duke ', n.scheme, ' - ', n.recipient_name,
                    CASE WHEN n.year IS NOT NULL THEN CONCAT(' (', n.year, ')') ELSE '' END)
    END AS display_name,
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
    n.funding_type,
    n.scheme AS funder_scheme,
    'doris_duke' AS provenance,
    TRY_TO_DATE(n.approved_on, 'yyyy-MM-dd') AS start_date,
    CAST(NULL AS DATE) AS end_date,
    TRY_CAST(n.year AS INT) AS start_year,
    CAST(NULL AS INT) AS end_year,
    CASE
        WHEN n.source_kind = 'organizational_grant' AND n.grantee_org IS NOT NULL THEN struct(
            CAST(NULL AS STRING) AS given_name,
            CAST(NULL AS STRING) AS family_name,
            CAST(NULL AS STRING) AS orcid,
            TRY_TO_DATE(n.approved_on, 'yyyy-MM-dd') AS role_start,
            struct(
                n.grantee_org AS name,
                CAST('US' AS STRING) AS country,
                CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
            ) AS affiliation
        )
        WHEN n.recipient_name IS NOT NULL THEN struct(
            n.given_name AS given_name,
            n.family_name AS family_name,
            CAST(NULL AS STRING) AS orcid,
            CAST(NULL AS DATE) AS role_start,
            struct(
                CAST(NULL AS STRING) AS name,  -- source doesn't expose institutional affiliation for artists/grantees
                CAST('US' AS STRING) AS country,
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
    n.landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(
               TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
           ))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM doris_duke_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
  AND (n.grantee_org IS NOT NULL OR n.recipient_name IS NOT NULL)
)
