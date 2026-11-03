-- Mapping from CreateEDCTPAwards.ipynb (origin e22fdf7f): its awards-table SELECT over edctp_grants_portal_input (was the
-- staging table read from s3://openalex-ingest/awards/edctp/edctp_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320338462
),
raw_prepped AS (
    SELECT
        *,
        TRY_CAST(amount AS DOUBLE) AS amount_double,
        CASE WHEN TRY_CAST(start_year AS INT) BETWEEN 1800 AND 2100 THEN TRY_CAST(start_year AS INT) END AS start_year_int,
        CASE WHEN TRY_CAST(end_year AS INT) BETWEEN 1800 AND 2100 THEN TRY_CAST(end_year AS INT) END AS end_year_int,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS start_date_d,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS end_date_d,
        NULLIF(TRIM(host_institution), '') AS leader_affiliation_name,
        NULLIF(TRIM(host_country), '') AS leader_affiliation_country,
        NULLIF(TRIM(lead_given_name), '') AS leader_given_name,
        NULLIF(TRIM(lead_family_name), '') AS leader_family_name
    FROM edctp_grants_portal_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT('4320338462:', LOWER(TRIM(rp.funder_award_id))))) % 9000000000 AS id,
    rp.display_name,
    NULLIF(TRIM(rp.description), '') AS description,
    4320338462 AS funder_id,
    rp.funder_award_id,
    rp.amount_double AS amount,
    CASE WHEN rp.amount_double IS NOT NULL THEN 'EUR' ELSE NULL END AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    COALESCE(NULLIF(TRIM(rp.funding_type), ''), 'research') AS funding_type,
    NULLIF(TRIM(rp.funder_scheme), '') AS funder_scheme,
    'edctp_grants_portal' AS provenance,
    rp.start_date_d AS start_date,
    rp.end_date_d AS end_date,
    rp.start_year_int AS start_year,
    rp.end_year_int AS end_year,
    CASE
        WHEN rp.leader_family_name IS NOT NULL OR rp.leader_affiliation_name IS NOT NULL THEN
            struct(
                rp.leader_given_name AS given_name,
                rp.leader_family_name AS family_name,
                CAST(NULL AS STRING) AS orcid,
                CAST(NULL AS DATE) AS role_start,
                struct(
                    rp.leader_affiliation_name AS name,
                    rp.leader_affiliation_country AS country,
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
    rp.landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT('4320338462:', LOWER(TRIM(rp.funder_award_id))))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM raw_prepped rp
CROSS JOIN funder_resolved f
WHERE rp.funder_award_id IS NOT NULL
  AND rp.display_name IS NOT NULL
)
