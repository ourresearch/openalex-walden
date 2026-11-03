-- Mapping from CreateANIIAwards.ipynb (origin e22fdf7f): its awards-table SELECT over anii_projects_portal_input (was the
-- staging table read from s3://openalex-ingest/awards/anii/anii_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320310753  -- ANII Uruguay
), typed AS (
    SELECT
        *,
        TRY_CAST(amount AS DOUBLE) AS amount_double,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS start_date_typed,
        TRY_CAST(duration_months AS INT) AS duration_months_int
    FROM anii_projects_portal_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(a.funder_award_id)
    ))) % 9000000000 AS id,
    a.title AS display_name,
    a.description,
    f.funder_id,
    a.funder_award_id,
    a.amount_double AS amount,
    CASE WHEN a.amount_double IS NOT NULL THEN a.currency ELSE NULL END AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    'research' AS funding_type,
    a.instrument AS funder_scheme,
    'anii_projects_portal' AS provenance,
    a.start_date_typed AS start_date,
    CASE
        WHEN a.start_date_typed IS NOT NULL AND a.duration_months_int IS NOT NULL
        THEN date_sub(add_months(a.start_date_typed, a.duration_months_int), 1)
        ELSE NULL
    END AS end_date,
    YEAR(a.start_date_typed) AS start_year,
    CASE
        WHEN a.start_date_typed IS NOT NULL AND a.duration_months_int IS NOT NULL
        THEN YEAR(date_sub(add_months(a.start_date_typed, a.duration_months_int), 1))
        ELSE NULL
    END AS end_year,
    CASE
        WHEN a.beneficiary_person IS NULL OR a.beneficiary_person = '' THEN NULL
        ELSE struct(
            a.lead_given_name AS given_name,
            a.lead_family_name AS family_name,
            CAST(NULL AS STRING) AS orcid,
            CAST(NULL AS DATE) AS role_start,
            CASE
                WHEN a.beneficiary_institution IS NULL OR a.beneficiary_institution = '' THEN CAST(NULL AS STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>)
                ELSE struct(
                    a.beneficiary_institution AS name,
                    'UY' AS country,
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
                )
            END AS affiliation
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
    a.source_url AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(
               TRY_CAST(f.funder_id AS STRING), ':', LOWER(a.funder_award_id)
           ))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM typed a
CROSS JOIN funder_resolved f
WHERE a.funder_award_id IS NOT NULL
  AND a.title IS NOT NULL
)
