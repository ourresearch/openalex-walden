-- Mapping from CreateZonMwAwards.ipynb (origin e22fdf7f): its awards-table SELECT over zonmw_projects_jsonapi_input (was the
-- staging table read from s3://openalex-ingest/awards/zonmw/zonmw_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321007
),
raw_prepped AS (
    SELECT
        *,
        TRY_CAST(project_budget_api AS DOUBLE) AS amount_double,
        CASE
            WHEN YEAR(TRY_TO_DATE(start_date, 'yyyy-MM-dd')) BETWEEN 1900 AND 2035
                THEN TRY_TO_DATE(start_date, 'yyyy-MM-dd')
            ELSE NULL
        END AS start_date_cast,
        CASE
            WHEN YEAR(TRY_TO_DATE(end_date, 'yyyy-MM-dd')) BETWEEN 1900 AND 2035
                THEN TRY_TO_DATE(end_date, 'yyyy-MM-dd')
            ELSE NULL
        END AS end_date_cast,
        COALESCE(
            CASE WHEN TRY_CAST(start_year AS INT) BETWEEN 1900 AND 2035 THEN TRY_CAST(start_year AS INT) END,
            CASE WHEN YEAR(TRY_TO_DATE(start_date, 'yyyy-MM-dd')) BETWEEN 1900 AND 2035 THEN YEAR(TRY_TO_DATE(start_date, 'yyyy-MM-dd')) END,
            CASE WHEN YEAR(TRY_TO_DATE(end_date, 'yyyy-MM-dd')) BETWEEN 1900 AND 2035 THEN YEAR(TRY_TO_DATE(end_date, 'yyyy-MM-dd')) END
        ) AS start_year_int,
        COALESCE(
            CASE WHEN TRY_CAST(end_year AS INT) BETWEEN 1900 AND 2035 THEN TRY_CAST(end_year AS INT) END,
            CASE WHEN YEAR(TRY_TO_DATE(end_date, 'yyyy-MM-dd')) BETWEEN 1900 AND 2035 THEN YEAR(TRY_TO_DATE(end_date, 'yyyy-MM-dd')) END
        ) AS end_year_int,
        NULLIF(TRIM(project_leader_name), '') AS leader_full_name,
        NULLIF(TRIM(primary_organization), '') AS leader_affiliation_name,
        CONCAT_WS('\\n\\n',
            NULLIF(TRIM(description), ''),
            CASE WHEN NULLIF(TRIM(application_summary), '') IS NOT NULL THEN CONCAT('Application summary: ', TRIM(application_summary)) END,
            CASE WHEN NULLIF(TRIM(progress_summary), '') IS NOT NULL THEN CONCAT('Progress summary: ', TRIM(progress_summary)) END,
            CASE WHEN NULLIF(TRIM(progress_results), '') IS NOT NULL THEN CONCAT('Progress results: ', TRIM(progress_results)) END,
            CASE WHEN NULLIF(TRIM(endreport_summary), '') IS NOT NULL THEN CONCAT('Final report summary: ', TRIM(endreport_summary)) END,
            CASE WHEN NULLIF(TRIM(endreport_results), '') IS NOT NULL THEN CONCAT('Final report results: ', TRIM(endreport_results)) END,
            CASE WHEN NULLIF(TRIM(keywords_json), '') IS NOT NULL THEN CONCAT('Keywords: ', TRIM(keywords_json)) END
        ) AS award_description,
        COALESCE(
            NULLIF(TRIM(related_subsidies_json), ''),
            NULLIF(TRIM(related_programs_json), ''),
            NULLIF(TRIM(main_subject), '')
        ) AS scheme_source
    FROM zonmw_projects_jsonapi_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT('4320321007:zonmw:', LOWER(TRIM(rp.slug))))) % 9000000000 AS id,
    rp.display_name,
    NULLIF(rp.award_description, '') AS description,
    4320321007 AS funder_id,
    rp.slug AS funder_award_id,
    rp.amount_double AS amount,
    CASE WHEN rp.amount_double IS NOT NULL THEN 'EUR' ELSE NULL END AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    'research' AS funding_type,
    rp.scheme_source AS funder_scheme,
    'zonmw_projects_jsonapi' AS provenance,
    rp.start_date_cast AS start_date,
    rp.end_date_cast AS end_date,
    rp.start_year_int AS start_year,
    rp.end_year_int AS end_year,
    CASE
        WHEN rp.leader_full_name IS NOT NULL OR rp.leader_affiliation_name IS NOT NULL THEN
            struct(
                CAST(NULL AS STRING) AS given_name,
                rp.leader_full_name AS family_name,
                CAST(NULL AS STRING) AS orcid,
                CAST(NULL AS DATE) AS role_start,
                struct(
                    rp.leader_affiliation_name AS name,
                    CAST(NULL AS STRING) AS country,
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
           TRY_CAST(abs(xxhash64(CONCAT('4320321007:zonmw:', LOWER(TRIM(rp.slug))))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM raw_prepped rp
JOIN funder_resolved f ON f.funder_id = 4320321007
WHERE rp.project_number IS NOT NULL
  AND rp.display_name IS NOT NULL
  AND rp.start_year_int IS NOT NULL
)
