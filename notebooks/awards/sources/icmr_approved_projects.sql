-- Mapping from CreateICMRAwards.ipynb (origin e22fdf7f): its awards-table SELECT over icmr_approved_projects_input (was the
-- staging table read from s3://openalex-ingest/awards/icmr/icmr_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320720
),
raw_prepped AS (
    SELECT
        *,
        NULLIF(TRIM(lead_given_name), '') AS leader_given_name,
        NULLIF(TRIM(lead_family_name), '') AS leader_family_name,
        NULLIF(TRIM(institution), '') AS leader_affiliation_name,
        CASE WHEN TRY_CAST(start_year AS INT) BETWEEN 1900 AND 2100
             THEN TRY_CAST(start_year AS INT) END AS start_year_int
    FROM icmr_approved_projects_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT('4320320720:', LOWER(TRIM(rp.funder_award_id))))) % 9000000000 AS id,
    rp.display_name,
    CAST(NULL AS STRING) AS description,
    4320320720 AS funder_id,
    rp.funder_award_id,
    CAST(NULL AS DOUBLE) AS amount,
    CAST(NULL AS STRING) AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    'research' AS funding_type,
    NULLIF(TRIM(rp.subject_area), '') AS funder_scheme,
    'icmr_approved_projects' AS provenance,
    CAST(NULL AS DATE) AS start_date,
    CAST(NULL AS DATE) AS end_date,
    rp.start_year_int AS start_year,
    CAST(NULL AS INT) AS end_year,
    CASE
        WHEN rp.leader_family_name IS NOT NULL OR rp.leader_affiliation_name IS NOT NULL THEN
            struct(
                rp.leader_given_name AS given_name,
                rp.leader_family_name AS family_name,
                CAST(NULL AS STRING) AS orcid,
                CAST(NULL AS DATE) AS role_start,
                struct(
                    rp.leader_affiliation_name AS name,
                    CASE WHEN rp.leader_affiliation_name IS NOT NULL THEN 'IN' ELSE NULL END AS country,
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
    rp.source_pdf_url AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT('4320320720:', LOWER(TRIM(rp.funder_award_id))))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM raw_prepped rp
CROSS JOIN funder_resolved f
WHERE rp.funder_award_id IS NOT NULL
  AND rp.display_name IS NOT NULL
)
