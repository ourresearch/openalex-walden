-- Mapping from CreateBHFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over bhf_annual_reports_input (was the
-- staging table read from s3://openalex-ingest/awards/bhf/bhf_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320319992  -- British Heart Foundation
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(b.funder_award_id)
    ))) % 9000000000 AS id,
    b.title AS display_name,
    CAST(NULL AS STRING) AS description,           -- BHF publishes no abstracts
    f.funder_id,
    b.funder_award_id,
    TRY_CAST(b.amount AS DOUBLE) AS amount,
    b.currency,                                    -- 'GBP' from script, NULL if no amount
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    b.funding_type,                                -- research / fellowship / training
    b.funder_scheme,                               -- committee/scheme heading
    'bhf_annual_reports' AS provenance,
    CAST(NULL AS DATE) AS start_date,              -- no exact award date published
    CAST(NULL AS DATE) AS end_date,
    TRY_CAST(b.report_year_start AS INT) AS start_year,   -- FY the award was made
    CASE
        WHEN TRY_CAST(b.report_year_start AS INT) IS NOT NULL
         AND TRY_CAST(b.duration_months AS DOUBLE) IS NOT NULL
        THEN TRY_CAST(b.report_year_start AS INT)
             + CAST(CEIL(TRY_CAST(b.duration_months AS DOUBLE) / 12.0) AS INT)
        ELSE NULL
    END AS end_year,
    CASE
        WHEN b.lead_family_name IS NULL OR b.lead_family_name = '' THEN NULL
        ELSE struct(
            b.lead_given_name AS given_name,
            b.lead_family_name AS family_name,
            CAST(NULL AS STRING) AS orcid,         -- BHF publishes no ORCIDs
            CAST(NULL AS DATE) AS role_start,
            struct(
                b.institution AS name,
                CASE WHEN b.funder_scheme ILIKE '%International%'
                     THEN CAST(NULL AS STRING) ELSE 'GB' END AS country,
                CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
            ) AS affiliation
        )
    END AS lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING,
        ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) AS co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING,
        ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) AS investigators,
    b.landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(
               TRY_CAST(f.funder_id AS STRING), ':', LOWER(b.funder_award_id)
           ))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM bhf_annual_reports_input b
CROSS JOIN funder_resolved f
WHERE b.funder_award_id IS NOT NULL
  AND b.title IS NOT NULL
)
