-- Mapping from CreateNEHAwards.ipynb (origin e22fdf7f): its awards-table SELECT over neh_open_data_input (was the
-- staging table read from s3://openalex-ingest/awards/neh/neh_grants.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306100  -- National Endowment for the Humanities
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    n.project_title AS display_name,
    n.description,
    f.funder_id,
    n.funder_award_id,
    TRY_CAST(n.amount_usd AS DOUBLE) AS amount,
    n.currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    CASE
        WHEN LOWER(n.program) LIKE '%fellowship%' THEN 'fellowship'
        WHEN LOWER(n.program) LIKE '%training%' OR LOWER(n.program) LIKE '%institute%' THEN 'training'
        ELSE 'research'
    END AS funding_type,
    n.program AS funder_scheme,
    'neh_open_data' AS provenance,
    TRY_TO_DATE(n.begin_grant, 'yyyy-MM-dd') AS start_date,
    TRY_TO_DATE(n.end_grant, 'yyyy-MM-dd') AS end_date,
    TRY_CAST(n.year_awarded AS INT) AS start_year,
    TRY_CAST(SUBSTRING(n.end_grant, 1, 4) AS INT) AS end_year,
    CASE
        WHEN n.lead_full_name IS NULL OR n.lead_full_name = '' THEN NULL
        ELSE struct(
            n.lead_given_name AS given_name,
            n.lead_family_name AS family_name,
            CAST(NULL AS STRING) AS orcid,
            TRY_TO_DATE(n.begin_grant, 'yyyy-MM-dd') AS role_start,
            struct(
                n.institution AS name,
                n.inst_country AS country,
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
FROM neh_open_data_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
  AND n.project_title IS NOT NULL
)
