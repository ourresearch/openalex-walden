-- Mapping from CreateAHAAwards.ipynb (origin e22fdf7f): its awards-table SELECT over aha_report_builder_input (was the
-- staging table read from s3://openalex-ingest/awards/aha/aha_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306230  -- American Heart Association
),
parsed AS (
    SELECT
        a.*,
        from_json(
            a.investigators_json,
            'array<struct<given_name:string,family_name:string,orcid:string,role_start:date,affiliation:struct<name:string,country:string,ids:array<struct<id:string,type:string,asserted_by:string>>>>>'
        ) AS investigators_arr
    FROM aha_report_builder_input a
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(p.funder_award_id)
    ))) % 9000000000 AS id,
    p.title AS display_name,
    p.description,                                 -- Technical abstract (General fallback)
    f.funder_id,
    p.funder_award_id,
    TRY_CAST(p.amount AS DOUBLE) AS amount,
    p.currency,                                    -- 'USD' from script, NULL if no amount
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    p.funding_type,                                -- research / fellowship / training
    p.funder_scheme,                               -- AHA funding mechanism
    'aha_report_builder' AS provenance,
    TRY_CAST(p.start_date AS DATE) AS start_date,  -- exact award dates (published)
    TRY_CAST(p.end_date AS DATE) AS end_date,
    TRY_CAST(p.start_year AS INT) AS start_year,
    TRY_CAST(p.end_year AS INT) AS end_year,
    try_element_at(p.investigators_arr, 1) AS lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) AS co_lead_investigator,
    p.investigators_arr AS investigators,          -- all PIs (multi-PI collaboratives)
    p.landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(
               TRY_CAST(f.funder_id AS STRING), ':', LOWER(p.funder_award_id)
           ))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM parsed p
CROSS JOIN funder_resolved f
WHERE p.funder_award_id IS NOT NULL
  AND p.title IS NOT NULL
)
