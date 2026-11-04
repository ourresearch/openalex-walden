-- Mapping from CreateRockefellerAwards.ipynb (origin e22fdf7f): its awards-table SELECT over rockefeller_wp_input (was the
-- staging table read from s3://openalex-ingest/awards/rockefeller/rockefeller_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 rockefeller_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306149
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.slug)))) % 9000000000 AS id,
    g.title AS display_name,
    NULLIF(g.description_raw, '') AS description,
    f.funder_id,
    g.slug AS funder_award_id,
    TRY_CAST(g.amount_usd AS DOUBLE) AS amount,
    g.currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    'grant' AS funding_type,
    NULLIF(g.focus_area_raw, '') AS funder_scheme,
    'rockefeller_wp' AS provenance,
    TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') AS start_date,
    TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') AS end_date,
    YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) AS start_year,
    YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) AS end_year,
    struct(
        CAST(NULL AS STRING) AS given_name,
        CAST(NULL AS STRING) AS family_name,
        CAST(NULL AS STRING) AS orcid,
        CAST(NULL AS DATE) AS role_start,
        struct(
            COALESCE(NULLIF(g.grantee_raw, ''), g.title) AS name,
            CAST(NULL AS STRING) AS country,
            CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
        ) AS affiliation
    ) AS lead_investigator,
    CAST(NULL AS STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >) AS co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >>) AS investigators,
    g.url AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.slug)))) % 9000000000) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM rockefeller_wp_input g
CROSS JOIN rockefeller_funder f
WHERE g.slug IS NOT NULL AND TRIM(g.slug) != ''
)
