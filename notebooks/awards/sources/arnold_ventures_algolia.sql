-- Mapping from CreateArnoldVenturesAwards.ipynb (origin e22fdf7f): its awards-table SELECT over arnold_ventures_algolia_input (was the
-- staging table read from s3://openalex-ingest/awards/arnold_ventures/arnold_ventures_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 arnold_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320315359
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.objectID)))) % 9000000000 AS id,
    g.title AS display_name,
    COALESCE(NULLIF(g.grant_description, ''), NULLIF(g.grant_body, '')) AS description,
    f.funder_id,
    g.objectID AS funder_award_id,
    TRY_CAST(g.grant_amount AS DOUBLE) AS amount,
    'USD' AS currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    'research' AS funding_type,
    NULLIF(g.funding_source, '') AS funder_scheme,
    'arnold_ventures_algolia' AS provenance,
    CASE WHEN g.start_year IS NOT NULL
         THEN TRY_TO_DATE(CONCAT(g.start_year, '-01-01'), 'yyyy-MM-dd') END AS start_date,
    CASE WHEN g.end_year IS NOT NULL
         THEN TRY_TO_DATE(CONCAT(g.end_year, '-12-31'), 'yyyy-MM-dd') END AS end_date,
    TRY_CAST(g.start_year AS INT) AS start_year,
    TRY_CAST(g.end_year AS INT) AS end_year,
    struct(
        CAST(NULL AS STRING) AS given_name,
        CAST(NULL AS STRING) AS family_name,
        CAST(NULL AS STRING) AS orcid,
        CAST(NULL AS DATE) AS role_start,
        struct(
            NULLIF(g.grantee_name, '') AS name,
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
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.objectID)))) % 9000000000) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM arnold_ventures_algolia_input g
CROSS JOIN arnold_funder f
WHERE g.objectID IS NOT NULL AND TRIM(g.objectID) != ''
)
