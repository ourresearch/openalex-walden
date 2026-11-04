-- Mapping from CreateKlingensteinSimonsAwards.ipynb (origin e22fdf7f): its awards-table SELECT over klingenstein_simons_input (was the
-- staging table read from s3://openalex-ingest/awards/klingenstein_simons/klingenstein_simons_fellows.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306403  -- Esther A. and Joseph Klingenstein Fund
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    CONCAT(n.scheme_label, ' - ', n.name,
           CASE WHEN n.year IS NOT NULL THEN CONCAT(' (', n.year, ')') ELSE '' END) AS display_name,
    n.research_title AS description,
    f.funder_id,
    n.funder_award_id,
    CAST(NULL AS DOUBLE) AS amount,
    CAST(NULL AS STRING) AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    'fellowship' AS funding_type,
    n.scheme_label AS funder_scheme,
    'klingenstein_simons' AS provenance,
    CAST(NULL AS DATE) AS start_date,
    CAST(NULL AS DATE) AS end_date,
    TRY_CAST(n.year AS INT) AS start_year,
    CAST(NULL AS INT) AS end_year,
    CASE
        WHEN n.name IS NULL OR n.name = '' THEN NULL
        WHEN n.name RLIKE '(?i)Ebert Clinical Scholar' THEN NULL  -- award designation captured as person, not a real PI (§6.4a, scraper fix tracked separately)
        ELSE struct(
            n.given_name AS given_name,
            n.family_name AS family_name,
            CAST(NULL AS STRING) AS orcid,
            CAST(NULL AS DATE) AS role_start,
            struct(
                n.institution AS name,
                CAST('US' AS STRING) AS country,  -- program eligibility limits to US institutions per the foundation page
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
FROM klingenstein_simons_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
  AND n.name IS NOT NULL
)
