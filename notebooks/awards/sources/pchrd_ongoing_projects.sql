-- Mapping from CreatePCHRDAwards.ipynb (origin e22fdf7f): its awards-table SELECT over pchrd_ongoing_projects_input (was the
-- staging table read from s3://openalex-ingest/awards/pchrd/pchrd_ongoing_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 normalized AS (
    SELECT
        *,
        TRY_CAST(funder_id AS BIGINT) AS funder_id_bigint,
        TRY_TO_DATE(source_posted_date, 'yyyy-MM-dd') AS source_posted_date_parsed,
        NULLIF(TRIM(source_implementing_agency), '') AS implementing_agency_clean,
        NULLIF(TRIM(project_category), '') AS project_category_clean,
        NULLIF(TRIM(description), '') AS description_clean
    FROM pchrd_ongoing_projects_input
    WHERE funder_award_id IS NOT NULL
      AND display_name IS NOT NULL
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(TRY_CAST(funder_id_bigint AS STRING), ':', LOWER(funder_award_id)))) % 9000000000 AS id,
    display_name,
    description_clean AS description,
    funder_id_bigint AS funder_id,
    funder_award_id,
    CAST(NULL AS DOUBLE) AS amount,
    CAST(NULL AS STRING) AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(funder_id_bigint AS STRING)) AS id,
        'Philippine Council for Health Research and Development' AS display_name,
        CAST(NULL AS STRING) AS ror_id,
        CAST(NULL AS STRING) AS doi
    ) AS funder,
    'research' AS funding_type,
    project_category_clean AS funder_scheme,
    'pchrd_ongoing_projects' AS provenance,
    source_posted_date_parsed AS start_date,
    CAST(NULL AS DATE) AS end_date,
    YEAR(source_posted_date_parsed) AS start_year,
    CAST(NULL AS INT) AS end_year,
    CASE
        WHEN implementing_agency_clean IS NULL THEN NULL
        ELSE struct(
            CAST(NULL AS STRING) AS given_name,
            CAST(NULL AS STRING) AS family_name,
            CAST(NULL AS STRING) AS orcid,
            source_posted_date_parsed AS role_start,
            struct(
                implementing_agency_clean AS name,
                'PH' AS country,
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
    landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(
               TRY_CAST(funder_id_bigint AS STRING), ':', LOWER(funder_award_id)
           ))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM normalized
)
