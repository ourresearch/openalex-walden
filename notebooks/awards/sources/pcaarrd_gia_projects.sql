-- Mapping from CreatePCAARRDAwards.ipynb (origin e22fdf7f): its awards-table SELECT over pcaarrd_gia_projects_input (was the
-- staging table read from s3://openalex-ingest/awards/pcaarrd/pcaarrd_gia_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 normalized AS (
    SELECT
        *,
        TRY_CAST(funder_id AS BIGINT) AS funder_id_bigint,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS start_date_parsed,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS end_date_parsed,
        TRY_CAST(amount AS DOUBLE) AS amount_double,
        NULLIF(TRIM(source_implementing_agency), '') AS implementing_agency_clean
    FROM pcaarrd_gia_projects_input
    WHERE funder_award_id IS NOT NULL
      AND display_name IS NOT NULL
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(TRY_CAST(funder_id_bigint AS STRING), ':', LOWER(funder_award_id)))) % 9000000000 AS id,
    display_name,
    description,
    funder_id_bigint AS funder_id,
    funder_award_id,
    amount_double AS amount,
    currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(funder_id_bigint AS STRING)) AS id,
        'Philippine Council for Agriculture, Aquatic and Natural Resources Research and Development' AS display_name,
        CAST(NULL AS STRING) AS ror_id,
        CAST(NULL AS STRING) AS doi
    ) AS funder,
    'grant' AS funding_type,
    program_title AS funder_scheme,
    'pcaarrd_gia_projects' AS provenance,
    start_date_parsed AS start_date,
    end_date_parsed AS end_date,
    YEAR(start_date_parsed) AS start_year,
    YEAR(end_date_parsed) AS end_year,
    CASE
        WHEN implementing_agency_clean IS NULL THEN NULL
        ELSE struct(
            CAST(NULL AS STRING) AS given_name,
            CAST(NULL AS STRING) AS family_name,
            CAST(NULL AS STRING) AS orcid,
            start_date_parsed AS role_start,
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
    source_pdf_url AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(
               TRY_CAST(funder_id_bigint AS STRING), ':', LOWER(funder_award_id)
           ))) % 9000000000 AS STRING)) AS works_api_url,
    CAST(NULL AS BOOLEAN) AS declined,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM normalized
)
