-- Mapping from CreateCottrellScholarsAwards.ipynb (origin e22fdf7f): its awards-table SELECT over cottrell_scholars_input (was the
-- staging table read from s3://openalex-ingest/awards/cottrell_scholars/cottrell_scholars.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306487  -- Research Corporation for Science Advancement
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    CONCAT('Cottrell Scholar - ', n.name, ' (', n.discipline, ', ', n.year, ')') AS display_name,
    CONCAT('Cottrell Scholar Award in ', COALESCE(n.discipline, 'science'),
           CASE WHEN n.institution_type IS NOT NULL
                THEN CONCAT(' at ', n.institution, ' (', n.institution_type, ')')
                ELSE CONCAT(' at ', COALESCE(n.institution, '(institution unknown)'))
           END) AS description,
    f.funder_id,
    n.funder_award_id,
    TRY_CAST(n.amount AS DOUBLE) AS amount,
    n.currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    'research' AS funding_type,
    'Cottrell Scholar Award' AS funder_scheme,
    'cottrell_scholars' AS provenance,
    CAST(NULL AS DATE) AS start_date,
    CAST(NULL AS DATE) AS end_date,
    TRY_CAST(n.year AS INT) AS start_year,
    TRY_CAST(n.year AS INT) + 2 AS end_year,  -- 3-year term per program page
    CASE
        WHEN n.name IS NULL OR n.name = '' THEN NULL
        ELSE struct(
            n.given_name AS given_name,
            n.family_name AS family_name,
            CAST(NULL AS STRING) AS orcid,
            CAST(NULL AS DATE) AS role_start,
            struct(
                n.institution AS name,
                n.country AS country,
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
FROM cottrell_scholars_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
  AND n.name IS NOT NULL
)
