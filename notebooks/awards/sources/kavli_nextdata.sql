-- Mapping from CreateKavliPrizeAwards.ipynb (origin e22fdf7f): its awards-table SELECT over kavli_nextdata_input (was the
-- staging table read from s3://openalex-ingest/awards/kavli/kavli_laureates.parquet); created_date and updated_date are the run timestamp.
WITH
 kavli_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306399
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':kavli:', LOWER(s.kavli_laureate_id)))) % 9000000000 AS id,
    CONCAT('Kavli Prize in ', initcap(s.field), ' ', CAST(s.year AS STRING),
           ' — ', s.name) AS display_name,
    NULLIF(s.citation, '') AS description,
    f.funder_id,
    s.kavli_laureate_id AS funder_award_id,
    CAST(NULL AS DOUBLE) AS amount,
    'USD' AS currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) AS id,
        f.display_name, f.ror_id, f.doi
    ) AS funder,
    'prize' AS funding_type,
    initcap(s.field) AS funder_scheme,
    'kavli_nextdata' AS provenance,
    TRY_TO_DATE(CONCAT(CAST(s.year AS STRING), '-01-01'), 'yyyy-MM-dd') AS start_date,
    TRY_TO_DATE(CONCAT(CAST(s.year AS STRING), '-12-31'), 'yyyy-MM-dd') AS end_date,
    s.year AS start_year,
    s.year AS end_year,
    struct(
        s.given_name AS given_name,
        s.family_name AS family_name,
        CAST(NULL AS STRING) AS orcid,
        CAST(NULL AS DATE) AS role_start,
        struct(
            NULLIF(s.institution, '') AS name,
            element_at(s.countries, 1) AS country,
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
    CONCAT('https://kavliprize.org/laureates/', COALESCE(s.slug, s.kavli_laureate_id)) AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':kavli:', LOWER(s.kavli_laureate_id)))) % 9000000000) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM kavli_nextdata_input s
CROSS JOIN kavli_funder f
WHERE s.kavli_laureate_id IS NOT NULL AND s.year IS NOT NULL
)
