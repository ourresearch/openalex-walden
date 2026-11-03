-- Mapping from CreateCONAHCYTAwards.ipynb (origin e22fdf7f): its awards-table SELECT over conahcyt_snii_ckan_input (was the
-- staging table read from s3://openalex-ingest/awards/conahcyt/conahcyt_snii.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321739  -- CONAHCYT / SECIHTI (CONACYT-era display name in OpenAlex)
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    CONCAT(n.snii_level_label, ' \u2014 ', n.researcher_full_name) AS display_name,
    CASE
        WHEN n.area_conocimiento IS NOT NULL AND n.institution IS NOT NULL
            THEN CONCAT('SNII appointment in ', n.area_conocimiento, ' at ', n.institution, '.')
        WHEN n.area_conocimiento IS NOT NULL
            THEN CONCAT('SNII appointment in ', n.area_conocimiento, '.')
        WHEN n.institution IS NOT NULL
            THEN CONCAT('SNII appointment at ', n.institution, '.')
        ELSE 'SNII appointment.'
    END AS description,
    f.funder_id,
    n.funder_award_id,
    TRY_CAST(n.amount_mxn AS DOUBLE) AS amount,
    n.currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    CASE
        WHEN UPPER(n.snii_level) = 'C' THEN 'fellowship'
        ELSE 'research'
    END AS funding_type,
    n.snii_level_label AS funder_scheme,
    'conahcyt_snii_ckan' AS provenance,
    TRY_TO_DATE(n.start_date, 'yyyy-MM-dd') AS start_date,
    TRY_TO_DATE(n.end_date, 'yyyy-MM-dd') AS end_date,
    TRY_CAST(SUBSTRING(n.start_date, 1, 4) AS INT) AS start_year,
    TRY_CAST(SUBSTRING(n.end_date, 1, 4) AS INT) AS end_year,
    struct(
        n.given_name AS given_name,
        n.family_name AS family_name,
        CAST(NULL AS STRING) AS orcid,
        TRY_TO_DATE(n.start_date, 'yyyy-MM-dd') AS role_start,
        struct(
            n.institution AS name,
            CAST('MX' AS STRING) AS country,  -- SNII members are by definition Mexico-affiliated
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
    n.landing_page_url,  -- NULL: CONAHCYT does not expose per-SNII-researcher public landing pages
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(
               TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
           ))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM conahcyt_snii_ckan_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
  AND n.cvu IS NOT NULL
  AND n.snii_level IS NOT NULL
)
