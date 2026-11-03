-- Mapping from CreateCyprusRIFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over cyprus_rif_funded_projects_input (was the
-- staging table read from s3://openalex-ingest/awards/cyprus_rif/cyprus_rif_funded_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 rif_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320330084
),
raw_prepped AS (
    SELECT
        *,
        NULLIF(TRIM(funder_award_id), '') AS award_id_clean,
        TRY_CAST(amount AS DOUBLE) AS amount_double,
        TRY_CAST(start_date AS DATE) AS start_date_cast,
        TRY_CAST(signature_date AS DATE) AS signature_date_cast,
        TRY_CAST(TRY_CAST(start_year AS DOUBLE) AS INT) AS start_year_int,
        TRY_CAST(TRY_CAST(end_year AS DOUBLE) AS INT) AS end_year_int,
        NULLIF(TRIM(lead_given_name), '') AS lead_given_clean,
        NULLIF(TRIM(lead_family_name), '') AS lead_family_clean,
        NULLIF(TRIM(executing_institution), '') AS lead_affiliation_name,
        NULLIF(TRIM(programme), '') AS programme_clean
    FROM cyprus_rif_funded_projects_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT('4320330084:', LOWER(TRIM(r.award_id_clean))))) % 9000000000 AS id,
    NULLIF(TRIM(r.display_name), '') AS display_name,
    NULLIF(TRIM(r.description), '') AS description,
    4320330084 AS funder_id,
    r.award_id_clean AS funder_award_id,
    CASE WHEN r.amount_double > 0 THEN r.amount_double ELSE NULL END AS amount,
    CASE WHEN r.amount_double > 0 THEN 'EUR' ELSE NULL END AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    CASE
        WHEN r.programme_clean ILIKE '%DIDAKTOR%' THEN 'fellowship'
        WHEN r.programme_clean ILIKE '%Post-Doctoral%' THEN 'fellowship'
        WHEN r.programme_clean ILIKE '%PhD%' THEN 'fellowship'
        ELSE 'research'
    END AS funding_type,
    r.programme_clean AS funder_scheme,
    'cyprus_rif_funded_projects' AS provenance,
    r.start_date_cast AS start_date,
    CAST(NULL AS DATE) AS end_date,
    CASE WHEN r.start_year_int BETWEEN 1900 AND YEAR(current_date()) + 1 THEN r.start_year_int END AS start_year,
    CASE WHEN r.end_year_int BETWEEN 1900 AND YEAR(current_date()) + 10 THEN r.end_year_int END AS end_year,
    CASE
        WHEN r.lead_family_clean IS NOT NULL OR r.lead_affiliation_name IS NOT NULL THEN
            struct(
                r.lead_given_clean AS given_name,
                r.lead_family_clean AS family_name,
                CAST(NULL AS STRING) AS orcid,
                r.start_date_cast AS role_start,
                struct(
                    r.lead_affiliation_name AS name,
                    CAST(NULL AS STRING) AS country,
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
                ) AS affiliation
            )
        ELSE NULL
    END AS lead_investigator,
    CAST(NULL AS STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >) AS co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >>) AS investigators,
    r.landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT('4320330084:', LOWER(TRIM(r.award_id_clean))))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date,
    212 AS priority
FROM raw_prepped r
CROSS JOIN rif_funder f
WHERE r.award_id_clean IS NOT NULL
  AND NULLIF(TRIM(r.display_name), '') IS NOT NULL
)
