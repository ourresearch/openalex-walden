-- Mapping from CreateMBIEAwards.ipynb (origin e22fdf7f): its awards-table SELECT over mbie_who_got_funded_input (was the
-- staging table read from s3://openalex-ingest/awards/mbie/mbie_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321983
),
raw_prepped AS (
    SELECT
        *,
        TRY_CAST(amount AS DOUBLE) AS amount_double,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS start_date_parsed,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS end_date_parsed,
        NULLIF(TRIM(organisation), '') AS leader_affiliation_name,
        NULLIF(TRIM(display_name), '') AS display_name_norm,
        NULLIF(TRIM(description), '') AS description_norm,
        NULLIF(TRIM(funder_scheme), '') AS funder_scheme_norm
    FROM mbie_who_got_funded_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT('4320321983:', LOWER(TRIM(rp.funder_award_id))))) % 9000000000 AS id,
    rp.display_name_norm AS display_name,
    rp.description_norm AS description,
    4320321983 AS funder_id,
    rp.funder_award_id,
    CASE WHEN rp.amount_double > 0 THEN rp.amount_double ELSE NULL END AS amount,
    CASE WHEN rp.amount_double > 0 THEN 'NZD' ELSE NULL END AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    CASE
        WHEN LOWER(COALESCE(rp.funder_scheme_norm, '')) RLIKE 'fellowship|scholarship|whitinga' THEN 'fellowship'
        WHEN LOWER(COALESCE(rp.funder_scheme_norm, '')) RLIKE 'studentship|internship|training' THEN 'training'
        ELSE 'research'
    END AS funding_type,
    rp.funder_scheme_norm AS funder_scheme,
    'mbie_who_got_funded' AS provenance,
    rp.start_date_parsed AS start_date,
    rp.end_date_parsed AS end_date,
    CASE
        WHEN YEAR(rp.start_date_parsed) > YEAR(current_date()) + 1 THEN NULL
        ELSE YEAR(rp.start_date_parsed)
    END AS start_year,
    CASE
        WHEN YEAR(rp.start_date_parsed) > YEAR(current_date()) + 1 THEN NULL
        ELSE YEAR(rp.end_date_parsed)
    END AS end_year,
    CASE
        WHEN rp.leader_affiliation_name IS NOT NULL THEN
            struct(
                CAST(NULL AS STRING) AS given_name,
                CAST(NULL AS STRING) AS family_name,
                CAST(NULL AS STRING) AS orcid,
                CAST(NULL AS DATE) AS role_start,
                struct(
                    rp.leader_affiliation_name AS name,
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
    rp.source_page_url AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT('4320321983:', LOWER(TRIM(rp.funder_award_id))))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM raw_prepped rp
CROSS JOIN funder_resolved f
WHERE rp.funder_award_id IS NOT NULL
  AND rp.display_name_norm IS NOT NULL
)
