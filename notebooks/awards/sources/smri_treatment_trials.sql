-- Mapping from CreateSMRITreatmentTrialsAwards.ipynb (origin e22fdf7f): its awards-table SELECT over smri_treatment_trials_input (was the
-- staging table read from s3://openalex-ingest/awards/smri_treatment_trials/smri_treatment_trials.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320309530
),
raw_prepped AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        NULLIF(TRIM(display_name), '') AS display_name_norm,
        NULLIF(TRIM(description), '') AS description_norm,
        NULLIF(TRIM(status), '') AS status_norm,
        NULLIF(TRIM(lead_name), '') AS lead_name_norm,
        NULLIF(TRIM(lead_given_name), '') AS lead_given_name_norm,
        NULLIF(TRIM(lead_family_name), '') AS lead_family_name_norm,
        NULLIF(TRIM(co_lead_name), '') AS co_lead_name_norm,
        NULLIF(TRIM(co_lead_given_name), '') AS co_lead_given_name_norm,
        NULLIF(TRIM(co_lead_family_name), '') AS co_lead_family_name_norm,
        NULLIF(TRIM(institution), '') AS institution_norm,
        NULLIF(TRIM(country_code), '') AS country_code_norm,
        NULLIF(TRIM(landing_page_url), '') AS landing_page_url_norm,
        CASE
            WHEN TRY_CAST(award_year AS INT) BETWEEN 1900 AND YEAR(current_date()) + 1
                THEN TRY_CAST(award_year AS INT)
            ELSE NULL
        END AS award_year_int,
        from_json(
            other_investigators,
            'ARRAY<STRUCT<name:STRING,given_name:STRING,family_name:STRING>>'
        ) AS other_investigators_array
    FROM smri_treatment_trials_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND display_name IS NOT NULL
      AND TRIM(display_name) != ''
),
mapped_awards AS (
    SELECT
        abs(xxhash64(CONCAT(TRY_CAST(f.funder_id AS STRING), ':', n.native_award_id))) % 9000000000 AS id,
        n.display_name_norm AS display_name,
        n.description_norm AS description,
        f.funder_id,
        TRIM(n.funder_award_id) AS funder_award_id,
        CAST(NULL AS DOUBLE) AS amount,
        CAST(NULL AS STRING) AS currency,
        struct(
            CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
            f.display_name,
            f.ror_id,
            f.doi
        ) AS funder,
        'research' AS funding_type,
        'Treatment Trials' AS funder_scheme,
        'smri_treatment_trials' AS provenance,
        CAST(NULL AS DATE) AS start_date,
        CAST(NULL AS DATE) AS end_date,
        CASE WHEN regexp_extract(n.funder_award_id, '^([0-9]{2})', 1) <> '' THEN (CASE WHEN CAST(regexp_extract(n.funder_award_id,'^([0-9]{2})',1) AS INT) >= 90 THEN 1900 ELSE 2000 END) + CAST(regexp_extract(n.funder_award_id,'^([0-9]{2})',1) AS INT) ELSE n.award_year_int END AS start_year,
        CAST(NULL AS INT) AS end_year,
        CASE
            WHEN n.lead_name_norm IS NOT NULL OR n.institution_norm IS NOT NULL OR n.country_code_norm IS NOT NULL THEN
                struct(
                    n.lead_given_name_norm AS given_name,
                    n.lead_family_name_norm AS family_name,
                    CAST(NULL AS STRING) AS orcid,
                    CAST(NULL AS DATE) AS role_start,
                    struct(
                        n.institution_norm AS name,
                        n.country_code_norm AS country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
                    ) AS affiliation
                )
            ELSE CAST(NULL AS STRUCT<
                given_name:STRING,
                family_name:STRING,
                orcid:STRING,
                role_start:DATE,
                affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
            >)
        END AS lead_investigator,
        CASE
            WHEN n.co_lead_name_norm IS NOT NULL THEN
                struct(
                    n.co_lead_given_name_norm AS given_name,
                    n.co_lead_family_name_norm AS family_name,
                    CAST(NULL AS STRING) AS orcid,
                    CAST(NULL AS DATE) AS role_start,
                    struct(
                        n.institution_norm AS name,
                        n.country_code_norm AS country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
                    ) AS affiliation
                )
            ELSE CAST(NULL AS STRUCT<
                given_name:STRING,
                family_name:STRING,
                orcid:STRING,
                role_start:DATE,
                affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
            >)
        END AS co_lead_investigator,
        CASE
            WHEN n.other_investigators_array IS NOT NULL THEN
                transform(
                    n.other_investigators_array,
                    x -> struct(
                        NULLIF(TRIM(x.given_name), '') AS given_name,
                        NULLIF(TRIM(x.family_name), '') AS family_name,
                        CAST(NULL AS STRING) AS orcid,
                        CAST(NULL AS DATE) AS role_start,
                        struct(
                            n.institution_norm AS name,
                            n.country_code_norm AS country,
                            CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
                        ) AS affiliation
                    )
                )
            ELSE CAST(NULL AS ARRAY<STRUCT<
                given_name:STRING,
                family_name:STRING,
                orcid:STRING,
                role_start:DATE,
                affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
            >>)
        END AS investigators,
        n.landing_page_url_norm AS landing_page_url,
        CAST(NULL AS STRING) AS doi,
        CONCAT(
            'https://api.openalex.org/works?filter=awards.id:G',
            TRY_CAST(abs(xxhash64(CONCAT(TRY_CAST(f.funder_id AS STRING), ':', n.native_award_id))) % 9000000000 AS STRING)
        ) AS works_api_url,
        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date
    FROM raw_prepped n
    CROSS JOIN funder_resolved f
),

awards_transformed AS (
SELECT *
FROM mapped_awards
)
