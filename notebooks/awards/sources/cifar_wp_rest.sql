-- Mapping from CreateCIFARAwards.ipynb (origin e22fdf7f): its awards-table SELECT over cifar_wp_rest_input (was the
-- staging table read from s3://openalex-ingest/awards/cifar/cifar_appointments.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320309949  -- Canadian Institute for Advanced Research
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(
        TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
    ))) % 9000000000 AS id,
    CONCAT('CIFAR ', n.program_name, ' \u2014 ', n.researcher_full_name) AS display_name,
    CASE
        WHEN TRY_CAST(n.declined AS BOOLEAN) = TRUE AND n.role_name IS NOT NULL
            THEN CONCAT('Declined the appointment. Role: ', n.role_name)
        WHEN TRY_CAST(n.declined AS BOOLEAN) = TRUE
            THEN 'Declined the appointment.'
        WHEN n.role_name IS NOT NULL
            THEN CONCAT(n.role_name, ' in CIFAR ', n.program_name, '.')
        ELSE CONCAT('Appointment in CIFAR ', n.program_name, '.')
    END AS description,
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
    CASE
        WHEN LOWER(n.role_name) RLIKE 'advisor|advisory|committee' THEN 'other'
        WHEN LOWER(n.role_name) RLIKE 'fellow|chair|scholar|investigator|co-?director|director'
            THEN 'research'
        ELSE 'other'
    END AS funding_type,
    n.program_name AS funder_scheme,
    'cifar_wp_rest' AS provenance,
    TRY_TO_DATE(SUBSTRING(n.first_seen_date, 1, 10), 'yyyy-MM-dd') AS start_date,
    CAST(NULL AS DATE) AS end_date,
    CAST(SUBSTRING(n.first_seen_date, 1, 4) AS INT) AS start_year,
    CAST(NULL AS INT) AS end_year,
    struct(
        n.researcher_given AS given_name,
        n.researcher_family AS family_name,
        CAST(NULL AS STRING) AS orcid,
        CAST(NULL AS DATE) AS role_start,
        struct(
            n.institution_name AS name,
            n.country_name AS country,
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
    n.landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(
               TRY_CAST(f.funder_id AS STRING), ':', LOWER(n.funder_award_id)
           ))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM cifar_wp_rest_input n
CROSS JOIN funder_resolved f
WHERE n.funder_award_id IS NOT NULL
  AND n.program_name IS NOT NULL
  AND n.researcher_full_name IS NOT NULL
)
