-- Mapping from CreateSFIAwards.ipynb (origin e22fdf7f): its awards-table SELECT over sfi_open_data_input (was the
-- staging table read from s3://openalex-ingest/awards/sfi/sfi_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320847   -- Science Foundation Ireland
),
base AS (
    SELECT
        n.funder_award_id, n.title, n.funder_scheme, n.funding_type,
        n.lead_given_name, n.lead_family_name, n.lead_orcid,
        n.institution_name, n.institution_ror, n.country,
        TRY_CAST(n.amount AS DOUBLE) AS amt, n.currency,
        TRY_TO_DATE(n.start_date, 'yyyy-MM-dd') AS sd,
        TRY_TO_DATE(n.end_date,   'yyyy-MM-dd') AS ed,
        f.funder_id, f.display_name AS f_name, f.ror_id, f.doi AS f_doi
    FROM sfi_open_data_input n
    CROSS JOIN funder_resolved f
    WHERE n.funder_award_id IS NOT NULL
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(TRY_CAST(funder_id AS STRING), ':', LOWER(funder_award_id)))) % 9000000000 AS id,
    title AS display_name,
    CAST(NULL AS STRING) AS description,
    funder_id,
    funder_award_id,
    amt AS amount,
    CASE WHEN amt IS NOT NULL THEN currency ELSE NULL END AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(funder_id AS STRING)) AS id,
        f_name AS display_name, ror_id AS ror_id, f_doi AS doi
    ) AS funder,
    funding_type,
    funder_scheme,
    'sfi_open_data' AS provenance,
    sd AS start_date,
    ed AS end_date,
    CASE WHEN YEAR(sd) > YEAR(current_date()) + 1 THEN NULL ELSE YEAR(sd) END AS start_year,
    CASE WHEN YEAR(sd) > YEAR(current_date()) + 1 THEN NULL ELSE YEAR(ed) END AS end_year,
    CASE WHEN lead_given_name IS NOT NULL OR lead_family_name IS NOT NULL OR institution_name IS NOT NULL THEN struct(
        lead_given_name AS given_name,
        lead_family_name AS family_name,
        lead_orcid AS orcid,
        CAST(NULL AS DATE) AS role_start,
        struct(
            institution_name AS name,
            country AS country,
            CASE WHEN institution_ror IS NOT NULL
                 THEN ARRAY(struct(institution_ror AS id, 'ror' AS type, 'sfi' AS asserted_by))
                 ELSE CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) END AS ids
        ) AS affiliation
    ) ELSE NULL END AS lead_investigator,
    CAST(NULL AS STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >) AS co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >>) AS investigators,
    CAST(NULL AS STRING) AS landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(TRY_CAST(funder_id AS STRING), ':', LOWER(funder_award_id)))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM base
)
