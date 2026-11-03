-- Mapping from CreateBELSPOAwards.ipynb (origin e22fdf7f): its awards-table SELECT over belspo_fris_input (was the
-- staging table read from s3://openalex-ingest/awards/belspo/belspo_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321454   -- BELSPO (Flemish Government) (BELSPO)
),
base AS (
    SELECT
        n.funder_award_id,
        n.title, n.abstract, n.funder_scheme, n.funding_type,
        n.lead_given_name, n.lead_family_name,
        n.colead_given_name, n.colead_family_name,
        n.institution_name, n.country, n.landing_page_url,
        TRY_TO_DATE(n.start_date, 'yyyy-MM-dd') AS sd,
        TRY_TO_DATE(n.end_date,   'yyyy-MM-dd') AS ed,
        f.funder_id, f.display_name AS f_name, f.ror_id, f.doi AS f_doi
    FROM belspo_fris_input n
    CROSS JOIN funder_resolved f
    WHERE n.funder_award_id IS NOT NULL
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(TRY_CAST(funder_id AS STRING), ':', LOWER(funder_award_id)))) % 9000000000 AS id,
    title AS display_name,
    abstract AS description,
    funder_id,
    funder_award_id,
    CAST(NULL AS DOUBLE) AS amount,
    CAST(NULL AS STRING) AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(funder_id AS STRING)) AS id,
        f_name AS display_name, ror_id AS ror_id, f_doi AS doi
    ) AS funder,
    funding_type,
    funder_scheme,
    'belspo_fris' AS provenance,
    sd AS start_date,
    ed AS end_date,
    CASE WHEN YEAR(sd) > YEAR(current_date()) + 1 THEN NULL ELSE YEAR(sd) END AS start_year,
    CASE WHEN YEAR(sd) > YEAR(current_date()) + 1 THEN NULL ELSE YEAR(ed) END AS end_year,
    CASE WHEN lead_given_name IS NOT NULL OR lead_family_name IS NOT NULL THEN struct(
        lead_given_name AS given_name, lead_family_name AS family_name,
        CAST(NULL AS STRING) AS orcid, CAST(NULL AS DATE) AS role_start,
        struct(institution_name AS name, country AS country,
               CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids) AS affiliation
    ) ELSE NULL END AS lead_investigator,
    CASE WHEN colead_given_name IS NOT NULL OR colead_family_name IS NOT NULL THEN struct(
        colead_given_name AS given_name, colead_family_name AS family_name,
        CAST(NULL AS STRING) AS orcid, CAST(NULL AS DATE) AS role_start,
        struct(institution_name AS name, country AS country,
               CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids) AS affiliation
    ) ELSE NULL END AS co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >>) AS investigators,
    landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT(TRY_CAST(funder_id AS STRING), ':', LOWER(funder_award_id)))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM base
)
