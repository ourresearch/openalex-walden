-- Mapping from CreateResearchCouncilNorwayAwards.ipynb (origin e22fdf7f): its awards-table SELECT over research_council_norway_project_bank_input (was the
-- staging table read from s3://openalex-ingest/awards/research_council_norway/research_council_norway_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320323299
),
raw_prepped AS (
    SELECT
        *,
        TRY_CAST(total_funding AS DOUBLE) AS amount_double,
        CASE
            WHEN YEAR(TRY_TO_DATE(start_date, 'yyyy-MM-dd')) BETWEEN 1800 AND 2100
                THEN TRY_TO_DATE(start_date, 'yyyy-MM-dd')
            ELSE NULL
        END AS start_date_cast,
        CASE
            WHEN YEAR(TRY_TO_DATE(end_date, 'yyyy-MM-dd')) BETWEEN 1800 AND 2100
                THEN TRY_TO_DATE(end_date, 'yyyy-MM-dd')
            ELSE NULL
        END AS end_date_cast,
        CASE WHEN TRY_CAST(start_year AS INT) BETWEEN 1800 AND 2100 THEN TRY_CAST(start_year AS INT) END AS start_year_int,
        CASE WHEN TRY_CAST(end_year AS INT) BETWEEN 1800 AND 2100 THEN TRY_CAST(end_year AS INT) END AS end_year_int,
        NULLIF(TRIM(lead_given_name), '') AS leader_given_name,
        NULLIF(TRIM(lead_family_name), '') AS leader_family_name,
        NULLIF(TRIM(lead_name), '') AS leader_full_name,
        NULLIF(TRIM(primary_organization), '') AS leader_affiliation_name,
        CONCAT_WS('\n\n',
            CASE WHEN NULLIF(TRIM(pop_sci_description), '') IS NOT NULL THEN CONCAT('Popular science description: ', TRIM(pop_sci_description)) END,
            CASE WHEN NULLIF(TRIM(project_summary), '') IS NOT NULL THEN CONCAT('Project summary: ', TRIM(project_summary)) END,
            CASE WHEN NULLIF(TRIM(teaser), '') IS NOT NULL THEN CONCAT('Teaser: ', TRIM(teaser)) END,
            CASE WHEN NULLIF(TRIM(disciplines_json), '') IS NOT NULL THEN CONCAT('Disciplines: ', TRIM(disciplines_json)) END,
            CASE WHEN NULLIF(TRIM(geographies_json), '') IS NOT NULL THEN CONCAT('Geographies: ', TRIM(geographies_json)) END
        ) AS award_description
    FROM research_council_norway_project_bank_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT('4320323299:', LOWER(TRIM(rp.funder_award_id))))) % 9000000000 AS id,
    rp.display_name,
    NULLIF(rp.award_description, '') AS description,
    4320323299 AS funder_id,
    rp.funder_award_id,
    rp.amount_double AS amount,
    CASE WHEN rp.amount_double IS NOT NULL THEN 'NOK' ELSE NULL END AS currency,
    struct(
        CONCAT('https://openalex.org/F', TRY_CAST(f.funder_id AS STRING)) AS id,
        f.display_name,
        f.ror_id,
        f.doi
    ) AS funder,
    'research' AS funding_type,
    NULLIF(TRIM(rp.current_activity_code), '') AS funder_scheme,
    'research_council_norway_project_bank' AS provenance,
    rp.start_date_cast AS start_date,
    rp.end_date_cast AS end_date,
    rp.start_year_int AS start_year,
    rp.end_year_int AS end_year,
    CASE
        WHEN rp.leader_full_name IS NOT NULL OR rp.leader_affiliation_name IS NOT NULL THEN
            struct(
                rp.leader_given_name AS given_name,
                COALESCE(rp.leader_family_name, rp.leader_full_name) AS family_name,
                CAST(NULL AS STRING) AS orcid,
                rp.start_date_cast AS role_start,
                struct(
                    rp.leader_affiliation_name AS name,
                    CASE WHEN rp.leader_affiliation_name IS NOT NULL THEN 'NO' ELSE NULL END AS country,
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
    rp.landing_page_url,
    CAST(NULL AS STRING) AS doi,
    CONCAT('https://api.openalex.org/works?filter=awards.id:G',
           TRY_CAST(abs(xxhash64(CONCAT('4320323299:', LOWER(TRIM(rp.funder_award_id))))) % 9000000000 AS STRING)) AS works_api_url,
    session.awards_timestamp AS created_date,
    session.awards_timestamp AS updated_date
FROM raw_prepped rp
JOIN funder_resolved f ON f.funder_id = 4320323299
WHERE rp.funder_award_id IS NOT NULL
  AND rp.display_name IS NOT NULL
)
