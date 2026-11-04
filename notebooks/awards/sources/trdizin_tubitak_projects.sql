-- Mapping from CreateTUBITAKAwards.ipynb (origin e22fdf7f): its awards-table SELECT over trdizin_tubitak_projects_input (was the
-- staging table read from s3://openalex-ingest/awards/tubitak/tubitak_trdizin_projects.parquet); created_date and updated_date are the run timestamp.
WITH
tubitak_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320322626
),
raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS parsed_end_date,
        CASE
            WHEN TRY_CAST(publication_year AS INT) = 1900 THEN NULL
            WHEN TRY_CAST(publication_year AS INT) > YEAR(current_date()) + 1 THEN NULL
            ELSE TRY_CAST(publication_year AS INT)
        END AS parsed_publication_year,
        FROM_JSON(other_investigators_json, 'ARRAY<STRUCT<name:STRING,given_name:STRING,family_name:STRING,orcid:STRING,duty:STRING,order:STRING,role_start:STRING,institution_name:STRING,institution_title:STRING,institution_root_title:STRING,institution_country:STRING>>') AS other_contributors
    FROM trdizin_tubitak_projects_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND display_name IS NOT NULL
      AND TRIM(display_name) != ''
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000 AS id,
        TRIM(g.display_name) AS display_name,
        CASE WHEN g.description IS NULL OR TRIM(g.description) = '' THEN NULL ELSE TRIM(g.description) END AS description,
        f.funder_id,
        TRIM(g.funder_award_id) AS funder_award_id,
        CAST(NULL AS DOUBLE) AS amount,
        CAST(NULL AS STRING) AS currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) AS id,
            f.display_name,
            f.ror_id,
            f.doi
        ) AS funder,
        'research' AS funding_type,
        COALESCE(NULLIF(TRIM(g.project_group), ''), 'TRDizin PROJECT') AS funder_scheme,
        'trdizin_tubitak_projects' AS provenance,
        g.parsed_start_date AS start_date,
        g.parsed_end_date AS end_date,
        COALESCE(YEAR(g.parsed_start_date), g.parsed_publication_year) AS start_year,
        COALESCE(YEAR(g.parsed_end_date), g.parsed_publication_year) AS end_year,
        CASE
            WHEN g.lead_name IS NULL OR TRIM(g.lead_name) = '' THEN CAST(NULL AS STRUCT<
                given_name:STRING,
                family_name:STRING,
                orcid:STRING,
                role_start:DATE,
                affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
            >)
            ELSE struct(
                NULLIF(TRIM(g.lead_given_name), '') AS given_name,
                NULLIF(TRIM(g.lead_family_name), '') AS family_name,
                NULLIF(TRIM(g.lead_orcid), '') AS orcid,
                g.parsed_start_date AS role_start,
                struct(
                    CAST(NULL AS STRING) AS name,
                    CAST(NULL AS STRING) AS country,
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
                ) AS affiliation
            )
        END AS lead_investigator,
        CAST(NULL AS STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) AS co_lead_investigator,
        CASE
            WHEN g.other_contributors IS NULL OR SIZE(g.other_contributors) = 0 THEN CAST(NULL AS ARRAY<STRUCT<
                given_name:STRING,
                family_name:STRING,
                orcid:STRING,
                role_start:DATE,
                affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
            >>)
            ELSE TRANSFORM(g.other_contributors, c -> struct(
                NULLIF(TRIM(c.given_name), '') AS given_name,
                NULLIF(TRIM(c.family_name), '') AS family_name,
                NULLIF(TRIM(c.orcid), '') AS orcid,
                g.parsed_start_date AS role_start,
                struct(
                    CAST(NULL AS STRING) AS name,
                    CAST(NULL AS STRING) AS country,
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) AS ids
                ) AS affiliation
            ))
        END AS investigators,
        NULLIF(TRIM(g.landing_page_url), '') AS landing_page_url,
        CAST(NULL AS STRING) AS doi,
        CONCAT('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000) AS works_api_url,
        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date
    FROM raw_prepared g
    CROSS JOIN tubitak_funder f
)
