-- Mapping from CreateFAPEMIGAwards.ipynb (origin e22fdf7f): its awards-table SELECT over fapemig_projetos_contratados_input (was the
-- staging table read from s3://openalex-ingest/awards/fapemig/fapemig_projetos_contratados.parquet); created_date and updated_date are the run timestamp.
WITH
fapemig_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320322980
),
raw_prepared AS (
    SELECT
        *,
        UPPER(REGEXP_REPLACE(TRIM(funder_award_id), '^(?i)fapemig-', '')) AS native_award_id,
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        NULLIF(TRIM(currency), '') AS parsed_currency,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS parsed_end_date,
        TRY_CAST(source_year AS INT) AS parsed_year
    FROM fapemig_projetos_contratados_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND display_name IS NOT NULL
      AND TRIM(display_name) != ''
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.native_award_id)))) % 9000000000 AS id,
        TRIM(g.display_name) AS display_name,
        CASE WHEN g.description IS NULL OR TRIM(g.description) = '' THEN NULL ELSE TRIM(g.description) END AS description,
        f.funder_id,
        g.native_award_id AS funder_award_id,
        g.parsed_amount AS amount,
        g.parsed_currency AS currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) AS id,
            f.display_name,
            f.ror_id,
            f.doi
        ) AS funder,
        COALESCE(NULLIF(TRIM(g.funding_type), ''), 'grant') AS funding_type,
        COALESCE(NULLIF(TRIM(g.funder_scheme), ''), NULLIF(TRIM(g.modalidade), ''), 'Projetos contratados') AS funder_scheme,
        'fapemig_projetos_contratados' AS provenance,
        g.parsed_start_date AS start_date,
        g.parsed_end_date AS end_date,
        COALESCE(YEAR(g.parsed_start_date), g.parsed_year) AS start_year,
        COALESCE(YEAR(g.parsed_end_date), g.parsed_year) AS end_year,
        CASE
            WHEN g.coordinator IS NULL OR TRIM(g.coordinator) = '' THEN CAST(NULL AS STRUCT<
                given_name:STRING,
                family_name:STRING,
                orcid:STRING,
                role_start:DATE,
                affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
            >)
            ELSE struct(
                NULLIF(TRIM(g.coordinator_given_name), '') AS given_name,
                NULLIF(TRIM(g.coordinator_family_name), '') AS family_name,
                CAST(NULL AS STRING) AS orcid,
                g.parsed_start_date AS role_start,
                struct(
                    NULLIF(TRIM(g.instituicaoexecutora_nome), '') AS name,
                    'BR' AS country,
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
        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) AS investigators,
        NULLIF(TRIM(g.landing_page_url), '') AS landing_page_url,
        CAST(NULL AS STRING) AS doi,
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.native_award_id)))) % 9000000000) AS works_api_url,
        session.awards_timestamp AS created_date,
        session.awards_timestamp AS updated_date
    FROM raw_prepared g
    CROSS JOIN fapemig_funder f
)
