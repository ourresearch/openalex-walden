-- Mapping from CreateMJFFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over mjff_funded_studies_input (was the
-- staging table read from s3://openalex-ingest/awards/mjff/mjff_funded_studies.parquet); created_date and updated_date are the run timestamp.
WITH
funder_resolved AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306136
),
raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_CAST(source_year AS INT) AS parsed_year,
        TRY_TO_DATE(CONCAT(source_year, '-01-01'), 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(CONCAT(source_year, '-12-31'), 'yyyy-MM-dd') AS parsed_end_date,
        NULLIF(TRIM(lead_given_name), '') AS lead_given_name_clean,
        NULLIF(TRIM(lead_family_name), '') AS lead_family_name_clean,
        NULLIF(TRIM(
            CASE
                WHEN lead_position_affiliation RLIKE ' at ' THEN REGEXP_EXTRACT(lead_position_affiliation, ' at (.+)$', 1)
                ELSE lead_position_affiliation
            END
        ), '') AS lead_affiliation_name
    FROM mjff_funded_studies_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND display_name IS NOT NULL
      AND TRIM(display_name) != ''
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000 as id,
        TRIM(g.display_name) as display_name,
        NULLIF(TRIM(g.description), '') as description,
        f.funder_id,
        g.native_award_id as funder_award_id,
        CAST(NULL AS DOUBLE) as amount,
        CAST(NULL AS STRING) as currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,
        'research' as funding_type,
        COALESCE(NULLIF(TRIM(g.source_program), ''), 'MJFF Funded Study') as funder_scheme,
        'mjff_funded_studies' as provenance,
        g.parsed_start_date as start_date,
        g.parsed_end_date as end_date,
        g.parsed_year as start_year,
        g.parsed_year as end_year,
        CASE
            WHEN g.lead_family_name_clean IS NULL THEN NULL
            ELSE struct(
                g.lead_given_name_clean as given_name,
                g.lead_family_name_clean as family_name,
                CAST(NULL AS STRING) as orcid,
                g.parsed_start_date as role_start,
                struct(
                    g.lead_affiliation_name as name,
                    CAST(NULL AS STRING) as country,
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                ) as affiliation
            )
        END as lead_investigator,
        CAST(NULL AS STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as co_lead_investigator,
        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) as investigators,
        NULLIF(TRIM(g.landing_page_url), '') as landing_page_url,
        CAST(NULL AS STRING) as doi,
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000) as works_api_url,
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date
    FROM raw_prepared g
    CROSS JOIN funder_resolved f
)
