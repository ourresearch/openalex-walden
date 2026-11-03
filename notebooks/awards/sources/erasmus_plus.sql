-- Mapping from CreateErasmusPlusAwards.ipynb (origin e22fdf7f): its awards-table SELECT over erasmus_plus_input (was the
-- staging table read from s3://openalex-ingest/awards/erasmus_plus/erasmus_plus_projects.parquet); created_date and updated_date are the run timestamp.
WITH
eplus_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320335551  -- Erasmus+
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_reference)))) % 9000000000 as id,

        g.project_title as display_name,

        COALESCE(g.project_description, g.description_objectives, g.description_background) as description,

        f.funder_id,
        g.project_reference as funder_award_id,

        TRY_CAST(g.granted_eu_amount AS DOUBLE) as amount,
        'EUR' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        'grant' as funding_type,

        COALESCE(g.level3_label, g.level2_label) as funder_scheme,

        'erasmus_plus' as provenance,

        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        CASE
            WHEN g.coordinator_name IS NOT NULL THEN
                struct(
                    CAST(NULL AS STRING) as given_name,
                    CAST(NULL AS STRING) as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.coordinator_name as name,
                        g.coordinator_country as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            ELSE NULL
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

        g.landing_page_url as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_reference)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM erasmus_plus_input g
    CROSS JOIN eplus_funder f
    WHERE g.project_reference IS NOT NULL
      AND TRIM(g.project_reference) != ''
)
