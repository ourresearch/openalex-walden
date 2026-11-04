-- Mapping from CreateMVCRAwards.ipynb (origin e22fdf7f): its awards-table SELECT over isvavai_cep_mvcr_input (was the
-- staging table read from s3://openalex-ingest/awards/mvcr/mvcr_projects.parquet); created_date and updated_date are the run timestamp.
WITH
mvcr_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320311333  -- Ministerstvo Vnitra České Republiky
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_code)))) % 9000000000 as id,

        COALESCE(NULLIF(TRIM(g.title_en), ''), g.title_cs, g.title) as display_name,

        COALESCE(NULLIF(TRIM(g.description_en), ''), g.description_cs, g.description) as description,

        f.funder_id,
        g.project_code as funder_award_id,

        COALESCE(g.funding_amount, g.total_cost) as amount,
        'CZK' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        'grant' as funding_type,

        g.program_code as funder_scheme,

        'isvavai_cep_mvcr' as provenance,

        CASE
            WHEN g.start_date IS NOT NULL THEN TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')
            WHEN TRY_CAST(g.start_year AS INT) IS NOT NULL THEN TRY_TO_DATE(CONCAT(g.start_year, '-01-01'), 'yyyy-MM-dd')
            ELSE NULL
        END as start_date,
        CASE
            WHEN g.end_date IS NOT NULL THEN TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')
            WHEN TRY_CAST(g.end_year AS INT) IS NOT NULL THEN TRY_TO_DATE(CONCAT(g.end_year, '-12-31'), 'yyyy-MM-dd')
            ELSE NULL
        END as end_date,
        TRY_CAST(g.start_year AS INT) as start_year,
        TRY_CAST(g.end_year AS INT) as end_year,

        CASE
            WHEN g.institution_name IS NOT NULL THEN
                struct(
                    CAST(NULL AS STRING) as given_name,
                    CAST(NULL AS STRING) as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.institution_name as name,
                        CASE g.institution_country
                            WHEN 'CZ' THEN 'Czech Republic'
                            ELSE g.institution_country
                        END as country,
                        CASE
                            WHEN g.ror_id IS NOT NULL THEN
                                array(struct(g.ror_id as id, 'ror' as type, 'isvavai' as asserted_by))
                            ELSE CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
                        END as ids
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

        g.landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_code)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM isvavai_cep_mvcr_input g
    CROSS JOIN mvcr_funder f
    WHERE g.project_code IS NOT NULL
)
