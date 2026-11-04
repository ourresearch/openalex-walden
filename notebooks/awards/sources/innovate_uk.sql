-- Mapping from CreateInnovateUKAwards.ipynb (origin e22fdf7f): its awards-table SELECT over innovate_uk_input (was the
-- staging table read from s3://openalex-ingest/awards/innovateuk/innovateuk_projects.parquet); created_date and updated_date are the run timestamp.
WITH
innovateuk_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320335087  -- Innovate UK
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', CAST(p.project_number AS STRING)))) % 9000000000 as id,

        p.project_title as display_name,

        p.public_description as description,

        f.funder_id,
        CAST(p.project_number AS STRING) as funder_award_id,

        CAST(p.award_amount AS DOUBLE) as amount,
        'GBP' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(p.product_type) LIKE '%fellowship%' THEN 'fellowship'
            WHEN LOWER(p.product_type) LIKE '%feasibility%' THEN 'research'
            WHEN LOWER(p.product_type) LIKE '%r&d%' OR LOWER(p.product_type) LIKE '%research%' THEN 'research'
            WHEN LOWER(p.product_type) LIKE '%innovation%' THEN 'research'
            WHEN LOWER(p.product_type) LIKE '%demonstrator%' THEN 'research'
            WHEN LOWER(p.product_type) LIKE '%smart%' THEN 'research'
            WHEN LOWER(p.product_type) LIKE '%knowledge transfer%' THEN 'training'
            WHEN LOWER(p.product_type) LIKE '%loan%' THEN 'loan'
            ELSE 'grant'
        END as funding_type,

        p.product_type as funder_scheme,

        'innovate_uk' as provenance,

        TRY_TO_DATE(p.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(p.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(p.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(p.end_date, 'yyyy-MM-dd')) as end_year,

        CASE
            WHEN p.participant_name IS NOT NULL THEN
                struct(
                    CAST(NULL AS STRING) as given_name,
                    CAST(NULL AS STRING) as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        p.participant_name as name,
                        'United Kingdom' as country,  -- All Innovate UK grants are UK-based
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

        CONCAT('https://gtr.ukri.org/projects?ref=', CAST(p.project_number AS STRING)) as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', CAST(p.project_number AS STRING)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date,

        p.sector,
        p.enterprise_size,
        p.region,
        p.competition_year,
        p.project_status,
        p.is_iscf

    FROM innovate_uk_input p
    CROSS JOIN innovateuk_funder f
    WHERE p.project_number IS NOT NULL
)
