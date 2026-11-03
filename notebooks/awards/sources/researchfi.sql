-- Mapping from CreateAcademyOfFinlandAwards.ipynb (origin e22fdf7f): its awards-table SELECT over researchfi_input (was the
-- staging table read from s3://openalex-ingest/awards/academy_of_finland/academy_of_finland_projects.parquet); created_date and updated_date are the run timestamp.
WITH
aka_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321108  -- Academy of Finland
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(CAST(g.project_id AS STRING))))) % 9000000000 as id,

        COALESCE(NULLIF(TRIM(g.title_en), ''), g.title_fi) as display_name,

        COALESCE(NULLIF(TRIM(g.description_en), ''), g.description_fi) as description,

        f.funder_id,
        g.funder_project_number as funder_award_id,

        TRY_CAST(g.amount_eur AS DOUBLE) as amount,
        'EUR' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(g.funding_type) LIKE '%fellowship%' THEN 'fellowship'
            WHEN LOWER(g.funding_type) LIKE '%postdoctoral%' THEN 'fellowship'
            WHEN LOWER(g.funding_type) LIKE '%research fellow%' THEN 'fellowship'
            WHEN LOWER(g.funding_type) LIKE '%academy professor%' THEN 'fellowship'
            WHEN LOWER(g.funding_type) LIKE '%centre of excellence%' THEN 'research'
            WHEN LOWER(g.funding_type) LIKE '%flagship%' THEN 'research'
            WHEN LOWER(g.funding_type) LIKE '%infrastructure%' THEN 'infrastructure'
            WHEN LOWER(g.funding_type) LIKE '%strategic%' THEN 'research'
            WHEN LOWER(g.funding_type) LIKE '%academy project%' THEN 'grant'
            ELSE 'grant'
        END as funding_type,

        g.call_programme as funder_scheme,

        'researchfi' as provenance,

        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        TRY_CAST(g.start_year AS INT) as start_year,
        TRY_CAST(g.end_year AS INT) as end_year,

        CASE
            WHEN g.pi_family_name IS NOT NULL THEN
                struct(
                    g.pi_given_name as given_name,
                    g.pi_family_name as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        CAST(NULL AS STRING) as name,
                        'Finland' as country,
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

        CONCAT('https://research.fi/en/results/funding/', CAST(g.project_id AS STRING)) as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(CAST(g.project_id AS STRING))))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM researchfi_input g
    CROSS JOIN aka_funder f
    WHERE g.project_id IS NOT NULL
)
