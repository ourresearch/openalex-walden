-- Mapping from CreateNIHRAwards.ipynb (origin e22fdf7f): its awards-table SELECT over nihr_input (was the
-- staging table read from s3://openalex-ingest/awards/nihr/nihr_projects.parquet); created_date and updated_date are the run timestamp.
WITH
nihr_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320319990  -- National Institute for Health and Care Research
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000 as id,

        g.title as display_name,

        COALESCE(g.plain_english_abstract, g.scientific_abstract) as description,

        f.funder_id,
        g.project_id as funder_award_id,

        TRY_CAST(g.award_amount AS DOUBLE) as amount,
        'GBP' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(g.programme) LIKE '%fellowship%' THEN 'fellowship'
            WHEN LOWER(g.programme) LIKE '%senior investigator%' THEN 'fellowship'
            WHEN LOWER(g.programme) LIKE '%academic%' THEN 'fellowship'
            WHEN LOWER(g.programme) LIKE '%bridging%' THEN 'fellowship'
            WHEN LOWER(g.programme) LIKE '%health technology%' THEN 'research'
            WHEN LOWER(g.programme) LIKE '%research for patient%' THEN 'research'
            WHEN LOWER(g.programme) LIKE '%programme grant%' THEN 'research'
            WHEN LOWER(g.programme) LIKE '%efficacy%' THEN 'research'
            WHEN LOWER(g.programme) LIKE '%public health%' THEN 'research'
            WHEN LOWER(g.programme) LIKE '%policy%' THEN 'research'
            WHEN LOWER(g.programme) LIKE '%global health%' THEN 'research'
            WHEN LOWER(g.programme) LIKE '%delivery%' THEN 'research'
            WHEN LOWER(g.programme) LIKE '%invention%' THEN 'research'
            WHEN LOWER(g.programme) LIKE '%infrastructure%' THEN 'infrastructure'
            ELSE 'grant'
        END as funding_type,

        g.programme as funder_scheme,

        'nihr' as provenance,

        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        CASE
            WHEN g.pi_family_name IS NOT NULL THEN
                struct(
                    g.pi_given_name as given_name,
                    g.pi_family_name as family_name,
                    g.orcid as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.contracted_organisation as name,
                        COALESCE(IF(size(g.institution_country) > 0, g.institution_country[0], NULL), 'United Kingdom') as country,
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

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM nihr_input g
    CROSS JOIN nihr_funder f
    WHERE g.project_id IS NOT NULL
      AND TRIM(g.project_id) != ''
)
