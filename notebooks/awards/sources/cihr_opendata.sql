-- Mapping from CreateCIHRAwards.ipynb (origin e22fdf7f): its awards-table SELECT over cihr_opendata_input (was the
-- staging table read from s3://openalex-ingest/awards/cihr/cihr_projects.parquet); created_date and updated_date are the run timestamp.
WITH
cihr_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320334506  -- Canadian Institutes of Health Research
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funding_reference_number))))) % 9000000000 as id,

        g.application_title as display_name,

        g.application_abstract as description,

        f.funder_id,
        TRIM(g.funding_reference_number) as funder_award_id,

        TRY_CAST(g.total_amount_awarded AS DOUBLE) as amount,
        'CAD' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(g.program_type) LIKE '%award%' THEN 'fellowship'
            WHEN LOWER(g.program_name) LIKE '%doctoral%' THEN 'fellowship'
            WHEN LOWER(g.program_name) LIKE '%postdoctoral%' THEN 'fellowship'
            WHEN LOWER(g.program_name) LIKE '%fellowship%' THEN 'fellowship'
            WHEN LOWER(g.program_name) LIKE '%scholarship%' THEN 'fellowship'
            WHEN LOWER(g.program_name) LIKE '%master%' THEN 'fellowship'
            WHEN LOWER(g.program_name) LIKE '%vanier%' THEN 'fellowship'
            WHEN LOWER(g.program_name) LIKE '%banting%' THEN 'fellowship'
            WHEN LOWER(g.program_name) LIKE '%canada research chair%' THEN 'fellowship'
            WHEN LOWER(g.program_name) LIKE '%salary%' THEN 'fellowship'
            WHEN LOWER(g.program_name) LIKE '%new investigator%' THEN 'fellowship'
            WHEN LOWER(g.program_name) LIKE '%career%' THEN 'fellowship'
            WHEN LOWER(g.program_name) LIKE '%travel%' THEN 'travel'
            WHEN LOWER(g.program_name) LIKE '%meeting%' THEN 'travel'
            WHEN LOWER(g.program_name) LIKE '%planning%' THEN 'planning'
            WHEN LOWER(g.program_name) LIKE '%operating grant%' THEN 'research'
            WHEN LOWER(g.program_name) LIKE '%project grant%' THEN 'research'
            WHEN LOWER(g.program_name) LIKE '%team grant%' THEN 'research'
            WHEN LOWER(g.program_name) LIKE '%foundation grant%' THEN 'research'
            WHEN LOWER(g.program_name) LIKE '%bridge%' THEN 'research'
            WHEN LOWER(g.program_name) LIKE '%catalyst%' THEN 'research'
            WHEN LOWER(g.program_name) LIKE '%randomized controlled%' THEN 'research'
            WHEN LOWER(g.program_type) LIKE '%grant%' THEN 'grant'
            ELSE 'grant'
        END as funding_type,

        g.program_name as funder_scheme,

        'cihr_opendata' as provenance,

        TRY_TO_DATE(g.funding_start_date) as start_date,
        TRY_TO_DATE(g.funding_end_date) as end_date,
        YEAR(TRY_TO_DATE(g.funding_start_date)) as start_year,
        YEAR(TRY_TO_DATE(g.funding_end_date)) as end_year,

        CASE
            WHEN g.family_name IS NOT NULL AND TRIM(g.family_name) != '' THEN
                struct(
                    TRIM(g.first_name) as given_name,
                    TRIM(g.family_name) as family_name,
                    CAST(NULL AS STRING) as orcid,
                    TRY_TO_DATE(g.funding_start_date) as role_start,
                    struct(
                        -- Else institution_paid_name, the institution CIHR paid ('Direct payment to individual ...' means it paid the person).
                        COALESCE(NULLIF(TRIM(g.research_institution_name), ''),
                                 CASE WHEN g.institution_paid_name NOT LIKE 'Direct payment%' THEN NULLIF(TRIM(g.institution_paid_name), '') END) as name,
                        'Canada' as country,
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

        'https://open.canada.ca/data/en/dataset/49edb1d7-5cb4-4fa7-897c-515d1aad5da3' as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funding_reference_number))))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM cihr_opendata_input g
    CROSS JOIN cihr_funder f
    WHERE g.funding_reference_number IS NOT NULL
      AND TRIM(g.funding_reference_number) != ''
)
