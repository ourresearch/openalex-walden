-- Mapping from CreateSSHRCAwards.ipynb (origin e22fdf7f): its awards-table SELECT over sshrc_opendata_input (was the
-- staging table read from s3://openalex-ingest/awards/sshrc/sshrc_projects.parquet); created_date and updated_date are the run timestamp.
WITH
sshrc_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320334617  -- Social Sciences and Humanities Research Council of Canada
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.file_number))))) % 9000000000 as id,

        g.title as display_name,

        CAST(NULL AS STRING) as description,

        f.funder_id,
        TRIM(g.file_number) as funder_award_id,

        CAST(g.amount AS DOUBLE) as amount,
        'CAD' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(g.program) LIKE '%doctoral%' THEN 'fellowship'
            WHEN LOWER(g.program) LIKE '%postdoctoral%' THEN 'fellowship'
            WHEN LOWER(g.program) LIKE '%fellowship%' THEN 'fellowship'
            WHEN LOWER(g.program) LIKE '%scholarship%' THEN 'fellowship'
            WHEN LOWER(g.program) LIKE '%vanier%' THEN 'fellowship'
            WHEN LOWER(g.program) LIKE '%banting%' THEN 'fellowship'
            WHEN LOWER(g.program) LIKE '%talent%' THEN 'fellowship'
            WHEN LOWER(g.program) LIKE '%canada research chair%' THEN 'fellowship'
            WHEN LOWER(g.program) LIKE '%partnership%' THEN 'research'
            WHEN LOWER(g.program) LIKE '%insight%' THEN 'research'
            WHEN LOWER(g.program) LIKE '%connection%' THEN 'research'
            WHEN LOWER(g.program) LIKE '%aid%' THEN 'research'
            WHEN LOWER(g.program) LIKE '%savoir%' THEN 'research'
            WHEN LOWER(g.program) LIKE '%sshrc %grant%' THEN 'grant'
            WHEN LOWER(g.program) LIKE '%general research%' THEN 'research'
            WHEN LOWER(g.program) LIKE '%major collaborative%' THEN 'research'
            WHEN LOWER(g.program) LIKE '%standard research%' THEN 'research'
            WHEN LOWER(g.program) LIKE '%strategic%' THEN 'research'
            ELSE 'grant'
        END as funding_type,

        g.program as funder_scheme,

        'sshrc_opendata' as provenance,

        CASE 
            WHEN g.start_year IS NOT NULL THEN TRY_TO_DATE(CONCAT(CAST(CAST(g.start_year AS INT) AS STRING), '-04-01'))
            ELSE NULL
        END as start_date,
        CAST(NULL AS DATE) as end_date,  -- Not available in SSHRC open data
        CAST(g.start_year AS INT) as start_year,
        CAST(NULL AS INT) as end_year,

        CASE
            WHEN g.applicant_name IS NOT NULL AND TRIM(g.applicant_name) != '' THEN
                struct(
                    CAST(NULL AS STRING) as given_name,  -- Not available separately
                    TRIM(g.applicant_name) as family_name,  -- Store full name in family_name
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        TRIM(g.institution) as name,
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

        'https://open.canada.ca/data/en/dataset/b4e2b302-9bc6-4b33-b880-6496f8cef0f1' as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.file_number))))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM sshrc_opendata_input g
    CROSS JOIN sshrc_funder f
    WHERE g.file_number IS NOT NULL
      AND TRIM(g.file_number) != ''
)
