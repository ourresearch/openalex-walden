-- Mapping from CreateISCIIIAwards.ipynb (origin e22fdf7f): its awards-table SELECT over portal_fis_isciii_input (was the
-- staging table read from s3://openalex-ingest/awards/isciii/isciii_projects.parquet); created_date and updated_date are the run timestamp.
WITH
isciii_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320334923  -- Instituto de Salud Carlos III
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000 as id,

        g.title as display_name,

        g.abstract as description,

        f.funder_id,
        g.project_id as funder_award_id,

        g.amount as amount,
        COALESCE(g.currency, 'EUR') as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN g.project_id LIKE 'FI%' THEN 'fellowship'
            WHEN g.project_id LIKE 'CP%' THEN 'fellowship'
            WHEN g.project_id LIKE 'RD%' THEN 'center'
            WHEN g.project_id LIKE 'DTS%' THEN 'infrastructure'
            WHEN g.project_id LIKE 'PI%' THEN 'grant'
            WHEN g.project_id LIKE 'AC%' THEN 'grant'
            WHEN g.project_id LIKE 'COV%' THEN 'grant'
            ELSE 'grant'
        END as funding_type,

        g.funder_scheme,

        'portal_fis_isciii' as provenance,

        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        CASE WHEN g.pi_family_name IS NOT NULL THEN
            struct(
                g.pi_given_name as given_name,
                g.pi_family_name as family_name,
                CAST(NULL AS STRING) as orcid,
                CAST(NULL AS DATE) as role_start,
                struct(
                    g.institution as name,
                    'ES' as country,
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                ) as affiliation
            )
        ELSE NULL END as lead_investigator,

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

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM portal_fis_isciii_input g
    CROSS JOIN isciii_funder f
    WHERE g.project_id IS NOT NULL
      AND TRIM(g.project_id) != ''
)
