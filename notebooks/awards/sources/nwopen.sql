-- Mapping from CreateNWOAwards.ipynb (origin e22fdf7f): its awards-table SELECT over nwopen_input (was the
-- staging table read from s3://openalex-ingest/awards/nwo/nwo_projects.parquet); created_date and updated_date are the run timestamp.
WITH
nwo_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321800  -- NWO (Nederlandse Organisatie voor Wetenschappelijk Onderzoek)
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000 as id,

        g.title as display_name,

        g.abstract as description,

        f.funder_id,
        g.project_id as funder_award_id,

        CAST(NULL AS DOUBLE) as amount,
        'EUR' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(g.grant_category) LIKE '%veni%' THEN 'fellowship'
            WHEN LOWER(g.grant_category) LIKE '%vidi%' THEN 'fellowship'
            WHEN LOWER(g.grant_category) LIKE '%vici%' THEN 'fellowship'
            WHEN LOWER(g.grant_category) LIKE '%fellowship%' THEN 'fellowship'
            WHEN LOWER(g.grant_category) LIKE '%promotiebeurs%' THEN 'fellowship'
            WHEN LOWER(g.grant_category) LIKE '%onderzoekstalent%' THEN 'fellowship'
            WHEN LOWER(g.grant_category) LIKE '%graduate%' THEN 'training'
            WHEN LOWER(g.grant_category) LIKE '%spinoza%' THEN 'prize'
            WHEN LOWER(g.grant_category) LIKE '%stevin%' THEN 'prize'
            WHEN LOWER(g.grant_category) LIKE '%gravitation%' THEN 'research'
            WHEN LOWER(g.grant_category) LIKE '%zwaartekracht%' THEN 'research'
            WHEN LOWER(g.grant_category) LIKE '%infrastructuur%' THEN 'infrastructure'
            WHEN LOWER(g.grant_category) LIKE '%infrastructure%' THEN 'infrastructure'
            ELSE 'grant'
        END as funding_type,

        g.grant_category as funder_scheme,

        'nwopen' as provenance,

        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        CASE
            WHEN g.pi_family_name IS NOT NULL THEN
                struct(
                    g.pi_given_name as given_name,
                    g.pi_family_name as family_name,
                    g.pi_id as orcid,  -- Already contains ORCID URL or NULL
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.lead_org_name as name,
                        'Netherlands' as country,
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

        CONCAT('https://www.nwo.nl/projecten/', REPLACE(g.project_id, '.', '-')) as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM nwopen_input g
    CROSS JOIN nwo_funder f
    WHERE g.project_id IS NOT NULL
      AND TRIM(g.project_id) != ''
)
