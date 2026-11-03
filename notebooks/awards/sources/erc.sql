-- Mapping from CreateERCAwards.ipynb (origin e22fdf7f): its awards-table SELECT over erc_input (was the
-- staging table read from s3://openalex-ingest/awards/erc/erc_projects.parquet); created_date and updated_date are the run timestamp.
WITH
erc_funders AS (
    SELECT * FROM (
        VALUES
        ('H2020', 4320338335, 'H2020 European Research Council', '10.13039/100010663'),
        ('FP7', 4320338352, 'FP7 Ideas: European Research Council', '10.13039/100011199'),
        ('HORIZON', 4320338453, 'HORIZON EUROPE European Research Council', '10.13039/100019180')
    ) AS t(framework_programme, funder_id, display_name, doi)
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(COALESCE(f.funder_id, 4320334678), ':', LOWER(g.project_id)))) % 9000000000 as id,

        g.title as display_name,

        g.abstract as description,

        COALESCE(f.funder_id, 4320334678) as funder_id,
        g.project_id as funder_award_id,

        TRY_CAST(REPLACE(g.ec_contribution, ',', '.') AS DOUBLE) as amount,
        'EUR' as currency,

        struct(
            CONCAT('https://openalex.org/F', COALESCE(f.funder_id, 4320334678)) as id,
            COALESCE(f.display_name, 'European Research Council') as display_name,
            'https://ror.org/0472cxd90' as ror_id,
            COALESCE(f.doi, '10.13039/501100000781') as doi
        ) as funder,

        CASE
            WHEN UPPER(g.funding_scheme) LIKE '%STG%' THEN 'fellowship'
            WHEN UPPER(g.funding_scheme) LIKE '%COG%' THEN 'fellowship'
            WHEN UPPER(g.funding_scheme) LIKE '%ADG%' THEN 'fellowship'
            WHEN UPPER(g.funding_scheme) LIKE '%SYG%' THEN 'research'
            WHEN UPPER(g.funding_scheme) LIKE '%POC%' THEN 'commercialization'
            WHEN UPPER(g.funding_scheme) LIKE '%LVG%' THEN 'travel'
            ELSE 'research'
        END as funding_type,

        g.funding_scheme as funder_scheme,

        'erc' as provenance,

        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        CASE
            WHEN g.host_institution IS NOT NULL THEN
                struct(
                    CAST(NULL AS STRING) as given_name,
                    CAST(NULL AS STRING) as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.host_institution as name,
                        g.host_country as country,
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

        CONCAT('https://cordis.europa.eu/project/id/', g.project_id) as landing_page_url,

        CASE
            WHEN g.grant_doi IS NOT NULL AND TRIM(g.grant_doi) != '' 
            THEN g.grant_doi
            ELSE NULL
        END as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(COALESCE(f.funder_id, 4320334678), ':', LOWER(g.project_id)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM erc_input g
    LEFT JOIN erc_funders f ON g.framework_programme = f.framework_programme
    WHERE g.project_id IS NOT NULL
      AND TRIM(g.project_id) != ''
)
