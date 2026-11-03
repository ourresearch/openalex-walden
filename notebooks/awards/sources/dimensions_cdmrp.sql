-- Mapping from CreateCDMRPAwards.ipynb (origin e22fdf7f): its awards-table SELECT over dimensions_cdmrp_input (was the
-- staging table read from s3://openalex-ingest/awards/cdmrp/cdmrp_grants.parquet); created_date and updated_date are the run timestamp.
WITH
cdmrp_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320338273  -- Congressionally Directed Medical Research Programs
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,
        g.title as display_name,
        g.short_abstract as description,
        f.funder_id,
        g.funder_award_id,
        TRY_CAST(g.amount AS DECIMAL(18,2)) as amount,
        COALESCE(NULLIF(TRIM(g.currency), ''), 'USD') as currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,
        'grant' as funding_type,
        g.program as funder_scheme,
        'dimensions_cdmrp' as provenance,
        TRY_TO_DATE(SUBSTRING(g.start_date, 1, 10), 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(SUBSTRING(g.end_date, 1, 10), 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(SUBSTRING(g.start_date, 1, 10), 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(SUBSTRING(g.end_date, 1, 10), 'yyyy-MM-dd')) as end_year,
        CASE
            WHEN g.pi_last_name IS NOT NULL THEN
                struct(
                    INITCAP(g.pi_first_name) as given_name,
                    INITCAP(g.pi_last_name) as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        COALESCE(g.institution, g.pi_affiliation) as name,
                        g.institution_country as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            ELSE NULL
        END as lead_investigator,
        CAST(NULL AS STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as co_lead_investigator,
        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) as investigators,
        g.landing_page_url,
        CAST(NULL AS STRING) as doi,
        CAST(NULL AS STRING) as works_api_url,
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date
    FROM dimensions_cdmrp_input g
    CROSS JOIN cdmrp_funder f
)
