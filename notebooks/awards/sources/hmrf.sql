-- Mapping from CreateHMRFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over hmrf_input (was the
-- staging table read from s3://openalex-ingest/awards/hmrf/hmrf_projects.parquet); created_date and updated_date are the run timestamp.
WITH
the_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320335055  -- HMRF (Hong Kong)
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.ref_no)))) % 9000000000 as id,
        g.project_title as display_name,
        COALESCE(g.abstract_final, g.abstract_proposal) as description,
        f.funder_id,
        g.ref_no as funder_award_id,
        TRY_CAST(g.approved_amount_hkd AS DECIMAL(18,2)) as amount,
        'HKD' as currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name, f.ror_id, f.doi
        ) as funder,
        'grant' as funding_type,
        g.fund as funder_scheme,
        'hmrf' as provenance,
        CASE WHEN TRY_CAST(g.call_year AS INT) IS NOT NULL
             THEN TRY_TO_DATE(CONCAT(g.call_year, '-01-01'), 'yyyy-MM-dd') END as start_date,
        CAST(NULL AS DATE) as end_date,
        TRY_CAST(g.call_year AS INT) as start_year,
        CAST(NULL AS INT) as end_year,
        CASE
            WHEN g.lead_family_name IS NOT NULL THEN
                struct(
                    g.lead_given_name as given_name,
                    g.lead_family_name as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.lead_affiliation as name,
                        'Hong Kong' as country,
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
        CAST(NULL AS STRING) as landing_page_url,
        CAST(NULL AS STRING) as doi,
        CAST(NULL AS STRING) as works_api_url,
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date
    FROM hmrf_input g
    CROSS JOIN the_funder f
)
