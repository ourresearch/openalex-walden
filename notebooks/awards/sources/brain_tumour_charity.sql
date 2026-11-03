-- Mapping from CreateBrainTumourCharityAwards.ipynb (origin e22fdf7f): its awards-table SELECT over brain_tumour_charity_input (was the
-- staging table read from s3://openalex-ingest/awards/brain_tumour_charity/brain_tumour_charity_grants.parquet); created_date and updated_date are the run timestamp.
WITH
the_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321000  -- The Brain Tumour Charity
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,
        g.title as display_name,
        CAST(NULL AS STRING) as description,
        f.funder_id,
        g.funder_award_id,
        TRY_CAST(g.amount AS DECIMAL(18,2)) as amount,
        'GBP' as currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name, f.ror_id, f.doi
        ) as funder,
        'grant' as funding_type,
        g.programme as funder_scheme,
        'brain_tumour_charity' as provenance,
        TRY_TO_DATE(g.start_date,'yyyy-MM-dd') as start_date,
        CAST(NULL AS DATE) as end_date,
        CASE WHEN TRY_CAST(g.start_year AS INT) BETWEEN 1990 AND 2030 THEN TRY_CAST(g.start_year AS INT) ELSE NULL END as start_year,
        CASE WHEN TRY_CAST(g.end_year AS INT) BETWEEN 1990 AND 2035 THEN TRY_CAST(g.end_year AS INT) ELSE NULL END as end_year,
        CASE
            WHEN g.pi_family IS NOT NULL THEN
                struct(
                    g.pi_given as given_name, g.pi_family as family_name,
                    CAST(NULL AS STRING) as orcid, CAST(NULL AS DATE) as role_start,
                    struct(g.institution as name, 'United Kingdom' as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids) as affiliation
                )
            ELSE NULL END as lead_investigator,
        CAST(NULL AS STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as co_lead_investigator,
        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) as investigators,
        g.landing_page_url as landing_page_url,
        CAST(NULL AS STRING) as doi,
        CAST(NULL AS STRING) as works_api_url,
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date
    FROM brain_tumour_charity_input g
    CROSS JOIN the_funder f
)
