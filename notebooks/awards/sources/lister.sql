-- Mapping from CreateListerAwards.ipynb (origin e22fdf7f): its awards-table SELECT over lister_input (was the
-- staging table read from s3://openalex-ingest/awards/lister/lister_fellowships.parquet); created_date and updated_date are the run timestamp.
WITH
the_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320615  -- Lister Institute
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,
        g.title as display_name,
        CAST(NULL AS STRING) as description,
        f.funder_id,
        g.funder_award_id,
        CAST(NULL AS DECIMAL(18,2)) as amount,
        CAST(NULL AS STRING) as currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name, f.ror_id, f.doi
        ) as funder,
        'fellowship' as funding_type,
        'Lister Research Prize Fellowship' as funder_scheme,
        'lister' as provenance,
        CASE WHEN TRY_CAST(g.start_year AS INT) BETWEEN 1980 AND 2027 THEN TRY_TO_DATE(CONCAT(g.start_year,'-01-01'),'yyyy-MM-dd') ELSE NULL END as start_date,
        CASE WHEN TRY_CAST(g.end_year AS INT) BETWEEN 1980 AND 2035 THEN TRY_TO_DATE(CONCAT(g.end_year,'-01-01'),'yyyy-MM-dd') ELSE NULL END as end_date,
        CASE WHEN TRY_CAST(g.start_year AS INT) BETWEEN 1980 AND 2027 THEN TRY_CAST(g.start_year AS INT) ELSE NULL END as start_year,
        CASE WHEN TRY_CAST(g.end_year AS INT) BETWEEN 1980 AND 2035 THEN TRY_CAST(g.end_year AS INT) ELSE NULL END as end_year,
        CASE
            WHEN g.pi_family IS NOT NULL THEN
                struct(
                    g.pi_given as given_name,
                    g.pi_family as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.institution as name,
                        'United Kingdom' as country,
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
    FROM lister_input g
    CROSS JOIN the_funder f
)
