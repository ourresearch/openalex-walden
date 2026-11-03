-- Mapping from CreateERUKAwards.ipynb (origin e22fdf7f): its awards-table SELECT over eruk_input (was the
-- staging table read from s3://openalex-ingest/awards/eruk/eruk_projects.parquet); created_date and updated_date are the run timestamp.
WITH
the_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320012  -- Epilepsy Research UK
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,
        g.title as display_name,
        CAST(NULL AS STRING) as description,
        f.funder_id,
        g.funder_award_id as funder_award_id,
        CAST(NULL AS DECIMAL(18,2)) as amount,
        CAST(NULL AS STRING) as currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name, f.ror_id, f.doi
        ) as funder,
        'research' as funding_type,
        CAST(NULL AS STRING) as funder_scheme,
        'eruk' as provenance,
        CAST(NULL AS DATE) as start_date,
        CAST(NULL AS DATE) as end_date,
        CAST(NULL AS INT) as start_year,
        CAST(NULL AS INT) as end_year,
        CASE
            WHEN CAST(NULL AS STRING) IS NOT NULL THEN
                struct(
                    CAST(NULL AS STRING) as given_name,
                    CAST(NULL AS STRING) as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        CAST(NULL AS STRING) as name,
                        CAST(NULL AS STRING) as country,
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
        g.landing_page_url as landing_page_url,
        CAST(NULL AS STRING) as doi,
        CAST(NULL AS STRING) as works_api_url,
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date
    FROM eruk_input g
    CROSS JOIN the_funder f
)
