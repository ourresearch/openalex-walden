-- Mapping from CreateBD2Awards.ipynb (origin e22fdf7f): its awards-table SELECT over bd2_input (was the
-- staging table read from s3://openalex-ingest/awards/bd2/bd2_grants.parquet); created_date and updated_date are the run timestamp.
WITH
funder_row AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 8901078393
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
        'grant' as funding_type,
        g.scheme as funder_scheme,
        'bd2' as provenance,
        CAST(NULL AS DATE) as start_date,
        CAST(NULL AS DATE) as end_date,
        TRY_CAST(g.year_awarded AS INT) as start_year,
        CAST(NULL AS INT) as end_year,
        CASE
            WHEN g.pi_family IS NOT NULL THEN
                struct(
                    g.pi_given as given_name,
                    g.pi_family as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.institution as name,
                        'United States' as country,
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
    FROM bd2_input g
    CROSS JOIN funder_row f
)
