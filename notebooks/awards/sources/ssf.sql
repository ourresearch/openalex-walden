-- Mapping from CreateSSFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over ssf_input (was the
-- staging table read from s3://openalex-ingest/awards/ssf/ssf_grants.parquet); created_date and updated_date are the run timestamp.
WITH
the_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320940  -- Swedish Foundation for Strategic Research
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,
        g.title as display_name,
        g.research_area as description,
        f.funder_id,
        g.funder_award_id,
        TRY_CAST(g.amount AS DECIMAL(18,2)) as amount,
        'SEK' as currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name, f.ror_id, f.doi
        ) as funder,
        'research' as funding_type,
        g.programme as funder_scheme,
        'ssf' as provenance,
        TRY_TO_DATE(g.start_date,'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date,'yyyy-MM-dd') as end_date,
        CASE WHEN TRY_CAST(g.start_year AS INT) BETWEEN 1980 AND 2030 THEN TRY_CAST(g.start_year AS INT) ELSE NULL END as start_year,
        YEAR(TRY_TO_DATE(g.end_date,'yyyy-MM-dd')) as end_year,
        CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as lead_investigator,
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
    FROM ssf_input g
    CROSS JOIN the_funder f
)
