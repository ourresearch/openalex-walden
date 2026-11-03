-- Mapping from CreateBusinessFinlandAwards.ipynb (origin e22fdf7f): its awards-table SELECT over business_finland_input (was the
-- staging table read from s3://openalex-ingest/awards/business_finland/business_finland_projects.parquet); created_date and updated_date are the run timestamp.
WITH
bf_funders AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id IN (4320321855, 4320328501)
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.diary_number))))) % 9000000000 as id,
        CONCAT(
            COALESCE(NULLIF(TRIM(g.program), ''), NULLIF(TRIM(g.funding_service), ''), 'Public research project'),
            ' — ',
            COALESCE(NULLIF(TRIM(g.organisation), ''), 'unknown organisation'),
            ' (', TRIM(g.diary_number), ')'
        ) as display_name,
        NULLIF(TRIM(g.abstract_fi), '') as description,
        f.funder_id,
        TRIM(g.diary_number) as funder_award_id,
        CASE
            WHEN g.amount_eur = 'Luottamuksellinen' THEN NULL   -- confidential in source
            ELSE NULLIF(TRY_CAST(g.amount_eur AS DOUBLE), 0)
        END as amount,
        'EUR' as currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,
        'research' as funding_type,
        COALESCE(NULLIF(TRIM(g.funding_service), ''), NULLIF(TRIM(g.program), '')) as funder_scheme,
        'business_finland' as provenance,
        TRY_TO_DATE(CONCAT(g.decision_year, '-01-01'), 'yyyy-MM-dd') as start_date,
        CAST(NULL AS DATE) as end_date,
        TRY_CAST(g.decision_year AS INT) as start_year,
        CAST(NULL AS INT) as end_year,
        CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as lead_investigator,
        CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
        CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) as investigators,
        CAST(NULL AS STRING) as landing_page_url,
        CAST(NULL AS STRING) as doi,
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.diary_number))))) % 9000000000) as works_api_url,
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date
    FROM business_finland_input g
    JOIN bf_funders f
      ON f.funder_id = TRY_CAST(g.openalex_funder_id AS BIGINT)
    WHERE g.diary_number IS NOT NULL AND TRIM(g.diary_number) != ''
)
