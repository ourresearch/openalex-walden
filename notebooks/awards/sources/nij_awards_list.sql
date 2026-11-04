-- Mapping from CreateNIJAwards.ipynb (origin e22fdf7f): its awards-table SELECT over nij_awards_list_input (was the
-- staging table read from s3://openalex-ingest/awards/nij/nij_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320337430  -- National Institute of Justice
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.award_number))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    CAST(NULL AS STRING) as description,
    f.funder_id,
    TRIM(g.award_number) as funder_award_id,
    CASE WHEN TRY_CAST(g.amount AS DOUBLE) > 0 THEN TRY_CAST(g.amount AS DOUBLE) END as amount,
    CASE WHEN TRY_CAST(g.amount AS DOUBLE) > 0 THEN 'USD' END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    CASE WHEN g.solicitation RLIKE '(?i)fellowship' THEN 'fellowship' ELSE 'research' END as funding_type,
    NULLIF(TRIM(g.solicitation), '') as funder_scheme,
    'nij_awards_list' as provenance,
    TRY_TO_DATE(CONCAT(g.fiscal_year, '-01-01'), 'yyyy-MM-dd') as start_date,
    CAST(NULL AS DATE) as end_date,
    TRY_CAST(g.fiscal_year AS INT) as start_year,
    CAST(NULL AS INT) as end_year,
    CASE WHEN NULLIF(TRIM(g.recipient), '') IS NOT NULL THEN named_struct(
        'given_name', CAST(NULL AS STRING),
        'family_name', CAST(NULL AS STRING),
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', TRIM(g.recipient),
            'country', CASE WHEN g.state IN ('FM', 'MH', 'PW') THEN g.state
                            WHEN g.state IS NOT NULL THEN 'US' END,
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) END as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.award_number))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM nij_awards_list_input g
CROSS JOIN src_funder f
WHERE g.excluded = '0'
  AND g.award_number IS NOT NULL AND TRIM(g.award_number) != ''
)
