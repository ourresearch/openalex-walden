-- Mapping from CreatePCFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over pcf_work_we_fund_input (was the
-- staging table read from s3://openalex-ingest/awards/pcf/pcf_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306163  -- Prostate Cancer Foundation
),

g AS (
    SELECT
        r.*,
        CASE WHEN NULLIF(TRIM(r.pi_family_name), '') IS NOT NULL THEN named_struct(
            'given_name', NULLIF(TRIM(r.pi_given_name), ''),
            'family_name', NULLIF(TRIM(r.pi_family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', CASE WHEN NULLIF(TRIM(r.pi_institution), '') IS NOT NULL THEN named_struct(
                'name', NULLIF(TRIM(r.pi_institution), ''),
                'country', CAST(NULL AS STRING),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            ) END
        ) END AS lead,
        filter(
            transform(
                COALESCE(from_json(r.co_investigators,
                    'ARRAY<STRUCT<name:STRING, given_name:STRING, family_name:STRING, institution:STRING>>'),
                    array()),
                p -> CASE WHEN NULLIF(TRIM(p.family_name), '') IS NOT NULL THEN named_struct(
                    'given_name', NULLIF(TRIM(p.given_name), ''),
                    'family_name', NULLIF(TRIM(p.family_name), ''),
                    'orcid', CAST(NULL AS STRING),
                    'role_start', CAST(NULL AS DATE),
                    'affiliation', CASE WHEN NULLIF(TRIM(p.institution), '') IS NOT NULL THEN named_struct(
                        'name', NULLIF(TRIM(p.institution), ''),
                        'country', CAST(NULL AS STRING),
                        'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
                    ) END
                ) END
            ),
            x -> x IS NOT NULL
        ) AS co
    FROM pcf_work_we_fund_input r
    WHERE r.funder_award_id IS NOT NULL AND TRIM(r.funder_award_id) != ''
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    NULLIF(TRIM(g.description), '') as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    CAST(NULL AS DOUBLE) as amount,      -- PCF publishes no per-award amounts
    CAST(NULL AS STRING) as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'research' as funding_type,
    CONCAT('PCF ', g.program) as funder_scheme,
    'pcf_work_we_fund' as provenance,
    TRY_TO_DATE(CONCAT(g.class_year, '-01-01'), 'yyyy-MM-dd') as start_date,
    CAST(NULL AS DATE) as end_date,
    TRY_CAST(g.class_year AS INT) as start_year,
    CAST(NULL AS INT) as end_year,
    g.lead as lead_investigator,
    try_element_at(g.co, 1) as co_lead_investigator,
    CASE WHEN g.lead IS NOT NULL THEN concat(array(g.lead), g.co)
         WHEN size(g.co) > 0 THEN g.co END as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM g
CROSS JOIN src_funder f
)
