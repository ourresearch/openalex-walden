-- Mapping from CreateFNPAwards.ipynb (origin e22fdf7f): its awards-table SELECT over fnp_laureates_input (was the
-- staging table read from s3://openalex-ingest/awards/fnp/fnp_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321042  -- Fundacja na rzecz Nauki Polskiej (FNP)
),

people AS (
    SELECT
        funder_award_id,
        TRANSFORM(
            COALESCE(from_json(people_json, 'ARRAY<STRUCT<name:STRING, given_name:STRING, family_name:STRING, institution:STRING>>'), ARRAY()),
            p -> named_struct(
                'given_name', NULLIF(TRIM(p.given_name), ''),
                'family_name', NULLIF(TRIM(p.family_name), ''),
                'orcid', CAST(NULL AS STRING),
                'role_start', CAST(NULL AS DATE),
                'affiliation', named_struct(
                    'name', NULLIF(TRIM(p.institution), ''),
                    'country', CAST(NULL AS STRING),
                    'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
                )
            )
        ) AS investigators
    FROM fnp_laureates_input
),

named AS (
    SELECT funder_award_id,
           array_join(TRANSFORM(investigators, x -> CONCAT_WS(' ', x.given_name, x.family_name)), ' and ') AS laureates
    FROM people
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    COALESCE(
        NULLIF(TRIM(g.title), ''),
        CONCAT(g.programme, ' ', g.year, ': ', NULLIF(n.laureates, ''))
    ) as display_name,
    CAST(NULL AS STRING) as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    TRY_CAST(g.amount AS DOUBLE) as amount,
    CASE WHEN TRY_CAST(g.amount AS DOUBLE) IS NOT NULL THEN 'PLN' END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    g.funding_type as funding_type,
    CASE WHEN g.call IS NOT NULL THEN CONCAT(g.programme, ' (call ', g.call, ')') ELSE g.programme END as funder_scheme,
    'fnp_laureates' as provenance,
    TRY_TO_DATE(CONCAT(TRY_CAST(g.year AS INT), '-01-01'), 'yyyy-MM-dd') as start_date,
    CAST(NULL AS DATE) as end_date,
    TRY_CAST(g.year AS INT) as start_year,
    CAST(NULL AS INT) as end_year,
    try_element_at(p.investigators, 1) as lead_investigator,
    try_element_at(p.investigators, 2) as co_lead_investigator,
    CASE WHEN size(p.investigators) > 0 THEN p.investigators END as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM fnp_laureates_input g
JOIN people p ON p.funder_award_id = g.funder_award_id
JOIN named n ON n.funder_award_id = g.funder_award_id
CROSS JOIN src_funder f
WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
)
