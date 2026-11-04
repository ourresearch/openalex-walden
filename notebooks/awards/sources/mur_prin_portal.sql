-- Mapping from CreatePRINAwards.ipynb (origin e22fdf7f): its awards-table SELECT over mur_prin_portal_input (was the
-- staging table read from s3://openalex-ingest/awards/prin/prin_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id IN (4320331528, 4320321873)
),

parsed AS (
    SELECT
        g.*,
        TRY_CAST(LEFT(g.call_year, 4) AS INT) AS call_year_int,
        CASE WHEN TRY_CAST(LEFT(g.call_year, 4) AS INT) >= 2020 THEN 4320331528
             ELSE 4320321873 END AS resolved_funder_id,
        from_json(g.units, 'ARRAY<STRUCT<unit:INT, institution:STRING, name:STRING, given_name:STRING, family_name:STRING, amount:DOUBLE>>') AS unit_arr
    FROM mur_prin_portal_input g
    WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
),

people AS (
    SELECT
        funder_award_id,
        TRANSFORM(COALESCE(unit_arr, ARRAY()), u -> named_struct(
            'given_name', NULLIF(TRIM(u.given_name), ''),
            'family_name', NULLIF(TRIM(u.family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(u.institution), ''),
                'country', 'IT',
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        )) AS investigators
    FROM parsed
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    NULLIF(TRIM(g.description), '') as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    TRY_CAST(g.amount AS DOUBLE) as amount,
    CASE WHEN TRY_CAST(g.amount AS DOUBLE) IS NOT NULL THEN 'EUR' END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'research' as funding_type,
    CONCAT('PRIN ', g.call_year) as funder_scheme,
    'mur_prin_portal' as provenance,
    CAST(NULL AS DATE) as start_date,
    CAST(NULL AS DATE) as end_date,
    g.call_year_int as start_year,
    CAST(NULL AS INT) as end_year,
    named_struct(
        'given_name', NULLIF(TRIM(g.lead_given_name), ''),
        'family_name', NULLIF(TRIM(g.lead_family_name), ''),
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', NULLIF(TRIM(g.institution), ''),
            'country', 'IT',
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) as lead_investigator,
    try_element_at(p.investigators, 2) as co_lead_investigator,
    CASE WHEN size(p.investigators) > 0 THEN p.investigators END as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM parsed g
JOIN people p ON p.funder_award_id = g.funder_award_id
JOIN src_funder f ON f.funder_id = g.resolved_funder_id
)
