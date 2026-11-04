-- Mapping from CreateMaratoTV3Awards.ipynb (origin e22fdf7f): its awards-table SELECT over marato_tv3_funded_projects_input (was the
-- staging table read from s3://openalex-ingest/awards/marato_tv3/marato_tv3_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320310701  -- Fundació la Marató de TV3
),

parsed AS (
    SELECT
        g.*,
        from_json(g.people, 'ARRAY<STRUCT<name:STRING, given_name:STRING, family_name:STRING, institution:STRING>>') AS people_arr
    FROM marato_tv3_funded_projects_input g
    WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
),

people AS (
    SELECT
        funder_award_id,
        TRANSFORM(COALESCE(people_arr, ARRAY()), p -> named_struct(
            'given_name', NULLIF(TRIM(p.given_name), ''),
            'family_name', NULLIF(TRIM(p.family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(p.institution), ''),
                'country', CAST(NULL AS STRING),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        )) AS investigators
    FROM parsed
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    CAST(NULL AS STRING) as description,
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
    CONCAT('La Marató ', g.edition_year, COALESCE(CONCAT(': ', NULLIF(TRIM(g.disease_area), '')), '')) as funder_scheme,
    'marato_tv3_funded_projects' as provenance,
    CAST(NULL AS DATE) as start_date,
    CAST(NULL AS DATE) as end_date,
    TRY_CAST(g.edition_year AS INT) as start_year,
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
FROM parsed g
JOIN people p ON p.funder_award_id = g.funder_award_id
CROSS JOIN src_funder f
)
