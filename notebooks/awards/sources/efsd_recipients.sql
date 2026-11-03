-- Mapping from CreateEFSDAwards.ipynb (origin e22fdf7f): its awards-table SELECT over efsd_recipients_input (was the
-- staging table read from s3://openalex-ingest/awards/efsd/efsd_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320870  -- European Foundation for the Study of Diabetes
),

parsed AS (
    SELECT
        g.*,
        CASE WHEN NULLIF(TRIM(g.family_name), '') IS NOT NULL THEN named_struct(
            'given_name', NULLIF(TRIM(g.given_name), ''),
            'family_name', NULLIF(TRIM(g.family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(g.institution), ''),
                'country', NULLIF(TRIM(g.country), ''),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        ) END AS pi
    FROM efsd_recipients_input g
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(p.funder_award_id))))) % 9000000000 as id,
    COALESCE(NULLIF(TRIM(p.title), ''),
             CASE WHEN p.mentor IS NOT NULL THEN CONCAT(p.programme, ' — mentor: ', TRIM(p.mentor)) END,
             p.programme) as display_name,
    CAST(NULL AS STRING) as description,
    f.funder_id,
    TRIM(p.funder_award_id) as funder_award_id,
    CAST(NULL AS DOUBLE) as amount,
    CAST(NULL AS STRING) as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    p.funding_type as funding_type,
    p.programme as funder_scheme,
    'efsd_recipients' as provenance,
    TRY_TO_DATE(CONCAT(p.award_year, '-01-01'), 'yyyy-MM-dd') as start_date,
    CAST(NULL AS DATE) as end_date,
    TRY_CAST(p.award_year AS INT) as start_year,
    CAST(NULL AS INT) as end_year,
    p.pi as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
         affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CASE WHEN p.pi IS NOT NULL THEN array(p.pi) END as investigators,
    p.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(p.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM parsed p
CROSS JOIN src_funder f
WHERE p.funder_award_id IS NOT NULL AND TRIM(p.funder_award_id) != ''
)
