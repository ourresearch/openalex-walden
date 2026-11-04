-- Mapping from CreateMitacsAwards.ipynb (origin e22fdf7f): its awards-table SELECT over mitacs_projects_input (was the
-- staging table read from s3://openalex-ingest/awards/mitacs/mitacs_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320322675  -- Mitacs
),

people AS (
    SELECT
        funder_award_id,
        TRANSFORM(
            FILTER(
                concat(
                    COALESCE(from_json(supervisors_json, 'array<struct<name:string,given_name:string,family_name:string>>'), array()),
                    COALESCE(from_json(students_json, 'array<struct<name:string,given_name:string,family_name:string>>'), array())
                ),
                p -> NULLIF(TRIM(p.family_name), '') IS NOT NULL
            ),
            p -> named_struct(
                'given_name', NULLIF(TRIM(p.given_name), ''),
                'family_name', NULLIF(TRIM(p.family_name), ''),
                'orcid', CAST(NULL AS STRING),
                'role_start', CAST(NULL AS DATE),
                'affiliation', named_struct(
                    'name', NULLIF(TRIM(university), ''),
                    'country', CASE WHEN NULLIF(TRIM(university), '') IS NOT NULL THEN 'CA' END,
                    'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
                )
            )
        ) AS investigators,
        CASE WHEN NULLIF(TRIM(lead_family_name), '') IS NOT NULL THEN named_struct(
            'given_name', NULLIF(TRIM(lead_given_name), ''),
            'family_name', NULLIF(TRIM(lead_family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(university), ''),
                'country', CASE WHEN NULLIF(TRIM(university), '') IS NOT NULL THEN 'CA' END,
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        ) END AS lead_investigator
    FROM mitacs_projects_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    NULLIF(TRIM(g.description), '') as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    CAST(NULL AS DOUBLE) as amount,
    CAST(NULL AS STRING) as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    CASE
        WHEN LOWER(g.program) LIKE '%elevate%' THEN 'fellowship'
        WHEN LOWER(g.program) LIKE '%globalink%' OR LOWER(g.program) LIKE '%business strategy%' THEN 'training'
        ELSE 'research'
    END as funding_type,
    NULLIF(TRIM(g.program), '') as funder_scheme,
    'mitacs_projects' as provenance,
    CAST(NULL AS DATE) as start_date,
    CAST(NULL AS DATE) as end_date,
    CAST(NULL AS INT) as start_year,
    CAST(NULL AS INT) as end_year,
    p.lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CASE WHEN size(p.investigators) > 0 THEN p.investigators END as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM mitacs_projects_input g
JOIN people p ON p.funder_award_id = g.funder_award_id
CROSS JOIN src_funder f
WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
)
