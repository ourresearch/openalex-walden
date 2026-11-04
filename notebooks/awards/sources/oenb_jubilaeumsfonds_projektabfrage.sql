-- Mapping from CreateOeNBJubilaeumsfondsAwards.ipynb (origin e22fdf7f): its awards-table SELECT over oenb_jubilaeumsfonds_projektabfrage_input (was the
-- staging table read from s3://openalex-ingest/awards/oenb_jubilaeumsfonds/oenb_jubilaeumsfonds_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320322325  -- Oesterreichische Nationalbank
),

src AS (
    SELECT *,
        named_struct(
            'given_name', NULLIF(TRIM(pi_given_name), ''),
            'family_name', NULLIF(TRIM(pi_family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(institute), ''),
                'country', CAST(NULL AS STRING),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        ) AS pi
    FROM oenb_jubilaeumsfonds_projektabfrage_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    COALESCE(NULLIF(TRIM(g.description), ''), NULLIF(TRIM(g.short_report), '')) as description,
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
    'research' as funding_type,
    CONCAT('Jubiläumsfonds', CASE WHEN g.discipline IS NOT NULL THEN CONCAT(' - ', g.discipline) ELSE '' END) as funder_scheme,
    'oenb_jubilaeumsfonds_projektabfrage' as provenance,
    TRY_TO_DATE(g.approved_date, 'yyyy-MM-dd') as start_date,
    CAST(NULL AS DATE) as end_date,
    YEAR(TRY_TO_DATE(g.approved_date, 'yyyy-MM-dd')) as start_year,
    CAST(NULL AS INT) as end_year,
    CASE WHEN g.pi.family_name IS NOT NULL THEN g.pi END as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
         affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CASE WHEN g.pi.family_name IS NOT NULL THEN array(g.pi) END as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM src g
CROSS JOIN src_funder f
WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
)
