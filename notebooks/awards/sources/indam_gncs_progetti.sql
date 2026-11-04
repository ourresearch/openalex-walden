-- Mapping from CreateGNCSAwards.ipynb (origin e22fdf7f): its awards-table SELECT over indam_gncs_progetti_input (was the
-- staging table read from s3://openalex-ingest/awards/gncs/gncs_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320334079  -- GNCS (INdAM)
),

lead AS (
    SELECT
        g.*,
        named_struct(
            'given_name', NULLIF(TRIM(g.lead_given_name), ''),
            'family_name', NULLIF(TRIM(g.lead_family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(g.lead_institution), ''),
                'country', 'IT',
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        ) AS lead_struct
    FROM indam_gncs_progetti_input g
    WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
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
    CONCAT(g.scheme, ' ', g.year) as funder_scheme,
    'indam_gncs_progetti' as provenance,
    CAST(NULL AS DATE) as start_date,
    CAST(NULL AS DATE) as end_date,
    TRY_CAST(g.year AS INT) as start_year,
    TRY_CAST(g.year AS INT) as end_year,
    g.lead_struct as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    ARRAY(g.lead_struct) as investigators,
    g.source_pdf as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM lead g
CROSS JOIN src_funder f
)
