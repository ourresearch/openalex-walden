-- Mapping from CreateCERCAwards.ipynb (origin e22fdf7f): its awards-table SELECT over cerc_chairholders_input (was the
-- staging table read from s3://openalex-ingest/awards/cerc/cerc_chairs.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321481  -- Canada Excellence Research Chairs
),

lead AS (
    SELECT
        funder_award_id,
        named_struct(
            'given_name', NULLIF(TRIM(lead_given_name), ''),
            'family_name', NULLIF(TRIM(lead_family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(institution), ''),
                'country', 'CA',
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        ) AS lead_investigator
    FROM cerc_chairholders_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.chair_title), '') as display_name,
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
    'fellowship' as funding_type,
    NULLIF(TRIM(g.funder_scheme), '') as funder_scheme,
    'cerc_chairholders' as provenance,
    CAST(NULL AS DATE) as start_date,
    CAST(NULL AS DATE) as end_date,
    CAST(NULL AS INT) as start_year,
    CAST(NULL AS INT) as end_year,
    l.lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    array(l.lead_investigator) as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM cerc_chairholders_input g
JOIN lead l ON l.funder_award_id = g.funder_award_id
CROSS JOIN src_funder f
WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
)
