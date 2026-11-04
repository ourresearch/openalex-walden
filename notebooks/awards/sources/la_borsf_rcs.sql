-- Mapping from CreateLouisianaBoRSFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over la_borsf_rcs_input (was the
-- staging table read from s3://openalex-ingest/awards/la_borsf_rcs/la_borsf_rcs_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320309392  -- Louisiana Board of Regents
),

lead AS (
    SELECT
        contract_number,
        named_struct(
            'given_name', NULLIF(TRIM(lead_given_name), ''),
            'family_name', NULLIF(TRIM(lead_family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(institution), ''),
                'country', 'US',
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        ) AS lead_investigator
    FROM la_borsf_rcs_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.contract_number))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    CAST(NULL AS STRING) as description,
    f.funder_id,
    TRIM(g.contract_number) as funder_award_id,
    TRY_CAST(g.amount AS DOUBLE) as amount,
    CASE WHEN TRY_CAST(g.amount AS DOUBLE) IS NOT NULL THEN 'USD' END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'grant' as funding_type,
    'Board of Regents Support Fund - Research Competitiveness Subprogram (RCS)' as funder_scheme,
    'la_borsf_rcs' as provenance,
    TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
    TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
    YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
    YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,
    l.lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    array(l.lead_investigator) as investigators,
    g.report_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.contract_number))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM la_borsf_rcs_input g
JOIN lead l ON l.contract_number = g.contract_number
CROSS JOIN src_funder f
WHERE g.contract_number IS NOT NULL AND TRIM(g.contract_number) != ''
)
