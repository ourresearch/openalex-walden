-- Mapping from CreateARAMISSERIAwards.ipynb (origin e22fdf7f): its awards-table SELECT over aramis_seri_input (was the
-- staging table read from s3://openalex-ingest/awards/aramis_seri/aramis_seri_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320335238  -- State Secretariat for Education, Research and Innovation
),
g AS (
    SELECT
        TRIM(aramis_number) AS funder_award_id,
        COALESCE(NULLIF(TRIM(title_en), ''), NULLIF(TRIM(title_de), ''), NULLIF(TRIM(title_fr), ''), NULLIF(TRIM(title_it), '')) AS title,
        NULLIF(TRIM(abstract), '') AS abstract,
        TRY_CAST(granted_total_costs_chf AS DOUBLE) AS amount,
        NULLIF(TRIM(section), '') AS section,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS start_d,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS end_d,
        CASE WHEN lead_family_name IS NOT NULL THEN named_struct(
            'given_name', NULLIF(TRIM(lead_given_name), ''),
            'family_name', NULLIF(TRIM(lead_family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(lead_institution), ''),
                'country', CAST(NULL AS STRING),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        ) END AS lead
    FROM aramis_seri_input
    WHERE aramis_number IS NOT NULL AND TRIM(aramis_number) != ''
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,
    g.title as display_name,
    g.abstract as description,
    f.funder_id,
    g.funder_award_id,
    g.amount,
    'CHF' as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'research' as funding_type,
    g.section as funder_scheme,
    'aramis_seri' as provenance,
    g.start_d as start_date,
    g.end_d as end_date,
    YEAR(g.start_d) as start_year,
    YEAR(g.end_d) as end_year,
    g.lead as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CASE WHEN g.lead IS NOT NULL THEN array(g.lead) END as investigators,
    CAST(NULL AS STRING) as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM g
CROSS JOIN src_funder f
)
