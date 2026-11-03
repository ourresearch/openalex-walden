-- Mapping from CreateDNRFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over dnrf_dg_dk_input (was the
-- staging table read from s3://openalex-ingest/awards/dnrf/dnrf_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320943  -- Danmarks Grundforskningsfond
),

g AS (
    SELECT
        r.*,
        TRY_TO_DATE(r.start_date, 'yyyy-MM-dd') AS sd,
        TRY_TO_DATE(r.end_date, 'yyyy-MM-dd') AS ed,
        CASE WHEN NULLIF(TRIM(r.lead_family_name), '') IS NOT NULL THEN named_struct(
            'given_name', NULLIF(TRIM(r.lead_given_name), ''),
            'family_name', NULLIF(TRIM(r.lead_family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(r.host_institution), ''),
                'country', CAST(NULL AS STRING),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        ) END AS lead,
        CASE WHEN NULLIF(TRIM(r.co_lead_family_name), '') IS NOT NULL THEN named_struct(
            'given_name', NULLIF(TRIM(r.co_lead_given_name), ''),
            'family_name', NULLIF(TRIM(r.co_lead_family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(r.host_institution), ''),
                'country', CAST(NULL AS STRING),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        ) END AS co_lead
    FROM dnrf_dg_dk_input r
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    NULLIF(TRIM(g.description), '') as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    TRY_CAST(g.amount AS DOUBLE) as amount,
    CASE WHEN TRY_CAST(g.amount AS DOUBLE) IS NOT NULL THEN 'DKK' END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    CASE WHEN g.programme IN ('Center of Excellence', 'Pioneer Centre') THEN 'research'
         ELSE 'fellowship' END as funding_type,
    g.programme as funder_scheme,
    'dnrf_dg_dk' as provenance,
    g.sd as start_date,
    g.ed as end_date,
    COALESCE(YEAR(g.sd), TRY_CAST(g.start_year AS INT), TRY_CAST(g.established_year AS INT)) as start_year,
    COALESCE(YEAR(g.ed), TRY_CAST(g.end_year AS INT)) as end_year,
    g.lead as lead_investigator,
    g.co_lead as co_lead_investigator,
    CASE WHEN g.lead IS NOT NULL AND g.co_lead IS NOT NULL THEN array(g.lead, g.co_lead)
         WHEN g.lead IS NOT NULL THEN array(g.lead) END as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM g
CROSS JOIN src_funder f
WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
)
