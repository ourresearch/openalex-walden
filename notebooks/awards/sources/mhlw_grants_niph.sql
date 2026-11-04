-- Mapping from CreateMHLWGrantsAwards.ipynb (origin e22fdf7f): its awards-table SELECT over mhlw_grants_niph_input (was the
-- staging table read from s3://openalex-ingest/awards/mhlw_grants/mhlw_grants_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321945  -- Ministry of Health, Labour and Welfare
),

g AS (
    SELECT
        r.*,
        TRIM(r.project_no) AS award_no,
        TRY_CAST(r.start_fy AS INT) AS sfy,
        TRY_CAST(r.end_fy AS INT) AS efy,
        NULLIF(TRIM(r.lead_given_name), '') AS given,
        NULLIF(TRIM(r.lead_family_name), '') AS family,
        from_json(r.co_investigators_json,
                  'array<struct<name:string, given_name:string, family_name:string, former_name:string, affiliation:string>>') AS co
    FROM mhlw_grants_niph_input r
    WHERE r.project_no IS NOT NULL AND TRIM(r.project_no) != ''
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.award_no)))) % 9000000000 as id,
    NULLIF(TRIM(g.title), '') as display_name,
    NULLIF(TRIM(g.aim), '') as description,
    f.funder_id,
    g.award_no as funder_award_id,
    TRY_CAST(g.amount_jpy AS DOUBLE) as amount,
    CASE WHEN TRY_CAST(g.amount_jpy AS DOUBLE) IS NOT NULL THEN 'JPY' END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'research' as funding_type,
    NULLIF(TRIM(g.scheme), '') as funder_scheme,
    'mhlw_grants_niph' as provenance,
    CASE WHEN g.sfy IS NOT NULL THEN TRY_TO_DATE(CONCAT(g.sfy, '-04-01'), 'yyyy-MM-dd') END as start_date,
    CASE WHEN g.efy IS NOT NULL THEN TRY_TO_DATE(CONCAT(g.efy + 1, '-03-31'), 'yyyy-MM-dd') END as end_date,
    g.sfy as start_year,
    g.efy as end_year,
    CASE WHEN g.family IS NOT NULL THEN named_struct(
        'given_name', g.given,
        'family_name', g.family,
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', NULLIF(TRIM(g.lead_affiliation), ''),
            'country', CAST(NULL AS STRING),
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) END as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
         affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CASE WHEN size(filter(COALESCE(g.co, array()), x -> x.family_name IS NOT NULL)) > 0 THEN
        transform(filter(g.co, x -> x.family_name IS NOT NULL), x -> named_struct(
            'given_name', x.given_name,
            'family_name', x.family_name,
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(x.affiliation), ''),
                'country', CAST(NULL AS STRING),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        ))
    END as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.award_no)))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM g
CROSS JOIN src_funder f
)
