-- Mapping from CreateAllianceCanRACAwards.ipynb (origin e22fdf7f): its awards-table SELECT over alliancecan_rac_input (was the
-- staging table read from s3://openalex-ingest/awards/alliancecan_rac/alliancecan_rac_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id IN (4320314000, 4320331257)
),

g AS (
    SELECT
        r.*,
        TRY_CAST(r.funder_id AS BIGINT) AS funder_id_num,
        TRIM(r.funder_award_id) AS award_id
    FROM alliancecan_rac_input r
    WHERE r.funder_award_id IS NOT NULL AND TRIM(r.funder_award_id) != ''
),

lead AS (
    SELECT
        funder_id_num,
        award_id,
        CASE WHEN NULLIF(TRIM(lead_family_name), '') IS NOT NULL THEN
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
        ) END AS lead_investigator
    FROM g
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.award_id)))) % 9000000000 as id,
    COALESCE(NULLIF(TRIM(g.title), ''), CONCAT('RAC allocation ', g.award_id)) as display_name,
    CONCAT_WS(' ',
        'In-kind allocation of national advanced research computing resources (no monetary award).',
        CONCAT(g.allocation_text, '.'),
        CASE WHEN NULLIF(TRIM(g.summary), '') IS NOT NULL THEN CONCAT('Project summary: ', TRIM(g.summary)) END
    ) as description,
    f.funder_id,
    g.award_id as funder_award_id,
    CAST(NULL AS DOUBLE) as amount,
    CAST(NULL AS STRING) as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'grant' as funding_type,
    NULLIF(TRIM(g.funder_scheme), '') as funder_scheme,
    'alliancecan_rac' as provenance,
    CAST(NULL AS DATE) as start_date,
    CAST(NULL AS DATE) as end_date,
    TRY_CAST(g.first_year AS INT) as start_year,
    TRY_CAST(g.last_year AS INT) + 1 as end_year,
    l.lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CASE WHEN l.lead_investigator IS NOT NULL THEN array(l.lead_investigator) END as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.award_id)))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM g
JOIN lead l ON l.funder_id_num = g.funder_id_num AND l.award_id = g.award_id
JOIN src_funder f ON f.funder_id = g.funder_id_num
)
