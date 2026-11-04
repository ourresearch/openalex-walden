-- Mapping from CreateTACRAwards.ipynb (origin e22fdf7f): its awards-table SELECT over starfos_tacr_input (was the
-- staging table read from s3://openalex-ingest/awards/tacr/tacr_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320315323  -- Technology Agency of the Czech Republic
),
g AS (
    SELECT
        r.*,
        TRY_TO_DATE(r.start_date, 'yyyy-MM-dd') AS start_d,
        TRY_TO_DATE(r.end_date, 'yyyy-MM-dd') AS end_d,
        COALESCE(from_json(r.investigators_json,
            'array<struct<name_raw:string,given_name:string,family_name:string,orcid:string,vedidk:string>>'),
            array()) AS people
    FROM starfos_tacr_input r
    WHERE r.project_code IS NOT NULL AND TRIM(r.project_code) != ''
),
p AS (
    SELECT
        g.*,
        CASE WHEN g.lead_family_name IS NOT NULL THEN named_struct(
            'given_name', NULLIF(TRIM(g.lead_given_name), ''),
            'family_name', NULLIF(TRIM(g.lead_family_name), ''),
            'orcid', CASE WHEN g.lead_orcid RLIKE '^[0-9]{4}-' THEN CONCAT('https://orcid.org/', g.lead_orcid) END,
            'role_start', CAST(NULL AS DATE),
            'affiliation', named_struct(
                'name', NULLIF(TRIM(g.lead_institution), ''),
                'country', NULLIF(TRIM(g.lead_country), ''),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            )
        ) END AS lead,
        TRANSFORM(g.people, x -> named_struct(
            'given_name', NULLIF(TRIM(x.given_name), ''),
            'family_name', NULLIF(TRIM(x.family_name), ''),
            'orcid', CASE WHEN x.orcid RLIKE '^[0-9]{4}-' THEN CONCAT('https://orcid.org/', x.orcid) END,
            'role_start', CAST(NULL AS DATE),
            'affiliation', CAST(NULL AS STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>)
        )) AS team
    FROM g
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(p.project_code))))) % 9000000000 as id,
    COALESCE(NULLIF(TRIM(p.title_en), ''), NULLIF(TRIM(p.title_cs), '')) as display_name,
    COALESCE(NULLIF(TRIM(p.description_en), ''), NULLIF(TRIM(p.description_cs), '')) as description,
    f.funder_id,
    TRIM(p.project_code) as funder_award_id,
    TRY_CAST(p.public_support_czk AS DOUBLE) as amount,
    'CZK' as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'research' as funding_type,
    NULLIF(TRIM(p.programme), '') as funder_scheme,
    'starfos_tacr' as provenance,
    p.start_d as start_date,
    p.end_d as end_date,
    COALESCE(YEAR(p.start_d), TRY_CAST(p.start_year AS INT)) as start_year,
    COALESCE(YEAR(p.end_d), TRY_CAST(p.end_year AS INT)) as end_year,
    p.lead as lead_investigator,
    CASE WHEN TRY_CAST(p.n_main_researchers AS INT) >= 2 THEN try_element_at(p.team, 2) END as co_lead_investigator,
    CASE WHEN p.lead IS NOT NULL AND size(p.team) > 0
         THEN concat(array(p.lead), slice(p.team, 2, size(p.team) - 1))
         WHEN size(p.team) > 0 THEN p.team END as investigators,
    p.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(p.project_code))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM p
CROSS JOIN src_funder f
)
