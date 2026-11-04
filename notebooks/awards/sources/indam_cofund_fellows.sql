-- Mapping from CreateINdAMAwards.ipynb (origin e22fdf7f): its awards-table SELECT over indam_cofund_fellows_input (was the
-- staging table read from s3://openalex-ingest/awards/indam/indam_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320311030  -- INdAM "Francesco Severi"
),

lead AS (
    SELECT
        g.*,
        named_struct(
            'given_name', NULLIF(TRIM(g.lead_given_name), ''),
            'family_name', NULLIF(TRIM(g.lead_family_name), ''),
            'orcid', CAST(NULL AS STRING),
            'role_start', CAST(NULL AS DATE),
            'affiliation', CASE WHEN NULLIF(TRIM(g.host_institution), '') IS NOT NULL THEN named_struct(
                'name', TRIM(g.host_institution),
                'country', CAST(NULL AS STRING),
                'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
            ) END
        ) AS lead_struct
    FROM indam_cofund_fellows_input g
    WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    COALESCE(NULLIF(TRIM(g.title), ''),
             CONCAT(g.programme, ' ', COALESCE(LOWER(g.fellow_type), 'post-doc'), ' fellowship: ', g.name)) as display_name,
    NULLIF(TRIM(g.abstract), '') as description,
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
    CONCAT_WS(' - ', g.programme, g.call, CASE WHEN g.fellow_type IS NOT NULL THEN CONCAT(g.fellow_type, ' fellowship') END) as funder_scheme,
    'indam_cofund_fellows' as provenance,
    TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
    TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
    YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
    YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,
    g.lead_struct as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    ARRAY(g.lead_struct) as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM lead g
CROSS JOIN src_funder f
)
