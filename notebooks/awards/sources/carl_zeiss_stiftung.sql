-- Mapping from CreateCarlZeissAwards.ipynb (origin e22fdf7f): its awards-table SELECT over carl_zeiss_stiftung_input (was the
-- staging table read from s3://openalex-ingest/awards/carl_zeiss/carl_zeiss_projects.parquet); created_date and updated_date are the run timestamp.
WITH
 czs_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320309895  -- Carl-Zeiss-Stiftung
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.slug)))) % 9000000000 as id,
    g.title as display_name,
    g.description,
    f.funder_id,
    g.slug as funder_award_id,
    TRY_CAST(g.amount_eur AS DOUBLE) as amount,
    g.currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    CASE
        WHEN LOWER(g.type_of_funding) LIKE '%individual%'
          OR LOWER(g.type_of_funding) LIKE '%fellowship%' THEN 'fellowship'
        WHEN LOWER(g.type_of_funding) LIKE '%project%' THEN 'research'
        ELSE 'grant'
    END as funding_type,
    g.programme as funder_scheme,
    'carl_zeiss_stiftung' as provenance,
    TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
    TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
    YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
    YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,
    struct(
        CAST(NULL AS STRING) as given_name,
        CAST(NULL AS STRING) as family_name,
        CAST(NULL AS STRING) as orcid,
        CAST(NULL AS DATE) as role_start,
        struct(
            g.funded_institution as name,
            'DE' as country,
            CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
        ) as affiliation
    ) as lead_investigator,
    CAST(NULL AS STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >) as co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<
        given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >>) as investigators,
    g.url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.slug)))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM carl_zeiss_stiftung_input g
CROSS JOIN czs_funder f
WHERE g.slug IS NOT NULL AND TRIM(g.slug) != ''
)
