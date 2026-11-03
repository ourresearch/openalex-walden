-- Mapping from CreateBMELAwards.ipynb (origin e22fdf7f): its awards-table SELECT over foerderkatalog_bmel_input (was the
-- staging table read from s3://openalex-ingest/awards/bmel/bmel_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320323499  -- Bundesministerium für Ernährung und Landwirtschaft
),
g AS (
    SELECT
        TRIM(fkz) AS funder_award_id,
        NULLIF(TRIM(title), '') AS title,
        TRY_CAST(amount_eur AS DOUBLE) AS amount,
        NULLIF(TRIM(lps_text), '') AS scheme,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS start_d,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS end_d,
        CASE WHEN NULLIF(TRIM(recipient), '') IS NOT NULL AND recipient NOT LIKE 'Keine Anzeige%' THEN TRIM(recipient) END AS inst,
        NULLIF(TRIM(recipient_country_iso), '') AS inst_country,
        landing_page_url
    FROM foerderkatalog_bmel_input
    WHERE fkz IS NOT NULL AND TRIM(fkz) != '' AND ressort = 'BMLEH'
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,
    g.title as display_name,
    CAST(NULL AS STRING) as description,
    f.funder_id,
    g.funder_award_id,
    g.amount,
    'EUR' as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    'research' as funding_type,
    g.scheme as funder_scheme,
    'foerderkatalog_bmel' as provenance,
    g.start_d as start_date,
    g.end_d as end_date,
    YEAR(g.start_d) as start_year,
    YEAR(g.end_d) as end_year,
    CASE WHEN g.inst IS NOT NULL THEN named_struct(
        'given_name', CAST(NULL AS STRING),
        'family_name', CAST(NULL AS STRING),
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', g.inst,
            'country', g.inst_country,
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) END as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) as investigators,
    g.landing_page_url as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM g
CROSS JOIN src_funder f
)
