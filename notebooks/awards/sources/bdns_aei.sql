-- Mapping from CreateAEIAwards.ipynb (origin e22fdf7f): its awards-table SELECT over bdns_aei_input (was the
-- staging table read from s3://openalex-ingest/awards/aei/aei_grants.parquet); created_date and updated_date are the run timestamp.
WITH
aei_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320335598  -- Agencia Estatal de Investigación
),

grants_with_org AS (
    SELECT
        *,
        TRIM(REGEXP_REPLACE(beneficiary, '^[A-Z0-9]+\\s+', '')) as org_name
    FROM bdns_aei_input
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.grant_code)))) % 9000000000 as id,

    g.call_title as display_name,

    g.call_title as description,

    f.funder_id,
    g.grant_code as funder_award_id,

    TRY_CAST(g.amount AS DOUBLE) as amount,
    'EUR' as currency,

    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,

    CASE
        WHEN g.instrument_type LIKE '%PRÉSTAMO%' THEN 'loan'
        WHEN g.instrument_type LIKE '%SUBVENCI%' THEN 'grant'
        ELSE 'grant'
    END as funding_type,

    g.instrument_type as funder_scheme,

    'bdns_aei' as provenance,

    TRY_TO_DATE(g.grant_date, 'yyyy-MM-dd') as start_date,
    CAST(NULL AS DATE) as end_date,
    TRY_CAST(g.grant_year AS INT) as start_year,
    CAST(NULL AS INT) as end_year,

    struct(
        CAST(NULL AS STRING) as given_name,
        CAST(NULL AS STRING) as family_name,
        CAST(NULL AS STRING) as orcid,
        CAST(NULL AS DATE) as role_start,
        struct(
            g.org_name as name,
            'ES' as country,
            CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
        ) as affiliation
    ) as lead_investigator,

    CAST(NULL AS STRUCT<
        given_name:STRING,
        family_name:STRING,
        orcid:STRING,
        role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
    >) as co_lead_investigator,

    CAST(NULL AS ARRAY<STRUCT<
        given_name:STRING,
        family_name:STRING,
        orcid:STRING,
        role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>
    >>>) as investigators,

    g.legal_basis_url as landing_page_url,

    CAST(NULL AS STRING) as doi,

    concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.grant_code)))) % 9000000000) as works_api_url,

    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date

FROM grants_with_org g
CROSS JOIN aei_funder f
WHERE g.grant_code IS NOT NULL
  AND TRIM(g.grant_code) != ''
)
