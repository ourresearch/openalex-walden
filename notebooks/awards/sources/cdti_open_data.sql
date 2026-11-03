-- Mapping from CreateCDTIAwards.ipynb (origin e22fdf7f): its awards-table SELECT over cdti_open_data_input (was the
-- staging table read from s3://openalex-ingest/awards/cdti/cdti_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321043  -- CDTI
),

awards_transformed AS (
SELECT
    abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,
    NULLIF(TRIM(g.TituloProyecto), '') as display_name,
    CAST(NULL AS STRING) as description,
    f.funder_id,
    TRIM(g.funder_award_id) as funder_award_id,
    NULLIF(TRY_CAST(g.amount_cdti_eur AS DOUBLE), 0) as amount,
    CASE WHEN NULLIF(TRY_CAST(g.amount_cdti_eur AS DOUBLE), 0) IS NOT NULL THEN 'EUR' END as currency,
    struct(
        CONCAT('https://openalex.org/F', f.funder_id) as id,
        f.display_name,
        f.ror_id,
        f.doi
    ) as funder,
    CASE WHEN g.TipoAyuda = 'Subvención' THEN 'grant'
         WHEN g.TipoAyuda = 'Ayudas Parcialmente Reembolsables' THEN 'loan'
         ELSE 'grant' END as funding_type,
    CASE WHEN NULLIF(TRIM(g.InstrumentoFinanciero), '') IS NOT NULL
         THEN CONCAT(TRIM(g.InstrumentoFinanciero), ' - ', TRIM(g.TipoAyuda))
         ELSE NULLIF(TRIM(g.TipoAyuda), '') END as funder_scheme,
    'cdti_open_data' as provenance,
    TRY_TO_DATE(g.approval_date, 'yyyy-MM-dd') as start_date,
    CAST(NULL AS DATE) as end_date,
    YEAR(TRY_TO_DATE(g.approval_date, 'yyyy-MM-dd')) as start_year,
    CAST(NULL AS INT) as end_year,
    CASE WHEN NULLIF(TRIM(g.RazonSocial), '') IS NOT NULL THEN named_struct(
        'given_name', CAST(NULL AS STRING),
        'family_name', CAST(NULL AS STRING),
        'orcid', CAST(NULL AS STRING),
        'role_start', CAST(NULL AS DATE),
        'affiliation', named_struct(
            'name', TRIM(g.RazonSocial),
            'country', 'ES',
            'ids', CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
        )
    ) END as lead_investigator,
    CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
    CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
        affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) as investigators,
    'https://www.cdti.es/datos-abiertos-creditos-subvenciones-y-lineas' as landing_page_url,
    CAST(NULL AS STRING) as doi,
    concat('https://api.openalex.org/works?filter=awards.id:G',
           abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM cdti_open_data_input g
CROSS JOIN src_funder f
WHERE g.funder_award_id IS NOT NULL AND TRIM(g.funder_award_id) != ''
  AND g.TipoAyuda = 'Subvención'
)
