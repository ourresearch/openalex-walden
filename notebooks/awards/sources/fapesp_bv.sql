-- Mapping from CreateFAPESPAwards.ipynb (origin e22fdf7f): its awards-table SELECT over fapesp_bv_input (was the
-- staging table read from s3://openalex-ingest/awards/fapesp/fapesp_projects.parquet); created_date and updated_date are the run timestamp.
WITH
fapesp_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320997  -- FAPESP
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.process_number)))) % 9000000000 as id,

        COALESCE(NULLIF(TRIM(g.title_en), ''), g.title_pt) as display_name,

        COALESCE(NULLIF(TRIM(g.abstract_en), ''), g.abstract_pt) as description,

        f.funder_id,
        g.process_number as funder_award_id,

        CAST(NULL AS DOUBLE) as amount,
        'BRL' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(g.support_modality) LIKE '%bolsa%' THEN 'fellowship'
            WHEN LOWER(g.support_modality) LIKE '%jovens pesquisadores%' THEN 'fellowship'
            WHEN LOWER(g.support_modality) LIKE '%young investigators%' THEN 'fellowship'
            WHEN LOWER(g.support_modality) LIKE '%pesquisador visitante%' THEN 'fellowship'
            WHEN LOWER(g.support_modality) LIKE '%visiting researcher%' THEN 'fellowship'
            WHEN LOWER(g.support_modality) LIKE '%infraestrutura%' THEN 'infrastructure'
            WHEN LOWER(g.support_modality) LIKE '%equipamentos%' THEN 'infrastructure'
            WHEN LOWER(g.support_modality) LIKE '%reunião%' THEN 'conference'
            WHEN LOWER(g.support_modality) LIKE '%organização%' THEN 'conference'
            WHEN LOWER(g.support_modality) LIKE '%publicações%' THEN 'publication'
            WHEN LOWER(g.support_modality) LIKE '%pipe%' THEN 'innovation'
            WHEN LOWER(g.support_modality) LIKE '%pite%' THEN 'innovation'
            WHEN LOWER(g.support_modality) LIKE '%inovativa%' THEN 'innovation'
            WHEN LOWER(g.support_modality) LIKE '%temático%' THEN 'research'
            WHEN LOWER(g.support_modality) LIKE '%thematic%' THEN 'research'
            ELSE 'grant'
        END as funding_type,

        g.support_modality as funder_scheme,

        'fapesp_bv' as provenance,

        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        TRY_CAST(YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) AS INT) as start_year,
        TRY_CAST(YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) AS INT) as end_year,

        CASE
            WHEN g.pi_family_name IS NOT NULL THEN
                struct(
                    g.pi_given_name as given_name,
                    g.pi_family_name as family_name,
                    CAST(NULL AS STRING) as orcid,  -- FAPESP data doesn't include ORCID
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.institution as name,
                        'Brazil' as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            ELSE NULL
        END as lead_investigator,

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
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) as investigators,

        CASE WHEN TRIM(g.process_number) RLIKE '^[0-9]{2}/[0-9]{5}-[0-9]$' THEN CONCAT('https://bv.fapesp.br/en/processo/', TRIM(g.process_number)) END as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.process_number)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM fapesp_bv_input g
    CROSS JOIN fapesp_funder f
    WHERE g.process_number IS NOT NULL
      AND TRIM(g.process_number) != ''
)
