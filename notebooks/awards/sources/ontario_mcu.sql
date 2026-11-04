-- Mapping from CreateOntarioMCUAwards.ipynb (origin e22fdf7f): its awards-table SELECT over ontario_mcu_input (was the
-- staging table read from s3://openalex-ingest/awards/ontario_mcu/ontario_mcu_grants.parquet); created_date and updated_date are the run timestamp.
WITH
ontario_funder AS (
    SELECT CAST(funder_id AS BIGINT) as funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders WHERE funder_id = 4320331473
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,
        COALESCE(NULLIF(TRIM(g.title), ''), CONCAT('Ontario ', COALESCE(g.scheme, ''), ' - ', g.institution), CONCAT('Ontario MCU project ', g.funder_award_id)) as display_name,
        g.description as description,
        f.funder_id,
        g.funder_award_id as funder_award_id,
        CASE WHEN TRY_CAST(g.amount AS DECIMAL(18,2)) > 0 THEN TRY_CAST(g.amount AS DECIMAL(18,2)) ELSE NULL END as amount,
        CASE WHEN TRY_CAST(g.amount AS DECIMAL(18,2)) > 0 THEN g.currency ELSE NULL END as currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name, f.ror_id, f.doi
        ) as funder,
        'grant' as funding_type,
        g.scheme as funder_scheme,
        'ontario_mcu' as provenance,
        TRY_TO_DATE(g.start_date_raw, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date_raw, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date_raw, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date_raw, 'yyyy-MM-dd')) as end_year,
        CASE
            WHEN g.pi_family IS NOT NULL THEN
                struct(
                    g.pi_given as given_name,
                    g.pi_family as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.institution as name,
                        'Canada' as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            ELSE NULL
        END as lead_investigator,
        CAST(NULL AS STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as co_lead_investigator,
        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) as investigators,
        g.landing_page_url,
        CAST(NULL AS STRING) as doi,
        CAST(NULL AS STRING) as works_api_url,
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date
    FROM ontario_mcu_input g
    CROSS JOIN ontario_funder f
)
