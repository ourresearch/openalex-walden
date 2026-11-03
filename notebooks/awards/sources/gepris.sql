-- Mapping from CreateDFGAwards.ipynb (origin e22fdf7f): its awards-table SELECT over gepris_input (was the
-- staging table read from s3://openalex-ingest/awards/dfg/dfg_projects.parquet); created_date and updated_date are the run timestamp.
WITH
dfg_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320879  -- Deutsche Forschungsgemeinschaft
),

dfg_projects AS (
    SELECT
        *,
        (end_year = start_year AND NOT (program_type <=> 'Forschungsgroßgeräte')) as end_unknown
    FROM gepris_input
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000 as id,

        g.title as display_name,

        g.description as description,

        f.funder_id,
        g.project_id as funder_award_id,

        TRY_CAST(g.amount AS DOUBLE) as amount,
        'EUR' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(g.program_type) LIKE '%emmy noether%' THEN 'fellowship'
            WHEN LOWER(g.program_type) LIKE '%heisenberg%' THEN 'fellowship'
            WHEN LOWER(g.program_type) LIKE '%walter benjamin%' THEN 'fellowship'
            WHEN LOWER(g.program_type) LIKE '%research fellowship%' THEN 'fellowship'
            WHEN LOWER(g.program_type) LIKE '%stipend%' THEN 'fellowship'
            WHEN LOWER(g.program_type) LIKE '%graduiertenkolleg%' THEN 'training'
            WHEN LOWER(g.program_type) LIKE '%research training%' THEN 'training'
            WHEN LOWER(g.program_type) LIKE '%collaborative research%' THEN 'research'
            WHEN LOWER(g.program_type) LIKE '%sonderforschungsbereich%' THEN 'research'
            WHEN LOWER(g.program_type) LIKE '%priority programme%' THEN 'research'
            WHEN LOWER(g.program_type) LIKE '%schwerpunktprogramm%' THEN 'research'
            WHEN LOWER(g.program_type) LIKE '%forschergruppe%' THEN 'research'
            WHEN LOWER(g.program_type) LIKE '%research unit%' THEN 'research'
            WHEN LOWER(g.program_type) LIKE '%cluster%' THEN 'research'
            WHEN LOWER(g.program_type) LIKE '%excellence%' THEN 'research'
            WHEN LOWER(g.program_type) LIKE '%infrastructure%' THEN 'infrastructure'
            WHEN LOWER(g.program_type) LIKE '%equipment%' THEN 'infrastructure'
            ELSE 'grant'
        END as funding_type,

        g.program_type as funder_scheme,

        'gepris' as provenance,

        COALESCE(
            TRY_TO_DATE(g.start_date, 'yyyy-MM-dd'),
            TRY_TO_DATE(CONCAT(g.start_year, '-01-01'), 'yyyy-MM-dd')
        ) as start_date,
        CASE WHEN g.end_unknown THEN NULL ELSE COALESCE(
            TRY_TO_DATE(g.end_date, 'yyyy-MM-dd'),
            TRY_TO_DATE(CONCAT(g.end_year, '-12-31'), 'yyyy-MM-dd')
        ) END as end_date,
        COALESCE(
            YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')),
            TRY_CAST(g.start_year AS INT)
        ) as start_year,
        CASE WHEN g.end_unknown THEN NULL ELSE COALESCE(
            YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')),
            TRY_CAST(g.end_year AS INT)
        ) END as end_year,

        CASE
            WHEN g.lead_inst IS NOT NULL AND TRIM(g.lead_inst) != '' THEN
                struct(
                    CAST(NULL AS STRING) as given_name,
                    CAST(NULL AS STRING) as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.lead_inst as name,
                        'Germany' as country,
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

        CONCAT('https://gepris.dfg.de/gepris/projekt/', g.project_id) as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM dfg_projects g
    CROSS JOIN dfg_funder f
    WHERE g.project_id IS NOT NULL
      AND TRIM(CAST(g.project_id AS STRING)) != ''
)
