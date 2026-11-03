-- Mapping from CreateBeijingBMSTCAwards.ipynb (origin e22fdf7f): its awards-table SELECT over beijing_bmstc_input (was the
-- staging table read from s3://openalex-ingest/awards/beijing_bmstc/beijing_bmstc_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320325902
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000 as id,

        g.display_name as display_name,
        CAST(NULL AS STRING) as description,

        f.funder_id,
        NULLIF(TRIM(g.funder_award_id), '') as funder_award_id,

        TRY_CAST(g.amount AS DOUBLE) as amount,
        CASE WHEN TRY_CAST(g.amount AS DOUBLE) IS NOT NULL THEN 'CNY' ELSE NULL END as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        'research' as funding_type,

        NULLIF(TRIM(g.programme), '') as funder_scheme,

        'beijing_bmstc' as provenance,

        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        COALESCE(YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')), TRY_CAST(g.start_year AS INT)) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        CASE
            WHEN (g.lead_family_name IS NOT NULL AND TRIM(g.lead_family_name) != '')
              OR (g.institution IS NOT NULL AND TRIM(g.institution) != '') THEN
                struct(
                    CAST(NULL AS STRING) as given_name,
                    NULLIF(TRIM(g.lead_family_name), '') as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        NULLIF(TRIM(g.institution), '') as name,
                        'China' as country,
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

        g.landing_page_url as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.funder_award_id))))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM beijing_bmstc_input g
    CROSS JOIN src_funder f
    WHERE g.display_name IS NOT NULL
      AND TRIM(g.display_name) != ''
      AND g.funder_award_id IS NOT NULL
      AND TRIM(g.funder_award_id) != ''
)
