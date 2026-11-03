-- Mapping from CreateAndaluciaAwards.ipynb (origin e22fdf7f): its awards-table SELECT over andalucia_input (was the
-- staging table read from s3://openalex-ingest/awards/andalucia/andalucia_projects.parquet); created_date and updated_date are the run timestamp.
WITH
andalucia_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320326754  -- Junta de Andalucía
),
mapped_awards AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,
        g.announcement as display_name,
        NULLIF(TRIM(g.finality), '') as description,
        f.funder_id,
        g.funder_award_id,
        CASE WHEN TRY_CAST(g.amount AS DOUBLE) > 0 THEN TRY_CAST(g.amount AS DOUBLE) ELSE NULL END as amount,
        CASE WHEN TRY_CAST(g.amount AS DOUBLE) > 0 THEN 'EUR' ELSE NULL END as currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name, f.ror_id, f.doi
        ) as funder,
        CASE
            WHEN LOWER(g.announcement) RLIKE '(predoctoral|postdoctoral|posdoctoral|contrat|talentia|beca)' THEN 'fellowship'
            ELSE 'research'
        END as funding_type,
        NULLIF(TRIM(g.name_program), '') as funder_scheme,
        'andalucia' as provenance,
        TRY_TO_DATE(g.grant_date, 'yyyy-MM-dd') as start_date,
        CAST(NULL AS DATE) as end_date,
        COALESCE(YEAR(TRY_TO_DATE(g.grant_date, 'yyyy-MM-dd')), TRY_CAST(g.award_year AS INT)) as start_year,
        CAST(NULL AS INT) as end_year,
        CASE
            WHEN g.institution_name IS NOT NULL AND TRIM(g.institution_name) != '' THEN
                struct(
                    CAST(NULL AS STRING) as given_name,
                    CAST(NULL AS STRING) as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.institution_name as name,
                        'Spain' as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            ELSE NULL
        END as lead_investigator,
        CAST(NULL AS STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING,
            role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as co_lead_investigator,
        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING,
            role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) as investigators,
        'https://www.juntadeandalucia.es/datosabiertos/portal/dataset/subvenciones-otorgadas-por-la-junta-de-andalucia' as landing_page_url,
        CAST(NULL AS STRING) as doi
    FROM andalucia_input g
    CROSS JOIN andalucia_funder f
),

awards_transformed AS (
SELECT *,
    concat('https://api.openalex.org/works?filter=awards.id:G', id) as works_api_url,
    session.awards_timestamp as created_date,
    session.awards_timestamp as updated_date
FROM mapped_awards
)
