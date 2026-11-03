-- Mapping from CreateCSACanadaAwards.ipynb (origin e22fdf7f): its awards-table SELECT over csa_canada_proactive_input (was the
-- staging table read from s3://openalex-ingest/awards/csa_canada/csa_canada_projects.parquet); created_date and updated_date are the run timestamp.
WITH
csa_canada_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320334436  -- Canadian Space Agency
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,
        COALESCE(g.title_en, g.description_en) as display_name,
        g.description_en as description,
        f.funder_id,
        g.funder_award_id as funder_award_id,
        CASE WHEN TRY_CAST(g.amount AS DOUBLE) > 0 THEN TRY_CAST(g.amount AS DOUBLE) ELSE NULL END as amount,
        CASE WHEN TRY_CAST(g.amount AS DOUBLE) > 0 THEN 'CAD' ELSE NULL END as currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name, f.ror_id, f.doi
        ) as funder,
        CASE
            WHEN LOWER(g.prog_name_en) RLIKE '(fellowship)' THEN 'fellowship'
            ELSE 'research'
        END as funding_type,
        g.prog_name_en as funder_scheme,
        'csa_canada_proactive' as provenance,
        CASE WHEN TRY_CAST(g.start_year AS INT) IS NOT NULL
             THEN CAST(CONCAT(g.start_year, '-01-01') AS DATE) ELSE NULL END as start_date,
        CASE WHEN TRY_CAST(g.end_year AS INT) IS NOT NULL
             THEN CAST(CONCAT(g.end_year, '-12-31') AS DATE) ELSE NULL END as end_date,
        TRY_CAST(g.start_year AS INT) as start_year,
        TRY_CAST(g.end_year AS INT) as end_year,
        CASE
            WHEN g.institution_name IS NOT NULL
             AND g.institution_name != ''
             AND TRIM(g.institution_name) != '' THEN
                struct(
                    CAST(NULL AS STRING) as given_name,
                    CAST(NULL AS STRING) as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.institution_name as name,
                        g.recipient_country as country,
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
        'https://analisi.transparenciacatalunya.cat/Economia/Concessions-del-RAISC-Registre-de-subvencions-i-aju/s9xt-n979' as landing_page_url,
        CAST(NULL AS STRING) as doi,
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000) as works_api_url,
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date
    FROM csa_canada_proactive_input g
    CROSS JOIN csa_canada_funder f
    WHERE g.funder_award_id IS NOT NULL
      AND TRIM(CAST(g.funder_award_id AS STRING)) != ''
)
