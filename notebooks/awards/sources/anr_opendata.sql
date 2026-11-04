-- Mapping from CreateANRAwards.ipynb (origin e22fdf7f): its awards-table SELECT over anr_opendata_input (was the
-- staging table read from s3://openalex-ingest/awards/anr/anr_projects.parquet); created_date and updated_date are the run timestamp.
WITH
anr_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320883  -- Agence Nationale de la Recherche
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000 as id,

        g.title as display_name,

        g.abstract as description,

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
            WHEN LOWER(g.funding_program) LIKE '%jcjc%' THEN 'fellowship'
            WHEN LOWER(g.funding_program) LIKE '%jeunes%chercheur%' THEN 'fellowship'
            WHEN LOWER(g.funding_program) LIKE '%chaire%' THEN 'fellowship'
            WHEN LOWER(g.funding_program) LIKE '%postdoc%' THEN 'fellowship'
            WHEN LOWER(g.funding_program) LIKE '%equipex%' THEN 'infrastructure'
            WHEN LOWER(g.funding_program) LIKE '%labex%' THEN 'infrastructure'
            WHEN LOWER(g.funding_program) LIKE '%infrastruct%' THEN 'infrastructure'
            WHEN LOWER(g.funding_program) LIKE '%idex%' THEN 'infrastructure'
            WHEN LOWER(g.funding_program) LIKE '%formation%' THEN 'training'
            WHEN LOWER(g.funding_program) LIKE '%astrid%' THEN 'research'
            WHEN LOWER(g.funding_program) LIKE '%blanc%' THEN 'research'
            WHEN LOWER(g.funding_program) LIKE '%aapg%' THEN 'research'
            ELSE 'grant'
        END as funding_type,

        g.funding_program as funder_scheme,

        'anr_opendata' as provenance,

        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        CAST(NULL AS DATE) as end_date,  -- ANR data doesn't include end dates
        COALESCE(
            YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')),
            TRY_CAST(g.edition_year AS INT)
        ) as start_year,
        CAST(NULL AS INT) as end_year,

        CASE
            WHEN g.pi_family_name IS NOT NULL THEN
                struct(
                    g.pi_given_name as given_name,
                    g.pi_family_name as family_name,
                    CASE 
                        WHEN g.pi_orcid IS NOT NULL AND g.pi_orcid != '' THEN
                            CASE 
                                WHEN g.pi_orcid LIKE 'https://orcid.org/%' THEN g.pi_orcid
                                ELSE CONCAT('https://orcid.org/', g.pi_orcid)
                            END
                        ELSE NULL
                    END as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.institution as name,
                        COALESCE(g.country, 'France') as country,
                        CASE 
                            WHEN g.rnsr_code IS NOT NULL THEN
                                array(struct(
                                    g.rnsr_code as id,
                                    'rnsr' as type,
                                    'anr' as asserted_by
                                ))
                            ELSE CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
                        END as ids
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

        CONCAT('https://anr.fr/Projet-', g.project_id) as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM anr_opendata_input g
    CROSS JOIN anr_funder f
    WHERE g.project_id IS NOT NULL
      AND TRIM(g.project_id) != ''
)
