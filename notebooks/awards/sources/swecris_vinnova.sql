-- Mapping from CreateVinnovaAwards.ipynb (origin e22fdf7f): its awards-table SELECT over swecris_vinnova_input (was the
-- staging table read from s3://openalex-ingest/awards/vinnova/vinnova_projects.parquet); created_date and updated_date are the run timestamp.
WITH
vinnova_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320321030  -- VINNOVA
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(REGEXP_REPLACE(TRIM(g.projectid), '_[A-Za-z]+$', ''))))) % 9000000000 as id,

        COALESCE(g.projecttitleen, g.projecttitlesv) as display_name,

        COALESCE(g.projectabstracten, g.projectabstractsv) as description,

        f.funder_id,
        REGEXP_REPLACE(TRIM(g.projectid), '_[A-Za-z]+$', '') as funder_award_id,  -- bare diarienummer, '_Vinnova' suffix stripped

        TRY_CAST(g.fundingssek AS DOUBLE) as amount,
        'SEK' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(g.typeofawarddescren) LIKE '%grant%' THEN 'grant'
            ELSE 'grant'
        END as funding_type,

        g.typeofawarddescren as funder_scheme,

        'swecris_vinnova' as provenance,

        TRY_TO_DATE(g.projectstartdate) as start_date,
        TRY_TO_DATE(g.projectenddate) as end_date,
        YEAR(TRY_TO_DATE(g.projectstartdate)) as start_year,
        YEAR(TRY_TO_DATE(g.projectenddate)) as end_year,

        CASE
            WHEN g.pi_name IS NOT NULL AND TRIM(g.pi_name) != '' THEN
                struct(
                    CAST(NULL AS STRING) as given_name,
                    TRIM(g.pi_name) as family_name,  -- Store full name in family_name
                    CASE 
                        WHEN g.pi_orcid IS NOT NULL AND TRIM(g.pi_orcid) != '' 
                        THEN CONCAT('https://orcid.org/', TRIM(g.pi_orcid))
                        ELSE NULL 
                    END as orcid,
                    TRY_TO_DATE(g.projectstartdate) as role_start,
                    struct(
                        TRIM(g.coordinatingorganisationnameen) as name,
                        'Sweden' as country,
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

        'https://www.vr.se/english/swecris.html' as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(REGEXP_REPLACE(TRIM(g.projectid), '_[A-Za-z]+$', ''))))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM swecris_vinnova_input g
    CROSS JOIN vinnova_funder f
    WHERE g.projectid IS NOT NULL
      AND TRIM(g.projectid) != ''
)
