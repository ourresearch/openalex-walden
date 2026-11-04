-- Mapping from CreateVillumAwards.ipynb (origin e22fdf7f): its awards-table SELECT over villum_veluxfonden_input (was the
-- staging table read from s3://openalex-ingest/awards/villum/villum_projects.parquet); created_date and updated_date are the run timestamp.
WITH
villum_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320310490  -- Villum Fonden
),
awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,
        g.title as display_name,
        CAST(NULL AS STRING) as description,
        f.funder_id,
        g.funder_award_id as funder_award_id,
        CASE WHEN TRY_CAST(g.amount AS DOUBLE) > 0 THEN TRY_CAST(g.amount AS DOUBLE) ELSE NULL END as amount,
        CASE WHEN TRY_CAST(g.amount AS DOUBLE) > 0 THEN 'DKK' ELSE NULL END as currency,
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name, f.ror_id, f.doi
        ) as funder,
        CASE
            WHEN LOWER(g.funder_scheme) RLIKE '(visiting professor|young invest|kavli|postdoc|fellowship|talent|phd|ph\\.d|stipend)' THEN 'fellowship'
            WHEN LOWER(g.funder_area) RLIKE '(technical|scientific|research)' THEN 'research'
            ELSE 'grant'
        END as funding_type,
        g.funder_scheme as funder_scheme,
        'villum_veluxfonden' as provenance,
        CASE WHEN TRY_CAST(g.year AS INT) IS NOT NULL
             THEN CAST(CONCAT(g.year, '-01-01') AS DATE) ELSE NULL END as start_date,
        CAST(NULL AS DATE) as end_date,
        TRY_CAST(g.year AS INT) as start_year,
        CAST(NULL AS INT) as end_year,
        CASE
            WHEN g.pi_family_name IS NOT NULL OR g.institution_name IS NOT NULL THEN
                struct(
                    g.pi_given_name as given_name,
                    g.pi_family_name as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.institution_name as name,
                        g.institution_country as country,
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
        COALESCE(g.landing_url, 'https://veluxfonden.dk/en/basic-page/projects-granted') as landing_page_url,
        CAST(NULL AS STRING) as doi,
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000) as works_api_url,
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date
    FROM villum_veluxfonden_input g
    CROSS JOIN villum_funder f
    WHERE g.funder_award_id IS NOT NULL
      AND TRIM(CAST(g.funder_award_id AS STRING)) != ''
)
