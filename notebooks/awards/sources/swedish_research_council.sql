-- Mapping from CreateSwedishResearchCouncilAwards.ipynb (origin e22fdf7f): its awards-table SELECT over swedish_research_council_input (was the
-- staging table read from s3://openalex-ingest/awards/swedish_research_council/swedish_research_council_projects.parquet); created_date and updated_date are the run timestamp.
WITH
vr_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320322581  -- Vetenskapsrådet (Swedish Research Council)
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(REGEXP_REPLACE(TRIM(g.project_id), '_[A-Za-z]+$', ''))))) % 9000000000 as id,

        COALESCE(g.title_english, g.title) as display_name,

        COALESCE(g.abstract_english, g.abstract) as description,

        f.funder_id,
        REGEXP_REPLACE(TRIM(g.project_id), '_[A-Za-z]+$', '') as funder_award_id,  -- bare diarienummer, SweCRIS suffix stripped (matches Energimyndigheten/IFAU/Ostersjostiftelsen/SweCRIS notebooks)

        TRY_CAST(g.amount AS DOUBLE) as amount,
        'SEK' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(g.type_of_award) LIKE '%positions%' THEN 'fellowship'
            WHEN LOWER(g.type_of_award) LIKE '%stipend%' THEN 'fellowship'
            WHEN LOWER(g.type_of_award) LIKE '%infrastructure%' THEN 'infrastructure'
            WHEN LOWER(g.type_of_award) LIKE '%environment%' THEN 'research'
            WHEN LOWER(g.type_of_award) LIKE '%project%' THEN 'research'
            WHEN LOWER(g.type_of_award) LIKE '%international%' THEN 'research'
            ELSE 'grant'
        END as funding_type,

        g.type_of_award as funder_scheme,

        'swedish_research_council' as provenance,

        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        CASE
            WHEN g.pi_family_name IS NOT NULL AND TRIM(g.pi_family_name) != '' THEN
                struct(
                    g.pi_given_name as given_name,
                    g.pi_family_name as family_name,
                    g.pi_orcid as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.coordinating_organisation as name,
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

        CONCAT('https://www.vr.se/swecris#/project/', g.project_id) as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(REGEXP_REPLACE(TRIM(g.project_id), '_[A-Za-z]+$', ''))))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM swedish_research_council_input g
    CROSS JOIN vr_funder f
    WHERE g.project_id IS NOT NULL
      AND TRIM(g.project_id) != ''
)
