-- Mapping from CreateGulbenkianAwards.ipynb (origin e22fdf7f): its awards-table SELECT over gulbenkian_projects_input (was the
-- staging table read from s3://openalex-ingest/awards/gulbenkian/gulbenkian_projects.parquet); created_date and updated_date are the run timestamp.
WITH
gulbenkian_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320323335
),

raw_prepared AS (
    SELECT
        *,
        TRY_CAST(amount AS DOUBLE) AS parsed_amount,
        TRY_CAST(start_year AS INT) AS parsed_start_year,
        TRY_CAST(end_year AS INT) AS parsed_end_year,
        TRY_TO_DATE(CONCAT(start_year, '-01-01'), 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(CONCAT(end_year, '-12-31'), 'yyyy-MM-dd') AS parsed_end_date
    FROM gulbenkian_projects_input
    WHERE title IS NOT NULL
      AND TRIM(title) != ''
      AND funder_award_id IS NOT NULL
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000 as id,

        g.title as display_name,
        g.description as description,

        f.funder_id,
        g.funder_award_id,

        g.parsed_amount as amount,
        g.currency as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        'research' as funding_type,
        'Gulbenkian Project' as funder_scheme,

        'gulbenkian_projects' as provenance,

        g.parsed_start_date as start_date,
        g.parsed_end_date as end_date,
        g.parsed_start_year as start_year,
        g.parsed_end_year as end_year,

        CASE
            WHEN g.partners IS NULL OR TRIM(g.partners) = '' THEN
                CAST(NULL AS STRUCT<
                    given_name:STRING,
                    family_name:STRING,
                    orcid:STRING,
                    role_start:DATE,
                    affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
                >)
            ELSE struct(
                CAST(NULL AS STRING) as given_name,
                CAST(NULL AS STRING) as family_name,
                CAST(NULL AS STRING) as orcid,
                g.parsed_start_date as role_start,
                struct(
                    g.partners as name,
                    'PT' as country,  -- most Gulbenkian projects are Portugal-based
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                ) as affiliation
            )
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

        g.link as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.funder_award_id)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM raw_prepared g
    CROSS JOIN gulbenkian_funder f
)
