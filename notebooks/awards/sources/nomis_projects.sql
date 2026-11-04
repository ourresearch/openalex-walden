-- Mapping from CreateNomisAwards.ipynb (origin e22fdf7f): its awards-table SELECT over nomis_projects_input (was the
-- staging table read from s3://openalex-ingest/awards/nomis/nomis_projects.parquet); created_date and updated_date are the run timestamp.
WITH
nomis_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320325162
),

raw_prepared AS (
    SELECT
        *,
        TRY_CAST(award_year AS INT) AS parsed_year,
        TRY_TO_DATE(CONCAT(award_year, '-01-01'), 'yyyy-MM-dd') AS parsed_start_date
    FROM nomis_projects_input
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

        CAST(NULL AS DOUBLE) as amount,    -- §6.7 waiver
        CAST(NULL AS STRING) as currency,  -- §6.7 waiver

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        'research' as funding_type,
        'NOMIS Research Project' as funder_scheme,

        'nomis_projects' as provenance,

        g.parsed_start_date as start_date,
        CAST(NULL AS DATE) as end_date,
        g.parsed_year as start_year,
        CAST(NULL AS INT) as end_year,

        CAST(NULL AS STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as lead_investigator,

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
    CROSS JOIN nomis_funder f
)
