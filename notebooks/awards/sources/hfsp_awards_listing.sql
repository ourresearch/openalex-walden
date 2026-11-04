-- Mapping from CreateHFSPAwards.ipynb (origin e22fdf7f): its awards-table SELECT over hfsp_awards_listing_input (was the
-- staging table read from s3://openalex-ingest/awards/hfsp/hfsp_awards.parquet); created_date and updated_date are the run timestamp.
WITH
hfsp_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320338
),

raw_prepared AS (
    SELECT
        *,
        LOWER(TRIM(funder_award_id)) AS native_award_id,
        TRY_TO_DATE(start_date, 'yyyy-MM-dd') AS parsed_start_date,
        TRY_TO_DATE(end_date, 'yyyy-MM-dd') AS parsed_end_date,
        TRY_CAST(source_year AS INT) AS parsed_year,
        from_json(
            investigators_json,
            'ARRAY<STRUCT<person_name:STRING,given_name:STRING,family_name:STRING,nationality:STRING,role:STRING,institution:STRING,city:STRING,country:STRING>>'
        ) AS awardees
    FROM hfsp_awards_listing_input
    WHERE funder_award_id IS NOT NULL
      AND TRIM(funder_award_id) != ''
      AND display_name IS NOT NULL
      AND TRIM(display_name) != ''
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000 as id,

        TRIM(g.display_name) as display_name,

        CASE
            WHEN g.description IS NULL OR TRIM(g.description) = '' THEN NULL
            ELSE TRIM(g.description)
        END as description,

        f.funder_id,
        g.native_award_id as funder_award_id,

        CAST(NULL AS DOUBLE) as amount,
        CAST(NULL AS STRING) as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        COALESCE(NULLIF(TRIM(g.funding_type), ''), 'grant') as funding_type,

        COALESCE(NULLIF(TRIM(g.program), ''), 'Unlabeled HFSP row') as funder_scheme,

        'hfsp_awards_listing' as provenance,

        g.parsed_start_date as start_date,
        g.parsed_end_date as end_date,
        COALESCE(YEAR(g.parsed_start_date), g.parsed_year) as start_year,
        COALESCE(YEAR(g.parsed_end_date), g.parsed_year) as end_year,

        CASE
            WHEN g.lead_person_name IS NULL OR TRIM(g.lead_person_name) = '' THEN
                CAST(NULL AS STRUCT<
                    given_name:STRING,
                    family_name:STRING,
                    orcid:STRING,
                    role_start:DATE,
                    affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
                >)
            ELSE struct(
                NULLIF(TRIM(g.lead_given_name), '') as given_name,
                NULLIF(TRIM(g.lead_family_name), '') as family_name,
                CAST(NULL AS STRING) as orcid,
                g.parsed_start_date as role_start,
                struct(
                    NULLIF(TRIM(g.lead_institution), '') as name,
                    NULLIF(TRIM(g.lead_country), '') as country,
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                ) as affiliation
            )
        END as lead_investigator,

        CASE
            WHEN g.co_person_name IS NULL OR TRIM(g.co_person_name) = '' THEN
                CAST(NULL AS STRUCT<
                    given_name:STRING,
                    family_name:STRING,
                    orcid:STRING,
                    role_start:DATE,
                    affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
                >)
            ELSE struct(
                NULLIF(TRIM(g.co_given_name), '') as given_name,
                NULLIF(TRIM(g.co_family_name), '') as family_name,
                CAST(NULL AS STRING) as orcid,
                g.parsed_start_date as role_start,
                struct(
                    NULLIF(TRIM(g.co_institution), '') as name,
                    NULLIF(TRIM(g.co_country), '') as country,
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                ) as affiliation
            )
        END as co_lead_investigator,

        CASE
            WHEN g.awardees IS NULL OR size(g.awardees) = 0 THEN
                CAST(NULL AS ARRAY<STRUCT<
                    given_name:STRING,
                    family_name:STRING,
                    orcid:STRING,
                    role_start:DATE,
                    affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
                >>)
            ELSE TRANSFORM(
                g.awardees,
                x -> struct(
                    NULLIF(TRIM(x.given_name), '') as given_name,
                    NULLIF(TRIM(x.family_name), '') as family_name,
                    CAST(NULL AS STRING) as orcid,
                    g.parsed_start_date as role_start,
                    struct(
                        NULLIF(TRIM(x.institution), '') as name,
                        NULLIF(TRIM(x.country), '') as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            )
        END as investigators,

        NULLIF(TRIM(g.landing_page_url), '') as landing_page_url,
        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', g.native_award_id))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM raw_prepared g
    CROSS JOIN hfsp_funder f
)
