-- Mapping from CreateWennerGrenAwards.ipynb (origin e22fdf7f): its awards-table SELECT over wenner_gren_grantees_input (was the
-- staging table read from s3://openalex-ingest/awards/wenner_gren/wenner_gren_grantees.parquet); created_date and updated_date are the run timestamp.
WITH
wg_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306550
),

raw_prepared AS (
    SELECT
        *,
        TRY_CAST(approve_year AS INT) AS parsed_year,
        TRY_TO_DATE(approve_date_iso, 'yyyy-MM-dd') AS parsed_start_date
    FROM wenner_gren_grantees_input
    WHERE grantee_name IS NOT NULL
      AND TRIM(grantee_name) != ''
      AND grant_number IS NOT NULL
      AND TRIM(grant_number) != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LOWER(REGEXP_REPLACE(TRIM(grant_number), '^Gr[.][ ]*', ''))
        ORDER BY slug
    ) = 1
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(REGEXP_REPLACE(TRIM(g.grant_number), '^Gr[.][ ]*', ''))))) % 9000000000 as id,

        g.grantee_name as display_name,

        g.project_title as description,

        f.funder_id,
        REGEXP_REPLACE(TRIM(g.grant_number), '^Gr[.][ ]*', '') as funder_award_id,  -- citable number ('WIF-293', '10742'), 'Gr. ' display prefix stripped

        CAST(NULL AS DOUBLE) as amount,    -- §6.7 waiver
        CAST(NULL AS STRING) as currency,  -- §6.7 waiver

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(COALESCE(g.grant_type, '')) RLIKE 'fellowship|scholarship|postdoc|dissertation|traineeship' THEN 'fellowship'
            WHEN LOWER(COALESCE(g.grant_type, '')) RLIKE 'conference|workshop|symposium' THEN 'travel'
            ELSE 'research'
        END as funding_type,

        COALESCE(NULLIF(TRIM(g.grant_type), ''), 'Wenner-Gren Foundation Grant') as funder_scheme,

        'wenner_gren_grantees' as provenance,

        g.parsed_start_date as start_date,
        CAST(NULL AS DATE) as end_date,
        g.parsed_year as start_year,
        CAST(NULL AS INT) as end_year,

        struct(
            NULLIF(TRIM(g.given_name), '') as given_name,
            NULLIF(TRIM(g.family_name), '') as family_name,
            CAST(NULL AS STRING) as orcid,
            g.parsed_start_date as role_start,
            struct(
                NULLIF(TRIM(g.affiliation), '') as name,
                CAST(NULL AS STRING) as country,  -- affiliation includes country prefix sometimes, not split out
                CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
            ) as affiliation
        ) as lead_investigator,

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

        g.landing_page_url as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(REGEXP_REPLACE(TRIM(g.grant_number), '^Gr[.][ ]*', ''))))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM raw_prepared g
    CROSS JOIN wg_funder f
)
