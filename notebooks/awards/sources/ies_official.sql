-- Mapping from CreateIESAwards.ipynb (origin e22fdf7f): its awards-table SELECT over ies_official_input (was the
-- staging table read from s3://openalex-ingest/awards/ies/ies_awards.parquet); created_date and updated_date are the run timestamp.
WITH
ies_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320332210  -- Institute of Education Sciences
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.mid))))) % 9000000000 as id,

        NULLIF(TRIM(g.mtitle), '') as display_name,

        COALESCE(NULLIF(TRIM(g.mdescriptionshort), ''), NULLIF(TRIM(g.mtitle), '')) as description,

        f.funder_id,
        TRIM(g.mid) as funder_award_id,

        TRY_CAST(g.lawardamount AS DOUBLE) as amount,
        'USD' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(TRIM(g.mcontenttype)) = 'contract' THEN 'contract'
            WHEN LOWER(TRIM(g.mcontenttype)) = 'cooperative agreement' THEN 'cooperative_agreement'
            WHEN LOWER(TRIM(g.mcontenttype)) = 'grant' THEN 'grant'
            ELSE 'grant'
        END as funding_type,

        NULLIF(TRIM(g.lprogram), '') as funder_scheme,

        'ies_official' as provenance,

        TRY_TO_DATE(SUBSTR(g.mdateprimary, 1, 10), 'yyyy-MM-dd') as start_date,
        CAST(NULL AS DATE) as end_date,
        YEAR(TRY_TO_DATE(SUBSTR(g.mdateprimary, 1, 10), 'yyyy-MM-dd')) as start_year,
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

        CASE
            WHEN g.landing_page_url RLIKE '^https?://' THEN g.landing_page_url
            WHEN g.murl IS NOT NULL AND TRIM(g.murl) != '' THEN CONCAT('https://ies.ed.gov', g.murl)
            ELSE NULL
        END as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(TRIM(g.mid))))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM ies_official_input g
    CROSS JOIN ies_funder f
    WHERE g.mid IS NOT NULL
      AND TRIM(g.mid) != ''
)
