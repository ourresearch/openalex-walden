-- Mapping from CreateHunanNSFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over hunan_nsf_input (was the
-- staging table read from s3://openalex-ingest/awards/hunan_nsf/hunan_nsf_projects.parquet); created_date and updated_date are the run timestamp.
WITH
src_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320322843
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(
            f.funder_id, ':',
            COALESCE(
                NULLIF(LOWER(TRIM(g.funder_award_id)), ''),
                CONCAT(LOWER(TRIM(g.display_name)), '|', LOWER(TRIM(COALESCE(g.institution, ''))))
            )
        ))) % 9000000000 as id,

        g.display_name as display_name,
        CAST(NULL AS STRING) as description,

        f.funder_id,
        NULLIF(TRIM(g.funder_award_id), '') as funder_award_id,

        CAST(NULL AS DOUBLE) as amount,
        CAST(NULL AS STRING) as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN g.funder_scheme LIKE '%杰出青年%' THEN 'fellowship'
            WHEN g.funder_scheme LIKE '%优秀青年%' THEN 'fellowship'
            WHEN g.funder_scheme LIKE '%青年%' THEN 'fellowship'
            WHEN g.funder_scheme LIKE '%博士%' THEN 'fellowship'
            WHEN g.funder_scheme LIKE '%启明星%' THEN 'fellowship'
            WHEN g.funder_scheme LIKE '%扬帆%' THEN 'fellowship'
            WHEN g.funder_scheme LIKE '%重大%' THEN 'research'
            WHEN g.funder_scheme LIKE '%重点%' THEN 'research'
            WHEN g.funder_scheme LIKE '%联合基金%' THEN 'research'
            ELSE 'grant'
        END as funding_type,

        NULLIF(TRIM(g.funder_scheme), '') as funder_scheme,

        'hunan_nsf' as provenance,

        CASE WHEN TRY_CAST(g.start_year AS INT) IS NOT NULL
             THEN TRY_TO_DATE(CONCAT(g.start_year, '-01-01'), 'yyyy-MM-dd')
             ELSE NULL END as start_date,
        CAST(NULL AS DATE) as end_date,
        TRY_CAST(g.start_year AS INT) as start_year,
        CAST(NULL AS INT) as end_year,

        CASE
            WHEN (g.lead_family_name IS NOT NULL AND TRIM(g.lead_family_name) != '')
              OR (g.institution IS NOT NULL AND TRIM(g.institution) != '') THEN
                struct(
                    CAST(NULL AS STRING) as given_name,
                    NULLIF(TRIM(g.lead_family_name), '') as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        NULLIF(TRIM(g.institution), '') as name,
                        'China' as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            ELSE NULL
        END as lead_investigator,

        CAST(NULL AS STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as co_lead_investigator,
        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) as investigators,

        g.landing_page_url as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G',
               abs(xxhash64(CONCAT(
            f.funder_id, ':',
            COALESCE(
                NULLIF(LOWER(TRIM(g.funder_award_id)), ''),
                CONCAT(LOWER(TRIM(g.display_name)), '|', LOWER(TRIM(COALESCE(g.institution, ''))))
            )
        ))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM hunan_nsf_input g
    CROSS JOIN src_funder f
    WHERE g.display_name IS NOT NULL
      AND TRIM(g.display_name) != ''
)
