-- Mapping from CreateZhejiangNSFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over zhejiang_nsf_input (was the
-- staging table read from s3://openalex-ingest/awards/zhejiang_nsf/zhejiang_nsf_projects.parquet); created_date and updated_date are the run timestamp.
WITH
funder_src AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320338464
),
mapped_awards AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':',
            COALESCE(LOWER(NULLIF(TRIM(g.funder_award_id), '')), g.row_key)))) % 9000000000 as id,

        g.display_name as display_name,
        CAST(NULL AS STRING) as description,

        f.funder_id,
        NULLIF(TRIM(g.funder_award_id), '') as funder_award_id,

        CAST(NULL AS DOUBLE) as amount,   -- no per-project amounts published (Step 6.7 waiver)
        CAST(NULL AS STRING) as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name, f.ror_id, f.doi
        ) as funder,

        CASE
            WHEN g.funder_scheme LIKE '%杰出青年%' THEN 'fellowship'
            WHEN g.funder_scheme LIKE '%优秀青年%' THEN 'fellowship'
            WHEN g.funder_scheme LIKE '%青年%' THEN 'fellowship'
            WHEN g.funder_scheme LIKE '%博士%' THEN 'fellowship'
            WHEN g.funder_scheme LIKE '%团队%' THEN 'research'
            WHEN g.funder_scheme LIKE '%重大%' THEN 'research'
            WHEN g.funder_scheme LIKE '%重点%' THEN 'research'
            ELSE 'grant'
        END as funding_type,

        g.funder_scheme as funder_scheme,
        'zhejiang_nsf' as provenance,

        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        TRY_CAST(g.start_year AS INT) as start_year,
        TRY_CAST(g.end_year AS INT) as end_year,

        CASE
            WHEN (g.family_name IS NOT NULL AND TRIM(g.family_name) != '')
              OR (g.institution IS NOT NULL AND TRIM(g.institution) != '') THEN
                struct(
                    NULLIF(TRIM(g.given_name), '') as given_name,
                    NULLIF(TRIM(g.family_name), '') as family_name,
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

        CAST(NULL AS STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>) as co_lead_investigator,
        CAST(NULL AS ARRAY<STRUCT<given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE, affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>>>) as investigators,

        g.landing_page_url as landing_page_url,
        CAST(NULL AS STRING) as doi,
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date
    FROM zhejiang_nsf_input g
    CROSS JOIN funder_src f
),

awards_transformed AS (
SELECT *,
    concat('https://api.openalex.org/works?filter=awards.id:G', id) as works_api_url
FROM mapped_awards
)
