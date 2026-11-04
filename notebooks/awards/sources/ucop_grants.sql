-- Mapping from CreateUCOPAwards.ipynb (origin e22fdf7f): its awards-table SELECT over ucop_grants_input (was the
-- staging table read from s3://openalex-ingest/awards/ucop/ucop_awards.parquet); created_date and updated_date are the run timestamp.
WITH
ucop_funder AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320333677
),

raw_with_pis AS (
    SELECT
        g.*,
        from_json(
            g.contacts_json,
            'ARRAY<STRUCT<institution:STRING, lastname:STRING, firstname:STRING, email:STRING, degree:STRING, role:STRING, subject:STRING>>'
        ) AS pis,
        TRY_CAST(REGEXP_REPLACE(g.approvedamount, '[^0-9.-]', '') AS DOUBLE) AS parsed_amount,
        COALESCE(
            TRY_TO_DATE(g.startdate, 'yyyy-MM-dd'),
            TRY_TO_DATE(SUBSTR(g.startdate, 1, 10), 'yyyy-MM-dd')
        ) AS parsed_start_date,
        COALESCE(
            TRY_TO_DATE(g.enddate, 'yyyy-MM-dd'),
            TRY_TO_DATE(SUBSTR(g.enddate, 1, 10), 'yyyy-MM-dd')
        ) AS parsed_end_date
    FROM ucop_grants_input g
    WHERE g.applicationid IS NOT NULL
      AND TRIM(g.applicationid) != ''
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.applicationid)))) % 9000000000 as id,

        g.title as display_name,

        CASE
            WHEN g.abstract IS NOT NULL AND TRIM(g.abstract) != '' THEN g.abstract
            WHEN g.progressreportabbstract IS NOT NULL AND TRIM(g.progressreportabbstract) != '' THEN g.progressreportabbstract
            ELSE NULL
        END as description,

        f.funder_id,
        g.applicationid as funder_award_id,

        g.parsed_amount as amount,
        'USD' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(COALESCE(g.awardtype, '')) RLIKE 'training|traineeship|fellowship|scholarship|postdoc|dissertation|predoc' THEN 'fellowship'
            ELSE 'grant'
        END as funding_type,

        COALESCE(NULLIF(TRIM(g.awardtype), ''), NULLIF(TRIM(g.program), '')) as funder_scheme,

        'ucop_grants' as provenance,

        g.parsed_start_date as start_date,
        g.parsed_end_date as end_date,
        YEAR(g.parsed_start_date) as start_year,
        YEAR(g.parsed_end_date) as end_year,

        CASE
            WHEN g.pis IS NULL OR size(g.pis) = 0 THEN
                CAST(NULL AS STRUCT<
                    given_name:STRING,
                    family_name:STRING,
                    orcid:STRING,
                    role_start:DATE,
                    affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
                >)
            ELSE struct(
                NULLIF(TRIM(try_element_at(g.pis, 1).firstname), '') as given_name,
                NULLIF(TRIM(try_element_at(g.pis, 1).lastname), '') as family_name,
                CAST(NULL AS STRING) as orcid,
                g.parsed_start_date as role_start,
                struct(
                    NULLIF(TRIM(try_element_at(g.pis, 1).institution), '') as name,
                    CAST(NULL AS STRING) as country,
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                ) as affiliation
            )
        END as lead_investigator,

        CASE
            WHEN g.pis IS NULL OR size(g.pis) < 2 THEN
                CAST(NULL AS STRUCT<
                    given_name:STRING,
                    family_name:STRING,
                    orcid:STRING,
                    role_start:DATE,
                    affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
                >)
            ELSE struct(
                NULLIF(TRIM(try_element_at(g.pis, 2).firstname), '') as given_name,
                NULLIF(TRIM(try_element_at(g.pis, 2).lastname), '') as family_name,
                CAST(NULL AS STRING) as orcid,
                g.parsed_start_date as role_start,
                struct(
                    NULLIF(TRIM(try_element_at(g.pis, 2).institution), '') as name,
                    CAST(NULL AS STRING) as country,
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                ) as affiliation
            )
        END as co_lead_investigator,

        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING,
            family_name:STRING,
            orcid:STRING,
            role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) as investigators,

        CAST(NULL AS STRING) as landing_page_url,

        NULLIF(TRIM(g.grant_doi), '') as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.applicationid)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM raw_with_pis g
    CROSS JOIN ucop_funder f
)
