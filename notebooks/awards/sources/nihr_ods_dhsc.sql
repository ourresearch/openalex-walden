-- Mapping from CreateDHSCAwards.ipynb (origin e22fdf7f): its awards-table SELECT over nihr_ods_dhsc_input (was the
-- staging table read from s3://openalex-ingest/awards/nihr_ods_dhsc/nihr_ods_dhsc_projects.parquet); created_date and updated_date are the run timestamp.
WITH
dhsc_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320319994  -- Department of Health and Social Care
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000 as id,

        g.project_title as display_name,

        COALESCE(g.plain_english_abstract, g.scientific_abstract) as description,

        f.funder_id,
        g.project_id as funder_award_id,

        TRY_CAST(g.award_amount_from_dh AS DOUBLE) as amount,
        'GBP' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(COALESCE(g.project_title, '')) LIKE '%training award%' THEN 'training'
            WHEN LOWER(COALESCE(g.programme, '')) LIKE '%fellowship%' THEN 'fellowship'
            ELSE 'research'
        END as funding_type,

        COALESCE(g.programme, g.funding_stream) as funder_scheme,

        'nihr_ods_dhsc' as provenance,

        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        CASE
            WHEN g.lead_family_name IS NOT NULL THEN
                struct(
                    g.lead_given_name as given_name,
                    g.lead_family_name as family_name,
                    CASE
                        WHEN g.orcid IS NOT NULL AND g.orcid RLIKE '^[0-9]{4}-'
                            THEN CONCAT('https://orcid.org/', g.orcid)
                        ELSE g.orcid
                    END as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.contracted_organisation as name,
                        'GB' as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            WHEN g.contracted_organisation IS NOT NULL THEN
                struct(
                    CAST(NULL AS STRING) as given_name,
                    CAST(NULL AS STRING) as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.contracted_organisation as name,
                        'GB' as country,
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

        g.funding_and_awards_link as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM nihr_ods_dhsc_input g
    CROSS JOIN dhsc_funder f
    WHERE g.project_id IS NOT NULL
      AND TRIM(g.project_id) != ''
)
