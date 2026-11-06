-- Mapping from CreateSNSFAwards.ipynb (origin e22fdf7f): its awards-table SELECT over snsf_input (was the
-- staging table read from s3://openalex-ingest/awards/snsf/snsf_projects.parquet); created_date and updated_date are the run timestamp.
WITH
snsf_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320320924  -- Schweizerischer Nationalfonds
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.grant_number)))) % 9000000000 as id,

        COALESCE(g.title_english, g.title) as display_name,

        COALESCE(g.lay_summary_en, g.abstract) as description,

        f.funder_id,
        g.grant_number as funder_award_id,

        TRY_CAST(g.amount AS DOUBLE) as amount,
        'CHF' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(g.funding_instrument_level1) LIKE '%careers%' THEN 'fellowship'
            WHEN LOWER(g.funding_instrument) LIKE '%fellowship%' THEN 'fellowship'
            WHEN LOWER(g.funding_instrument) LIKE '%professorship%' THEN 'fellowship'
            WHEN LOWER(g.funding_instrument) LIKE '%postdoc%' THEN 'fellowship'
            WHEN LOWER(g.funding_instrument) LIKE '%doc.ch%' THEN 'fellowship'
            WHEN LOWER(g.funding_instrument) LIKE '%ambizione%' THEN 'fellowship'
            WHEN LOWER(g.funding_instrument) LIKE '%prima%' THEN 'fellowship'
            WHEN LOWER(g.funding_instrument) LIKE '%eccellenza%' THEN 'fellowship'
            WHEN LOWER(g.funding_instrument_level1) LIKE '%infrastructure%' THEN 'infrastructure'
            WHEN LOWER(g.funding_instrument_level1) LIKE '%programmes%' THEN 'research'
            WHEN LOWER(g.funding_instrument_level1) LIKE '%project funding%' THEN 'research'
            WHEN LOWER(g.funding_instrument) LIKE '%nccr%' THEN 'research'
            WHEN LOWER(g.funding_instrument) LIKE '%sinergia%' THEN 'research'
            WHEN LOWER(g.funding_instrument) LIKE '%project%' THEN 'research'
            ELSE 'grant'
        END as funding_type,

        g.funding_instrument as funder_scheme,

        'snsf' as provenance,

        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        CASE
            WHEN g.responsible_applicant IS NOT NULL AND TRIM(g.responsible_applicant) != '' THEN
                struct(
                    CASE 
                        WHEN INSTR(TRIM(g.responsible_applicant), ' ') > 0 
                        THEN TRIM(SUBSTRING(TRIM(g.responsible_applicant), 1, LENGTH(TRIM(g.responsible_applicant)) - INSTR(REVERSE(TRIM(g.responsible_applicant)), ' ')))
                        ELSE NULL
                    END as given_name,
                    CASE 
                        WHEN INSTR(TRIM(g.responsible_applicant), ' ') > 0 
                        THEN TRIM(SUBSTRING(TRIM(g.responsible_applicant), LENGTH(TRIM(g.responsible_applicant)) - INSTR(REVERSE(TRIM(g.responsible_applicant)), ' ') + 2))
                        ELSE g.responsible_applicant
                    END as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        -- For grants abroad or outside SNSF's lists, research_institution is a category ('Institution abroad - ...',
                        -- 'Unassignable - NA', 'Non-profit organisations ... - NPO') and the host is in institute.
                        COALESCE(CASE WHEN g.research_institution LIKE 'Institution abroad%' OR g.research_institution LIKE 'Unassignable%'
                                           OR g.research_institution LIKE 'Non-profit organisations%' THEN NULLIF(TRIM(g.institute), '') END,
                                 g.research_institution, g.institute) as name,
                        COALESCE(g.institute_country, 'Switzerland') as country,
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

        CONCAT('https://data.snf.ch/grants/grant/', g.grant_number) as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.grant_number)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM snsf_input g
    CROSS JOIN snsf_funder f
    WHERE g.grant_number IS NOT NULL
      AND TRIM(g.grant_number) != ''
)
