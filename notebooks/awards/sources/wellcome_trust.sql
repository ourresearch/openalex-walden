-- Legacy mapping from CreateWellcomeAwards.ipynb, origin c3db0a32.
-- _change is runner metadata, never a raw column. Compare all 22 other columns null-safely.
WITH
-- Get Wellcome Trust funder from OpenAlex by explicit funder_id
wellcome_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320311904  -- Wellcome Trust
),

-- Parse lead applicant name into given/family name
parsed_names AS (
    SELECT
        w.*,
        -- Lead applicant format varies: "Smith, John" or "John Smith"
        -- We have applicant_surname separately, so use that for family name
        w.applicant_surname as parsed_family_name,
        -- Extract given name by removing surname from full name
        CASE
            WHEN w.lead_applicant_name LIKE CONCAT(w.applicant_surname, ',%') THEN
                TRIM(SUBSTRING(w.lead_applicant_name, LENGTH(w.applicant_surname) + 2))
            WHEN w.lead_applicant_name LIKE CONCAT('% ', w.applicant_surname) THEN
                TRIM(SUBSTRING(w.lead_applicant_name, 1, LENGTH(w.lead_applicant_name) - LENGTH(w.applicant_surname) - 1))
            ELSE
                -- If pattern doesn't match, try to extract first name from lead_applicant_name
                CASE
                    WHEN INSTR(w.lead_applicant_name, ',') > 0 THEN
                        TRIM(SUBSTRING(w.lead_applicant_name, INSTR(w.lead_applicant_name, ',') + 1))
                    WHEN INSTR(w.lead_applicant_name, ' ') > 0 THEN
                        TRIM(SUBSTRING(w.lead_applicant_name, 1, INSTR(w.lead_applicant_name, ' ') - 1))
                    ELSE NULL
                END
        END as parsed_given_name
    FROM wellcome_trust_input w
),

awards_transformed AS (
    SELECT
        -- Generate unique ID: xxhash64 of funder_id:grant_ref (citable reference)
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(w.grant_ref)))) % 9000000000 as id,

        -- Display name = title
        RTRIM(w.title) as display_name,

        -- Description
        w.description as description,

        -- Funder info
        f.funder_id,
        w.grant_ref as funder_award_id,

        -- Amount in GBP
        TRY_CAST(w.amount AS DOUBLE) as amount,
        COALESCE(w.currency, 'GBP') as currency,

        -- Funder struct
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        -- Funding type - map from grant programme
        CASE
            WHEN LOWER(w.grant_programme) LIKE '%phd%' THEN 'fellowship'
            WHEN LOWER(w.grant_programme) LIKE '%studentship%' THEN 'fellowship'
            WHEN LOWER(w.grant_programme) LIKE '%fellowship%' THEN 'fellowship'
            WHEN LOWER(w.grant_programme) LIKE '%scholarship%' THEN 'fellowship'
            WHEN LOWER(w.grant_programme) LIKE '%training%' THEN 'training'
            WHEN LOWER(w.grant_programme) LIKE '%career%' THEN 'fellowship'
            WHEN LOWER(w.grant_programme) LIKE '%investigator%' THEN 'research'
            WHEN LOWER(w.grant_programme) LIKE '%project%' THEN 'research'
            WHEN LOWER(w.grant_programme) LIKE '%strategic%' THEN 'research'
            WHEN LOWER(w.grant_programme) LIKE '%collaborative%' THEN 'research'
            WHEN LOWER(w.grant_programme) LIKE '%seed%' THEN 'research'
            WHEN LOWER(w.grant_programme) LIKE '%discovery%' THEN 'research'
            WHEN LOWER(w.grant_programme) LIKE '%equipment%' THEN 'infrastructure'
            WHEN LOWER(w.grant_programme) LIKE '%capital%' THEN 'infrastructure'
            WHEN LOWER(w.grant_programme) LIKE '%building%' THEN 'infrastructure'
            WHEN LOWER(w.grant_programme) LIKE '%open access%' THEN 'other'
            WHEN LOWER(w.grant_programme) LIKE '%vacation%' THEN 'fellowship'
            ELSE 'grant'
        END as funding_type,

        -- Funder scheme = grant programme
        w.grant_programme as funder_scheme,

        -- Provenance
        'wellcome_trust' as provenance,

        -- Dates
        TRY_TO_DATE(w.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(w.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(w.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(w.end_date, 'yyyy-MM-dd')) as end_year,

        -- Lead investigator
        CASE
            WHEN w.lead_applicant_name IS NOT NULL THEN
                struct(
                    w.parsed_given_name as given_name,
                    w.parsed_family_name as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        w.recipient_org_name as name,
                        w.recipient_orgcountry as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            ELSE NULL
        END as lead_investigator,

        -- Co-lead and other investigators (not available in this format)
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

        -- Landing page URL - Wellcome grants page
        CONCAT('https://wellcome.org/grant-funding/people-and-projects/grants-awarded?q=', w.grant_ref) as landing_page_url,

        -- No DOI for Wellcome grants
        CAST(NULL AS STRING) as doi,

        -- Works API URL
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(w.grant_ref)))) % 9000000000) as works_api_url,

        -- Timestamps
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM parsed_names w
    CROSS JOIN wellcome_funder f
    WHERE w.grant_ref IS NOT NULL
      AND TRIM(w.grant_ref) != ''
      -- guard: the citable ref must never carry the 360Giving prefix
      AND w.grant_ref NOT LIKE '360G-%'
)
