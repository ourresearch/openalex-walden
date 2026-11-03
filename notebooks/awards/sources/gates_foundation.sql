-- Legacy mapping from CreateGatesAwards.ipynb, origin c3db0a32.
-- _change is runner metadata, never a raw column. Compare all 22 other columns null-safely.
WITH
-- Get Gates Foundation funder from OpenAlex by explicit funder_id
gates_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320306137  -- Bill and Melinda Gates Foundation
),

awards_transformed AS (
    SELECT
        -- Generate unique ID: xxhash64 of funder_id:grant_id
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.grant_id)))) % 9000000000 as id,

        -- Display name = grantee_name + purpose (truncated)
        -- Gates grants don't have titles, so we use grantee + purpose summary
        CONCAT(
            g.grantee_name,
            ' - ',
            CASE
                WHEN LENGTH(g.purpose) > 150 THEN CONCAT(SUBSTRING(g.purpose, 1, 147), '...')
                ELSE g.purpose
            END
        ) as display_name,

        -- Description = full purpose statement
        g.purpose as description,

        -- Funder info
        f.funder_id,
        g.grant_id as funder_award_id,

        -- Amount in USD
        CAST(g.amount AS DOUBLE) as amount,
        'USD' as currency,

        -- Funder struct
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        -- Funding type - Gates Foundation primarily makes grants
        -- Map divisions/topics to funding types where possible
        CASE
            WHEN LOWER(g.topic) LIKE '%fellowship%' THEN 'fellowship'
            WHEN LOWER(g.topic) LIKE '%scholarship%' THEN 'fellowship'
            WHEN LOWER(g.topic) LIKE '%training%' THEN 'training'
            WHEN LOWER(g.topic) LIKE '%research%' THEN 'research'
            WHEN LOWER(g.topic) LIKE '%discovery%' THEN 'research'
            WHEN LOWER(g.topic) LIKE '%translational%' THEN 'research'
            WHEN LOWER(g.division) LIKE '%global health%' THEN 'research'
            WHEN LOWER(g.topic) LIKE '%education%' THEN 'training'
            WHEN LOWER(g.topic) LIKE '%infrastructure%' THEN 'infrastructure'
            ELSE 'grant'
        END as funding_type,

        -- Funder scheme = division (e.g., "Global Health", "Global Development")
        g.division as funder_scheme,

        -- Provenance
        'gates_foundation' as provenance,

        -- Dates (stored as strings in YYYY-MM-DD format)
        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        -- Lead investigator - Gates grants go to organizations, not individuals
        -- We store the grantee organization in the affiliation field
        CASE
            WHEN g.grantee_name IS NOT NULL THEN
                struct(
                    CAST(NULL AS STRING) as given_name,
                    CAST(NULL AS STRING) as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.grantee_name as name,
                        g.grantee_country as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            ELSE NULL
        END as lead_investigator,

        -- Co-lead and other investigators (not available)
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

        -- Landing page URL - Gates committed grants page
        CONCAT('https://www.gatesfoundation.org/about/committed-grants?q=', g.grant_id) as landing_page_url,

        -- No DOI for Gates grants
        CAST(NULL AS STRING) as doi,

        -- Works API URL
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.grant_id)))) % 9000000000) as works_api_url,

        -- Timestamps
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM gates_foundation_input g
    CROSS JOIN gates_funder f
    WHERE g.grant_id IS NOT NULL
      AND TRIM(g.grant_id) != ''
)
