-- Legacy mapping from CreateEuropeanCommissionAwards.ipynb, origin c3db0a32.
-- _change is runner metadata, never a raw column. Compare all 22 other columns null-safely.
WITH
-- Resolve the three emitting funders by ID from the canonical table.
funders AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
    WHERE funder_id IN (4320320300, 4320338438, 4320338448)
),

-- Scope each CORDIS project and route it to its target funder.
--   * ERC projects are EXCLUDED: the ERC ingest (priority 21, funder
--     4320334678) is authoritative for them. The filter below mirrors the
--     ERC scraper's exact inclusion rule
--     (fundingScheme.upper().startswith(("ERC-","HORIZON-ERC"))), so there is
--     no gap or overlap. Without it the same CORDIS project landed under BOTH
--     funders -- different funder_id => different award id => it survived the
--     PARTITION BY id dedup in CreateAwards.ipynb -- double-counting ~17.5k
--     ERC grants (~EUR 30.8B).
--   * Horizon Europe MSCA / EIC projects are routed to their specific OpenAlex
--     sub-body funders. ONLY HORIZON-framework projects are split: OpenAlex has
--     no H2020/FP7 MSCA/EIC funders, so older Marie-Curie / EIC-precursor
--     projects stay under the generic EC funder rather than be mislabeled with
--     a "HORIZON EUROPE ..." funder name.
--   NOTE: the %MSCA% / %EIC% substring patterns were written without sight of
--   the raw funding_scheme distribution; verify coverage with the
--   "Step 2 split audit" cell below before trusting the re-attribution.
scoped AS (
    SELECT
        g.*,
        CASE
            WHEN g.framework_programme = 'HORIZON'
                 AND UPPER(g.funding_scheme) LIKE '%MSCA%' THEN 4320338438
            WHEN g.framework_programme = 'HORIZON'
                 AND UPPER(g.funding_scheme) LIKE '%EIC%' THEN 4320338448
            ELSE 4320320300
        END AS target_funder_id
    FROM cordis_input g
    WHERE g.cordis_project_id IS NOT NULL
      AND TRIM(g.cordis_project_id) != ''
      AND COALESCE(UPPER(g.funding_scheme), '') NOT LIKE 'ERC-%'
      AND COALESCE(UPPER(g.funding_scheme), '') NOT LIKE 'HORIZON-ERC%'
),

awards_transformed AS (
    SELECT
        -- Generate unique ID using xxhash64 of funder_id:project_id
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.cordis_project_id)))) % 9000000000 as id,

        -- Display name = project title
        g.title as display_name,

        -- Description = objective
        g.objective as description,

        -- Funder info
        f.funder_id,
        g.cordis_project_id as funder_award_id,

        -- Funding amount (EC max contribution)
        TRY_CAST(g.ec_max_contribution AS DOUBLE) as amount,
        'EUR' as currency,

        -- Funder struct
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        -- Funding type - map common EC schemes
        CASE
            -- Fellowships/training
            WHEN LOWER(g.funding_scheme) LIKE '%msca%' THEN 'fellowship'
            WHEN LOWER(g.funding_scheme) LIKE '%itn%' THEN 'training'
            WHEN LOWER(g.funding_scheme) LIKE '%mc-itn%' THEN 'training'
            WHEN LOWER(g.funding_scheme) LIKE '%if%' THEN 'fellowship'
            WHEN LOWER(g.funding_scheme) LIKE '%irf%' THEN 'fellowship'
            WHEN LOWER(g.funding_scheme) LIKE '%ief%' THEN 'fellowship'
            WHEN LOWER(g.funding_scheme) LIKE '%iof%' THEN 'fellowship'
            WHEN LOWER(g.funding_scheme) LIKE '%cofund%' THEN 'fellowship'
            -- ERC grants
            WHEN LOWER(g.funding_scheme) LIKE '%erc%' THEN 'research'
            WHEN LOWER(g.funding_scheme) LIKE 'horizon-erc%' THEN 'research'
            -- SME instruments
            WHEN LOWER(g.funding_scheme) LIKE '%sme%' THEN 'commercial'
            -- Research & Innovation Actions
            WHEN UPPER(g.funding_scheme) IN ('RIA', 'HORIZON-RIA') THEN 'research'
            WHEN UPPER(g.funding_scheme) IN ('IA', 'HORIZON-IA') THEN 'research'
            -- Coordination/Support
            WHEN UPPER(g.funding_scheme) IN ('CSA', 'HORIZON-CSA') THEN 'grant'
            -- Infrastructure
            WHEN LOWER(g.funding_scheme) LIKE '%infra%' THEN 'infrastructure'
            -- Collaborative projects
            WHEN UPPER(g.funding_scheme) IN ('CP', 'CP-FP', 'CP-IP') THEN 'research'
            ELSE 'grant'
        END as funding_type,

        -- Funder scheme = funding_scheme (the specific EC programme)
        CONCAT(g.framework_programme, ' - ', COALESCE(g.funding_scheme, 'Unknown')) as funder_scheme,

        -- Provenance
        'cordis' as provenance,

        -- Dates
        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        -- Lead investigator (coordinator organization - CORDIS doesn't provide PI names at project level)
        CASE
            WHEN g.coordinator_name IS NOT NULL THEN
                struct(
                    CAST(NULL AS STRING) as given_name,
                    CAST(NULL AS STRING) as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.coordinator_name as name,
                        g.coordinator_country as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            ELSE NULL
        END as lead_investigator,

        -- Co-lead and other investigators (not available at project level)
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

        -- Landing page URL (CORDIS project page)
        CONCAT('https://cordis.europa.eu/project/id/', g.cordis_project_id) as landing_page_url,

        -- DOI (grant DOI from CORDIS)
        g.grant_doi as doi,

        -- Works API URL
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.cordis_project_id)))) % 9000000000) as works_api_url,

        -- Timestamps
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM scoped g
    JOIN funders f ON f.funder_id = g.target_funder_id
)
