-- Reference mapping template only. The executable SQL is inline in CreateOrUpdateAwards.sql.
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
    FROM parquet.`{{file_path}}` g
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
        CAST('{{run_timestamp}}' AS TIMESTAMP) as created_date,
        CAST('{{run_timestamp}}' AS TIMESTAMP) as updated_date

    FROM scoped g
    JOIN funders f ON f.funder_id = g.target_funder_id
),
incoming AS (
    SELECT CAST(id AS bigint) AS id,
        CAST(display_name AS string) AS display_name,
        CAST(description AS string) AS description,
        CAST(funder_id AS bigint) AS funder_id,
        CAST(funder_award_id AS string) AS funder_award_id,
        CAST(amount AS double) AS amount,
        CAST(currency AS string) AS currency,
        CAST(funder AS struct<id:string,display_name:string,ror_id:string,doi:string>) AS funder,
        CAST(funding_type AS string) AS funding_type,
        CAST(funder_scheme AS string) AS funder_scheme,
        CAST(provenance AS string) AS provenance,
        CAST(start_date AS date) AS start_date,
        CAST(end_date AS date) AS end_date,
        CAST(start_year AS bigint) AS start_year,
        CAST(end_year AS bigint) AS end_year,
        CAST(lead_investigator AS struct<given_name:string,family_name:string,orcid:string,role_start:date,affiliation:struct<name:string,country:string,ids:array<struct<id:string,type:string,asserted_by:string>>>>) AS lead_investigator,
        CAST(co_lead_investigator AS struct<given_name:string,family_name:string,orcid:string,role_start:date,affiliation:struct<name:string,country:string,ids:array<struct<id:string,type:string,asserted_by:string>>>>) AS co_lead_investigator,
        CAST(investigators AS array<struct<given_name:string,family_name:string,orcid:string,role_start:date,affiliation:struct<name:string,country:string,ids:array<struct<id:string,type:string,asserted_by:string>>>>>) AS investigators,
        CAST(landing_page_url AS string) AS landing_page_url,
        CAST(doi AS string) AS doi,
        CAST(works_api_url AS string) AS works_api_url,
        CAST(created_date AS timestamp) AS created_date,
        CAST(updated_date AS timestamp) AS updated_date,
        27 AS priority
    FROM awards_transformed
),
baseline AS (
    SELECT * FROM {{raw_table}} VERSION AS OF {{raw_version}}
    WHERE provenance = 'cordis' AND priority = 27
),
-- Compare trailing-space-normalized payload only; never normalize emitted baseline values.
-- Existing funder structs are frozen independently of other payload changes.
compared AS (
    SELECT i.*, struct(b.*) AS baseline_row,
        b.funder_id IS NOT NULL AS _exists,
        (i.id <=> b.id)
        AND (rtrim(i.display_name) <=> rtrim(b.display_name))
        AND (rtrim(i.description) <=> rtrim(b.description))
        AND (i.funder_id <=> b.funder_id)
        AND (rtrim(i.funder_award_id) <=> rtrim(b.funder_award_id))
        AND (i.amount <=> b.amount)
        AND (rtrim(i.currency) <=> rtrim(b.currency))
        AND (rtrim(i.funding_type) <=> rtrim(b.funding_type))
        AND (rtrim(i.funder_scheme) <=> rtrim(b.funder_scheme))
        AND (rtrim(i.provenance) <=> rtrim(b.provenance))
        AND (i.start_date <=> b.start_date)
        AND (i.end_date <=> b.end_date)
        AND (i.start_year <=> b.start_year)
        AND (i.end_year <=> b.end_year)
        AND (IF(i.lead_investigator IS NULL, NULL, named_struct('given_name', rtrim(i.lead_investigator.given_name), 'family_name', rtrim(i.lead_investigator.family_name), 'orcid', rtrim(i.lead_investigator.orcid), 'role_start', i.lead_investigator.role_start, 'affiliation', IF(i.lead_investigator.affiliation IS NULL, NULL, named_struct('name', rtrim(i.lead_investigator.affiliation.name), 'country', rtrim(i.lead_investigator.affiliation.country), 'ids', transform(i.lead_investigator.affiliation.ids, x2 -> IF(x2 IS NULL, NULL, named_struct('id', rtrim(x2.id), 'type', rtrim(x2.type), 'asserted_by', rtrim(x2.asserted_by)))))))) <=> IF(b.lead_investigator IS NULL, NULL, named_struct('given_name', rtrim(b.lead_investigator.given_name), 'family_name', rtrim(b.lead_investigator.family_name), 'orcid', rtrim(b.lead_investigator.orcid), 'role_start', b.lead_investigator.role_start, 'affiliation', IF(b.lead_investigator.affiliation IS NULL, NULL, named_struct('name', rtrim(b.lead_investigator.affiliation.name), 'country', rtrim(b.lead_investigator.affiliation.country), 'ids', transform(b.lead_investigator.affiliation.ids, x2 -> IF(x2 IS NULL, NULL, named_struct('id', rtrim(x2.id), 'type', rtrim(x2.type), 'asserted_by', rtrim(x2.asserted_by)))))))))
        AND (IF(i.co_lead_investigator IS NULL, NULL, named_struct('given_name', rtrim(i.co_lead_investigator.given_name), 'family_name', rtrim(i.co_lead_investigator.family_name), 'orcid', rtrim(i.co_lead_investigator.orcid), 'role_start', i.co_lead_investigator.role_start, 'affiliation', IF(i.co_lead_investigator.affiliation IS NULL, NULL, named_struct('name', rtrim(i.co_lead_investigator.affiliation.name), 'country', rtrim(i.co_lead_investigator.affiliation.country), 'ids', transform(i.co_lead_investigator.affiliation.ids, x2 -> IF(x2 IS NULL, NULL, named_struct('id', rtrim(x2.id), 'type', rtrim(x2.type), 'asserted_by', rtrim(x2.asserted_by)))))))) <=> IF(b.co_lead_investigator IS NULL, NULL, named_struct('given_name', rtrim(b.co_lead_investigator.given_name), 'family_name', rtrim(b.co_lead_investigator.family_name), 'orcid', rtrim(b.co_lead_investigator.orcid), 'role_start', b.co_lead_investigator.role_start, 'affiliation', IF(b.co_lead_investigator.affiliation IS NULL, NULL, named_struct('name', rtrim(b.co_lead_investigator.affiliation.name), 'country', rtrim(b.co_lead_investigator.affiliation.country), 'ids', transform(b.co_lead_investigator.affiliation.ids, x2 -> IF(x2 IS NULL, NULL, named_struct('id', rtrim(x2.id), 'type', rtrim(x2.type), 'asserted_by', rtrim(x2.asserted_by)))))))))
        AND (transform(i.investigators, x0 -> IF(x0 IS NULL, NULL, named_struct('given_name', rtrim(x0.given_name), 'family_name', rtrim(x0.family_name), 'orcid', rtrim(x0.orcid), 'role_start', x0.role_start, 'affiliation', IF(x0.affiliation IS NULL, NULL, named_struct('name', rtrim(x0.affiliation.name), 'country', rtrim(x0.affiliation.country), 'ids', transform(x0.affiliation.ids, x3 -> IF(x3 IS NULL, NULL, named_struct('id', rtrim(x3.id), 'type', rtrim(x3.type), 'asserted_by', rtrim(x3.asserted_by))))))))) <=> transform(b.investigators, x0 -> IF(x0 IS NULL, NULL, named_struct('given_name', rtrim(x0.given_name), 'family_name', rtrim(x0.family_name), 'orcid', rtrim(x0.orcid), 'role_start', x0.role_start, 'affiliation', IF(x0.affiliation IS NULL, NULL, named_struct('name', rtrim(x0.affiliation.name), 'country', rtrim(x0.affiliation.country), 'ids', transform(x0.affiliation.ids, x3 -> IF(x3 IS NULL, NULL, named_struct('id', rtrim(x3.id), 'type', rtrim(x3.type), 'asserted_by', rtrim(x3.asserted_by))))))))))
        AND (rtrim(i.landing_page_url) <=> rtrim(b.landing_page_url))
        AND (rtrim(i.doi) <=> rtrim(b.doi))
        AND (rtrim(i.works_api_url) <=> rtrim(b.works_api_url))
        AND (i.priority <=> b.priority) AS _unchanged
    FROM incoming i LEFT JOIN baseline b
      ON i.funder_id <=> b.funder_id
     AND lower(i.funder_award_id) <=> lower(b.funder_award_id)
)
SELECT
    CASE WHEN _exists AND _unchanged THEN baseline_row.id ELSE id END AS id,
    CASE WHEN _exists AND _unchanged THEN baseline_row.display_name ELSE display_name END AS display_name,
    CASE WHEN _exists AND _unchanged THEN baseline_row.description ELSE description END AS description,
    CASE WHEN _exists AND _unchanged THEN baseline_row.funder_id ELSE funder_id END AS funder_id,
    CASE WHEN _exists AND _unchanged THEN baseline_row.funder_award_id ELSE funder_award_id END AS funder_award_id,
    CASE WHEN _exists AND _unchanged THEN baseline_row.amount ELSE amount END AS amount,
    CASE WHEN _exists AND _unchanged THEN baseline_row.currency ELSE currency END AS currency,
    CASE WHEN _exists THEN baseline_row.funder ELSE funder END AS funder,
    CASE WHEN _exists AND _unchanged THEN baseline_row.funding_type ELSE funding_type END AS funding_type,
    CASE WHEN _exists AND _unchanged THEN baseline_row.funder_scheme ELSE funder_scheme END AS funder_scheme,
    CASE WHEN _exists AND _unchanged THEN baseline_row.provenance ELSE provenance END AS provenance,
    CASE WHEN _exists AND _unchanged THEN baseline_row.start_date ELSE start_date END AS start_date,
    CASE WHEN _exists AND _unchanged THEN baseline_row.end_date ELSE end_date END AS end_date,
    CASE WHEN _exists AND _unchanged THEN baseline_row.start_year ELSE start_year END AS start_year,
    CASE WHEN _exists AND _unchanged THEN baseline_row.end_year ELSE end_year END AS end_year,
    CASE WHEN _exists AND _unchanged THEN baseline_row.lead_investigator ELSE lead_investigator END AS lead_investigator,
    CASE WHEN _exists AND _unchanged THEN baseline_row.co_lead_investigator ELSE co_lead_investigator END AS co_lead_investigator,
    CASE WHEN _exists AND _unchanged THEN baseline_row.investigators ELSE investigators END AS investigators,
    CASE WHEN _exists AND _unchanged THEN baseline_row.landing_page_url ELSE landing_page_url END AS landing_page_url,
    CASE WHEN _exists AND _unchanged THEN baseline_row.doi ELSE doi END AS doi,
    CASE WHEN _exists AND _unchanged THEN baseline_row.works_api_url ELSE works_api_url END AS works_api_url,
    CASE WHEN _exists THEN baseline_row.created_date ELSE created_date END AS created_date,
    CASE WHEN _exists AND _unchanged THEN baseline_row.updated_date ELSE updated_date END AS updated_date,
    CASE WHEN _exists AND _unchanged THEN baseline_row.priority ELSE priority END AS priority,
    CASE WHEN NOT _exists THEN 'new' WHEN _unchanged THEN 'unchanged' ELSE 'changed' END AS _change
FROM compared
UNION ALL
SELECT b.id, b.display_name, b.description, b.funder_id, b.funder_award_id, b.amount, b.currency, b.funder, b.funding_type, b.funder_scheme, b.provenance, b.start_date, b.end_date, b.start_year, b.end_year, b.lead_investigator, b.co_lead_investigator, b.investigators, b.landing_page_url, b.doi, b.works_api_url, b.created_date, b.updated_date, b.priority, 'retained' AS _change
FROM baseline b LEFT ANTI JOIN incoming i
  ON i.funder_id <=> b.funder_id
 AND lower(i.funder_award_id) <=> lower(b.funder_award_id)
