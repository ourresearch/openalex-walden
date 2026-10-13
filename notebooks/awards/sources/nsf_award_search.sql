-- Reference mapping template only. The executable SQL is inline in CreateOrUpdateAwards.sql.
-- Legacy mapping from CreateNSFAwards.ipynb, origin c3db0a32.
-- _change is runner metadata, never a raw column. Compare all 22 other columns null-safely.
WITH nsf_funder AS (
    -- NSF funder from OpenAlex (update funder_id after running Step 2)
    SELECT
        f.funder_id,
        f.display_name,
        f.ror_id,
        f.doi
    FROM openalex.funders.funders f
    JOIN openalex.funders.funder_match_policy p
      ON p.funder_id = f.funder_id
     AND p.matcher = 'datacite_name_v1' AND p.eligible
    WHERE ror_id = 'https://ror.org/021nxhr62'  -- National Science Foundation
),

awards_transformed AS (
    SELECT
        -- Generate unique ID using xxhash64 of funder + award_id
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.awd_id)))) % 9000000000 as id,

        -- Display name = award title
        g.awd_titl_txt as display_name,

        -- Description = abstract
        g.awd_abstract_narration as description,

        -- Funder info
        f.funder_id,
        LOWER(g.awd_id) as funder_award_id,

        -- Funding amount
        CAST(g.awd_amount AS DOUBLE) as amount,
        'USD' as currency,

        -- Funder struct
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        -- Funding type based on transaction type and instrument
        CASE
            WHEN LOWER(g.awd_istr_txt) LIKE '%fellowship%' THEN 'fellowship'
            WHEN LOWER(g.awd_istr_txt) LIKE '%cooperative%' THEN 'cooperative_agreement'
            WHEN LOWER(g.awd_istr_txt) LIKE '%contract%' THEN 'contract'
            WHEN LOWER(g.awd_istr_txt) LIKE '%continuing%' THEN 'grant'
            WHEN LOWER(g.awd_istr_txt) LIKE '%standard%' THEN 'grant'
            ELSE 'grant'
        END as funding_type,

        -- Funder scheme = award instrument type
        g.awd_istr_txt as funder_scheme,

        -- Provenance
        'nsf_award_search' as provenance,

        -- Dates (NSF dates are in yyyy-MM-dd format)
        TRY_TO_DATE(g.awd_eff_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.awd_exp_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.awd_eff_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.awd_exp_date, 'yyyy-MM-dd')) as end_year,

        -- Lead investigator
        CASE
            WHEN g.pi_last_name IS NOT NULL THEN
                struct(
                    g.pi_first_name as given_name,
                    g.pi_last_name as family_name,
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        g.inst_name as name,
                        g.inst_country_name as country,
                        CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                    ) as affiliation
                )
            ELSE NULL
        END as lead_investigator,

        -- Co-lead and other investigators (NULL for now)
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

        -- Landing page URL (NSF Award Search page)
        CONCAT('https://www.nsf.gov/awardsearch/showAward?AWD_ID=', g.awd_id) as landing_page_url,

        -- No DOI for NSF grants typically
        CAST(NULL AS STRING) as doi,

        -- Works API URL
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.awd_id)))) % 9000000000) as works_api_url,

        -- Timestamps
        CAST('{{run_timestamp}}' AS TIMESTAMP) as created_date,
        CAST('{{run_timestamp}}' AS TIMESTAMP) as updated_date

    FROM parquet.`{{file_path}}` g
    CROSS JOIN nsf_funder f
    WHERE g.awd_id IS NOT NULL
      AND TRIM(g.awd_id) != ''
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
        3 AS priority
    FROM awards_transformed
),
baseline AS (
    SELECT * FROM {{raw_table}} VERSION AS OF {{raw_version}}
    WHERE provenance = 'nsf_award_search' AND priority = 3
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
