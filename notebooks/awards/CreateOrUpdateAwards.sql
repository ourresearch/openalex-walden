-- Databricks notebook source
-- Five explicit source groups. sources.csv and sources/*.sql are reference documentation only.
-- KAKEN uses its frozen production object read-only until a fresh dev object exists.
-- Job base_parameters create raw_table/stage widgets; named markers bind them here.
-- No production allowlist is enabled in this five-source dev package.
SELECT CASE WHEN :stage = 'dev' AND :raw_table = 'openalex_dev.rohan_lab.gr1009_review_shadow_raw'
 THEN 'dev target accepted' ELSE raise_error('Awards: only stage=dev and the exact review shadow target are allowed; production allowlist is empty') END;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE awards_target STRING DEFAULT :raw_table;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE awards_skipped ARRAY<STRING> DEFAULT array();

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE awards_version BIGINT;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE awards_sql STRING;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE awards_timestamp TIMESTAMP DEFAULT current_timestamp();

-- COMMAND ----------

-- nsf_award_search: candidate mapping. weekly fetch cadence (descriptive).
CREATE OR REPLACE TEMP VIEW nsf_award_search_files AS
SELECT *, _metadata.file_path AS _file_path, _metadata.file_modification_time AS _file_modified
FROM read_files('s3://openalex-ingest/awards/dev-gr1009/nsf/*.parquet', format => 'parquet');

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE nsf_award_search_object STRING;

-- COMMAND ----------

-- Max modification time, then path: exactly one file even on a timestamp tie.
SET VAR nsf_award_search_object = (SELECT _file_path FROM nsf_award_search_files
 GROUP BY _file_path, _file_modified ORDER BY _file_modified DESC, _file_path DESC LIMIT 1);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW nsf_award_search_input AS SELECT * EXCEPT (_file_path, _file_modified) FROM nsf_award_search_files WHERE _file_path = session.nsf_award_search_object;

-- COMMAND ----------

SET VAR awards_version = (SELECT max(version) FROM (DESCRIBE HISTORY openalex_dev.rohan_lab.gr1009_review_shadow_raw));

-- COMMAND ----------

SET VAR awards_sql = 'CREATE OR REPLACE TEMP VIEW nsf_award_search_baseline AS SELECT * FROM ' || session.awards_target || ' VERSION AS OF ' || CAST(session.awards_version AS STRING) || ' WHERE provenance = ' || chr(39) || 'nsf_award_search' || chr(39) || ' AND priority = 3';

-- COMMAND ----------

EXECUTE IMMEDIATE awards_sql;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW nsf_award_search_candidate AS
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
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM nsf_award_search_input g
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
    SELECT * FROM nsf_award_search_baseline
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
FROM compared;

-- COMMAND ----------

-- nsf_award_search: freeze guard metrics before MERGE; views themselves are lazy.
DECLARE OR REPLACE VARIABLE nsf_award_search_result DEFAULT named_struct(
 'source', 'nsf_award_search', 'object', '', 'baseline_version', CAST(0 AS BIGINT),
 'rows_in', CAST(0 AS BIGINT), 'rows_prev', CAST(0 AS BIGINT),
 'new', CAST(0 AS BIGINT), 'changed', CAST(0 AS BIGINT), 'unchanged', CAST(0 AS BIGINT),
 'retained', CAST(0 AS BIGINT), 'duplicate_keys', CAST(0 AS BIGINT),
 'invalid_rows', CAST(0 AS BIGINT), 'state', 'pending');

-- COMMAND ----------

SET VAR nsf_award_search_result = (
WITH stats AS (
 SELECT count(*) rows_in,
 (SELECT count(*) FROM nsf_award_search_baseline) rows_prev,
 count_if(_change = 'new') new, count_if(_change = 'changed') changed,
 count_if(_change = 'unchanged') unchanged,
 (SELECT count(*) FROM nsf_award_search_baseline b LEFT ANTI JOIN nsf_award_search_candidate c
 ON b.funder_id=c.funder_id AND lower(b.funder_award_id)=lower(c.funder_award_id)) retained,
 (SELECT count(*) FROM (SELECT funder_id, lower(funder_award_id) FROM nsf_award_search_candidate GROUP BY ALL HAVING count(*) > 1)) +
 (SELECT count(*) FROM (SELECT funder_id, lower(funder_award_id) FROM nsf_award_search_baseline GROUP BY ALL HAVING count(*) > 1)) duplicate_keys,
 count_if(funder_id IS NULL OR funder_award_id IS NULL OR trim(funder_award_id) = ''
 OR NOT (provenance <=> 'nsf_award_search') OR NOT (priority <=> 3) OR funder_id NOT IN (4320306076)) invalid_rows
 FROM nsf_award_search_candidate
)
SELECT named_struct('source', 'nsf_award_search', 'object', session.nsf_award_search_object, 'baseline_version', session.awards_version,
 'rows_in', rows_in, 'rows_prev', rows_prev, 'new', new, 'changed', changed, 'unchanged', unchanged,
 'retained', retained, 'duplicate_keys', duplicate_keys, 'invalid_rows', invalid_rows,
 'state', CASE WHEN rows_in * 100 < rows_prev * 98 OR duplicate_keys > 0 OR invalid_rows > 0 OR session.nsf_award_search_object IS NULL THEN 'skipped' ELSE 'written' END)
FROM stats);

-- COMMAND ----------

-- nsf_award_search: empty source on guard rejection. No DELETE branch and no absent-row UNION.
CREATE OR REPLACE TEMP VIEW nsf_award_search_guarded_candidate AS
SELECT * FROM nsf_award_search_candidate WHERE session.nsf_award_search_result.state = 'written';

-- COMMAND ----------

-- Fail closed on target drift between guard and write. One writer per slice is still required.
SELECT CASE WHEN (SELECT max(version) FROM (DESCRIBE HISTORY openalex_dev.rohan_lab.gr1009_review_shadow_raw)) = session.awards_version
 THEN 'baseline current' ELSE raise_error('Awards target advanced during validation; rerun') END;

-- COMMAND ----------

-- nsf_award_search: write, one MERGE statement.
MERGE INTO IDENTIFIER(session.awards_target) t
USING nsf_award_search_guarded_candidate c
ON t.provenance = 'nsf_award_search' AND t.priority = 3
 AND t.funder_id = c.funder_id AND lower(t.funder_award_id) = lower(c.funder_award_id)
WHEN MATCHED AND c._change = 'changed' THEN UPDATE SET
 id = c.id,
 display_name = c.display_name,
 description = c.description,
 funder_id = c.funder_id,
 funder_award_id = c.funder_award_id,
 amount = c.amount,
 currency = c.currency,
 funder = t.funder,
 funding_type = c.funding_type,
 funder_scheme = c.funder_scheme,
 provenance = c.provenance,
 start_date = c.start_date,
 end_date = c.end_date,
 start_year = c.start_year,
 end_year = c.end_year,
 lead_investigator = c.lead_investigator,
 co_lead_investigator = c.co_lead_investigator,
 investigators = c.investigators,
 landing_page_url = c.landing_page_url,
 doi = c.doi,
 works_api_url = c.works_api_url,
 created_date = t.created_date,
 updated_date = c.updated_date,
 priority = c.priority
WHEN NOT MATCHED THEN INSERT (id, display_name, description, funder_id, funder_award_id, amount, currency, funder, funding_type, funder_scheme, provenance, start_date, end_date, start_year, end_year, lead_investigator, co_lead_investigator, investigators, landing_page_url, doi, works_api_url, created_date, updated_date, priority)
VALUES (c.id, c.display_name, c.description, c.funder_id, c.funder_award_id, c.amount, c.currency, c.funder, c.funding_type, c.funder_scheme, c.provenance, c.start_date, c.end_date, c.start_year, c.end_year, c.lead_investigator, c.co_lead_investigator, c.investigators, c.landing_page_url, c.doi, c.works_api_url, c.created_date, c.updated_date, c.priority);

-- COMMAND ----------

SET VAR awards_skipped = CASE WHEN session.nsf_award_search_result.state = 'skipped' THEN array_append(session.awards_skipped, 'nsf_award_search') ELSE session.awards_skipped END;

-- COMMAND ----------

-- nsf_award_search: result row (pre-write metrics remain frozen).
SELECT to_json(session.nsf_award_search_result) AS result;

-- COMMAND ----------

-- wellcome_trust: candidate mapping. monthly fetch cadence (descriptive).
CREATE OR REPLACE TEMP VIEW wellcome_trust_files AS
SELECT *, _metadata.file_path AS _file_path, _metadata.file_modification_time AS _file_modified
FROM read_files('s3://openalex-ingest/awards/dev-gr1009/wellcome/*.parquet', format => 'parquet');

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE wellcome_trust_object STRING;

-- COMMAND ----------

-- Max modification time, then path: exactly one file even on a timestamp tie.
SET VAR wellcome_trust_object = (SELECT _file_path FROM wellcome_trust_files
 GROUP BY _file_path, _file_modified ORDER BY _file_modified DESC, _file_path DESC LIMIT 1);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW wellcome_trust_input AS SELECT * EXCEPT (_file_path, _file_modified) FROM wellcome_trust_files WHERE _file_path = session.wellcome_trust_object;

-- COMMAND ----------

SET VAR awards_version = (SELECT max(version) FROM (DESCRIBE HISTORY openalex_dev.rohan_lab.gr1009_review_shadow_raw));

-- COMMAND ----------

SET VAR awards_sql = 'CREATE OR REPLACE TEMP VIEW wellcome_trust_baseline AS SELECT * FROM ' || session.awards_target || ' VERSION AS OF ' || CAST(session.awards_version AS STRING) || ' WHERE provenance = ' || chr(39) || 'wellcome_trust' || chr(39) || ' AND priority = 22';

-- COMMAND ----------

EXECUTE IMMEDIATE awards_sql;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW wellcome_trust_candidate AS
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
        w.title as display_name,

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
        22 AS priority
    FROM awards_transformed
),
baseline AS (
    SELECT * FROM wellcome_trust_baseline
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
FROM compared;

-- COMMAND ----------

-- wellcome_trust: freeze guard metrics before MERGE; views themselves are lazy.
DECLARE OR REPLACE VARIABLE wellcome_trust_result DEFAULT named_struct(
 'source', 'wellcome_trust', 'object', '', 'baseline_version', CAST(0 AS BIGINT),
 'rows_in', CAST(0 AS BIGINT), 'rows_prev', CAST(0 AS BIGINT),
 'new', CAST(0 AS BIGINT), 'changed', CAST(0 AS BIGINT), 'unchanged', CAST(0 AS BIGINT),
 'retained', CAST(0 AS BIGINT), 'duplicate_keys', CAST(0 AS BIGINT),
 'invalid_rows', CAST(0 AS BIGINT), 'state', 'pending');

-- COMMAND ----------

SET VAR wellcome_trust_result = (
WITH stats AS (
 SELECT count(*) rows_in,
 (SELECT count(*) FROM wellcome_trust_baseline) rows_prev,
 count_if(_change = 'new') new, count_if(_change = 'changed') changed,
 count_if(_change = 'unchanged') unchanged,
 (SELECT count(*) FROM wellcome_trust_baseline b LEFT ANTI JOIN wellcome_trust_candidate c
 ON b.funder_id=c.funder_id AND lower(b.funder_award_id)=lower(c.funder_award_id)) retained,
 (SELECT count(*) FROM (SELECT funder_id, lower(funder_award_id) FROM wellcome_trust_candidate GROUP BY ALL HAVING count(*) > 1)) +
 (SELECT count(*) FROM (SELECT funder_id, lower(funder_award_id) FROM wellcome_trust_baseline GROUP BY ALL HAVING count(*) > 1)) duplicate_keys,
 count_if(funder_id IS NULL OR funder_award_id IS NULL OR trim(funder_award_id) = ''
 OR NOT (provenance <=> 'wellcome_trust') OR NOT (priority <=> 22) OR funder_id NOT IN (4320311904)) invalid_rows
 FROM wellcome_trust_candidate
)
SELECT named_struct('source', 'wellcome_trust', 'object', session.wellcome_trust_object, 'baseline_version', session.awards_version,
 'rows_in', rows_in, 'rows_prev', rows_prev, 'new', new, 'changed', changed, 'unchanged', unchanged,
 'retained', retained, 'duplicate_keys', duplicate_keys, 'invalid_rows', invalid_rows,
 'state', CASE WHEN rows_in * 100 < rows_prev * 98 OR duplicate_keys > 0 OR invalid_rows > 0 OR session.wellcome_trust_object IS NULL THEN 'skipped' ELSE 'written' END)
FROM stats);

-- COMMAND ----------

-- wellcome_trust: empty source on guard rejection. No DELETE branch and no absent-row UNION.
CREATE OR REPLACE TEMP VIEW wellcome_trust_guarded_candidate AS
SELECT * FROM wellcome_trust_candidate WHERE session.wellcome_trust_result.state = 'written';

-- COMMAND ----------

-- Fail closed on target drift between guard and write. One writer per slice is still required.
SELECT CASE WHEN (SELECT max(version) FROM (DESCRIBE HISTORY openalex_dev.rohan_lab.gr1009_review_shadow_raw)) = session.awards_version
 THEN 'baseline current' ELSE raise_error('Awards target advanced during validation; rerun') END;

-- COMMAND ----------

-- wellcome_trust: write, one MERGE statement.
MERGE INTO IDENTIFIER(session.awards_target) t
USING wellcome_trust_guarded_candidate c
ON t.provenance = 'wellcome_trust' AND t.priority = 22
 AND t.funder_id = c.funder_id AND lower(t.funder_award_id) = lower(c.funder_award_id)
WHEN MATCHED AND c._change = 'changed' THEN UPDATE SET
 id = c.id,
 display_name = c.display_name,
 description = c.description,
 funder_id = c.funder_id,
 funder_award_id = c.funder_award_id,
 amount = c.amount,
 currency = c.currency,
 funder = t.funder,
 funding_type = c.funding_type,
 funder_scheme = c.funder_scheme,
 provenance = c.provenance,
 start_date = c.start_date,
 end_date = c.end_date,
 start_year = c.start_year,
 end_year = c.end_year,
 lead_investigator = c.lead_investigator,
 co_lead_investigator = c.co_lead_investigator,
 investigators = c.investigators,
 landing_page_url = c.landing_page_url,
 doi = c.doi,
 works_api_url = c.works_api_url,
 created_date = t.created_date,
 updated_date = c.updated_date,
 priority = c.priority
WHEN NOT MATCHED THEN INSERT (id, display_name, description, funder_id, funder_award_id, amount, currency, funder, funding_type, funder_scheme, provenance, start_date, end_date, start_year, end_year, lead_investigator, co_lead_investigator, investigators, landing_page_url, doi, works_api_url, created_date, updated_date, priority)
VALUES (c.id, c.display_name, c.description, c.funder_id, c.funder_award_id, c.amount, c.currency, c.funder, c.funding_type, c.funder_scheme, c.provenance, c.start_date, c.end_date, c.start_year, c.end_year, c.lead_investigator, c.co_lead_investigator, c.investigators, c.landing_page_url, c.doi, c.works_api_url, c.created_date, c.updated_date, c.priority);

-- COMMAND ----------

SET VAR awards_skipped = CASE WHEN session.wellcome_trust_result.state = 'skipped' THEN array_append(session.awards_skipped, 'wellcome_trust') ELSE session.awards_skipped END;

-- COMMAND ----------

-- wellcome_trust: result row (pre-write metrics remain frozen).
SELECT to_json(session.wellcome_trust_result) AS result;

-- COMMAND ----------

-- cordis: candidate mapping. manual fetch cadence (descriptive).
CREATE OR REPLACE TEMP VIEW cordis_files AS
SELECT *, _metadata.file_path AS _file_path, _metadata.file_modification_time AS _file_modified
FROM read_files('s3://openalex-ingest/awards/dev-gr1009/european_commission/*.parquet', format => 'parquet');

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE cordis_object STRING;

-- COMMAND ----------

-- Max modification time, then path: exactly one file even on a timestamp tie.
SET VAR cordis_object = (SELECT _file_path FROM cordis_files
 GROUP BY _file_path, _file_modified ORDER BY _file_modified DESC, _file_path DESC LIMIT 1);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW cordis_input AS SELECT * EXCEPT (_file_path, _file_modified) FROM cordis_files WHERE _file_path = session.cordis_object;

-- COMMAND ----------

SET VAR awards_version = (SELECT max(version) FROM (DESCRIBE HISTORY openalex_dev.rohan_lab.gr1009_review_shadow_raw));

-- COMMAND ----------

SET VAR awards_sql = 'CREATE OR REPLACE TEMP VIEW cordis_baseline AS SELECT * FROM ' || session.awards_target || ' VERSION AS OF ' || CAST(session.awards_version AS STRING) || ' WHERE provenance = ' || chr(39) || 'cordis' || chr(39) || ' AND priority = 27';

-- COMMAND ----------

EXECUTE IMMEDIATE awards_sql;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW cordis_candidate AS
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
    SELECT * FROM cordis_baseline
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
FROM compared;

-- COMMAND ----------

-- cordis: freeze guard metrics before MERGE; views themselves are lazy.
DECLARE OR REPLACE VARIABLE cordis_result DEFAULT named_struct(
 'source', 'cordis', 'object', '', 'baseline_version', CAST(0 AS BIGINT),
 'rows_in', CAST(0 AS BIGINT), 'rows_prev', CAST(0 AS BIGINT),
 'new', CAST(0 AS BIGINT), 'changed', CAST(0 AS BIGINT), 'unchanged', CAST(0 AS BIGINT),
 'retained', CAST(0 AS BIGINT), 'duplicate_keys', CAST(0 AS BIGINT),
 'invalid_rows', CAST(0 AS BIGINT), 'state', 'pending');

-- COMMAND ----------

SET VAR cordis_result = (
WITH stats AS (
 SELECT count(*) rows_in,
 (SELECT count(*) FROM cordis_baseline) rows_prev,
 count_if(_change = 'new') new, count_if(_change = 'changed') changed,
 count_if(_change = 'unchanged') unchanged,
 (SELECT count(*) FROM cordis_baseline b LEFT ANTI JOIN cordis_candidate c
 ON b.funder_id=c.funder_id AND lower(b.funder_award_id)=lower(c.funder_award_id)) retained,
 (SELECT count(*) FROM (SELECT funder_id, lower(funder_award_id) FROM cordis_candidate GROUP BY ALL HAVING count(*) > 1)) +
 (SELECT count(*) FROM (SELECT funder_id, lower(funder_award_id) FROM cordis_baseline GROUP BY ALL HAVING count(*) > 1)) duplicate_keys,
 count_if(funder_id IS NULL OR funder_award_id IS NULL OR trim(funder_award_id) = ''
 OR NOT (provenance <=> 'cordis') OR NOT (priority <=> 27) OR funder_id NOT IN (4320320300, 4320338438, 4320338448)) invalid_rows
 FROM cordis_candidate
)
SELECT named_struct('source', 'cordis', 'object', session.cordis_object, 'baseline_version', session.awards_version,
 'rows_in', rows_in, 'rows_prev', rows_prev, 'new', new, 'changed', changed, 'unchanged', unchanged,
 'retained', retained, 'duplicate_keys', duplicate_keys, 'invalid_rows', invalid_rows,
 'state', CASE WHEN rows_in * 100 < rows_prev * 98 OR duplicate_keys > 0 OR invalid_rows > 0 OR session.cordis_object IS NULL THEN 'skipped' ELSE 'written' END)
FROM stats);

-- COMMAND ----------

-- cordis: empty source on guard rejection. No DELETE branch and no absent-row UNION.
CREATE OR REPLACE TEMP VIEW cordis_guarded_candidate AS
SELECT * FROM cordis_candidate WHERE session.cordis_result.state = 'written';

-- COMMAND ----------

-- Fail closed on target drift between guard and write. One writer per slice is still required.
SELECT CASE WHEN (SELECT max(version) FROM (DESCRIBE HISTORY openalex_dev.rohan_lab.gr1009_review_shadow_raw)) = session.awards_version
 THEN 'baseline current' ELSE raise_error('Awards target advanced during validation; rerun') END;

-- COMMAND ----------

-- cordis: write, one MERGE statement.
MERGE INTO IDENTIFIER(session.awards_target) t
USING cordis_guarded_candidate c
ON t.provenance = 'cordis' AND t.priority = 27
 AND t.funder_id = c.funder_id AND lower(t.funder_award_id) = lower(c.funder_award_id)
WHEN MATCHED AND c._change = 'changed' THEN UPDATE SET
 id = c.id,
 display_name = c.display_name,
 description = c.description,
 funder_id = c.funder_id,
 funder_award_id = c.funder_award_id,
 amount = c.amount,
 currency = c.currency,
 funder = t.funder,
 funding_type = c.funding_type,
 funder_scheme = c.funder_scheme,
 provenance = c.provenance,
 start_date = c.start_date,
 end_date = c.end_date,
 start_year = c.start_year,
 end_year = c.end_year,
 lead_investigator = c.lead_investigator,
 co_lead_investigator = c.co_lead_investigator,
 investigators = c.investigators,
 landing_page_url = c.landing_page_url,
 doi = c.doi,
 works_api_url = c.works_api_url,
 created_date = t.created_date,
 updated_date = c.updated_date,
 priority = c.priority
WHEN NOT MATCHED THEN INSERT (id, display_name, description, funder_id, funder_award_id, amount, currency, funder, funding_type, funder_scheme, provenance, start_date, end_date, start_year, end_year, lead_investigator, co_lead_investigator, investigators, landing_page_url, doi, works_api_url, created_date, updated_date, priority)
VALUES (c.id, c.display_name, c.description, c.funder_id, c.funder_award_id, c.amount, c.currency, c.funder, c.funding_type, c.funder_scheme, c.provenance, c.start_date, c.end_date, c.start_year, c.end_year, c.lead_investigator, c.co_lead_investigator, c.investigators, c.landing_page_url, c.doi, c.works_api_url, c.created_date, c.updated_date, c.priority);

-- COMMAND ----------

SET VAR awards_skipped = CASE WHEN session.cordis_result.state = 'skipped' THEN array_append(session.awards_skipped, 'cordis') ELSE session.awards_skipped END;

-- COMMAND ----------

-- cordis: result row (pre-write metrics remain frozen).
SELECT to_json(session.cordis_result) AS result;

-- COMMAND ----------

-- gates_foundation: candidate mapping. manual fetch cadence (descriptive).
CREATE OR REPLACE TEMP VIEW gates_foundation_files AS
SELECT *, _metadata.file_path AS _file_path, _metadata.file_modification_time AS _file_modified
FROM read_files('s3://openalex-ingest/awards/dev-gr1009/gates/*.parquet', format => 'parquet');

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE gates_foundation_object STRING;

-- COMMAND ----------

-- Max modification time, then path: exactly one file even on a timestamp tie.
SET VAR gates_foundation_object = (SELECT _file_path FROM gates_foundation_files
 GROUP BY _file_path, _file_modified ORDER BY _file_modified DESC, _file_path DESC LIMIT 1);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW gates_foundation_input AS SELECT * EXCEPT (_file_path, _file_modified) FROM gates_foundation_files WHERE _file_path = session.gates_foundation_object;

-- COMMAND ----------

SET VAR awards_version = (SELECT max(version) FROM (DESCRIBE HISTORY openalex_dev.rohan_lab.gr1009_review_shadow_raw));

-- COMMAND ----------

SET VAR awards_sql = 'CREATE OR REPLACE TEMP VIEW gates_foundation_baseline AS SELECT * FROM ' || session.awards_target || ' VERSION AS OF ' || CAST(session.awards_version AS STRING) || ' WHERE provenance = ' || chr(39) || 'gates_foundation' || chr(39) || ' AND priority = 4';

-- COMMAND ----------

EXECUTE IMMEDIATE awards_sql;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW gates_foundation_candidate AS
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
        4 AS priority
    FROM awards_transformed
),
baseline AS (
    SELECT * FROM gates_foundation_baseline
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
FROM compared;

-- COMMAND ----------

-- gates_foundation: freeze guard metrics before MERGE; views themselves are lazy.
DECLARE OR REPLACE VARIABLE gates_foundation_result DEFAULT named_struct(
 'source', 'gates_foundation', 'object', '', 'baseline_version', CAST(0 AS BIGINT),
 'rows_in', CAST(0 AS BIGINT), 'rows_prev', CAST(0 AS BIGINT),
 'new', CAST(0 AS BIGINT), 'changed', CAST(0 AS BIGINT), 'unchanged', CAST(0 AS BIGINT),
 'retained', CAST(0 AS BIGINT), 'duplicate_keys', CAST(0 AS BIGINT),
 'invalid_rows', CAST(0 AS BIGINT), 'state', 'pending');

-- COMMAND ----------

SET VAR gates_foundation_result = (
WITH stats AS (
 SELECT count(*) rows_in,
 (SELECT count(*) FROM gates_foundation_baseline) rows_prev,
 count_if(_change = 'new') new, count_if(_change = 'changed') changed,
 count_if(_change = 'unchanged') unchanged,
 (SELECT count(*) FROM gates_foundation_baseline b LEFT ANTI JOIN gates_foundation_candidate c
 ON b.funder_id=c.funder_id AND lower(b.funder_award_id)=lower(c.funder_award_id)) retained,
 (SELECT count(*) FROM (SELECT funder_id, lower(funder_award_id) FROM gates_foundation_candidate GROUP BY ALL HAVING count(*) > 1)) +
 (SELECT count(*) FROM (SELECT funder_id, lower(funder_award_id) FROM gates_foundation_baseline GROUP BY ALL HAVING count(*) > 1)) duplicate_keys,
 count_if(funder_id IS NULL OR funder_award_id IS NULL OR trim(funder_award_id) = ''
 OR NOT (provenance <=> 'gates_foundation') OR NOT (priority <=> 4) OR funder_id NOT IN (4320306137)) invalid_rows
 FROM gates_foundation_candidate
)
SELECT named_struct('source', 'gates_foundation', 'object', session.gates_foundation_object, 'baseline_version', session.awards_version,
 'rows_in', rows_in, 'rows_prev', rows_prev, 'new', new, 'changed', changed, 'unchanged', unchanged,
 'retained', retained, 'duplicate_keys', duplicate_keys, 'invalid_rows', invalid_rows,
 'state', CASE WHEN rows_in * 100 < rows_prev * 98 OR duplicate_keys > 0 OR invalid_rows > 0 OR session.gates_foundation_object IS NULL THEN 'skipped' ELSE 'written' END)
FROM stats);

-- COMMAND ----------

-- gates_foundation: empty source on guard rejection. No DELETE branch and no absent-row UNION.
CREATE OR REPLACE TEMP VIEW gates_foundation_guarded_candidate AS
SELECT * FROM gates_foundation_candidate WHERE session.gates_foundation_result.state = 'written';

-- COMMAND ----------

-- Fail closed on target drift between guard and write. One writer per slice is still required.
SELECT CASE WHEN (SELECT max(version) FROM (DESCRIBE HISTORY openalex_dev.rohan_lab.gr1009_review_shadow_raw)) = session.awards_version
 THEN 'baseline current' ELSE raise_error('Awards target advanced during validation; rerun') END;

-- COMMAND ----------

-- gates_foundation: write, one MERGE statement.
MERGE INTO IDENTIFIER(session.awards_target) t
USING gates_foundation_guarded_candidate c
ON t.provenance = 'gates_foundation' AND t.priority = 4
 AND t.funder_id = c.funder_id AND lower(t.funder_award_id) = lower(c.funder_award_id)
WHEN MATCHED AND c._change = 'changed' THEN UPDATE SET
 id = c.id,
 display_name = c.display_name,
 description = c.description,
 funder_id = c.funder_id,
 funder_award_id = c.funder_award_id,
 amount = c.amount,
 currency = c.currency,
 funder = t.funder,
 funding_type = c.funding_type,
 funder_scheme = c.funder_scheme,
 provenance = c.provenance,
 start_date = c.start_date,
 end_date = c.end_date,
 start_year = c.start_year,
 end_year = c.end_year,
 lead_investigator = c.lead_investigator,
 co_lead_investigator = c.co_lead_investigator,
 investigators = c.investigators,
 landing_page_url = c.landing_page_url,
 doi = c.doi,
 works_api_url = c.works_api_url,
 created_date = t.created_date,
 updated_date = c.updated_date,
 priority = c.priority
WHEN NOT MATCHED THEN INSERT (id, display_name, description, funder_id, funder_award_id, amount, currency, funder, funding_type, funder_scheme, provenance, start_date, end_date, start_year, end_year, lead_investigator, co_lead_investigator, investigators, landing_page_url, doi, works_api_url, created_date, updated_date, priority)
VALUES (c.id, c.display_name, c.description, c.funder_id, c.funder_award_id, c.amount, c.currency, c.funder, c.funding_type, c.funder_scheme, c.provenance, c.start_date, c.end_date, c.start_year, c.end_year, c.lead_investigator, c.co_lead_investigator, c.investigators, c.landing_page_url, c.doi, c.works_api_url, c.created_date, c.updated_date, c.priority);

-- COMMAND ----------

SET VAR awards_skipped = CASE WHEN session.gates_foundation_result.state = 'skipped' THEN array_append(session.awards_skipped, 'gates_foundation') ELSE session.awards_skipped END;

-- COMMAND ----------

-- gates_foundation: result row (pre-write metrics remain frozen).
SELECT to_json(session.gates_foundation_result) AS result;

-- COMMAND ----------

-- kaken: candidate mapping. monthly fetch cadence (descriptive).
CREATE OR REPLACE TEMP VIEW kaken_files AS
SELECT *, _metadata.file_path AS _file_path, _metadata.file_modification_time AS _file_modified
FROM read_files('s3://openalex-ingest/awards/kaken/*.parquet', format => 'parquet');

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE kaken_object STRING;

-- COMMAND ----------

-- Max modification time, then path: exactly one file even on a timestamp tie.
SET VAR kaken_object = (SELECT _file_path FROM kaken_files
 GROUP BY _file_path, _file_modified ORDER BY _file_modified DESC, _file_path DESC LIMIT 1);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW kaken_input AS SELECT * EXCEPT (_file_path, _file_modified) FROM kaken_files WHERE _file_path = session.kaken_object;

-- COMMAND ----------

SET VAR awards_version = (SELECT max(version) FROM (DESCRIBE HISTORY openalex_dev.rohan_lab.gr1009_review_shadow_raw));

-- COMMAND ----------

SET VAR awards_sql = 'CREATE OR REPLACE TEMP VIEW kaken_baseline AS SELECT * FROM ' || session.awards_target || ' VERSION AS OF ' || CAST(session.awards_version AS STRING) || ' WHERE provenance = ' || chr(39) || 'kaken' || chr(39) || ' AND priority = 10';

-- COMMAND ----------

EXECUTE IMMEDIATE awards_sql;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW kaken_candidate AS
-- Legacy mapping from CreateKAKENAwards.ipynb, origin c3db0a32.
-- _change is runner metadata, never a raw column. Compare all 22 other columns null-safely.
WITH
-- Get JSPS funder from OpenAlex
jsps_funder AS (
    SELECT
        funder_id,
        display_name,
        ror_id,
        doi
    FROM openalex.funders.funders
    WHERE funder_id = 4320334764  -- Japan Society for the Promotion of Science
),

awards_transformed AS (
    SELECT
        -- Generate unique ID using xxhash64 of funder_id:project_id
        abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000 as id,

        -- Display name = project title
        g.title as display_name,

        -- Description = abstract
        g.abstract as description,

        -- Funder info
        f.funder_id,
        g.project_id as funder_award_id,

        -- Amount in JPY
        g.amount as amount,
        'JPY' as currency,

        -- Funder struct
        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        -- Funding type - map KAKEN categories to types
        CASE
            WHEN LOWER(g.category) LIKE '%fellow%' THEN 'fellowship'
            WHEN LOWER(g.category) LIKE '%young%' THEN 'fellowship'
            WHEN LOWER(g.category) LIKE '%early-career%' THEN 'fellowship'
            WHEN LOWER(g.category) LIKE '%encouragement%' THEN 'fellowship'
            WHEN LOWER(g.category) LIKE '%start%' THEN 'fellowship'
            WHEN LOWER(g.category) LIKE '%specially promoted%' THEN 'research'
            WHEN LOWER(g.category) LIKE '%scientific research%' THEN 'research'
            WHEN LOWER(g.category) LIKE '%challenging%' THEN 'research'
            WHEN LOWER(g.category) LIKE '%transformative%' THEN 'research'
            WHEN LOWER(g.category) LIKE '%pioneering%' THEN 'research'
            WHEN LOWER(g.category) LIKE '%international%' THEN 'research'
            WHEN LOWER(g.category) LIKE '%publication%' THEN 'other'
            ELSE 'grant'
        END as funding_type,

        -- Funder scheme = research category
        g.category as funder_scheme,

        -- Provenance
        'kaken' as provenance,

        -- Dates
        TRY_TO_DATE(g.start_date, 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(g.end_date, 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(g.start_date, 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(g.end_date, 'yyyy-MM-dd')) as end_year,

        -- Lead investigator
        CASE
            WHEN g.pi_family_name IS NOT NULL THEN
                struct(
                    g.pi_given_name as given_name,
                    g.pi_family_name as family_name,
                    -- NRID is not an ORCID, so we leave orcid NULL
                    CAST(NULL AS STRING) as orcid,
                    CAST(NULL AS DATE) as role_start,
                    struct(
                        COALESCE(g.institution, g.pi_affiliation) as name,
                        'Japan' as country,
                        -- Include NRID as an identifier if available
                        CASE
                            WHEN g.pi_nrid IS NOT NULL THEN
                                array(struct(
                                    CONCAT('https://nrid.nii.ac.jp/nrid/', g.pi_nrid) as id,
                                    'nrid' as type,
                                    'kaken' as asserted_by
                                ))
                            ELSE CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>)
                        END as ids
                    ) as affiliation
                )
            ELSE NULL
        END as lead_investigator,

        -- Co-lead and other investigators (not available in scraped data)
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

        -- Landing page URL
        g.landing_page_url,

        -- No DOI for KAKEN grants
        CAST(NULL AS STRING) as doi,

        -- Works API URL
        concat('https://api.openalex.org/works?filter=awards.id:G', abs(xxhash64(CONCAT(f.funder_id, ':', LOWER(g.project_id)))) % 9000000000) as works_api_url,

        -- Timestamps
        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM kaken_input g
    CROSS JOIN jsps_funder f
    WHERE g.project_id IS NOT NULL
      AND TRIM(g.project_id) != ''
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
        10 AS priority
    FROM awards_transformed
),
baseline AS (
    SELECT * FROM kaken_baseline
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
FROM compared;

-- COMMAND ----------

-- kaken: freeze guard metrics before MERGE; views themselves are lazy.
DECLARE OR REPLACE VARIABLE kaken_result DEFAULT named_struct(
 'source', 'kaken', 'object', '', 'baseline_version', CAST(0 AS BIGINT),
 'rows_in', CAST(0 AS BIGINT), 'rows_prev', CAST(0 AS BIGINT),
 'new', CAST(0 AS BIGINT), 'changed', CAST(0 AS BIGINT), 'unchanged', CAST(0 AS BIGINT),
 'retained', CAST(0 AS BIGINT), 'duplicate_keys', CAST(0 AS BIGINT),
 'invalid_rows', CAST(0 AS BIGINT), 'state', 'pending');

-- COMMAND ----------

SET VAR kaken_result = (
WITH stats AS (
 SELECT count(*) rows_in,
 (SELECT count(*) FROM kaken_baseline) rows_prev,
 count_if(_change = 'new') new, count_if(_change = 'changed') changed,
 count_if(_change = 'unchanged') unchanged,
 (SELECT count(*) FROM kaken_baseline b LEFT ANTI JOIN kaken_candidate c
 ON b.funder_id=c.funder_id AND lower(b.funder_award_id)=lower(c.funder_award_id)) retained,
 (SELECT count(*) FROM (SELECT funder_id, lower(funder_award_id) FROM kaken_candidate GROUP BY ALL HAVING count(*) > 1)) +
 (SELECT count(*) FROM (SELECT funder_id, lower(funder_award_id) FROM kaken_baseline GROUP BY ALL HAVING count(*) > 1)) duplicate_keys,
 count_if(funder_id IS NULL OR funder_award_id IS NULL OR trim(funder_award_id) = ''
 OR NOT (provenance <=> 'kaken') OR NOT (priority <=> 10) OR funder_id NOT IN (4320334764)) invalid_rows
 FROM kaken_candidate
)
SELECT named_struct('source', 'kaken', 'object', session.kaken_object, 'baseline_version', session.awards_version,
 'rows_in', rows_in, 'rows_prev', rows_prev, 'new', new, 'changed', changed, 'unchanged', unchanged,
 'retained', retained, 'duplicate_keys', duplicate_keys, 'invalid_rows', invalid_rows,
 'state', CASE WHEN rows_in * 100 < rows_prev * 98 OR duplicate_keys > 0 OR invalid_rows > 0 OR session.kaken_object IS NULL THEN 'skipped' ELSE 'written' END)
FROM stats);

-- COMMAND ----------

-- kaken: empty source on guard rejection. No DELETE branch and no absent-row UNION.
CREATE OR REPLACE TEMP VIEW kaken_guarded_candidate AS
SELECT * FROM kaken_candidate WHERE session.kaken_result.state = 'written';

-- COMMAND ----------

-- Fail closed on target drift between guard and write. One writer per slice is still required.
SELECT CASE WHEN (SELECT max(version) FROM (DESCRIBE HISTORY openalex_dev.rohan_lab.gr1009_review_shadow_raw)) = session.awards_version
 THEN 'baseline current' ELSE raise_error('Awards target advanced during validation; rerun') END;

-- COMMAND ----------

-- kaken: write, one MERGE statement.
MERGE INTO IDENTIFIER(session.awards_target) t
USING kaken_guarded_candidate c
ON t.provenance = 'kaken' AND t.priority = 10
 AND t.funder_id = c.funder_id AND lower(t.funder_award_id) = lower(c.funder_award_id)
WHEN MATCHED AND c._change = 'changed' THEN UPDATE SET
 id = c.id,
 display_name = c.display_name,
 description = c.description,
 funder_id = c.funder_id,
 funder_award_id = c.funder_award_id,
 amount = c.amount,
 currency = c.currency,
 funder = t.funder,
 funding_type = c.funding_type,
 funder_scheme = c.funder_scheme,
 provenance = c.provenance,
 start_date = c.start_date,
 end_date = c.end_date,
 start_year = c.start_year,
 end_year = c.end_year,
 lead_investigator = c.lead_investigator,
 co_lead_investigator = c.co_lead_investigator,
 investigators = c.investigators,
 landing_page_url = c.landing_page_url,
 doi = c.doi,
 works_api_url = c.works_api_url,
 created_date = t.created_date,
 updated_date = c.updated_date,
 priority = c.priority
WHEN NOT MATCHED THEN INSERT (id, display_name, description, funder_id, funder_award_id, amount, currency, funder, funding_type, funder_scheme, provenance, start_date, end_date, start_year, end_year, lead_investigator, co_lead_investigator, investigators, landing_page_url, doi, works_api_url, created_date, updated_date, priority)
VALUES (c.id, c.display_name, c.description, c.funder_id, c.funder_award_id, c.amount, c.currency, c.funder, c.funding_type, c.funder_scheme, c.provenance, c.start_date, c.end_date, c.start_year, c.end_year, c.lead_investigator, c.co_lead_investigator, c.investigators, c.landing_page_url, c.doi, c.works_api_url, c.created_date, c.updated_date, c.priority);

-- COMMAND ----------

SET VAR awards_skipped = CASE WHEN session.kaken_result.state = 'skipped' THEN array_append(session.awards_skipped, 'kaken') ELSE session.awards_skipped END;

-- COMMAND ----------

-- kaken: result row (pre-write metrics remain frozen).
SELECT to_json(session.kaken_result) AS result;

-- COMMAND ----------

SELECT CASE WHEN size(session.awards_skipped) > 0 THEN raise_error('Skipped sources: ' || concat_ws(', ', session.awards_skipped)) ELSE 'All five sources written' END AS result;
