-- dd1016-b3: withdraw the funders with no ROR id, no award, no work link in any source, no merge alias and no mid.entity_link role (Kyle 10-08),
-- publish the catalogue and re-sign the
-- name lookup. F06 still runs the link MERGE ... DELETE on every link table as a guard; by the rule it removes 0 work-funder rows.
-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization. One SQL session, statements in order.
-- Run on the ship day after the code push has deployed, before 18:00 PT (readers start 19:00 PT). Every gate is an assert_true: on
-- any failure STOP and run ROLLBACK.sql from the top (it is safe at every point after F02).
-- F00 preconditions: the switch is live (served funders is a VIEW over funders_compat, signed lookup present).
SELECT assert_true(date_format(convert_timezone('America/Los_Angeles', current_timestamp()),'HHmm') BETWEEN '0800' AND '1630','start only 08:00-16:30 PT (readers start 19:00 PT, dev run took about 10 min): STOP and ship another day') ok;
SELECT assert_true(count(*)=1,'served funders must be the post-switch VIEW: STOP') ok FROM openalex.information_schema.tables WHERE table_schema='funders' AND table_name='funders' AND table_type='VIEW';
SELECT assert_true(count(*)=1,'signed name lookup missing: STOP') ok FROM openalex.information_schema.tables WHERE table_schema='funders' AND table_name='funder_name_lookup';
SELECT assert_true(count(*)=1,'crossref_no_doi_main_cut missing (switch not complete): STOP') ok FROM openalex.information_schema.tables WHERE table_schema='awards' AND table_name='crossref_no_doi_main_cut';
SELECT assert_true(count(*)=0,'batch dd1016-b3 already in the ledger: STOP (rerun needs ROLLBACK first)') ok FROM openalex.funders.deleted_funders WHERE batch='dd1016-b3';
SELECT assert_true((SELECT concat(count(*), char(58), cast(sum(cast(xxhash64(funder_id,display_name,alternate_titles,ror_id,doi,crossref_id,merge_into_id) AS DECIMAL(38,0))) AS STRING)) FROM openalex.funders.funders)=(SELECT max(catalogue_signature) FROM openalex.funders.funder_name_lookup),'live lookup is not signed for the live catalogue: STOP') ok;
-- F01 stage the reviewed drop set (CREATE without REPLACE: immutable per release id) and refuse any drift from the reviewed 2,191 + 0.
CREATE TABLE openalex.funders.dd1016_drop_live AS WITH linked AS (SELECT funder_id FROM openalex.mid.work_funder
 UNION SELECT CAST(regexp_extract(funder_id,'F([0-9]+)$',1) AS BIGINT) funder_id FROM openalex.works.fulltext_work_funders
 UNION SELECT funder_id FROM openalex.awards.crossref_work_funders
 UNION SELECT funder_id FROM openalex.awards.crossref_no_doi_main_cut
 UNION SELECT funder_id FROM openalex.awards.datacite_work_funders
 UNION SELECT funder_id FROM openalex.awards.europepmc_work_funders
 UNION SELECT funder_id FROM openalex.awards.funder_reported_work_funders
 UNION SELECT funder_id FROM openalex.awards.hakai_work_funders
 UNION SELECT funder_id FROM openalex.awards.anr_work_funders
 UNION SELECT funder_id FROM openalex.awards.kaken_work_funders
 UNION SELECT funder_id FROM openalex.awards.nwo_work_funders
 UNION SELECT funder_id FROM openalex.awards.datacite_work_funder_evidence
 UNION SELECT coalesce(funder_id, CAST(regexp_extract(funder.id,'F([0-9]+)$',1) AS BIGINT)) funder_id FROM openalex.awards.gtr_awards WHERE work_id IS NOT NULL
 UNION SELECT CAST(regexp_extract(award.funder_id,'F([0-9]+)$',1) AS BIGINT) funder_id FROM openalex.awards.work_awards
 UNION SELECT CAST(regexp_extract(wf.id,'F([0-9]+)$',1) AS BIGINT) funder_id FROM openalex.works.openalex_works LATERAL VIEW explode(funders) x AS wf
 UNION SELECT CAST(nullif(regexp_extract(id_1,'^F([0-9]+)$',1),'') AS BIGINT) funder_id FROM openalex.mid.entity_link
 UNION SELECT CAST(nullif(regexp_extract(id_2,'^F([0-9]+)$',1),'') AS BIGINT) funder_id FROM openalex.mid.entity_link)
SELECT f.funder_id FROM openalex.funders.funders f
WHERE f.merge_into_id IS NULL AND nullif(trim(f.ror_id),'') IS NULL
AND NOT EXISTS (SELECT 1 FROM openalex.awards.openalex_awards a WHERE a.funder_id = f.funder_id)
AND NOT EXISTS (SELECT 1 FROM openalex.funders.funders c JOIN linked l ON l.funder_id = c.funder_id WHERE coalesce(c.merge_into_id, c.funder_id) = f.funder_id)
AND NOT EXISTS (SELECT 1 FROM openalex.funders.funders t WHERE t.merge_into_id = f.funder_id)
AND NOT EXISTS (SELECT 1 FROM openalex.funders.funder r WHERE r.merged_into_id = f.funder_id);
CREATE TABLE openalex.funders.dd1016_drop AS SELECT funder_id, 'live' kind FROM openalex.funders.dd1016_drop_live
UNION ALL SELECT f.funder_id, 'merged_into_dropped' kind FROM openalex.funders.funders f JOIN openalex.funders.dd1016_drop_live d ON f.merge_into_id = d.funder_id;
SELECT assert_true((SELECT count(*) FROM openalex.funders.dd1016_drop WHERE kind='live')=2191 AND (SELECT sha2(concat(array_join(transform(sort_array(collect_list(funder_id)),x -> cast(x AS string)), '\n'),'\n'),256) FROM openalex.funders.dd1016_drop WHERE kind='live')='ca97a58804a3f90ceabad9165415e1759ce5073e655b2511eb1d44815e74ce14'
 AND (SELECT count(*) FROM openalex.funders.dd1016_drop WHERE kind='merged_into_dropped')=0 AND (SELECT sha2(concat(array_join(transform(sort_array(collect_list(funder_id)),x -> cast(x AS string)), '\n'),'\n'),256) FROM openalex.funders.dd1016_drop WHERE kind='merged_into_dropped')='01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b'
 AND (SELECT count(DISTINCT funder_id) FROM openalex.funders.dd1016_drop)=2191,'drop set differs from the reviewed 2,191 + 0: STOP, re-review') ok;
SELECT assert_true(count(*)=0,'a funder merges into a dropped alias (second level): STOP') ok FROM openalex.funders.funders f JOIN openalex.funders.dd1016_drop d ON f.merge_into_id=d.funder_id AND d.kind='merged_into_dropped';
-- F02 before images (rollback inputs).
CREATE TABLE openalex.funders.dd1016_catalogue_before AS SELECT * FROM openalex.funders.funders;
CREATE TABLE openalex.funders.dd1016_registry_before AS SELECT * FROM openalex.funders.funder;
CREATE TABLE openalex.funders.dd1016_lookup_before AS SELECT * FROM openalex.funders.funder_name_lookup;
CREATE TABLE openalex.funders.dd1016_bk_mid_work_funder AS SELECT * FROM openalex.mid.work_funder WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop);
CREATE TABLE openalex.funders.dd1016_bk_fulltext_work_funders AS SELECT * FROM openalex.works.fulltext_work_funders WHERE funder_id IN (SELECT concat('https://openalex.org/F', funder_id) FROM openalex.funders.dd1016_drop);
CREATE TABLE openalex.funders.dd1016_bk_crossref_work_funders AS SELECT * FROM openalex.awards.crossref_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop);
CREATE TABLE openalex.funders.dd1016_bk_crossref_no_doi_main_cut AS SELECT * FROM openalex.awards.crossref_no_doi_main_cut WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop);
CREATE TABLE openalex.funders.dd1016_bk_datacite_work_funders AS SELECT * FROM openalex.awards.datacite_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop);
CREATE TABLE openalex.funders.dd1016_bk_europepmc_work_funders AS SELECT * FROM openalex.awards.europepmc_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop);
CREATE TABLE openalex.funders.dd1016_bk_funder_reported_work_funders AS SELECT * FROM openalex.awards.funder_reported_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop);
CREATE TABLE openalex.funders.dd1016_bk_hakai_work_funders AS SELECT * FROM openalex.awards.hakai_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop);
CREATE TABLE openalex.funders.dd1016_bk_anr_work_funders AS SELECT * FROM openalex.awards.anr_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop);
CREATE TABLE openalex.funders.dd1016_bk_kaken_work_funders AS SELECT * FROM openalex.awards.kaken_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop);
CREATE TABLE openalex.funders.dd1016_bk_nwo_work_funders AS SELECT * FROM openalex.awards.nwo_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop);
CREATE TABLE openalex.funders.dd1016_bk_funder_match_policy AS SELECT * FROM openalex.funders.funder_match_policy WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop);
CREATE TABLE openalex.funders.dd1016_bk_funder_names_keep AS SELECT * FROM openalex.common.funder_names_keep WHERE id IN (SELECT concat('https://openalex.org/F', funder_id) FROM openalex.funders.dd1016_drop);
CREATE TABLE openalex.funders.dd1016_counts_before AS SELECT 'mid_work_funder' tbl, (SELECT count(*) FROM openalex.mid.work_funder) n, (SELECT count(*) FROM openalex.funders.dd1016_bk_mid_work_funder) dropped UNION ALL SELECT 'fulltext_work_funders' tbl, (SELECT count(*) FROM openalex.works.fulltext_work_funders) n, (SELECT count(*) FROM openalex.funders.dd1016_bk_fulltext_work_funders) dropped UNION ALL SELECT 'crossref_work_funders' tbl, (SELECT count(*) FROM openalex.awards.crossref_work_funders) n, (SELECT count(*) FROM openalex.funders.dd1016_bk_crossref_work_funders) dropped UNION ALL SELECT 'crossref_no_doi_main_cut' tbl, (SELECT count(*) FROM openalex.awards.crossref_no_doi_main_cut) n, (SELECT count(*) FROM openalex.funders.dd1016_bk_crossref_no_doi_main_cut) dropped UNION ALL SELECT 'datacite_work_funders' tbl, (SELECT count(*) FROM openalex.awards.datacite_work_funders) n, (SELECT count(*) FROM openalex.funders.dd1016_bk_datacite_work_funders) dropped UNION ALL SELECT 'europepmc_work_funders' tbl, (SELECT count(*) FROM openalex.awards.europepmc_work_funders) n, (SELECT count(*) FROM openalex.funders.dd1016_bk_europepmc_work_funders) dropped UNION ALL SELECT 'funder_reported_work_funders' tbl, (SELECT count(*) FROM openalex.awards.funder_reported_work_funders) n, (SELECT count(*) FROM openalex.funders.dd1016_bk_funder_reported_work_funders) dropped UNION ALL SELECT 'hakai_work_funders' tbl, (SELECT count(*) FROM openalex.awards.hakai_work_funders) n, (SELECT count(*) FROM openalex.funders.dd1016_bk_hakai_work_funders) dropped UNION ALL SELECT 'anr_work_funders' tbl, (SELECT count(*) FROM openalex.awards.anr_work_funders) n, (SELECT count(*) FROM openalex.funders.dd1016_bk_anr_work_funders) dropped UNION ALL SELECT 'kaken_work_funders' tbl, (SELECT count(*) FROM openalex.awards.kaken_work_funders) n, (SELECT count(*) FROM openalex.funders.dd1016_bk_kaken_work_funders) dropped UNION ALL SELECT 'nwo_work_funders' tbl, (SELECT count(*) FROM openalex.awards.nwo_work_funders) n, (SELECT count(*) FROM openalex.funders.dd1016_bk_nwo_work_funders) dropped UNION ALL SELECT 'funder_match_policy' tbl, (SELECT count(*) FROM openalex.funders.funder_match_policy) n, (SELECT count(*) FROM openalex.funders.dd1016_bk_funder_match_policy) dropped UNION ALL SELECT 'funder_names_keep' tbl, (SELECT count(*) FROM openalex.common.funder_names_keep) n, (SELECT count(*) FROM openalex.funders.dd1016_bk_funder_names_keep) dropped;
-- F03 ledger rows first (the existing empty-funder pattern: batch, reason, full before image).
INSERT INTO openalex.funders.deleted_funders (funder_id, crossref_id, ror_id, deleted_at, reason, batch, before_image_json)
SELECT c.funder_id, c.crossref_id, c.ror_id, current_timestamp(), CASE WHEN d.kind='live' THEN 'no ROR, no award, no work link, no merge alias, no entity link (Kyle 10-08)' ELSE 'merged into a funder withdrawn in dd1016-b3' END, 'dd1016-b3',
 to_json(struct(c.*), map('ignoreNullFields','false'))
FROM openalex.funders.dd1016_catalogue_before c JOIN openalex.funders.dd1016_drop d ON d.funder_id = c.funder_id;
SELECT assert_true((SELECT count(*) FROM openalex.funders.deleted_funders WHERE batch='dd1016-b3')=2191
 AND (SELECT count(*) FROM (SELECT funder_id FROM openalex.funders.deleted_funders WHERE batch='dd1016-b3' EXCEPT SELECT funder_id FROM openalex.funders.dd1016_drop))=0,'ledger not exact: STOP') ok;
-- F04 registry: the edits.csv 'withdraw' statement of CreateOrUpdateFunders (edit_withdraw), for the staged ids.
UPDATE openalex.funders.funder SET status = 'withdrawn', ror_id = NULL, ror_primary = NULL, updated_date = current_timestamp(), run_id = 'dd1016'
WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop) AND status <> 'withdrawn';
SELECT assert_true((SELECT count(*) FROM openalex.funders.funder WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop) AND status='withdrawn' AND run_id='dd1016')=2191
 AND (SELECT count(*) FROM ((SELECT * FROM openalex.funders.funder WHERE funder_id NOT IN (SELECT funder_id FROM openalex.funders.dd1016_drop) EXCEPT ALL SELECT * FROM openalex.funders.dd1016_registry_before)
   UNION ALL (SELECT * FROM openalex.funders.dd1016_registry_before WHERE funder_id NOT IN (SELECT funder_id FROM openalex.funders.dd1016_drop) EXCEPT ALL SELECT * FROM openalex.funders.funder)))=0
 AND (SELECT count(*) FROM (SELECT * EXCEPT(status,ror_id,ror_primary,updated_date,run_id) FROM openalex.funders.funder WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)
   EXCEPT ALL SELECT * EXCEPT(status,ror_id,ror_primary,updated_date,run_id) FROM openalex.funders.dd1016_registry_before WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)))=0,
 'registry changed outside the withdrawal: STOP, ROLLBACK') ok;
-- F05 publish the catalogue: the switch's republish statement (059_republish.sql, unchanged). Withdrawn rows leave the served view.
SELECT assert_true(date_format(convert_timezone('America/Los_Angeles', current_timestamp()),'HHmm') <= '1730','past 17:30 PT before F05: STOP and run ROLLBACK.sql (run-dd1016.py rollback, allowed until 18:45 PT, nothing public has changed yet)') ok;
CREATE OR REPLACE TABLE openalex.funders.funders_compat AS
SELECT f.funder_id, CAST(get_json_object(v41.j, '$.crossref_id') AS BIGINT) crossref_id,
  get_json_object(v41.j, '$.location') location, f.display_name,
  CASE WHEN CASE WHEN v41.j IS NULL AND r.source_key IS NOT NULL THEN array_sort(array_distinct(concat(coalesce(f.alt_names, array()), coalesce(r.alt_names, array())))) ELSE f.alt_names END <=> from_json(get_json_object(v41.j, '$.alternate_titles'), 'array<string>') THEN get_json_object(v41.j, '$.alternate_titles') ELSE to_json(coalesce(CASE WHEN v41.j IS NULL AND r.source_key IS NOT NULL THEN array_sort(array_distinct(concat(coalesce(f.alt_names, array()), coalesce(r.alt_names, array())))) ELSE f.alt_names END, array())) END alternate_titles,
  get_json_object(v41.j, '$.uri') uri,
  coalesce(get_json_object(v41.j, '$.replaces'), CASE WHEN v41.j IS NULL THEN '[]' END) replaces,
  coalesce(get_json_object(v41.j, '$.replaced_by'), CASE WHEN v41.j IS NULL THEN '[]' END) replaced_by,
  get_json_object(v41.j, '$.tokens') tokens, CASE WHEN v41.j IS NOT NULL THEN get_json_object(v41.j, '$.ror_id') ELSE f.ror_id END ror_id,
  CASE WHEN f.status = 'merged' THEN f.merged_into_id END merge_into_id,
  CASE WHEN v41.j IS NOT NULL THEN CAST(get_json_object(v41.j, '$.merge_into_date') AS TIMESTAMP) WHEN f.status='merged' THEN f.updated_date END merge_into_date,
  coalesce(get_json_object(v41.j, '$.created_date'), CAST(f.created_date AS STRING)) created_date, f.wikidata_id,
  get_json_object(v41.j, '$.doi') doi,
  f.country_code, f.homepage_url, f.description, f.image_url, f.image_thumbnail_url, get_json_object(v41.j, '$.json_entity_hash') json_entity_hash, CASE WHEN v41.j IS NOT NULL THEN CAST(get_json_object(v41.j, '$.updated_date') AS TIMESTAMP) ELSE f.updated_date END updated_date
FROM openalex.funders.funder f
LEFT JOIN (SELECT CAST(source_key AS BIGINT) funder_id, record_json j FROM openalex.funders.funder_source_record WHERE source = 'openalex_v41') v41 ON v41.funder_id = f.funder_id
LEFT JOIN openalex.funders.funder_source_record r ON r.source = 'ror' AND r.source_key = lower(regexp_extract(f.ror_id, '([0-9a-z]{9})/*$', 1)) AND r.record_status <> 'gone'
WHERE f.status IN ('active', 'merged');
SELECT assert_true((SELECT count(*) FROM (
 (SELECT * FROM openalex.funders.funders EXCEPT ALL SELECT * FROM openalex.funders.dd1016_catalogue_before WHERE funder_id NOT IN (SELECT funder_id FROM openalex.funders.dd1016_drop))
 UNION ALL (SELECT * FROM openalex.funders.dd1016_catalogue_before WHERE funder_id NOT IN (SELECT funder_id FROM openalex.funders.dd1016_drop) EXCEPT ALL SELECT * FROM openalex.funders.funders)))=0
 AND (SELECT count(*) FROM openalex.funders.funders)=(SELECT count(*) FROM openalex.funders.dd1016_catalogue_before)-2191,
 'catalogue is not exactly before minus the drop set: STOP, ROLLBACK') ok;
-- F06 remove the dropped funders' work links (the registry propagation 'withdrawn' branch: MERGE ... DELETE), plus the
-- switch's frozen Crossref main cut so the preserved-union reader cannot bring them back. No redirect target.
MERGE INTO openalex.mid.work_funder t USING openalex.funders.dd1016_drop m ON t.funder_id = m.funder_id WHEN MATCHED THEN DELETE;
MERGE INTO openalex.works.fulltext_work_funders t USING openalex.funders.dd1016_drop m ON t.funder_id = concat('https://openalex.org/F', m.funder_id) WHEN MATCHED THEN DELETE;
MERGE INTO openalex.awards.crossref_work_funders t USING openalex.funders.dd1016_drop m ON t.funder_id = m.funder_id WHEN MATCHED THEN DELETE;
MERGE INTO openalex.awards.crossref_no_doi_main_cut t USING openalex.funders.dd1016_drop m ON t.funder_id = m.funder_id WHEN MATCHED THEN DELETE;
MERGE INTO openalex.awards.datacite_work_funders t USING openalex.funders.dd1016_drop m ON t.funder_id = m.funder_id WHEN MATCHED THEN DELETE;
MERGE INTO openalex.awards.europepmc_work_funders t USING openalex.funders.dd1016_drop m ON t.funder_id = m.funder_id WHEN MATCHED THEN DELETE;
MERGE INTO openalex.awards.funder_reported_work_funders t USING openalex.funders.dd1016_drop m ON t.funder_id = m.funder_id WHEN MATCHED THEN DELETE;
MERGE INTO openalex.awards.hakai_work_funders t USING openalex.funders.dd1016_drop m ON t.funder_id = m.funder_id WHEN MATCHED THEN DELETE;
MERGE INTO openalex.awards.anr_work_funders t USING openalex.funders.dd1016_drop m ON t.funder_id = m.funder_id WHEN MATCHED THEN DELETE;
MERGE INTO openalex.awards.kaken_work_funders t USING openalex.funders.dd1016_drop m ON t.funder_id = m.funder_id WHEN MATCHED THEN DELETE;
MERGE INTO openalex.awards.nwo_work_funders t USING openalex.funders.dd1016_drop m ON t.funder_id = m.funder_id WHEN MATCHED THEN DELETE;
MERGE INTO openalex.funders.funder_match_policy t USING openalex.funders.dd1016_drop m ON t.funder_id = m.funder_id WHEN MATCHED THEN DELETE;
MERGE INTO openalex.common.funder_names_keep t USING openalex.funders.dd1016_drop m ON t.id = concat('https://openalex.org/F', m.funder_id) WHEN MATCHED THEN DELETE;
SELECT assert_true((SELECT count(*) FROM openalex.mid.work_funder WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))=0
 AND (SELECT count(*) FROM openalex.mid.work_funder)=(SELECT n-dropped FROM openalex.funders.dd1016_counts_before WHERE tbl='mid_work_funder'),'residue or extra delete in mid_work_funder: STOP, ROLLBACK') ok;
SELECT assert_true((SELECT count(*) FROM openalex.works.fulltext_work_funders WHERE funder_id IN (SELECT concat('https://openalex.org/F', funder_id) FROM openalex.funders.dd1016_drop))=0
 AND (SELECT count(*) FROM openalex.works.fulltext_work_funders)=(SELECT n-dropped FROM openalex.funders.dd1016_counts_before WHERE tbl='fulltext_work_funders'),'residue or extra delete in fulltext_work_funders: STOP, ROLLBACK') ok;
SELECT assert_true((SELECT count(*) FROM openalex.awards.crossref_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))=0
 AND (SELECT count(*) FROM openalex.awards.crossref_work_funders)=(SELECT n-dropped FROM openalex.funders.dd1016_counts_before WHERE tbl='crossref_work_funders'),'residue or extra delete in crossref_work_funders: STOP, ROLLBACK') ok;
SELECT assert_true((SELECT count(*) FROM openalex.awards.crossref_no_doi_main_cut WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))=0
 AND (SELECT count(*) FROM openalex.awards.crossref_no_doi_main_cut)=(SELECT n-dropped FROM openalex.funders.dd1016_counts_before WHERE tbl='crossref_no_doi_main_cut'),'residue or extra delete in crossref_no_doi_main_cut: STOP, ROLLBACK') ok;
SELECT assert_true((SELECT count(*) FROM openalex.awards.datacite_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))=0
 AND (SELECT count(*) FROM openalex.awards.datacite_work_funders)=(SELECT n-dropped FROM openalex.funders.dd1016_counts_before WHERE tbl='datacite_work_funders'),'residue or extra delete in datacite_work_funders: STOP, ROLLBACK') ok;
SELECT assert_true((SELECT count(*) FROM openalex.awards.europepmc_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))=0
 AND (SELECT count(*) FROM openalex.awards.europepmc_work_funders)=(SELECT n-dropped FROM openalex.funders.dd1016_counts_before WHERE tbl='europepmc_work_funders'),'residue or extra delete in europepmc_work_funders: STOP, ROLLBACK') ok;
SELECT assert_true((SELECT count(*) FROM openalex.awards.funder_reported_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))=0
 AND (SELECT count(*) FROM openalex.awards.funder_reported_work_funders)=(SELECT n-dropped FROM openalex.funders.dd1016_counts_before WHERE tbl='funder_reported_work_funders'),'residue or extra delete in funder_reported_work_funders: STOP, ROLLBACK') ok;
SELECT assert_true((SELECT count(*) FROM openalex.awards.hakai_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))=0
 AND (SELECT count(*) FROM openalex.awards.hakai_work_funders)=(SELECT n-dropped FROM openalex.funders.dd1016_counts_before WHERE tbl='hakai_work_funders'),'residue or extra delete in hakai_work_funders: STOP, ROLLBACK') ok;
SELECT assert_true((SELECT count(*) FROM openalex.awards.anr_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))=0
 AND (SELECT count(*) FROM openalex.awards.anr_work_funders)=(SELECT n-dropped FROM openalex.funders.dd1016_counts_before WHERE tbl='anr_work_funders'),'residue or extra delete in anr_work_funders: STOP, ROLLBACK') ok;
SELECT assert_true((SELECT count(*) FROM openalex.awards.kaken_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))=0
 AND (SELECT count(*) FROM openalex.awards.kaken_work_funders)=(SELECT n-dropped FROM openalex.funders.dd1016_counts_before WHERE tbl='kaken_work_funders'),'residue or extra delete in kaken_work_funders: STOP, ROLLBACK') ok;
SELECT assert_true((SELECT count(*) FROM openalex.awards.nwo_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))=0
 AND (SELECT count(*) FROM openalex.awards.nwo_work_funders)=(SELECT n-dropped FROM openalex.funders.dd1016_counts_before WHERE tbl='nwo_work_funders'),'residue or extra delete in nwo_work_funders: STOP, ROLLBACK') ok;
SELECT assert_true((SELECT count(*) FROM openalex.funders.funder_match_policy WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))=0
 AND (SELECT count(*) FROM openalex.funders.funder_match_policy)=(SELECT n-dropped FROM openalex.funders.dd1016_counts_before WHERE tbl='funder_match_policy'),'residue or extra delete in funder_match_policy: STOP, ROLLBACK') ok;
SELECT assert_true((SELECT count(*) FROM openalex.common.funder_names_keep WHERE id IN (SELECT concat('https://openalex.org/F', funder_id) FROM openalex.funders.dd1016_drop))=0
 AND (SELECT count(*) FROM openalex.common.funder_names_keep)=(SELECT n-dropped FROM openalex.funders.dd1016_counts_before WHERE tbl='funder_names_keep'),'residue or extra delete in funder_names_keep: STOP, ROLLBACK') ok;
-- F07 re-sign the lookup for the new catalogue: same rows minus the dropped targets (no name is re-decided), new signature.
CREATE TABLE openalex.funders.dd1016_lookup AS SELECT l.rule, l.name_key, l.funder_id, 'openalex.funders.dd1016_' release_id, s.catalogue_signature
FROM openalex.funders.dd1016_lookup_before l LEFT ANTI JOIN openalex.funders.dd1016_drop d ON d.funder_id = l.funder_id
CROSS JOIN (SELECT concat(count(*), char(58), cast(sum(cast(xxhash64(funder_id,display_name,alternate_titles,ror_id,doi,crossref_id,merge_into_id) AS DECIMAL(38,0))) AS STRING)) FROM openalex.funders.funders) s(catalogue_signature);
SELECT assert_true((SELECT count(*) FROM (SELECT rule,name_key,funder_id FROM openalex.funders.dd1016_lookup_before EXCEPT ALL SELECT rule,name_key,funder_id FROM openalex.funders.dd1016_lookup))
   =(SELECT count(*) FROM openalex.funders.dd1016_lookup_before WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))
 AND (SELECT count(*) FROM (SELECT rule,name_key,funder_id FROM openalex.funders.dd1016_lookup EXCEPT ALL SELECT rule,name_key,funder_id FROM openalex.funders.dd1016_lookup_before))=0
 AND (SELECT count(*) FROM (SELECT rule,name_key FROM openalex.funders.dd1016_lookup GROUP BY ALL HAVING count(*)<>1))=0,'lookup re-sign changed more than the dropped targets: STOP, ROLLBACK') ok;
CREATE OR REPLACE TABLE openalex.funders.funder_name_lookup AS SELECT * FROM openalex.funders.dd1016_lookup;
-- F08 the readers' own runtime gate (CreateCrossrefWorkFunders / DataCite / FunderReported) must now pass.
SELECT assert_true(EXISTS(SELECT 1 FROM openalex.funders.funder_name_lookup WHERE catalogue_signature = (SELECT concat(count(*), char(58), cast(sum(cast(xxhash64(funder_id,display_name,alternate_titles,ror_id,doi,crossref_id,merge_into_id) AS DECIMAL(38,0))) AS STRING)) FROM openalex.funders.funders)),'readers would fail closed: STOP, ROLLBACK') ok;
SELECT tbl, n rows_before, dropped rows_removed FROM openalex.funders.dd1016_counts_before ORDER BY tbl;
