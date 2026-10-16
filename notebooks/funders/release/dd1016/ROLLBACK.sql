-- dd1016-b3 inverse. REVIEW ONLY; owner Rohan. Reverses only what FORWARD changed, row by row, for the staged drop set of batch dd1016-b3:
-- registry columns of the 2,191 rows, their catalogue rows, the removed link rows, the removed lookup rows and the ledger rows.
-- Safe after any FORWARD step and after any number of Funders Daily / reader / Nightly runs: nothing outside the drop set is
-- compared to or restored from a before image; every gate checks that ROLLBACK itself changed nothing else. Re-runnable.
SELECT assert_true((SELECT count(*) FROM openalex.funders.dd1016_drop)=2191,'staged drop set missing or wrong: STOP') ok;
-- pre-ROLLBACK images, part of R00 (replaced on every run).
CREATE OR REPLACE TABLE openalex.funders.dd1016_rb_registry_pre AS SELECT * FROM openalex.funders.funder;
CREATE OR REPLACE TABLE openalex.funders.dd1016_rb_catalogue_pre AS SELECT * FROM openalex.funders.funders;
CREATE OR REPLACE TABLE openalex.funders.dd1016_rb_lookup_pre AS SELECT * FROM openalex.funders.funder_name_lookup;
-- Stop if anything other than FORWARD has since changed a drop-set row's withdrawn state (needs a person, not this script).
SELECT assert_true(count(*)=0,'a drop-set registry row was changed after FORWARD by something else: STOP, review by hand') ok
FROM openalex.funders.funder t JOIN openalex.funders.dd1016_registry_before b ON b.funder_id = t.funder_id
WHERE t.funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop) AND NOT (t.status = 'withdrawn' AND t.run_id = 'dd1016')
 AND NOT (named_struct('s',t.status,'r',t.ror_id,'p',t.ror_primary,'u',t.updated_date,'i',t.run_id) <=> named_struct('s',b.status,'r',b.ror_id,'p',b.ror_primary,'u',b.updated_date,'i',b.run_id));
-- R01 registry: the five FORWARD columns of rows still in FORWARD's state go back to their before values.
MERGE INTO openalex.funders.funder t USING (SELECT * FROM openalex.funders.dd1016_registry_before WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)) b ON t.funder_id = b.funder_id
WHEN MATCHED AND t.status = 'withdrawn' AND t.run_id = 'dd1016' THEN UPDATE SET t.status = b.status, t.ror_id = b.ror_id, t.ror_primary = b.ror_primary,
 t.updated_date = b.updated_date, t.run_id = b.run_id;
SELECT assert_true((SELECT count(*) FROM ((SELECT * FROM openalex.funders.funder WHERE funder_id NOT IN (SELECT funder_id FROM openalex.funders.dd1016_drop) EXCEPT ALL SELECT * FROM openalex.funders.dd1016_rb_registry_pre WHERE funder_id NOT IN (SELECT funder_id FROM openalex.funders.dd1016_drop))
   UNION ALL (SELECT * FROM openalex.funders.dd1016_rb_registry_pre WHERE funder_id NOT IN (SELECT funder_id FROM openalex.funders.dd1016_drop) EXCEPT ALL SELECT * FROM openalex.funders.funder WHERE funder_id NOT IN (SELECT funder_id FROM openalex.funders.dd1016_drop))))=0
 AND (SELECT count(*) FROM ((SELECT funder_id,status,ror_id,ror_primary,updated_date,run_id FROM openalex.funders.funder WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop) EXCEPT ALL SELECT funder_id,status,ror_id,ror_primary,updated_date,run_id FROM openalex.funders.dd1016_registry_before WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))
   UNION ALL (SELECT funder_id,status,ror_id,ror_primary,updated_date,run_id FROM openalex.funders.dd1016_registry_before WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop) EXCEPT ALL SELECT funder_id,status,ror_id,ror_primary,updated_date,run_id FROM openalex.funders.funder WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))))=0,
 'registry: drop-set rows not restored, or another row changed: STOP') ok;
-- R02 republish the catalogue (same statement as FORWARD F05): the drop-set rows come back, every other row is as it was before ROLLBACK.
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
SELECT assert_true((SELECT count(*) FROM ((SELECT * FROM openalex.funders.funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop) EXCEPT ALL SELECT * FROM openalex.funders.dd1016_catalogue_before WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))
   UNION ALL (SELECT * FROM openalex.funders.dd1016_catalogue_before WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop) EXCEPT ALL SELECT * FROM openalex.funders.funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop))))=0
 AND (SELECT count(*) FROM ((SELECT * FROM openalex.funders.funders WHERE funder_id NOT IN (SELECT funder_id FROM openalex.funders.dd1016_drop) EXCEPT ALL SELECT * FROM openalex.funders.dd1016_rb_catalogue_pre WHERE funder_id NOT IN (SELECT funder_id FROM openalex.funders.dd1016_drop))
   UNION ALL (SELECT * FROM openalex.funders.dd1016_rb_catalogue_pre WHERE funder_id NOT IN (SELECT funder_id FROM openalex.funders.dd1016_drop) EXCEPT ALL SELECT * FROM openalex.funders.funders WHERE funder_id NOT IN (SELECT funder_id FROM openalex.funders.dd1016_drop))))=0,
 'catalogue: drop-set rows not restored, or another row changed: STOP') ok;
-- R03 re-insert exactly the removed link rows that are still missing (row-scoped multiset).
INSERT INTO openalex.mid.work_funder SELECT * FROM (SELECT * FROM openalex.funders.dd1016_bk_mid_work_funder EXCEPT ALL SELECT * FROM openalex.mid.work_funder WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop));
INSERT INTO openalex.works.fulltext_work_funders SELECT * FROM (SELECT * FROM openalex.funders.dd1016_bk_fulltext_work_funders EXCEPT ALL SELECT * FROM openalex.works.fulltext_work_funders WHERE funder_id IN (SELECT concat('https://openalex.org/F', funder_id) FROM openalex.funders.dd1016_drop));
INSERT INTO openalex.awards.crossref_work_funders SELECT * FROM (SELECT * FROM openalex.funders.dd1016_bk_crossref_work_funders EXCEPT ALL SELECT * FROM openalex.awards.crossref_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop));
INSERT INTO openalex.awards.crossref_no_doi_main_cut SELECT * FROM (SELECT * FROM openalex.funders.dd1016_bk_crossref_no_doi_main_cut EXCEPT ALL SELECT * FROM openalex.awards.crossref_no_doi_main_cut WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop));
INSERT INTO openalex.awards.datacite_work_funders SELECT * FROM (SELECT * FROM openalex.funders.dd1016_bk_datacite_work_funders EXCEPT ALL SELECT * FROM openalex.awards.datacite_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop));
INSERT INTO openalex.awards.europepmc_work_funders SELECT * FROM (SELECT * FROM openalex.funders.dd1016_bk_europepmc_work_funders EXCEPT ALL SELECT * FROM openalex.awards.europepmc_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop));
INSERT INTO openalex.awards.funder_reported_work_funders SELECT * FROM (SELECT * FROM openalex.funders.dd1016_bk_funder_reported_work_funders EXCEPT ALL SELECT * FROM openalex.awards.funder_reported_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop));
INSERT INTO openalex.awards.hakai_work_funders SELECT * FROM (SELECT * FROM openalex.funders.dd1016_bk_hakai_work_funders EXCEPT ALL SELECT * FROM openalex.awards.hakai_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop));
INSERT INTO openalex.awards.anr_work_funders SELECT * FROM (SELECT * FROM openalex.funders.dd1016_bk_anr_work_funders EXCEPT ALL SELECT * FROM openalex.awards.anr_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop));
INSERT INTO openalex.awards.kaken_work_funders SELECT * FROM (SELECT * FROM openalex.funders.dd1016_bk_kaken_work_funders EXCEPT ALL SELECT * FROM openalex.awards.kaken_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop));
INSERT INTO openalex.awards.nwo_work_funders SELECT * FROM (SELECT * FROM openalex.funders.dd1016_bk_nwo_work_funders EXCEPT ALL SELECT * FROM openalex.awards.nwo_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop));
INSERT INTO openalex.funders.funder_match_policy SELECT * FROM (SELECT * FROM openalex.funders.dd1016_bk_funder_match_policy EXCEPT ALL SELECT * FROM openalex.funders.funder_match_policy WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop));
INSERT INTO openalex.common.funder_names_keep SELECT * FROM (SELECT * FROM openalex.funders.dd1016_bk_funder_names_keep EXCEPT ALL SELECT * FROM openalex.common.funder_names_keep WHERE id IN (SELECT concat('https://openalex.org/F', funder_id) FROM openalex.funders.dd1016_drop));
SELECT assert_true((SELECT count(*) FROM (SELECT * FROM openalex.funders.dd1016_bk_mid_work_funder EXCEPT ALL SELECT * FROM openalex.mid.work_funder WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)))=0,'mid_work_funder not restored: STOP') ok;
SELECT assert_true((SELECT count(*) FROM (SELECT * FROM openalex.funders.dd1016_bk_fulltext_work_funders EXCEPT ALL SELECT * FROM openalex.works.fulltext_work_funders WHERE funder_id IN (SELECT concat('https://openalex.org/F', funder_id) FROM openalex.funders.dd1016_drop)))=0,'fulltext_work_funders not restored: STOP') ok;
SELECT assert_true((SELECT count(*) FROM (SELECT * FROM openalex.funders.dd1016_bk_crossref_work_funders EXCEPT ALL SELECT * FROM openalex.awards.crossref_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)))=0,'crossref_work_funders not restored: STOP') ok;
SELECT assert_true((SELECT count(*) FROM (SELECT * FROM openalex.funders.dd1016_bk_crossref_no_doi_main_cut EXCEPT ALL SELECT * FROM openalex.awards.crossref_no_doi_main_cut WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)))=0,'crossref_no_doi_main_cut not restored: STOP') ok;
SELECT assert_true((SELECT count(*) FROM (SELECT * FROM openalex.funders.dd1016_bk_datacite_work_funders EXCEPT ALL SELECT * FROM openalex.awards.datacite_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)))=0,'datacite_work_funders not restored: STOP') ok;
SELECT assert_true((SELECT count(*) FROM (SELECT * FROM openalex.funders.dd1016_bk_europepmc_work_funders EXCEPT ALL SELECT * FROM openalex.awards.europepmc_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)))=0,'europepmc_work_funders not restored: STOP') ok;
SELECT assert_true((SELECT count(*) FROM (SELECT * FROM openalex.funders.dd1016_bk_funder_reported_work_funders EXCEPT ALL SELECT * FROM openalex.awards.funder_reported_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)))=0,'funder_reported_work_funders not restored: STOP') ok;
SELECT assert_true((SELECT count(*) FROM (SELECT * FROM openalex.funders.dd1016_bk_hakai_work_funders EXCEPT ALL SELECT * FROM openalex.awards.hakai_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)))=0,'hakai_work_funders not restored: STOP') ok;
SELECT assert_true((SELECT count(*) FROM (SELECT * FROM openalex.funders.dd1016_bk_anr_work_funders EXCEPT ALL SELECT * FROM openalex.awards.anr_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)))=0,'anr_work_funders not restored: STOP') ok;
SELECT assert_true((SELECT count(*) FROM (SELECT * FROM openalex.funders.dd1016_bk_kaken_work_funders EXCEPT ALL SELECT * FROM openalex.awards.kaken_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)))=0,'kaken_work_funders not restored: STOP') ok;
SELECT assert_true((SELECT count(*) FROM (SELECT * FROM openalex.funders.dd1016_bk_nwo_work_funders EXCEPT ALL SELECT * FROM openalex.awards.nwo_work_funders WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)))=0,'nwo_work_funders not restored: STOP') ok;
SELECT assert_true((SELECT count(*) FROM (SELECT * FROM openalex.funders.dd1016_bk_funder_match_policy EXCEPT ALL SELECT * FROM openalex.funders.funder_match_policy WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)))=0,'funder_match_policy not restored: STOP') ok;
SELECT assert_true((SELECT count(*) FROM (SELECT * FROM openalex.funders.dd1016_bk_funder_names_keep EXCEPT ALL SELECT * FROM openalex.common.funder_names_keep WHERE id IN (SELECT concat('https://openalex.org/F', funder_id) FROM openalex.funders.dd1016_drop)))=0,'funder_names_keep not restored: STOP') ok;
-- R04 lookup: the current lookup plus the rows FORWARD removed (unless a later release now owns that rule/name), re-signed for the
-- restored catalogue with the F07 expression. The readers' gate is checked right after.
CREATE OR REPLACE TABLE openalex.funders.dd1016_rb_lookup AS SELECT x.rule, x.name_key, x.funder_id, 'openalex.funders.dd1016_rollback_' release_id, s.catalogue_signature FROM (
 SELECT rule, name_key, funder_id FROM openalex.funders.dd1016_rb_lookup_pre
 UNION ALL SELECT b.rule, b.name_key, b.funder_id FROM openalex.funders.dd1016_lookup_before b LEFT ANTI JOIN openalex.funders.dd1016_rb_lookup_pre c ON c.rule = b.rule AND c.name_key = b.name_key
 WHERE b.funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)) x
CROSS JOIN (SELECT concat(count(*), char(58), cast(sum(cast(xxhash64(funder_id,display_name,alternate_titles,ror_id,doi,crossref_id,merge_into_id) AS DECIMAL(38,0))) AS STRING)) FROM openalex.funders.funders) s(catalogue_signature);
SELECT assert_true((SELECT count(*) FROM (SELECT rule,name_key FROM openalex.funders.dd1016_rb_lookup GROUP BY ALL HAVING count(*)<>1))=0
 AND (SELECT count(*) FROM (SELECT rule,name_key,funder_id FROM openalex.funders.dd1016_rb_lookup_pre EXCEPT ALL SELECT rule,name_key,funder_id FROM openalex.funders.dd1016_rb_lookup))=0,'lookup rebuild dropped or duplicated rows: STOP') ok;
SELECT count(*) restored_lookup_rows, (SELECT count(*) FROM openalex.funders.dd1016_lookup_before WHERE funder_id IN (SELECT funder_id FROM openalex.funders.dd1016_drop)) removed_by_forward FROM (SELECT rule,name_key,funder_id FROM openalex.funders.dd1016_rb_lookup EXCEPT ALL SELECT rule,name_key,funder_id FROM openalex.funders.dd1016_rb_lookup_pre);
CREATE OR REPLACE TABLE openalex.funders.funder_name_lookup AS SELECT * FROM openalex.funders.dd1016_rb_lookup;
SELECT assert_true(EXISTS(SELECT 1 FROM openalex.funders.funder_name_lookup WHERE catalogue_signature = (SELECT concat(count(*), char(58), cast(sum(cast(xxhash64(funder_id,display_name,alternate_titles,ror_id,doi,crossref_id,merge_into_id) AS DECIMAL(38,0))) AS STRING)) FROM openalex.funders.funders)),'readers would fail closed: STOP') ok;
-- R05 ledger last (CreateWorksEnriched and the funders ES sync read it).
DELETE FROM openalex.funders.deleted_funders WHERE batch = 'dd1016-b3';
SELECT assert_true(count(*)=0,'ledger rows remain') ok FROM openalex.funders.deleted_funders WHERE batch='dd1016-b3';
