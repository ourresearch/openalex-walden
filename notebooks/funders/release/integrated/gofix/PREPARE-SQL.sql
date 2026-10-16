SELECT assert_true(current_user()=(SELECT table_owner FROM openalex.information_schema.tables WHERE table_schema='funders' AND table_name='funders'),'same-owner executor required; ownership transfer is out of scope');
-- REVIEW ONLY. Run in ONE persistent SQL session, stop on the first error.
-- FILE 000_p0.sql
-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- served version: substitute <N> only after the fresh freeze/read
DESCRIBE HISTORY openalex.funders.funders LIMIT 1;

-- FILE 001_p0.sql
-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- served shape
DESCRIBE openalex.funders.funders;

-- FILE 002_p0.sql
-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- require unswapped table
SELECT assert_true(count(*)=1,'served name must be a table') FROM openalex.information_schema.tables WHERE table_schema='funders' AND table_name='funders' AND table_type<>'VIEW';

-- FILE 003_p0.sql
-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- new relations must be absent
SELECT assert_true(count(*)=0,'cutover name collision') FROM openalex.information_schema.tables WHERE table_schema='funders' AND table_name IN ('funders_api_cutover_check','funder','funder_id','funder_source_record','funder_decision','f_ids_ever_issued','ror_successor_baseline','fundref_predecessor_bridge','funders_compat','funders_v41_clone','funders_v41_table','funders_api_pre_cutover','funders_api_hash_pre_cutover');

-- FILE gofix/capture-input-barriers.sql
CREATE TABLE openalex.funders.nm1013_integrated_input_barriers AS
SELECT 'openalex.awards.anr_work_funders' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.anr_work_funders)
UNION ALL
SELECT 'openalex.awards.award_bindings_last' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.award_bindings_last)
UNION ALL
SELECT 'openalex.awards.award_entities' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.award_entities)
UNION ALL
SELECT 'openalex.awards.award_funders_basis' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.award_funders_basis)
UNION ALL
SELECT 'openalex.awards.award_id_aliases' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.award_id_aliases)
UNION ALL
SELECT 'openalex.awards.award_id_guard' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.award_id_guard)
UNION ALL
SELECT 'openalex.awards.award_keys' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.award_keys)
UNION ALL
SELECT 'openalex.awards.award_merge_doi_pairs' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.award_merge_doi_pairs)
UNION ALL
SELECT 'openalex.awards.award_migration_manifest' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.award_migration_manifest)
UNION ALL
SELECT 'openalex.awards.award_nightly_runs' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.award_nightly_runs)
UNION ALL
SELECT 'openalex.awards.award_relations' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.award_relations)
UNION ALL
SELECT 'openalex.awards.award_relations_raw' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.award_relations_raw)
UNION ALL
SELECT 'openalex.awards.award_relink_map' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.award_relink_map)
UNION ALL
SELECT 'openalex.awards.award_state_transitions' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.award_state_transitions)
UNION ALL
SELECT 'openalex.awards.award_topics' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.award_topics)
UNION ALL
SELECT 'openalex.awards.awards_api' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.awards_api)
UNION ALL
SELECT 'openalex.awards.cr1303_work_funder_links_frozen' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.cr1303_work_funder_links_frozen)
UNION ALL
SELECT 'openalex.awards.crossref_awards' relation,concat('rows:',count(*),':',cast(sum(cast(xxhash64(to_json(struct(*))) AS DECIMAL(38,0))) AS STRING)) fingerprint FROM openalex.awards.crossref_awards
UNION ALL
SELECT 'openalex.awards.crossref_grants_deduplicated' relation,concat('rows:',count(*),':',cast(sum(cast(xxhash64(to_json(struct(*))) AS DECIMAL(38,0))) AS STRING)) fingerprint FROM openalex.awards.crossref_grants_deduplicated
UNION ALL
SELECT 'openalex.awards.crossref_work_funders' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.crossref_work_funders)
UNION ALL
SELECT 'openalex.awards.datacite_award_items_deduplicated' relation,concat('rows:',count(*),':',cast(sum(cast(xxhash64(to_json(struct(*))) AS DECIMAL(38,0))) AS STRING)) fingerprint FROM openalex.awards.datacite_award_items_deduplicated
UNION ALL
SELECT 'openalex.awards.datacite_awards' relation,concat('rows:',count(*),':',cast(sum(cast(xxhash64(to_json(struct(*))) AS DECIMAL(38,0))) AS STRING)) fingerprint FROM openalex.awards.datacite_awards
UNION ALL
SELECT 'openalex.awards.datacite_work_funders' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.datacite_work_funders)
UNION ALL
SELECT 'openalex.awards.europepmc_work_funders' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.europepmc_work_funders)
UNION ALL
SELECT 'openalex.awards.funder_reported_work_funders' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.funder_reported_work_funders)
UNION ALL
SELECT 'openalex.awards.gtr_awards' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.gtr_awards)
UNION ALL
SELECT 'openalex.awards.hakai_work_funders' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.hakai_work_funders)
UNION ALL
SELECT 'openalex.awards.kaken_projects_raw' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.kaken_projects_raw)
UNION ALL
SELECT 'openalex.awards.kaken_work_funders' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.kaken_work_funders)
UNION ALL
SELECT 'openalex.awards.nwo_work_funders' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.nwo_work_funders)
UNION ALL
SELECT 'openalex.awards.openalex_awards' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.openalex_awards)
UNION ALL
SELECT 'openalex.awards.openalex_awards_raw' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.openalex_awards_raw)
UNION ALL
SELECT 'openalex.awards.work_awards' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.work_awards)
UNION ALL
SELECT 'openalex.common.funder_names_keep' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.common.funder_names_keep)
UNION ALL
SELECT 'openalex.funders.funders_api' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.funders.funders_api)
UNION ALL
SELECT 'openalex.institutions.affiliation_matcher_answers' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.institutions.affiliation_matcher_answers)
UNION ALL
SELECT 'openalex.institutions.affiliation_strings_lookup' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.institutions.affiliation_strings_lookup)
UNION ALL
SELECT 'openalex.institutions.institutions_api' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.institutions.institutions_api)
UNION ALL
SELECT 'openalex.mid.work_funder' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.mid.work_funder)
UNION ALL
SELECT 'openalex.pdf.grobid_award_matches' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.pdf.grobid_award_matches)
UNION ALL
SELECT 'openalex.works.fulltext_work_funders' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.works.fulltext_work_funders)
UNION ALL
SELECT 'openalex.works.locations_mapped' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.works.locations_mapped)
UNION ALL
SELECT 'openalex.works.merged_work_ids' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.works.merged_work_ids)
UNION ALL
SELECT 'openalex.works.openalex_works' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.works.openalex_works)
UNION ALL
SELECT 'openalex.works.openalex_works_hash' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.works.openalex_works_hash);

-- FILE gofix/capture-barriers.sql
-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- Record openalex.awards.crossref_work_funders
CREATE TABLE openalex.funders.nm1013_integrated_barrier_0 AS SELECT version FROM (DESCRIBE HISTORY openalex.awards.crossref_work_funders LIMIT 1);

-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- Record openalex.awards.datacite_work_funders
CREATE TABLE openalex.funders.nm1013_integrated_barrier_1 AS SELECT version FROM (DESCRIBE HISTORY openalex.awards.datacite_work_funders LIMIT 1);

-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- Record openalex.awards.funder_reported_work_funders
CREATE TABLE openalex.funders.nm1013_integrated_barrier_2 AS SELECT version FROM (DESCRIBE HISTORY openalex.awards.funder_reported_work_funders LIMIT 1);

-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- Record openalex.awards.work_awards
CREATE TABLE openalex.funders.nm1013_integrated_barrier_3 AS SELECT version FROM (DESCRIBE HISTORY openalex.awards.work_awards LIMIT 1);

-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- Record openalex.awards.awards_api
CREATE TABLE openalex.funders.nm1013_integrated_barrier_4 AS SELECT version FROM (DESCRIBE HISTORY openalex.awards.awards_api LIMIT 1);

-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- Record openalex.awards.award_entities
CREATE TABLE openalex.funders.nm1013_integrated_barrier_5 AS SELECT version FROM (DESCRIBE HISTORY openalex.awards.award_entities LIMIT 1);

-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- Record openalex.awards.award_keys
CREATE TABLE openalex.funders.nm1013_integrated_barrier_6 AS SELECT version FROM (DESCRIBE HISTORY openalex.awards.award_keys LIMIT 1);

-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- Record openalex.works.openalex_works
CREATE TABLE openalex.funders.nm1013_integrated_barrier_7 AS SELECT version FROM (DESCRIBE HISTORY openalex.works.openalex_works LIMIT 1);

-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- Record openalex.funders.funders_api
CREATE TABLE openalex.funders.nm1013_integrated_barrier_8 AS SELECT version FROM (DESCRIBE HISTORY openalex.funders.funders_api LIMIT 1);
