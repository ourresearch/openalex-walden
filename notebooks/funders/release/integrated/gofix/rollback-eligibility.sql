WITH live AS (SELECT 'openalex.awards.anr_work_funders' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.awards.anr_work_funders)
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
SELECT 'openalex.works.openalex_works_hash' relation,concat('version:',max(version)) fingerprint FROM (DESCRIBE HISTORY openalex.works.openalex_works_hash)), saved AS (SELECT * FROM openalex.funders.nm1013_integrated_input_barriers) SELECT assert_true(count_if(l.relation IS NULL OR b.relation IS NULL OR NOT(l.fingerprint<=>b.fingerprint))=0,'replay input state changed: STOP') FROM live l FULL JOIN saved b USING(relation);

-- EARLY ROLLBACK ONLY: before any resumed producer/nightly/served publication.
-- After the first night, stop: identity-aware inverse is a separate blocked gate.
-- Keep all backups, cuts, and audit tables. No table deletion.

SELECT assert_true(max(version)=(SELECT version FROM openalex.funders.nm1013_integrated_barrier_0),'downstream writer advanced: early rollback forbidden') FROM (DESCRIBE HISTORY openalex.awards.crossref_work_funders);

SELECT assert_true(max(version)=(SELECT version FROM openalex.funders.nm1013_integrated_barrier_1),'downstream writer advanced: early rollback forbidden') FROM (DESCRIBE HISTORY openalex.awards.datacite_work_funders);

SELECT assert_true(max(version)=(SELECT version FROM openalex.funders.nm1013_integrated_barrier_2),'downstream writer advanced: early rollback forbidden') FROM (DESCRIBE HISTORY openalex.awards.funder_reported_work_funders);

SELECT assert_true(max(version)=(SELECT version FROM openalex.funders.nm1013_integrated_barrier_3),'downstream writer advanced: early rollback forbidden') FROM (DESCRIBE HISTORY openalex.awards.work_awards);

SELECT assert_true(max(version)=(SELECT version FROM openalex.funders.nm1013_integrated_barrier_4),'downstream writer advanced: early rollback forbidden') FROM (DESCRIBE HISTORY openalex.awards.awards_api);

SELECT assert_true(max(version)=(SELECT version FROM openalex.funders.nm1013_integrated_barrier_5),'downstream writer advanced: early rollback forbidden') FROM (DESCRIBE HISTORY openalex.awards.award_entities);

SELECT assert_true(max(version)=(SELECT version FROM openalex.funders.nm1013_integrated_barrier_6),'downstream writer advanced: early rollback forbidden') FROM (DESCRIBE HISTORY openalex.awards.award_keys);

SELECT assert_true(max(version)=(SELECT version FROM openalex.funders.nm1013_integrated_barrier_7),'downstream writer advanced: early rollback forbidden') FROM (DESCRIBE HISTORY openalex.works.openalex_works);

SELECT assert_true(max(version)=(SELECT version FROM openalex.funders.nm1013_integrated_barrier_8),'downstream writer advanced: early rollback forbidden') FROM (DESCRIBE HISTORY openalex.funders.funders_api);

SELECT assert_true(count(*)=0,'funder outside frozen before/after states: reprove inverse') FROM (SELECT * FROM openalex.funders.funder EXCEPT ALL (SELECT * FROM openalex.funders.nm1013_integrated_registry_after UNION ALL SELECT * FROM openalex.funders.nm1013_integrated_registry_before));

SELECT assert_true((SELECT count(*) FROM openalex.funders.funder)=(SELECT count(*) FROM openalex.funders.nm1013_integrated_registry_before),'row count changed: reprove inverse');

SELECT assert_true(count(*)=0,'funder_id outside frozen before/after states: reprove inverse') FROM (SELECT * FROM openalex.funders.funder_id EXCEPT ALL (SELECT * FROM openalex.funders.nm1013_integrated_identifiers_after UNION ALL SELECT * FROM openalex.funders.nm1013_integrated_identifiers_before));

SELECT assert_true((SELECT count(*) FROM openalex.funders.funder_id)=(SELECT count(*) FROM openalex.funders.nm1013_integrated_identifiers_before),'row count changed: reprove inverse');

SELECT assert_true(count(*)=0,'funders_compat outside frozen before/after states: reprove inverse') FROM (SELECT * EXCEPT(merge_into_id),merge_into_id FROM openalex.funders.funders_compat EXCEPT ALL (SELECT * EXCEPT(merge_into_id),merge_into_id FROM openalex.funders.nm1013_integrated_catalogue_after UNION ALL SELECT * EXCEPT(merge_into_id),merge_into_id FROM openalex.funders.nm1013_integrated_catalogue_before));

SELECT assert_true((SELECT count(*) FROM openalex.funders.funders_compat)=(SELECT count(*) FROM openalex.funders.nm1013_integrated_catalogue_before),'row count changed: reprove inverse');

SELECT assert_true(max(version)=41,'retained original advanced: STOP') FROM (DESCRIBE HISTORY openalex.funders.funders_v41_table);
SELECT assert_true(count(*)=0,'retained original bytes changed: STOP') FROM ((SELECT * FROM openalex.funders.funders_v41_table EXCEPT ALL SELECT * FROM openalex.funders.funders_v41_clone) UNION ALL (SELECT * FROM openalex.funders.funders_v41_clone EXCEPT ALL SELECT * FROM openalex.funders.funders_v41_table));
SELECT assert_true(count(*)=1,'served name is not expected compat view: STOP') FROM openalex.information_schema.views WHERE table_schema='funders' AND table_name='funders' AND lower(regexp_replace(view_definition,'[`;\\s]',''))='select*fromopenalex.funders.funders_compat';

-- Zero merges: there is no registry/identifier/compat mutation to undo.
SELECT assert_true(count(*)=0,'no-merge rollback requires exact funder before-image') FROM ((SELECT * FROM openalex.funders.funder EXCEPT ALL SELECT * FROM openalex.funders.nm1013_integrated_registry_before) UNION ALL (SELECT * FROM openalex.funders.nm1013_integrated_registry_before EXCEPT ALL SELECT * FROM openalex.funders.funder));
SELECT assert_true(count(*)=0,'no-merge rollback requires exact funder_id before-image') FROM ((SELECT * FROM openalex.funders.funder_id EXCEPT ALL SELECT * FROM openalex.funders.nm1013_integrated_identifiers_before) UNION ALL (SELECT * FROM openalex.funders.nm1013_integrated_identifiers_before EXCEPT ALL SELECT * FROM openalex.funders.funder_id));
SELECT assert_true(count(*)=0,'no-merge rollback requires exact funders_compat before-image') FROM ((SELECT * FROM openalex.funders.funders_compat EXCEPT ALL SELECT * FROM openalex.funders.nm1013_integrated_catalogue_before) UNION ALL (SELECT * FROM openalex.funders.nm1013_integrated_catalogue_before EXCEPT ALL SELECT * FROM openalex.funders.funders_compat));
