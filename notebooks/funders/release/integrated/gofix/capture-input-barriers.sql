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
