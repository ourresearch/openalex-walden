-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- new relations must be absent
SELECT assert_true(count(*)=0,'cutover name collision') FROM openalex.information_schema.tables WHERE table_schema='funders' AND table_name IN ('funders_api_cutover_check','funder','funder_id','funder_source_record','funder_decision','f_ids_ever_issued','ror_successor_baseline','fundref_predecessor_bridge','funders_compat','funders_v41_clone','funders_v41_table','funders_api_pre_cutover','funders_api_hash_pre_cutover');
