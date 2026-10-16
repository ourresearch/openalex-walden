-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- merged notebook cell 37
SELECT assert_true(count(*)=0,'non-seed funder has a FundRef key') c_no_mint_fundref FROM openalex.funders.funder_id i LEFT ANTI JOIN openalex.funders.funder_source_record s ON s.source='openalex_v41' AND s.source_key=CAST(i.funder_id AS STRING) WHERE i.id_type='fundref';
