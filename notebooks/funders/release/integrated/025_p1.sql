-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- baseline / bridge gates
SELECT assert_true((SELECT count(*) FROM openalex.funders.ror_successor_baseline)=2269,'baseline drift'),assert_true((SELECT count(*) FROM openalex.funders.fundref_predecessor_bridge)=66,'bridge drift'),assert_true((SELECT count(*) FROM openalex.funders.funder_decision WHERE action='AUTO_MERGE_ROR_SUCCESSOR')=0,'unexpected baseline merge');
