-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- stop on served-version drift
SELECT assert_true(max(version)=41,'served version changed: restage') FROM (DESCRIBE HISTORY openalex.funders.funders);
