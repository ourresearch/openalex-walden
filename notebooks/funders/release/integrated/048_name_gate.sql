-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- Require first activation and post-cutover view
SELECT assert_true(count(*)=0,'live name lookup already exists: new combined review required') FROM openalex.information_schema.tables WHERE table_schema='funders' AND table_name='funder_name_lookup';
SELECT assert_true(count(*)=1,'served funders must be a view') FROM openalex.information_schema.tables WHERE table_schema='funders' AND table_name='funders' AND table_type='VIEW';
