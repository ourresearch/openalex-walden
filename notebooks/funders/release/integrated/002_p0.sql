-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- require unswapped table
SELECT assert_true(count(*)=1,'served name must be a table') FROM openalex.information_schema.tables WHERE table_schema='funders' AND table_name='funders' AND table_type<>'VIEW';
