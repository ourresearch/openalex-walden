-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- serve the preserved 22-column compat relation
CREATE VIEW openalex.funders.funders AS SELECT * FROM openalex.funders.funders_compat;
