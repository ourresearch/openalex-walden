-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- crossref TABLE
-- Immutable deployment cut. Never replace this table during ordinary runs.
-- Initial execution freezes existing references; later new/changed content is forward input.
CREATE TABLE openalex.awards.crossref_funder_reference_cut
USING delta AS SELECT DISTINCT reference_key FROM crossref_funder_references;
