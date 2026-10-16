-- REVIEW ONLY; owner Rohan; each production action requires fresh authorization.
-- datacite TABLE
-- Immutable deployment cut. Never replace this table during ordinary runs.
-- Initial execution freezes existing references; later new/changed content is forward input.
CREATE TABLE openalex.awards.datacite_funder_reference_cut
USING delta AS SELECT DISTINCT reference_key FROM datacite_funder_references;
