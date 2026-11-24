-- #1348 Vector_Cards: the seats MatchAuthors will match tonight (same predicate as MatchAuthors cell 7 needs_matching) with card components.
DECLARE OR REPLACE VARIABLE max_updated_date TIMESTAMP DEFAULT to_timestamp('1900-01-01');
-- oxjob #1642 (21 Nov 2026): the same watermark as MatchAuthors cell 1. Rows stamped after the newest Works_Base stamp don't
-- count: #2133's byline re-seat step stamps its moved rows with current_timestamp(), and night 11-21's Vector_Cards then
-- picked 15 seats instead of ~800K (MatchAuthors, guarded since 10 Nov, matched them all). Same value on a normal night.
SET VARIABLE max_updated_date = COALESCE((SELECT MAX(updated_at) - INTERVAL 1 SECOND FROM openalex.works.work_authors
                                          WHERE updated_at <= (SELECT MAX(updated_date) FROM openalex.works.openalex_works_base)),
                                         to_timestamp('1900-01-01'));
CREATE OR REPLACE TABLE openalex.authors.vector_match_cards AS
-- oxjob #2342 (23 Nov 2026): the seats are joined to work_authors without the work's whole authorships array. Each seat row
-- used to carry it into the (work_id, author_sequence) shuffle, so a byline of n seats shuffled n copies of n authorships:
-- night 11-23 spilled 642 GB there and Vector_Cards took 17 min (2-6 min before). The co-author pick reads a per-work array
-- of (author_order_number, raw_author_name) joined on work_id after the filter. Same expressions, same output.
WITH base AS (
  SELECT id AS work_id, created_date, publication_year, substr(title, 1, 120) AS title,
         COALESCE(primary_location.source.display_name, primary_location.raw_source_name) AS venue, primary_topic.subfield.display_name AS subfield, authorships
  FROM openalex.works.openalex_works_base
  WHERE (updated_date > max_updated_date OR id IN (SELECT work_id FROM openalex.authors.author_rematch_worklist)) AND authorships IS NOT NULL AND SIZE(authorships) > 0),
raw_exploded AS (
  SELECT work_id, created_date, POSEXPLODE(authorships) AS (author_sequence, authorship) FROM base),
nm AS (
  SELECT r.* , wa.raw_affiliation_strings AS wa_aff
  FROM raw_exploded r INNER JOIN openalex.works.work_authors wa ON r.work_id = wa.work_id AND r.author_sequence = wa.author_sequence
  WHERE wa.author_id IS NULL AND ((r.work_id > 7000000000 AND r.created_date >= to_timestamp('2025-12-20')) OR r.work_id IN (SELECT work_id FROM openalex.authors.author_rematch_worklist))
    AND (openalex.authors.placeholder_author_name(r.authorship.raw_author_name) IS NULL OR NULLIF(TRIM(r.authorship.raw_orcid), '') IS NOT NULL)),  -- oxjob #1881
names AS (
  SELECT work_id, publication_year, title, venue, subfield,
         TRANSFORM(authorships, o -> named_struct('author_order_number', o.author_order_number, 'raw_author_name', o.raw_author_name)) AS authorships
  FROM base)
SELECT nm.work_id, nm.author_sequence, CAST(NULL AS BIGINT) AS author_id, authorship.raw_author_name AS raw_name,
  COALESCE(slice(FILTER(authorship.raw_affiliation_strings, x -> x IS NOT NULL AND x <> ''), 1, 2), slice(FILTER(wa_aff, x -> x IS NOT NULL AND x <> ''), 1, 2)) AS aff_strings,
  FILTER(TRANSFORM(authorship.institutions, i -> i.display_name), x -> x IS NOT NULL) AS inst_names, w.subfield, w.venue, w.publication_year,
  slice(TRANSFORM(array_sort(TRANSFORM(FILTER(w.authorships, o -> o.author_order_number <> nm.author_sequence AND o.raw_author_name IS NOT NULL AND o.raw_author_name <> ''),
           o -> struct(abs(o.author_order_number - nm.author_sequence) AS d, o.raw_author_name AS n))), x -> x.n), 1, 5) AS coauthors, w.title
FROM nm JOIN names w ON w.work_id = nm.work_id;
