-- #1348 Vector_Cards: the seats MatchAuthors will match tonight (same predicate as MatchAuthors cell 7 needs_matching) with card components.
DECLARE OR REPLACE VARIABLE max_updated_date TIMESTAMP DEFAULT to_timestamp('1900-01-01');
SET VARIABLE max_updated_date = COALESCE((SELECT MAX(updated_at) - INTERVAL 1 SECOND FROM openalex.works.work_authors), to_timestamp('1900-01-01'));
CREATE OR REPLACE TABLE openalex.authors.vector_match_cards AS
WITH raw_exploded AS (
  SELECT id AS work_id, created_date, publication_year, substr(title, 1, 120) AS title,
         COALESCE(primary_location.source.display_name, primary_location.raw_source_name) AS venue, primary_topic.subfield.display_name AS subfield, authorships,
         POSEXPLODE(authorships) AS (author_sequence, authorship)
  FROM openalex.works.openalex_works_base
  WHERE (updated_date > max_updated_date OR id IN (SELECT work_id FROM openalex.authors.author_rematch_worklist)) AND authorships IS NOT NULL AND SIZE(authorships) > 0),
nm AS (
  SELECT r.* , wa.raw_affiliation_strings AS wa_aff
  FROM raw_exploded r INNER JOIN openalex.works.work_authors wa ON r.work_id = wa.work_id AND r.author_sequence = wa.author_sequence
  WHERE wa.author_id IS NULL AND ((r.work_id > 7000000000 AND r.created_date >= to_timestamp('2025-12-20')) OR r.work_id IN (SELECT work_id FROM openalex.authors.author_rematch_worklist)))
SELECT work_id, author_sequence, CAST(NULL AS BIGINT) AS author_id, authorship.raw_author_name AS raw_name,
  COALESCE(slice(FILTER(authorship.raw_affiliation_strings, x -> x IS NOT NULL AND x <> ''), 1, 2), slice(FILTER(wa_aff, x -> x IS NOT NULL AND x <> ''), 1, 2)) AS aff_strings,
  FILTER(TRANSFORM(authorship.institutions, i -> i.display_name), x -> x IS NOT NULL) AS inst_names, subfield, venue, publication_year,
  slice(TRANSFORM(array_sort(TRANSFORM(FILTER(authorships, o -> o.author_order_number <> nm.author_sequence AND o.raw_author_name IS NOT NULL AND o.raw_author_name <> ''),
           o -> struct(abs(o.author_order_number - nm.author_sequence) AS d, o.raw_author_name AS n))), x -> x.n), 1, 5) AS coauthors, title
FROM nm;
