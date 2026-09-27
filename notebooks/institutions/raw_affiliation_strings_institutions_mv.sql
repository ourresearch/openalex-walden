-- raw_affiliation_strings_institutions_mv: one row per affiliation string, the institution ids every
-- work carrying that string gets (work_author_affiliations_mv -> work_authorships -> openalex_works).
--
-- This file is the definition. The nightly refresh (jobs/institutions_frontfill.yaml) only runs
-- REFRESH, so an edit here reaches production when someone runs this statement once:
--   scripts/affiliation_matcher_swap.py define-mv
-- (oxjob #1386 moved it into the repo; before that it lived only in the MV itself.)
--
-- Base ids, per string:
--   1. openalex.institutions.affiliation_matcher_answers when it has a row for the string
--      (the retrieve -> decide -> choose matcher, oxjobs #1363/#1385/#1386; [] = names no institution);
--   2. otherwise the legacy answer: hand-rules override if non-empty, else the TF model.
-- Then curations: (base + curated adds) - curated removes. A string answered by the new matcher takes
-- the curations without the curation bot's (ras_curations_without_bot, Jason 2026-09-27: the matcher
-- is the single source, librarian curations keep winning); a legacy string keeps every curation,
-- since the bot's were corrections to the legacy output.
--
-- With affiliation_matcher_answers empty, every column equals the pre-#1386 definition exactly.
-- Output columns are unchanged: RefreshRasWorksCounts and the raw-affiliation-strings ES index read
-- them. For matcher strings, model_institution_ids carries the matcher's ids and
-- institution_ids_override is [].
CREATE OR REPLACE MATERIALIZED VIEW openalex.institutions.raw_affiliation_strings_institutions_mv
CLUSTER BY (raw_affiliation_string)
AS
SELECT
  asl.raw_affiliation_string,
  ARRAY_EXCEPT(
    ARRAY_UNION(
      FILTER(
        CASE
          WHEN ans.raw_affiliation_string IS NOT NULL THEN COALESCE(ans.institution_ids, array())
          WHEN asl.institution_ids_override != array() THEN asl.institution_ids_override
          WHEN SIZE(asl.institution_ids) > 0 AND asl.institution_ids[0] IS NULL THEN array()
          ELSE COALESCE(asl.institution_ids, array())
        END,
        x -> x IS NOT NULL AND x != -1
      ),
      COALESCE(CASE WHEN ans.raw_affiliation_string IS NOT NULL THEN rnb.curated_add_ids ELSE rac.curated_add_ids END, array())
    ),
    COALESCE(CASE WHEN ans.raw_affiliation_string IS NOT NULL THEN rnb.curated_remove_ids ELSE rac.curated_remove_ids END, array())
  ) AS institution_ids,
  -- Read by the authorships country fallback only when every matched institution lacks a country code.
  CASE
    WHEN ans.raw_affiliation_string IS NOT NULL AND SIZE(ans.countries) > 0 THEN ans.countries
    WHEN COALESCE(asl.countries, array()) = array('') THEN array()
    ELSE COALESCE(asl.countries, array())
  END AS countries,
  CASE WHEN ans.raw_affiliation_string IS NOT NULL THEN 'matcher' ELSE asl.source END AS source,
  CASE WHEN ans.raw_affiliation_string IS NOT NULL THEN ans.institution_ids ELSE asl.institution_ids END AS model_institution_ids,
  CASE WHEN ans.raw_affiliation_string IS NOT NULL THEN CAST(array() AS ARRAY<BIGINT>) ELSE asl.institution_ids_override END AS institution_ids_override,
  CASE WHEN ans.raw_affiliation_string IS NOT NULL THEN rnb.curated_add_ids ELSE rac.curated_add_ids END AS curated_add_ids,
  CASE WHEN ans.raw_affiliation_string IS NOT NULL THEN rnb.curated_remove_ids ELSE rac.curated_remove_ids END AS curated_remove_ids,
  asl.created_datetime,
  CASE WHEN ans.raw_affiliation_string IS NOT NULL THEN ans.run_at ELSE asl.updated_datetime END AS updated_datetime
FROM openalex.institutions.affiliation_strings_lookup asl
LEFT JOIN openalex.institutions.affiliation_matcher_answers ans
  ON asl.raw_affiliation_string = ans.raw_affiliation_string
LEFT JOIN openalex.institutions.ras_curations rac
  ON asl.raw_affiliation_string = rac.raw_affiliation_string
LEFT JOIN openalex.institutions.ras_curations_without_bot rnb
  ON asl.raw_affiliation_string = rnb.raw_affiliation_string
