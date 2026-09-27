-- Institutions Frontfill, last task: refresh the per-string institution ids every work reads.
-- Definition: raw_affiliation_strings_institutions_mv.sql (this task only refreshes it).

-- oxjob #1386: the MV LEFT JOINs affiliation_matcher_answers on the string, so two rows for one string
-- would duplicate that string's affiliations in every work. Fail here, before anything reads it.
SELECT
  CASE WHEN COUNT(*) <> COUNT(DISTINCT raw_affiliation_string)
       THEN RAISE_ERROR(CONCAT('affiliation_matcher_answers has ', COUNT(*) - COUNT(DISTINCT raw_affiliation_string),
                               ' duplicate strings: dedupe it before refreshing raw_affiliation_strings_institutions_mv (oxjob #1386)'))
  END AS duplicate_check
FROM openalex.institutions.affiliation_matcher_answers;

REFRESH MATERIALIZED VIEW openalex.institutions.raw_affiliation_strings_institutions_mv;
