-- Investigator authors (oxjob #1847): the one write to the served table (Nightly Grants reads it as extra input investigator_authors),
-- keyed by grant slot; an unchanged row is not rewritten and a slot no longer served is deleted.
MERGE INTO __P__authors t
USING to_serve s
ON t.award_id = s.award_id AND t.slot = s.slot AND t.position = s.position
WHEN MATCHED AND (t.author_id <> s.author_id OR NOT (t.given_name <=> s.given_name) OR NOT (t.family_name <=> s.family_name)
  OR NOT (t.orcid <=> s.orcid)
  OR t.method <> s.method OR t.layer <> s.layer OR NOT (t.evidence <=> s.evidence) OR NOT (t.model_version <=> s.model_version))
  THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE
;
SELECT count(*) AS served, count(DISTINCT award_id) AS awards, count(DISTINCT author_id) AS authors FROM __P__authors
