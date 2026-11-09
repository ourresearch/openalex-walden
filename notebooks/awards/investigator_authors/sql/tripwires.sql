-- Investigator authors (oxjob #1847): tripwires on to_serve against what is served now; a count above 0 fails the run before the MERGE.
--   shrink  the new set is under 90% of what is served now (an input went missing)
--   churn   more than max_change of the served rows would be added, removed or re-pointed; each re-dates its award (1.0 on the go-live run)
SELECT CASE WHEN shrink + churn > 0
  THEN raise_error(concat('investigator authors tripwire: ', to_json(named_struct('shrink', shrink, 'churn', churn, 'rows', n_new,
    'served_now', n_now, 'changed', n_changed)))) END AS tripped,
  n_new, n_now, n_changed
FROM (
  SELECT c.*, CASE WHEN n_now > 0 AND n_new < 0.9 * n_now THEN 1 ELSE 0 END AS shrink,
    CASE WHEN n_now > 0 AND n_changed > __MAXCHG__ * n_now THEN 1 ELSE 0 END AS churn
  FROM (
    SELECT (SELECT count(*) FROM to_serve) AS n_new, (SELECT count(*) FROM __P__authors) AS n_now,
      (SELECT count(*) FROM to_serve t FULL OUTER JOIN __P__authors s ON s.award_id = t.award_id AND s.slot = t.slot AND s.position = t.position
         WHERE t.author_id IS NULL OR s.author_id IS NULL OR s.author_id <> t.author_id) AS n_changed
  ) c
) z
