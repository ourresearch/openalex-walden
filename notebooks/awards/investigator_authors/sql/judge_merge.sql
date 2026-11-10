-- Investigator authors (oxjob #1847): Judge_With_Jev keeps tonight's answers. jev_out (a temp view of the Jev calls: entry_id, list_hash,
-- decision, author_id, p, model) is MERGEd into __P__jev, one row per slot of each answered entry; the same answers again change
-- nothing. A failed or unasked entry has no row in jev_out: it keeps what it had and is asked again another night.
MERGE INTO __P__jev t
USING (
  SELECT i.award_id, s.slot, s.position, s.given_name, s.family_name, o.entry_id, o.list_hash, o.decision, o.author_id, o.p,
    DATE'__RUN__' AS judged_on, o.model
  FROM jev_out o JOIN __P__jev_items i ON i.entry_id = o.entry_id AND i.list_hash = o.list_hash
  LATERAL VIEW explode(i.slots) x AS s
) s
ON t.award_id = s.award_id AND t.slot = s.slot AND t.position = s.position
WHEN MATCHED AND (NOT (t.list_hash <=> s.list_hash) OR NOT (t.decision <=> s.decision) OR NOT (t.author_id <=> s.author_id)
  OR NOT (t.p <=> s.p) OR NOT (t.given_name <=> s.given_name) OR NOT (t.family_name <=> s.family_name) OR NOT (t.model <=> s.model))
  THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
;
-- A seeded pick (no list_hash yet) on a slot that still prints the judged name takes tonight's list, so a later change to that list is
-- asked about; its decision stays. A seed whose name changed is asked again instead (judge.sql).
MERGE INTO __P__jev t
USING (SELECT l.award_id, s.slot, s.position, s.given_name, s.family_name, l.entry_id, l.list_hash
       FROM __P__jev_lists l LATERAL VIEW explode(l.slots) x AS s) s
ON t.award_id = s.award_id AND t.slot = s.slot AND t.position = s.position AND t.list_hash IS NULL
  AND lower(trim(coalesce(t.given_name, ''))) = lower(trim(coalesce(s.given_name, ''))) AND lower(trim(t.family_name)) = lower(trim(s.family_name))
WHEN MATCHED THEN UPDATE SET list_hash = s.list_hash, entry_id = s.entry_id
;
-- A kept link whose profile another person on the award now holds (__P__jev_owned) is retired, so that person's own layer can serve
-- (decide.sql would otherwise hold both); its list_hash 'retired' matches no list, so the slot is asked again when it has one
MERGE INTO __P__jev t
USING (
  SELECT DISTINCT j.award_id, j.slot, j.position FROM __P__jev j
  JOIN __P__people p ON p.award_id = j.award_id AND p.slot = j.slot AND p.position = j.position
  JOIN __P__jev_owned o ON o.award_id = j.award_id AND o.author_id = j.author_id AND o.entry_id <> concat(p.award_id, '|', p.person_key)
  WHERE j.decision = 'link'
) s
ON t.award_id = s.award_id AND t.slot = s.slot AND t.position = s.position
WHEN MATCHED THEN UPDATE SET decision = 'unsure', author_id = NULL, list_hash = 'retired', judged_on = DATE'__RUN__'
;
SELECT decision, count(*) AS slots, max(judged_on) AS last_judged FROM __P__jev GROUP BY 1 ORDER BY 1
