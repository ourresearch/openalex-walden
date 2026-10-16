-- Cutover morning, after fold-ship rebuilds fs1012_prefold_links from the prior-night publication.
-- Bind control_prefix and candidate_prefix to NEW same-cut replay namespaces ending before_/after_.
-- Never bind retained ff1013_ig_ shadows against a changed baseline and call it a fresh replay.
WITH baseline AS (SELECT * FROM openalex_dev.rohan_lab.fs1012_prefold_links VERSION AS OF 0),
b AS (SELECT x.* FROM baseline x
 LEFT JOIN IDENTIFIER(:control_prefix || 'resolve_candidate') r ON r.owner_id=x.member_id
 LEFT JOIN IDENTIFIER(:control_prefix || 'work_survivors') w ON w.loser_work_id=x.work_id
 LEFT ANTI JOIN IDENTIFIER(:control_prefix || 'work_awards_candidate') a
 ON a.work_id=coalesce(w.survivor_work_id,x.work_id) AND a.award.id=concat('https://openalex.org/G',r.stable_id)),
a AS (SELECT x.* FROM baseline x
 LEFT JOIN IDENTIFIER(:candidate_prefix || 'resolve_candidate') r ON r.owner_id=x.member_id
 LEFT JOIN IDENTIFIER(:candidate_prefix || 'work_survivors') w ON w.loser_work_id=x.work_id
 LEFT ANTI JOIN IDENTIFIER(:candidate_prefix || 'work_awards_candidate') a
 ON a.work_id=coalesce(w.survivor_work_id,x.work_id) AND a.award.id=concat('https://openalex.org/G',r.stable_id)),
m AS (SELECT (SELECT count(*) FROM baseline) baseline_links,(SELECT count(*) FROM b) control_missing,
 (SELECT count(*) FROM a) after_missing,
 (SELECT count(*) FROM a LEFT ANTI JOIN b USING(work_id,member_id,survivor_id)) lost_by_switch)
SELECT *,assert_true(baseline_links>0 AND after_missing=0 AND lost_by_switch=0,'cutover baseline switch loss: STOP') gate FROM m;
