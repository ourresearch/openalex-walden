-- Investigator authors (oxjob #1847): Jev's picks, kept by the job (prepare task). One row per grant slot Jev was asked about, with the
-- printed name it judged, the same-name list (list_hash), the decision ('link' carries the author; 'none' and 'unsure' do not), p, the
-- run date and the model. Judge_With_Jev MERGEs into it (judge_merge.sql); routes.sql's jev_link reads it.
CREATE TABLE IF NOT EXISTS __P__jev (award_id BIGINT, slot STRING, position INT, given_name STRING, family_name STRING, entry_id STRING,
  list_hash STRING, decision STRING, author_id BIGINT, p DOUBLE, judged_on DATE, model STRING)
CLUSTER BY (award_id)
COMMENT 'Investigator authors (oxjob #1847): the pick Jev made per grant slot (Judge_With_Jev); routes.sql reads it as jev_link'
;
-- The one-time seed, only into an empty table: the graded 11-06 answers (__JEVSEED__: openalex.awards.investigator_jev_links, the
-- picks, and openalex.awards.investigator_jev_unlinked, the none / unsure answers, while they exist; else an empty set). A pick has no
-- list_hash until a judge night reads its list (judge_merge.sql), so nothing served on the first night is lost. A none / unsure answer
-- carries the 11-06 list's hash (the job's definition): judge.sql asks it again only when tonight's list or printed name differs.
-- One row per slot: a pick first, then the highest p.
INSERT INTO __P__jev
SELECT coalesce(rd.canonical_id, j.award_id), j.slot, j.position, j.given_name, j.family_name, CAST(NULL AS STRING), j.list_hash,
  j.decision, j.author_id, j.p, DATE'__RUN__', concat('seed: ', j.src)
FROM __JEVSEED__ j
LEFT JOIN (SELECT old_id, max(canonical_id) AS canonical_id FROM openalex.awards.award_id_redirects GROUP BY 1) rd ON rd.old_id = j.award_id
WHERE NOT EXISTS (SELECT 1 FROM __P__jev)
QUALIFY row_number() OVER (PARTITION BY coalesce(rd.canonical_id, j.award_id), j.slot, j.position
                           ORDER BY CASE WHEN j.decision = 'link' THEN 0 ELSE 1 END, j.p DESC, j.author_id) = 1
;
-- Award redirects, every night: a row filed under an old award id moves to its canonical id, so judge.sql and judge_merge.sql (which
-- match ids as they are) see it. One row per canonical slot: one the canonical slot already has wins, else the lowest old id's.
MERGE INTO __P__jev t
USING (
  SELECT m.*, c.award_id IS NOT NULL AS has_canonical,
    row_number() OVER (PARTITION BY m.canonical_id, m.slot, m.position ORDER BY m.old_id) AS rk
  FROM (SELECT DISTINCT j.award_id AS old_id, rd.canonical_id, j.slot, j.position FROM __P__jev j
        JOIN (SELECT old_id, max(canonical_id) AS canonical_id FROM openalex.awards.award_id_redirects GROUP BY 1) rd
          ON rd.old_id = j.award_id AND rd.canonical_id <> j.award_id) m
  LEFT JOIN (SELECT DISTINCT award_id, slot, position FROM __P__jev) c
    ON c.award_id = m.canonical_id AND c.slot = m.slot AND c.position = m.position
) s
ON t.award_id = s.old_id AND t.slot = s.slot AND t.position = s.position
WHEN MATCHED AND (s.has_canonical OR s.rk > 1) THEN DELETE
WHEN MATCHED THEN UPDATE SET award_id = s.canonical_id
