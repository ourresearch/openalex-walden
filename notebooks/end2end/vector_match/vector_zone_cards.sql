-- #1348 Vector_Zone_Cards: Jev input for tonight's zone seats (rule = 'zone'), built on the serverless warehouse (the same join took 31 min
-- on the single-node job cluster, run 1051386767155394). One row per zone seat: the seat's card + its top-3 candidate profiles, each shown as
-- up to 8 of its most recent pool seats with card components from openalex.authors.oxjob1342_all_comp.
CREATE OR REPLACE TABLE openalex.authors.vector_match_zone_cards AS
WITH z AS (SELECT work_id, author_sequence, tier, top3 FROM openalex.authors.vector_match_decisions WHERE run_date = CAST(current_date() AS STRING) AND rule = 'zone' AND size(top3) > 0),
c AS (SELECT z.work_id, z.author_sequence, t.pos, t.c.pid AS pid, t.c.wc AS wc FROM z LATERAL VIEW posexplode(z.top3) t AS pos, c),
ps AS (SELECT c.work_id, c.author_sequence, c.pos, c.pid, c.wc, p.work_id AS pw, p.author_sequence AS ps,
              ROW_NUMBER() OVER (PARTITION BY c.work_id, c.author_sequence, c.pid ORDER BY p.publication_year DESC NULLS LAST, p.work_id) rn
       FROM c JOIN openalex.authors.vector_match_pool p ON p.author_id = c.pid),
cards AS (SELECT ps.work_id, ps.author_sequence, ps.pos, ps.pid, ps.wc,
                 collect_list(struct(ac.raw_name, ac.aff_strings, ac.inst_names, ac.title, ac.publication_year, ac.venue, ac.subfield, ac.coauthors)) AS seats
          FROM ps JOIN openalex.authors.oxjob1342_all_comp ac ON ac.work_id = ps.pw AND ac.author_sequence = ps.ps WHERE ps.rn <= 8 GROUP BY 1, 2, 3, 4, 5),
cands AS (SELECT work_id, author_sequence, array_sort(collect_list(struct(pos, pid, wc, seats)), (l, r) -> CASE WHEN l.pos < r.pos THEN -1 WHEN l.pos > r.pos THEN 1 ELSE 0 END) AS cands
          FROM cards GROUP BY 1, 2)
SELECT z.work_id, z.author_sequence, z.tier, struct(d.raw_name, d.aff_strings, d.inst_names, d.title, d.publication_year, d.venue, d.subfield, d.coauthors) AS seat, cands.cands
FROM z JOIN openalex.authors.vector_match_cards d ON d.work_id = z.work_id AND d.author_sequence = z.author_sequence
JOIN cands ON cands.work_id = z.work_id AND cands.author_sequence = z.author_sequence;
