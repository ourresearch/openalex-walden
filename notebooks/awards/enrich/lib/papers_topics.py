"""Topics for awards with no text, from their papers (job AwardTopics, task `papers`; oxjob #2125 part 2).

The rule (method 2 of the 2026-11-09 test, ~/topics-papers-20261109/CARD.md, plus the 1-paper add of ~/topics-improve-20261109/CARD.md):
- an award qualifies when it is in `awards` and has no text-model row in the topics table (any row whose source is not SOURCE,
  not_classifiable included), so a text row always wins and a papers row never replaces one;
- its papers are its works in `work_awards`; each paper's topics (works' served q8b_2m top 3, `work_topics_v2`) are scaled to sum
  to 1, then averaged over the award's papers that have topics: share(topic) = sum of the scaled scores / papers;
- served when the award has >= MIN_PAPERS papers with topics AND the top share is >= MIN_SHARE ("multi"), or when it has exactly one
  paper with topics and at least 2 of 3 independent signals agree with that paper's top topic ("one"; no veto, below):
  1. funder's papers: the topic is >= 2% of the papers of the award's funder (each paper counted once, by its primary topic), and
     >= 3 of them, after taking off this award's share of the topic (as tested: it subtracts the share, not the paper) and its paper
     from the total (the funder needs >= 50 other papers);
  2. co-grant: the award's only linked paper thanks at most 50 grants, every grant on it with a text topic has this topic, and one of
     them is from the same funder;
  3. funder's text grants: >= 2% of the text grants in the award's most specific funder cell with >= 20 text grants have it as their
     primary topic (cells, most specific first: funder + grant-number length + its first 3 / NIH institute code / first 2 / first 1
     characters, then the funder);
  veto: the paper (thanking <= 50 grants, as for signal 2) also thanks grants with text topics and none is in its top subfield.
- topics = the top 3 by share (ties: more papers with it as their primary topic, then the lower topic id), score = ROUND(share, 4),
  names from openalex.common.* as the text rows get them; primary_topic = topics[0] where it is served.
Shares are summed as decimals (exact in any order) and rounded to 6 places, so two runs on the same inputs serve the same rows.
Pure strings: the notebook, the tests (sqlglot -> DuckDB) and the full-size proof all build their SQL here.
"""

SOURCE = "papers_v1"
MIN_PAPERS = 2
MIN_SHARE = 0.5
# the 1-paper add: thresholds as tested (topics-improve 2026-11-10, "double-confirmed": +37,719 awards)
FUNDER_PAPERS_SHARE, FUNDER_PAPERS_MIN, FUNDER_PAPERS_DEN = 0.02, 3, 50
CO_MAX_GRANTS = 50
TEXT_CELL_SHARE, TEXT_CELL_MIN = 0.02, 20
NIH_IC = r"(?:^|[^A-Z])[0-9]?[ -]?[A-Z][A-Z0-9][0-9][ -]?([A-Z]{2})[ -]?[0-9]{5,6}"
TOPICS_TYPE = ("ARRAY<STRUCT<id: STRING, display_name: STRING, score: FLOAT, subfield: STRUCT<id: STRING, display_name: STRING>, "
               "field: STRUCT<id: STRING, display_name: STRING>, domain: STRUCT<id: STRING, display_name: STRING>>>")
VOCAB = """SELECT CAST(t.topic_id AS INT) AS tid, t.display_name,
    named_struct('id', concat('https://openalex.org/subfields/', s.subfield_id), 'display_name', s.display_name) AS subfield,
    named_struct('id', concat('https://openalex.org/fields/', f.field_id), 'display_name', f.display_name) AS field,
    named_struct('id', concat('https://openalex.org/domains/', d.domain_id), 'display_name', d.display_name) AS domain
  FROM openalex.common.topics t JOIN openalex.common.subfields s USING (subfield_id) JOIN openalex.common.fields f USING (field_id)
  JOIN openalex.common.domains d USING (domain_id)"""


def links_sql(work_awards):
    """Every award-paper link once, with how many grants the paper thanks. Staged per run (`<state_prefix>papers_links`): the rule
    reads it several times, and Spark would otherwise rescan work_awards for each read."""
    return f"""SELECT award_id, work_id, count(*) OVER (PARTITION BY work_id) AS n_grants FROM (
  SELECT DISTINCT try_cast(substring(award.id, 23) AS BIGINT) AS award_id, work_id FROM {work_awards}) l
WHERE award_id IS NOT NULL"""


def paper_topics_sql(work_topics, links):
    """Each linked paper's topics (works' served top 3), scaled to sum to 1. Staged per run (`<state_prefix>papers_paper_topics`)."""
    return f"""SELECT s.work_id, s.pos, CAST(substring(s.col.id, 23) AS INT) AS tid, s.col.score / greatest(s.total, 1e-9) AS part FROM (
  SELECT w.work_id, aggregate(transform(w.topics, x -> CAST(x.score AS DOUBLE)), CAST(0 AS DOUBLE), (acc, y) -> acc + y) AS total,
    posexplode(w.topics) AS (pos, col)
  FROM {work_topics} w LEFT SEMI JOIN (SELECT DISTINCT work_id FROM {links}) l ON l.work_id = w.work_id
  WHERE size(w.topics) > 0) s"""


def served_sql(awards, target, links, paper_topics, vocab=VOCAB, min_papers=MIN_PAPERS, min_share=MIN_SHARE):
    """One row per award that gets a papers topic: award_id, topics (TOPICS_TYPE), papers, share (of the top topic, 6 places), rule
    ('multi' | 'one'). `links` and `paper_topics` are the staged tables above. Equi-joins only (the vocabulary is broadcast); the only
    generators are the 5 cells of an award and posexplode of an award's <= 3 kept topics."""
    return f"""WITH meta AS ({vocab}),
award_keys AS (
  SELECT a.id AS award_id, a.funder_id, regexp_replace(upper(a.funder_award_id), '[^A-Z0-9]', '') AS nid,
    nullif(regexp_extract(upper(a.funder_award_id), '{NIH_IC}', 1), '') AS nih_ic
  FROM {awards} a),
text_topic AS (
  SELECT award_id, CAST(substring(topics[0].id, 23) AS INT) AS x0, topics[0].subfield.id AS xsf
  FROM {target} WHERE NOT (source <=> '{SOURCE}') AND NOT not_classifiable AND size(topics) > 0),
eligible AS (
  SELECT k.* FROM award_keys k
  LEFT ANTI JOIN (SELECT award_id FROM {target} WHERE NOT (source <=> '{SOURCE}')) x ON x.award_id = k.award_id),
links AS (
  SELECT l.award_id, l.work_id FROM {links} l JOIN eligible e ON e.award_id = l.award_id),
award_topic AS (
  SELECT l.award_id, p.tid, sum(CAST(p.part AS DECIMAL(24, 12))) AS w, count_if(p.pos = 0) AS n_primary
  FROM links l JOIN {paper_topics} p ON p.work_id = l.work_id
  GROUP BY l.award_id, p.tid),
per_award AS (
  SELECT award_id, sum(n_primary) AS papers, sum(w) AS total,
    collect_list(named_struct('tid', tid, 'w', w, 'n_primary', n_primary)) AS tl
  FROM award_topic GROUP BY award_id),
ranked AS (
  SELECT award_id, papers,
    slice(array_sort(transform(tl, x -> named_struct('neg_share', -round(x.w / total, 6), 'neg_primary', -x.n_primary,
      'tid', x.tid))), 1, 3) AS top3
  FROM per_award WHERE total > 0),
one_base AS (SELECT award_id, top3, top3[0].tid AS ta, -top3[0].neg_share AS sa FROM ranked WHERE papers = 1),
one_subfield AS (   -- the paper's top subfield: the most share, then the bigger topic, then the lower topic id
  SELECT award_id, max_by(sfid, struct(sfw, w, -tid)) AS sf_top FROM (
    SELECT /*+ BROADCAST(m) */ a.award_id, a.tid, a.w, m.subfield.id AS sfid,
      sum(a.w) OVER (PARTITION BY a.award_id, m.subfield.id) AS sfw
    FROM award_topic a JOIN one_base o ON o.award_id = a.award_id JOIN meta m ON m.tid = a.tid) s
  GROUP BY award_id),
one_paper AS (   -- 1-paper awards: the paper's top topic and share, and its top subfield
  SELECT o.*, s.sf_top FROM one_base o LEFT JOIN one_subfield s ON s.award_id = o.award_id),
one_links AS (
  SELECT o.award_id, count(*) AS n_links, min(l.work_id) AS work_id FROM one_paper o JOIN links l ON l.award_id = o.award_id
  GROUP BY o.award_id),
funder_papers AS (   -- per funder: its papers by primary topic, each paper once
  SELECT k.funder_id, p.tid, count(DISTINCT l.work_id) AS n
  FROM {links} l JOIN award_keys k ON k.award_id = l.award_id JOIN {paper_topics} p ON p.work_id = l.work_id AND p.pos = 0
  WHERE k.funder_id IS NOT NULL GROUP BY k.funder_id, p.tid),
funder_papers_total AS (SELECT funder_id, sum(n) AS tot FROM funder_papers GROUP BY funder_id),
text_cells AS (   -- text grants by funder cell (a cell counts only with >= TEXT_CELL_MIN text grants)
  SELECT s.c.cell, s.c.k, s.x0 FROM (
    SELECT g.x0, explode(array(
      named_struct('cell', 'F', 'k', CAST(g.funder_id AS STRING)),
      named_struct('cell', 'P1', 'k', CASE WHEN g.nlen IS NOT NULL THEN concat_ws('|', g.funder_id, g.nlen, left(g.n3, 1)) END),
      named_struct('cell', 'P2', 'k', CASE WHEN g.nlen IS NOT NULL THEN concat_ws('|', g.funder_id, g.nlen, left(g.n3, 2)) END),
      named_struct('cell', 'IC', 'k', CASE WHEN g.nih_ic IS NOT NULL THEN concat_ws('|', g.funder_id, g.nih_ic) END),
      named_struct('cell', 'P3', 'k', CASE WHEN g.nlen IS NOT NULL THEN concat_ws('|', g.funder_id, g.nlen, g.n3) END))) AS c
    FROM (SELECT k.funder_id, length(k.nid) AS nlen, left(k.nid, 3) AS n3, k.nih_ic, t.x0
          FROM text_topic t JOIN award_keys k ON k.award_id = t.award_id WHERE k.funder_id IS NOT NULL) g) s
  WHERE s.c.k IS NOT NULL),
cell_total AS (SELECT cell, k, count(*) AS tot FROM text_cells GROUP BY cell, k HAVING count(*) >= {TEXT_CELL_MIN}),
cell_topic AS (SELECT cell, k, x0, count(*) AS ct FROM text_cells GROUP BY cell, k, x0),
one_cells AS (   -- each 1-paper award's share of its topic in each of its cells that has enough text grants
  SELECT oc.award_id, oc.c.cell, coalesce(ct.ct, 0) / tot.tot AS pt FROM (
    SELECT o.award_id, o.ta, explode(array(
      named_struct('cell', 'F', 'k', CAST(k.funder_id AS STRING)),
      named_struct('cell', 'P1', 'k', CASE WHEN k.nid IS NOT NULL THEN concat_ws('|', k.funder_id, length(k.nid), left(k.nid, 1)) END),
      named_struct('cell', 'P2', 'k', CASE WHEN k.nid IS NOT NULL THEN concat_ws('|', k.funder_id, length(k.nid), left(k.nid, 2)) END),
      named_struct('cell', 'IC', 'k', CASE WHEN k.nih_ic IS NOT NULL THEN concat_ws('|', k.funder_id, k.nih_ic) END),
      named_struct('cell', 'P3', 'k', CASE WHEN k.nid IS NOT NULL THEN concat_ws('|', k.funder_id, length(k.nid), left(k.nid, 3)) END))) AS c
    FROM one_paper o JOIN award_keys k ON k.award_id = o.award_id WHERE k.funder_id IS NOT NULL) oc
  JOIN cell_total tot ON tot.cell = oc.c.cell AND tot.k = oc.c.k
  LEFT JOIN cell_topic ct ON ct.cell = oc.c.cell AND ct.k = oc.c.k AND ct.x0 = oc.ta),
one_text AS (
  SELECT award_id, max_by(pt, CASE cell WHEN 'P3' THEN 5 WHEN 'IC' THEN 4 WHEN 'P2' THEN 3 WHEN 'P1' THEN 2 ELSE 1 END) AS pt
  FROM one_cells GROUP BY award_id),
one_co AS (   -- the other grants with text topics on the award's only paper
  SELECT o.award_id, count(*) AS n_co, count_if(t.x0 = o.ta) AS n_co_eq, count_if(t.xsf = o.sf_top) AS n_co_sf,
    count_if(ck.funder_id = k.funder_id) AS n_co_same_funder
  FROM one_paper o JOIN one_links ol ON ol.award_id = o.award_id AND ol.n_links = 1
  JOIN award_keys k ON k.award_id = o.award_id
  JOIN {links} g ON g.work_id = ol.work_id AND g.award_id <> o.award_id AND g.n_grants <= {CO_MAX_GRANTS}
  JOIN text_topic t ON t.award_id = g.award_id JOIN award_keys ck ON ck.award_id = g.award_id
  GROUP BY o.award_id),
one_signals AS (
  SELECT o.award_id, o.top3, o.sa,
    (coalesce(greatest(coalesce(fp.n, 0) - o.sa, 0) / CASE WHEN ft.tot - 1 >= {FUNDER_PAPERS_DEN} THEN ft.tot - 1 END, 0) >= {FUNDER_PAPERS_SHARE}
       AND greatest(coalesce(fp.n, 0) - o.sa, 0) >= {FUNDER_PAPERS_MIN}) AS funder_papers,
    (coalesce(co.n_co, 0) >= 1 AND co.n_co_eq = co.n_co AND co.n_co_same_funder > 0) AS co_grant,
    (coalesce(tx.pt, 0) >= {TEXT_CELL_SHARE}) AS funder_text,
    (coalesce(co.n_co, 0) >= 1 AND co.n_co_sf = 0) AS veto
  FROM one_paper o JOIN award_keys k ON k.award_id = o.award_id
  LEFT JOIN funder_papers fp ON fp.funder_id = k.funder_id AND fp.tid = o.ta
  LEFT JOIN funder_papers_total ft ON ft.funder_id = k.funder_id
  LEFT JOIN one_text tx ON tx.award_id = o.award_id
  LEFT JOIN one_co co ON co.award_id = o.award_id),
kept AS (
  SELECT award_id, papers, -top3[0].neg_share AS share, top3, 'multi' AS rule FROM ranked
  WHERE papers >= {int(min_papers)} AND -top3[0].neg_share >= {float(min_share)}
  UNION ALL
  SELECT award_id, 1, sa, top3, 'one' FROM one_signals
  WHERE CAST(funder_papers AS INT) + CAST(co_grant AS INT) + CAST(funder_text AS INT) >= 2 AND NOT veto),
picked AS (
  SELECT e.award_id, e.papers, e.share, e.rule, e.rn, e.x.tid AS tid, CAST(ROUND(-e.x.neg_share, 4) AS FLOAT) AS score FROM (
    SELECT k.award_id, k.papers, k.share, k.rule, posexplode(k.top3) AS (rn, x) FROM kept k) e)
SELECT /*+ BROADCAST(m) */ p.award_id, CAST(transform(array_sort(collect_list(named_struct('rn', p.rn,
      'id', concat('https://openalex.org/T', p.tid), 'display_name', m.display_name, 'score', p.score,
      'subfield', m.subfield, 'field', m.field, 'domain', m.domain))),
    x -> named_struct('id', x.id, 'display_name', x.display_name, 'score', x.score, 'subfield', x.subfield, 'field', x.field,
                      'domain', x.domain)) AS {TOPICS_TYPE}) AS topics, p.papers, p.share, p.rule
FROM picked p LEFT JOIN meta m ON m.tid = p.tid GROUP BY p.award_id, p.papers, p.share, p.rule"""


def plan_sql(target, served):
    """What the MERGE would do, in one pass over the served rows (`served`: a table, or a parenthesised served_sql for a dry run)
    and the target. Nothing is written."""
    mine, theirs = f"v.source <=> '{SOURCE}'", f"v.award_id IS NOT NULL AND NOT (v.source <=> '{SOURCE}')"
    return f"""SELECT count(r.award_id) AS served, count(DISTINCT r.award_id) AS served_ids, count_if(r.rule = 'multi') AS multi,
  count_if(r.rule = 'one') AS one_paper, count_if({mine}) AS papers_rows,
  count_if(r.award_id IS NOT NULL AND v.award_id IS NULL) AS would_insert,
  count_if(r.award_id IS NOT NULL AND {mine} AND NOT (to_json(v.topics) <=> to_json(r.topics))) AS would_update,
  count_if(r.award_id IS NULL AND {mine}) AS would_delete,
  count_if(r.award_id IS NOT NULL AND {theirs}) AS over_text_rows,
  count_if(r.award_id IS NOT NULL AND (NOT (size(r.topics) BETWEEN 1 AND 3)
    OR NOT ((r.rule = 'multi' AND r.share >= {MIN_SHARE} AND r.papers >= {MIN_PAPERS}) OR (r.rule = 'one' AND r.papers = 1))
    OR exists(r.topics, t -> t.display_name IS NULL OR t.field.id IS NULL))) AS bad_rows
FROM {served} r FULL OUTER JOIN {target} v ON v.award_id = r.award_id"""


def merge_sql(target, served):
    """Serve the papers rows: insert new ones, update changed ones, delete papers rows no longer served. A text row (any other source)
    is never matched for an update or delete, so text always wins; unchanged rows are not rewritten."""
    return f"""MERGE INTO {target} v USING (SELECT award_id, topics FROM {served}) r ON v.award_id = r.award_id
WHEN MATCHED AND v.source <=> '{SOURCE}' AND NOT (to_json(v.topics) <=> to_json(r.topics)) THEN
  UPDATE SET topics = r.topics, updated_datetime = current_timestamp()
WHEN NOT MATCHED THEN INSERT (award_id, topics, not_classifiable, source, created_datetime, updated_datetime, text_key)
  VALUES (r.award_id, r.topics, false, '{SOURCE}', current_timestamp(), current_timestamp(), CAST(NULL AS BIGINT))
WHEN NOT MATCHED BY SOURCE AND v.source <=> '{SOURCE}' THEN DELETE"""
