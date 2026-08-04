#!/usr/bin/env python3
"""Name-shape fix validation (n3-n5 investigation, 2026-08-04).

Simulates the proposed MatchAuthors normalization (strip trailing dots from
parsed first/middle, NULLIF empty middles) on the CURRENT author_matching_batch,
diffs per-seat name-cascade decisions against pending_author_assignments (same
batch), then opus-judges stratified samples from each transition class.

Usage: python3 qa/ntierfix_validate.py <sim|diff|sample|judge|tally>
"""
import json
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

WAREHOUSE_ID = "3996dc0a9b183ce3"

P = "openalex.authors.pending_author_assignments"
B = "openalex.authors.author_matching_batch"
AFM = "openalex.authors.authors_for_matching"
INST = "openalex.institutions.institutions"
WB = "openalex.works.openalex_works_base"

SIM = "openalex.authors.ntierfix_sim"
DIFF = "openalex.authors.ntierfix_diff"
SAMPLE = "openalex.authors.ntierfix_sample"
JUDGE = "openalex.authors.ntierfix_judge"

PER_CLASS = 60
MODEL = "databricks-claude-opus-4-8"
MODEL_IN_USD, MODEL_OUT_USD = 15.0, 75.0
ABORT_THRESHOLD_USD = 50.0
ASSUMED_OUT_CHARS = 80


def execute(w, sql):
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=sql, wait_timeout="50s")
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(10)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(resp.status.error.message if resp.status.error else resp.status.state)
    return resp


# Proposed normalization under test: trailing-dot strip + empty-string -> NULL.
def nf(col):
    return f"NULLIF(REGEXP_REPLACE({col}, '\\\\.$', ''), '')"


def sim(w):
    sql = f"""CREATE OR REPLACE TABLE {SIM} AS
WITH enriched_batch AS (
  SELECT
    b.work_id,
    b.author_sequence,
    b.raw_author_name,
    TRANSFORM(b.all_institution_ids, x -> CONCAT('https://openalex.org/I', CAST(x AS STRING))) AS institution_ids,
    {nf('pn.parsed_name.first')} AS pn_first,
    SUBSTRING({nf('pn.parsed_name.first')}, 1, 1) AS pn_first_initial,
    {nf('pn.parsed_name.middle')} AS pn_middle,
    pn.parsed_name.last AS pn_last,
    COALESCE(wtf.topics, ARRAY()) AS topics,
    ARRAY_DISTINCT(TRANSFORM(FILTER(w.locations, x -> x.source.id IS NOT NULL), x -> x.source.id)) AS work_source_ids
  FROM {B} b
  LEFT JOIN openalex.authors.author_names pn ON TRIM(b.raw_author_name) = pn.raw_author_name
  LEFT JOIN openalex.works.work_topics wtf ON b.work_id = wtf.work_id
  LEFT JOIN {WB} w ON b.work_id = w.id
),
authors_prepared AS (
  SELECT
    work_id, author_sequence, raw_author_name,
    pn_first, pn_first_initial, pn_middle, pn_last,
    CASE
      WHEN pn_last IS NULL THEN NULL
      WHEN pn_first_initial IS NULL OR pn_first_initial = '' THEN pn_last
      ELSE CONCAT(pn_first_initial, ' ', pn_last)
    END AS block_key,
    institution_ids,
    TRANSFORM(topics, t -> t.id) AS topic_ids,
    work_source_ids
  FROM enriched_batch
  WHERE raw_author_name IS NOT NULL
),
blocked_candidates AS (
  SELECT
    e.work_id, e.author_sequence, e.raw_author_name,
    e.pn_first, e.pn_first_initial, e.pn_middle, e.pn_last, e.block_key,
    e.institution_ids, e.topic_ids, e.work_source_ids,
    alm.author_id,
    {nf('alm.first')} AS cand_first,
    SUBSTRING({nf('alm.first')}, 1, 1) AS cand_first_initial,
    {nf('alm.middle')} AS cand_middle,
    alm.last AS cand_last,
    alm.institution_ids AS candidate_institution_ids,
    alm.topic_ids AS candidate_topic_ids,
    alm.source_ids AS candidate_source_ids
  FROM authors_prepared e
  LEFT JOIN {AFM} alm ON alm.block_key = e.block_key AND e.block_key != ''
),
with_match_signals AS (
  SELECT *,
    (size(institution_ids) > 0 AND size(candidate_institution_ids) > 0
     AND arrays_overlap(candidate_institution_ids, institution_ids)) AS has_inst,
    (size(topic_ids) > 0 AND size(candidate_topic_ids) > 0
     AND arrays_overlap(candidate_topic_ids, topic_ids)) AS has_topic,
    (SIZE(work_source_ids) > 0 AND SIZE(candidate_source_ids) > 0
     AND ARRAYS_OVERLAP(candidate_source_ids, work_source_ids)) AS has_source
  FROM blocked_candidates
),
with_name_matches AS (
  SELECT *,
    (LENGTH(pn_first) > 1 AND LENGTH(pn_middle) > 1 AND LENGTH(cand_first) > 1 AND LENGTH(cand_middle) > 1
     AND pn_first = cand_first AND pn_middle = cand_middle AND pn_last = cand_last
    ) AS pattern_1_exact_full,
    (LENGTH(pn_first) > 1 AND (pn_middle IS NULL OR LENGTH(pn_middle) <= 1)
     AND LENGTH(cand_first) > 1 AND pn_first = cand_first AND pn_last = cand_last
     AND (cand_middle IS NULL OR pn_middle IS NULL OR SUBSTRING(pn_middle, 1, 1) = SUBSTRING(cand_middle, 1, 1))
    ) AS pattern_2_exact_first_mid_init,
    (LENGTH(pn_first) > 1 AND LENGTH(cand_first) > 1
     AND pn_first = cand_first AND pn_last = cand_last AND pn_middle IS NULL
    ) AS pattern_5_exact_first_last,
    (LENGTH(pn_first) = 1 AND pn_middle IS NULL
     AND LENGTH(cand_first) > 1 AND pn_first_initial = cand_first_initial AND pn_last = cand_last
    ) AS pattern_6_first_init_to_full
  FROM with_match_signals
),
aggregated_counts AS (
  SELECT
    work_id, author_sequence, raw_author_name, block_key, institution_ids,
    pn_first, pn_first_initial, pn_middle, pn_last, work_source_ids,
    count_if(pattern_1_exact_full) AS s1_n1,
    count_if(pattern_2_exact_first_mid_init) AS s1_n2,
    count_if(pattern_5_exact_first_last) AS s1_n5,
    count_if(pattern_1_exact_full AND has_inst) AS s2_n1,
    count_if(pattern_2_exact_first_mid_init AND has_inst) AS s2_n2,
    count_if(pattern_5_exact_first_last AND has_inst) AS s2_n5,
    count_if(pattern_6_first_init_to_full AND has_inst) AS s2_n6,
    count_if(pattern_1_exact_full AND has_inst AND has_source) AS s6_n1,
    count_if(pattern_2_exact_first_mid_init AND has_inst AND has_source) AS s6_n2,
    count_if(pattern_5_exact_first_last AND has_inst AND has_source) AS s6_n5,
    count_if(pattern_6_first_init_to_full AND has_inst AND has_source) AS s6_n6,
    count_if(pattern_1_exact_full AND has_inst AND has_topic) AS s4_n1,
    count_if(pattern_2_exact_first_mid_init AND has_inst AND has_topic) AS s4_n2,
    count_if(pattern_5_exact_first_last AND has_inst AND has_topic) AS s4_n5,
    count_if(pattern_6_first_init_to_full AND has_inst AND has_topic) AS s4_n6,
    count_if(pattern_1_exact_full AND has_source) AS s5_n1,
    count_if(pattern_2_exact_first_mid_init AND has_source) AS s5_n2,
    count_if(pattern_5_exact_first_last AND has_source) AS s5_n5,
    count_if(pattern_6_first_init_to_full AND has_source) AS s5_n6,
    count_if(pattern_1_exact_full AND has_topic) AS s3_n1,
    count_if(pattern_2_exact_first_mid_init AND has_topic) AS s3_n2,
    count_if(pattern_5_exact_first_last AND has_topic) AS s3_n5,
    MAX(CASE WHEN pattern_1_exact_full THEN author_id END) AS m_s1_n1,
    MAX(CASE WHEN pattern_2_exact_first_mid_init THEN author_id END) AS m_s1_n2,
    MAX(CASE WHEN pattern_5_exact_first_last THEN author_id END) AS m_s1_n5,
    MAX(CASE WHEN pattern_1_exact_full AND has_inst THEN author_id END) AS m_s2_n1,
    MAX(CASE WHEN pattern_2_exact_first_mid_init AND has_inst THEN author_id END) AS m_s2_n2,
    MAX(CASE WHEN pattern_5_exact_first_last AND has_inst THEN author_id END) AS m_s2_n5,
    MAX(CASE WHEN pattern_6_first_init_to_full AND has_inst THEN author_id END) AS m_s2_n6,
    MAX(CASE WHEN pattern_1_exact_full AND has_inst AND has_source THEN author_id END) AS m_s6_n1,
    MAX(CASE WHEN pattern_2_exact_first_mid_init AND has_inst AND has_source THEN author_id END) AS m_s6_n2,
    MAX(CASE WHEN pattern_5_exact_first_last AND has_inst AND has_source THEN author_id END) AS m_s6_n5,
    MAX(CASE WHEN pattern_6_first_init_to_full AND has_inst AND has_source THEN author_id END) AS m_s6_n6,
    MAX(CASE WHEN pattern_1_exact_full AND has_inst AND has_topic THEN author_id END) AS m_s4_n1,
    MAX(CASE WHEN pattern_2_exact_first_mid_init AND has_inst AND has_topic THEN author_id END) AS m_s4_n2,
    MAX(CASE WHEN pattern_5_exact_first_last AND has_inst AND has_topic THEN author_id END) AS m_s4_n5,
    MAX(CASE WHEN pattern_6_first_init_to_full AND has_inst AND has_topic THEN author_id END) AS m_s4_n6,
    MAX(CASE WHEN pattern_1_exact_full AND has_source THEN author_id END) AS m_s5_n1,
    MAX(CASE WHEN pattern_2_exact_first_mid_init AND has_source THEN author_id END) AS m_s5_n2,
    MAX(CASE WHEN pattern_5_exact_first_last AND has_source THEN author_id END) AS m_s5_n5,
    MAX(CASE WHEN pattern_6_first_init_to_full AND has_source THEN author_id END) AS m_s5_n6,
    MAX(CASE WHEN pattern_1_exact_full AND has_topic THEN author_id END) AS m_s3_n1,
    MAX(CASE WHEN pattern_2_exact_first_mid_init AND has_topic THEN author_id END) AS m_s3_n2,
    MAX(CASE WHEN pattern_5_exact_first_last AND has_topic THEN author_id END) AS m_s3_n5,
    COUNT(author_id) AS total_candidates_in_block
  FROM with_name_matches
  GROUP BY work_id, author_sequence, raw_author_name, block_key, institution_ids,
           pn_first, pn_first_initial, pn_middle, pn_last, work_source_ids
)
SELECT
  work_id, author_sequence, raw_author_name, pn_first, pn_middle, pn_last,
  total_candidates_in_block,
  CASE
    WHEN s1_n1 = 1 THEN m_s1_n1 WHEN s1_n2 = 1 THEN m_s1_n2 WHEN s1_n5 = 1 THEN m_s1_n5
    WHEN s6_n1 = 1 THEN m_s6_n1 WHEN s6_n2 = 1 THEN m_s6_n2 WHEN s6_n5 = 1 THEN m_s6_n5 WHEN s6_n6 = 1 THEN m_s6_n6
    WHEN s2_n1 = 1 THEN m_s2_n1 WHEN s2_n2 = 1 THEN m_s2_n2 WHEN s2_n5 = 1 THEN m_s2_n5 WHEN s2_n6 = 1 THEN m_s2_n6
    WHEN s4_n1 = 1 THEN m_s4_n1 WHEN s4_n2 = 1 THEN m_s4_n2 WHEN s4_n5 = 1 THEN m_s4_n5 WHEN s4_n6 = 1 THEN m_s4_n6
    WHEN s5_n1 = 1 THEN m_s5_n1 WHEN s5_n2 = 1 THEN m_s5_n2 WHEN s5_n5 = 1 THEN m_s5_n5 WHEN s5_n6 = 1 THEN m_s5_n6
    WHEN s3_n1 = 1 THEN m_s3_n1 WHEN s3_n2 = 1 THEN m_s3_n2 WHEN s3_n5 = 1 THEN m_s3_n5
    ELSE NULL
  END AS f_name_author_id,
  CASE
    WHEN s1_n1 = 1 THEN 's1_n1' WHEN s1_n2 = 1 THEN 's1_n2' WHEN s1_n5 = 1 THEN 's1_n5'
    WHEN s6_n1 = 1 THEN 's6_n1' WHEN s6_n2 = 1 THEN 's6_n2' WHEN s6_n5 = 1 THEN 's6_n5' WHEN s6_n6 = 1 THEN 's6_n6'
    WHEN s2_n1 = 1 THEN 's2_n1' WHEN s2_n2 = 1 THEN 's2_n2' WHEN s2_n5 = 1 THEN 's2_n5' WHEN s2_n6 = 1 THEN 's2_n6'
    WHEN s4_n1 = 1 THEN 's4_n1' WHEN s4_n2 = 1 THEN 's4_n2' WHEN s4_n5 = 1 THEN 's4_n5' WHEN s4_n6 = 1 THEN 's4_n6'
    WHEN s5_n1 = 1 THEN 's5_n1' WHEN s5_n2 = 1 THEN 's5_n2' WHEN s5_n5 = 1 THEN 's5_n5' WHEN s5_n6 = 1 THEN 's5_n6'
    WHEN s3_n1 = 1 THEN 's3_n1' WHEN s3_n2 = 1 THEN 's3_n2' WHEN s3_n5 = 1 THEN 's3_n5'
    ELSE NULL
  END AS f_name_match_tier
FROM aggregated_counts"""
    t0 = time.time()
    execute(w, sql)
    print(f"built {SIM} in {time.time()-t0:.0f}s")


def diff(w):
    sql = f"""CREATE OR REPLACE TABLE {DIFF} AS
SELECT
  p.work_id, p.author_sequence, p.raw_author_name,
  p.name_author_id AS old_id, f.f_name_author_id AS new_id,
  p.name_match_tier AS old_tier, f.f_name_match_tier AS new_tier,
  (p.orcid_author_id IS NOT NULL) AS orcid_bound,
  (p.pn_first LIKE '_.') AS dotted_first,
  CASE
    WHEN p.name_author_id <=> f.f_name_author_id
         AND p.name_match_tier <=> f.f_name_match_tier THEN 'unchanged'
    WHEN p.name_author_id <=> f.f_name_author_id THEN 'tier_only'
    WHEN p.name_author_id IS NULL AND f.f_name_match_tier LIKE '%_n6' THEN 'new_n6'
    WHEN p.name_author_id IS NULL THEN 'new_repair'
    WHEN f.f_name_author_id IS NULL AND p.pn_first LIKE '_.' THEN 'lost_dotted'
    WHEN f.f_name_author_id IS NULL THEN 'lost_unique'
    ELSE 'id_changed'
  END AS change_class
FROM {P} p
JOIN {SIM} f ON p.work_id = f.work_id AND p.author_sequence = f.author_sequence"""
    execute(w, sql)
    resp = execute(w, f"""
SELECT change_class, orcid_bound, COUNT(*) n
FROM {DIFF} GROUP BY 1,2 ORDER BY 1,2""")
    print("change_class, orcid_bound(binding unaffected if true), n")
    for row in resp.result.data_array or []:
        print(" ", row)


INCOMING_BLOCK = """
      'INCOMING AUTHORSHIP\\n',
      'name: ', s.raw_author_name, '\\n',
      'coauthors on this work: ', COALESCE(ca.coauthors, '(none listed)'), '\\n',
      'institutions: ', COALESCE(ii.inst_names, '(none listed)'), '\\n',
      'work: "', COALESCE(w.title, '(untitled)'), '" (', COALESCE(CAST(w.publication_year AS STRING), '?'), ')\\n'
"""

PROFILE_EXPR = """
      CONCAT(
        a.display_name,
        CASE WHEN SIZE(a.name_variants) > 1
             THEN CONCAT(' (variants: ', CONCAT_WS('; ', SLICE(a.name_variants, 1, 4)), ')')
             ELSE '' END,
        '; institutions: ', COALESCE(pi.inst_names, '(none)'),
        '; active ', COALESCE(CAST(a.first_active_year AS STRING), '?'),
        '-', COALESCE(CAST(a.last_active_year AS STRING), '?'),
        '; works: ', CAST(a.works_count AS STRING)
      )
"""


def sample(w):
    # judged_id: the binding whose correctness the class question hinges on —
    # new matches judge the incoming binding, lost matches judge what we give up.
    # id_changed emits both sides as separate rows.
    sql = f"""CREATE OR REPLACE TABLE {SAMPLE} AS
WITH base AS (
  SELECT d.*, CASE
      WHEN d.change_class IN ('new_n6', 'new_repair') THEN d.new_id
      WHEN d.change_class IN ('lost_dotted', 'lost_unique') THEN d.old_id
    END AS judged_id,
    CASE WHEN d.change_class IN ('new_n6', 'new_repair') THEN d.new_tier
         ELSE d.old_tier END AS judged_tier
  FROM {DIFF} d
  WHERE d.change_class IN ('new_n6', 'new_repair', 'lost_dotted', 'lost_unique')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.change_class
    ORDER BY xxhash64(CONCAT(CAST(d.work_id AS STRING), ':', CAST(d.author_sequence AS STRING)))
  ) <= {PER_CLASS}
),
idch AS (
  SELECT d.*, side.judged_id, side.tag AS side_tag,
         CASE WHEN side.tag = 'old' THEN d.old_tier ELSE d.new_tier END AS judged_tier
  FROM {DIFF} d
  LATERAL VIEW EXPLODE(ARRAY(
    NAMED_STRUCT('judged_id', d.old_id, 'tag', 'old'),
    NAMED_STRUCT('judged_id', d.new_id, 'tag', 'new'))) x AS side
  WHERE d.change_class = 'id_changed'
    AND d.work_id IN (
      SELECT work_id FROM {DIFF} WHERE change_class = 'id_changed'
      QUALIFY ROW_NUMBER() OVER (ORDER BY xxhash64(CAST(work_id AS STRING))) <= {PER_CLASS})
),
sample AS (
  SELECT work_id, author_sequence, raw_author_name, change_class, CAST(NULL AS STRING) AS side_tag,
         judged_id, judged_tier, old_id, new_id FROM base
  UNION ALL
  SELECT work_id, author_sequence, raw_author_name, CONCAT(change_class, '_', side_tag), side_tag,
         judged_id, judged_tier, old_id, new_id FROM idch
),
coauth AS (
  SELECT s.work_id, s.author_sequence,
         CONCAT_WS('; ', SLICE(COLLECT_LIST(b.raw_author_name), 1, 8)) AS coauthors
  FROM sample s
  JOIN {B} b ON b.work_id = s.work_id AND b.author_sequence <> s.author_sequence
  GROUP BY s.work_id, s.author_sequence
),
inc_inst AS (
  SELECT x.work_id, x.author_sequence,
         CONCAT_WS('; ', SLICE(COLLECT_LIST(i.display_name), 1, 5)) AS inst_names
  FROM (
    SELECT s.work_id, s.author_sequence, iid
    FROM sample s
    JOIN {P} p ON p.work_id = s.work_id AND p.author_sequence = s.author_sequence
    LATERAL VIEW EXPLODE(p.institution_ids) t AS iid
  ) x
  JOIN {INST} i ON CAST(SUBSTRING(x.iid, 23) AS BIGINT) = i.id
  GROUP BY x.work_id, x.author_sequence
),
wk AS (
  SELECT id, title, publication_year FROM {WB} WHERE id IN (SELECT work_id FROM sample)
),
cand_ids AS (SELECT DISTINCT judged_id AS author_id FROM sample WHERE judged_id IS NOT NULL),
prof_inst AS (
  SELECT x.author_id,
         CONCAT_WS('; ', SLICE(COLLECT_LIST(i.display_name), 1, 5)) AS inst_names
  FROM (
    SELECT a.author_id, iid
    FROM {AFM} a LATERAL VIEW EXPLODE(a.institution_ids) t AS iid
    WHERE a.author_id IN (SELECT author_id FROM cand_ids)
  ) x
  JOIN {INST} i ON CAST(SUBSTRING(x.iid, 23) AS BIGINT) = i.id
  GROUP BY x.author_id
)
SELECT s.work_id, s.author_sequence, s.raw_author_name, s.change_class, s.side_tag,
  s.judged_id, s.judged_tier, s.old_id, s.new_id,
  CONCAT(
    'Decide whether the incoming authorship and the author profile are the same person. ',
    'Weigh name compatibility, institutions, research era, and field. Answer only via the schema.\\n\\n',
    {INCOMING_BLOCK},
    '\\nCANDIDATE AUTHOR PROFILE\\n',
    {PROFILE_EXPR}
  ) AS prompt
FROM sample s
LEFT JOIN coauth ca ON ca.work_id = s.work_id AND ca.author_sequence = s.author_sequence
LEFT JOIN inc_inst ii ON ii.work_id = s.work_id AND ii.author_sequence = s.author_sequence
LEFT JOIN wk w ON w.id = s.work_id
JOIN {AFM} a ON a.author_id = s.judged_id
LEFT JOIN prof_inst pi ON pi.author_id = s.judged_id"""
    execute(w, sql)
    resp = execute(w, f"""
SELECT change_class, COUNT(*) n, SUM(LENGTH(prompt)) chars FROM {SAMPLE} GROUP BY 1 ORDER BY 1""")
    total_rows, total_chars = 0, 0
    for cls, n, chars in resp.result.data_array or []:
        print(f"  {cls}: {n} rows, {chars} prompt chars")
        total_rows += int(n)
        total_chars += int(chars)
    projected = (total_chars / 4 / 1e6 * MODEL_IN_USD
                 + total_rows * ASSUMED_OUT_CHARS / 4 / 1e6 * MODEL_OUT_USD)
    print(f"staged {total_rows} prompts, projected ${projected:.2f}")
    if projected > ABORT_THRESHOLD_USD:
        raise SystemExit(f"projected cost ${projected:.2f} > ${ABORT_THRESHOLD_USD} — aborting")


SCHEMA = json.dumps({"type": "json_schema", "json_schema": {
    "name": "verdict", "schema": {
        "type": "object",
        "properties": {"verdict": {"type": "string",
                                   "enum": ["same_person", "different_person", "cannot_determine"]},
                       "reason": {"type": "string"}},
        "required": ["verdict"]},
    "strict": True}})


def judge(w):
    sql = f"""CREATE OR REPLACE TABLE {JUDGE} AS
SELECT work_id, author_sequence, raw_author_name, change_class, side_tag,
  judged_id, judged_tier, old_id, new_id,
  ai_query('{MODEL}', prompt, responseFormat => '{SCHEMA}') AS verdict_raw
FROM {SAMPLE}"""
    t0 = time.time()
    execute(w, sql)
    print(f"judged in {time.time()-t0:.0f}s")


def tally(w):
    resp = execute(w, f"""
SELECT change_class, judged_tier, raw_author_name, judged_id, verdict_raw FROM {JUDGE}""")
    from collections import Counter
    per, details = {}, {}
    for cls, tier, name, jid, raw in resp.result.data_array or []:
        try:
            v = json.loads(raw)["verdict"]
        except Exception:
            v = "unparseable"
        per.setdefault(cls, Counter())[v] += 1
        details.setdefault(cls, []).append((v, tier, name, jid))
    for cls in sorted(per):
        c = per[cls]
        dec = c["same_person"] + c["different_person"]
        prec = c["same_person"] / dec * 100 if dec else 0
        print(f"{cls}: {dict(c)}  same_person(decisive)={prec:.0f}%")
    print("\ndifferent_person rows:")
    for cls in sorted(details):
        for v, tier, name, jid in details[cls]:
            if v == "different_person":
                print(f"  {cls} {tier} {name!r} -> {jid}")


if __name__ == "__main__":
    w = WorkspaceClient(profile="DEFAULT")
    {"sim": sim, "diff": diff, "sample": sample, "judge": judge, "tally": tally}[sys.argv[1]](w)
