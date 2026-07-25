# Databricks notebook source
# MAGIC %md
# MAGIC # Authorship Quality Judge (oxjob #640 Tier 2)
# MAGIC
# MAGIC Nightly LLM-judged quality sample of the day's author-matching decisions.
# MAGIC Runs as task 2 of the Authorship Daily Metrics job, in the same 22:30 UTC
# MAGIC window while the ephemeral run-state tables still hold today's run.
# MAGIC Observation-only: writes `openalex.authors.authorship_daily_quality_sample`
# MAGIC and judge metrics rows — never pipeline tables.
# MAGIC
# MAGIC **Arm A** — assigned-match precision (binary): stratified sample per
# MAGIC `match_tier` (flat N per tier so thin tiers get equal coverage; blind
# MAGIC ORCID matches get their own stratum). Judge sees the incoming authorship
# MAGIC (name, coauthors, institutions, title, year) vs the assigned author's
# MAGIC profile. Verdict: same_person / different_person / cannot_determine.
# MAGIC
# MAGIC **Arm B** — mint correctness + missed matches (multiple-choice): sample of
# MAGIC AMBIGUOUS seats (the population that mints new authors) vs their top-5
# MAGIC block candidates, each with sample coauthors — the 2026-07-25 backtest
# MAGIC showed coauthor context converts 11/15 cannot_determine into 13/15
# MAGIC decisive, hand-verified verdicts. verdict=candidate_k means the mint was
# MAGIC a splinter AND the cascade missed a real match.
# MAGIC
# MAGIC **Arm D** — deterministic ORCID collisions (no LLM): authors minted today
# MAGIC whose ORCID already exists on an older profile. A floor on the true
# MAGIC wrong-mint rate and a calibration reference for the judge.
# MAGIC
# MAGIC **Cost guard** (standing rule 2026-07-20): prompts are materialized FIRST,
# MAGIC and the pass aborts if projected cost exceeds the approval threshold.
# MAGIC Backtest-measured rates: Arm A ~$0.0037/row, Arm B ~$0.009/row — at
# MAGIC default sample sizes ≈ $2.7/day, ~20x under the $50 threshold.

# COMMAND ----------

import json
from datetime import datetime, timezone

MODEL = "databricks-claude-opus-4-8"
# List-price ceiling per 1M tokens (input, output); ~4 chars/token.
MODEL_IN_USD, MODEL_OUT_USD = 15.0, 75.0
ABORT_THRESHOLD_USD = 50.0
ASSUMED_OUT_CHARS = 80  # measured 46-51 in backtest; padded

P = "openalex.authors.pending_author_assignments"
B = "openalex.authors.author_matching_batch"
AFM = "openalex.authors.authors_for_matching"
INST = "openalex.institutions.institutions"
WB = "openalex.works.openalex_works_base"
SEATS = "openalex.works.work_authors"
AUTHORS = "openalex.authors.authors"

DST_SAMPLE = "openalex.authors.authorship_daily_quality_sample"
DST_METRICS = "openalex.authors.authorship_daily_metrics"
PROMPTS_A = "openalex.authors.judge_prompts_arm_a"  # per-run scratch
PROMPTS_B = "openalex.authors.judge_prompts_arm_b"  # per-run scratch

SCHEMA_BINARY = json.dumps({
    "type": "json_schema",
    "json_schema": {"name": "verdict", "schema": {
        "type": "object",
        "properties": {
            "verdict": {"type": "string", "enum": ["same_person", "different_person", "cannot_determine"]},
            "confidence": {"type": "string", "enum": ["high", "medium", "low"]}},
        "required": ["verdict", "confidence"]}, "strict": True},
})
SCHEMA_CHOICE = json.dumps({
    "type": "json_schema",
    "json_schema": {"name": "verdict", "schema": {
        "type": "object",
        "properties": {
            "verdict": {"type": "string", "enum": ["candidate_1", "candidate_2", "candidate_3",
                                                   "candidate_4", "candidate_5", "none_correct",
                                                   "cannot_determine"]},
            "confidence": {"type": "string", "enum": ["high", "medium", "low"]}},
        "required": ["verdict", "confidence"]}, "strict": True},
})

# COMMAND ----------

dbutils.widgets.text("snapshot_date", "", "Snapshot date (YYYY-MM-DD, blank = today UTC)")
dbutils.widgets.text("arm_a_per_tier", "25", "Arm A samples per match_tier")
dbutils.widgets.text("arm_b_seats", "150", "Arm B ambiguous seats to judge")

_sd = dbutils.widgets.get("snapshot_date").strip()
RUN_DATE = (datetime.strptime(_sd, "%Y-%m-%d").date()
            if _sd else datetime.now(timezone.utc).date())
A_PER_TIER = int(dbutils.widgets.get("arm_a_per_tier"))
B_SEATS = int(dbutils.widgets.get("arm_b_seats"))
print(f"RUN_DATE={RUN_DATE} arm_a_per_tier={A_PER_TIER} arm_b_seats={B_SEATS}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DST_SAMPLE} (
  sample_date        DATE,
  arm                STRING,   -- armA | armB | orcid_collision
  work_id            BIGINT,
  author_sequence    INT,
  match_tier         STRING,   -- armA stratum (tier / orcid / orcid_blind); NULL for armB/D
  assigned_author_id BIGINT,   -- armA: the id being judged; D: the minted id
  cand_author_ids    STRING,   -- armB: comma-joined candidate ids in lineup order
  verdict            STRING,
  confidence         STRING,
  model              STRING,
  prompt_chars       INT,
  judged_at          TIMESTAMP
)
""")

if not spark.catalog.tableExists(P):
    dbutils.notebook.exit("pending_author_assignments missing — nothing to judge")
if "name_match_tier" not in [f.name for f in spark.table(P).schema]:
    dbutils.notebook.exit("pre-match_tier batch — skipping judge pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build prompts (no LLM calls yet — cost is projected from these first)

# COMMAND ----------

# Shared SQL fragments (proven in qa/oxjob640_judge_backtest.py)
INCOMING_CTES = f"""
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
    FROM sample s LATERAL VIEW EXPLODE(s.institution_ids) t AS iid
  ) x
  JOIN {INST} i ON CAST(SUBSTRING(x.iid, 23) AS BIGINT) = i.id
  GROUP BY x.work_id, x.author_sequence
),
wk AS (
  SELECT id, title, publication_year
  FROM {WB} WHERE id IN (SELECT work_id FROM sample)
)
"""

PROF_INST = f"""
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
"""

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

spark.sql(f"""
CREATE OR REPLACE TABLE {PROMPTS_A} AS
WITH sample AS (
  SELECT p.work_id, p.author_sequence, p.raw_author_name, p.institution_ids,
         p.existing_author_id,
         CASE WHEN p.match_method = 'orcid' AND p.orcid_blind_match THEN 'orcid_blind'
              WHEN p.match_method = 'orcid' THEN 'orcid'
              ELSE p.name_match_tier END AS tier
  FROM {P} p
  WHERE p.match_outcome = 'MATCHED' AND p.existing_author_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CASE WHEN p.match_method = 'orcid' AND p.orcid_blind_match THEN 'orcid_blind'
                      WHEN p.match_method = 'orcid' THEN 'orcid'
                      ELSE p.name_match_tier END
    ORDER BY xxhash64(CONCAT(CAST(p.work_id AS STRING), ':', CAST(p.author_sequence AS STRING)))
  ) <= {A_PER_TIER}
),
cand_ids AS (SELECT DISTINCT existing_author_id AS author_id FROM sample),
{INCOMING_CTES},
{PROF_INST}
SELECT s.work_id, s.author_sequence, s.tier, s.existing_author_id,
  CONCAT(
    'Decide whether the incoming authorship and the author profile are the same person. ',
    'Weigh name compatibility, institutions, research era, and field. Answer only via the schema.\\n\\n',
    {INCOMING_BLOCK},
    '\\nASSIGNED AUTHOR PROFILE\\n',
    {PROFILE_EXPR}
  ) AS prompt
FROM sample s
LEFT JOIN coauth ca ON ca.work_id = s.work_id AND ca.author_sequence = s.author_sequence
LEFT JOIN inc_inst ii ON ii.work_id = s.work_id AND ii.author_sequence = s.author_sequence
LEFT JOIN wk w ON w.id = s.work_id
JOIN {AFM} a ON a.author_id = s.existing_author_id
LEFT JOIN prof_inst pi ON pi.author_id = s.existing_author_id
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {PROMPTS_B} AS
WITH sample AS (
  SELECT p.work_id, p.author_sequence, p.raw_author_name, p.institution_ids,
         p.work_source_ids, p.block_key
  FROM {P} p
  WHERE p.match_outcome = 'AMBIGUOUS' AND p.block_key IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    ORDER BY xxhash64(CONCAT(CAST(p.work_id AS STRING), ':', CAST(p.author_sequence AS STRING)))
  ) <= {B_SEATS}
),
ranked_cands AS (
  SELECT s.work_id, s.author_sequence, a.author_id,
         ROW_NUMBER() OVER (
           PARTITION BY s.work_id, s.author_sequence
           ORDER BY (SIZE(s.institution_ids) > 0 AND SIZE(a.institution_ids) > 0
                     AND ARRAYS_OVERLAP(a.institution_ids, s.institution_ids)) DESC,
                    (SIZE(s.work_source_ids) > 0 AND SIZE(a.source_ids) > 0
                     AND ARRAYS_OVERLAP(a.source_ids, s.work_source_ids)) DESC,
                    a.works_count DESC, a.author_id
         ) AS rank
  FROM sample s
  JOIN {AFM} a ON a.block_key = s.block_key
  QUALIFY rank <= 5
),
cand_ids AS (SELECT DISTINCT author_id FROM ranked_cands),
cand_works AS (
  SELECT author_id, work_id
  FROM {SEATS}
  WHERE author_id IN (SELECT author_id FROM cand_ids)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY work_id DESC) <= 3
),
cand_coauth AS (
  SELECT cw.author_id,
         CONCAT_WS('; ', SLICE(COLLECT_SET(wa2.raw_author_name), 1, 8)) AS coauthors
  FROM cand_works cw
  JOIN {SEATS} wa2 ON wa2.work_id = cw.work_id
   AND NOT (wa2.author_id <=> cw.author_id)
  GROUP BY cw.author_id
),
{INCOMING_CTES},
{PROF_INST},
cand_lines AS (
  SELECT rc.work_id, rc.author_sequence,
         CONCAT_WS('\\n', TRANSFORM(
           ARRAY_SORT(COLLECT_LIST(STRUCT(rc.rank,
             CONCAT('candidate_', CAST(rc.rank AS STRING), ': ', {PROFILE_EXPR},
                    '; sample coauthors: ', COALESCE(cc.coauthors, '(unknown)')) AS line))),
           x -> x.line)) AS lineup,
         CONCAT_WS(',', TRANSFORM(
           ARRAY_SORT(COLLECT_LIST(STRUCT(rc.rank, rc.author_id AS aid))),
           x -> CAST(x.aid AS STRING))) AS cand_author_ids
  FROM ranked_cands rc
  JOIN {AFM} a ON a.author_id = rc.author_id
  LEFT JOIN prof_inst pi ON pi.author_id = rc.author_id
  LEFT JOIN cand_coauth cc ON cc.author_id = rc.author_id
  GROUP BY rc.work_id, rc.author_sequence
)
SELECT s.work_id, s.author_sequence, cl.cand_author_ids,
  CONCAT(
    'The incoming authorship was NOT matched to any existing author (a new author profile would be minted). ',
    'Decide whether it is actually the same person as one of the candidate profiles below. ',
    'Only pick a candidate if the evidence genuinely supports it; otherwise answer none_correct, ',
    'or cannot_determine if the evidence is insufficient to decide. Answer only via the schema.\\n\\n',
    {INCOMING_BLOCK},
    '\\nCANDIDATE PROFILES\\n', cl.lineup
  ) AS prompt
FROM sample s
JOIN cand_lines cl ON cl.work_id = s.work_id AND cl.author_sequence = s.author_sequence
LEFT JOIN coauth ca ON ca.work_id = s.work_id AND ca.author_sequence = s.author_sequence
LEFT JOIN inc_inst ii ON ii.work_id = s.work_id AND ii.author_sequence = s.author_sequence
LEFT JOIN wk w ON w.id = s.work_id
""")

# COMMAND ----------

# Cost projection from the ACTUAL rendered prompts; abort above threshold.
stats = spark.sql(f"""
    SELECT (SELECT COALESCE(SUM(LENGTH(prompt)), 0) FROM {PROMPTS_A}) AS a_chars,
           (SELECT COUNT(*) FROM {PROMPTS_A}) AS a_rows,
           (SELECT COALESCE(SUM(LENGTH(prompt)), 0) FROM {PROMPTS_B}) AS b_chars,
           (SELECT COUNT(*) FROM {PROMPTS_B}) AS b_rows
""").collect()[0]
total_rows = stats["a_rows"] + stats["b_rows"]
in_tokens = (stats["a_chars"] + stats["b_chars"]) / 4.0
out_tokens = total_rows * ASSUMED_OUT_CHARS / 4.0
projected_usd = (in_tokens * MODEL_IN_USD + out_tokens * MODEL_OUT_USD) / 1e6
print(f"prompts: armA={stats['a_rows']} armB={stats['b_rows']} projected=${projected_usd:.2f}")
if projected_usd >= ABORT_THRESHOLD_USD:
    raise RuntimeError(
        f"judge pass projected at ${projected_usd:.2f} >= ${ABORT_THRESHOLD_USD} — "
        "aborting per standing ai_query cost rule; reduce sample widgets or approve explicitly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Judge (ai_query) → append verdicts

# COMMAND ----------

spark.sql(f"""
INSERT INTO {DST_SAMPLE}
  (sample_date, arm, work_id, author_sequence, match_tier, assigned_author_id,
   cand_author_ids, verdict, confidence, model, prompt_chars, judged_at)
SELECT DATE'{RUN_DATE}', 'armA', work_id, author_sequence, tier, existing_author_id,
       NULL,
       get_json_object(out, '$.verdict'),
       get_json_object(out, '$.confidence'),
       '{MODEL}', LENGTH(prompt), current_timestamp()
FROM (
  SELECT *, ai_query('{MODEL}', prompt, responseFormat => '{SCHEMA_BINARY}') AS out
  FROM {PROMPTS_A}
)
""")

spark.sql(f"""
INSERT INTO {DST_SAMPLE}
  (sample_date, arm, work_id, author_sequence, match_tier, assigned_author_id,
   cand_author_ids, verdict, confidence, model, prompt_chars, judged_at)
SELECT DATE'{RUN_DATE}', 'armB', work_id, author_sequence, NULL, NULL,
       cand_author_ids,
       get_json_object(out, '$.verdict'),
       get_json_object(out, '$.confidence'),
       '{MODEL}', LENGTH(prompt), current_timestamp()
FROM (
  SELECT *, ai_query('{MODEL}', prompt, responseFormat => '{SCHEMA_CHOICE}') AS out
  FROM {PROMPTS_B}
)
""")

# COMMAND ----------

# Arm D: deterministic ORCID collisions among today's mints (no LLM).
spark.sql(f"""
INSERT INTO {DST_SAMPLE}
  (sample_date, arm, work_id, author_sequence, match_tier, assigned_author_id,
   cand_author_ids, verdict, confidence, model, prompt_chars, judged_at)
SELECT DATE'{RUN_DATE}', 'orcid_collision', NULL, NULL, NULL, minted.id,
       CAST(older.id AS STRING), 'collision', 'high', NULL, NULL, current_timestamp()
FROM {AUTHORS} minted
JOIN {AUTHORS} older
  ON minted.orcid = older.orcid AND older.id < minted.id
WHERE DATE(minted.created_date) = DATE'{RUN_DATE}'
  AND minted.orcid IS NOT NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Roll judge metrics into the tall table (idempotent for this date)

# COMMAND ----------

spark.sql(f"""
DELETE FROM {DST_METRICS}
WHERE snapshot_date = DATE'{RUN_DATE}'
  AND metric IN ('judge_arm_a', 'judge_arm_b', 'orcid_mint_collisions', 'judge_cost_cents')
""")

spark.sql(f"""
INSERT INTO {DST_METRICS} (snapshot_date, snapshot_version, metric, dimension, value, computed_at)
SELECT DATE'{RUN_DATE}', NULL, 'judge_arm_a', CONCAT(match_tier, '|', verdict), COUNT(*), current_timestamp()
FROM {DST_SAMPLE}
WHERE sample_date = DATE'{RUN_DATE}' AND arm = 'armA'
GROUP BY match_tier, verdict
""")

spark.sql(f"""
INSERT INTO {DST_METRICS} (snapshot_date, snapshot_version, metric, dimension, value, computed_at)
SELECT DATE'{RUN_DATE}', NULL, 'judge_arm_b',
       CASE WHEN verdict LIKE 'candidate%' THEN 'candidate_pick' ELSE verdict END,
       COUNT(*), current_timestamp()
FROM {DST_SAMPLE}
WHERE sample_date = DATE'{RUN_DATE}' AND arm = 'armB'
GROUP BY CASE WHEN verdict LIKE 'candidate%' THEN 'candidate_pick' ELSE verdict END
""")

spark.sql(f"""
INSERT INTO {DST_METRICS} (snapshot_date, snapshot_version, metric, dimension, value, computed_at)
SELECT DATE'{RUN_DATE}', NULL, 'orcid_mint_collisions', NULL,
       COUNT(DISTINCT assigned_author_id), current_timestamp()
FROM {DST_SAMPLE}
WHERE sample_date = DATE'{RUN_DATE}' AND arm = 'orcid_collision'
""")

spark.sql(f"""
INSERT INTO {DST_METRICS} (snapshot_date, snapshot_version, metric, dimension, value, computed_at)
VALUES (DATE'{RUN_DATE}', NULL, 'judge_cost_cents', NULL,
        CAST({projected_usd} * 100 AS BIGINT), current_timestamp())
""")

# COMMAND ----------

display(spark.sql(f"""
    SELECT metric, dimension, value
    FROM {DST_METRICS}
    WHERE snapshot_date = DATE'{RUN_DATE}'
      AND metric IN ('judge_arm_a', 'judge_arm_b', 'orcid_mint_collisions', 'judge_cost_cents')
    ORDER BY metric, value DESC
"""))
