"""oxjob #640 Tier 2: read-only backtest of the authorship LLM judge.

Runs small samples of each judge arm through ai_query via run_query (SELECT
only — nothing is written server-side), saves verdicts to qa/ JSON for hand
review, and reports measured prompt/output sizes for cost projection.
"""
import json
import sys

sys.path.insert(0, ".")
from utils.databricks_sql import run_query
from utils.ai_query_cost_guard import MODEL_PRICES, _tokens_from_chars

MODEL = "databricks-claude-opus-4-8"

SCHEMA_BINARY = json.dumps({
    "type": "json_schema",
    "json_schema": {
        "name": "verdict",
        "schema": {
            "type": "object",
            "properties": {
                "verdict": {"type": "string", "enum": ["same_person", "different_person", "cannot_determine"]},
                "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
            },
            "required": ["verdict", "confidence"],
        },
        "strict": True,
    },
})

SCHEMA_CHOICE = json.dumps({
    "type": "json_schema",
    "json_schema": {
        "name": "verdict",
        "schema": {
            "type": "object",
            "properties": {
                "verdict": {"type": "string",
                            "enum": ["candidate_1", "candidate_2", "candidate_3", "candidate_4",
                                     "candidate_5", "none_correct", "cannot_determine"]},
                "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
            },
            "required": ["verdict", "confidence"],
        },
        "strict": True,
    },
})

P = "openalex.authors.pending_author_assignments"
B = "openalex.authors.author_matching_batch"
Q = "openalex.authors.author_matching_new_author_queue"
AFM = "openalex.authors.authors_for_matching"
INST = "openalex.institutions.institutions"
WB = "openalex.works.openalex_works_base"

# Shared context CTEs: incoming side (coauthors, institution names, work
# title/year) and a rendered profile string per candidate author.
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

def profile_sql(author_id_col):
    """Rendered one-line profile for an author from authors_for_matching."""
    return f"""
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


def arm_a(n_per_tier=2):
    sql = f"""
WITH sample AS (
  SELECT p.work_id, p.author_sequence, p.raw_author_name, p.institution_ids,
         p.existing_author_id,
         CASE WHEN p.match_method = 'orcid' THEN 'orcid' ELSE p.name_match_tier END AS tier
  FROM {P} p
  WHERE p.match_outcome = 'MATCHED' AND p.existing_author_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CASE WHEN p.match_method = 'orcid' THEN 'orcid' ELSE p.name_match_tier END
    ORDER BY xxhash64(CONCAT(CAST(p.work_id AS STRING), ':', CAST(p.author_sequence AS STRING)))
  ) <= {n_per_tier}
),
cand_ids AS (SELECT DISTINCT existing_author_id AS author_id FROM sample),
{INCOMING_CTES},
{PROF_INST},
prompts AS (
  SELECT s.work_id, s.author_sequence, s.tier, s.raw_author_name,
         s.existing_author_id,
    CONCAT(
      'Decide whether the incoming authorship and the author profile are the same person. ',
      'Weigh name compatibility, institutions, research era, and field. Answer only via the schema.\\n\\n',
      {INCOMING_BLOCK},
      '\\nASSIGNED AUTHOR PROFILE\\n',
      {profile_sql('s.existing_author_id')}
    ) AS prompt
  FROM sample s
  LEFT JOIN coauth ca ON ca.work_id = s.work_id AND ca.author_sequence = s.author_sequence
  LEFT JOIN inc_inst ii ON ii.work_id = s.work_id AND ii.author_sequence = s.author_sequence
  LEFT JOIN wk w ON w.id = s.work_id
  JOIN {AFM} a ON a.author_id = s.existing_author_id
  LEFT JOIN prof_inst pi ON pi.author_id = s.existing_author_id
)
SELECT work_id, author_sequence, tier, raw_author_name, existing_author_id,
       prompt, LENGTH(prompt) AS prompt_chars,
       ai_query('{MODEL}', prompt, responseFormat => '{SCHEMA_BINARY}') AS out
FROM prompts
"""
    return run_query(sql)


def ARM_B_SQL(n_seats=15):
    """Unmatched (AMBIGUOUS) seats vs their top-5 block candidates.
    verdict=candidate_k => the mint was a splinter AND the cascade missed a
    match; none_correct => minting was right; cannot_determine => ambiguity
    is legitimate on available evidence."""
    sql = f"""
WITH sample AS (
  SELECT p.work_id, p.author_sequence, p.raw_author_name, p.institution_ids,
         p.work_source_ids, p.block_key
  FROM {P} p
  WHERE p.match_outcome = 'AMBIGUOUS' AND p.block_key IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    ORDER BY xxhash64(CONCAT(CAST(p.work_id AS STRING), ':', CAST(p.author_sequence AS STRING)))
  ) <= {n_seats}
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
{INCOMING_CTES},
{PROF_INST},
cand_lines AS (
  SELECT rc.work_id, rc.author_sequence,
         CONCAT_WS('\\n', TRANSFORM(
           ARRAY_SORT(COLLECT_LIST(STRUCT(rc.rank, CONCAT('candidate_', CAST(rc.rank AS STRING), ': ', {profile_sql('rc.author_id')}) AS line))),
           x -> x.line)) AS lineup,
         CONCAT_WS(',', TRANSFORM(
           ARRAY_SORT(COLLECT_LIST(STRUCT(rc.rank, rc.author_id AS aid))),
           x -> CAST(x.aid AS STRING))) AS cand_author_ids
  FROM ranked_cands rc
  JOIN {AFM} a ON a.author_id = rc.author_id
  LEFT JOIN prof_inst pi ON pi.author_id = rc.author_id
  GROUP BY rc.work_id, rc.author_sequence
),
prompts AS (
  SELECT s.work_id, s.author_sequence, s.raw_author_name, cl.cand_author_ids,
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
)
SELECT work_id, author_sequence, raw_author_name, cand_author_ids,
       prompt, LENGTH(prompt) AS prompt_chars,
       ai_query('{MODEL}', prompt, responseFormat => '{SCHEMA_CHOICE}') AS out
FROM prompts
"""
    return sql


def arm_b(n_seats=15):
    return run_query(ARM_B_SQL(n_seats))


CAND_COAUTH = f"""
cand_works AS (
  SELECT author_id, work_id
  FROM {{SEATS}}
  WHERE author_id IN (SELECT author_id FROM cand_ids)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY work_id DESC) <= 3
),
cand_coauth AS (
  SELECT cw.author_id,
         CONCAT_WS('; ', SLICE(COLLECT_SET(wa2.raw_author_name), 1, 8)) AS coauthors
  FROM cand_works cw
  JOIN {{SEATS}} wa2 ON wa2.work_id = cw.work_id
   AND NOT (wa2.author_id <=> cw.author_id)
  GROUP BY cw.author_id
)
""".replace("{SEATS}", "openalex.works.work_authors")


def arm_b_coauth(n_seats=15):
    base = ARM_B_SQL(n_seats)
    base = base.replace("cand_ids AS (SELECT DISTINCT author_id FROM ranked_cands),",
                        "cand_ids AS (SELECT DISTINCT author_id FROM ranked_cands),\n" + CAND_COAUTH + ",")
    base = base.replace("LEFT JOIN prof_inst pi ON pi.author_id = rc.author_id",
                        "LEFT JOIN prof_inst pi ON pi.author_id = rc.author_id\n  LEFT JOIN cand_coauth cc ON cc.author_id = rc.author_id")
    base = base.replace("'; works: ', CAST(a.works_count AS STRING)",
                        "'; works: ', CAST(a.works_count AS STRING),\n        '; sample coauthors: ', COALESCE(cc.coauthors, '(unknown)')")
    return run_query(base)


if __name__ == "__main__":
    from collections import Counter
    inp, outp = MODEL_PRICES[MODEL]
    import sys as _s
    arms = {"armA": arm_a, "armB": arm_b, "armB_coauth": arm_b_coauth}
    todo = _s.argv[1:] or list(arms)
    for name, fn in [(k, arms[k]) for k in todo]:
        rows = fn()
        json.dump([dict(r) for r in rows],
                  open(f"qa/oxjob640-judge-backtest-{name}.json", "w"), indent=1, default=str)
        in_chars = sum(r["prompt_chars"] for r in rows) / len(rows)
        out_chars = sum(len(str(r["out"])) for r in rows) / len(rows)
        per_row = (_tokens_from_chars(in_chars) * inp + _tokens_from_chars(out_chars) * outp) / 1e6
        print(f"{name}: {len(rows)} rows, avg in {in_chars:.0f} chars, out {out_chars:.0f}, ${per_row:.5f}/row")
        print("  ", Counter(json.loads(r["out"])["verdict"] for r in rows if r["out"]))
