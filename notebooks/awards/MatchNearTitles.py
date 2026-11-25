# Databricks notebook source
# MAGIC %md
# MAGIC # Match near titles: listed articles whose title is a near spelling of one work's title (oxjob #2632, draft for Rohan)
# MAGIC
# MAGIC The `neartitle` tier, after `refmatch`, for sources that set it in `CreateFunderReportedLinks.sql`'s `link_sources`
# MAGIC (KAKEN). A funder's list often holds a paper under a title a little off the published one: a typo, "carboxy" for
# MAGIC "carboxyl", the work's title with a special-issue note or subtitle the list leaves off. Neither the title tier nor the
# MAGIC refmatch title guard (equal titles) links those. This task decides each such listed article once into
# MAGIC `openalex.awards.funder_reported_neartitle_picks`, keyed by (source, title_key, year); tomorrow's
# MAGIC `create_funder_reported_links` reads the linked picks as the `neartitle` tier.
# MAGIC
# MAGIC **Rule, per listed article no tier resolved today** (`output_type` = 'article', a title of at least 12 kanji/kana or 24
# MAGIC other characters once spaces are dropped):
# MAGIC 1. Keys: the listed `title_key` and each work's `location_work_match_keys.nt` (both `match_text`), through the links
# MAGIC    notebook's `cjk_key` (widths), then with every space dropped.
# MAGIC 2. Candidates: works whose key starts with the same 12 (kanji/kana) or 24 (other) characters, published within
# MAGIC    `max_year_gap` of the listed year (or either year unknown). A start shared by more than `max_prefix_works` works is
# MAGIC    skipped (status `common_prefix`): generic starts ("studies on the ...") give no evidence.
# MAGIC 3. Accept a candidate when the edit distance is at most `max_distance` of the shorter key, or the work's key holds the
# MAGIC    listed key whole (the work's title is longer: a subtitle or issue note) with the listed key at least 15 kanji/kana or
# MAGIC    30 other characters. Closest first (distance, then containment); linked only when one work is closest (else `tie`).
# MAGIC
# MAGIC Measured 25 Nov 2026 (#2632, desk dry run of this notebook's SQL on production inputs, picks written to a scratch table):
# MAGIC of 2,666,684 KAKEN article titles (with year) no tier resolved, 1,443,934 linked: 1,153,463 equal titles (refmatch decides
# MAGIC those first), 290,471 near titles (203,063 by edit distance, 87,408 by containment); 97,241 ties, 113,543 common prefixes.
# MAGIC Blind draw of 60 near-title links graded by reading: 57 right, 2 probably right, 1 wrong (a conference abstract, which
# MAGIC the held-record rule stops). On #2632's 100 hand-judged unmatched KAKEN outputs: 13 of the 34 that are OpenAlex works
# MAGIC linked (one to the work's duplicate record), 0 links among the 66 that are not.
# MAGIC A run scans the keys table once (~2.5 min on an X-Large warehouse, ~$2.40 list); a day with nothing new skips it.
# MAGIC **Cost guard:** more than `max_new` outputs to decide stops the task before the scan (a first fill raises it on purpose).
# MAGIC A run with nothing new to decide reads only the matches table. A failure writes nothing and never stops the links task.
# MAGIC Undo: drop `neartitle` from the source's tiers (the next links run drops its links); to unlink one run's picks for good,
# MAGIC `UPDATE openalex.awards.funder_reported_neartitle_picks SET status = 'unlinked' WHERE run_id = '<run>'`.

# COMMAND ----------

import datetime as dt
import os
import re

for name, default in [('matches_table', 'openalex.awards.funder_reported_link_matches'),
                      ('picks_table', 'openalex.awards.funder_reported_neartitle_picks'),
                      ('keys_table', 'openalex.works.location_work_match_keys'),
                      ('max_year_gap', '1'), ('max_distance', '0.15'), ('max_prefix_works', '200'),
                      ('retry_days', '30'), ('max_new', '200000'), ('run_id', '')]:
    dbutils.widgets.text(name, default)
W = {k: dbutils.widgets.get(k).strip() for k in ('matches_table', 'picks_table', 'keys_table', 'max_year_gap', 'max_distance',
                                                  'max_prefix_works', 'retry_days', 'max_new', 'run_id')}
RUN_ID = W['run_id'] or dt.datetime.utcnow().strftime('neartitle-%Y-%m-%dT%H:%M:%S')

# One place for each rule: the links notebook's sources and cjk_key.
LINKS_SQL = open(os.path.join(os.getcwd(), 'CreateFunderReportedLinks.sql'), encoding='utf-8').read()
SOURCES_CELL = re.search(r'CREATE OR REPLACE TEMP VIEW link_sources AS SELECT \* FROM VALUES(.*?)AS t\(source, tiers, check\)',
                         LINKS_SQL, re.S).group(1)
SOURCES = [s for s, t in re.findall(r"\('(\w+)', '([a-z,]+)', '\w+'\)", SOURCES_CELL) if 'neartitle' in t.split(',')]
CJK_KEY = re.search(r'CREATE OR REPLACE TEMPORARY FUNCTION cjk_key\(.*?;', LINKS_SQL, re.S).group(0).rstrip(';')
spark.sql(CJK_KEY)
print(RUN_ID, 'sources:', SOURCES)

# COMMAND ----------

# The listed articles no tier resolved today, not yet decided (or with no candidate retry_days ago).
TODO_SQL = """
SELECT DISTINCT m.source, m.title_key, m.year, regexp_replace(cjk_key(m.title_key), ' ', '') AS lkey
FROM {matches_table} m
LEFT ANTI JOIN (SELECT * FROM {picks_table}
                WHERE status <> 'no_candidates' OR decided_at > current_timestamp() - INTERVAL {retry_days} DAYS) p
  ON p.source = m.source AND p.title_key = m.title_key AND p.year <=> m.year
WHERE m.source IN ({sources}) AND m.work_id IS NULL AND m.output_type = 'article' AND m.title_key IS NOT NULL
  AND length(regexp_replace(cjk_key(m.title_key), ' ', '')) >= CASE WHEN m.title_key RLIKE '[\\\\p{{IsHan}}\\\\p{{IsHiragana}}\\\\p{{IsKatakana}}]' THEN 12 ELSE 24 END
"""

# Candidates and the decision, one row per listed (source, title_key, year).
DECIDE_SQL = """
WITH todo AS (
  SELECT *, substr(lkey, 1, CASE WHEN cjk THEN 12 ELSE 24 END) AS pfx
  FROM (SELECT *, lkey RLIKE '[\\\\p{{IsHan}}\\\\p{{IsHiragana}}\\\\p{{IsKatakana}}]' AS cjk FROM ({todo}))
),
keys AS (
  -- one read of the keys: each work's prefix by its own key's script (as the listed key's), and how many works share it
  SELECT id, yr, wkey, cjk, pfx, count(*) OVER (PARTITION BY pfx) AS prefix_works
  FROM (SELECT id, yr, wkey, cjk, substr(wkey, 1, CASE WHEN cjk THEN 12 ELSE 24 END) AS pfx
        FROM (SELECT id, yr, wkey, wkey RLIKE '[\\\\p{{IsHan}}\\\\p{{IsHiragana}}\\\\p{{IsKatakana}}]' AS cjk
              -- the keys table holds a row per location: one per work and key here, so a prefix counts works
              FROM (SELECT DISTINCT id, yr, regexp_replace(cjk_key(nt), ' ', '') AS wkey FROM {keys_table} WHERE nt IS NOT NULL)))
  WHERE pfx IN (SELECT pfx FROM todo)
),
pairs AS (
  SELECT t.source, t.title_key, t.year, k.id AS work_id,
    levenshtein(substr(k.wkey, 1, 255), substr(t.lkey, 1, 255)) / least(length(k.wkey), length(t.lkey), 255) AS distance,
    instr(k.wkey, t.lkey) > 0 AND length(t.lkey) >= CASE WHEN t.cjk THEN 15 ELSE 30 END AS extends
  FROM todo t JOIN keys k ON k.pfx = t.pfx AND k.cjk = t.cjk
  WHERE k.prefix_works <= {max_prefix_works} AND (t.year IS NULL OR k.yr IS NULL OR abs(k.yr - t.year) <= {max_year_gap})
),
ok AS (
  -- closest first: an edit distance within max_distance ranks before a containment
  SELECT *, CASE WHEN distance <= {max_distance} THEN distance ELSE 1 + distance END AS rank_key
  FROM pairs WHERE distance <= {max_distance} OR extends
),
picked AS (
  SELECT source, title_key, year, min(work_id) AS work_id, min(distance) AS distance, max(extends) AS extends,
    count(DISTINCT work_id) AS n_best
  FROM (SELECT *, min(rank_key) OVER (PARTITION BY source, title_key, year) AS best FROM ok) WHERE rank_key = best
  GROUP BY source, title_key, year
),
common AS (SELECT DISTINCT pfx FROM keys WHERE prefix_works > {max_prefix_works})
SELECT t.source, t.title_key, t.year, IF(p.n_best = 1, p.work_id, NULL) AS work_id, p.distance, p.extends, p.n_best,
  CASE WHEN p.n_best = 1 THEN 'linked' WHEN p.n_best > 1 THEN 'tie' WHEN c.pfx IS NOT NULL THEN 'common_prefix'
       ELSE 'no_candidates' END AS status
FROM todo t
LEFT JOIN picked p ON p.source = t.source AND p.title_key = t.title_key AND p.year <=> t.year
LEFT JOIN common c ON c.pfx = t.pfx
"""


def todo_sql():
    return TODO_SQL.format(sources=', '.join(f"'{s}'" for s in SOURCES) or "''", **W)


def decide_sql():
    return DECIDE_SQL.format(todo=todo_sql(), **W)

# COMMAND ----------

n_todo = spark.sql(f"SELECT count(*) AS n FROM ({todo_sql()})").first().n if SOURCES else 0
print(f'{n_todo:,} listed articles to decide')
if n_todo > int(W['max_new']):
    raise RuntimeError(f"{n_todo:,} to decide is more than max_new {W['max_new']}: raise it on purpose for a first fill")

# COMMAND ----------

if n_todo:
    spark.sql(f"""
    MERGE INTO {W['picks_table']} t
    USING (SELECT d.*, current_timestamp() AS decided_at, '{RUN_ID}' AS run_id
           FROM ({decide_sql()}) d) s
    ON t.source = s.source AND t.title_key = s.title_key AND t.year <=> s.year
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    display(spark.sql(f"SELECT status, count(*) AS n FROM {W['picks_table']} WHERE run_id = '{RUN_ID}' GROUP BY status"))
