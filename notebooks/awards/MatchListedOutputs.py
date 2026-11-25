# Databricks notebook source
# MAGIC %md
# MAGIC # Match listed outputs: Casey's reference matcher for the publications funders list without an identifier
# MAGIC
# MAGIC Funders' own output lists (every source in `CreateFunderReportedLinks.sql`'s `link_sources`: refmatch is its last tier for
# MAGIC all of them, and a source that lists only DOIs never reaches it) hold publications that no DOI or PMID names. This task writes each such
# MAGIC output like a reference and lets Casey's reference matcher (`notebooks/references/matcher/refmatch.py`, imported, not
# MAGIC copied) pick the work it is, the way the nightly matcher decides a reference (#2000, #2296). It runs after
# MAGIC `create_funder_reported_links` and keeps its picks in `openalex.awards.funder_reported_refmatch_picks`; the next run of that
# MAGIC notebook reads them as the last tier, `refmatch`, and accepts a pick only when the work's title is the listed title.
# MAGIC
# MAGIC **Rule, per new output** (a publication with a title of at least `min_title_chars`, unresolved in today's
# MAGIC `funder_reported_link_matches`, never decided as listed, by `listed_key`: title and surnames, since FWF renumbers its
# MAGIC output ids between fetches; each is decided once, except that one with no same-title work is looked at again every
# MAGIC `retry_days`, as OpenAlex gains records):
# MAGIC 1. Text: "first 3 surnames (et al). title. year" through Casey's text key (`sql/ref_text_key.sql`); FWF lists no year.
# MAGIC 2. Candidates: every work whose normalized title (`location_work_match_keys.nt`) is the listed title with a chapter's
# MAGIC    "; In: <book>" cut, at most `max_same_title` of them (the title guard admits no other work, so no other is searched).
# MAGIC 3. Score: the cosine of the text and each work's reference-style text ("first 3 authors (et al). title. journal. year",
# MAGIC    read from `EmbedWorkReferenceStyleIncremental.py`), both embedded with the endpoint Casey's work vectors use.
# MAGIC 4. Decision: Casey's checklist, combiner, link line (>= 0.97, or >= 0.9191 with first author + volume + page) and type
# MAGIC    tie-break, on models refit from his training rows and his review bench re-checked first; equal scores rank by work
# MAGIC    id, as the nightly ranks its search. The unsure band would go to his reader, which is not run here: those outputs
# MAGIC    stay unlinked (`status` = `band`).
# MAGIC
# MAGIC Measured 23 Nov 2026 on FWF (ref-matcher-1123, RESULT-PROD.md): a fresh blind grade of 200 links that add a served
# MAGIC pair, 190 right (95.0%, Wilson 91.0-97.3); with the links notebook's held records, 190 of 196 (96.9%).
# MAGIC **Cost guard:** more than `max_new` outputs to embed, or an embedding estimate over `max_usd`, stops the task before it
# MAGIC embeds anything (a first fill raises them on purpose). A failure here writes nothing and never stops the links task,
# MAGIC which keeps reading the picks it has.
# MAGIC Undo: revert the task (the next links run drops the refmatch links); to unlink one run's picks for good, `UPDATE
# MAGIC openalex.awards.funder_reported_refmatch_picks SET linked = false WHERE run_id = '<run>'` (a DELETE would let the next run
# MAGIC decide them again).

# COMMAND ----------

import datetime as dt
import html
import json
import os
import re
import sys
import time

import numpy as np
import pandas as pd

HERE = os.getcwd()                                                            # notebooks/awards
MATCHER = os.path.normpath(os.path.join(HERE, '..', 'references', 'matcher'))
sys.path.insert(0, MATCHER)
import refmatch as RM  # noqa: E402

for name, default in [('matches_table', 'openalex.awards.funder_reported_link_matches'),
                      ('decisions_table', 'openalex.awards.funder_reported_refmatch_picks'),
                      ('unmatched_table', 'openalex.awards.listed_outputs_unmatched'),
                      ('keys_table', 'openalex.works.location_work_match_keys'), ('works_table', 'openalex.works.openalex_works'),
                      ('model_dir', '/Volumes/openalex/works/data/reference_matcher/model'),
                      ('min_title_chars', '20'), ('max_same_title', '50'), ('max_new', '5000'), ('max_usd', '5'),
                      ('usd_per_million_texts', '3.4'), ('retry_days', '7'), ('max_per_run', '5000'),
                      ('allow_backlog_jump', 'false'), ('runs_table', 'openalex.awards.funder_reported_refmatch_runs'), ('run_id', '')]:
    dbutils.widgets.text(name, default)
W = {k: dbutils.widgets.get(k).strip() for k in ('matches_table', 'decisions_table', 'unmatched_table', 'keys_table', 'works_table', 'model_dir',
                                                  'min_title_chars', 'max_same_title', 'max_new', 'max_usd',
                                                  'usd_per_million_texts', 'retry_days', 'max_per_run', 'allow_backlog_jump',
                                                  'runs_table', 'run_id')}
RUN_ID = W['run_id'] or dt.datetime.utcnow().strftime('refmatch-%Y-%m-%dT%H:%M:%S')
T0 = time.time()


def log(msg):
    print(f'{dt.datetime.utcnow():%H:%M:%S} UTC  {msg}', flush=True)


# One place for each rule: the links notebook's sources and match_text, Casey's text key and instruction, and the text
# every work was embedded with.
LINKS_SQL = open(os.path.join(HERE, 'CreateFunderReportedLinks.sql'), encoding='utf-8').read()
SOURCES_CELL = re.search(r'CREATE OR REPLACE TEMP VIEW link_sources AS SELECT \* FROM VALUES(.*?)AS t\(source, tiers, check\)',
                         LINKS_SQL, re.S).group(1)
SOURCES = [s for s, _ in re.findall(r"\('(\w+)', '([a-z,]+)', '\w+'\)", SOURCES_CELL)]   # every source: refmatch is on for all
MATCH_TEXT = re.search(r'CREATE OR REPLACE TEMPORARY FUNCTION match_text\(s STRING\) RETURNS STRING RETURN.*?;', LINKS_SQL,
                       re.S).group(0).rstrip(';')
LISTED_KEY = re.search(r'CREATE OR REPLACE TEMPORARY FUNCTION listed_key\(.*?;', LINKS_SQL, re.S).group(0).rstrip(';')
CJK_KEY = re.search(r'CREATE OR REPLACE TEMPORARY FUNCTION cjk_key\(.*?;', LINKS_SQL, re.S).group(0).rstrip(';')


def sql_part(text, key):
    """A '-- @key' section of a .sql file (as MatchReferences reads sql/ref_text_key.sql)."""
    out, on = [], False
    for line in text.splitlines():
        s = line.strip()
        if s.startswith('-- @'):
            on = s[4:] == key
        elif on and s and not s.startswith('--'):
            out.append(s)
    return ' '.join(out)


KEY_SQL = open(os.path.join(MATCHER, 'sql', 'ref_text_key.sql'), encoding='utf-8').read()
TEXT_SQL, INSTRUCTION = sql_part(KEY_SQL, 'text'), sql_part(KEY_SQL, 'instruction')
EMBED_PY = open(os.path.join(HERE, '..', 'vector_search', 'EmbedWorkReferenceStyleIncremental.py'), encoding='utf-8').read()
WORK_TEXT = re.search(r'^TEXT = """(.*?)"""', EMBED_PY, re.S | re.M).group(1)
ENDPOINT = re.search(r'^ENDPOINT = "([^"]+)"', EMBED_PY, re.M).group(1)
log(f'sources {SOURCES}, endpoint {ENDPOINT}, run {RUN_ID}')
spark.sql(MATCH_TEXT)
spark.sql(LISTED_KEY)
spark.sql(CJK_KEY)
spark.sql(f"""CREATE TABLE IF NOT EXISTS {W['decisions_table']} (source STRING, listed_key STRING, output_key STRING,
  title_key STRING, guard_key STRING, text STRING, text_hash BIGINT, work_id BIGINT, score DOUBLE, checklist_p DOUBLE,
  in_band BOOLEAN, linked BOOLEAN, n_candidates INT, status STRING, decided_at TIMESTAMP, run_id STRING) USING delta""")

# COMMAND ----------

# ---------- the outputs to decide: publications unresolved today never decided as listed, and those that had no same-title
# work at their last look, every retry_days ----------


def guard_title(title):
    """The listed title the title guard compares: HTML entities read, a chapter's '; In: <book>' cut (GUARD.md, 23 Nov)."""
    return re.split(r';\s*in:', html.unescape(title or ''), maxsplit=1, flags=re.I)[0]


src_list = ', '.join(f"'{s}'" for s in SOURCES) or "''"
SELECTABLE = f"""
WITH unresolved AS (
  -- one row per output as listed (FWF lists one output under several projects, and renumbers its ids between fetches)
  SELECT source, listed_key(title, surnames) AS listed_key, min(output_key) AS output_key, any_value(title_key) AS title_key,
    any_value(title) AS title, any_value(surnames) AS surnames, any_value(year) AS year
  FROM {W['matches_table']}
  WHERE source IN ({src_list}) AND (work_id IS NULL OR tier = 'refmatch') AND output_type IS NOT NULL
    AND length(regexp_replace(title_key, '[\\\\p{{IsHan}}\\\\p{{IsHiragana}}\\\\p{{IsKatakana}}]', 'xxx')) >= {int(W['min_title_chars'])}   -- a kanji or kana counts 3
  GROUP BY 1, 2
)
SELECT u.*, d.listed_key IS NOT NULL AS retry FROM unresolved u
LEFT JOIN {W['decisions_table']} d ON d.source = u.source AND d.listed_key = u.listed_key
WHERE d.listed_key IS NULL
   OR (d.status = 'no_candidates' AND d.decided_at < current_timestamp() - INTERVAL {int(W['retry_days'])} DAYS)"""
# The backlog: the outputs never decided (retries are not counted). A night decides at most max_per_run outputs, new before
# retries and the oldest retries first, so a backlog drains over the nights instead of failing one. Its alarm: more never-
# decided outputs than the last run left (its backlog less what it decided) plus max_new (a lost picks table, a broken
# identifier tier) stops the task, unless allow_backlog_jump is set on purpose (a new source, a backfill's first run).
spark.sql(f"CREATE TABLE IF NOT EXISTS {W['runs_table']} (run_id STRING, at TIMESTAMP, backlog BIGINT, decided BIGINT) USING delta")
b = spark.sql(f"SELECT count_if(NOT retry) AS fresh, count(*) AS n FROM ({SELECTABLE})").first()
backlog = b.fresh
left = spark.sql(f"SELECT max_by(backlog - decided, at) AS left FROM {W['runs_table']}").first().left
if left is not None and backlog > left + int(W['max_new']) and W['allow_backlog_jump'].lower() != 'true':
    raise RuntimeError(f"backlog {backlog:,} never-decided outputs, over the {left:,} the last run left plus max_new {W['max_new']}: "
                       "if it is meant (a new source, a backfill), rerun with allow_backlog_jump true")
mpr = int(W['max_per_run'])
new = spark.sql(SELECTABLE + (f"\nORDER BY retry, d.decided_at, u.listed_key LIMIT {mpr}" if mpr > 0 else '')).toPandas()
log(f"{int((~new.retry).sum()):,} new outputs, {int(new.retry.sum()):,} looked at again (no same-title work {W['retry_days']}+ days ago);"
    f" backlog {backlog:,} never decided, {b.n - len(new):,} left for the next runs (max_per_run {mpr})")


def write_unmatched():
    """What is still unmatched: every listed publication decided and not accepted, as the funder lists it, for Casey's Match
    References to read in its list mode (text_hash, text, raw_text: the input its test path reads). Rebuilt every run."""
    spark.sql(f"""CREATE OR REPLACE TABLE {W['unmatched_table']} AS
SELECT p.source, p.listed_key, p.text_hash, p.text, p.text AS raw_text, p.status, p.decided_at, any_value(m.title) AS title,
  any_value(m.surnames) AS surnames, any_value(m.year) AS year, any_value(m.output_type) AS output_type,
  CASE WHEN any_value(m.title) RLIKE '[\\\\p{{IsHiragana}}\\\\p{{IsKatakana}}]' THEN 'ja'
       WHEN any_value(m.title) RLIKE '\\\\p{{IsHan}}' THEN 'cjk' END AS script,
  array_sort(collect_set(m.award_id)) AS award_ids, array_sort(collect_set(m.output_key)) AS output_keys
FROM {W['decisions_table']} p
JOIN {W['matches_table']} m ON m.source = p.source AND listed_key(m.title, m.surnames) = p.listed_key
WHERE m.output_type IS NOT NULL AND NOT coalesce(m.accepted, false)
  AND (NOT p.linked OR m.tier = 'refmatch')   -- a pick linked today counts once the links notebook has turned it down
GROUP BY p.source, p.listed_key, p.text_hash, p.text, p.status, p.decided_at""")


def record_run(decided):
    spark.sql(f"INSERT INTO {W['runs_table']} VALUES ('{RUN_ID}', current_timestamp(), {backlog}, {decided})")


if new.empty:
    write_unmatched()
    record_run(0)
    dbutils.notebook.exit(json.dumps({'run_id': RUN_ID, 'new': 0, 'backlog': backlog}))

# COMMAND ----------

# ---------- text, title key and candidates ----------

new['guard_title'] = new.title.map(guard_title)
new['surnames'] = new.surnames.map(lambda a: list(a) if a is not None else [])
new['year'] = new.year.map(lambda y: None if pd.isna(y) else int(y)).astype(object)
spark.createDataFrame(new[['source', 'listed_key', 'output_key', 'title_key', 'title', 'surnames', 'year', 'guard_title', 'retry']],
                      'source string, listed_key string, output_key string, title_key string, title string, surnames array<string>, '
                      'year int, guard_title string, retry boolean').createOrReplaceTempView('rm_new_raw')
spark.sql(f"""CREATE OR REPLACE TEMP VIEW rm_new AS
WITH f AS (
  SELECT *, filter(surnames, x -> length(trim(x)) >= 2 AND NOT trim(x) RLIKE '[.;]$') AS sn FROM rm_new_raw
),
r AS (
  -- the reference written the way the matcher's work vectors are: first 3 authors (et al). title. year
  SELECT f.*, concat_ws('. ',
      nullif(concat(array_join(slice(sn, 1, 3), ', '), CASE WHEN size(sn) > 3 THEN ' et al' ELSE '' END), ''),
      nullif(regexp_replace(trim(title), '[.\\\\s]+$', ''), ''), CAST(year AS STRING)) AS raw
  FROM f
),
k AS (
  -- Casey's text key reads the columns raw, authors, year and title (raw is never empty here)
  SELECT source, listed_key, output_key, title_key, retry, raw, match_text(guard_title) AS title_match_key,
    cjk_key(match_text(guard_title)) AS guard_key, {TEXT_SQL} AS text
  FROM (SELECT source, listed_key, output_key, title_key, retry, guard_title, raw, CAST(NULL AS STRING) AS authors,
          CAST(year AS STRING) AS year, title FROM r) t
)
SELECT *, xxhash64(text) AS qi FROM k""")
spark.sql(f"""CREATE OR REPLACE TEMP VIEW rm_cands AS
WITH q AS (SELECT DISTINCT qi, title_match_key AS t, guard_key AS g FROM rm_new),
v AS (
  -- the title as keyed, and for a Japanese or Chinese title the forms an OpenAlex title key may take: full- or half-width
  -- digits and letters, with or without a space where kanji or kana meet ASCII (cjk_key compares them all equal)
  SELECT qi, explode(CASE WHEN g RLIKE '[\\\\p{{IsHan}}\\\\p{{IsHiragana}}\\\\p{{IsKatakana}}]' THEN array_distinct(array(t, g,
      regexp_replace(g, '(?<=[a-z0-9])(?=[\\\\p{{IsHan}}\\\\p{{IsHiragana}}\\\\p{{IsKatakana}}])|(?<=[\\\\p{{IsHan}}\\\\p{{IsHiragana}}\\\\p{{IsKatakana}}])(?=[a-z0-9])', ' '),
      translate(t, '0123456789abcdefghijklmnopqrstuvwxyz', '０１２３４５６７８９ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ'), translate(g, '0123456789abcdefghijklmnopqrstuvwxyz', '０１２３４５６７８９ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ'))) ELSE array(t) END) AS key
  FROM q
),
p AS (SELECT DISTINCT v.qi, k.id AS cand FROM v JOIN {W['keys_table']} k ON k.nt = v.key)
SELECT * FROM p QUALIFY count(*) OVER (PARTITION BY qi) <= {int(W['max_same_title'])}""")
c = spark.sql("""SELECT count(DISTINCT qi) texts, count(*) pairs, count(DISTINCT cand) works,
  (SELECT count(DISTINCT qi) FROM rm_new) all_texts FROM rm_cands""").first()
est = (c.texts + c.works) * float(W['usd_per_million_texts']) / 1e6   # only texts with a candidate are embedded
log(f'{c.all_texts:,} texts, {c.texts:,} with candidates; {c.pairs:,} pairs on {c.works:,} works; embedding ~${est:.2f}')
if c.texts > int(W['max_new']):
    raise RuntimeError(f"{c.texts:,} texts to embed > max_new {W['max_new']}: raise max_new on purpose (a first fill) to decide them")
if est > float(W['max_usd']):
    raise RuntimeError(f"embedding estimate ${est:.2f} > max_usd {W['max_usd']}: raise max_usd on purpose")

# COMMAND ----------

# ---------- embed both sides with the endpoint every reference-style vector comes from, and score each pair ----------

spark.sql(f"""CREATE OR REPLACE TEMP VIEW rm_pairs AS
WITH qv AS (
  SELECT qi, text, ai_query('{ENDPOINT}', concat({INSTRUCTION}, text)) AS v
  FROM (SELECT DISTINCT qi, text FROM rm_new WHERE qi IN (SELECT qi FROM rm_cands))
),
w AS (SELECT * FROM {W['works_table']} WHERE id IN (SELECT cand FROM rm_cands)),
wv AS (SELECT w.id, ai_query('{ENDPOINT}', {WORK_TEXT}) AS v FROM w)
SELECT p.qi, p.cand, w.title, w.publication_year AS year, w.type, CAST(w.doi IS NOT NULL AS STRING) AS has_doi,
  slice(transform(w.authorships, a -> a.author.display_name), 1, 5) AS au, size(w.authorships) AS n_au,
  w.primary_location.source.display_name AS venue, w.biblio.volume AS vol, w.biblio.issue AS iss, w.biblio.first_page AS fp,
  w.biblio.last_page AS lp, cjk_key(match_text(w.title)) AS work_title_key,
  aggregate(zip_with(qv.v, wv.v, (x, y) -> CAST(x AS DOUBLE) * y), 0D, (s, x) -> s + x)
    / sqrt(aggregate(qv.v, 0D, (s, x) -> s + CAST(x AS DOUBLE) * x)) / sqrt(aggregate(wv.v, 0D, (s, x) -> s + CAST(x AS DOUBLE) * x)) AS score
FROM rm_cands p JOIN qv ON qv.qi = p.qi JOIN wv ON wv.id = p.cand JOIN w ON w.id = p.cand""")
pairs = spark.sql('SELECT * FROM rm_pairs').toPandas()
texts = spark.sql('SELECT * FROM rm_new').toPandas()
log(f'{len(pairs):,} scored pairs')
if pairs.score.isna().any():
    raise RuntimeError(f'{int(pairs.score.isna().sum()):,} pairs without a score: an embedding failed')

# COMMAND ----------

# ---------- Casey's decision, unchanged, and one row per new output ----------

PAIR_COLS = ['qi', 'cand', 'score', 'rank', 'from_lookup', 'cand_canon', 'title', 'year', 'type', 'au', 'n_au', 'venue', 'vol',
             'iss', 'fp', 'lp', 'rtext', 'ord']


def decide_pairs(pairs, raw, chk, comb, fe, bucket=2000):
    """Scored pairs (qi, cand, score and the work's fields) -> refmatch's decision per text, as the nightly makes it: rank by
    score, the checklist on every pair, the combiner on the unsure band (no reader: its features read 'unread'), the link
    line and the type tie-break."""
    P = pairs.copy()
    P['au'] = P.au.map(lambda a: list(a) if a is not None else [])
    for col in ('title', 'type', 'venue', 'vol', 'iss', 'fp', 'lp'):
        P[col] = P[col].astype(object).where(P[col].notna(), None)
    P['year'] = P.year.astype('float')
    P = P.sort_values(['qi', 'score', 'cand'], ascending=[True, False, True]).reset_index(drop=True)
    P['rank'] = P.groupby('qi').cumcount() + 1   # best score first, equal scores by work id (MatchReferences' search rank)
    P['ord'] = P.groupby('qi').cumcount() + 1
    P['from_lookup'], P['cand_canon'] = False, P.cand
    P['rtext'] = P.qi.map(raw)
    out = []
    for qis in np.array_split(P.qi.unique(), max(1, int(np.ceil(P.qi.nunique() / bucket)))):
        B = P[P.qi.isin(set(qis))]
        X = RM.features_bucket(B[PAIR_COLS].copy(), chk, fe)
        X['rp'] = np.nan
        out.append(RM.decide_bucket(X, B[['qi', 'cand_canon', 'type', 'title', 'au', 'year', 'venue', 'rtext', 'ord']].copy(), comb, fe))
    D = pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=list(RM.DECIDE_COLS))
    D['link'] = D.link.astype(bool) & ~D.in_band.astype(bool)   # the band is the reader's to decide, and the reader is not run
    return D


def decision_rows(texts, pairs, D):
    """One row per new output: the pick, its scores and a status (linked, band, not_linked, no_candidates, no_decision;
    linked_other_title when the pick's title is not the listed one, which the links notebook will not accept)."""
    wkey = dict(zip(zip(pairs.qi, pairs.cand), pairs.work_title_key))
    ncand = pairs.groupby('qi').size().to_dict()
    dec = D.drop_duplicates('qi').set_index('qi')
    rows = []
    for t in texts.itertuples():
        if t.qi not in dec.index and getattr(t, 'retry', False) and not ncand.get(t.qi):
            rows.append((t.source, t.listed_key, t.output_key, t.title_key, t.guard_key, t.text, int(t.qi), None, None, None, None,
                         False, 0, 'no_candidates'))   # looked at again: still none (its decided_at moves on)
            continue
        if t.qi not in dec.index:
            rows.append((t.source, t.listed_key, t.output_key, t.title_key, t.guard_key, t.text, int(t.qi), None, None, None, None,
                         False, int(ncand.get(t.qi, 0)), 'no_candidates' if not ncand.get(t.qi) else 'no_decision'))
            continue
        d = dec.loc[t.qi]
        work, linked, band = int(d.cand), bool(d.link), bool(d.in_band)
        status = 'linked' if linked else 'band' if band else 'not_linked'
        if linked and wkey.get((t.qi, work)) != t.guard_key:
            status = 'linked_other_title'
        rows.append((t.source, t.listed_key, t.output_key, t.title_key, t.guard_key, t.text, int(t.qi), work, float(d.s),
                     float(d.checklist_p), band, linked, int(ncand.get(t.qi, 0)), status))
    return rows

# COMMAND ----------

chk, comb, fe = RM.fit_models(W['model_dir'])
n_bench, same = RM.check_bench(W['model_dir'], chk, comb, fe)
if same != n_bench:
    raise RuntimeError(f"review bench: {same} of {n_bench} decisions as #2000's; refmatch.py or a library changed")
log(f'bench {same}/{n_bench}')
D = decide_pairs(pairs, dict(zip(texts.qi, texts.raw)), chk, comb, fe)
log(f"{len(D):,} decided: {int(D.link.sum()):,} linked, {int(D.in_band.sum()):,} in the reader's band")
rows = decision_rows(texts, pairs, D)
out = spark.createDataFrame(rows, 'source string, listed_key string, output_key string, title_key string, guard_key string, text string, '
                                  'text_hash bigint, work_id bigint, score double, checklist_p double, in_band boolean, '
                                  'linked boolean, n_candidates int, status string')
out.createOrReplaceTempView('rm_out')
spark.sql(f"""MERGE INTO {W['decisions_table']} t
USING (SELECT *, current_timestamp() AS decided_at, '{RUN_ID}' AS run_id FROM rm_out) s
ON t.source = s.source AND t.listed_key = s.listed_key
WHEN MATCHED AND t.status = 'no_candidates' THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *""")
write_unmatched()
record_run(int((~new.retry).sum()))   # the never-decided outputs this run decided
summary = spark.sql(f"SELECT status, count(*) n FROM {W['decisions_table']} WHERE run_id = '{RUN_ID}' GROUP BY 1").toPandas()
info = {'run_id': RUN_ID, 'new': len(new), 'backlog': backlog, 'texts_with_candidates': int(c.texts), 'pairs': int(c.pairs), 'embed_usd_est': round(est, 2),
        'statuses': dict(zip(summary.status, summary.n.astype(int))), 'seconds': round(time.time() - T0, 1)}
log(json.dumps(info))
dbutils.notebook.exit(json.dumps(info))
