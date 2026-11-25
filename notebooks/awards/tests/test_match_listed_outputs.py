"""MatchListedOutputs: Casey's reference matcher for the publications funders list without an identifier.

The contract tests run anywhere. The decision test runs Casey's decision code on recorded pairs (fixtures/
match_listed_outputs_pairs.json, from the 23 Nov dev run on FWF) and needs his training rows: set REFMATCH_MODEL_DIR to a copy
of /Volumes/openalex/works/data/reference_matcher/model (on Databricks, that path). Elsewhere it skips.
"""
import ast
import json
import os
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
NB = ROOT / 'notebooks/awards/MatchListedOutputs.py'
SRC = NB.read_text()
CELLS = SRC.split('# COMMAND ----------')
LINKS_SQL = (ROOT / 'notebooks/awards/CreateFunderReportedLinks.sql').read_text()
MATCHER = ROOT / 'notebooks/references/matcher'
FIXTURE = Path(__file__).resolve().parent / 'fixtures/match_listed_outputs_pairs.json'


class Stop(Exception):
    pass


class Widgets:
    def __init__(self):
        self.v = {}

    def text(self, k, default):
        self.v.setdefault(k, default)

    def get(self, k):
        return self.v[k]


class Spark:
    def __init__(self):
        self.statements = []

    def sql(self, s):
        self.statements.append(s)


def cell(needle):
    found = [c for c in CELLS if needle in c]
    assert len(found) == 1, (needle, len(found))
    return found[0]


def first_cell():
    """Run the notebook's first code cell (imports, widgets, the rules read from their files) with a stand-in spark."""
    g = {'dbutils': type('D', (), {'widgets': Widgets()})(), 'spark': Spark(), '__name__': 'test'}
    cwd = os.getcwd()
    os.chdir(ROOT / 'notebooks/awards')
    try:
        body = cell('import refmatch as RM')
        exec(compile(body, str(NB), 'exec'), g)
    finally:
        os.chdir(cwd)
    return g


def defs_cell(name):
    """The cell that defines `name`, executed on its own (it holds only definitions)."""
    found = [c for c in CELLS if f'def {name}(' in c]
    assert len(found) == 1, name
    pd = pytest.importorskip('pandas')
    np = pytest.importorskip('numpy')
    sys.path.insert(0, str(MATCHER))
    import refmatch as RM
    import html
    g = {'RM': RM, 'pd': pd, 'np': np, 're': re, 'html': html}
    tree = ast.parse(found[0])
    tree.body = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                 or (isinstance(n, ast.Assign) and isinstance(n.value, (ast.List, ast.Tuple, ast.Constant)))]   # definitions only
    exec(compile(tree, str(NB), 'exec'), g)
    return g


# ---------------------------------------------------------------- contract

def test_imports_caseys_matcher_and_copies_none_of_it():
    assert "MATCHER = os.path.normpath(os.path.join(HERE, '..', 'references', 'matcher'))" in SRC
    assert 'import refmatch as RM' in SRC
    theirs = {n.name for n in ast.parse((MATCHER / 'refmatch.py').read_text()).body if isinstance(n, ast.FunctionDef)}
    mine = {n.name for n in ast.parse(SRC).body if isinstance(n, ast.FunctionDef)}
    assert not theirs & mine, theirs & mine
    for f in ('fit_models', 'check_bench', 'features_bucket', 'decide_bucket'):
        assert f'RM.{f}(' in SRC, f


def test_reads_each_rule_from_its_one_place():
    g = first_cell()
    assert g['SOURCES'] == ['nwopen', 'fwf_open_api', 'anr_opendata', 'kaken', 'hakai_publications', 'cordis', 'erc']   # every source;
    # a source that lists only DOIs has no titled publication to select (output_type and title stay NULL)
    assert "CREATE OR REPLACE TABLE {W['unmatched_table']} AS" in SRC and 'WHERE m.output_type IS NOT NULL AND NOT coalesce(m.accepted, false)' in SRC
    assert 'p.text_hash, p.text, p.text AS raw_text' in SRC   # Match References' test_texts columns
    assert SRC.count('write_unmatched()') == 3   # defined, and run on both exits (nothing new, decided)
    assert g['MATCH_TEXT'] == re.search(r'CREATE OR REPLACE TEMPORARY FUNCTION match_text.*?;', LINKS_SQL, re.S).group(0).rstrip(';')
    key = (MATCHER / 'sql/ref_text_key.sql').read_text()
    assert g['TEXT_SQL'].startswith('left(trim(regexp_replace(regexp_replace(coalesce(nullif(trim(raw)') and g['TEXT_SQL'] in key.replace('\n', ' ')
    assert g['INSTRUCTION'] == "'Instruct: Given a web search query, retrieve relevant passages that answer the query\\nQuery: '"
    embed = (ROOT / 'notebooks/vector_search/EmbedWorkReferenceStyleIncremental.py').read_text()
    assert f'TEXT = """{g["WORK_TEXT"]}"""' in embed and 'w.authorships' in g['WORK_TEXT']
    assert g['ENDPOINT'] == 'databricks-qwen3-embedding-0-6b' and f'ENDPOINT = "{g["ENDPOINT"]}"' in embed
    assert g['LISTED_KEY'] == re.search(r'CREATE OR REPLACE TEMPORARY FUNCTION listed_key\(.*?;', LINKS_SQL, re.S).group(0).rstrip(';')
    assert g['spark'].statements[:2] == [g['MATCH_TEXT'], g['LISTED_KEY']]


def test_the_decisions_table_has_one_schema_in_both_notebooks():
    mine = re.search(r"CREATE TABLE IF NOT EXISTS \{W\['decisions_table'\]\} \((.*?)\) USING delta", SRC, re.S).group(1)
    links = re.search(r'CREATE TABLE IF NOT EXISTS openalex\.awards\.funder_reported_refmatch_picks \((.*?)\) USING delta', LINKS_SQL, re.S).group(1)
    assert ' '.join(mine.split()) == ' '.join(links.split())
    assert "('decisions_table', 'openalex.awards.funder_reported_refmatch_picks')" in SRC
    assert "('matches_table', 'openalex.awards.funder_reported_link_matches')" in SRC


def test_cost_guard_stops_before_anything_is_embedded():
    first_embed = SRC.index("ai_query('{ENDPOINT}'")
    for guard in ("raise RuntimeError(f\"{c.texts:,} texts to embed > max_new {W['max_new']}",
                  "raise RuntimeError(f\"embedding estimate ${est:.2f} > max_usd {W['max_usd']}"):
        assert SRC.index(guard) < first_embed, guard
    assert "('max_new', '5000'), ('max_usd', '5')" in SRC   # a week of new FWF outputs is a few hundred; a first fill raises them
    assert SRC.index("MERGE INTO {W['decisions_table']}") > SRC.index('RM.check_bench(')   # nothing written before the bench passes


def test_decides_only_new_unresolved_publications_and_writes_each_once():
    sel = ' '.join(cell('unresolved AS (').split())
    assert "WHERE source IN ({src_list}) AND (work_id IS NULL OR tier = 'refmatch') AND output_type IS NOT NULL" in sel
    assert "SELECT source, listed_key(title, surnames) AS listed_key, min(output_key) AS output_key" in sel   # FWF renumbers ids
    assert "LEFT JOIN {W['decisions_table']} d ON d.source = u.source AND d.listed_key = u.listed_key" in sel
    # never decided, or no same-title work at the last look, retry_days ago: decided once otherwise
    assert ("WHERE d.listed_key IS NULL OR (d.status = 'no_candidates' AND d.decided_at < current_timestamp() - INTERVAL "
            "{int(W['retry_days'])} DAYS)") in sel
    merge = ' '.join(SRC.split("MERGE INTO {W['decisions_table']} t", 1)[1].split('""")', 1)[0].split())
    assert merge.endswith("ON t.source = s.source AND t.listed_key = s.listed_key WHEN MATCHED AND t.status = 'no_candidates' "
                          "THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")   # a decision is never overwritten


def test_candidates_are_the_works_the_title_guard_admits():
    c = ' '.join(SRC.split())
    assert "p AS (SELECT DISTINCT v.qi, k.id AS cand FROM v JOIN {W['keys_table']} k ON k.nt = v.key)" in c
    assert 'ELSE array(t) END) AS key' in c   # a title without kanji or kana is looked up as keyed, nothing else
    assert "cjk_key(match_text(guard_title)) AS guard_key" in c and "cjk_key(match_text(w.title)) AS work_title_key" in c
    assert 'ORDER BY retry, d.decided_at, u.listed_key LIMIT {mpr}' in c   # new first, then the oldest retries: none starve
    assert "if left is not None and backlog > left + int(W['max_new']) and W['allow_backlog_jump'].lower() != 'true':" in c
    assert "count_if(NOT retry) AS fresh" in c and 'record_run(int((~new.retry).sum()))' in c   # retries never trip the alarm
    assert "RLIKE '[\\\\\\\\p{{IsHiragana}}\\\\\\\\p{{IsKatakana}}]' THEN 'ja'" in c   # escaped for the f-string and the SQL literal
    assert "QUALIFY count(*) OVER (PARTITION BY qi) <= {int(W['max_same_title'])}" in c
    assert "('keys_table', 'openalex.works.location_work_match_keys')" in SRC   # the title tier's table, location_work_match_keys.nt


def test_guard_title_cuts_a_chapters_book_and_reads_entities():
    g = defs_cell('guard_title')
    assert g['guard_title']('Stiftung und X; In: Medialitäten von Heiligkeit') == 'Stiftung und X'
    assert g['guard_title']('A &ndash; B;in:C') == 'A – B'
    assert g['guard_title']('No book here; a semicolon') == 'No book here; a semicolon'
    assert g['guard_title'](None) == ''


def test_the_daily_job_runs_it_after_the_links_task_on_serverless():
    yaml = pytest.importorskip('yaml')
    job = yaml.safe_load((ROOT / 'jobs/funder_reported_work_funders.yaml').read_text())['resources']['jobs']['Funder_Reported_Work_Funders']
    task = {t['task_key']: t for t in job['tasks']}['match_listed_outputs']
    assert task['depends_on'] == [{'task_key': 'create_funder_reported_links'}] and task['run_if'] == 'ALL_DONE'
    assert task['notebook_task'] == {'notebook_path': 'notebooks/awards/MatchListedOutputs', 'source': 'GIT'}
    assert task['environment_key'] == 'Matcher' and task['timeout_seconds'] == 3600
    assert task['email_notifications']['on_failure'] == ['rohan@openalex.org']
    env = {e['environment_key']: e['spec'] for e in job['environments']}['Matcher']
    theirs = yaml.safe_load((ROOT / 'jobs/match_references.yaml').read_text())['resources']['jobs']['Match_References']['environments'][0]['spec']
    assert env == theirs   # the scikit-learn that refits Casey's models exactly, as his own job pins it
    links = {t['task_key']: t for t in job['tasks']}['create_funder_reported_links']
    assert 'depends_on' not in links   # the links task never waits for the matcher: it reads the decisions it has


# ---------------------------------------------------------------- decision, on recorded pairs

@pytest.fixture(scope='module')
def models():
    d = os.environ.get('REFMATCH_MODEL_DIR')
    if not d or not Path(d, 'feats.json').exists():
        pytest.skip("needs Casey's training rows: REFMATCH_MODEL_DIR")
    pytest.importorskip('sklearn')
    g = defs_cell('decide_pairs')
    return g, g['RM'].fit_models(d)


def test_decides_recorded_pairs_as_recorded_in_any_row_order_and_ties_go_to_the_lower_work_id(models):
    pd = pytest.importorskip('pandas')
    g, (chk, comb, fe) = models
    rec = json.loads(FIXTURE.read_text())
    pairs, texts = pd.DataFrame(rec['pairs']), pd.DataFrame(rec['texts'])
    raw = dict(zip(texts.qi, texts.raw))
    want = {int(k): tuple(v) for k, v in rec['decided'].items()}
    for seed in (0, 1, 2):   # the pick never depends on the order Spark hands the pairs over
        D = g['decide_pairs'](pairs.sample(frac=1, random_state=seed), raw, chk, comb, fe)
        assert {int(r.qi): (int(r.cand), bool(r.link), bool(r.in_band)) for r in D.itertuples()} == want
    tied = pairs[pairs.duplicated(['qi', 'score'], keep=False)]
    assert len(tied), 'the fixture keeps one tie at the top score'
    for q, grp in tied.groupby('qi'):
        if grp.score.max() == pairs[pairs.qi == q].score.max():
            assert want[int(q)][0] == int(grp.cand.min())
    rows = g['decision_rows'](texts.assign(retry=False), pairs, D)
    assert {r[1]: r[-1] for r in rows} == rec['statuses']
    assert not any(r[10] and r[11] for r in rows)   # (in_band, linked): a pick in the reader's band never links


def test_flags_a_pick_with_another_title_a_text_without_a_decision_and_a_retry_still_without_a_candidate(models):
    pd = pytest.importorskip('pandas')
    g, (chk, comb, fe) = models
    rec = json.loads(FIXTURE.read_text())
    pairs, texts = pd.DataFrame(rec['pairs']), pd.DataFrame(rec['texts']).assign(retry=False)
    linked = next(int(k) for k, v in rec['decided'].items() if v[1])
    pairs.loc[(pairs.qi == linked) & (pairs.cand == rec['decided'][str(linked)][0]), 'work_title_key'] = 'another title'
    no_title = pairs[pairs.qi == linked].head(1).assign(qi=111, title=None)          # its only candidate has no title
    pairs = pd.concat([pairs, no_title], ignore_index=True)
    extra = texts.head(1).assign(qi=111, listed_key='k-no-title|x')
    retry = texts.head(1).assign(qi=222, listed_key='k-retry|x', retry=True)        # looked at again, still no candidate
    texts = pd.concat([texts, extra, retry], ignore_index=True)
    D = g['decide_pairs'](pairs, dict(zip(texts.qi, texts.raw)), chk, comb, fe)
    st = {r[1]: r[-1] for r in g['decision_rows'](texts, pairs, D)}
    key = texts.set_index('qi').listed_key
    assert st[key[linked]] == 'linked_other_title'   # the links notebook will not accept it
    assert st['k-no-title|x'] == 'no_decision' and st['k-retry|x'] == 'no_candidates'


# ---------------------------------------------------------------- behaviour: the whole notebook on fixture tables

@pytest.fixture(scope='module')
def spark():
    if not (os.environ.get('DATABRICKS_CONFIG_PROFILE') and os.environ.get('LINKS_TEST_SCHEMA') and os.environ.get('REFMATCH_MODEL_DIR')):
        pytest.skip('needs Databricks Connect, a scratch schema and the model rows (DATABRICKS_CONFIG_PROFILE, LINKS_TEST_SCHEMA, REFMATCH_MODEL_DIR)')
    dc = pytest.importorskip('databricks.connect')
    s = dc.DatabricksSession.builder.profile(os.environ['DATABRICKS_CONFIG_PROFILE']).serverless(True).getOrCreate()
    yield s
    s.sql(f'DROP TABLE IF EXISTS {PICKS}')
    s.sql(f'DROP TABLE IF EXISTS {UNMATCHED}')
    s.sql(f'DROP TABLE IF EXISTS {RUNS}')


PICKS = f"{os.environ.get('LINKS_TEST_SCHEMA', 'scratch')}.mlo_test_picks"
UNMATCHED = f"{os.environ.get('LINKS_TEST_SCHEMA', 'scratch')}.mlo_test_unmatched"   # never the production table
RUNS = f"{os.environ.get('LINKS_TEST_SCHEMA', 'scratch')}.mlo_test_runs"
TITLE = 'Polarizable molecular dynamics simulations of ionic liquids at charged interfaces'
LATER = 'A thesis on the glass transition of colloids that OpenAlex gains later'
JA_FULL, JA_HALF = '小学校社会科指導法に関する遠隔学修教材の作成（２）', '小学校社会科指導法に関する遠隔学修教材の作成 (2)'
JA_SHORT = 'たばこ訴訟の論点と課題'
JA_NONE = '日本語教育の現状と課題についての一考察'


def nt(t):   # match_text: letters and digits of any script
    return ' '.join(re.sub(r'[\W_]+', ' ', t.lower()).split())


class Exit(Exception):
    pass


def views(spark, later_work=False, extra=0):
    """Today's matches (FWF: two unresolved publications, a talk, a DOI-resolved article), the title keys and the works."""
    m = [('fwf_open_api', 'P1', 'o-1', TITLE, nt(TITLE), ['Heid', 'Schroeder'], None, None, None, 'article'),
         ('fwf_open_api', 'P2', 'o-1', TITLE, nt(TITLE), ['Heid', 'Schroeder'], None, None, None, 'article'),  # under two grants
         ('fwf_open_api', 'P1', 'o-2', LATER, nt(LATER), ['Heid'], None, None, None, 'thesis'),
         ('fwf_open_api', 'P1', 'o-3', TITLE, nt(TITLE), ['Heid'], None, None, None, None),                     # a talk
         ('fwf_open_api', 'P1', 'o-4', 'A paper its DOI names', nt('A paper its DOI names'), ['Heid'], None, 7, 'doi', 'article'),
         ('kaken', 'K1', 'k-1', JA_FULL, nt(JA_FULL), [], 2023, None, None, 'article'),    # full-width (２); OpenAlex keys ' 2'
         ('kaken', 'K1', 'k-2', JA_SHORT, nt(JA_SHORT), [], 2009, None, None, 'article'),  # 11 kanji/kana: kept (counts 33)
         ('kaken', 'K1', 'k-3', JA_NONE, nt(JA_NONE), [], 2012, None, None, 'article')]     # no such work: stays unmatched
    m += [('fwf_open_api', 'P9', f'x-{i}', f'An output nobody expected number {i}', nt(f'An output nobody expected number {i}'),
           ['Heid'], None, None, None, 'article') for i in range(extra)]   # a sudden backlog (the alarm)
    m = [r + (r[8] is not None,) for r in m]   # accepted: as the links notebook left it (a DOI link here)
    spark.createDataFrame(m, 'source string, award_id string, output_key string, title string, title_key string, '
                          'surnames array<string>, year int, work_id bigint, tier string, output_type string, accepted boolean'
                          ).createOrReplaceTempView('t_mlo_matches')
    works = [(501, TITLE, 2019, 'article', '10.1/a', ['Elisa Heid', 'Christian Schroeder'], 'The Journal of Chemical Physics'),
             (502, TITLE, 2019, 'dataset', None, ['Zenodo Uploader'], 'Zenodo'),
             (505, JA_HALF, 2023, 'article', None, ['吉永 潤'], '教育科学論集'), (506, JA_SHORT, 2009, 'article', None, ['田中 謙'], '法学論集')]
    if later_work:
        works.append((503, LATER, 2022, 'dissertation', None, ['Elisa Heid'], 'University of Vienna'))
    spark.createDataFrame([(w[0], nt(w[1])) for w in works], 'id bigint, nt string').createOrReplaceTempView('t_mlo_keys')
    spark.createDataFrame([(i, t, y, ty, d, [((a,),) for a in au], ((v,),), (None, None, None, None)) for i, t, y, ty, d, au, v in works],
                          'id bigint, title string, publication_year int, type string, doi string, '
                          'authorships array<struct<author:struct<display_name:string>>>, primary_location struct<source:struct<display_name:string>>, '
                          'biblio struct<volume:string,issue:string,first_page:string,last_page:string>').createOrReplaceTempView('t_mlo_works')


def run(spark, run_id, max_new='10', allow='false', max_per_run=None):
    w = {'matches_table': 't_mlo_matches', 'decisions_table': PICKS, 'unmatched_table': UNMATCHED, 'runs_table': RUNS, 'keys_table': 't_mlo_keys',
         'works_table': 't_mlo_works',
         'model_dir': os.environ['REFMATCH_MODEL_DIR'], 'max_new': max_new, 'max_usd': '1', 'run_id': run_id}
    widgets = Widgets()
    widgets.v.update(w)
    widgets.v['allow_backlog_jump'] = allow
    if max_per_run is not None:
        widgets.v['max_per_run'] = max_per_run
    out = {}

    def exit_(s):
        out.update(json.loads(s))
        raise Exit()
    g = {'spark': spark, 'dbutils': type('D', (), {'widgets': widgets, 'notebook': type('N', (), {'exit': staticmethod(exit_)})})(),
         '__name__': 'test'}
    cwd = os.getcwd()
    os.chdir(ROOT / 'notebooks/awards')
    try:
        for c in CELLS:
            body = '\n'.join(l for l in c.splitlines() if not l.startswith('# MAGIC'))
            exec(compile(body, str(NB), 'exec'), g)
    except Exit:
        pass
    finally:
        os.chdir(cwd)
    return out


def picks(spark):
    return {r.output_key: (r.status, r.work_id, r.n_candidates) for r in spark.sql(f'SELECT * FROM {PICKS}').collect()}


def test_the_notebook_decides_each_output_once_and_looks_again_at_one_without_a_candidate(spark):
    spark.sql(f'DROP TABLE IF EXISTS {PICKS}')
    views(spark)
    first = run(spark, 't1')
    got = picks(spark)
    # o-1 once (two grants, one listing); o-2 has no same-title work; the talk and the DOI-resolved article are not decided
    assert set(got) == {'o-1', 'o-2', 'k-1', 'k-2', 'k-3'} and first['new'] == 5 and first['backlog'] == 5, (got, first)
    um = {r.listed_key: (r.status, r.script) for r in spark.table(UNMATCHED).collect()}   # what Casey's list mode would read
    assert um[nt(LATER) + '|heid'] == ('no_candidates', None) and um[nt(JA_NONE)] == ('no_candidates', 'ja'), um   # listed_key
    assert not any(k.startswith(nt(TITLE)) for k in um)   # linked today: not unmatched (the links run has not judged it yet)
    for k in ('k-1', 'k-2'):   # found under the key OpenAlex holds, and the title guard reads them as the same title
        assert got[k][2] == 1 and got[k][0] != 'linked_other_title', (k, got[k])
    assert got['o-1'][1] == 501 and got['o-1'][2] == 2           # of the two same-title records, the article with its authors
    assert got['o-2'] == ('no_candidates', None, 0)
    with pytest.raises(Exception, match='texts to embed'):       # the cost guard stops before anything is embedded
        spark.sql(f"DELETE FROM {PICKS} WHERE output_key = 'o-1'")
        run(spark, 't2', max_new='0', allow='true')               # (past the backlog alarm on purpose: this is the cost guard)
    run(spark, 't3')                                             # o-1 decided again, nothing else
    assert picks(spark) == got
    assert run(spark, 't4') == {'run_id': 't4', 'new': 0, 'backlog': 0}   # decided once: nothing new, no backlog
    views(spark, later_work=True)                               # OpenAlex gains the thesis o-2 is
    spark.sql(f"UPDATE {PICKS} SET decided_at = current_timestamp() - INTERVAL 8 DAYS WHERE output_key = 'o-2'")
    assert run(spark, 't5')['backlog'] == 0   # a retry is not backlog: looking at o-2 again never trips the alarm
    after = picks(spark)
    assert after['o-2'][1] == 503 and after['o-2'][0] != 'no_candidates' and after['o-1'] == got['o-1']
    views(spark, later_work=True, extra=11)                      # 11 never-decided outputs appear: over max_new (10)
    with pytest.raises(Exception, match='allow_backlog_jump'):
        run(spark, 't6')
    assert run(spark, 't7', allow='true')['new'] == 11            # meant: decided, and the next run's baseline
    # retries go oldest first, so none starve: x-7 (9 days) before x-3 (8 days), though x-3's key sorts first
    spark.sql(f"UPDATE {PICKS} SET decided_at = current_timestamp() - INTERVAL 9 DAYS WHERE output_key = 'x-7'")
    spark.sql(f"UPDATE {PICKS} SET decided_at = current_timestamp() - INTERVAL 8 DAYS WHERE output_key = 'x-3'")
    run(spark, 't8', max_per_run='1')
    at = {r.output_key: r.run_id for r in spark.table(PICKS).where("output_key IN ('x-3', 'x-7')").collect()}
    assert at == {'x-7': 't8', 'x-3': 't7'}, at
