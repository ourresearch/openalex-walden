"""CreatePubmedWorkFunders (oxjob #2544): PubMed's own GrantList -> pubmed_work_funders, a Nightly Grants leg.

Contract tests, offline. The notebook's behaviour on live data (agency resolution agreeing with CreateEuropePmcWorkFunders'
Python resolver on every in-scope string, rows, shells, a Nightly Grants check run) is checked on scratch copies before
it ships; see oxjob #2544.
"""
import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
SQL = (ROOT / 'notebooks/awards/CreatePubmedWorkFunders.sql').read_text()
CELLS = SQL.split('-- COMMAND ----------')
EPMC = (ROOT / 'notebooks/awards/CreateEuropePmcWorkFunders.py').read_text()
LEG, RAW = 'openalex.awards.pubmed_work_funders', 'openalex.awards.openalex_awards_raw'
PROVENANCE = 'pubmed_work_funders'


def code(text):
    return '\n'.join(re.sub(r'\s+--\s.*$', '', l) for l in text.splitlines() if not l.strip().startswith('--'))


def cell(needle):
    found = [c for c in CELLS if needle in c]
    assert len(found) == 1, (needle, len(found))
    return found[0]


def squash(s):
    return ' '.join(s.split())


def job():
    d = yaml.safe_load((ROOT / 'jobs/funder_reported_work_funders.yaml').read_text())
    return {t['task_key']: t for t in d['resources']['jobs']['Funder_Reported_Work_Funders']['tasks']}


def test_writes_only_its_leg_and_its_own_shells():
    writes = re.findall(r'\b(INSERT INTO|MERGE INTO|DELETE FROM|UPDATE(?! SET)|CREATE (?:OR REPLACE )?TABLE(?: IF NOT EXISTS)?)\s+(\S+)', code(SQL))
    assert writes == [('CREATE TABLE IF NOT EXISTS', 'IDENTIFIER(pwf_leg)'), ('MERGE INTO', 'IDENTIFIER(pwf_leg)'),
                      ('MERGE INTO', 'IDENTIFIER(pwf_raw)')], writes
    raw = squash(code(cell('MERGE INTO IDENTIFIER(pwf_raw)')))
    assert f"ON t.provenance = '{PROVENANCE}' AND t.id = s.id" in raw
    assert f"WHEN NOT MATCHED BY SOURCE AND t.provenance = '{PROVENANCE}' THEN DELETE" in raw   # never another source's rows
    assert raw.rstrip().endswith(f"WHEN NOT MATCHED BY SOURCE AND t.provenance = '{PROVENANCE}' THEN DELETE")
    assert "s.updated_date, 0)" in raw   # priority 0: a paper-side shell, never a registry (direct) record


def test_targets_are_production_or_its_own_scratch():
    guard = squash(code(CELLS[1]))
    assert f":leg_table = '{LEG}' AND :raw_table = '{RAW}'" in guard
    assert "RLIKE '^openalex[.]oxlane[.]oxjob2544_[a-z0-9_]+$'" in guard
    task = job()['create_pubmed_work_funders']
    assert task['notebook_task']['notebook_path'] == 'notebooks/awards/CreatePubmedWorkFunders'
    assert task['notebook_task']['base_parameters'] == {'leg_table': LEG, 'raw_table': RAW, 'before_year': '2000',
                                                        'first_fill_max': '1500000'}


def test_scope_is_pre_pmc_public_health_service_grants():
    mentions = squash(code(cell('CREATE OR REPLACE TEMPORARY VIEW pwf_mentions')))
    assert "lm.provenance = 'pubmed' AND lm.work_id IS NOT NULL" in mentions
    assert 'lm.published_date < pwf_before' in mentions
    assert "trim(f.name) RLIKE '(HHS| VA)$'" in mentions
    assert 'LATERAL VIEW OUTER explode(x.awards)' in mentions   # an agency with no grant keeps its funder link


def test_nightly_reads_the_leg_like_europe_pmc():
    nightly = (ROOT / 'notebooks/awards/nightly_grants/lib/work_awards.py').read_text()
    epmc = re.search(r"\('europepmc_work_funders'(,[^)]*)\)", nightly).group(1)
    assert re.search(r"\('pubmed_work_funders'" + re.escape(epmc) + r"\)", nightly)   # same priority, sharp, mode
    runtime = (ROOT / 'notebooks/awards/nightly_grants/lib/nightly_runtime.py').read_text()
    assert '"pubmed_work_funders"' in runtime.split('REQUIRED_INPUTS', 1)[1].split('}', 1)[0]
    config = json.loads((ROOT / 'notebooks/awards/nightly_grants/config.prod.json').read_text())
    assert config['inputs']['pubmed_work_funders'] == LEG


def test_every_deposited_list_names_the_leg():
    """The paper-side provenances (work_awards.DEPOSITED) are copied into SQL notebooks; all copies must agree."""
    nightly = (ROOT / 'notebooks/awards/nightly_grants/lib/work_awards.py').read_text()
    deposited = re.search(r'^DEPOSITED = "(\(.*?\))"', nightly, re.M).group(1)
    assert f"'{PROVENANCE}'" in deposited and "'europepmc_work_funders'" in deposited
    copies = 0
    for p in sorted((ROOT / 'notebooks').rglob('*')):
        if p.suffix not in ('.sql', '.py', '.ipynb') or 'tests' in p.parts:
            continue
        for lst in re.findall(r"\('crossref_work_funders','crossref_work\.grants'[^)]*\)", p.read_text()):
            assert lst == deposited, (str(p), lst)
            copies += 1
    assert copies >= 4


def test_union_reads_the_leg_after_it_is_rebuilt():
    union = (ROOT / 'notebooks/awards/CreateFunderReportedWorkFunders.ipynb').read_text()
    assert f"UNION SELECT work_id, funder_id, '{PROVENANCE}'    FROM {LEG}" in union
    tasks = job()
    assert tasks['create_funder_reported_work_funders']['depends_on'] == [{'task_key': 'create_pubmed_work_funders'}]
    assert tasks['create_funder_reported_work_funders']['run_if'] == 'ALL_DONE'   # a failure here never stops the union
    assert 'depends_on' not in tasks['create_pubmed_work_funders']


def test_shells_are_minted_as_europe_pmc_mints_them():
    def select(text):
        body = squash(text.split('FROM guarded e JOIN funders f ON f.funder_id = e.funder_id', 1)[0])
        return body[body.index('guarded AS ('):]
    epmc_view = EPMC.split('CREATE OR REPLACE TEMPORARY VIEW europepmc_work_funder_awards AS', 1)[1].split('# COMMAND', 1)[0]
    epmc_view = re.sub(r'^# MAGIC ?', '', epmc_view, flags=re.M)
    epmc_view = '\n'.join(l for l in epmc_view.splitlines() if not l.strip().startswith('--'))
    mine = code(cell('CREATE OR REPLACE TEMPORARY VIEW pwf_shells'))
    expected = select(epmc_view).replace("'europepmc_work_funders' AS provenance", f"'{PROVENANCE}' AS provenance")
    expected = expected.replace('CAST(NULL AS INT) AS start_year, CAST(NULL AS INT) AS end_year',
                                'CAST(NULL AS BIGINT) AS start_year, CAST(NULL AS BIGINT) AS end_year')
    assert select(mine) == expected
    erc = lambda t: squash(t[t.index("WHERE NOT (coalesce(f.display_name"):]).split(' -- ')[0].rstrip(';')
    assert erc(mine) == erc(epmc_view)


def test_resolver_follows_europe_pmc_branch_order():
    """The SQL CASE takes CreateEuropePmcWorkFunders.resolve()'s acronym branches in the same order and fallbacks."""
    py = [m for m in re.findall(r'if st\.endswith\("([^"]+)"\)(?: and spec)?:\s+return (spec or acr\.get\("(\w+)"\)|spec)', EPMC)]
    assert py == [('NIH HHS', 'spec or acr.get("NIH")', 'NIH'), ('CDC HHS', 'spec or acr.get("CDC")', 'CDC'),
                  (' VA', 'spec or acr.get("VA")', 'VA'), ('HHS', 'spec', '')], py
    assert 'spec = acr.get(first) or acr.get(first.upper())' in EPMC
    case = squash(code(cell('CREATE OR REPLACE TEMPORARY VIEW pwf_agency_funder')))
    assert re.findall(r"WHEN s\.agency LIKE '%( ?[A-Z ]+)'", case) == ['NIH HHS', 'CDC HHS', ' VA', 'HHS']
    for parent in ('NIH', 'CDC', 'VA'):
        assert f"coalesce(s.spec, (SELECT funder_id FROM pwf_nih_map WHERE acr = '{parent}'))" in case
    assert 'm1.acr = a.first_token' in case and 'm2.acr = upper(a.first_token)' in case
    assert "nih_funder_map.json" in EPMC and "read_files('s3://openalex-ingest/awards/europepmc/nih_funder_map.json'" in SQL


def test_gates_run_before_any_write():
    for gate in ('PWF_REGEX_MODE', 'PWF_NULL_KEY', 'PWF_FUNDER_NOT_LIVE', 'PWF_SHRINK_GUARD', 'PWF_GROWTH_CEILING', 'PWF_SHELL_CEILING'):
        assert SQL.index(gate) < SQL.index('MERGE INTO IDENTIFIER(pwf_leg)'), gate
    assert SQL.index('MERGE INTO IDENTIFIER(pwf_leg)') < SQL.index('MERGE INTO IDENTIFIER(pwf_raw)')


def test_regex_backslashes_are_double_escaped_and_no_dollar_parameters():
    body = code(SQL)
    assert not re.search(r'(?<!\\)\\(?!\\)', body.replace('\\\\', '')), 'a single backslash in SQL'
    assert not re.search(r'\$(\d|[A-Za-z_{])', body), 'a dollar parameter a job run would blank'
