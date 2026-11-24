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
    assert tasks['create_funder_reported_work_funders']['depends_on'] == [   # and the links it reads for ANR and Hakai (#2014)
        {'task_key': 'create_pubmed_work_funders'}, {'task_key': 'create_funder_reported_links'}]
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


def test_shells_only_for_grants_no_award_carries():
    """A shell for a grant that is already an award takes its paper links in Nightly's election and twins it (check run,
    oxjob #2544): shells come only from pwf_shell_spellings, which drops spellings another source already carries."""
    shells = squash(code(cell('CREATE OR REPLACE TEMPORARY VIEW pwf_shells')))
    assert 'FROM pwf_shell_spellings' in shells and 'pwf_candidate' not in shells
    spellings = squash(code(cell('CREATE OR REPLACE TEMPORARY VIEW pwf_shell_spellings')))
    assert f"FROM {RAW} r WHERE r.provenance <> 'pubmed_work_funders'" in spellings
    assert "LEFT ANTI JOIN (SELECT DISTINCT funder_id, gkey FROM keys WHERE side = 'other') o ON o.funder_id = l.funder_id AND o.gkey = l.gkey" in spellings
    assert "LEFT ANTI JOIN (SELECT DISTINCT gkey FROM keys WHERE side = 'other' AND fam) f ON l.fam AND f.gkey = l.gkey" in spellings
    assert "LEFT ANTI JOIN (SELECT DISTINCT nkey FROM keys WHERE side = 'other' AND nkey IS NOT NULL) n ON n.nkey = l.nkey" in spellings
    assert 'PARTITION BY CASE WHEN fam THEN 0 ELSE funder_id END, gkey ORDER BY mentions DESC' in spellings
    assert 'PARTITION BY nkey ORDER BY mentions DESC' in spellings


def test_shell_keys_match_nightly():
    import sys
    sys.path.insert(0, str(ROOT / 'notebooks/awards/nightly_grants/lib'))
    import nih_fold
    from stable_award_ids import generic_sql
    spellings = squash(code(cell('CREATE OR REPLACE TEMPORARY VIEW pwf_shell_spellings')))
    assert squash(generic_sql('funder_award_id')) + ' gkey' in spellings
    families = re.findall(r'IN \((4320332161(?:, \d+)+)\)', spellings)
    assert len(families) == 2 and all(tuple(int(x) for x in f.split(', ')) == nih_fold.NIH_FAMILY for f in families)


def test_nih_key_is_nightlys_nih_parent_key():
    """The institute + serial is Nightly's sharp key for the NIH parent (AwardNormKey.sql, funder 4320332161): its paper-side
    regex, else its registry regex, on the same upper-cased, dash- and space-normalized text."""
    spellings = code(cell('CREATE OR REPLACE TEMPORARY VIEW pwf_shell_spellings'))
    norm_key = (ROOT / 'notebooks/awards/AwardNormKey.sql').read_text()
    deposited, registry = r"'([A-Z]{2}) ?-?(\\d{5,6})(?!\\d)'", r"'([A-Z]{2}\\d{6})'"
    assert f"WHEN funder_id = 4320332161 THEN (NULLIF(regexp_extract(_n, {registry}, 1), ''))" in norm_key
    assert f"WHEN funder_id = 4320332161 THEN (CASE WHEN NULLIF(regexp_extract(_n, {deposited}, 1), '') IS NOT NULL" in norm_key
    assert f"regexp_extract(n, {deposited}, 1) <> ''" in spellings and f"nullif(regexp_extract(n, {registry}, 1), '')" in spellings
    assert spellings.index(deposited) < spellings.index(registry) < spellings.index(r"'(?<![A-Z])([A-Z]{2}) ?-?(\\d{4})(?!\\d)'")
    assert "upper(trim(funder_award_id))" in spellings
    assert "' ?- ?', '-'), '[(.]', ' '), '  +', ' '), '(?<=[A-Z]{2})O(?=[0-9])', '0'), '(?<=[A-Z]{2}[ -]?[0-9]{1,3})[ -](?=[0-9])', '') n" in spellings

    def nkey(s):
        n = re.sub('  +', ' ', s.strip().upper())
        n = re.sub('  +', ' ', re.sub(r'[(.]', ' ', re.sub(r' ?- ?', '-', n)))
        n = re.sub(r'(?<=[A-Z]{2}[ -][0-9])[ -](?=[0-9])|(?<=[A-Z]{2}[ -][0-9]{2})[ -](?=[0-9])|(?<=[A-Z]{2}[ -][0-9]{3})[ -](?=[0-9])'
                   r'|(?<=[A-Z]{2}[0-9])[ -](?=[0-9])|(?<=[A-Z]{2}[0-9]{2})[ -](?=[0-9])|(?<=[A-Z]{2}[0-9]{3})[ -](?=[0-9])', '',
                   re.sub(r'(?<=[A-Z]{2})O(?=[0-9])', '0', n))   # Python lookbehind is fixed-width: the SQL's one pattern, spelled out
        m = re.search(r'([A-Z]{2}) ?-?(\d{5,6})(?!\d)', n)
        if m:
            return m.group(1) + m.group(2).rjust(6, '0')
        m = re.search(r'([A-Z]{2}\d{6})', n)
        if m:
            return m.group(1)
        m = re.search(r'(?<![A-Z])([A-Z]{2}) ?-?(\d{4})(?!\d)', n)   # 4-digit serial, after Nightly's two forms
        return m.group(1) + m.group(2).rjust(6, '0') if m else None
    cases = {'5p60am022125-08': 'AM022125', 'AM 22125.': 'AM022125', 'AM-22125': 'AM022125', 'CA 11051': 'CA011051',
             '5R01CA011051-14': 'CA011051', '5R01CA01105114': 'CA011051', 'R01 DK026190': 'DK026190', 'HL-06825': 'HL006825',
             '1-KO-4 HD-00634': 'HD000634', 'AM-19920-4': 'AM019920', 'NIAMDD AM 18370': 'AM018370', 'BNS 13464': 'NS013464',
             'L.L.GM-27256': 'GM027256', '5k04am00504-2': 'AM000504', 'am 26481-phy': 'AM026481', 'EY0597': 'EY000597',
             'ey00597': 'EY000597', 'GCRC RR 0065': 'RR000065', '5R01 AI 1450-08': 'AI001450', 'R01EY6645-02': 'EY006645',
             'BNS 1234': None, '2-T32-AMO7055': 'AM007055', '2G08-LM0-4419': 'LM004419', '1 RO1 AM - 22036': 'AM022036',
             'NICHD(10570)': 'HD010570', 'HL 305-72-01': 'HL030572', 'RR-00-287-12': 'RR000287', '5 ROI GM19.536-08': 'GM019536',
             'CA0-07175': 'CA007175', '5R01 AI 1450-08': 'AI001450', 'unknown': None, '12345': None}
    assert {s: nkey(s) for s in cases} == cases


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
