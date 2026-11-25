"""CreateFunderReportedLinks: the papers funders list for their own grants -> funder_reported_links, one table for every source.

The contract tests run anywhere. The behaviour tests run the whole notebook, cell by cell, on fixture rows; its writes need
Delta, so they run on Databricks Connect only: set DATABRICKS_CONFIG_PROFILE and LINKS_TEST_SCHEMA (a scratch schema such
as openalex_dev.<you>, where they create and drop frl_test_matches and frl_test_links). Elsewhere they skip.
"""
import csv
import json
import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SQL = (ROOT / 'notebooks/awards/CreateFunderReportedLinks.sql').read_text()
CELLS = SQL.split('-- COMMAND ----------')
PIN = 'max_by(_metadata.file_path, struct(_metadata.file_modification_time, _metadata.file_path))'
MATCHES, LINKS = 'openalex.awards.funder_reported_link_matches', 'openalex.awards.funder_reported_links'
REFMATCH = 'openalex.awards.funder_reported_refmatch_picks'
NEARTITLE = 'openalex.awards.funder_reported_neartitle_picks'   # MatchNearTitles.py's decisions (oxjob #2632)
OVERRIDES = 'openalex.awards.funder_reported_links_gate_overrides'   # a dated, per-source pass of shrink and growth
MERGED = 'openalex.works.merged_work_ids'   # a refmatch pick merged away moves to its survivor   # the reference matcher's decisions (MatchListedOutputs.py)
SOURCES = ('nwopen', 'fwf_open_api', 'anr_opendata', 'kaken', 'hakai_publications')
OBJECTS = {  # the object each mapping reads: a prefix's newest *.parquet, or one object
    'nwopen': 's3://openalex-ingest/awards/nwopen/*.parquet',
    'fwf_open_api': 's3://openalex-ingest/awards/fwf_open_api/*.parquet',
    'anr_opendata': 's3://openalex-ingest/awards/anr/anr_work_links.parquet',
    'kaken': 's3://openalex-ingest/awards/kaken_api/*.parquet',
    'hakai_publications': 's3://openalex-ingest/awards/hakai/hakai_outputs.parquet',
}
HAL, KAKEN_OLD = 'openalex.awards.hal_anr_work_links', 'openalex.awards.kaken_work_funders'
HAL_EU, HAL_LOCS = 's3://openalex-ingest/awards/hal_eu/*.parquet', 'openalex.works.location_work_ids'   # HAL EU links its text confirms (#2594)
HAL_SOURCES = ('cordis', 'erc')   # European project numbers a HAL deposit declares: grants under these provenances in raw
COLUMNS = ['award_id', 'funder_id', 'output_key', 'doi', 'pmid', 'work_id', 'title', 'year', 'volume', 'first_page', 'output_type',
           'surnames', 'object']


def code(text):
    # comment lines out, then trailing ' -- ...' comments (no SQL string here holds ' -- ')
    return '\n'.join(re.sub(r'\s+--\s.*$', '', l) for l in text.splitlines() if not l.strip().startswith('--'))


def cell(needle):
    found = [c for c in CELLS if needle in c]
    assert len(found) == 1, (needle, len(found))
    return found[0]


def squash(s):
    return ' '.join(s.split())


# ---------------------------------------------------------------- contract

def test_paths_shared_with_the_awards_job_match():
    with open(ROOT / 'notebooks/awards/sources.csv', newline='') as f:
        awards = {r['source']: r['s3_path'] for r in csv.DictReader(f)}
    assert set(SOURCES) - set(awards) == {'hakai_publications'}   # a source with grants is the provenance of its awards
    for s in ('nwopen', 'fwf_open_api'):   # one object for grants and outputs (ANR keeps its paper links apart)
        assert OBJECTS[s] == awards[s] + '*.parquet', s
    # kaken reads the API fetch (awards/kaken_api/); sources.csv says awards/kaken/ until the KAKEN grants feed's push moves it


def test_each_source_has_one_mapping_reading_its_path():
    union = squash(code(cell('CREATE OR REPLACE TEMP VIEW link_listed')))
    for s in SOURCES:
        mapping = squash(code(cell(f'CREATE OR REPLACE TEMP VIEW {s}_listed AS')))
        assert re.findall(r"read_files\('([^']+)'", mapping) == [OBJECTS[s]], s
        if OBJECTS[s].endswith('/*.parquet'):   # a prefix: its newest object, the Awards job's pin
            assert f'WHERE _metadata.file_path = (SELECT {PIN} FROM objects)' in mapping, s
        assert f"SELECT '{s}'" in union and f'FROM {s}_listed' in union, s
    assert code(SQL).count('read_files(') == len(SOURCES) + 1   # and HAL's verified European links, read once for cordis and erc


def test_reads_the_newest_object_like_the_awards_job():
    awards_job = (ROOT / 'notebooks/awards/CreateOrUpdateAwards.sql').read_text()
    for s in ('nwopen', 'fwf_open_api'):
        assert f"SELECT '{s}' AS source, {PIN} AS object\n FROM read_files('{OBJECTS[s]}'" in awards_job, s


def test_one_matching_path_with_no_per_source_code():
    shared = code(SQL.split('CREATE OR REPLACE TEMP VIEW link_listed', 1)[1].split('-- COMMAND ----------', 1)[1])   # after the union cell
    for s in SOURCES + HAL_SOURCES:
        assert s not in shared, s
    assert 'CASE source' not in code(SQL) and 'CASE l.source' not in code(SQL)


def test_tables_read_and_written():
    tables = set(re.findall(r'\bopenalex\.\w+\.\w+', code(SQL)))
    assert tables == {MATCHES, LINKS, REFMATCH, NEARTITLE, OVERRIDES, HAL, KAKEN_OLD, 'openalex.awards.openalex_awards_raw', 'openalex.works.openalex_works',
                      'openalex.works.location_work_match_keys', MERGED, HAL_LOCS}, tables
    assert code(SQL).count(f'FROM {HAL_LOCS}') == 1   # one view reads HAL; its two sources read the view
    assert HAL in cell('CREATE OR REPLACE TEMP VIEW anr_opendata_listed')
    assert KAKEN_OLD in cell('CREATE OR REPLACE TEMP VIEW kaken_listed')
    writes = re.findall(r'\b(CREATE TABLE IF NOT EXISTS|CREATE OR REPLACE TABLE|INSERT INTO|MERGE INTO|DELETE FROM|UPDATE)\s+(\w+\.\w+\.\w+)',
                        code(SQL))
    # the matches table holds what each source lists (read once), then those outputs with their candidates; one write to the links
    assert writes == [('CREATE TABLE IF NOT EXISTS', LINKS), ('CREATE TABLE IF NOT EXISTS', OVERRIDES), ('CREATE TABLE IF NOT EXISTS', REFMATCH), ('CREATE TABLE IF NOT EXISTS', NEARTITLE), ('CREATE OR REPLACE TABLE', MATCHES),
                      ('CREATE OR REPLACE TABLE', MATCHES), ('MERGE INTO', LINKS)], writes
    assert SQL.index(f'CREATE TABLE IF NOT EXISTS {LINKS}') < SQL.index('LINKS_REGEX_MODE')   # its readers always have their input
    assert SQL.index(f'CREATE TABLE IF NOT EXISTS {REFMATCH}') < SQL.index('LINKS_REGEX_MODE')
    assert code(SQL).count(REFMATCH) == 2   # created if missing, then read once (refmatch_picks); only the matcher writes it
    assert SQL.index(f'CREATE TABLE IF NOT EXISTS {NEARTITLE}') < SQL.index('LINKS_REGEX_MODE')
    assert code(SQL).count(NEARTITLE) == 2   # created if missing, then read once (neartitle_picks); only MatchNearTitles writes it
    # one read for every source's candidates, and KAKEN's 8 Jul links dropped on a paper with no DOI now
    assert code(cell('CREATE OR REPLACE TABLE ' + MATCHES + ' USING delta AS\nWITH')).count('openalex.works.openalex_works') == 1
    assert code(cell('CREATE OR REPLACE TEMP VIEW kaken_listed')).count('openalex.works.openalex_works') == 1
    assert code(SQL).count('openalex.works.openalex_works') == 2


def test_hal_sources_read_the_text_verified_links_once_and_only_grants_raw_holds():
    view = squash(code(cell('CREATE OR REPLACE TEMP VIEW hal_eu_listed AS')))
    assert re.findall(r"read_files\('([^']+)'", view) == [HAL_EU]
    assert f'WHERE _metadata.file_path = (SELECT {PIN} FROM objects)' in view   # the newest object, as every source
    assert f'FROM {HAL_LOCS}' in view and "native_id LIKE 'oai:HAL:%'" in view and 'work_id IS NOT NULL' in view
    assert "FROM openalex.awards.openalex_awards_raw WHERE provenance IN ('cordis', 'erc')" in view
    sources = squash(code(cell('CREATE OR REPLACE TEMP VIEW link_sources AS')))
    union = squash(code(cell('CREATE OR REPLACE TEMP VIEW link_listed')))
    for s in HAL_SOURCES:
        assert f"('{s}', 'work', 'none')" in sources, s   # the deposit names its work; its text names the grant
        mapping = squash(code(cell(f'CREATE OR REPLACE TEMP VIEW {s}_listed AS')))
        assert f"FROM hal_eu_listed WHERE source = '{s}'" in mapping and 'surnames, object FROM' in mapping, s
        assert f"SELECT '{s}', * FROM {s}_listed" in union, s
        # refmatch (on for every source) takes any listed row with a title and an output type, ungraded: HAL rows carry the
        # deposit's work and neither, so a later edit cannot send them to the reference matcher by accident
        assert 'CAST(NULL AS STRING) AS title' in mapping and 'CAST(NULL AS STRING) AS output_type' in mapping, s
    assert 'hal_tei' not in SQL   # HAL's declared projects alone are not links (66/100 graded right, 25 Nov)


def test_gates_are_frozen_before_the_one_merge_and_the_run_fails_after_it():
    order = ['LINKS_REGEX_MODE', f'CREATE OR REPLACE TABLE {MATCHES}', 'SET VAR link_results', 'SELECT inline(session.link_results)',
             f'MERGE INTO {LINKS}', 'raise_error(']
    assert [SQL.index(x) for x in order] == sorted(SQL.index(x) for x in order)
    gates = squash(code(cell('SET VAR link_results')))
    for rule in ("WHEN listed = 0 OR award_ids_in_raw < 0.99 * award_ids THEN 'kept: coverage'",
                 "WHEN links < 0.95 * links_now THEN 'kept: shrink'",
                 "WHEN links_now > 0 AND links > 1.25 * links_now THEN 'kept: growth'"):
        assert rule in gates, rule
    merge = squash(code(cell(f'MERGE INTO {LINKS}')))
    assert "WHEN NOT MATCHED BY SOURCE AND NOT exists(session.link_results, r -> r.source = t.source AND r.state <> 'written') THEN DELETE" in merge


def test_an_override_row_passes_shrink_and_growth_for_one_source_on_one_day():
    gates = squash(code(cell('SET VAR link_results')))
    assert gates.index('WHEN overridden THEN') > gates.index("THEN 'kept: coverage'")   # coverage is never overridden
    assert gates.index('WHEN overridden THEN') < gates.index("THEN 'kept: shrink'") < gates.index("THEN 'kept: growth'")
    assert ('SELECT DISTINCT source FROM ' + OVERRIDES + ' WHERE run_date = current_date()') in gates   # that day only


def test_refmatch_is_a_last_tier_that_needs_the_title_guard():
    sources = re.findall(r"\('(\w+)', '([a-z,]+)', '(\w+)'\)", cell('CREATE OR REPLACE TEMP VIEW link_sources'))
    tiers = {s: t.split(',') for s, t, _ in sources}
    assert not any('refmatch' in t for t in tiers.values())   # one setting, not a per-source list: on for every source,
    step2 = squash(code(cell('CREATE OR REPLACE TABLE ' + MATCHES + ' USING delta AS\nWITH')))
    # after every identifier tier; only a source's neartitle (oxjob #2632) comes after it
    assert ("concat(filter(split(s.tiers, ','), t -> t <> 'neartitle'), array('refmatch'), "
            "filter(split(s.tiers, ','), t -> t = 'neartitle')) AS tiers") in step2
    assert ('refmatch_picks AS ( SELECT p.source, p.listed_key, p.guard_key, coalesce(m.winner_work_id, p.work_id) AS work_id, p.score '
            'FROM (SELECT * FROM ' + REFMATCH + ' WHERE linked QUALIFY row_number() OVER (PARTITION BY source, listed_key ORDER BY '
            'decided_at DESC) = 1) p LEFT JOIN (SELECT loser_work_id, max(winner_work_id) AS winner_work_id FROM ' + MERGED
            + ' GROUP BY 1) m ON m.loser_work_id = p.work_id )') in step2
    # a pick is keyed by the output as listed (title and surnames; FWF renumbers its output ids), and accepted only when the
    # work's title is the listed one (the guard)
    assert 'JOIN refmatch_picks p ON p.source = l.source AND p.listed_key = listed_key(l.title, l.surnames)' in step2
    key = squash(code(cell('CREATE OR REPLACE TEMPORARY FUNCTION listed_key')))
    assert key == ("CREATE OR REPLACE TEMPORARY FUNCTION listed_key(title STRING, surnames ARRAY<STRING>) RETURNS STRING RETURN "
                   "concat_ws('|', match_text(title), match_text(array_join(array_sort(array_distinct(transform(coalesce(surnames, array()), "
                   "x -> lower(x)))), ' ')));")
    assert 'coalesce(cjk_key(d.title_key) = cjk_key(p.guard_key), false) AS refmatch_title_equal' in step2   # either width, both sides
    assert ('size(coalesce(l.surnames, array())) > 0 OR l.year IS NULL OR d.year IS NULL OR abs(d.year - l.year) <= 1 '
            'AS refmatch_year_ok') in step2   # a title-only pick (no surnames) also needs the year
    assert "THEN x.refmatch_title_equal AND x.refmatch_year_ok" in step2
    assert ("x.work_id IS NOT NULL AND x.funder_id IS NOT NULL AND CASE WHEN x.tier = 'refmatch' THEN x.refmatch_title_equal AND x.refmatch_year_ok "
            "AND NOT coalesce(array_contains(r.held_work_types, x.work_type) AND x.output_type IN ('article', 'book', 'thesis'), false) "
            "WHEN x.tier = 'neartitle' THEN NOT coalesce(array_contains(r.held_work_types, x.work_type) AND x.output_type IN "
            "('article', 'book', 'thesis'), false) "
            "ELSE x.check = 'none' OR coalesce(CASE x.tier") in step2   # the title tier's held records; never NULL
    assert ("AND NOT (array_contains(r.held_work_types, x.work_type) AND coalesce(x.output_type IN ('article', 'book', 'thesis'), "
            "false))") in step2   # the title tier's own rule, unchanged
    # FWF marks its publications, the only outputs the matcher decides
    fwf = squash(code(cell('CREATE OR REPLACE TEMP VIEW fwf_open_api_listed AS')))
    assert "CASE WHEN o.category = 'publications' THEN CASE o.type WHEN 'Journal Article' THEN 'article' WHEN 'Preprint' THEN 'article'" in fwf


def test_neartitle_is_after_refmatch_for_a_source_that_sets_it_and_reads_one_pick_per_title_and_year():
    sources = re.findall(r"\('(\w+)', '([a-z,]+)', '(\w+)'\)", cell('CREATE OR REPLACE TEMP VIEW link_sources'))
    assert [s for s, t, _ in sources if 'neartitle' in t.split(',')] == ['kaken']
    assert all(t.split(',')[-1] == 'neartitle' for s, t, _ in sources if 'neartitle' in t)   # a source's last listed tier
    step2 = squash(code(cell('CREATE OR REPLACE TABLE ' + MATCHES + ' USING delta AS\nWITH')))
    assert ('neartitle_picks AS ( SELECT p.source, p.title_key, p.year, coalesce(m.winner_work_id, p.work_id) AS work_id '
            "FROM (SELECT * FROM " + NEARTITLE + " WHERE status = 'linked' QUALIFY row_number() OVER (PARTITION BY source, "
            'title_key, year ORDER BY decided_at DESC) = 1) p LEFT JOIN (SELECT loser_work_id, max(winner_work_id) AS '
            'winner_work_id FROM ' + MERGED + ' GROUP BY 1) m ON m.loser_work_id = p.work_id )') in step2
    assert ("JOIN neartitle_picks p ON p.source = l.source AND p.title_key = l.title_key AND p.year <=> l.year "
            "WHERE array_contains(l.tiers, 'neartitle')") in step2
    # the notebook that writes the picks reads its sources and cjk_key from here (one place each)
    nt = (ROOT / 'notebooks/awards/MatchNearTitles.py').read_text()
    assert "open(os.path.join(os.getcwd(), 'CreateFunderReportedLinks.sql')" in nt and "if 'neartitle' in t.split(',')" in nt
    assert 'CREATE OR REPLACE TEMPORARY FUNCTION cjk_key' in nt and 'def cjk_key' not in nt
    job = (ROOT / 'jobs/funder_reported_work_funders.yaml').read_text()
    task = job.split('- task_key: match_near_titles', 1)[1].split('\n        - task_key:', 1)[0]   # to the next task
    assert 'notebook_path: notebooks/awards/MatchNearTitles' in task and '- task_key: create_funder_reported_links' in task
    assert 'run_if: ALL_DONE' in task   # a failure here never stops the links task


def test_match_text_is_location_work_match_keys_nt():
    keys = squash((ROOT / 'notebooks/location_matching/sql/keys.sql').read_text())
    nt = re.search(r"trim\(regexp_replace\(lower\(translate\(regexp_replace\(coalesce\(title, ''\).*?' '\)\) AS nt", keys).group(0)[:-len(' AS nt')]
    fn = squash(code(cell('CREATE OR REPLACE TEMPORARY FUNCTION match_text')))
    assert fn == f"CREATE OR REPLACE TEMPORARY FUNCTION match_text(s STRING) RETURNS STRING RETURN nullif({nt.replace('coalesce(title', 'coalesce(s')}, '');"
    cjk = re.search(r'CREATE OR REPLACE TEMPORARY FUNCTION cjk_key\(.*?;', SQL, re.S).group(0)
    assert code(SQL.replace(cjk, '')).count('translate(') == 1   # titles and names are folded only there (cjk_key: widths)


def test_one_statement_per_cell_double_escaped_regex_and_no_semicolon_in_strings():
    body = code(SQL)
    assert not re.search(r'(?<!\\)\\(?!\\)', body.replace('\\\\', '')), 'a single backslash in SQL'
    for literal in re.findall(r"'((?:[^']|'')*)'", body):
        assert ';' not in literal, literal   # statement splitters cut on ';'
    for c in CELLS:
        assert code(c).strip().rstrip(';').count(';') == 0, c[:80]


def test_the_daily_job_runs_it_next_to_the_union():
    job = (ROOT / 'jobs/funder_reported_work_funders.yaml').read_text()
    task = job.split('- task_key: create_funder_reported_links', 1)[1].split('git_source:', 1)[0]
    assert 'notebook_path: notebooks/awards/CreateFunderReportedLinks' in task
    assert 'source: GIT' in task and 'warehouse_id: 3996dc0a9b183ce3' in task
    assert 'timeout_seconds: 3600' in task and 'on_failure:' in task and 'rohan@openalex.org' in task
    assert 'quartz_cron_expression: 0 30 23 * * ?' in job   # 23:30 UTC, before Nightly Grants (00:00)


# ---------------------------------------------------------------- behaviour: the whole notebook on fixture rows

@pytest.fixture(scope='module')
def spark():
    if not (os.environ.get('DATABRICKS_CONFIG_PROFILE') and os.environ.get('LINKS_TEST_SCHEMA')):
        pytest.skip('needs Databricks Connect and a scratch schema (DATABRICKS_CONFIG_PROFILE, LINKS_TEST_SCHEMA)')
    dc = pytest.importorskip('databricks.connect')
    s = dc.DatabricksSession.builder.profile(os.environ['DATABRICKS_CONFIG_PROFILE']).serverless(True).getOrCreate()
    yield s
    for t in (T_MATCHES, T_LINKS, T_REFMATCH, T_OVERRIDES):
        s.sql(f'DROP TABLE IF EXISTS {t}')


SCHEMA = os.environ.get('LINKS_TEST_SCHEMA', 'scratch')
T_MATCHES, T_LINKS, T_REFMATCH = f'{SCHEMA}.frl_test_matches', f'{SCHEMA}.frl_test_links', f'{SCHEMA}.frl_test_refmatch'
T_OVERRIDES = f'{SCHEMA}.frl_test_overrides'   # never the production table
NWO_F, FWF_F, ANR_F, KAKEN_F, HAKAI_F = 4320321800, 4320321181, 4320320883, 4320334764, 4320334031
T = lambda k: f'{k} of the colloidal glass transition in long range systems'
ART, BOOK, PATENT, OTHER = 'Wetenschappelijk artikel', 'Boek', 'Octrooi', 'Overige Output'


def nwo(ptype, title, year, authors, url=None, volume=None, start=None):
    p = {'type': ptype, 'title': title, 'year': year, 'authors': [{'last_name': a, 'initials': 'X'} for a in authors]}
    p.update({k: v for k, v in (('url_open_access', url), ('edition', volume), ('start', start)) if v})
    return p


def fwf(oid, title, doi=None, pmid=None, surnames=('Heid',), category='publications', type='Journal Article'):
    return dict(output_id=oid, category=category, type=type, doi=doi, pmid=pmid, title=title, surnames=list(surnames))


NWO = {'P1': [nwo(ART, T('A study'), 2020, ['Linden'], url='https://doi.org/10.1000/ ABC.1'),                    # 0 DOI, salvaged
              nwo(ART, T('Unrelated'), 2020, ['Linden'], url='https://doi.org/10.1000/xyz.2'),                   # 1 DOI, another item
              nwo(ART, T('Crystal growth'), 2019, ['van der Linden']),                                           # 2 title, same year
              nwo(ART, T('Shear melting'), 2019, ['Linden']),                                                    # 3 title, year apart
              nwo(ART, T('Hopping'), 2019, ['Linden'], volume='12', start='345'),                                # 4 year apart, page
              nwo(ART, T('Light scattering data'), 2018, ['Linden']),                                            # 5 a dataset record
              nwo(ART, T('Rheology'), 2017, ['Linden']),                                                         # 6 two same-title works
              nwo(PATENT, T('Crystal growth'), 2019, ['Linden']),                                                # 7 patents: no row
              nwo(ART, 'Short note', 2019, ['Linden']),                                                          # 8 too short
              nwo(ART, T('Sedimentation'), 2020, ['Linden'], url='https://dx.doi.org/10.1000/dup%281%29/full?via=x'),  # 9 shared DOI
              nwo(BOOK, T('Unrelated'), 2020, ['Linden'], url='https://doi.org/10.1000/xyz.2'),                  # 10 DOI decides
              nwo(OTHER, T('Survey data'), 2018, ['Linden']),                                                    # 11 untyped output
              nwo(ART, T('Inverted names'), 2016, ['Linden'])]}                                                  # 12 'Linden, M.'
NWO_OLD = {'P1': [nwo(ART, 'An old listing from last week of the glass transition', 2010, ['Linden'])]}
FWF = {'F1': [fwf('o-1', T('Charge transfer'), doi='10.1000/f.1'),                       # DOI, same title
              fwf('o-2', 'Ein deutscher Titel', pmid='333'),                             # PMID, other title, author on it
              fwf('o-3', 'Ein Konsortium', doi='10.1000/big.7'),                         # 101 authors: the title decides
              fwf('o-4', 'Anderer Titel', doi='10.1000/tm.8', surnames=('Thomas',)),     # a given name on the work, not a last name
              fwf('o-5', T('Diffusion'), doi='10.1000/b.5', pmid='555'),                 # the DOI's work is another item: PMID not tried
              fwf('o-6', T('Glass relaxation')),                                         # refmatch: the latest pick, the work's title is it
              fwf('o-7', T('Aging dynamics')),                                           # refmatch: the pick's title is another one
              fwf('o-8', T('Yielding')),                                                 # decided under an older title: not used
              fwf('o-9', T('Charge transfer'), doi='10.1000/f.1'),                       # a DOI and a pick: the DOI decides
              fwf('o-10', T('Nucleation') + '; In: A handbook of soft matter', type='Book Chapter'),   # a chapter: "; In:" cut
              fwf('o-11', T('Creep')),                                                   # in the reader's band, not linked: not used
              fwf('o-12', T('Glass relaxation'), category='science communication', type='A talk or presentation'),  # not a publication
              fwf('o-13', T('Glass relaxation')),                                        # o-6 renumbered: the same pick
              fwf('o-14', T('Gelation'), surnames=('Other',)),                          # a pick for another author list: not used
              fwf('o-15', T('Femtoscopy'), type='Preprint'),                             # a paper listed, the pick is a dataset: held
              fwf('o-16', T('Shear banding'), type='Conference Proceeding Abstract'),   # an abstract listed, an abstract record: kept
              fwf('o-17', T('Sol-gel transition'), surnames=('HEID',))]}                # the pick merged away: its survivor; case-blind key
ANR = ([('10.2000/a.1', ['ANR-2', 'ANR-1']),                # two grants: award_ids sorted
        ('10.2000/shared', ['ANR-1']),                      # a DOI two works hold: both linked
        ('10.2000/c.3', ['ANR-X']),                         # not an award in raw: not linked
        (' 10.2000/D.4 ', ['ANR-1']),                       # spaces and capitals around the DOI
        ('https://doi.org/10.2000/e.5', ['ANR-2'])]         # the DOI as a resolver URL
       + [(f'10.2000/none.{k}', [f'ANR-K{k}']) for k in range(120)])   # grants with unindexed papers: 122 of 123 grants are awards
JA_FULL, JA_HALF = '小学校社会科指導法に関する遠隔学修教材の作成（２）', '小学校社会科指導法に関する遠隔学修教材の作成 (2)'
KAKEN = {'K1': [{'type': 'journal_article', 'doi': '10.3000/k.1'}, {'type': 'presentation', 'title': 'A talk'},
                {'type': 'journal_article', 'title': JA_FULL, 'year': '2023'},    # its pick's work keys the title half-width
                {'type': 'journal_article', 'title': JA_FULL, 'year': '2015'}]}   # the same title, another year: not that paper
KAKEN_LEGACY = [(301, KAKEN_F, ['K1']),          # also in the API's list: one link
                (302, KAKEN_F, ['K1']),          # the paper still has a DOI: kept
                (303, KAKEN_F, ['K1']),          # the paper is in OpenAlex with no DOI now: dropped
                (304, KAKEN_F, ['K1'])]          # merged away or gone: kept as named (Nightly moves it to the survivor)
HAKAI = ['https://doi.org/10.4000/h.1', 'https://doi.org/10.4000/unindexed']
EC_F, ERC_F = 4320320300, 4320338335


HAL_EU_ROWS = [  # hal_id, grant_number, object (file name), as hal_eu_links_to_s3.py writes them
    ('hal-e1', '231940', 'hal_eu_links_2026-11-25.parquet'),     # FP7: cordis
    ('hal-e2', '340770', 'hal_eu_links_2026-11-25.parquet'),     # an ERC grant
    ('hal-e4', '999999', 'hal_eu_links_2026-11-25.parquet'),     # not an award in raw: not listed, so no coverage loss
    ('hal-e5', '340770', 'hal_eu_links_2026-11-25.parquet'),     # no OpenAlex work holds this deposit: not linked
    ('hal-e8', '231940', 'hal_eu_links_2026-11-25.parquet'),     # its only pin has no work yet: not linked
    ('hal-e2', 'ERC-340770', 'hal_eu_links_2026-11-25.parquet'),  # not a grant agreement number: not read
    ('hal-e9', '231940', 'hal_eu_links_2026-11-18.parquet')]     # an older object only: not read
HAL_LOC_ROWS = [('oai:HAL:hal-e1v2', 'repo', 501), ('oai:HAL:hal-e1v1', 'repo_backfill', 501),   # two versions, one work: one link
                ('oai:HAL:hal-e2v1', 'repo', 502), ('oai:HAL:hal-e4v1', 'repo', 504), ('oai:HAL:hal-e9v1', 'repo', 509),
                ('oai:HAL:hal-e8v1', 'repo', None),               # a pin with no work yet: never read
                ('oai:arXiv.org:hal-e5v1', 'repo', 505)]          # another repository's id: not HAL's
HAL_ROWS = [(205, ANR_F, 'ANR-1', 'hal-01'),       # a work with no DOI, named by HAL
            (99999, ANR_F, 'ANR-2', 'hal-02'),     # a work OpenAlex has merged away: passed through as named
            (201, ANR_F, 'ANR-1', 'hal-03')]       # the link the DOI also gives: one link
WORKS = [  # id, doi, pmid, type, title, year, volume, first page, author names
    (1, '10.1000/abc.1', None, 'article', T('A study'), 2020, None, None, ['M. Linden']),
    (2, '10.1000/xyz.2', None, 'article', 'Something else entirely', 2020, None, None, ['Q. Other']),
    (3, None, None, 'article', T('Crystal growth'), 2019, None, None, ['Marjolein van der Linden']),
    (4, None, None, 'preprint', T('Shear melting'), 2018, None, None, ['M. Linden']),
    (5, '10.1000/p.5', None, 'article', T('Hopping'), 2020, '12', '345-350', ['M. Linden']),
    (6, '10.1000/d.6', None, 'dataset', T('Light scattering data'), 2018, None, None, ['M. Linden']),
    (66, None, None, 'dataset', T('Survey data'), 2018, None, None, ['M. Linden']),
    (12, None, None, 'article', T('Inverted names'), 2016, None, None, ['Linden, M.']),
    (7, None, None, 'article', T('Rheology'), 2017, None, None, ['M. Linden']),
    (77, None, None, 'article', T('Rheology'), 2017, None, None, ['Z. Someone']),
    (8, None, None, 'article', T('Unrelated'), 2020, None, None, ['M. Linden']),            # outputs 1 and 10's title, never tried
    (9, None, None, 'article', 'An old listing from last week of the glass transition', 2010, None, None, ['M. Linden']),
    (21, '10.1000/dup(1)', None, 'article', T('Sedimentation'), 2020, None, None, ['M. Linden']),
    (22, '10.1000/dup(1)', None, 'article', 'A different record on that DOI', 2020, None, None, ['Q. Other']),
    (101, '10.1000/f.1', None, 'article', T('Charge transfer'), 2021, None, None, ['Elisa Heid']),
    (104, None, '333', 'article', 'Polarizable MD of ionic liquids', 2021, None, None, ['Elisa Heid', 'Christian Schroeder']),
    (106, '10.1000/big.7', None, 'article', 'A consortium paper', 2021, None, None, ['Elisa Heid'] + [f'Author {i}' for i in range(100)]),
    (107, '10.1000/tm.8', None, 'article', 'Something else', 2021, None, None, ['Thomas Schmidt']),
    (105, '10.1000/b.5', None, 'article', 'A different paper on that DOI', 2021, None, None, ['Q. Other']),
    (155, None, '555', 'article', T('Diffusion'), 2021, None, None, ['Elisa Heid']),
    (160, None, None, 'article', T('Glass relaxation'), 2021, None, None, ['Elisa Heid']),
    (165, None, None, 'preprint', T('Glass relaxation'), 2020, None, None, ['Elisa Heid']),
    (161, None, None, 'article', T('Aging dynamics in gels'), 2021, None, None, ['Elisa Heid']),
    (162, None, None, 'article', T('Yielding old'), 2021, None, None, ['Elisa Heid']),
    (163, None, None, 'book-chapter', T('Nucleation'), 2021, None, None, ['Elisa Heid']),
    (164, None, None, 'article', T('Creep'), 2021, None, None, ['Elisa Heid']),
    (166, None, None, 'dataset', T('Femtoscopy'), 2021, None, None, ['Elisa Heid']),
    (167, None, None, 'conference-abstract', T('Shear banding'), 2021, None, None, ['Elisa Heid']),
    (310, None, None, 'article', JA_HALF, 2023, None, None, ['吉永 潤']),   # KAKEN's paper, keyed half-width with a space
    (169, None, None, 'article', T('Sol-gel transition'), 2021, None, None, ['Elisa Heid']),   # 199 was merged into it
    (201, '10.2000/a.1', None, 'article', 'ANR paper', 2015, None, None, ['A. B']),
    (202, '10.2000/shared', None, 'article', 'Shared one', 2015, None, None, ['A. B']),
    (203, '10.2000/shared', None, 'article', 'Shared two', 2015, None, None, ['A. B']),
    (204, '10.2000/c.3', None, 'article', 'Unknown grant', 2015, None, None, ['A. B']),
    (205, None, None, 'article', 'A HAL deposit', 2015, None, None, ['A. B']),
    (206, '10.2000/d.4', None, 'article', 'ANR paper four', 2015, None, None, ['A. B']),
    (207, '10.2000/e.5', None, 'article', 'ANR paper five', 2015, None, None, ['A. B']),
    (301, '10.3000/k.1', None, 'article', 'KAKEN one', 2015, None, None, ['A. B']),
    (302, '10.3000/k.2', None, 'article', 'KAKEN two', 2015, None, None, ['A. B']),
    (303, None, None, 'article', 'A record that lost its DOI', 2015, None, None, ['A. B']),
    (401, '10.4000/h.1', None, 'article', 'Hakai paper', 2015, None, None, ['A. B']),
    (501, None, None, 'conference-paper', 'A HAL talk on robots', 2012, None, None, ['A. B']),
    (502, None, None, 'article', 'A HAL paper on proteins', 2016, None, None, ['A. B']),
    (504, None, None, 'article', 'Unknown project', 2016, None, None, ['A. B']),
    (509, None, None, 'article', 'Listed last week only', 2016, None, None, ['A. B']),
]
RAW = ([('P1', 'nwopen', NWO_F), ('F1', 'fwf_open_api', FWF_F), ('ANR-1', 'anr_opendata', ANR_F), ('ANR-2', 'anr_opendata', ANR_F),
        ('ANR-X', 'crossref_work', ANR_F), ('K1', 'kaken', KAKEN_F),   # ANR-X: another source's award
        ('231940', 'cordis', EC_F), ('340770', 'erc', ERC_F)]
       + [(f'ANR-K{k}', 'anr_opendata', ANR_F) for k in range(120)])
LINKED = {  # source -> {(work_id, funder_id): award_ids}
    'nwopen': {(1, NWO_F): ['P1'], (3, NWO_F): ['P1'], (5, NWO_F): ['P1'], (7, NWO_F): ['P1'], (21, NWO_F): ['P1'], (66, NWO_F): ['P1'],
               (12, NWO_F): ['P1']},
    'fwf_open_api': {(101, FWF_F): ['F1'], (104, FWF_F): ['F1'], (160, FWF_F): ['F1'], (163, FWF_F): ['F1'], (167, FWF_F): ['F1'],
                     (169, FWF_F): ['F1']},
    'anr_opendata': {(201, ANR_F): ['ANR-1', 'ANR-2'], (202, ANR_F): ['ANR-1'], (203, ANR_F): ['ANR-1'], (205, ANR_F): ['ANR-1'],
                     (99999, ANR_F): ['ANR-2'], (206, ANR_F): ['ANR-1'], (207, ANR_F): ['ANR-2']},
    'kaken': {(301, KAKEN_F): ['K1'], (302, KAKEN_F): ['K1'], (304, KAKEN_F): ['K1'], (310, KAKEN_F): ['K1']},
    'hakai_publications': {(401, HAKAI_F): []},
    'cordis': {(501, EC_F): ['231940']},
    'erc': {(502, ERC_F): ['340770']},
}


def nt(title):   # match_text: letters and digits of any script (\\p{L}\\p{N})
    return ' '.join(re.sub(r'[\W_]+', ' ', title.lower()).split())


def pick(oid, title, work, linked=True, guard=None, at='2026-12-01 03:00:00', surnames=('Heid',), source='fwf_open_api', guard_key=None):
    # one decision of MatchListedOutputs: (source, listed_key, output_key, title_key, guard_key, ..., work_id, ..., linked, ...)
    import datetime
    names = nt(' '.join(sorted({x.lower() for x in surnames})))
    listed = nt(title) + ('|' + names if names else '')   # listed_key: concat_ws skips the NULL of no surnames
    return (source, listed, oid, nt(title), guard_key or nt(guard or title), 'a text', 1, work, 0.99, 0.99, not linked, linked, 2,
            'linked' if linked else 'band', datetime.datetime.fromisoformat(at), 'r1')


REFMATCH_ROWS = [pick('o-6', T('Glass relaxation'), 165, at='2026-11-30 03:00:00'),   # an older pick of the same title
                 pick('o-6', T('Glass relaxation'), 160),
                 pick('o-7', T('Aging dynamics'), 161),
                 pick('o-8', T('Yielding old'), 162),
                 pick('o-9', T('Charge transfer'), 160),
                 pick('o-10', T('Nucleation') + '; In: A handbook of soft matter', 163, guard=T('Nucleation')),
                 pick('o-11', T('Creep'), 164, linked=False),
                 pick('o-14', T('Gelation'), 160),   # decided for the surnames ['Heid']; o-14 lists ['Other']
                 pick('o-15', T('Femtoscopy'), 166),
                 pick('o-16', T('Shear banding'), 167),
                 pick('o-17', T('Sol-gel transition'), 199),
                 # KAKEN: no surnames, so the title alone; stored the way MatchListedOutputs stores it, cjk_key'd ('作成2')
                 pick('2', JA_FULL, 310, surnames=(), source='kaken', guard_key='小学校社会科指導法に関する遠隔学修教材の作成2')]   # decided as 'Heid' on work 199, since merged into 169


def views(spark, raw=RAW):
    meta = 'struct<file_path:string,file_modification_time:timestamp>'
    import datetime
    old, new = datetime.datetime(2026, 11, 2), datetime.datetime(2026, 11, 9)
    meta_of = lambda s, name, t=new: (OBJECTS[s].replace('*.parquet', name), t)
    spark.createDataFrame([(p, json.dumps(v), meta_of('nwopen', '2026-11-09.parquet')) for p, v in NWO.items()]
                          + [(p, json.dumps(v), meta_of('nwopen', '2026-11-02.parquet', old)) for p, v in NWO_OLD.items()],
                          f'project_id string, products_json string, _metadata {meta}').createOrReplaceTempView('t_nwopen')
    spark.createDataFrame([(c, v, meta_of('fwf_open_api', '2026-11-13.parquet')) for c, v in FWF.items()],
                          'code string, outputs array<struct<output_id:string,category:string,type:string,doi:string,pmid:string,'
                          f'title:string,surnames:array<string>>>, _metadata {meta}').createOrReplaceTempView('t_fwf')
    spark.createDataFrame([(d, g, meta_of('anr_opendata', '')) for d, g in ANR],
                          f'doi string, anr_grants array<string>, _metadata {meta}').createOrReplaceTempView('t_anr')
    spark.createDataFrame([(p, json.dumps(v), meta_of('kaken', '2026-11-17.parquet')) for p, v in KAKEN.items()],
                          f'project_id string, products_json string, _metadata {meta}').createOrReplaceTempView('t_kaken')
    spark.createDataFrame(KAKEN_LEGACY, 'work_id bigint, funder_id bigint, award_ids array<string>').createOrReplaceTempView('t_kaken_old')
    spark.createDataFrame([(d, str(HAKAI_F), 'hakai_publications', meta_of('hakai_publications', '')) for d in HAKAI],
                          f'doi string, funder_id string, provenance string, _metadata {meta}').createOrReplaceTempView('t_hakai')
    spark.createDataFrame([r + ('hal_tei_text_verified', old) for r in HAL_ROWS], 'work_id bigint, funder_id bigint, funder_award_id string, '
                          'hal_id string, provenance string, created_at timestamp').createOrReplaceTempView('t_hal')
    spark.createDataFrame(raw, 'funder_award_id string, provenance string, funder_id bigint').createOrReplaceTempView('t_raw')
    hal_meta = lambda name: (HAL_EU.replace('*.parquet', name), new if name.endswith('25.parquet') else old)
    spark.createDataFrame([(h, n, hal_meta(f)) for h, n, f in HAL_EU_ROWS],
                          f'hal_id string, grant_number string, _metadata {meta}').createOrReplaceTempView('t_hal_eu')
    spark.createDataFrame(HAL_LOC_ROWS, 'native_id string, provenance string, work_id bigint').createOrReplaceTempView('t_hal_locs')
    spark.createDataFrame([(w[0], w[5], nt(w[4])) for w in WORKS], 'id bigint, yr int, nt string').createOrReplaceTempView('t_keys')
    spark.createDataFrame([(199, 169)], 'loser_work_id bigint, winner_work_id bigint').createOrReplaceTempView('t_merged')
    spark.createDataFrame(
        [(i, d and f'https://doi.org/{d}', {'pmid': f'https://pubmed.ncbi.nlm.nih.gov/{p}'} if p else {}, ty, t, y, (v, fp),
          [((a,),) for a in names]) for i, d, p, ty, t, y, v, fp, names in WORKS],
        'id bigint, doi string, ids map<string,string>, type string, title string, publication_year int, '
        'biblio struct<volume:string,first_page:string>, authorships array<struct<author:struct<display_name:string>>>'
    ).createOrReplaceTempView('t_works')


def sub(sql, drop_source=None):
    names = {OBJECTS[s]: v for s, v in zip(SOURCES, ('t_nwopen', 't_fwf', 't_anr', 't_kaken', 't_hakai'))} | {HAL_EU: 't_hal_eu'}
    sql = re.sub(r"read_files\('([^']+)'[^)]*\)", lambda m: names[m.group(1)], sql)
    for table, name in ((HAL, 't_hal'), (HAL_LOCS, 't_hal_locs'), (KAKEN_OLD, 't_kaken_old'), ('openalex.awards.openalex_awards_raw', 't_raw'), ('openalex.works.openalex_works', 't_works'),
                        ('openalex.works.location_work_match_keys', 't_keys'), (MATCHES, T_MATCHES), (LINKS, T_LINKS),
                        (REFMATCH, T_REFMATCH), (OVERRIDES, T_OVERRIDES), (MERGED, 't_merged')):
        sql = re.sub(re.escape(table) + r'(?!\w)', name, sql)   # whole names only: LINKS is a prefix of OVERRIDES
    if drop_source:
        sql = re.sub(r",\n  \('%s',[^\n]*\)" % drop_source, '', sql)
    return sql


def run_notebook(spark, drop_source=None):
    for c in CELLS:
        statement = sub(code(c), drop_source).strip().rstrip(';')
        if statement:
            spark.sql(statement).collect()


def fresh(spark, links=(), refmatch=REFMATCH_ROWS):
    for t in (T_MATCHES, T_LINKS, T_REFMATCH, T_OVERRIDES):
        spark.sql(f'DROP TABLE IF EXISTS {t}')
    spark.sql(f'CREATE TABLE {T_LINKS} (source STRING, work_id BIGINT, funder_id BIGINT, award_ids ARRAY<STRING>) USING delta')
    spark.createDataFrame(list(refmatch), 'source string, listed_key string, output_key string, title_key string, guard_key string, text string, '
                          'text_hash bigint, work_id bigint, score double, checklist_p double, in_band boolean, linked boolean, '
                          'n_candidates int, status string, decided_at timestamp, run_id string').write.saveAsTable(T_REFMATCH)
    if links:
        spark.createDataFrame(links, 'source string, work_id bigint, funder_id bigint, award_ids array<string>').write.mode('append').saveAsTable(T_LINKS)


def links(spark):
    out = {}
    for r in spark.sql(f'SELECT * FROM {T_LINKS}').collect():
        out.setdefault(r.source, {})[(r.work_id, r.funder_id)] = list(r.award_ids)
    return out


def matches(spark, source):
    return {(r.award_id, r.output_key, r.work_id): (r.tier, r.matched_by and list(r.matched_by), r.accepted)
            for r in spark.sql(f"SELECT * FROM {T_MATCHES} WHERE source = '{source}'").collect()}


def last_merge(spark):
    m = spark.sql(f"SELECT operationMetrics FROM (DESCRIBE HISTORY {T_LINKS}) WHERE operation = 'MERGE' ORDER BY version DESC LIMIT 1").first()[0]
    return {k: int(v) for k, v in m.items() if k in ('numTargetRowsInserted', 'numTargetRowsUpdated', 'numTargetRowsDeleted')}


def test_each_tier_and_check_on_fixture_rows_and_a_rerun_writes_nothing(spark):
    views(spark)
    fresh(spark)
    run_notebook(spark)
    for s in SOURCES + HAL_SOURCES:
        assert spark.table(f'{s}_listed').columns == COLUMNS, s
    for s in HAL_SOURCES:   # never refmatch's: no title, no output type, decided by the work tier only
        assert spark.sql(f'SELECT count(*) n FROM {s}_listed WHERE title IS NOT NULL OR output_type IS NOT NULL').first().n == 0, s
        assert {tier for tier, _, _ in matches(spark, s).values()} <= {'work', None}, s
    assert matches(spark, 'nwopen') == {
        ('P1', '0', 1): ('doi', ['cleaned', 'salvaged'], True),   # DOI salvaged from a link with a space in it
        ('P1', '1', 2): ('doi', ['cleaned', 'salvaged'], False),  # another title, none of the listed authors
        ('P1', '2', 3): ('title', ['title'], True),    # same title and year, 'van der Linden' -> linden is the work's last name
        ('P1', '3', 4): ('title', ['title'], False),   # a year apart and no number in common (a preprint of it)
        ('P1', '4', 5): ('title', ['title'], True),    # a year apart, the first page agrees
        ('P1', '5', 6): ('title', ['title'], False),   # NWO lists an article, the record is a dataset
        ('P1', '6', 7): ('title', ['title'], True),    # two same-title works, one shares the listed author
        ('P1', '8', None): (None, None, False),        # too short for the title tier
        ('P1', '9', 21): ('doi', ['cleaned'], True),   # %28/%29 decoded, '/full' and '?via' cut: each holder judged
        ('P1', '9', 22): ('doi', ['cleaned'], False),
        ('P1', '10', 2): ('doi', ['cleaned', 'salvaged'], False),  # the DOI decides: work 8's title is never tried
        ('P1', '11', 66): ('title', ['title'], True),  # a dataset record is held back only when NWO lists an article, book or thesis
        ('P1', '12', 12): ('title', ['title'], True),  # 'Linden, M.': the last name is the part before the comma
    }                                                  # no rows: the patent, last week's object
    assert matches(spark, 'fwf_open_api') == {
        ('F1', 'o-1', 101): ('doi', ['cleaned', 'salvaged'], True),
        ('F1', 'o-2', 104): ('pmid', ['pmid'], True),   # another title, FWF's author is the work's last name
        ('F1', 'o-3', 106): ('doi', ['cleaned', 'salvaged'], False),   # 101 authors: an author in common is not enough
        ('F1', 'o-4', 107): ('doi', ['cleaned', 'salvaged'], False),   # "Thomas" is a given name on the work
        ('F1', 'o-5', 105): ('doi', ['cleaned', 'salvaged'], False),   # the PMID's same-title work is never tried
        ('F1', 'o-6', 160): ('refmatch', ['refmatch'], True),      # the latest pick of that title (165 was the older one)
        ('F1', 'o-7', 161): ('refmatch', ['refmatch'], False),     # the guard: the work's title is not the listed one
        ('F1', 'o-8', None): (None, None, False),                  # the pick was for an older title
        ('F1', 'o-9', 101): ('doi', ['cleaned', 'salvaged'], True),   # the DOI decides: the pick is never tried
        ('F1', 'o-10', 163): ('refmatch', ['refmatch'], True),     # the chapter's title with "; In: <book>" cut is the work's
        ('F1', 'o-11', None): (None, None, False),                 # not linked by the matcher: no candidate
        ('F1', 'o-12', 160): ('refmatch', ['refmatch'], True),     # o-6's listed title and surnames: the tier reads the pick (the
                                                                   # matcher decides publications only, so a talk is never its own)
        ('F1', 'o-13', 160): ('refmatch', ['refmatch'], True),     # another id, the same output as listed: the same pick
        ('F1', 'o-14', None): (None, None, False),                 # the pick was made for another author list
        ('F1', 'o-15', 166): ('refmatch', ['refmatch'], False),    # FWF lists a preprint (an article), the record is a dataset
        ('F1', 'o-16', 167): ('refmatch', ['refmatch'], True),     # FWF lists an abstract: an abstract record may be it
        ('F1', 'o-17', 169): ('refmatch', ['refmatch'], True),     # listed as 'HEID' now; the pick 199 lives on as 169
    }
    types = {r.output_key: r.output_type for r in spark.sql(f"SELECT DISTINCT output_key, output_type FROM {T_MATCHES} WHERE source = 'fwf_open_api'").collect()}
    assert (types['o-1'], types['o-10'], types['o-12']) == ('article', 'publication', None)   # a talk is not a publication
    titles = {r.output_key: r.title for r in spark.sql(f"SELECT DISTINCT output_key, title FROM {T_MATCHES} WHERE source = 'fwf_open_api'").collect()}
    assert titles['o-10'] == T('Nucleation') + '; In: A handbook of soft matter'   # the title as listed, for the matcher
    anr = matches(spark, 'anr_opendata')
    assert anr[('ANR-X', '10.2000/c.3', 204)] == ('doi', ['cleaned', 'salvaged'], False)   # not an anr_opendata award
    assert sum(1 for k in anr if k[2] is None) == 120   # listed papers OpenAlex does not have: kept as unresolved rows
    assert {k: v for k, v in anr.items() if v[0] == 'work'} == {   # the work tier: the work HAL names, as named
        ('ANR-1', 'hal-01/205', 205): ('work', ['work'], True), ('ANR-2', 'hal-02/99999', 99999): ('work', ['work'], True),
        ('ANR-1', 'hal-03/201', 201): ('work', ['work'], True)}
    assert anr[('ANR-1', ' 10.2000/D.4 ', 206)] == ('doi', ['cleaned', 'salvaged'], True)   # spaces and capitals dropped
    assert anr[('ANR-2', 'https://doi.org/10.2000/e.5', 207)] == ('doi', ['cleaned', 'salvaged'], True)   # resolver prefix
    assert matches(spark, 'kaken') == {   # the API's list, and the 8 Jul links except on a paper with no DOI now (303)
        ('K1', '0', 301): ('doi', ['cleaned', 'salvaged'], True), ('K1', '1', None): (None, None, False),
        ('K1', 'W301', 301): ('work', ['work'], True), ('K1', 'W302', 302): ('work', ['work'], True),
        ('K1', 'W304', 304): ('work', ['work'], True),
        ('K1', '2', 310): ('refmatch', ['refmatch'], True),     # full-width listed, half-width keyed: cjk_key reads them equal
        ('K1', '3', 310): ('refmatch', ['refmatch'], False)}    # one title, no surnames, the year 8 years off: not accepted
    assert links(spark) == LINKED
    assert spark.sql("SELECT state FROM (SELECT inline(session.link_results)) WHERE state <> 'written'").collect() == []

    files = sorted(spark.table(T_LINKS).inputFiles())
    run_notebook(spark)                                # the same objects again: the MERGE changes nothing
    assert links(spark) == LINKED and sorted(spark.table(T_LINKS).inputFiles()) == files
    assert last_merge(spark) == {'numTargetRowsInserted': 0, 'numTargetRowsUpdated': 0, 'numTargetRowsDeleted': 0}


def test_a_source_failing_a_gate_keeps_its_links_while_the_others_are_written(spark):
    views(spark, raw=[r for r in RAW if r[0] != 'P1'])   # NWO's one grant is missing from raw
    held = [('fwf_open_api', 900 + k, FWF_F, ['F1']) for k in range(10)] + [('anr_opendata', 201, ANR_F, ['ANR-1'])]
    fresh(spark, links=held)
    with pytest.raises(Exception, match=r'LINKS_GATES: anr_opendata kept: growth, fwf_open_api kept: shrink, nwopen kept: coverage'):
        run_notebook(spark)
    got = links(spark)
    assert got['fwf_open_api'] == {(w, f): a for _, w, f, a in held[:10]}   # 2 links against 10: kept as they were
    assert got['anr_opendata'] == {(201, ANR_F): ['ANR-1']}                  # 8 links against 1: kept as they were
    assert 'nwopen' not in got                                               # none of its grants is an award: none written
    assert {s: got[s] for s in ('kaken', 'hakai_publications')} == {s: LINKED[s] for s in ('kaken', 'hakai_publications')}


def test_an_override_row_lets_one_source_through_shrink_for_the_day(spark):
    views(spark, raw=[r for r in RAW if r[0] != 'P1'])   # as above: FWF shrinks, ANR grows, NWO fails coverage
    held = [('fwf_open_api', 900 + k, FWF_F, ['F1']) for k in range(10)] + [('anr_opendata', 201, ANR_F, ['ANR-1'])]
    fresh(spark, links=held)
    spark.sql(f'DROP TABLE IF EXISTS {T_OVERRIDES}')
    spark.sql(f'CREATE TABLE {T_OVERRIDES} (source STRING, run_date DATE, reason STRING, by STRING) USING delta')
    spark.sql(f"INSERT INTO {T_OVERRIDES} VALUES ('fwf_open_api', current_date(), 'an intended undo', 'test'), "
              f"('nwopen', current_date(), 'coverage is never overridden', 'test'), ('anr_opendata', current_date() - 1, 'yesterday', 'test')")
    with pytest.raises(Exception, match=r'LINKS_GATES: anr_opendata kept: growth, nwopen kept: coverage'):
        run_notebook(spark)
    got = links(spark)
    assert got['fwf_open_api'] == LINKED['fwf_open_api']                    # today's row: written despite the shrink
    assert got['anr_opendata'] == {(201, ANR_F): ['ANR-1']}                  # yesterday's row: kept as it was
    assert 'nwopen' not in got
    spark.sql(f'DROP TABLE IF EXISTS {T_OVERRIDES}')


def test_a_dropped_link_is_deleted_and_a_source_removed_from_the_config_loses_its_links(spark):
    views(spark)
    fresh(spark, links=[('anr_opendata', w, ANR_F, ['ANR-1']) for w in (201, 202, 203, 205, 206, 999)]
                 + [('anr_opendata', 207, ANR_F, ['ANR-2'])])
    run_notebook(spark)                                # ANR no longer lists 999, and lists two grants on 201
    assert links(spark) == LINKED
    assert last_merge(spark) == {'numTargetRowsInserted': 21, 'numTargetRowsUpdated': 1, 'numTargetRowsDeleted': 1}   # 19 + HAL's CORDIS and ERC links
    run_notebook(spark, drop_source='hakai_publications')
    assert links(spark) == {s: v for s, v in LINKED.items() if s != 'hakai_publications'}
