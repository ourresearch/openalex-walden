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
SOURCES = ('nwopen', 'fwf_open_api', 'anr_opendata', 'kaken', 'hakai_publications')
OBJECTS = {  # the object each mapping reads: a prefix's newest *.parquet, or one object
    'nwopen': 's3://openalex-ingest/awards/nwopen/*.parquet',
    'fwf_open_api': 's3://openalex-ingest/awards/fwf_open_api/*.parquet',
    'anr_opendata': 's3://openalex-ingest/awards/anr/anr_work_links.parquet',
    'kaken': 's3://openalex-ingest/awards/kaken_api/*.parquet',
    'hakai_publications': 's3://openalex-ingest/awards/hakai/hakai_outputs.parquet',
}
HAL, KAKEN_OLD = 'openalex.awards.hal_anr_work_links', 'openalex.awards.kaken_work_funders'
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
    assert code(SQL).count('read_files(') == len(SOURCES)


def test_reads_the_newest_object_like_the_awards_job():
    awards_job = (ROOT / 'notebooks/awards/CreateOrUpdateAwards.sql').read_text()
    for s in ('nwopen', 'fwf_open_api'):
        assert f"SELECT '{s}' AS source, {PIN} AS object\n FROM read_files('{OBJECTS[s]}'" in awards_job, s


def test_one_matching_path_with_no_per_source_code():
    shared = code(SQL.split('CREATE OR REPLACE TEMP VIEW link_listed', 1)[1].split('\n', 7)[-1])
    for s in SOURCES:
        assert s not in shared, s
    assert 'CASE source' not in code(SQL) and 'CASE l.source' not in code(SQL)


def test_tables_read_and_written():
    tables = set(re.findall(r'\bopenalex\.\w+\.\w+', code(SQL)))
    assert tables == {MATCHES, LINKS, HAL, KAKEN_OLD, 'openalex.awards.openalex_awards_raw', 'openalex.works.openalex_works',
                      'openalex.works.location_work_match_keys'}, tables
    assert HAL in cell('CREATE OR REPLACE TEMP VIEW anr_opendata_listed')
    assert KAKEN_OLD in cell('CREATE OR REPLACE TEMP VIEW kaken_listed')
    writes = re.findall(r'\b(CREATE TABLE IF NOT EXISTS|CREATE OR REPLACE TABLE|INSERT INTO|MERGE INTO|DELETE FROM|UPDATE)\s+(\w+\.\w+\.\w+)',
                        code(SQL))
    # the matches table holds what each source lists (read once), then those outputs with their candidates; one write to the links
    assert writes == [('CREATE TABLE IF NOT EXISTS', LINKS), ('CREATE OR REPLACE TABLE', MATCHES), ('CREATE OR REPLACE TABLE', MATCHES),
                      ('MERGE INTO', LINKS)], writes
    assert SQL.index(f'CREATE TABLE IF NOT EXISTS {LINKS}') < SQL.index('LINKS_REGEX_MODE')   # its readers always have their input
    # one read for every source's candidates, and KAKEN's 8 Jul links dropped on a paper with no DOI now
    assert code(cell('CREATE OR REPLACE TABLE ' + MATCHES + ' USING delta AS\nWITH')).count('openalex.works.openalex_works') == 1
    assert code(cell('CREATE OR REPLACE TEMP VIEW kaken_listed')).count('openalex.works.openalex_works') == 1
    assert code(SQL).count('openalex.works.openalex_works') == 2


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


def test_match_text_is_location_work_match_keys_nt():
    keys = squash((ROOT / 'notebooks/location_matching/sql/keys.sql').read_text())
    nt = re.search(r"trim\(regexp_replace\(lower\(translate\(regexp_replace\(coalesce\(title, ''\).*?' '\)\) AS nt", keys).group(0)[:-len(' AS nt')]
    fn = squash(code(cell('CREATE OR REPLACE TEMPORARY FUNCTION match_text')))
    assert fn == f"CREATE OR REPLACE TEMPORARY FUNCTION match_text(s STRING) RETURNS STRING RETURN nullif({nt.replace('coalesce(title', 'coalesce(s')}, '');"
    assert code(SQL).count('translate(') == 1   # titles and names are folded only there


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
    for t in (T_MATCHES, T_LINKS):
        s.sql(f'DROP TABLE IF EXISTS {t}')


SCHEMA = os.environ.get('LINKS_TEST_SCHEMA', 'scratch')
T_MATCHES, T_LINKS = f'{SCHEMA}.frl_test_matches', f'{SCHEMA}.frl_test_links'
NWO_F, FWF_F, ANR_F, KAKEN_F, HAKAI_F = 4320321800, 4320321181, 4320320883, 4320334764, 4320334031
T = lambda k: f'{k} of the colloidal glass transition in long range systems'
ART, BOOK, PATENT, OTHER = 'Wetenschappelijk artikel', 'Boek', 'Octrooi', 'Overige Output'


def nwo(ptype, title, year, authors, url=None, volume=None, start=None):
    p = {'type': ptype, 'title': title, 'year': year, 'authors': [{'last_name': a, 'initials': 'X'} for a in authors]}
    p.update({k: v for k, v in (('url_open_access', url), ('edition', volume), ('start', start)) if v})
    return p


def fwf(oid, title, doi=None, pmid=None, surnames=('Heid',)):
    return dict(output_id=oid, category='publications', type='Journal Article', doi=doi, pmid=pmid, title=title, surnames=list(surnames))


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
              fwf('o-5', T('Diffusion'), doi='10.1000/b.5', pmid='555')]}                # the DOI's work is another item: PMID not tried
ANR = ([('10.2000/a.1', ['ANR-2', 'ANR-1']),                # two grants: award_ids sorted
        ('10.2000/shared', ['ANR-1']),                      # a DOI two works hold: both linked
        ('10.2000/c.3', ['ANR-X']),                         # not an award in raw: not linked
        (' 10.2000/D.4 ', ['ANR-1']),                       # spaces and capitals around the DOI
        ('https://doi.org/10.2000/e.5', ['ANR-2'])]         # the DOI as a resolver URL
       + [(f'10.2000/none.{k}', [f'ANR-K{k}']) for k in range(120)])   # grants with unindexed papers: 122 of 123 grants are awards
KAKEN = {'K1': [{'type': 'Journal Article', 'doi': '10.3000/k.1'}, {'type': 'Presentation', 'title': 'A talk'}]}
KAKEN_LEGACY = [(301, KAKEN_F, ['K1']),          # also in the API's list: one link
                (302, KAKEN_F, ['K1']),          # the paper still has a DOI: kept
                (303, KAKEN_F, ['K1']),          # the paper is in OpenAlex with no DOI now: dropped
                (304, KAKEN_F, ['K1'])]          # merged away or gone: kept as named (Nightly moves it to the survivor)
HAKAI = ['https://doi.org/10.4000/h.1', 'https://doi.org/10.4000/unindexed']
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
]
RAW = ([('P1', 'nwopen', NWO_F), ('F1', 'fwf_open_api', FWF_F), ('ANR-1', 'anr_opendata', ANR_F), ('ANR-2', 'anr_opendata', ANR_F),
        ('ANR-X', 'crossref_work', ANR_F), ('K1', 'kaken', KAKEN_F)]   # ANR-X: another source's award
       + [(f'ANR-K{k}', 'anr_opendata', ANR_F) for k in range(120)])
LINKED = {  # source -> {(work_id, funder_id): award_ids}
    'nwopen': {(1, NWO_F): ['P1'], (3, NWO_F): ['P1'], (5, NWO_F): ['P1'], (7, NWO_F): ['P1'], (21, NWO_F): ['P1'], (66, NWO_F): ['P1'],
               (12, NWO_F): ['P1']},
    'fwf_open_api': {(101, FWF_F): ['F1'], (104, FWF_F): ['F1']},
    'anr_opendata': {(201, ANR_F): ['ANR-1', 'ANR-2'], (202, ANR_F): ['ANR-1'], (203, ANR_F): ['ANR-1'], (205, ANR_F): ['ANR-1'],
                     (99999, ANR_F): ['ANR-2'], (206, ANR_F): ['ANR-1'], (207, ANR_F): ['ANR-2']},
    'kaken': {(301, KAKEN_F): ['K1'], (302, KAKEN_F): ['K1'], (304, KAKEN_F): ['K1']},
    'hakai_publications': {(401, HAKAI_F): []},
}


def nt(title):
    return ' '.join(re.sub(r'[^a-z0-9]+', ' ', title.lower()).split())


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
    spark.createDataFrame([(w[0], w[5], nt(w[4])) for w in WORKS], 'id bigint, yr int, nt string').createOrReplaceTempView('t_keys')
    spark.createDataFrame(
        [(i, d and f'https://doi.org/{d}', {'pmid': f'https://pubmed.ncbi.nlm.nih.gov/{p}'} if p else {}, ty, t, y, (v, fp),
          [((a,),) for a in names]) for i, d, p, ty, t, y, v, fp, names in WORKS],
        'id bigint, doi string, ids map<string,string>, type string, title string, publication_year int, '
        'biblio struct<volume:string,first_page:string>, authorships array<struct<author:struct<display_name:string>>>'
    ).createOrReplaceTempView('t_works')


def sub(sql, drop_source=None):
    names = {OBJECTS[s]: v for s, v in zip(SOURCES, ('t_nwopen', 't_fwf', 't_anr', 't_kaken', 't_hakai'))}
    sql = re.sub(r"read_files\('([^']+)'[^)]*\)", lambda m: names[m.group(1)], sql)
    for table, name in ((HAL, 't_hal'), (KAKEN_OLD, 't_kaken_old'), ('openalex.awards.openalex_awards_raw', 't_raw'), ('openalex.works.openalex_works', 't_works'),
                        ('openalex.works.location_work_match_keys', 't_keys'), (MATCHES, T_MATCHES), (LINKS, T_LINKS)):
        sql = sql.replace(table, name)
    if drop_source:
        sql = re.sub(r",\n  \('%s',[^\n]*\)" % drop_source, '', sql)
    return sql


def run_notebook(spark, drop_source=None):
    for c in CELLS:
        statement = sub(code(c), drop_source).strip().rstrip(';')
        if statement:
            spark.sql(statement).collect()


def fresh(spark, links=()):
    for t in (T_MATCHES, T_LINKS):
        spark.sql(f'DROP TABLE IF EXISTS {t}')
    spark.sql(f'CREATE TABLE {T_LINKS} (source STRING, work_id BIGINT, funder_id BIGINT, award_ids ARRAY<STRING>) USING delta')
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
    for s in SOURCES:
        assert spark.table(f'{s}_listed').columns == COLUMNS, s
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
    }
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
        ('K1', 'W304', 304): ('work', ['work'], True)}
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


def test_a_dropped_link_is_deleted_and_a_source_removed_from_the_config_loses_its_links(spark):
    views(spark)
    fresh(spark, links=[('anr_opendata', w, ANR_F, ['ANR-1']) for w in (201, 202, 203, 205, 206, 999)]
                 + [('anr_opendata', 207, ANR_F, ['ANR-2'])])
    run_notebook(spark)                                # ANR no longer lists 999, and lists two grants on 201
    assert links(spark) == LINKED
    assert last_merge(spark) == {'numTargetRowsInserted': 14, 'numTargetRowsUpdated': 1, 'numTargetRowsDeleted': 1}
    run_notebook(spark, drop_source='hakai_publications')
    assert links(spark) == {s: v for s, v in LINKED.items() if s != 'hakai_publications'}
