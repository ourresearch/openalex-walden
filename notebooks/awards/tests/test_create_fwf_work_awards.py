"""CreateFWFWorkAwards (oxjob #1765): FWF's own output lists -> fwf_work_funders, a Nightly Grants leg.

The contract tests run anywhere. The behaviour tests run the whole notebook, cell by cell, on fixture rows; its writes need
Delta, so they run on Databricks Connect only: set DATABRICKS_CONFIG_PROFILE and FWF_TEST_SCHEMA (a scratch schema such
as openalex_dev.<you>, where they create and drop fwf_test_matches and fwf_test_leg). Elsewhere they skip.
"""
import json
import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SQL = (ROOT / 'notebooks/awards/CreateFWFWorkAwards.sql').read_text()
CELLS = SQL.split('-- COMMAND ----------')
OBJECTS = "read_files('s3://openalex-ingest/awards/fwf_open_api/*.parquet', format => 'parquet', mergeSchema => true)"
PIN = 'max_by(_metadata.file_path, struct(_metadata.file_modification_time, _metadata.file_path))'
MATCHES, LEG = 'openalex.awards.fwf_work_award_matches', 'openalex.awards.fwf_work_funders'
LISTED = f'INSERT INTO {MATCHES}'
RESOLVE = f'MERGE INTO {MATCHES}'
WRITE_LEG = f'MERGE INTO {LEG}'


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

def test_reads_the_newest_fwf_object_like_the_awards_job():
    awards_job = (ROOT / 'notebooks/awards/CreateOrUpdateAwards.sql').read_text()
    assert f"SELECT 'fwf_open_api' AS source, {PIN} AS object" in awards_job
    pin = squash(code(cell('SET VAR fwf_object')))
    assert pin == squash(f"SET VAR fwf_object = (SELECT {PIN} FROM {OBJECTS});")   # mergeSchema: older objects read as NULL
    listed = squash(code(cell(LISTED)))
    assert f"FROM {OBJECTS} WHERE _metadata.file_path = session.fwf_object AND outputs IS NOT NULL AND session.fwf_changed" in listed
    assert code(SQL).count('read_files(') == 3   # the pin, what FWF lists, the coverage gate: each reads it once
    assert code(SQL).count('openalex.works.openalex_works') == 1


def test_tables_read_and_written():
    tables = set(re.findall(r'\bopenalex\.\w+\.\w+', code(SQL)))
    assert tables == {MATCHES, LEG, 'openalex.awards.openalex_awards_raw', 'openalex.works.openalex_works'}, tables
    assert re.findall(r'CREATE (?:OR REPLACE )?TABLE (?:IF NOT EXISTS )?(\S+)', SQL) == [LEG, MATCHES]
    assert 'CREATE OR REPLACE TABLE' not in SQL
    writes = re.findall(r'\b(INSERT INTO|MERGE INTO|DELETE FROM|UPDATE)\s+(\w+\.\w+\.\w+)', code(SQL))
    assert writes == [('DELETE FROM', MATCHES), ('INSERT INTO', MATCHES), ('MERGE INTO', MATCHES), ('MERGE INTO', LEG)], writes
    assert SQL.index(f'CREATE TABLE IF NOT EXISTS {LEG}') < SQL.index('FWF_REGEX_MODE')   # Nightly always has its input


def test_nightly_reads_the_leg_and_a_task_of_the_union_job_builds_it():
    nightly = (ROOT / 'notebooks/awards/nightly_grants/lib/work_awards.py').read_text()
    assert re.search(r"\(\s*'fwf_work_funders'\s*,\s*'work_id'\s*,\s*'award_ids'\s*,\s*True\s*,\s*3\s*,", nightly)
    runtime = (ROOT / 'notebooks/awards/nightly_grants/lib/nightly_runtime.py').read_text()
    assert '"fwf_work_funders"' in runtime.split('REQUIRED_INPUTS', 1)[1].split('}', 1)[0]
    config = json.loads((ROOT / 'notebooks/awards/nightly_grants/config.prod.json').read_text())
    assert config['inputs']['fwf_work_funders'] == LEG
    job = (ROOT / 'jobs/funder_reported_work_funders.yaml').read_text()
    assert 'quartz_cron_expression: 0 30 2 * * ?' in job   # 02:30 UTC, before Nightly Grants (03:00)
    task = job.split('- task_key: create_fwf_work_funders', 1)[1].split('git_source:', 1)[0]
    assert 'notebook_path: notebooks/awards/CreateFWFWorkAwards' in task
    assert 'depends_on:' not in code(job.replace('#', '--'))   # neither task can stop the other
    assert 'on_failure:' in task and 'timeout_seconds: 3600' in task
    assert 'fwf' not in (ROOT / 'databricks.yml').read_text().lower()
    # deletions must drop: the additive union (MERGE ... WHEN NOT MATCHED) never reads this leg
    assert 'fwf' not in (ROOT / 'notebooks/awards/CreateFunderReportedWorkFunders.ipynb').read_text().lower()


def test_leg_table_keeps_the_shape_nightly_reads():
    leg = squash(code(cell(WRITE_LEG)))
    assert ('SELECT work_id, CAST(4320321181 AS BIGINT) AS funder_id, array_sort(array_distinct(collect_list(project_id))) '
            'AS award_ids') in leg
    assert squash(code(cell(f'CREATE TABLE IF NOT EXISTS {LEG}'))) == squash(
        f'CREATE TABLE IF NOT EXISTS {LEG} (work_id BIGINT, funder_id BIGINT, award_ids ARRAY<STRING>) USING delta;')


def test_gates_run_before_the_leg_changes():
    for gate in ('FWF_REGEX_MODE', 'FWF_RAW_COVERAGE', 'FWF_SHRINK_GUARD', 'FWF_GROWTH_CEILING'):
        assert SQL.index(gate) < SQL.index(WRITE_LEG), gate
    assert SQL.index(RESOLVE) < SQL.index('FWF_RAW_COVERAGE')


def test_match_text_is_location_work_match_keys_nt():
    keys = squash((ROOT / 'notebooks/location_matching/sql/keys.sql').read_text())
    nt = re.search(r"trim\(regexp_replace\(lower\(translate\(regexp_replace\(coalesce\(title, ''\).*?' '\)\) AS nt", keys).group(0)[:-len(' AS nt')]
    fn = squash(code(cell('CREATE OR REPLACE TEMPORARY FUNCTION match_text')))
    assert fn == f"CREATE OR REPLACE TEMPORARY FUNCTION match_text(s STRING) RETURNS STRING RETURN nullif({nt.replace('coalesce(title', 'coalesce(s')}, '');"
    body = code(SQL)
    assert body.count('translate(') == 1   # titles and names are folded only there


def test_regex_backslashes_are_double_escaped_and_strings_hold_no_semicolon():
    body = code(SQL)
    assert not re.search(r'(?<!\\)\\(?!\\)', body.replace('\\\\', '')), 'a single backslash in SQL'
    for literal in re.findall(r"'((?:[^']|'')*)'", body):
        assert ';' not in literal, literal


# ---------------------------------------------------------------- behaviour: the whole notebook on fixture rows

@pytest.fixture(scope='module')
def spark():
    if not (os.environ.get('DATABRICKS_CONFIG_PROFILE') and os.environ.get('FWF_TEST_SCHEMA')):
        pytest.skip('needs Databricks Connect and a scratch schema (DATABRICKS_CONFIG_PROFILE, FWF_TEST_SCHEMA)')
    dc = pytest.importorskip('databricks.connect')
    s = dc.DatabricksSession.builder.profile(os.environ['DATABRICKS_CONFIG_PROFILE']).serverless(True).getOrCreate()
    yield s
    for t in (T_MATCHES, T_LEG):
        s.sql(f'DROP TABLE IF EXISTS {t}')


SCHEMA = os.environ.get('FWF_TEST_SCHEMA', 'scratch')
T_MATCHES, T_LEG = f'{SCHEMA}.fwf_test_matches', f'{SCHEMA}.fwf_test_leg'
T = lambda k: f'{k} in polarizable molecular dynamics simulations of ionic liquids'


def out(oid, title, doi=None, pmid=None, surnames=('Heid', 'Boresch')):
    return dict(output_id=oid, category='publications', type='Journal Article', doi=doi, pmid=pmid, title=title,
                surnames=list(surnames) if surnames else None)


NEW = {'P1': [out('output-1', T('Charge transfer'), doi='10.1000/a.1'),                      # DOI, same title
              out('output-2', T('Diffusion'), doi='10.1000/b.2', pmid='222'),               # the DOI decides: another item
              out('output-3', 'Ein deutscher Titel', pmid='333', surnames=('Heid',)),       # PMID, other title, author on it
              out('output-6', T('Solvation'), doi='10.1000/dup.6'),                         # a DOI two works hold
              out('output-7', T('Nothing'), surnames=None),                                 # nothing to resolve
              out('output-10', 'Ein Konsortium', doi='10.1000/big.7', surnames=('Heid',)),  # 101 authors: the title decides
              out('output-11', 'Anderer Titel', doi='10.1000/tm.8', surnames=('Thomas', 'Müller')),  # a given name only
              out('output-12', 'Noch ein Titel', doi='10.1000/xu.9', surnames=('Xu',))],    # a 2-letter surname
       'P2': [out('output-1', T('Charge transfer'), doi='10.1000/a.1')],                    # one output, two projects
       'P9': [out('output-8', T('Charge transfer'), doi='10.1000/a.1')]}
OLD = {'P1': [out('output-1', 'Last week', doi='10.1000/old.1')]}
WORKS = [  # id, doi, pmid, title, author names
    (1, 'https://doi.org/10.1000/a.1', None, T('Charge transfer'), ['Elisa Heid']),
    (2, 'https://doi.org/10.1000/b.2', None, 'A different paper on that DOI', ['Q. Other']),
    (3, None, '222', T('Diffusion'), ['Elisa Heid']),
    (4, None, '333', 'Polarizable MD of ionic liquids', ['Elisa Heid', 'Christian Schröder']),
    (21, 'https://doi.org/10.1000/dup.6', None, T('Solvation'), ['E. Heid']),
    (22, 'https://doi.org/10.1000/dup.6', None, 'Another record', ['E. Heid']),
    (6, 'https://doi.org/10.1000/big.7', None, 'A consortium paper', ['Elisa Heid'] + [f'Author {i}' for i in range(100)]),
    (7, 'https://doi.org/10.1000/tm.8', None, 'Something else', ['Thomas Schmidt']),
    (8, 'https://doi.org/10.1000/xu.9', None, 'Another title', ['Ning Xu']),
    (31, 'https://doi.org/10.1000/old.1', None, 'Last week', ['E. Heid']),
]
RAW = [(p, 'fwf_open_api', 4320321181) for p in ('P1', 'P2', 'P9')]


def views(spark, raw=RAW):
    from pyspark.sql.types import ArrayType, LongType, MapType, StringType, StructField, StructType
    o = StructType([StructField(k, StringType()) for k in ('output_id', 'category', 'type', 'doi', 'pmid', 'title')]
                   + [StructField('surnames', ArrayType(StringType()))])
    files = [(c, v, 's3://x/new.parquet', 2) for c, v in NEW.items()] + [(c, v, 's3://x/old.parquet', 1) for c, v in OLD.items()]
    spark.createDataFrame(files, StructType([StructField('code', StringType()), StructField('outputs', ArrayType(o)),
                          StructField('file_path', StringType()), StructField('file_modification_time', LongType())])
                          ).createOrReplaceTempView('t_fwf')
    spark.createDataFrame(raw, 'funder_award_id string, provenance string, funder_id bigint').createOrReplaceTempView('t_raw')
    spark.createDataFrame([(i, d, {'pmid': f'https://pubmed.ncbi.nlm.nih.gov/{p}'} if p else {}, t, [((a,),) for a in names])
                           for i, d, p, t, names in WORKS],
                          StructType([StructField('id', LongType()), StructField('doi', StringType()),
                                      StructField('ids', MapType(StringType(), StringType())), StructField('title', StringType()),
                                      StructField('authorships', ArrayType(StructType([StructField('author', StructType([
                                          StructField('display_name', StringType())]))])))])).createOrReplaceTempView('t_works')


def sub(sql):
    for table, name in ((OBJECTS, 't_fwf'), ('openalex.awards.openalex_awards_raw', 't_raw'), ('openalex.works.openalex_works', 't_works'),
                        (MATCHES, T_MATCHES), (LEG, T_LEG)):
        sql = sql.replace(table, name)
    return sql.replace('_metadata.file_path', 'file_path').replace('_metadata.file_modification_time', 'file_modification_time')


def run_notebook(spark, only=None):
    for c in CELLS if only is None else [cell(only)]:
        statement = sub(code(c)).strip().rstrip(';')
        if statement:
            spark.sql(statement).collect()


def fresh(spark, leg=()):
    for t in (T_MATCHES, T_LEG):
        spark.sql(f'DROP TABLE IF EXISTS {t}')
    spark.sql(f'CREATE TABLE {T_LEG} (work_id BIGINT, funder_id BIGINT, award_ids ARRAY<STRING>) USING delta')
    if leg:
        spark.createDataFrame(leg, 'work_id bigint, funder_id bigint, award_ids array<string>').write.mode('append').saveAsTable(T_LEG)


def state(spark):
    matches = {(r.project_id, r.output_id): (r.work_id, r.tier, r.accepted) for r in spark.sql(f'SELECT * FROM {T_MATCHES}').collect()}
    return matches, {r.work_id: list(r.award_ids) for r in spark.sql(f'SELECT * FROM {T_LEG}').collect()}


def files(spark):
    return [sorted(spark.table(t).inputFiles()) for t in (T_MATCHES, T_LEG)]


def test_the_rule_on_fixture_rows_and_an_unchanged_object_is_skipped(spark):
    views(spark)
    fresh(spark)
    run_notebook(spark)
    matches, leg = state(spark)
    assert matches == {
        ('P1', 'output-1'): (1, 'doi', True),       # DOI, same title
        ('P1', 'output-2'): (2, 'doi', False),      # the DOI's work is another item: not linked (the PMID is not tried)
        ('P1', 'output-3'): (4, 'pmid', True),      # title in another language, FWF's author on the work
        ('P1', 'output-6'): (None, None, False),    # two works hold that DOI: unresolved
        ('P1', 'output-7'): (None, None, False),    # nothing to resolve: kept as an unresolved row
        ('P1', 'output-10'): (6, 'doi', False),     # a 101-author work: an author in common is not enough
        ('P1', 'output-11'): (7, 'doi', False),     # "Thomas" is a given name on the work, not a last name
        ('P1', 'output-12'): (8, 'doi', True),      # Xu: two letters count
        ('P2', 'output-1'): (1, 'doi', True),
        ('P9', 'output-8'): (1, 'doi', True),
    }                                               # nothing from last week's object
    assert leg == {1: ['P1', 'P2', 'P9'], 4: ['P1'], 8: ['P1']}
    before = files(spark)
    run_notebook(spark)                             # the same object again: empty commits, no data file added or removed
    assert spark.sql('SELECT session.fwf_changed').collect()[0][0] is False
    assert state(spark) == (matches, leg) and files(spark) == before, 'a skipped run rewrote a table'


def test_gates_stop_the_run_and_the_next_run_redoes_it(spark):
    views(spark, raw=RAW[:2] + [('P9', 'openaire_fwf', 4320321181)])   # P9 lists outputs but isn't an FWF award in raw
    fresh(spark)
    with pytest.raises(Exception, match='FWF_RAW_COVERAGE'):
        run_notebook(spark)
    matches, leg = state(spark)
    assert leg == {} and not any(p == 'P9' for p, _ in matches)
    views(spark)                                    # raw refreshed: the next run redoes the build
    run_notebook(spark)
    assert state(spark)[1] == {1: ['P1', 'P2', 'P9'], 4: ['P1'], 8: ['P1']}

    fresh(spark, leg=[(k, 4320321181, ['P1']) for k in range(100, 110)])
    with pytest.raises(Exception, match='FWF_SHRINK_GUARD'):   # 5 accepted links against 10
        run_notebook(spark)
    assert len(state(spark)[1]) == 10

    fresh(spark)
    run_notebook(spark)
    spark.sql(f"INSERT INTO {T_MATCHES} SELECT project_id, concat(output_id, '-', id), ptype, doi, pmid, title_key, surname_keys, "
              f"id, tier, title_distance, author_on_work, true, fwf_object FROM {T_MATCHES} CROSS JOIN range(200001) "
              f"WHERE project_id = 'P1' AND output_id = 'output-1'")
    spark.sql(f'DELETE FROM {T_LEG}')
    with pytest.raises(Exception, match='FWF_GROWTH_CEILING'):   # over 200,000 accepted links against an empty leg
        run_notebook(spark, only='FWF_GROWTH_CEILING')
