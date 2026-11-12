"""CreateNWOWorkAwards (oxjob #244, refresh and title tier #2014): NWO's own output lists -> nwo_work_funders.

The contract tests run anywhere. The behaviour test runs the notebook's own SQL on fixture views where a real Spark is
available: local pyspark + JVM, or Databricks Connect when DATABRICKS_CONFIG_PROFILE is set. Elsewhere it skips.
"""
import json
import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
NB = ROOT / 'notebooks/awards/CreateNWOWorkAwards.sql'
SQL = NB.read_text()
CELLS = SQL.split('-- COMMAND ----------')
NWOPEN = "read_files('s3://openalex-ingest/awards/nwopen/*.parquet', format => 'parquet')"
PIN = 'max_by(_metadata.file_path, struct(_metadata.file_modification_time, _metadata.file_path))'
MATCHES = 'CREATE OR REPLACE TABLE openalex.awards.nwo_work_award_matches USING delta AS'
LEG = 'CREATE OR REPLACE TABLE openalex.awards.nwo_work_funders USING delta AS'


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

def test_one_notebook_for_the_job_and_the_job_unchanged():
    assert not (ROOT / 'notebooks/awards/CreateNWOWorkAwards.ipynb').exists()
    job = (ROOT / 'jobs/nwo_work_funders.yaml').read_text()
    assert 'notebook_path: notebooks/awards/CreateNWOWorkAwards' in job
    assert 'pause_status: PAUSED' in job  # on demand: run after the weekly NWOpen fetch (#244)


def test_reads_the_newest_nwopen_object_like_the_awards_job():
    awards_job = (ROOT / 'notebooks/awards/CreateOrUpdateAwards.sql').read_text()
    assert f"SELECT 'nwopen' AS source, {PIN} AS object" in awards_job  # the Awards job's own pin for this source
    m = code(cell(MATCHES))
    assert PIN in m and m.count(NWOPEN) == 2
    for stale in ('nwo_projects_raw', 'openalex.awards.nwo_awards', 'openalex.works.locations '):
        assert stale not in SQL, stale  # the 01-08 laptop table, its stale staging twin, a table that no longer exists


def test_tables_read_and_written():
    tables = set(re.findall(r'\bopenalex\.\w+\.\w+', code(SQL)))
    assert tables == {'openalex.awards.nwo_work_award_matches', 'openalex.awards.nwo_work_funders',
                      'openalex.awards.openalex_awards_raw', 'openalex.works.location_work_match_keys',
                      'openalex.works.openalex_works'}, tables
    written = re.findall(r'CREATE OR REPLACE TABLE (\S+)', SQL)
    assert written == ['openalex.awards.nwo_work_award_matches', 'openalex.awards.nwo_work_funders']
    assert not re.search(r'\b(INSERT|MERGE|DELETE|UPDATE)\b', code(SQL))


def test_leg_table_keeps_the_shape_nightly_reads():
    leg = squash(code(cell(LEG)))
    assert 'SELECT work_id, CAST(4320321800 AS BIGINT) AS funder_id, array_sort(array_distinct(collect_list(project_id))) AS award_ids' in leg
    assert 'WHERE accepted GROUP BY work_id' in leg
    nightly = (ROOT / 'notebooks/awards/nightly_grants/lib/work_awards.py').read_text()
    assert re.search(r"\(\s*'nwo_work_funders'\s*,\s*'work_id'\s*,\s*'award_ids'\s*,\s*True\s*,\s*3\s*,", nightly)


def test_gates_run_before_the_leg_table_is_replaced():
    for gate in ('NWO_REGEX_MODE', 'NWO_RAW_COVERAGE', 'NWO_DUPLICATE_OUTPUTS', 'NWO_SHRINK_GUARD'):
        assert SQL.index(gate) < SQL.index(LEG), gate
    assert SQL.index(MATCHES) < SQL.index('NWO_RAW_COVERAGE')


def test_awards_come_from_raw_nwopen_rows():
    m = squash(code(cell(MATCHES)))
    assert ("SELECT DISTINCT funder_award_id AS project_id FROM openalex.awards.openalex_awards_raw "
            "WHERE provenance = 'nwopen' AND funder_id = 4320321800") in m
    assert 'JOIN nwo_award_keys k ON k.project_id = e.project_id' in m


def test_title_normalization_is_the_location_keys_expression():
    keys = squash((ROOT / 'notebooks/location_matching/sql/keys.sql').read_text())
    nt = re.search(r"trim\(regexp_replace\(lower\(translate\(regexp_replace\(coalesce\(title, ''\).*?' '\)\) AS nt", keys).group(0)
    nt = nt[:-len(' AS nt')]
    m = squash(cell(MATCHES))
    assert nt in m                                    # listed titles, as the keys table normalizes works' titles
    assert nt.replace('coalesce(title', 'coalesce(w.title') in m  # and the DOI tier's work titles


def test_regex_backslashes_are_double_escaped_and_strings_hold_no_semicolon():
    body = code(SQL)
    assert not re.search(r'(?<!\\)\\(?!\\)', body.replace('\\\\', '')), 'a single backslash in SQL'
    for literal in re.findall(r"'((?:[^']|'')*)'", body):
        assert ';' not in literal, literal  # statement splitters (q.py, dry runs) cut on ';'


def test_the_rule_is_one_row_of_settings():
    rules = squash(cell(MATCHES).split('nwo_object AS', 1)[0])
    assert ('SELECT 20 AS min_title_chars, 1 AS max_year_gap, 50 AS max_same_title_works, 0.2 AS max_doi_title_distance' in rules)
    m = squash(code(cell(MATCHES)))
    # a title match is accepted with a shared author, not to a dataset-like record, and with the same year or a shared number
    assert ("x.tier = 'title' AND x.nt = w.wnt AND arrays_overlap(x.surnames, w.name_words) AND NOT (array_contains(r.held_work_types, w.type) "
            "AND array_contains(r.typed_entries, x.ptype)) AND (x.yr = w.yr OR x.volume_number = w.volume_number "
            "OR x.first_page_number = w.first_page_number)") in m


# ---------------------------------------------------------------- behaviour on fixture rows (needs a Spark)

@pytest.fixture(scope='module')
def spark():
    if os.environ.get('DATABRICKS_CONFIG_PROFILE'):
        dc = pytest.importorskip('databricks.connect')
        yield dc.DatabricksSession.builder.profile(os.environ['DATABRICKS_CONFIG_PROFILE']).serverless(True).getOrCreate()
        return
    pyspark = pytest.importorskip('pyspark', minversion='4.0')  # try_url_decode
    if not hasattr(pyspark, '__version__'):
        pytest.skip('pyspark stub, not real Spark')
    from pyspark.sql import SparkSession
    try:
        s = SparkSession.builder.master('local[1]').appName('nwo_work_awards').getOrCreate()
    except Exception as e:  # pragma: no cover - no JVM
        pytest.skip(f'no local Spark: {e}')
    yield s
    s.stop()


def nt(title):
    return ' '.join(re.sub(r'[^a-z0-9]+', ' ', title.lower()).split())


def product(ptype, title, year, authors, url=None, volume=None, start=None):
    p = {'type': ptype, 'title': title, 'year': year, 'authors': [{'last_name': a, 'initials': 'X'} for a in authors]}
    if url:
        p['url_open_access'] = url
    if volume:
        p['edition'] = volume
    if start:
        p['start'] = start
    return p


ART, BOOK, PATENT = 'Wetenschappelijk artikel', 'Boek', 'Octrooi'
T = {k: f'{v} of the colloidal glass transition in long range systems' for k, v in {
    'doi': 'A study', 'far': 'Unrelated listing', 'exact': 'Crystal growth', 'off': 'Shear melting', 'page': 'Hopping dynamics',
    'review': 'Aging', 'dataset': 'Light scattering data', 'twins': 'Rheology', 'dup': 'Sedimentation', 'noyear': 'Diffusion',
    'cap': 'Glass formers'}.items()}
NEW = {  # project -> listed outputs (the newest object)
    'P1': [product(ART, T['doi'], 2020, ['Linden'], url='https://doi.org/10.1000/ ABC.1'),           # 0 DOI, salvaged
           product(ART, T['far'], 2020, ['Linden'], url='https://doi.org/10.1000/xyz.2'),            # 1 DOI, another item
           product(ART, T['exact'], 2019, ['van der Linden']),                                        # 2 title, same year
           product(ART, T['off'], 2019, ['Linden']),                                                  # 3 title, year apart
           product(ART, T['page'], 2019, ['Linden'], volume='12', start='345'),                       # 4 year apart, page
           product(BOOK, T['review'], 2015, ['Linden']),                                              # 5 a review of it
           product(ART, T['dataset'], 2018, ['Linden']),                                              # 6 dataset record
           product(ART, T['twins'], 2017, ['Linden']),                                                # 7 two same-title works
           product(PATENT, T['exact'], 2019, ['Linden']),                                             # 8 patents: no row
           product(ART, 'Short note', 2019, ['Linden']),                                              # 9 too short
           product(ART, T['dup'], 2020, ['Linden'], url='https://dx.doi.org/10.1000/dup%281%29/full?via=x'),  # 10 shared DOI
           product(ART, T['noyear'], None, ['Linden']),                                               # 11 no year
           product(ART, T['cap'], 2016, ['Linden'])],                                                 # 12 51 same-title works
    'P9': [product(ART, T['doi'], 2020, ['Linden'], url='https://doi.org/10.1000/abc.1')],           # not an NWO award
}
OLD = {'P1': [product(ART, 'An old listing from last week of the glass transition', 2010, ['Linden'])]}
WORKS = [  # id, doi, type, title, year, volume, first page, author names
    (1, 'https://doi.org/10.1000/abc.1', 'article', T['doi'], 2020, None, None, ['M. Linden']),
    (2, 'https://doi.org/10.1000/xyz.2', 'article', 'Something else entirely', 2020, None, None, ['Q. Other']),
    (3, None, 'article', T['exact'], 2019, None, None, ['Marjolein van der Linden']),
    (4, None, 'preprint', T['off'], 2018, None, None, ['M. Linden']),
    (5, 'https://doi.org/10.1000/p.5', 'article', T['page'], 2020, '12', '345-350', ['M. Linden']),
    (6, 'https://doi.org/10.1000/r.6', 'book-review', T['review'], 2015, None, None, ['A. Reviewer']),
    (7, 'https://doi.org/10.1000/d.7', 'dataset', T['dataset'], 2018, None, None, ['M. Linden']),
    (8, None, 'article', T['twins'], 2017, None, None, ['M. Linden']),
    (88, None, 'article', T['twins'], 2017, None, None, ['Z. Someone']),
    (11, None, 'article', 'An old listing from last week of the glass transition', 2010, None, None, ['M. Linden']),
    (21, 'https://doi.org/10.1000/dup(1)', 'article', T['dup'], 2020, None, None, ['M. Linden']),     # holds the listed title
    (22, 'https://doi.org/10.1000/dup(1)', 'article', 'A different record on that DOI', 2020, None, None, ['M. Linden']),
    (31, None, 'article', T['noyear'], 2019, None, None, ['M. Linden']),
] + [(1000 + k, None, 'article', T['cap'], 2016, None, None, ['M. Linden']) for k in range(51)]


def run_matches(spark):
    from pyspark.sql.types import (ArrayType, IntegerType, LongType, StringType, StructField, StructType)
    files = [(p, json.dumps(v), 's3://x/new.parquet', 2) for p, v in NEW.items()] + \
            [(p, json.dumps(v), 's3://x/old.parquet', 1) for p, v in OLD.items()]
    views = {
        NWOPEN: ('t_nwopen', spark.createDataFrame(files, 'project_id string, products_json string, file_path string, file_modification_time bigint')),
        'openalex.awards.openalex_awards_raw': ('t_raw', spark.createDataFrame(
            [('P1', 'nwopen', 4320321800), ('P9', 'crossref_work', 4320321800)], 'funder_award_id string, provenance string, funder_id bigint')),
        'openalex.works.location_work_match_keys': ('t_keys', spark.createDataFrame(
            [(w[0], w[4], nt(w[3])) for w in WORKS], 'id bigint, yr int, nt string')),
        'openalex.works.openalex_works': ('t_works', spark.createDataFrame(
            [(w[0], w[1], w[2], w[3], w[4], (w[5], w[6]), [((a,),) for a in w[7]]) for w in WORKS],
            StructType([StructField('id', LongType()), StructField('doi', StringType()), StructField('type', StringType()),
                        StructField('title', StringType()), StructField('publication_year', IntegerType()),
                        StructField('biblio', StructType([StructField('volume', StringType()), StructField('first_page', StringType())])),
                        StructField('authorships', ArrayType(StructType([StructField('author', StructType([
                            StructField('display_name', StringType())]))])))]))),
    }
    query = code(cell(MATCHES)).split(MATCHES, 1)[1].strip().rstrip(';')
    for table, (name, df) in views.items():
        df.createOrReplaceTempView(name)
        query = query.replace(table, name)
    query = query.replace('_metadata.file_path', 'file_path').replace('_metadata.file_modification_time', 'file_modification_time')
    rows = spark.sql(query).collect()
    spark.sql(query).createOrReplaceTempView('t_matches')
    leg = code(cell(LEG)).split(LEG, 1)[1].strip().rstrip(';').replace('openalex.awards.nwo_work_award_matches', 't_matches')
    return {(r.project_id, r.pidx): (r.work_id, r.tier, r.accepted) for r in rows}, \
           {r.work_id: list(r.award_ids) for r in spark.sql(leg).collect()}


def test_each_tier_and_guard_on_fixture_rows(spark):
    got, leg = run_matches(spark)
    assert got == {
        ('P1', 0): (1, 'doi', True),      # DOI salvaged from a link with a space in it
        ('P1', 1): (2, 'doi', False),     # the DOI's work has another title and none of the listed authors
        ('P1', 2): (3, 'title', True),    # same title and year, 'van der Linden' -> 'linden' on the work
        ('P1', 3): (4, 'title', False),   # a year apart and no number in common (a preprint of it)
        ('P1', 4): (5, 'title', True),    # a year apart, the first page agrees
        ('P1', 5): (6, 'title', False),   # the only same-title record is someone's review of the book
        ('P1', 6): (7, 'title', False),   # NWO lists an article, the record is a dataset
        ('P1', 7): (8, 'title', True),    # two same-title works, one shares the listed author
        ('P1', 9): (None, None, False),   # too short for the title tier: kept as an unresolved row
        ('P1', 10): (21, 'doi', True),    # %28/%29 decoded, '/full' and '?via' cut; of two works on the DOI, the one with the title
        ('P1', 11): (None, None, False),  # no listed year: no title match
        ('P1', 12): (None, None, False),  # 51 works share the title (cap 50): not used
    }                                     # no rows: the patent, P9 (not an NWO award), last week's object
    assert leg == {1: ['P1'], 3: ['P1'], 5: ['P1'], 8: ['P1'], 21: ['P1']}


def test_gates_pass_on_the_fixture_run_and_the_shrink_guard_stops_a_drop(spark):
    run_matches(spark)   # leaves t_matches and t_nwopen / t_raw in the session
    gates = [code(cell(g)).strip().rstrip(';') for g in ('NWO_RAW_COVERAGE', 'NWO_DUPLICATE_OUTPUTS', 'NWO_SHRINK_GUARD')]
    spark.createDataFrame([('P1', 'nwopen', 4320321800), ('P9', 'nwopen', 4320321800)],
                          'funder_award_id string, provenance string, funder_id bigint').createOrReplaceTempView('t_raw_full')
    def run(sql, leg_now, raw='t_raw_full'):
        spark.createDataFrame(leg_now, 'work_id bigint, funder_id bigint, award_ids array<string>').createOrReplaceTempView('t_leg_now')
        sql = sql.replace(NWOPEN, 't_nwopen').replace('_metadata.file_path', 'file_path')
        for table, name in (('openalex.awards.nwo_work_award_matches', 't_matches'), ('openalex.awards.openalex_awards_raw', raw),
                            ('openalex.awards.nwo_work_funders', 't_leg_now')):
            sql = sql.replace(table, name)
        return spark.sql(sql).collect()
    small = [(1, 4320321800, ['P1'])]
    for g in gates:
        run(g, small)  # all three pass: both projects in raw, one row per output, 5 accepted links >= 0.95 x 1
    with pytest.raises(Exception, match='NWO_RAW_COVERAGE'):
        run(gates[0], small, raw='t_raw')  # a raw missing P9 covers 1 of the object's 2 projects
    big = [(k, 4320321800, ['P1']) for k in range(100, 110)]  # the leg holds 10 links, the run accepts 5
    with pytest.raises(Exception, match='NWO_SHRINK_GUARD'):
        run(gates[2], big)
