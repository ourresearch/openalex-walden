"""The two readers that gate on the latest Nightly Grants run (DataCite Work Funders, Produce NIH PubMed Relinks) count only
data commits when they read a scratch table's Delta history (oxjob #1501).

Table maintenance (OPTIMIZE, VACUUM, statistics, table properties, REORG) rewrites files or metadata, not rows, and runs on
its own schedule. Prod history shows COMPUTE STATS and SET TBLPROPERTIES commits on the scratch tables, which neither guard's
old list named, and DataCite pinned max(version) of every commit, so an OPTIMIZE landing during its build failed it. Both
files now carry one list, byte-identical, and DataCite pins and re-checks the latest data version.

The behaviour tests run the notebooks' own guard SQL in DuckDB via sqlglot on a fake history and skip without them.
"""
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
DATACITE = ''.join(''.join(c['source']) for c in json.loads((ROOT / 'notebooks/awards/CreateDataCiteWorkFunders.ipynb').read_text())['cells'])
NIH = (ROOT / 'notebooks/awards/ProduceNihPubmedRelinks.sql').read_text()
MAINTENANCE = ("('OPTIMIZE','VACUUM START','VACUUM END','ANALYZE','COMPUTE STATISTICS','COMPUTE STATS','SET TBLPROPERTIES',"
               "'UNSET TBLPROPERTIES','REORG')")
DATA_OPS = ('WRITE', 'MERGE', 'UPDATE', 'DELETE', 'TRUNCATE', 'RESTORE', 'STREAMING UPDATE', 'CREATE TABLE AS SELECT',
            'REPLACE TABLE AS SELECT', 'CREATE OR REPLACE TABLE AS SELECT', 'CREATE TABLE', 'CREATE OR REPLACE TABLE', 'COPY INTO')


def lists(text):
    return re.findall(r"operation NOT IN (\([^)]*\))", text)


def test_one_maintenance_list_in_both_guards():
    assert lists(DATACITE) == [MAINTENANCE] * 4    # pin, first freshness check, re-check of the pin, second freshness check
    assert lists(NIH) == [MAINTENANCE] * 7         # one per scratch table
    named = set(re.findall(r"'([^']+)'", MAINTENANCE))
    assert {'COMPUTE STATS', 'SET TBLPROPERTIES'} <= named   # seen on the prod scratch tables, 20 Nov
    assert not named & set(DATA_OPS)


def test_every_history_read_is_filtered():
    for text in (DATACITE, NIH):
        for m in re.finditer(r"\(DESCRIBE HISTORY openalex\.awards\.\w+\)", text):
            assert text[m.end():m.end() + 80].lstrip().startswith('WHERE operation NOT IN'), text[m.start():m.end() + 80]


@pytest.fixture
def con():
    duckdb = pytest.importorskip('duckdb')
    pytest.importorskip('sqlglot')
    c = duckdb.connect()
    c.execute("CREATE TABLE runs (run_id VARCHAR, databricks_run_id VARCHAR, status VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP)")
    c.execute("INSERT INTO runs VALUES ('night', 'R1', 'SUCCEEDED', now() - INTERVAL 6 HOUR, now() - INTERVAL 4 HOUR), "
              "('older', 'R0', 'SUCCEEDED', now() - INTERVAL 30 HOUR, now() - INTERVAL 28 HOUR)")
    c.execute("CREATE TABLE hist (version BIGINT, timestamp TIMESTAMP, operation VARCHAR, job STRUCT(jobRunId VARCHAR))")
    c.execute("INSERT INTO hist VALUES (1, now() - INTERVAL 29 HOUR, 'CREATE OR REPLACE TABLE AS SELECT', {'jobRunId': 'R0'}), "
              "(2, now() - INTERVAL 5 HOUR, 'CREATE OR REPLACE TABLE AS SELECT', {'jobRunId': 'R1'})")
    return c


def add(con, *ops):
    v = con.execute('SELECT max(version) FROM hist').fetchone()[0]
    for i, (op, owner) in enumerate(ops, 1):
        con.execute("INSERT INTO hist VALUES (?, now() - INTERVAL 3 HOUR + ? * INTERVAL 1 MINUTE, ?, {'jobRunId': ?})", [v + i, i, op, owner])


def duck(sql):
    import sqlglot
    sql = re.sub(r"\(DESCRIBE HISTORY openalex\.awards\.\w+\)", 'hist', sql).replace('openalex.awards.award_nightly_runs', 'runs')
    return sqlglot.parse_one(sql, read='spark').sql(dialect='duckdb')


def datacite_fresh(con):
    """The notebook's freshness condition (the first assert_true of the pin cell), with the run variable bound to 'night'."""
    i = DATACITE.index("SELECT assert_true(COALESCE((SELECT r.status='SUCCEEDED'")
    expr = DATACITE[i + len('SELECT assert_true('):DATACITE.index(",\n  'DataCite: the latest Nightly Grants run", i)]
    return con.execute('SELECT ' + duck(expr.replace('datacite_award_key_run', "'night'"))).fetchone()[0]


def datacite_pin(con):
    i = DATACITE.index('SET VAR datacite_award_key_map_version = ')
    q = DATACITE[i + len('SET VAR datacite_award_key_map_version = '):DATACITE.index(';', i)]
    return con.execute('SELECT ' + duck(q)).fetchone()[0]


def nih_good(con):
    """The notebook's per-table check: the table's last data commit lies inside the latest SUCCEEDED run (1 = good)."""
    i = NIH.index('WITH run AS'); j = NIH.index("UNION ALL SELECT 'ngr_normalization_lookup'", i)
    with_clause = NIH[i:j] + ')'
    good = re.search(r"count\(DISTINCT CASE WHEN coalesce\(h\.ts BETWEEN run\.started_at AND run\.finished_at, false\) THEN h\.t END\)", NIH).group(0)
    return con.execute(duck(f"{with_clause} SELECT {good} FROM run CROSS JOIN h")).fetchone()[0]


def test_maintenance_commits_after_the_night_do_not_trip_the_guards(con):
    assert datacite_fresh(con) is True and nih_good(con) == 1
    pinned = datacite_pin(con)
    add(con, ('OPTIMIZE', 'M1'), ('COMPUTE STATS', 'M2'), ('SET TBLPROPERTIES', 'M3'), ('VACUUM START', 'M4'), ('VACUUM END', 'M4'), ('REORG', 'M5'))
    assert datacite_fresh(con) is True and nih_good(con) == 1
    assert datacite_pin(con) == pinned == 2   # a maintenance commit during DataCite's build leaves the pinned data version as it was


def test_a_data_commit_after_the_night_trips_both_guards(con):
    pinned = datacite_pin(con)
    add(con, ('OPTIMIZE', 'M1'), ('WRITE', 'X1'))   # e.g. a check run or a failed run rewrote the table
    assert datacite_fresh(con) is False and nih_good(con) == 0
    assert datacite_pin(con) != pinned            # DataCite's re-check of the pin fails too


def test_an_unknown_operation_counts_as_data(con):
    add(con, ('SOME NEW DELTA OPERATION', 'Y1'))   # fail closed: only the listed maintenance is ignored
    assert datacite_fresh(con) is False and nih_good(con) == 0
