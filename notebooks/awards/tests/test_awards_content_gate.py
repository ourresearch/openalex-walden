"""Offline checks of the Awards job's content gate (pipeline piece 7) and of what a skipped source reports.
The gate's behaviour on real and planted objects was run on dev (awards-fct-1123); these pin its shape in the generated notebook."""
import csv
import importlib.util
from pathlib import Path
import re

import pytest

ROOT = Path(__file__).resolve().parents[3]
AWARDS = ROOT / 'notebooks/awards'
SQL = (AWARDS / 'CreateOrUpdateAwards.sql').read_text()
CELLS = SQL.split('-- COMMAND ----------')
ROWS = list(csv.DictReader((AWARDS / 'sources.csv').open(newline='')))

_spec = importlib.util.spec_from_file_location('gen_awards_sql', ROOT / 'scripts/gen_awards_sql.py')
gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen)

VIEWS = [c for c in CELLS if 'CREATE OR REPLACE TEMP VIEW awards_candidate_' in c]
GUARDS = [c for c in CELLS if 'SET VAR awards_results = concat(' in c]
NAMES = ['lead_changed', 'institution_changed'] + [f'lost_{n}' for n, _ in gen.GATE_FIELDS]


def test_one_rule_for_every_source():
    """The plan's fixed floor, in one place: no per-source threshold, no new sources.csv column, no exception list."""
    assert (gen.GATE_FILL_PTS, gen.GATE_CHANGE_PCT) == (5, 2)
    assert gen.COLUMNS == ['source', 'priority', 'funder_ids', 's3_path', 'fetch_cadence', 'row_floor_pct']
    assert len({n for n, _ in gen.GATE_FIELDS}) == len(gen.GATE_FIELDS) >= 15
    for guard in GUARDS:
        assert guard.count('matched * 2,') == 2 and guard.count('rows_prev * 2,') == 1   # leads, institutions; funders
        assert guard.count('matched * 5,') == len(gen.GATE_FIELDS)                       # each field's fill


def test_gate_row_only_on_rows_raw_already_has():
    """Per matched row: lead changed, institution changed, then +1/-1 per field when raw has it and the file doesn't (or back)."""
    for view in VIEWS:
        row = view[view.index('AS _unchanged,'):view.index(' AS _gate')]
        assert row.count('CAST(') == 2 + 2 * len(gen.GATE_FIELDS)
        assert 'CASE WHEN b.funder_id IS NULL THEN NULL ELSE array(' in row   # a new row carries no gate entry
        assert "CAST(nullif(trim(b.description), '') IS NOT NULL AS INT) - CAST(nullif(trim(i.description), '') IS NOT NULL AS INT)" in row
        # a change is a value on both sides that differs (a blank filled in is not a change; case changes are)
        assert "AND NOT (rtrim(i.lead_investigator.affiliation.name) <=> rtrim(b.lead_investigator.affiliation.name))" in row
        assert "AND NOT (rtrim(i.lead_investigator.family_name) <=> rtrim(b.lead_investigator.family_name)))" in row
        assert 'i.lead_investigator.given_name) <=>' not in row   # titles and middle names in given names are not a new lead
        assert "'changed' END AS _change,\n    _gate\nFROM compared;" in view


def test_guard_sums_the_gate_in_its_one_pass():
    for guard in GUARDS:
        k = guard[guard.index('k AS ('):guard.index('c AS (')]
        assert ', '.join(f'sum(_gate[{j}]) {n}' for j, n in enumerate(NAMES)) in k
        assert ', '.join(f'sum({n}) {n}' for n in NAMES) in guard
        assert ', '.join(f'coalesce(c.{n}, 0) {n}' for n in NAMES) in guard
        assert 'coalesce(c.changed, 0) + coalesce(c.unchanged, 0) matched' in guard
        assert guard.count('FROM awards_candidate_') == 1


def test_funder_change_is_an_award_id_that_moved():
    for guard in GUARDS:
        assert 'retained AS (SELECT provenance, count(*) retained FROM gone GROUP BY provenance)' in guard
        assert 'JOIN k ON k._source = g.provenance AND k.award_key = g.award_key AND k.new > 0' in guard


def test_gate_only_for_prefix_rows_with_rows_in_raw():
    """A frozen row reads an object someone named; a prefix row's newest object is what nobody has looked at."""
    for guard in GUARDS:
        assert "s.fetch_cadence <> 'frozen' gated" in guard
        reasons = guard[guard.index('g AS ('):guard.index(') gate_reason')]
        ifs = re.findall(r'\n IF\((.*)', reasons)
        assert len(ifs) == 4 + len(gen.GATE_FIELDS) and all(i.startswith('gated AND rows_prev > 0 AND ') for i in ifs)
        assert ' / ' not in reasons and '/ ' not in reasons   # thresholds compare integers; the shares shown divide with try_divide
        assert reasons.count('try_divide(') == 3 + len(gen.GATE_FIELDS)
    prefix = [r for r in ROWS if r['fetch_cadence'] != 'frozen']
    assert prefix and all(r['s3_path'].endswith('/') for r in prefix)   # gen.load() refuses a non-frozen row naming one object


def test_a_file_whose_award_ids_changed_is_held():
    """The row floor counts rows: a file with every award id in a new format has as many rows, all new, and would add the
    whole source a second time beside the old rows (raw has no DELETE branch). The file must hold the floor's share of raw's keys."""
    for guard in GUARDS:
        assert "IF(gated AND rows_prev > 0 AND matched * 100 < rows_prev * row_floor_pct, format_string('only %d of %d rows in raw are in the file', matched, rows_prev), NULL)" in guard


def test_a_manual_row_reads_a_prefix(tmp_path):
    (tmp_path / 'sources').mkdir()
    r = dict(gen.load()[0], fetch_cadence='manual', s3_path='s3://openalex-ingest/awards/x/one.parquet')
    (tmp_path / 'sources.csv').write_text(','.join(gen.COLUMNS) + '\n' + ','.join(r[c] for c in gen.COLUMNS) + '\n')
    (tmp_path / 'sources' / f"{r['source']}.sql").write_text(r['mapping'] + '\n')
    with pytest.raises(SystemExit, match='a manual source reads a prefix'):
        gen.load(tmp_path)


def test_a_skipped_source_says_why():
    assert 'state: STRING, reason: STRING>> DEFAULT array();' in SQL
    for guard in GUARDS:
        assert "OR object IS NULL OR gate_reason <> '' THEN 'skipped' ELSE 'written' END,\n 'reason', nullif(reason, '')" in guard
        for text in ("format_string('%d rows < %d%% of %d'", "format_string('%d duplicate keys'",
                     "format_string('%d invalid rows'", "'no object'", "nullif(gate_reason, '')"):
            assert text in guard
    last = CELLS[-1]
    assert "transform(filter(session.awards_results, r -> r.state = 'skipped'), r -> r.source || ' (' || r.reason || ')')" in last


def test_merge_never_writes_the_gate():
    merge = next(c for c in CELLS if 'MERGE INTO ' in c)
    assert '_gate' not in merge.split('USING', 1)[1].split(') c\n', 1)[1]


def test_no_source_is_skipped_for_its_age():
    """Staleness never turns the run red: a prefix row whose newest object is already loaded reads it again and writes nothing."""
    for r in ROWS:
        mapping = (AWARDS / 'sources' / f"{r['source']}.sql").read_text()
        assert 'awards_objects' not in mapping, r['source']   # a mapping never reads its object's name or date
    assert 'AS fresh' not in SQL and 'fct_fresh' not in SQL
