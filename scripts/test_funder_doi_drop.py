#!/usr/bin/env python3
"""Offline checks for the no-ROR/no-award funder drop (dd1016-b3); funder DOIs stay as a side identifier. No warehouse, no network.

Run from any directory: python3 scripts/test_funder_doi_drop.py   (also collected by pytest)
"""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RELEASE = ROOT / 'notebooks/funders/release/dd1016'
LINK_TABLES = [
    'openalex.mid.work_funder', 'openalex.works.fulltext_work_funders', 'openalex.awards.crossref_work_funders',
    'openalex.awards.crossref_no_doi_main_cut', 'openalex.awards.datacite_work_funders', 'openalex.awards.europepmc_work_funders',
    'openalex.awards.funder_reported_work_funders', 'openalex.awards.hakai_work_funders', 'openalex.awards.anr_work_funders',
    'openalex.awards.kaken_work_funders', 'openalex.awards.nwo_work_funders', 'openalex.funders.funder_match_policy',
    'openalex.common.funder_names_keep',
]


def cells(rel):
    return [''.join(c['source']) for c in json.loads((ROOT / rel).read_text())['cells']]


def ids_struct(sql):
    return sql[sql.index('STRUCT(\n    CONCAT(\'https://openalex.org/F\''):sql.index(') AS ids')]


def test_funders_api_keeps_doi_as_side_id():
    # Round 3 (standup 10-15): funder DOIs stay public, demoted to a historical side identifier / backup crosswalk key.
    api = next(s for s in cells('notebooks/funders/CreateFundersAPI.ipynb') if 'CREATE OR REPLACE TABLE openalex.funders.funders_api AS' in s)
    dev = next(s for s in cells('notebooks/funders/CreateOrUpdateFunders.ipynb') if '-- funders_api_1' in s)
    for sql in (api, dev):
        ids = ids_struct(sql)
        assert 'f.crossref_id  AS crossref' in ids and 'f.doi          AS doi' in ids
        assert 'CAST(NULL' not in ids
        assert 'historical artifact' in ids and 'backup crosswalk key' in ids and 'NOT the primary key' in ids
        assert 'f.ror_id       AS ror' in ids and 'f.wikidata_id  AS wikidata' in ids


def test_awards_funder_doi_unchanged():
    # build_awards.py is not part of the release: award funder.doi is built from the funder row exactly as today.
    src = (ROOT / 'notebooks/awards/nightly_grants/lib/build_awards.py').read_text()
    block = src[src.index("c.artifact('awards_candidate'"):src.index("nih_fold.project(c)")]
    assert "'ror_id',f.ror_id,'doi',f.doi) END funder" in block
    assert "CAST(NULL AS STRING)" not in block


def test_works_funders_drop_withdrawn():
    cell = next(s for s in cells('notebooks/end2end/CreateWorksEnriched.ipynb') if '1) Build rolled funders' in s)
    assert 'FROM openalex.funders.deleted_funders' in cell
    assert 'LEFT ANTI JOIN withdrawn_funders x ON x.funder_id = work_projected.funder_id' in cell
    assert 'USING (SELECT work_id, funders FROM rolled_up UNION ALL SELECT work_id, funders FROM withdrawn_only) AS source' in cell
    assert 'LEFT ANTI JOIN rolled_up r ON r.work_id = w.id' in cell
    # Spark: a JOIN may not follow LATERAL VIEW in one FROM clause (the first draft did; dev parse caught it)
    assert not re.search(r'LATERAL VIEW[^\n]*\n\s*(LEFT |RIGHT |INNER )?(SEMI |ANTI )?JOIN', cell)


# The ES stale-delete cap exemption is tested with the merge-redirect cell in notebooks/elastic/tests/test_sync_funders_merge_redirects.py
# (test_dd1016_*), which executes the whole notebook against an in-memory Spark/ES fake.


def _statements(path):
    text = path.read_text()
    out, cur = [], []
    for line in text.splitlines():
        if line.strip().startswith('--') and not cur:
            continue
        assert ';' not in line.rstrip()[:-1], line             # ';' only as a statement terminator at line end
        cur.append(line)
        if line.rstrip().endswith(';'):
            out.append('\n'.join(cur)); cur = []
    assert not ''.join(cur).strip()
    return out


def test_release_sql_shape():
    fwd, inv = _statements(RELEASE / 'FORWARD.sql'), _statements(RELEASE / 'ROLLBACK.sql')
    f, r = '\n'.join(fwd), '\n'.join(inv)
    assert "'ca97a58804a3f90ceabad9165415e1759ce5073e655b2511eb1d44815e74ce14'" in f and '=2191' in f and "='merged_into_dropped')=0 " in f
    assert "'01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b'" in f and ')=2191,' in f
    for t in LINK_TABLES:
        assert re.search(rf'MERGE INTO {re.escape(t)} t USING openalex\.funders\.dd1016_drop m ON t\.\w+ = .* WHEN MATCHED THEN DELETE;', f), t
        assert f'INSERT INTO {t} SELECT * FROM (SELECT * FROM openalex.funders.dd1016_bk_' in r, t
    assert 'DELETE FROM' not in f                                # links leave only through MERGE ... DELETE (6 h DELETE skip, binder 0eed17e6)
    republish = (ROOT / 'notebooks/funders/release/integrated/059_republish.sql').read_text()
    republish = republish[republish.index('CREATE OR REPLACE TABLE'):].strip()
    assert f.count(republish) == 1 and r.count(republish) == 1  # the switch's compat publication, byte for byte
    assert "UPDATE openalex.funders.funder SET status = 'withdrawn', ror_id = NULL, ror_primary = NULL" in f
    assert 'CREATE OR REPLACE TABLE openalex.funders.funder_name_lookup AS SELECT * FROM openalex.funders.dd1016_lookup;' in f
    assert 'CREATE OR REPLACE TABLE openalex.funders.funder_name_lookup AS SELECT * FROM openalex.funders.dd1016_rb_lookup;' in r
    # P2-1: ROLLBACK is row-scoped. No whole-table equality with a before image, and the lookup is re-signed for the current catalogue.
    assert 'dd1016_lookup_before;' not in r and 'EXCEPT ALL SELECT * FROM openalex.funders.dd1016_catalogue_before)' not in r
    assert "WHEN MATCHED AND t.status = 'withdrawn' AND t.run_id = 'dd1016' THEN UPDATE SET t.status = b.status" in r
    assert 'UPDATE SET *' not in r and r.count("readers would fail closed") == 1
    assert "BETWEEN '0800' AND '1630'" in f and "<= '1730'" in f
    assert "DELETE FROM openalex.funders.deleted_funders WHERE batch = 'dd1016-b3';" in r
    assert 'openalex_dev' not in f and 'openalex_dev' not in r



def test_drop_rule_excludes_every_work_link_source():
    """Round 4 (Kyle 10-08): a funder with a work link in ANY source is never dropped; F01 and PRECHECK apply the same rule."""
    f = (RELEASE / 'FORWARD.sql').read_text()
    pre = (RELEASE / 'PRECHECK.sql').read_text()
    f01 = f[f.index('CREATE TABLE openalex.funders.dd1016_drop_live AS '):]
    f01 = f01[:f01.index(';')]
    rule = f01[len('CREATE TABLE openalex.funders.dd1016_drop_live AS '):]
    assert rule.replace('\n', '\n ') in pre                          # PRECHECK embeds the identical rule
    sources = [t for t in LINK_TABLES if t not in ('openalex.funders.funder_match_policy', 'openalex.common.funder_names_keep')] + [
        'openalex.awards.datacite_work_funder_evidence', 'openalex.awards.gtr_awards', 'openalex.awards.work_awards',
        'openalex.works.openalex_works LATERAL VIEW explode(funders)', 'openalex.mid.entity_link']
    for t in sources:
        assert f' FROM {t}' in rule, t
    assert 'NOT EXISTS (SELECT 1 FROM openalex.awards.openalex_awards a WHERE a.funder_id = f.funder_id)' in rule
    assert 'JOIN linked l ON l.funder_id = c.funder_id WHERE coalesce(c.merge_into_id, c.funder_id) = f.funder_id' in rule  # aliases count
    assert "regexp_extract(id_1,'^F([0-9]+)$',1)" in rule and "regexp_extract(id_2,'^F([0-9]+)$',1)" in rule   # entity_link, both sides
    # round 5 (Codex r4 P2-3): a merge target is never dropped (catalogue or registry edge), so no redirect points at a withdrawn id
    assert 'AND NOT EXISTS (SELECT 1 FROM openalex.funders.funders t WHERE t.merge_into_id = f.funder_id)' in rule
    assert 'AND NOT EXISTS (SELECT 1 FROM openalex.funders.funder r WHERE r.merged_into_id = f.funder_id)' in rule

if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_'):
            fn()
            print('ok', name)
