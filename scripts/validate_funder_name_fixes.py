#!/usr/bin/env python3
"""Check the committed SQL/CSV contract without warehouse access.

Run from any directory: python scripts/validate_funder_name_fixes.py
Warehouse execution evidence lives in Rohan's funder-fixes-20261013 lane.
"""
import csv
import json
import re
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FUNDER_DIR = ROOT / 'notebooks' / 'funders'


def sources(path):
    return [''.join(c['source']) for c in json.loads(path.read_text())['cells']]


SET_VAR_CELL = ["-- Top-level SET VAR (rejected inside BEGIN...END on the DBSQL warehouse). Same values as before.",
    "SET VAR funder_names_active = (SELECT EXISTS(SELECT 1 FROM funder_name_lookup_local));",
    "SET VAR funder_name_lookup = 'funder_name_lookup_local';"]


def validate():
    funders = sources(FUNDER_DIR / 'CreateOrUpdateFunders.ipynb')
    # The reviewed alias and twin-merge mirrors live in PublishFunderNameRelease (the lookup publisher); the daily has no
    # dev lookup copy any more (moved out of the repo with the rest of the dev harness).
    release = sources(FUNDER_DIR / 'PublishFunderNameRelease.ipynb')
    alias_cell = next(s for s in release if '-- name_aliases:' in s)
    aliases = list(csv.DictReader((FUNDER_DIR / 'name_aliases.csv').open(newline='')))
    csv_aliases = [(r['name_key'], int(r['funder_id']), r['rule']) for r in aliases]
    sql_aliases = [(bytes.fromhex(k).decode('utf-8'), int(fid), rule)
                   for k, fid, rule in re.findall(r"\('([0-9a-f]+)',(\d+)L,'(reviewed|programme)'\)", alias_cell)]
    assert csv_aliases == sql_aliases, 'SQL alias bytes/order differ from name_aliases.csv'
    assert Counter(r[2] for r in csv_aliases) == {'reviewed': 183, 'programme': 21}
    assert len({(r[0], r[2]) for r in csv_aliases}) == 204, 'Duplicate alias keys'
    assert all(k == k.strip().lower() and k for k, _, _ in csv_aliases)

    edits = list(csv.DictReader((FUNDER_DIR / 'edits.csv').open(newline='')))
    csv_merges = [(int(r['funder_id']), int(r['target'])) for r in edits if r['action'] == 'merge']
    twin_cell = next(s for s in release if '-- name_twin_merges:' in s)
    sql_merges = [(int(a), int(b)) for a, b in re.findall(r'\((\d+)L,(\d+)L\)', twin_cell)]
    assert csv_merges == sql_merges and len(csv_merges) == 0
    deferred=list(csv.DictReader((FUNDER_DIR / 'deferred_merges.csv').open(newline='')))
    assert len(deferred)==59 and all(r['action']=='merge' for r in deferred)
    assert 'WHERE false' in twin_cell
    assert all(c.startswith('%sql') for c in funders), 'Registry notebook must be SQL only'
    assert all(c.startswith('%sql') for c in sources(FUNDER_DIR / 'SeedFunders.ipynb')), 'Seed notebook must be SQL only'
    assert not any(':mode' in s for s in funders), 'Daily notebook has no mode: the seed is SeedFunders.ipynb'
    edits_cell=next(s for s in funders if '-- registry_edits:' in s)
    assert 'WHERE false' in edits_cell, 'Deferred edits must stay inert'
    assert [(int(a),int(b)) for a,b in re.findall(r"\(\d+L,'merge',(\d+)L,'(\d+)',CAST\(NULL AS STRING\)\)",edits_cell)] == csv_merges
    losers = {a for a, _ in csv_merges}
    assert len(losers) == 0 and not losers.intersection(b for _, b in csv_merges), 'Merge chain/cycle'
    assert all(r['reviewed_by'] and r['reason'] for r in edits)

    merge = next(s for s in funders if '-- Apply reviewed edits' in s)
    merge = merge[merge.index('-- edit_merge:'):merge.index('-- edit_merge_ids')]
    fields = set(re.findall(r'f\.(\w+)\s*=', merge.split('UPDATE SET', 1)[1]))
    assert fields == {'status', 'merged_into_id', 'updated_date', 'run_id'}, fields
    assert not any("trim(:prefix) <> 'openalex.funders.'" in s for s in funders), 'No dev-only harness in the daily notebook'
    for s in (alias_cell, twin_cell):
        assert s.startswith("%sql\nBEGIN\nIF :phase = 'stage' THEN\n") and "CREATE TABLE IDENTIFIER(:release_prefix ||" in s
        assert not re.search(r'FROM\s+\S*nm1012_jev_', s, re.I)

    cr_cells = sources(ROOT / 'notebooks/awards/CreateCrossrefWorkFunders.ipynb')
    cr = next(s for s in cr_cells if 'CREATE OR REPLACE TABLE openalex.awards.crossref_work_funders' in s)
    candidates = next(s for s in cr_cells if 'CREATE OR REPLACE TEMP VIEW crossref_funder_name_candidates' in s)
    assert 'FROM openalex.awards.crossref_work_funder_evidence' in cr
    assert 'coalesce(e.funder_id,h.funder_id,hs.funder_id,a.funder_id,t.funder_id,p.funder_id)' in candidates
    assert 'served.id=target.funder_id' in candidates
    dc_cells = sources(ROOT / 'notebooks/awards/CreateDataCiteWorkFunders.ipynb')
    dc = next(s for s in dc_cells if 'CREATE OR REPLACE TABLE openalex.awards.datacite_work_funder_evidence' in s)
    assert "p.matcher='datacite_name_v1' AND p.eligible" in dc
    assert 'SELECT old.work_id,old.funder_id,' in dc
    assert "WHEN m.hold_reason IS NOT NULL THEN m.hold_reason ELSE 'accepted'" in dc
    assert "CASE WHEN m.resolution_path='ROR' AND m.usable" in dc
    assert 'coalesce(e.funder_id,h.funder_id,hs.funder_id,a.funder_id,t.funder_id,p.funder_id)' in dc
    holds = list(csv.DictReader((FUNDER_DIR / 'name_match_holds.csv').open(newline='')))
    assert len(holds) == 105 and len({r['name_key'] for r in holds}) == 105
    expected_holds = [(r['name_key'], r['reason']) for r in holds]
    for reader in (candidates, dc):
        actual_holds = [(bytes.fromhex(k).decode('utf-8'), reason) for k, reason in re.findall(r"\('([0-9a-f]+)','(reviewed_generic_ambiguity|country_ambiguous_alias|predecessor_programme_scope)'\)", reader)]
        assert actual_holds == expected_holds
        assert "'target_ror_alias_scope'" in reader
        assert "'global_name_ambiguity'" in reader and "'undocumented_short_name'" in reader
        assert 'LEFT JOIN funder_name_lookup_local e ON' in reader and 'IDENTIFIER(funder_name_lookup' not in reader
        assert 'openalex_dev.' not in reader
        assert "'duplicate_name_identity'" in reader
        assert 'conflict_display_owners' in reader
        assert not re.search(r'FROM\s+\S*nm1012_jev_', reader, re.I)
        # FundRef DOI is a demoted backup join key (standup 10-15): read only through the shared funder_fundref_owners view.
        # Crossref: ROR, then names; the DOI fills no-name refs and confirms same-funder names. DataCite: ROR, DOI, names, with
        # DOI/name conflicts held. Name holds never apply to ROR or DOI matches. No legacy DOI CTEs/paths.
        assert 'conflict_doi' not in reader and 'doi_owners AS (' not in reader
        assert '10.13039' not in reader and 'Crossref DOI' not in reader
        assert 'doi_name_conflict' not in reader
        assert "LEFT JOIN funder_fundref_owners d ON d.fk=e.fk AND e.fk<>''" in reader
        if reader is candidates:  # Crossref: name answer wins a DOI/name conflict (coordinator 10-15); DOI fills no-name refs
            assert 'coalesce(CASE WHEN r.owners=1 THEN r.funder_id END,CASE WHEN n.funder_id IS NULL OR n.funder_id=d.funder_id THEN d.funder_id END,n.funder_id) funder_id' in reader
            assert "CASE WHEN r.owners=1 THEN 'ROR' WHEN d.funder_id IS NOT NULL AND (n.funder_id IS NULL OR n.funder_id=d.funder_id) THEN 'FundRef DOI' ELSE n.name_rule END resolution_path" in reader
        else:  # DataCite: DOI path, and a disagreeing name holds the pair for identity review
            assert 'coalesce(CASE WHEN r.owners=1 THEN r.funder_id END,d.funder_id,n.funder_id) funder_id' in reader
            assert "CASE WHEN r.owners=1 THEN 'ROR' WHEN d.funder_id IS NOT NULL THEN 'FundRef DOI' ELSE n.name_rule END resolution_path" in reader
        assert "resolution_path<>'ROR' AND h.reason" not in reader and "resolution_path<>'ROR' AND (n.owners" not in reader
        assert "resolution_path NOT IN ('ROR','FundRef DOI') AND h.reason IS NOT NULL THEN h.reason" in reader
        assert "resolution_path NOT IN ('ROR','FundRef DOI') AND (n.owners>1 OR t.owners>1) THEN 'duplicate_name_identity'" in reader
        assert 'funder_reference_cut' in reader
    for cells in (cr_cells, dc_cells):
        assert any("funder_name_lookup STRING DEFAULT 'openalex.funders.funder_name_lookup'" in c for c in cells)
        assert any('assert_true(funder_names_active' in c for c in cells)
        cut = next(c for c in cells if 'CREATE TABLE IF NOT EXISTS' in c)
        assert 'funder_reference_cut' in cut and 'CREATE OR REPLACE' not in cut
        gate = next(c for c in cells if '-- Resolve optional release' in c)
        assert 'system.information_schema.tables' in gate and 'EXECUTE IMMEDIATE' in gate
        assert 'WHERE false' in gate and 'catalogue_signature' in gate
        # DBSQL warehouses reject SET VAR inside BEGIN...END: set both variables top-level, right after the gate.
        assert 'SET VAR' not in gate and gate.rstrip().endswith('END;')
        assert cells[cells.index(gate)+1].splitlines()[1:] == SET_VAR_CELL
        fundref = [c for c in cells if 'CREATE OR REPLACE TEMP VIEW funder_fundref_owners' in c]
        activation = next(c for c in cells if 'assert_true(funder_names_active' in c)
        assert len(fundref) == 1 and cells.index(fundref[0]) == cells.index(activation) + 1, 'FundRef view must follow the activation assert'
        assert fundref[0].startswith('%sql\n') and 'SET VAR' not in fundref[0] and 'IDENTIFIER(' not in fundref[0] and 'BEGIN' not in fundref[0]
        for k in ('FROM openalex.funders.deleted_funders', 'LEFT ANTI JOIN deleted d ON d.fk=k.fk', 'LEFT ANTI JOIN deleted g ON g.funder_id=k.funder_id',
                  "WHERE r.status='active'", 'o.owners=1', 'LEFT ANTI JOIN catalogue_keys c ON c.fk=x.fk',
                  'CASE WHEN o.owners=1 AND s.id IS NOT NULL THEN o.funder_id END funder_id',
                  't.funder_id=o.funder_id AND t.merge_into_id IS NULL', 'openalex.funders.funders_api s ON s.id=t.funder_id'):
            assert k in fundref[0], k
        assert 'openalex_dev.' not in fundref[0]
        deferred=list(csv.DictReader((FUNDER_DIR / 'deferred_merges.csv').open(newline='')))
    assert len(deferred)==59 and all(r['action']=='merge' for r in deferred)
    assert 'WHERE false' in twin_cell
    assert all(c.startswith('%sql') for c in cells if c.lstrip().startswith(('%python','%sql')))
    # Production lookup publication is PublishFunderNameRelease (stage/activate) plus the daily's own re-sign; the daily's
    # dev-only lookup cells never write the production lookup.
    assert [i for i, s in enumerate(funders) if 'openalex.funders.funder_name_lookup' in s] == \
        [i for i, s in enumerate(funders) if s.startswith(('%sql\n-- name_lookup_resign_pin:', '%sql\n-- resign_name_lookup:'))], \
        'Only the pin and the re-sign touch the production lookup'
    assert [c for c in cr_cells if 'TEMP VIEW funder_fundref_owners' in c] == [c for c in dc_cells if 'TEMP VIEW funder_fundref_owners' in c], 'FundRef view differs between readers'
    cr_refs = next(s for s in cr_cells if 'CREATE OR REPLACE TEMP VIEW crossref_funder_references' in s)
    assert "sha2(to_json(named_struct('native_id',lower(lm.native_id),'name',f.name,'ror',f.ror,'awards',array_sort(f.awards))),256) reference_key" in cr_refs, 'Crossref reference_key must not change (immutable cut)'
    assert "13039/)?([0-9]+)$',1) fk," in cr_refs
    dc_refs = next(s for s in dc_cells if 'CREATE OR REPLACE TEMP VIEW datacite_funder_references' in s)
    assert "sha2(to_json(named_struct('native_id',native_id,'name',funder_name,'ror',rk,'award_number',award_number,'award_uri',award_uri)),256) reference_key" in dc_refs, 'DataCite reference_key must not change (immutable cut)'
    assert "13039/([0-9]+)/?$',1) AS fk," in dc_refs
    assert "coalesce(e.resolution_path='FundRef DOI' AND e.name_funder_id<>e.funder_id,false) identity_disagreement" in dc
    assert "e.resolution_path NOT IN ('ROR','FundRef DOI') AND p.funder_id IS NULL THEN 'name_policy'" in dc
    cr_evidence = next(s for s in cr_cells if 'CREATE OR REPLACE TABLE openalex.awards.crossref_work_funder_evidence' in s)
    assert 'crossref_no_doi_main_cut' in cr_evidence
    assert 'LEFT ANTI JOIN preserved old USING(work_id,funder_id)' in cr_evidence
    dc_junction = next(s for s in dc_cells if 'CREATE OR REPLACE TABLE openalex.awards.datacite_work_funders\n' in s)
    assert 'LEFT ANTI JOIN preserved p USING(work_id,funder_id)' in dc_junction
    assert 'SELECT * FROM preserved UNION ALL SELECT * FROM additions' in dc_junction
    release = sources(FUNDER_DIR / 'PublishFunderNameRelease.ipynb')
    release_aliases=next(s for s in release if '-- name_aliases:' in s)
    assert [(bytes.fromhex(k).decode('utf-8'),int(fid),rule) for k,fid,rule in re.findall(r"\('([0-9a-f]+)',(\d+)L,'(reviewed|programme)'\)",release_aliases)] == csv_aliases
    release_merges=next(s for s in release if '-- name_twin_merges:' in s)
    assert 'CAST(NULL AS BIGINT) loser_id' in release_merges and 'WHERE false' in release_merges
    assert [(int(a),int(b)) for a,b in re.findall(r'\((\d+)L,(\d+)L\)',release_merges)] == csv_merges
    activate = next(s for s in release if "IF :phase = 'activate'" in s)
    assert "MERGE INTO" not in activate
    assert "count(*)=0" in activate
    assert 'MERGE INTO IDENTIFIER(:catalogue)' not in activate
    assert (4320337364,4320337611) not in csv_merges
    assert "f.ror_id =" not in activate
    assert 'EXCEPT ALL' in activate
    assert "FROM IDENTIFIER(:release_prefix || 'registry_after')" in activate
    assert "FROM IDENTIFIER(:release_prefix || 'identifiers_after')" in activate
    assert not any('fr1006_' in s for s in release)
    import yaml
    job = yaml.safe_load((ROOT/'jobs/funder_name_release.yaml').read_text())['resources']['jobs']['Funder_Name_Release']
    assert 'schedule' not in job and 'trigger' not in job
    assert all(p['default'].startswith('openalex_dev.rohan_lab.ff1013_') for p in job['parameters'] if p['name']!='phase')
    reported = sources(ROOT / 'notebooks/awards/CreateFunderReportedWorkFunders.ipynb')
    reported_source = next(s for s in reported if 'WITH reviewed_merges AS' in s)
    reported_gate = next(c for c in reported if '-- Resolve optional release' in c)
    assert 'SET VAR' not in reported_gate and reported.index(reported_gate)+2 == reported.index(reported_source)
    assert reported[reported.index(reported_gate)+1].splitlines()[1:] == SET_VAR_CELL
    assert 'CAST(NULL AS BIGINT) loser_id' in reported_source and 'WHERE false' in reported_source
    assert [(int(a),int(b)) for a,b in re.findall(r'\((\d+)L,(\d+)L\)',reported_source)] == csv_merges
    assert 'CASE WHEN funder_names_active THEN coalesce(m.survivor_id,s.funder_id) ELSE s.funder_id END' in reported_source
    assert 'SELECT DISTINCT s.work_id' in reported_source
    assert not any('fundref' in c.lower() or '10.13039' in c for c in reported), 'FunderReported has no DOI input: no FundRef step'
    package=FUNDER_DIR/'release/integrated'
    manifest=json.loads((package/'steps.json').read_text())
    expected={f"{i:03d}_{step['phase'].lower()}.sql" for i,step in enumerate(manifest)}
    assert {p.name for p in package.glob('[0-9]*.sql')} == expected
    for i,step in enumerate(manifest):
        text=(package/f"{i:03d}_{step['phase'].lower()}.sql").read_text()
        assert text.endswith(step['sql'].strip().rstrip(';')+';\n')
    assert 'MERGE INTO openalex.funders.funders ' not in (package/'ROLLBACK.sql').read_text()
    assert 'MERGE INTO' not in (package/'ROLLBACK.sql').read_text(), 'Zero-merge inverse needs no registry DML'
    assert 'DROP VIEW openalex.funders.funders;' in (package/'ROLLBACK.sql').read_text()
    assert 'ALTER TABLE openalex.funders.funders_v41_table RENAME TO openalex.funders.funders;' in (package/'ROLLBACK.sql').read_text()
    assert 'early rollback forbidden' in (package/'ROLLBACK.sql').read_text()
    print('PASS: 204 aliases, 0 active merges (59 deferred), preserved fields, forward readers ROR -> names with FundRef DOI backup key (fills no-name refs; Crossref name wins conflicts, DataCite holds them; deleted funders never match), immutable reference cuts, literal history preservation, required catalogue-bound lookup, production-only publication, unscheduled dev-default release job')


if __name__ == '__main__':
    validate()
