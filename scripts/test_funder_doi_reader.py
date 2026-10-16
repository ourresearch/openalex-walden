#!/usr/bin/env python3
"""Offline tests for the FundRef DOI -> funder step in the Crossref and DataCite work-funder readers.

FundRef DOIs are a demoted backup join key (standup 2026-10-15): Crossref now deposits ROR, but FundRef DOIs remain in
records. Crossref resolves a new reference by unique ROR, then names; the DOI fills references with no name match and
confirms a same-funder name (a disagreeing name wins). DataCite resolves ROR, DOI, names and holds DOI/name conflicts for
identity review. Dev proof and warehouse probe live
in Rohan's doi-reader lane. Run: python3 scripts/test_funder_doi_reader.py (stdlib only; Python 3.9 compatible).
"""
import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AW = ROOT / 'notebooks' / 'awards'
VIEW = 'CREATE OR REPLACE TEMP VIEW funder_fundref_owners'


def cells(name):
    return [''.join(c['source']) for c in json.loads((AW / (name + '.ipynb')).read_text())['cells']]


def one(cs, needle):
    hits = [c for c in cs if needle in c]
    assert len(hits) == 1, (needle, len(hits))
    return hits[0]


def sql_regex(cell, marker):
    """The r'...' regex literal on the line carrying `marker`, as Python re text (same syntax subset as Java)."""
    line = next(l for l in cell.splitlines() if marker in l)
    return re.search(r"r'(\(\?i\)[^']*)'", line).group(1)


def extract(pattern, value):
    m = re.search(pattern, value.strip())
    return m.group(1) if m else ''


CR, DC, FR = cells('CreateCrossrefWorkFunders'), cells('CreateDataCiteWorkFunders'), cells('CreateFunderReportedWorkFunders')


class FundRefView(unittest.TestCase):
    def test_one_shared_view_after_activation_assert(self):
        for cs in (CR, DC):
            v = one(cs, VIEW)
            self.assertEqual(cs.index(v), cs.index(one(cs, 'assert_true(funder_names_active')) + 1)
            self.assertTrue(v.startswith('%sql\n'))
        self.assertEqual(one(CR, VIEW), one(DC, VIEW))

    def test_dbsql_safe(self):
        v = one(CR, VIEW)
        for bad in ('SET VAR', 'IDENTIFIER(', 'BEGIN', 'EXECUTE IMMEDIATE', 'openalex_dev.', ';'):
            self.assertNotIn(bad, v)

    def test_withdrawn_and_dropped_never_match(self):
        v = one(CR, VIEW)
        self.assertIn('FROM openalex.funders.deleted_funders', v)
        self.assertIn('LEFT ANTI JOIN deleted d ON d.fk=k.fk', v)                  # the id itself is tombstoned
        self.assertIn('LEFT ANTI JOIN deleted g ON g.funder_id=k.funder_id', v)    # the owning funder is withdrawn
        self.assertIn("WHERE r.status='active'", v)                                # withdrawn/inactive ROR records never crosswalk
        self.assertIn('CASE WHEN o.owners=1 AND s.id IS NOT NULL THEN o.funder_id END funder_id', v)
        self.assertIn('t.funder_id=o.funder_id AND t.merge_into_id IS NULL', v)

    def test_catalogue_key_wins_over_crosswalk(self):
        v = one(CR, VIEW)
        self.assertIn('LEFT ANTI JOIN catalogue_keys c ON c.fk=x.fk', v)
        self.assertIn('JOIN ror_owners o ON o.k=x.rid AND o.k<>\'\' AND o.owners=1', v)
        self.assertIn('coalesce(merge_into_id,funder_id) funder_id', v)            # merge losers project to the survivor


class Precedence(unittest.TestCase):
    CAND = {'cr': lambda: one(CR, 'CREATE OR REPLACE TEMP VIEW crossref_funder_name_candidates'),
            'dc': lambda: one(DC, 'CREATE OR REPLACE TABLE openalex.awards.datacite_work_funder_evidence')}

    def test_crossref_name_wins_conflicts(self):
        # Coordinator 10-15 (standup: DOI is a backup key): ROR, then names; the DOI fills references with no name match
        # and confirms a held name match to the same funder. On a DOI/name conflict the name answer wins.
        c = self.CAND['cr']()
        self.assertIn('coalesce(CASE WHEN r.owners=1 THEN r.funder_id END,CASE WHEN n.funder_id IS NULL OR n.funder_id=d.funder_id THEN d.funder_id END,n.funder_id) funder_id', c)
        self.assertIn("CASE WHEN r.owners=1 THEN 'ROR' WHEN d.funder_id IS NOT NULL AND (n.funder_id IS NULL OR n.funder_id=d.funder_id) THEN 'FundRef DOI' ELSE n.name_rule END resolution_path", c)
        self.assertIn('d.funder_id fundref_funder_id', c)

    def test_datacite_doi_path_then_identity_hold(self):
        c = self.CAND['dc']()
        self.assertIn('coalesce(CASE WHEN r.owners=1 THEN r.funder_id END,d.funder_id,n.funder_id) funder_id', c)
        self.assertIn("CASE WHEN r.owners=1 THEN 'ROR' WHEN d.funder_id IS NOT NULL THEN 'FundRef DOI' ELSE n.name_rule END resolution_path", c)

    def test_doi_join_shared(self):
        for k, f in self.CAND.items():
            c = f()
            self.assertIn("LEFT JOIN funder_fundref_owners d ON d.fk=e.fk AND e.fk<>''", c, k)
            self.assertNotIn('10.13039', c, k)

    def test_name_holds_skip_identifier_matches(self):
        for k, f in self.CAND.items():
            c = f()
            self.assertNotIn("resolution_path<>'ROR' AND h.reason", c, k)
            self.assertNotIn("resolution_path<>'ROR' AND (n.owners", c, k)
            self.assertIn("NOT IN ('ROR','FundRef DOI') AND h.reason IS NOT NULL THEN h.reason", c, k)
            # exact-name-only holds are untouched and never fire on a DOI path
            self.assertIn("WHEN {0}.resolution_path='exact' AND gr.owners>1 THEN 'global_name_ambiguity'".format('p' if k == 'cr' else 'e'), c, k)

    def test_datacite_identity_hold_and_ror_only_awards(self):
        c = self.CAND['dc']()
        self.assertIn("coalesce(e.resolution_path='FundRef DOI' AND e.name_funder_id<>e.funder_id,false) identity_disagreement", c)
        self.assertIn("e.fundref_owners doi_owners", c)
        self.assertIn("CASE WHEN m.resolution_path='ROR' AND m.usable", c)
        self.assertIn("AND NOT pair_disagreement AND resolution_path='ROR' AND award_number IS NOT NULL", c)

    def test_crossref_new_links_carry_no_awards(self):
        ev = one(CR, 'CREATE OR REPLACE TABLE openalex.awards.crossref_work_funder_evidence')
        self.assertIn("SELECT DISTINCT n.work_id,n.funder_id,CAST(array() AS ARRAY<STRING>) award_ids,'accepted' disposition", ev)
        self.assertIn('LEFT ANTI JOIN preserved old USING(work_id,funder_id)', ev)


class ExistingGates(unittest.TestCase):
    def test_reference_keys_unchanged(self):
        self.assertIn("sha2(to_json(named_struct('native_id',lower(lm.native_id),'name',f.name,'ror',f.ror,'awards',array_sort(f.awards))),256) reference_key",
                      one(CR, 'TEMP VIEW crossref_funder_references'))
        self.assertIn("sha2(to_json(named_struct('native_id',native_id,'name',funder_name,'ror',rk,'award_number',award_number,'award_uri',award_uri)),256) reference_key",
                      one(DC, 'TEMP VIEW datacite_funder_references'))

    def test_cut_preserve_and_activation_gates(self):
        for cs, src in ((CR, 'crossref'), (DC, 'datacite')):
            cut = one(cs, 'CREATE TABLE IF NOT EXISTS openalex.awards.%s_funder_reference_cut' % src)
            self.assertNotIn('CREATE OR REPLACE', cut)
            self.assertIn("SELECT assert_true(funder_names_active,'No-DOI matching requires the reviewed catalogue-bound name lookup; junction unchanged');",
                          one(cs, 'assert_true(funder_names_active'))
            gate = one(cs, '-- Resolve optional release')
            self.assertNotIn('SET VAR', gate)
        one(CR, "'Crossref: preserved pair or award array changed'")
        one(DC, "'DataCite: lost existing pair or award string'")
        one(DC, "'DataCite: missing, merged or unserved funder - preserve and review before replacing'")
        one(DC, "'DataCite: accepted conflicting/excluded reference'")

    def test_funder_reported_has_no_doi_input(self):
        self.assertFalse(any('fundref' in c.lower() or '10.13039' in c or 'f.doi' in c for c in FR))


class Parsing(unittest.TestCase):
    def test_crossref_doi_field(self):
        p = sql_regex(one(CR, 'TEMP VIEW crossref_funder_references'), "',1) fk,")
        for raw, want in (('10.13039/501100001809', '501100001809'), ('https://doi.org/10.13039/100000001', '100000001'),
                          ('http://dx.doi.org/10.13039/100000002 ', '100000002'), ('doi:10.13039/100000002', '100000002'),
                          ('10.13039/na', ''), ('10.13039/http://dx.doi.org/10.13039/100013747', ''), ('10.1234/567', ''),
                          ('501100001809', '501100001809')):
            self.assertEqual(extract(p, raw), want, raw)

    def test_datacite_identifier_requires_fundref_prefix(self):
        p = sql_regex(one(DC, 'TEMP VIEW datacite_funder_references'), 'AS fk,')
        for raw, want in (('https://doi.org/10.13039/501100000780', '501100000780'), ('10.13039/100000001/', '100000001'),
                          ('501100000780', ''), ('https://ror.org/00k4n6c32', ''), ('0000 0001 2097 5006', '')):
            self.assertEqual(extract(p, raw), want, raw)

    def test_crosswalk_values(self):
        p = sql_regex(one(CR, VIEW), "',1) fk")
        for raw, want in (('501100000780', '501100000780'), ('10.13039/501100000780', '501100000780'), ('', '')):
            self.assertEqual(extract(p, raw), want, raw)


if __name__ == '__main__':
    unittest.main(verbosity=1)
