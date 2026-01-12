# Issue: Crossref Author/Affiliation Interleaving

**Status**: in-progress
**Discovered**: 2026-01-12
**Severity**: high
**Component**: pipeline

## Summary

Some Crossref publishers submit metadata where author affiliations are incorrectly parsed as separate author entries. This results in institution names (e.g., "Kazan University") appearing as author given/family names instead of affiliations. Expanded detection finds ~1.9M affected records.

## Impact

| Metric | Value |
|--------|-------|
| Records affected | ~1.9M (expanded detection) |
| Coverage impact | Inflated author counts, polluted author disambiguation |
| User-visible symptoms | Fake "authors" like "Kazan University" in API results |
| Time range | Ongoing, concentrated in 2024-2026 |

## Files in This Issue

| File | Status | Description |
|------|--------|-------------|
| `INVESTIGATION.md` | pending | Root cause analysis |
| `PLAN.md` | complete | Fix approach |
| `ACCEPTANCE.md` | pending | Verification tests |
| `evidence/` | complete | Analysis notebook with detection queries |
| `fix/` | complete | Implementation in `notebooks/ingest/Crossref.py` and `notebooks/maintenance/CleanupAffiliationAsAuthor.py` |

## Quick Links

- Related Databricks tables: `openalex.crossref.crossref_works`
- Related API endpoints: works, authors
- Similar past issues: None identified

---

## Detection Summary

The issue is detected by looking for institution/organization keywords in author `given` and `family` fields:

- **English**: University, Institute, College, Hospital, Department, School, Center, Centre, Laboratory, Faculty, Academy
- **Non-English**: Universiteit, Universidade, Universita, Uniwersytet, Universitesi, Hochschule, Fakultat, Klinikum, Krankenhaus, Politecnico, Politechnika
- **Corporate**: Inc, LLC, Ltd, Corp, Corporation, Company, GmbH, Consortium, Association, Collaboration, Committee, Council, Organization
- **Additional**: Clinic, Medical, Research, Museum, Library, Foundation, Polytechnic

### Breakdown by Detection Category

| Category | Records |
|----------|---------|
| English institution keywords | ~1.54M |
| Non-English institution keywords | ~54k |
| Corporate/organization patterns | ~281k |
| Additional keywords | ~35k |

---

## Next Steps

- [ ] Complete root cause analysis
- [x] Write fix plan
- [ ] Define acceptance criteria
- [x] Implement fix
- [ ] Deploy fix to Crossref DLT pipeline
- [ ] Run cleanup notebook for existing records
- [ ] Run acceptance tests
- [ ] Close issue

---

## Log

| Date | Who | Action |
|------|-----|--------|
| 2026-01-12 | claude | Issue created, expanded detection from 870k to 1.9M records |
| 2026-01-12 | claude | Created fix plan (PLAN.md), implemented fix in Crossref.py, created cleanup notebook |
