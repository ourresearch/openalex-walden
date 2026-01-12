# Issue: PubMed Affiliations Not Used as Source

**Status**: open
**Discovered**: 2026-01-12
**Severity**: high
**Component**: pipeline

## Summary

PubMed is not being used as a source for raw affiliation strings in the walden pipeline, despite having ~28M records with affiliation data. The `CreateCrossrefSuperAuthorships.ipynb` notebook explicitly filters affiliations to only `crossref`, `pdf`, and `landing_page` provenances, excluding PubMed.

## Impact

| Metric | Value |
|--------|-------|
| Records affected | ~16.7M works (PubMed has affiliations, Crossref doesn't) |
| Coverage impact | ~930K recent works (since 2025-06) missing affiliations |
| User-visible symptoms | Works with zero affiliations in API despite PubMed having them |
| Time range | Ongoing for new records |

## Files in This Issue

| File | Status | Description |
|------|--------|-------------|
| `PLAN.md` | complete | Fix approach |
| `ACCEPTANCE.md` | complete | Verification tests |
| `evidence/validation_queries.sql` | complete | SQL queries for validation |

## Quick Links

- Related Databricks tables: `openalex.works.locations_parsed`, `openalex.works.crossref_super_authorships`, `openalex.works.openalex_works`
- Related notebooks: `notebooks/end2end/CreateCrossrefSuperAuthorships.ipynb`
- Priority table: `openalex.system.priority_table` (pubmed is priority 3)

---

## Verified Example

**DOI**: `10.7759/cureus.97342` (PMID: 41426802)

| Source | Authors | Authors with Affiliations |
|--------|---------|---------------------------|
| PubMed | 7 | **7** |
| Crossref | 7 | 0 |
| PDF | 0 | 0 |
| Landing Page | 14 | 0 |
| **Final openalex_works** | 7 | **0** |

PubMed is the ONLY source with affiliation data for this work, but it's not being used.

---

## Next Steps

- [x] Complete root cause analysis
- [x] Write fix plan
- [x] Define acceptance criteria
- [x] Implement fix
- [ ] Deploy to Databricks and run pipeline
- [ ] Run acceptance tests
- [ ] Close issue

---

## Log

| Date | Who | Action |
|------|-----|--------|
| 2026-01-12 | claude | Issue created, validated with database queries |
| 2026-01-12 | claude | Identified ~930K recent works affected |
| 2026-01-12 | claude | Implemented fix: added 'pubmed' to provenance filter in CreateCrossrefSuperAuthorships.ipynb |
