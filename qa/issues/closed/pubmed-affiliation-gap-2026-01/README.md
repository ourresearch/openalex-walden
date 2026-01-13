# Issue: PubMed Affiliations Not Used as Source

**Status**: closed
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

### Before Fix
| Source | Authors | Affiliations |
|--------|---------|--------------|
| PubMed | 7 | **7** |
| Crossref | 7 | 0 |
| PDF | 0 | 0 |
| Landing Page | 14 | 0 |
| **Final openalex_works** | 7 | **0** |

### After Fix
| DOI | Authors | Affiliations |
|-----|---------|--------------|
| `10.7759/cureus.97342` | 7 | **8** |
| `10.1016/j.bioadv.2025.214692` | 8 | **8** |
| `10.1016/j.cpm.2025.06.004` | 4 | **4** |

All test DOIs now have PubMed affiliations in `openalex_works`.

---

## Next Steps

- [x] Complete root cause analysis
- [x] Write fix plan
- [x] Define acceptance criteria
- [x] Implement fix
- [x] Deploy to Databricks and run pipeline
- [x] Run acceptance tests (validated in openalex_works_base)
- [x] Run acceptance tests (validated in openalex_works - final table)
- [x] Commit changes
- [x] Close issue

---

## Results

- **~19.6M works** updated with PubMed affiliations
- **~11.7M additional works** now have institution matches
- Institution coverage for updated works: **89.8%** (vs 30.8% baseline)
- Recent years (2023-2025): **~877K additional works** with institutions

---

## Log

| Date | Who | Action |
|------|-----|--------|
| 2026-01-12 | claude | Issue created, validated with database queries |
| 2026-01-12 | claude | Identified ~930K recent works affected |
| 2026-01-12 | claude | Implemented fix: added 'pubmed' to provenance filter in CreateCrossrefSuperAuthorships.ipynb |
| 2026-01-12 | claude | Validated fix in openalex_works_base - test DOIs now have affiliations |
| 2026-01-13 | claude | Validated fix in openalex_works (final table) - all test DOIs confirmed |
| 2026-01-13 | claude | Confirmed ~11.7M additional works with institutions, issue closed |
