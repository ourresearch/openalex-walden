# Issue: Landing Page Parser Regression

**Status**: fixing
**Discovered**: 2026-01-09
**Severity**: high
**Component**: parseland | pipeline

## Summary

A regression in the Parseland/DLT landing page pipeline caused ~1.22 million records to be processed with 0 authors during December 27, 2025 - January 3, 2026. The fix was deployed around January 4, 2026, restoring normal operation for NEW records, but existing records retained stale data because DLT streaming tables don't re-process already-processed records.

## Impact

| Metric | Value |
|--------|-------|
| Records affected | ~1.22 million |
| Coverage impact | ~75% of Elsevier records missing affiliations |
| User-visible symptoms | API returns no affiliations for affected works |
| Time range | Dec 27, 2025 - Jan 3, 2026 |

## Files in This Issue

| File | Status | Description |
|------|--------|-------------|
| `INVESTIGATION.md` | complete | Root cause analysis |
| `PLAN.md` | complete | Fix approach |
| `ACCEPTANCE.md` | pending | Verification tests |
| `evidence/` | | Supporting queries |
| `fix/RefreshStaleParserResponses.py` | complete | Fix notebook |

## Quick Links

- Related Databricks tables: `openalex.landing_page.taxicab_enriched_new`, `openalex.works.locations_parsed`
- Related notebooks: `notebooks/ingest/LandingPage.py`
- Fix notebook: `fix/RefreshStaleParserResponses.py`

---

## Next Steps

- [x] Complete root cause analysis
- [x] Write fix plan
- [x] Define acceptance criteria
- [ ] Run fix notebook on Databricks
- [ ] Run acceptance tests
- [ ] Close issue

---

## Log

| Date | Who | Action |
|------|-----|--------|
| 2026-01-09 | Claude (AI agent) | Investigation complete, issue created |
| 2026-01-09 | Claude (AI agent) | Fix notebook created |
| 2026-01-10 | Claude (AI agent) | Issue restructured into new QA format |
