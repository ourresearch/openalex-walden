# Issue: Landing Page Parser Regression

**Status**: in-progress
**Discovered**: 2026-01-09
**Severity**: high
**Component**: parseland | pipeline | DLT
**Reporter**: Nees Jan van Eck (CWTS Leiden)

## Summary

A regression in the Parseland/DLT landing page pipeline caused ~1.22 million records to be processed with 0 authors during December 27, 2025 - January 3, 2026. The fix was deployed around January 4, 2026, restoring normal operation for NEW records, but existing records retained stale data because DLT streaming tables don't re-process already-processed records.

## Background

This issue was surfaced by Nees Jan van Eck from CWTS Leiden in December 2025. His analysis showed a significant drop in metadata completeness for 2024 publications:

- Affiliations for Scopus/WoS works dropped from ~94% (2022) to ~85% (2024)
- Elsevier is a major contributor to the affiliation drop
- IEEE is a major contributor to the references drop

See: `qa/exploration/datasets/nees/email_thread_2025-12.md`

Nees's hypothesis: *"Could it be that scraping landing pages has become harder lately because many publishers and platforms are nowadays using services like Cloudflare?"*

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
| `PLAN.md` | complete | Fix approach (updated with Phase 4) |
| `ACCEPTANCE.md` | pending | Verification tests |
| `evidence/` | | Supporting queries |
| `fix/RefreshStaleParserResponses.py` | complete | Phase 1-3: Fix parser responses in taxicab_enriched_new |
| `notebooks/maintenance/BackfillLandingPageWorks.py` | complete | Phase 4: Propagate fixed records to landing_page_works |

## Quick Links

- Related Databricks tables: `openalex.landing_page.taxicab_enriched_new`, `openalex.works.locations_parsed`
- Related notebooks: `notebooks/ingest/LandingPage.py`
- Fix notebook: `fix/RefreshStaleParserResponses.py`
- Test dataset: `qa/exploration/datasets/nees/` (316 DOIs from Nees Jan van Eck)
- Email thread: `qa/exploration/datasets/nees/email_thread_2025-12.md`

---

## Next Steps

- [x] Complete root cause analysis
- [x] Write fix plan
- [x] Define acceptance criteria
- [x] Run baseline "Before" queries (recorded 2026-01-17)
- [x] Fix Databricks notebook syntax error
- [x] Run RefreshStaleParserResponses to fix `taxicab_enriched_new`
- [ ] **Reset DLT checkpoint** (unblocks pipeline for new records)
- [ ] **Run BackfillLandingPageWorks.py** (propagates fixed records to `landing_page_works`)
- [ ] Run end2end pipeline (propagates to `openalex_works`)
- [ ] Run acceptance tests
- [ ] Close issue

---

## Log

| Date | Who | Action |
|------|-----|--------|
| 2025-12-10 | Nees Jan van Eck | Initial report of metadata completeness drop |
| 2025-12-14 | Nees Jan van Eck | Provided sample DOIs spreadsheet |
| 2026-01-09 | Claude (AI agent) | Investigation complete, issue created |
| 2026-01-09 | Claude (AI agent) | Fix notebook created |
| 2026-01-10 | Claude (AI agent) | Issue restructured into new QA format |
| 2026-01-11 | Claude (AI agent) | Added email thread and linked to nees dataset |
| 2026-01-17 | Claude (AI agent) | Ran baseline queries, recorded in ACCEPTANCE.md |
| 2026-01-17 | Claude (AI agent) | Started Phase 1 fix job on Databricks (Job ID: 348519510931285) |
| 2026-01-17 | Claude (AI agent) | Job failed - SyntaxError in update_schema due to malformed indentation |
| 2026-01-17 | Claude (AI agent) | Attempted browser-based fix but Databricks notebook editing unreliable via automation |
| 2026-01-17 | Claude (AI agent) | Updated PLAN.md with fix instructions - manual edit of Databricks notebook required |
| 2026-02-04 | Casey/Coworker | RefreshStaleParserResponses ran successfully, fixed records in `taxicab_enriched_new` |
| 2026-02-04 | Coworker | Identified DLT streaming failure - MERGE on `taxicab_enriched_new` broke `landing_page_works_staged_new` checkpoint |
| 2026-02-04 | Claude (AI agent) | Created `BackfillLandingPageWorks.py` to propagate fixed records bypassing DLT |
| 2026-02-04 | Claude (AI agent) | Updated issue files with new plan: reset checkpoint + backfill |
