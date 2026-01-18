# Issue: Landing Page Parser Regression

**Status**: blocked
**Discovered**: 2026-01-09
**Severity**: high
**Component**: parseland | pipeline
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
| `PLAN.md` | complete | Fix approach |
| `ACCEPTANCE.md` | pending | Verification tests |
| `evidence/` | | Supporting queries |
| `fix/RefreshStaleParserResponses.py` | complete | Fix notebook |

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
- [ ] **Fix Databricks notebook syntax error** (**BLOCKED** - see PLAN.md for details)
- [ ] Run fix notebook on Databricks
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
