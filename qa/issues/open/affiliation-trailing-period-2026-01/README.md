# Issue: Raw Affiliation Strings Duplicated Due to Trailing Periods

**Status**: open
**Discovered**: 2026-01-13
**Severity**: medium
**Component**: pipeline

## Summary

Raw affiliation strings are appearing as duplicates in `openalex_works.authorships[*].raw_affiliation_strings` where the only difference is a trailing period. This creates unnecessary duplication in the data and affects the `affiliation_strings_lookup` table.

## Impact

| Metric | Value |
|--------|-------|
| Records affected | TBD (need to query) |
| Coverage impact | Duplicate affiliation strings inflating counts |
| User-visible symptoms | Same affiliation appearing twice in API (with/without period) |
| Time range | Ongoing |

## Files in This Issue

| File | Status | Description |
|------|--------|-------------|
| `INVESTIGATION.md` | complete | Root cause analysis |
| `PLAN.md` | complete | Fix approach |
| `ACCEPTANCE.md` | complete | Verification tests |

## Quick Links

- Related Databricks tables: `openalex.works.work_authors`, `openalex.works.work_authorships`, `openalex.works.openalex_works`, `openalex.institutions.affiliation_strings_lookup`
- Related notebooks: `notebooks/end2end/CreateWorksBase.ipynb`, `notebooks/maintenance/CleanupAffiliationTrailingPeriods.ipynb`

---

## Verified Example

**work_id**: 4414994979

### Current State
| Affiliation String |
|--------------------|
| `Department of General Surgery, Sir Run Run Hospital, Nanjing Medical University, Nanjing, Jiangsu, 211112, China.` |
| `Department of General Surgery, Sir Run Run Hospital, Nanjing Medical University, Nanjing, Jiangsu, 211112, China` |

These are the same affiliation, duplicated only because of the trailing period.

---

## Next Steps

- [x] Complete root cause analysis
- [x] Write fix plan
- [x] Define acceptance criteria
- [x] Implement fix
- [x] Run acceptance tests (verified: 0 affiliations >5 chars with trailing period)
- [ ] Run cleanup on existing data (work_authors, affiliation_strings_lookup)
- [ ] Close issue

---

## Log

| Date | Who | Action |
|------|-----|--------|
| 2026-01-13 | Claude (AI agent) | Issue created |
| 2026-01-13 | Claude (AI agent) | Root cause identified: trailing periods not stripped in CreateWorksBase.ipynb |
| 2026-01-14 | Claude (AI agent) | Fix implemented: added RTRIM to CreateWorksBase.ipynb, created CleanupAffiliationTrailingPeriods.ipynb |
| 2026-01-14 | Claude (AI agent) | Fixed RTRIM syntax for Databricks: changed to TRIM(TRAILING '.' FROM ...) |
| 2026-01-14 | Claude (AI agent) | Verified fix: 0 affiliations >5 chars with trailing period in openalex_works_base |
