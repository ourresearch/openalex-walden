# Issue: Elsevier __PRELOADED_STATE__ and Springer Corresponding Author

**Status**: closed
**Discovered**: 2025-01-03
**Severity**: high
**Component**: parseland

## Summary

Parseland was failing to extract author/affiliation data from modern ScienceDirect (Elsevier) pages that use `__PRELOADED_STATE__` JavaScript instead of JSON script tags. Additionally, Springer pages were not correctly identifying corresponding authors from "Correspondence to" sections.

## Impact

| Metric | Value |
|--------|-------|
| Records affected | 2,484 DOIs identified as failing |
| Coverage impact | ~12% of failures fixed |
| User-visible symptoms | Missing affiliations and corresponding author markers |
| Time range | Issue discovered and fixed on 2025-01-03 |

## Files in This Issue

| File | Status | Description |
|------|--------|-------------|
| `INVESTIGATION.md` | complete | Root cause analysis |
| `ACCEPTANCE.md` | complete | Verification results (PASS) |
| `evidence/` | | Supporting data files |
| `fix/` | | Reference to parseland-lib commit |

## Quick Links

- **parseland-lib commit**: 19363d4
- Related files: `sciencedirect.py`, `springer.py`

---

## Results

- **293 DOIs fixed** (11.8% of tested failures)
- **286 Springer corresponding author fixes**
- **7 Elsevier affiliation fixes**
- **0 regressions**

---

## Log

| Date | Who | Action |
|------|-----|--------|
| 2025-01-03 | Human + Claude | Investigation and fix deployed |
| 2025-01-03 | Claude | Verification complete, PASS |
| 2026-01-10 | Claude | Migrated to new QA structure |
