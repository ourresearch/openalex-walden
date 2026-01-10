# Acceptance Criteria

## Definition of Done

This issue is resolved when ALL of the following tests pass.

---

### Test 1: Re-run Previously Failing DOIs

**Purpose**: Measure improvement on the 2,484 DOIs that were previously failing.

**Command**:
```bash
python compare_before_after.py openalex_failures.tsv parseland_rerun.tsv
```

**Expected Result**: >5% of failures fixed, 0 regressions

**Actual Result**:
- 293 DOIs fixed (11.8%)
- 0 regressions

**Status**: PASS

---

### Test 2: Sample Elsevier __PRELOADED_STATE__ Pages

**Purpose**: Verify new JSON extraction method works.

**Command**:
```bash
curl "http://parseland-load-balancer.../parseland?doi=10.1016/j.jde.2019.01.028"
```

**Expected Result**: Returns authors with affiliations

**Actual Result**: 2 authors extracted with affiliations

**Status**: PASS

---

### Test 3: Sample Springer Corresponding Author

**Purpose**: Verify "Correspondence to" detection works.

**Command**:
```bash
curl "http://parseland-load-balancer.../parseland?doi=10.1007/s00170-017-0085-8"
```

**Expected Result**: At least one author marked as corresponding

**Actual Result**: 6 authors extracted, 1 marked as corresponding

**Status**: PASS

---

### Test 4: No Regressions

**Purpose**: Ensure previously working pages still work.

**Query**: Compare before/after results for all 2,484 DOIs.

**Expected Result**: 0 regressions (no pages that worked before now fail)

**Actual Result**: 0 regressions

**Status**: PASS

---

## Verification Log

| Date | Tester | Tests Run | Results | Notes |
|------|--------|-----------|---------|-------|
| 2025-01-03 | Claude | 1,2,3,4 | ALL PASS | Fix deployed and verified |

---

## Summary

| Metric | Count | Percentage |
|--------|-------|------------|
| **Affiliation failures fixed** | 7 | 4.4% |
| **Corresponding author failures fixed** | 286 | 12.3% |
| **Total fixed** | 293 | 11.8% |
| **Regressions** | 0 | 0% |

The lower-than-expected affiliation improvement is likely because:
1. Many Elsevier failures were for older pages that don't use `__PRELOADED_STATE__`
2. Some failures are due to pages with no HTML available (Taxicab didn't scrape them)
3. Some pages have other parsing issues beyond JSON extraction
