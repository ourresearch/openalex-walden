# Investigation: Landing Page Parser Regression

**Status**: Complete
**Investigator**: Claude (AI agent)
**Date**: 2026-01-09

## Timeline Discovery

| Date | Success Rate | Status |
|------|--------------|--------|
| Dec 24-26, 2025 | 32-57% | Normal operation |
| **Dec 27, 2025** | **13%** | **Regression begins** |
| Dec 28 - Jan 3 | 8-13% | Broken period |
| **Jan 4, 2026** | **73-81%** | **Fix deployed** |
| Jan 5-9, 2026 | 72-81% | Normal operation |

### Evidence Query

```sql
SELECT
    DATE(processed_date) as process_date,
    COUNT(*) as total,
    SUM(CASE WHEN size(parser_response.authors) > 0 THEN 1 ELSE 0 END) as with_authors,
    ROUND(100.0 * SUM(CASE WHEN size(parser_response.authors) > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as pct
FROM openalex.landing_page.taxicab_enriched_new
WHERE processed_date >= '2025-12-20'
  AND native_id LIKE '10.1016/%'
GROUP BY DATE(processed_date)
ORDER BY process_date DESC;
```

---

## Root Cause

### The Pipeline Flow

```
taxicab_results → taxicab_filtered_new → taxicab_enriched_new → landing_page_combined_new → locations_parsed → openalex_works
                                              ↑
                                    parser_udf calls Parseland API
                                    Results cached in parser_response column
```

### What Went Wrong

1. **Streaming Table Behavior:** `taxicab_enriched_new` is a DLT streaming table that only processes NEW records from `taxicab_filtered_new`

2. **Regression Period:** During Dec 27 - Jan 3, something caused Parseland to return empty author arrays (the exact cause of the original regression is not yet identified)

3. **Data Cached Forever:** Once a record is processed and `parser_response` is written, the streaming table never re-processes it

4. **Fix Applied to New Records Only:** The Jan 4 fix restored normal operation for new records, but existing records retained their stale (broken) data

### Verification: Parseland Works Now

```bash
# Direct Parseland call returns correct data
curl "http://parseland.../parseland/c1653ca3-5d5c-4e2c-9017-b5638e688c65"
# Returns: {"authors": [{"name": "Khaled I. Alghamdi", "affiliations": [...], ...}]}

# But taxicab_enriched_new shows 0 authors for same UUID
SELECT size(parser_response.authors) FROM taxicab_enriched_new
WHERE taxicab_id = 'c1653ca3-5d5c-4e2c-9017-b5638e688c65';
# Returns: 0
```

---

## Impact Assessment

### Scale

| Category | Count |
|----------|-------|
| Total records processed Dec 27 - Jan 3 | ~4.75M |
| Records with 0 authors (potentially fixable) | ~1.79M |
| **Non-PDF URLs with 0 authors** | **~1.22M** |
| Elsevier non-PDF records | ~337K |

### By Publisher

| Publisher | Affected Records |
|-----------|------------------|
| Other | 840,754 |
| Elsevier (10.1016) | 337,180 |
| Wiley (10.1002) | 23,629 |
| Springer (10.1007) | 17,088 |

### Downstream Effects

- `locations_parsed` shows `affiliations_exist = false` for landing page records
- `openalex_works` has 0 `raw_affiliation_strings` for affected works
- Live API returns no affiliations for these works

---

## Open Questions

1. **What caused the Dec 27 regression?** - The original bug that caused Parseland to return empty results is not yet identified. Should investigate Parseland deployment history.

2. **Prevent recurrence?** - Consider adding monitoring for parser success rate anomalies.

3. **PDF URLs?** - PDF URLs have low success rates (~9%) even during normal operation. This is expected since PDF pages don't have structured author data.
