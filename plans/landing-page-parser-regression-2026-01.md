# Landing Page Parser Regression: Dec 27, 2025 - Jan 3, 2026

## Summary

A regression in the Parseland/DLT landing page pipeline caused **~1.22 million records** to be processed with 0 authors during December 27, 2025 - January 3, 2026. A fix was deployed around January 4, 2026, restoring normal operation for NEW records, but **existing records retained stale data** because DLT streaming tables don't re-process already-processed records.

**Key Finding:** Parseland returns correct data when called directly, but the cached `parser_response` in `taxicab_enriched_new` contains empty author arrays from the broken period.

---

## Timeline

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

## Impact

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

## Fix Options

### Option 1: Delete and Re-process (NOT RECOMMENDED)

Delete stale records from `taxicab_enriched_new`, let streaming table re-process.

| Metric | Value |
|--------|-------|
| Time to complete | ~25 days |
| API disruption | YES - data disappears during reprocessing |
| Risk | High |

### Option 2: Batch UPDATE (RECOMMENDED)

Write a notebook that queries affected UUIDs, calls Parseland API, and UPDATEs `parser_response` in place.

| Metric | Value |
|--------|-------|
| Time to complete | ~4 hours (50 workers) |
| API disruption | None |
| Risk | Low |

**Implementation:** See `notebooks/scraping/RefreshStaleParserResponses.py`

---

## Fix Implementation

### Notebook Location

`notebooks/scraping/RefreshStaleParserResponses.py`

### How to Run

1. Create a new Databricks Job with one task pointing to the notebook
2. Use a dedicated cluster (e.g., `i3.xlarge` with 4-8 workers)
3. Set parameters:
   - `start_date`: `2025-12-27`
   - `end_date`: `2026-01-03`
   - `publisher_filter`: `10.1016/%` (start with Elsevier, expand later)
   - `parallelism`: `50`
4. Run as one-off job (does not need to coordinate with end2end)

### Expected Runtime

| Scope | Records | Time (50 workers) |
|-------|---------|-------------------|
| Elsevier only | ~337K | ~4 hours |
| All publishers | ~1.22M | ~14 hours |

---

## Verification Queries

### Before Fix: Check Affected Records

```sql
SELECT COUNT(*) as affected
FROM openalex.landing_page.taxicab_enriched_new
WHERE processed_date BETWEEN '2025-12-27' AND '2026-01-03'
  AND size(parser_response.authors) = 0
  AND url NOT LIKE '%/pdf%'
  AND native_id LIKE '10.1016/%';
```

### After Fix: Verify Records Updated

```sql
-- Check that previously-0-author records now have authors
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN size(parser_response.authors) > 0 THEN 1 ELSE 0 END) as fixed
FROM openalex.landing_page.taxicab_enriched_new
WHERE processed_date BETWEEN '2025-12-27' AND '2026-01-03'
  AND url NOT LIKE '%/pdf%'
  AND native_id LIKE '10.1016/%';
```

### Verify API Impact

```bash
# Check a specific DOI that was affected
curl "https://api.openalex.org/works/https://doi.org/10.1016/j.enbuild.2024.114587" \
  | jq '.authorships[0].raw_affiliation_strings'
```

---

## Open Questions

1. **What caused the Dec 27 regression?** - The original bug that caused Parseland to return empty results is not yet identified. Should investigate Parseland deployment history.

2. **Prevent recurrence?** - Consider adding monitoring for parser success rate anomalies.

3. **PDF URLs?** - PDF URLs have low success rates (~9%) even during normal operation. This is expected since PDF pages don't have structured author data.

---

## Related Files

| File | Purpose |
|------|---------|
| `notebooks/scraping/RefreshStaleParserResponses.py` | Fix notebook |
| `notebooks/ingest/LandingPage.py` | DLT pipeline (lines 161-190 show streaming enrichment) |
| `CHANGELOG.md` | Add entry after fix is deployed |

---

## Investigation Conducted

**Date:** January 9, 2026
**Investigator:** Claude (AI agent)
**Ticket/Request:** NEES Elsevier affiliation coverage analysis → pipeline debugging

**Key discoveries:**
1. NEES DOIs processed during broken period have 0 authors in `taxicab_enriched_new`
2. Parseland returns correct data when called directly
3. Success rate timeline clearly shows Dec 27 - Jan 3 regression
4. DLT streaming tables don't re-process existing records
