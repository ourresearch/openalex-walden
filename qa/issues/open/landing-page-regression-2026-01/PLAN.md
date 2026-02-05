# Fix Plan: Landing Page Parser Regression

## Approach Comparison

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

---

## Chosen Approach: Option 2

### Implementation

A Databricks notebook that:
1. Queries `taxicab_enriched_new` for records in the affected date range with 0 authors
2. Calls Parseland API directly for each UUID using parallel workers
3. Uses MERGE to UPDATE the `parser_response` column in place
4. Tracks progress with checkpointing for resumability

### Notebook Location

`fix/RefreshStaleParserResponses.py`

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

## Rollout Strategy

1. **Phase 1: Elsevier (10.1016)** - **BLOCKED - NOTEBOOK FIX REQUIRED**
   - Run with `publisher_filter = '10.1016/%'`
   - ~330K records, ~4 hours
   - Verify success before continuing
   - **Status**: Job failed multiple times due to schema inference error
   - **Issue**: The `update_schema` definition in the Databricks notebook has malformed indentation (progressive indentation on each StructField line) causing `SyntaxError: unmatched ']'`
   - **Root Cause**: When the schema was added to the notebook, the text was pasted incorrectly with progressively increasing indentation
   - **Fix Required**: Edit cell 8 of the Databricks notebook at `Users/richard@openalex.org/openalex-walden/notebooks/scraping/RefreshStaleParserResponses` to fix the `update_schema` definition indentation
   - **Correct Code** (from local file `fix/RefreshStaleParserResponses.py` lines 131-140):
     ```python
     # Schema for the update DataFrame (explicit to avoid type inference failures on empty arrays)
     update_schema = StructType([
         StructField("taxicab_id", StringType(), True),
         StructField("new_authors", ArrayType(author_schema), True),
         StructField("new_urls", ArrayType(url_schema), True),
         StructField("new_license", StringType(), True),
         StructField("new_version", StringType(), True),
         StructField("new_abstract", StringType(), True),
         StructField("new_had_error", BooleanType(), True)
     ])
     ```
   - **Note**: Browser-based editing of Databricks notebooks via automation proved unreliable - recommend manual editing in Databricks UI

2. **Phase 2: Other major publishers** - PENDING
   - Wiley (10.1002): ~24K records
   - Springer (10.1007): ~17K records
   - Run each separately to catch any publisher-specific issues

3. **Phase 3: Remaining publishers** - PENDING
   - Remove publisher filter
   - Process all remaining ~850K records

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Parseland API overload | Limit parallelism to 50 workers |
| Partial failures | Checkpointing allows resume |
| Wrong records updated | Dry run mode available |
| Stale HTML in Taxicab | Accept - Parseland returns what's available |

---

## Phase 4: DLT Checkpoint Reset and Backfill (Added 2026-02-04) - SUPERSEDED

> **Note**: This approach was superseded by Phase 5 (full refresh). The backfill notebook is no longer needed.

### Problem

The RefreshStaleParserResponses job successfully MERGE'd corrected parser responses into `taxicab_enriched_new`. However, this caused a downstream failure:

- DLT streaming tables require **append-only** sources
- `landing_page_works_staged_new` reads from `taxicab_enriched_new` via `dlt.read_stream()`
- The MERGE operation created a non-append commit (version 3334)
- The DLT pipeline now fails every time it tries to process that commit

### Original Solution (Not Used)

The original plan was a two-step fix:
1. Reset checkpoint to skip past the MERGE commit
2. Run BackfillLandingPageWorks.py to manually propagate fixed records

This approach was abandoned because checkpoint reset caused cascading issues with downstream tables.

---

## Phase 5: Full Refresh of Intermediate Tables (Updated 2026-02-05) - CURRENT APPROACH

### What Happened

The checkpoint reset and `startingTimestamp` workarounds caused cascading issues:

1. Reset checkpoint on `landing_page_works_staged_new` → tried to reprocess all 59M records
2. Added `startingTimestamp` to skip past MERGE → worked (376K records processed)
3. But `landing_page_combined_new` failed because it detected deletes from the full refresh
4. Tried adding `skipChangeCommits` and `startingTimestamp` to `landing_page_combined_new`
5. This would have required making `landing_page_works_staged_new` non-temporary
6. The backfill source has 236M records - would take too long to reprocess

### Code Reverted

All temporary workarounds were removed from `LandingPage.py`:
- Reverted `landing_page_works_staged_new` back to `dlt.read_stream("taxicab_enriched_new")` with `temporary=True`
- Reverted `landing_page_combined_new` back to `dlt.read_stream()` for both sources

The code is now back to its original structure.

### Current Plan: Full Refresh of Intermediate Tables

Do a full refresh of the intermediate tables (NOT `landing_page_works` which has 55M records):

```bash
databricks api post /api/2.0/pipelines/ff5e63c2-b1b8-49c2-a7c0-2ee246e89e69/updates \
  --json '{"full_refresh_selection": ["landing_page_works_staged_new", "landing_page_combined_new", "landing_page_enriched"]}'
```

This will:
1. Clear checkpoints for the intermediate tables
2. Re-read ALL records from `taxicab_enriched_new` (including the fixed records from RefreshStaleParserResponses)
3. Reprocess 58.8M staged + 236M backfill records through the pipeline
4. UPSERT into `landing_page_works` (existing records preserved)

**Key point:** The fixed records are automatically included - no separate backfill step needed. The full refresh re-reads everything from the source tables, including the MERGE'd corrections.

**Note:** This will reprocess the 236M backfill records, which will take time but is a one-time cost to get the pipeline healthy.

### Execution Steps

1. Run the full refresh command above
2. Wait for pipeline to complete
3. Run end2end pipeline to propagate to `openalex_works`
4. Run acceptance tests

### Alternative: Skip Backfill Temporarily

If the full refresh takes too long, consider:
1. Add `startingTimestamp` to the `landing_page_backfill` view to skip historical records
2. Run the pipeline with only recent records
3. Remove `startingTimestamp` once healthy (checkpoints will preserve position)

---

## Determinism Analysis (Added 2026-02-05)

Before proceeding with the full refresh, we analyzed whether the refresh would produce deterministic results and preserve existing data.

### Concern: `updated_date` Changes

In `landing_page_works_staged_new`, the code sets:
```python
F.current_timestamp().alias("updated_date"),
F.current_timestamp().alias("created_date"),
```

After a full refresh, all 58.8M staged records would get NEW timestamps (Feb 2026 instead of their current Feb 2, 2026 timestamp).

### Data Overlap Analysis

| Source | Records | Overlap |
|--------|---------|---------|
| Backfill (`landing_page_works_backfill`) | 236.3M | 14.9M shared with staged |
| Staged (`taxicab_enriched_new`) | 58.8M | 14.9M shared with backfill |
| Backfill-only | 222.6M | No overlap |
| Staged-only | 43.9M | No overlap |

For overlapping records, staged already wins 99.995% of the time (newer timestamps). After refresh, staged continues to win.

### Downstream Dependencies on `updated_date`

#### 1. `locations_parsed_union` → `locations_parsed`

```python
# UnionAllWorksIntoLocationsParsed.ipynb
spark.readStream
    .option("readChangeFeed", "true")  # Reads changes from landing_page_works
    .table(table_name)

dlt.apply_changes(
    target="locations_parsed",
    source="locations_parsed_union",
    keys=["native_id"],
    sequence_by="updated_date",  # Uses updated_date for conflict resolution
)
```

**Impact**: Records with changed `updated_date` will be emitted as `update_postimage` events via Change Data Feed and flow through the pipeline.

#### 2. `CreateSuperLocations.ipynb`

Uses `updated_date` as a **tiebreaker** when selecting which landing page URL wins for a DOI:

```sql
row_number() over(
  partition by doi
  order by
    url_priority ASC,
    updated_date DESC NULLS LAST,  -- Tiebreaker
    native_id ASC NULLS LAST       -- Final tiebreaker
) as row_num
```

**Analysis**:
- 13.6M DOIs have multiple landing page URLs
- 6.75M DOIs have multiple URLs at the same priority level (rely on `updated_date` tiebreaker)
- **However**: Staged records already have a uniform `updated_date` (Feb 2, 2026)
- Changing to a new uniform date (Feb 5, 2026) won't affect tiebreaker outcomes

#### Current `updated_date` Distribution in `locations_parsed`

| Date | Records | Source |
|------|---------|--------|
| 2026-02-02 | 55.5M | Staged |
| 2025-02-19 | 60.0M | Backfill |
| 2025-02-16 | 64.8M | Backfill |
| 2025-02-15 | 64.7M | Backfill |
| 2025-02-14 | 18.5M | Backfill |

### Conclusion: Refresh is Safe

**Data Correctness**: ✅ Safe
- Staged records already have uniform `updated_date` - changing to a new uniform date won't affect tiebreaker outcomes
- Staged (Feb 2026) continues to win over backfill (Feb 2025) for overlapping records
- Data content (authors, affiliations) comes from `taxicab_enriched_new` which is preserved
- No records will be lost - `apply_changes` does UPSERT, not delete

**Performance Concern**: ⚠️ Reprocessing overhead
- 58.8M records will be marked as "updated" in the Change Data Feed
- This triggers reprocessing through `locations_parsed` → `superlocations` → `works_base` → `openalex_works`
- The end2end pipeline will effectively re-process all landing page data
- This is a one-time cost to restore pipeline health
