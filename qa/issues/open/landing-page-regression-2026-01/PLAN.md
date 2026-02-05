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

## Phase 4: DLT Checkpoint Reset and Backfill (Added 2026-02-04)

### Problem

The RefreshStaleParserResponses job successfully MERGE'd corrected parser responses into `taxicab_enriched_new`. However, this caused a downstream failure:

- DLT streaming tables require **append-only** sources
- `landing_page_works_staged_new` reads from `taxicab_enriched_new` via `dlt.read_stream()`
- The MERGE operation created a non-append commit (version 3334)
- The DLT pipeline now fails every time it tries to process that commit

### Solution: Two-Step Fix

**Step 1: Reset DLT Checkpoint**

Reset the streaming checkpoint for `landing_page_works_staged_new` to skip past the MERGE commit:

```bash
databricks api post /api/2.0/pipelines/ff5e63c2-b1b8-49c2-a7c0-2ee246e89e69/updates \
  --json '{"reset_checkpoint_selection": ["landing_page_works_staged_new"]}'
```

This unblocks the pipeline for NEW records going forward.

**Step 2: Run Backfill Notebook**

The checkpoint reset skips the MERGE'd records, so they won't flow through DLT automatically. Run the backfill notebook to propagate them directly:

- **Notebook**: `notebooks/maintenance/BackfillLandingPageWorks.py`
- **What it does**:
  1. Queries fixed records from `taxicab_enriched_new` (those with authors > 0)
  2. Applies the same transformations as the DLT pipeline
  3. MERGEs directly into `landing_page_works`
- **Safe because**: `landing_page_works` has Change Data Feed enabled, and downstream consumers use `readChangeFeed: true`

### Why MERGE into landing_page_works is Safe

| Table | Downstream Consumer | Read Method | MERGE Safe? |
|-------|--------------------|--------------| ------------|
| `taxicab_enriched_new` | `landing_page_works_staged_new` | `dlt.read_stream()` (direct) | ❌ No |
| `landing_page_works` | `locations_parsed_union` | `.readStream.option("readChangeFeed", "true")` | ✅ Yes |

The Change Data Feed tracks all changes including MERGE updates, so downstream propagation works correctly.

### Execution Order

1. Reset checkpoint (Step 1) - unblocks DLT pipeline
2. Run backfill notebook (Step 2) - propagates fixed records
3. Run end2end pipeline - propagates to `openalex_works`
4. Run acceptance tests
