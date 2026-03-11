# Vector Search Index Sync Protocol

This document describes the safe procedure for syncing the Databricks Vector Search index for OpenAlex's `/find/works` endpoint.

## Overview

The Vector Search index uses a **storage-optimized endpoint**, which means:
- Every sync **partially rebuilds** the index (not fully incremental)
- Syncs can take several hours for large datasets
- **Modifications to the source table during sync can cause failures**

## Pre-Sync Checklist

Before triggering a sync, verify ALL of the following:

### 1. Stop All Embedding Loops

Check for running embedding loops:
```bash
# Check for running SQL statements
databricks api get /api/2.0/sql/statements/<STATEMENT_ID> | jq '.status.state'
```

If any loops are running, either:
- Wait for them to complete, OR
- Cancel them (if safe to do so)

### 2. Verify Table is Stable

```sql
-- Check recent table history for any pending operations
DESCRIBE HISTORY openalex.vector_search.work_embeddings_v2 LIMIT 5
```

Ensure:
- No recent `CREATE OR REPLACE` operations
- No active MERGE/INSERT operations
- Last few operations show `numOutputRows: 0` (empty batches = loop finished)

### 3. Verify Data Integrity

```sql
-- Count embeddings
SELECT COUNT(*) FROM openalex.vector_search.work_embeddings_v2;

-- Check for duplicates (should be 0)
SELECT COUNT(*) - COUNT(DISTINCT work_id) as duplicates
FROM openalex.vector_search.work_embeddings_v2;
```

### 4. Check Current Index Status

```bash
databricks api get /api/2.0/vector-search/indexes/openalex.vector_search.work_embeddings_index | jq '.status'
```

Verify:
- `ready: true` (can serve queries)
- `detailed_state` is not `*_UPDATE` or `*_FAILED`

## Trigger Sync

Once all checks pass:

```bash
# Trigger the sync
databricks api post /api/2.0/vector-search/indexes/openalex.vector_search.work_embeddings_index/sync

# Start monitoring (in a tmux/screen session)
cd /path/to/openalex-walden
python scripts/monitor_vector_sync.py --interval 300 --slack-channel CRRBCGH36
```

## During Sync

**DO NOT:**
- Run any INSERT/UPDATE/DELETE on `work_embeddings_v2`
- Run any `CREATE OR REPLACE TABLE` operations
- Run any table optimization/vacuum operations
- Cancel or restart the sync

**DO:**
- Monitor progress via the monitoring script
- Watch for Slack notifications
- Be patient - syncs can take 4-8 hours for 200M+ rows

## After Sync

### If Successful

1. Verify the index is serving queries:
```bash
curl -s "https://api.openalex.org/find/works/health" | jq '.index'
```

2. Test a search query:
```bash
curl -s "https://api.openalex.org/find/works?query=machine+learning&count=3" | jq '.meta'
```

3. Log the successful sync in the job file (if applicable)

### If Failed

1. Check the error:
```bash
databricks api get /api/2.0/vector-search/indexes/openalex.vector_search.work_embeddings_index | jq '.status.failed_status'
```

2. Common failure causes:
   - **Table modified during sync**: Wait for stability, re-trigger
   - **IngestionFailed**: May be transient, try re-triggering
   - **Persistent failures**: Contact Databricks support

3. Re-trigger (if appropriate):
```bash
databricks api post /api/2.0/vector-search/indexes/openalex.vector_search.work_embeddings_index/sync
```

## Known Issues

### Storage-Optimized Limitations

- **No true checkpointing**: If sync fails at 90%, you start over
- **Partial rebuilds**: Even "incremental" syncs rebuild portions of the index
- **No continuous sync**: Only triggered sync is supported

### Previous Failure (2026-01-25)

The sync failed at Delta commit version 538 because a `CREATE OR REPLACE TABLE AS SELECT` (deduplication) operation modified the table mid-sync.

**Lesson learned**: Never modify the table structure during a sync.

## Monitoring Script Usage

```bash
# Check current status once
python scripts/monitor_vector_sync.py --once

# Monitor continuously (5 min interval, notify Slack)
python scripts/monitor_vector_sync.py --interval 300 --slack-channel CRRBCGH36

# Monitor continuously (1 min interval, no Slack)
python scripts/monitor_vector_sync.py --interval 60
```

Required environment variables:
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `SLACK_BOT_TOKEN` (optional, for notifications)

## Quick Reference

| Command | Purpose |
|---------|---------|
| `databricks api get /api/2.0/vector-search/indexes/openalex.vector_search.work_embeddings_index \| jq '.status'` | Check index status |
| `databricks api post /api/2.0/vector-search/indexes/openalex.vector_search.work_embeddings_index/sync` | Trigger sync |
| `python scripts/monitor_vector_sync.py --once` | One-time status check |
| `DESCRIBE HISTORY openalex.vector_search.work_embeddings_v2 LIMIT 5` | Check recent table changes |
