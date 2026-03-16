# Vector Embeddings Notebook Optimization

## Problem
The current notebook is too slow to run at scale:
- Cell 3 (progress check) took **31 minutes** just to count rows
- Cell 4 (NOT EXISTS query) never completed after 36+ minutes
- The `NOT EXISTS` subquery does a correlated lookup for each of 217M rows

## Optimization Ideas

### 1. Replace NOT EXISTS with LEFT ANTI JOIN
```sql
-- Instead of:
SELECT ... FROM source_table s
WHERE NOT EXISTS (SELECT 1 FROM output_table o WHERE o.work_id = CAST(s.id AS STRING))

-- Use:
SELECT ... FROM source_table s
LEFT ANTI JOIN output_table o ON o.work_id = CAST(s.id AS STRING)
```

### 2. Skip expensive count queries
- Remove cells 3 and 4 entirely from the batch processing loop
- The INSERT is already idempotent (checks for existing embeddings)
- Just process and let it run - check progress separately if needed

### 3. Use a watermark/checkpoint approach
- Track last processed `work_id` or `updated_date` instead of NOT EXISTS
- Process in sorted order and maintain a simple checkpoint value
- Much faster than scanning the entire output table

### 4. Partition the work
- Process by publication_year or work_id range
- Allows parallelization and easier progress tracking
- Can restart specific partitions if they fail

### 5. Reduce batch overhead
- Current BATCH_SIZE = 50,000 but the NOT EXISTS check dominates
- With optimized query, could potentially increase batch size

## Priority
High - current implementation is unusable at scale (would take weeks just on count queries)
