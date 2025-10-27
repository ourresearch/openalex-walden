# Topics Frontfill Refactor - Serverless + CUDA Memory Analysis

## Current Issues

### 1. RDD Operations Not Allowed on Serverless
- Current code uses `mapPartitions` on RDD
- Serverless doesn't support RDD operations
- Need to refactor to use DataFrame operations

### 2. CUDA Out of Memory
From the error:
- GPU has 21.96 GiB total
- Only 8.62 MiB free
- Multiple processes consuming memory:
  - Process 8236: 6.66 GiB
  - Process 8261: 6.88 GiB  
  - Process 8234: 6.88 GiB
  - Process 9392: 798 MiB
  - Process 9396: 758 MiB

**Root Causes:**
1. **Multiple workers loading models simultaneously** - Each partition loads the model independently
2. **Model loaded in every worker** - 384 partitions = 384 model instances
3. **No model sharing** - Each partition process gets its own copy
4. **Batching size too large** - BATCH_SIZE=150 may be too large for available GPU memory

## Solutions

### For Serverless Compatibility (foreachPartition Alternative)

**Option 1: Use DataFrame operations with pandas UDF**
```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, StructType

@pandas_udf(returnType=output_schema, functionType=F.PANDAS_SCALAR_ITER)
def process_batch(iterator):
    ModelCache.load()
    model = ModelCache.model
    
    for batch_df in iterator:
        # Process batch with pandas
        results = []
        for _, row in batch_df.iterrows():
            # Your processing logic
            results.append(process_row(row))
        yield pd.DataFrame(results)
```

**Option 2: Use toLocalIterator with batching**
```python
def process_with_iteration():
    ModelCache.load()
    model = ModelCache.model
    
    results = []
    batch_count = 0
    
    for row in df.toLocalIterator():
        # Process row
        results.append(process_row(row))
        batch_count += 1
        
        if batch_count >= BATCH_SIZE:
            # Write batch
            spark.createDataFrame(results, output_schema).write.mode("append").saveAsTable(...)
            results = []
            batch_count = 0
    
    # Write remaining
    if results:
        spark.createDataFrame(results, output_schema).write.mode("append").saveAsTable(...)
```

**Option 3: Use foreach with direct DBFS writes**
```python
def process_partition_to_dbfs(partition_id, rows):
    ModelCache.load()
    model = ModelCache.model
    
    # Process and write directly to DBFS/dbfs:/tmp/ 
    # This avoids RDD operations
    pass
```

### For CUDA Memory Issues

**Recommendations:**

1. **Reduce partition count** - 384 is excessive, try 32-64
   ```python
   df.repartition(32)  # Instead of 384
   ```

2. **Single model load across all workers** - Use broadcast
   ```python
   model_broadcast = spark.sparkContext.broadcast(ModelCache.model)
   ```

3. **Use CPU for some operations** - Reduce GPU load
   ```python
   model = pipeline(
       device=-1,  # Use CPU instead of GPU
   )
   ```

4. **Reduce batch size**
   ```python
   BATCH_SIZE = 50  # Instead of 150
   ```

5. **Clear CUDA cache after each batch**
   ```python
   torch.cuda.empty_cache()
   ```

6. **Use gradient checkpointing** to reduce memory
   ```python
   model = model.half()  # Use FP16 instead of FP32
   ```

7. **Process sequentially** - One partition at a time
   ```python
   partitions = df.rdd.glom().collect()
   for i, partition_data in enumerate(partitions):
       process_partition(i, partition_data)
   ```

## Recommended Approach

Combine **Option 2 (toLocalIterator)** with **memory optimizations**:

1. Reduce partitions from 384 to 32-64
2. Use toLocalIterator instead of mapPartitions
3. Load model once globally, not per partition
4. Reduce batch size to 50
5. Clear CUDA cache between batches
6. Consider CPU inference if GPU is out of memory

