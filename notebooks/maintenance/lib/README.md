# Bulk Processing Utilities

## The Connection Pool Trap

**NEVER use raw `requests.get()` or `requests.post()` in bulk jobs!**

```python
# BAD - limits to 10 concurrent connections regardless of thread count
with ThreadPoolExecutor(max_workers=120) as executor:
    futures = [executor.submit(lambda: requests.post(url, json=p)) for p in payloads]
```

```python
# GOOD - uses session with proper connection pooling
from lib.bulk_http_client import BulkHttpClient

client = BulkHttpClient("http://example.com", max_workers=120)
results = client.post_many("/endpoint", payloads, result_mapper, error_mapper)
```

## Why This Matters

Python's `requests` library uses a default connection pool of **10 connections per host**. Even with 120 threads, you'll only get ~10 concurrent requests, limiting throughput to ~1-3 req/sec instead of ~15-20 req/sec.

## Quick Checklist for Bulk Jobs

Before running any bulk HTTP job, verify:

- [ ] Using `BulkHttpClient` or a properly configured `requests.Session`
- [ ] `pool_connections` and `pool_maxsize` >= `max_workers`
- [ ] Session is shared across all threads (not created per-request)
- [ ] Appropriate timeout set (default: 60s)
- [ ] Retry logic configured for 5xx errors

## Available Clients

```python
from lib.bulk_http_client import create_taxicab_client, create_parseland_client

# For Taxicab bulk operations
taxicab = create_taxicab_client(max_workers=120)

# For Parseland bulk operations
parseland = create_parseland_client(max_workers=120)
```

## Expected Throughput

With proper configuration:

| Service | Response Time | Max Workers | Expected Throughput |
|---------|--------------|-------------|---------------------|
| Taxicab (via Zyte) | 5-9 sec | 120 | ~15-20 req/sec |
| Taxicab (direct) | 0.5-1 sec | 120 | ~120+ req/sec |
| Parseland | 0.2-0.5 sec | 120 | ~300+ req/sec |

If you're seeing <5 req/sec with 100+ workers, check connection pooling first!

## ECS Auto-Scaling Gotcha

Taxicab uses CPU-based auto-scaling (target: 40%). But Taxicab is **I/O-bound** (waiting on Zyte), so CPU stays low → auto-scaler scales DOWN even when you need more containers.

**Before bulk jobs, suspend auto-scaling:**

```bash
# Check current state
aws ecs describe-services --cluster harvester --service harvester-service \
  --query 'services[0].{desired: desiredCount, running: runningCount}'

# Suspend auto-scaling
aws application-autoscaling register-scalable-target \
  --service-namespace ecs \
  --resource-id service/harvester/harvester-service \
  --scalable-dimension ecs:service:DesiredCount \
  --suspended-state '{"DynamicScalingInSuspended":true,"DynamicScalingOutSuspended":true}'

# Scale to desired count
aws ecs update-service --cluster harvester --service harvester-service --desired-count 120

# After job: re-enable auto-scaling
aws application-autoscaling register-scalable-target \
  --service-namespace ecs \
  --resource-id service/harvester/harvester-service \
  --scalable-dimension ecs:service:DesiredCount \
  --suspended-state '{"DynamicScalingInSuspended":false,"DynamicScalingOutSuspended":false}'
```

## Pre-Flight Checklist

Before running a bulk Taxicab/Parseland job:

- [ ] **Suspend ECS auto-scaling** (see above)
- [ ] **Scale containers** to match worker count
- [ ] **Wait for containers** to reach "running" state
- [ ] **Verify target health**: all containers should be "healthy" in load balancer
- [ ] **Use BulkHttpClient** with proper connection pooling
- [ ] **Filter metadata from payloads** — prefix internal fields with `_` (e.g., `_created_date`)

## Payload Metadata Convention

Use underscore prefix for fields that are metadata (not sent to API):

```python
payloads.append({
    "url": row["url"],           # Sent to API
    "native_id": native_id,      # Sent to API
    "_created_date": row["date"], # NOT sent to API (underscore prefix)
    "_old_id": row["id"]          # NOT sent to API (underscore prefix)
})
```

`BulkHttpClient` automatically filters out `_`-prefixed fields before sending.
