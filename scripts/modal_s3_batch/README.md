# Modal GPU batch via S3

Run a big GPU job (embeddings, model scoring) on Modal when AWS GPUs are short, and land the results in a Databricks table,
with no stored keys and no copy through a laptop. How and when: [docs/runbooks/modal-gpu-batch-via-s3.md](../../docs/runbooks/modal-gpu-batch-via-s3.md).

| File | Step | What it does |
|---|---|---|
| `role.yaml` | 1 | CloudFormation for a per-job IAM role: one Modal app, one S3 prefix, OIDC sign-in, no key |
| `export.py` | 2 | Databricks table shards -> `s3://<bucket>/<prefix>/in/shard=S/*.parquet` |
| `app_template.py` | 3 | Modal app skeleton: read a shard, run `transform()`, write `out/shard=S/part-0.parquet`, skip finished shards |
| `embed_qwen3_app.py` | 3 | Worked example from oxjob #2000: Qwen3-Embedding-0.6B with 4 vLLM engines per H100 |
| `s3io.py` | 3 | Container-side helpers: assume the role from `MODAL_IDENTITY_TOKEN`, read/write shards |
| `load_dbx.py` | 5 | Databricks notebook: check each batch, append with an idempotent write id, progress table |
| `launch.py` | 2-5 | Export, Modal run, then submit the load notebook and wait |
| `dbsql.py` | | SQL statement helper (SDK, profile DEFAULT) |
