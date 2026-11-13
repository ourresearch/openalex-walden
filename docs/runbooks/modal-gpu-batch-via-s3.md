# Runbook: run a GPU batch on Modal and land the results in Databricks through S3

Use this when a big GPU job (embedding hundreds of millions of texts, scoring with a model) can't get GPUs on AWS, or would take
days on what AWS gives. Modal gives us many H100s within minutes; this runbook gets the inputs to Modal and the results back
into a Databricks table without stored keys and without copying anything through a laptop. Code: `scripts/modal_s3_batch/`.

First used for oxjob #2000 (12-13 Nov 2026): 1.62B reference texts (then a 144M top-up) embedded into
`openalex.vector_search.reference_embeddings_qwen3`, most of it in ~2.5 hours on ~50 H100s, after AWS GPUs ran out.

## When to use it

- **AWS can't give you GPUs.** On 12 Nov 2026 (22:40-00:05 UTC) us-east-1 had no on-demand g6e.2xlarge (L40S) in any zone
  (1a-1d; 1f doesn't offer g6e at all), no p4d.24xlarge / p5.48xlarge / p5.4xlarge (A100/H100) in 1b-1f, spot g6e gave 1-2 of
  7, and L4 (g6.2xlarge) peaked at ~48 of the ~100 asked, with two zones giving none. Each error named other zones as having
  capacity; those failed too. Full log: oxjobs `working/reference-matcher` (`~/ox/oxdata/reference-matcher/vt/capacity_evidence.md`).
- **Or the job is big and the GPUs AWS gives are slow.** The same work on ~44 L4s was going to take ~24 hours across two
  nights (the L4 fleet must pause for the 05:00 UTC nightly's GPU jobs); Modal did the rest in ~2.5 hours.
- **Not** for small or nightly jobs: Databricks' own embedding endpoint (`ai_query('databricks-qwen3-embedding-0-6b', ...)`,
  ~$3.4 per 1M texts) or a Databricks GPU cluster needs no role and no S3 round trip.

## The shape

```
Databricks table (key, input cols, shard)            per-job IAM role (role.yaml): one Modal app, one S3 prefix, OIDC, no key
   | export.py: external parquet table                    ^ Modal containers present MODAL_IDENTITY_TOKEN
   v                                                      |
s3://openalex-sandbox/<prefix>/in/shard=S/*.parquet --> Modal app (GPU) --> s3://openalex-sandbox/<prefix>/out/shard=S/part-0.parquet
                                                                               | load_dbx.py: checks, idempotent write id per batch
                                                                               v
                                                                         Delta table (+ progress table)
```

- **No keys.** AWS already trusts Modal's identity provider (`arn:aws:iam::639657227068:oidc-provider/oidc.modal.com`). Each
  job gets its own role that only its Modal app can assume and that can only read and write its own prefix. Sessions last an
  hour. Precedents: `argus-offdesk-readonly` (#1831), `oxjob2000-modal-refemb` (#2000).
- **Why `openalex-sandbox`:** it has a Unity Catalog external location, so Databricks can write the inputs there and read the
  outputs back directly.
- **Shards and batches.** Give the source table an INT `shard` column (e.g. `pmod(xxhash64(key), 1024)`); Modal works one shard
  per call; the load works a batch of 8 shards at a time. Everything is rerun-safe: Modal skips shards whose output exists, the
  load skips batches already written.

## Steps

### 0. Prove it on a sample first (minutes, a few dollars)

Run the Modal app on ~2,000 inputs with a known answer (for embeddings: the same texts from the Databricks endpoint) and check
the match. #2000: vLLM vectors vs `databricks-qwen3-embedding-0-6b`, mean cosine 0.99999, min 0.99988, all >= 0.9998. Then
measure one container's speed (step 4) before sizing the fleet.

### 1. The role (an AWS admin approves it)

Creating an IAM role needs an AWS admin. On the mini: `aws-mini-admin` with a 6-digit code from Casey's phone (one command per
code, the code lasts 30 seconds). Get the Modal workspace id once with `modal run <app>.py::ident` (it's in the token's `sub`;
ours is `ac-y1mqNX3ygQK2Qujkqwnkvf`, the default in the template).

```
aws-mini-admin <code> cloudformation deploy --template-file scripts/modal_s3_batch/role.yaml \
  --stack-name <job>-modal-s3 --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides RoleName=<job>-modal-s3 AppName=<modal app name> Prefix=<job>/<task>
```

Put the role ARN, bucket, prefix and app name into your Modal app (`app_template.py` or `embed_qwen3_app.py`).

### 2. Export the inputs

```
python scripts/modal_s3_batch/export.py --source <table with shard> --cols text_hash,text \
  --staging openalex.transient.<job>_modal_in --s3 s3://openalex-sandbox/<job>/<task> --shards 0-255
```

#2000: 404M texts (59.9 GB of parquet) exported in 32 seconds on shared-3xl.

### 3. The Modal app

Copy `app_template.py` (or start from `embed_qwen3_app.py`), fill in `transform()`, and `modal deploy` it. Rules learned the
hard way:

- **Model weights on a Modal volume**, copied once (`modal run embed_qwen3_app.py::model`), not downloaded from Hugging Face by
  every container (hundreds of containers each pulling the model is slow and can hit Hugging Face's limits).
- **One vLLM engine per GPU is CPU-bound** (~2,000 texts/s for Qwen3-Embedding-0.6B); 4 engines sharing one H100 gave
  ~4,000-4,200 texts/s. The example starts them as child processes and fails fast if one dies.
- **`max_model_len` 8,192, not 2,048:** 1,000 characters of some scripts (CJK, Cyrillic with diacritics) run past 2,048 tokens and
  kill the engine.
- **Write one output file per shard** and check `head_object` first, so reruns skip finished work.

### 4. Size the containers, then run

Measure one container before the fleet: Modal bills the CPU and memory you reserve, not what you use. #2000: a busy H100
container used ~4.3 cores and ~21 GB; 12 CPU / 48 GB ran as fast as 32 / 128 (3,960 vs ~4,000 texts/s) for $4.90 vs $6.48 an
hour. Then:

```
modal run embed_qwen3_app.py::backfill --shards 0-255      # or launch.py for steps 2-5 in one go
```

Our Team plan runs up to 50 GPUs at once (a probe of 60 got 42). #2000 ran ~50 H100s at ~170K texts/s.

### 5. Load into Databricks

`load_dbx.py` (submitted by `launch.py`, or run it as a notebook) checks each batch against the source table (same row count, no
duplicate key, no bad value, e.g. `v IS NULL OR length(v) != 2048`) and appends it with `txnAppId = <write_id_prefix><batch>`,
`txnVersion = 1`. Delta skips a write id it has already committed, so a rerun or a second route never duplicates rows; a batch
that fails its checks writes nothing and the run fails. #2000: ~2-2.5 minutes per 12.6M-row batch on 8 rd-fleet.4xlarge workers;
two or three load runs keep up with 50 H100s.

```
python scripts/modal_s3_batch/launch.py --load-only --batches 0-31 --source ... --s3 ... --dest ... \
  --select "CAST(text_hash AS BIGINT) AS text_hash, text, v, current_timestamp() AS embedded_at" \
  --bad "v IS NULL OR length(v) != 2048" --write-id-prefix <job>_b --progress openalex.transient.<job>_progress
```

Then check the whole table: rows = source rows, distinct keys = rows, no bad values, a sample of vectors at unit length.

### 6. Clean up

- **Empty the prefix, old versions too.** `openalex-sandbox` keeps old versions and never expires them, so `aws s3 rm --recursive`
  only adds delete markers and the 3 TB stays billed. Delete the versions and markers as well (#2000 removed 11,246):
  ```
  aws s3 rm s3://openalex-sandbox/<job>/<task>/ --recursive
  aws s3api list-object-versions --bucket openalex-sandbox --prefix <job>/<task>/ --output json \
    --query '{Objects: [Versions[].{Key:Key,VersionId:VersionId}, DeleteMarkers[].{Key:Key,VersionId:VersionId}][]}' > /tmp/v.json
  aws s3api delete-objects --bucket openalex-sandbox --delete file:///tmp/v.json   # 1,000 keys per call: loop for more
  ```
- Drop the staging external table and any transient input tables.
- Delete the role (one more admin code): `aws-mini-admin <code> cloudformation delete-stack --stack-name <job>-modal-s3`, unless the
  job will reuse it soon.

## Cost (measured, #2000)

| | Per 1M texts | #2000 |
|---|---|---|
| Modal H100 + CPU + memory (12 CPU / 48 GB) | ~$0.33-0.45 | $644 (incl. a restart and the oversized first hour) |
| Modal egress, results to S3 (~2.2 KB per text) | ~$0.08 | $111 |
| AWS S3 reads into Modal | ~$0.01 | up to ~$20 |
| Databricks load runs | ~$0.06 | ~$90 |

- **Modal egress is $0.04/GiB** (since 1 Oct 2026). The Team plan includes 10 TiB a month, but it's shared by every app in the
  workspace; in November other apps had used it up before #2000 started, so count on paying it.
- **Modal's billing report lags up to an hour** (`modal billing report --show-resources` splits GPU, CPU, memory, egress); check it
  after the first hour and right-size then.
- For comparison, the same embedding on AWS via Databricks GPU clusters measured ~$0.77/1M on g6e (L40S) and ~$0.87/1M on g6 (L4),
  when the machines come; the Databricks endpoint is ~$3.4/1M.

## Gotchas

- **Two routes at once.** If an AWS fleet and Modal work the same job, give every batch the same write id on both routes (Delta
  then refuses duplicates) and keep a claims table so they don't do the same batch twice; one owner per set of clusters (in #2000
  two agents briefly stopped each other's runs).
- **Pin what you can measure.** Equivalence check before the big run (step 0); a downstream check if one exists (#2000: the same
  top search result for 98.55% of 2,000 references, the rest near-ties).
- **Credentials last an hour:** get a fresh S3 client before each upload (`s3io.write_shard` does).
- **Status polling:** check progress from `shared-m` (69a583ace3bdc8d0), not shared-xl; a poll every couple of minutes keeps a big
  warehouse awake all night.
- **GPU shortage evidence for AWS:** log type, zone, UTC times, asked vs got, the exact error and run ids as they happen; Casey
  takes it to our AWS rep.
