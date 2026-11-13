"""Step 2: write the input rows of some shards as parquet to s3://<bucket>/<prefix>/in/shard=S/ for the Modal app.

The source table needs an INT `shard` column (e.g. pmod(xxhash64(key), 1024)) so every step agrees on what a shard is. The
export goes through an external parquet table at the prefix (the bucket's Unity Catalog external location makes that possible);
shards already exported are skipped.

    python export.py --source openalex.transient.myjob_input --cols text_hash,text \
        --staging openalex.transient.myjob_modal_in --s3 s3://openalex-sandbox/myjob/embed --shards 0-255
"""
import argparse

from dbsql import sql
from s3io import parse_ranges

ap = argparse.ArgumentParser()
ap.add_argument("--source", required=True)
ap.add_argument("--cols", required=True, help="columns to send, comma-separated (keep the key)")
ap.add_argument("--staging", required=True, help="external table to create at <s3>/in (dropped at cleanup)")
ap.add_argument("--s3", required=True, help="s3://bucket/prefix (no trailing slash)")
ap.add_argument("--shards", required=True)
a = ap.parse_args()

want = sorted(set(parse_ranges(a.shards)))
schema, name = a.staging.rsplit(".", 1)
exists = bool(sql(f"SHOW TABLES IN {schema} LIKE '{name}'"))
have = {int(r["shard"]) for r in sql(f"SELECT DISTINCT shard FROM {a.staging}")} if exists else set()
todo = [s for s in want if s not in have]
print(f"{len(want)} shards asked, {len(set(want) & have)} already exported, {len(todo)} to write", flush=True)
if todo:
    cond = f"shard IN ({', '.join(map(str, todo))})"
    if not exists:
        sql(f"""CREATE TABLE {a.staging} USING PARQUET PARTITIONED BY (shard) LOCATION '{a.s3}/in'
COMMENT 'Modal GPU batch input (scripts/modal_s3_batch); drop after the run'
AS SELECT {a.cols}, shard FROM {a.source} WHERE {cond}""", write=True)
    else:
        sql(f"INSERT INTO {a.staging} SELECT {a.cols}, shard FROM {a.source} WHERE {cond}", write=True)
print(sql(f"SELECT count(DISTINCT shard) shards, count(*) rows FROM {a.staging} WHERE shard IN ({', '.join(map(str, want))})"))
