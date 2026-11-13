"""Shard I/O for a Modal GPU batch that reads and writes one S3 prefix through a per-job IAM role (role.yaml).

Layout under s3://<bucket>/<prefix>/:  in/shard=S/*.parquet (written by export.py)  ->  out/shard=S/part-0.parquet (written here).
Credentials come from the container's MODAL_IDENTITY_TOKEN (AssumeRoleWithWebIdentity); nothing is stored. Sessions last
an hour, so call client() again before a long upload. Runbook: docs/runbooks/modal-gpu-batch-via-s3.md
"""
import io
import os
import time


def client(role_arn, session="modal-s3-batch"):
    import boto3
    c = boto3.client("sts", region_name="us-east-1").assume_role_with_web_identity(
        RoleArn=role_arn, RoleSessionName=f"{session}-{int(time.time())}", WebIdentityToken=os.environ["MODAL_IDENTITY_TOKEN"],
        DurationSeconds=3600)["Credentials"]
    return boto3.client("s3", region_name="us-east-1", aws_access_key_id=c["AccessKeyId"],
                        aws_secret_access_key=c["SecretAccessKey"], aws_session_token=c["SessionToken"])


def out_key(prefix, shard):
    return f"{prefix}/out/shard={shard}/part-0.parquet"


def done(c, bucket, prefix, shard):
    """True if this shard's output exists (a rerun skips it)."""
    try:
        c.head_object(Bucket=bucket, Key=out_key(prefix, shard))
        return True
    except c.exceptions.ClientError:
        return False


def read_shard(c, bucket, prefix, shard, columns=None):
    """All input parquet files of one shard as one pyarrow Table (empty table if none)."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    keys = [o["Key"] for page in c.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/in/shard={shard}/")
            for o in page.get("Contents", []) if o["Key"].endswith(".parquet")]
    tabs = [pq.read_table(io.BytesIO(c.get_object(Bucket=bucket, Key=k)["Body"].read()), columns=columns) for k in keys]
    return pa.concat_tables(tabs) if tabs else None


def write_shard(role_arn, bucket, prefix, shard, table):
    """Write one shard's output (zstd parquet) with fresh credentials."""
    import pyarrow.parquet as pq
    local = f"/tmp/shard_{shard}.parquet"
    pq.write_table(table, local, compression="zstd")
    client(role_arn).upload_file(local, bucket, out_key(prefix, shard))
    os.remove(local)


def parse_ranges(arg):
    """'0-7,64-71' -> [0..7, 64..71]"""
    out = []
    for part in str(arg).split(","):
        a, _, b = part.partition("-")
        out += list(range(int(a), int(b or a) + 1))
    return out


def whoami_claims():
    """The container's OIDC claims; the role's trust needs the workspace id from `sub`."""
    import base64
    import json
    tok = os.environ.get("MODAL_IDENTITY_TOKEN")
    if not tok:
        return {"error": "no MODAL_IDENTITY_TOKEN in this container"}
    p = tok.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(p + "=" * (-len(p) % 4)))
