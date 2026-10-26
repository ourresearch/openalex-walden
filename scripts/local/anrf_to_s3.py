#!/usr/bin/env python3
"""
Arthritis National Research Foundation (ANRF, US) to S3 Data Pipeline
=====================================================================

Source: ANRF's own master list of every grant it has made, 1973-2026 (285 grants to 273 scholars,
USD 35.96M), sent to OpenAlex by Laura Douglas, ANRF Senior Manager of Grants and Impact, on
2026-10-26 ("Master List of Grants (2).xlsx"). ANRF's website (curearthritis.org scientist database
and scientist spotlight pages, 2013-2026 cards + 1990s-2012 legacy pages) lists a subset of the same
grants; it adds no grant that is missing from the list. `build_awards.py` merges the two into
`anrf_awards.csv` (sheet authoritative; website adds bio URL, named fellowship, research/lay summary and
two documented corrections). This script only converts that CSV to an all-string parquet and uploads it.

funder_award_id: ANRF publishes no grant numbers (applications run through ProposalCentral; no number
is public), so the key is the synthetic ANRF-{start year}-{grant code}-{LEAD FAMILY}-{given initial},
e.g. ANRF-2026-ALL-WOBMA-H. Checked 2026-10-26: unique, and 0 collisions with any id in
openalex_awards_raw / openalex_awards (so 0 citation stubs collapse onto these awards).

Output: s3://openalex-ingest/awards/anrf/anrf_projects.parquet

Usage (from this folder):
  .venv/bin/python scripts/anrf_to_s3.py --skip-upload          # build + check the parquet only
  .venv/bin/python scripts/anrf_to_s3.py                        # build, shrink-check, upload (needs AWS creds)
"""
import argparse
import csv
import re
import sys
from datetime import datetime
from pathlib import Path

# --- UTF-8 shim (runbook §1.2; grep anchor: sys.stdout.reconfigure) ---
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

import pyarrow as pa
import pyarrow.parquet as pq

BASE = Path(__file__).resolve().parent.parent
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/anrf/anrf_projects.parquet"
ID_SHAPE = re.compile(r"^ANRF-(19|20)\d{2}-[A-Z]+-[A-Z]+-[A-Z](-\d+)?$")
REQUIRED = ["funder_award_id", "title", "start_year", "end_year", "grant_code", "funder_scheme",
            "lead_given_name", "lead_family_name", "institution", "amount", "excluded"]


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def main():
    p = argparse.ArgumentParser(description="ANRF master grants list -> parquet -> S3")
    p.add_argument("--input", type=Path, default=BASE / "anrf_awards.csv")
    p.add_argument("--output-dir", type=Path, default=BASE / "out")
    p.add_argument("--limit", type=int, default=None, help="first N rows only (smoke test; never upload a limited file)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    rows = list(csv.DictReader(args.input.open(newline="", encoding="utf-8")))
    if args.limit:
        rows = rows[: args.limit]
    cols = list(rows[0].keys())
    missing = [c for c in REQUIRED if c not in cols]
    if missing:
        raise SystemExit(f"input lacks columns {missing}")

    ids = [r["funder_award_id"] for r in rows]
    if len({i.lower() for i in ids}) != len(ids):
        raise SystemExit("duplicate funder_award_id")
    bad = [i for i in ids if not ID_SHAPE.match(i)]
    if bad:
        raise SystemExit(f"funder_award_id shape: {bad[:5]}")
    for r in rows:
        if r["amount"] and float(r["amount"]) <= 0:
            raise SystemExit(f"non-positive amount on {r['funder_award_id']}")
        if not (1970 <= int(r["start_year"]) <= int(r["end_year"]) <= 2030):
            raise SystemExit(f"bad years on {r['funder_award_id']}")

    kept = [r for r in rows if r["excluded"] == "0"]
    log(f"{len(rows)} rows, {len(kept)} to ship; USD {sum(float(r['amount'] or 0) for r in kept):,.2f}")
    for c in ["title", "amount", "lead_family_name", "institution", "description", "landing_page_url", "named_award"]:
        log(f"  {c:18s} {sum(1 for r in kept if r[c]) / len(kept):6.1%}")

    # all columns as strings (runbook §1.2 item 5); empty -> null
    table = pa.table({c: pa.array([(r[c] if r[c] != "" else None) for r in rows], type=pa.string()) for c in cols})
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "anrf_projects.parquet"
    pq.write_table(table, parquet_path)
    log(f"Wrote {table.num_rows} rows x {table.num_columns} cols to {parquet_path}")

    if args.skip_upload:
        return
    if args.limit:
        raise SystemExit("refusing to upload a --limit file")
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_anrf_projects.parquet"
    try:  # runbook §1.4: never shrink the corpus on re-ingest
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev = pq.read_table(previous).num_rows
        log(f"Shrink check: previous {prev}, new {table.num_rows}")
        if table.num_rows < prev and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev} -> {table.num_rows}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
