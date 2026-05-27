#!/usr/bin/env python3
"""
Warren Alpert Foundation Prize -> S3 Data Pipeline
==================================================

Downloads Warren Alpert Foundation Prize recipient rows from the foundation's
official public WordPress endpoint and writes a staging parquet file.

Source authority
----------------
The source is first-party:

    https://www.warrenalpert.org/prize/
    https://www.warrenalpert.org/prize-recipients/
    https://www.warrenalpert.org/wp-json/winners/v1/info

The custom `winners/v1/info` endpoint returns prize cohorts, cohort-level
descriptions, and recipient records with names, bios, positions, years, and
headshot URLs.

Amount handling
---------------
The official prize page states: "The value of the prize is U.S. $500,000 (to
be split equally if more than one recipient is selected)." This script divides
USD 500,000 by the number of recipients in each cohort and emits that per-row
amount. This follows the same shared-prize approach as other prize ingests.

OpenAlex funder mapping
-----------------------
OpenAlex has Warren Alpert Foundation as F4320307125 (no DOI/ROR on the public
funder row). The official site states that the prize is awarded by the Warren
Alpert Foundation and administered in concert with Harvard Medical School, so
the foundation row is the funder.

Output
------
s3://openalex-ingest/awards/warren_alpert_prize/warren_alpert_prize_recipients.parquet
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
import sys as _sys_utf8
try:
    _sys_utf8.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    _sys_utf8.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if _sys_utf8.platform == "win32":
    import builtins as _builtins_utf8
    import pathlib as _pathlib_utf8

    _orig_wt = _pathlib_utf8.Path.write_text
    def _wt(self, data, encoding=None, errors=None, newline=None):
        return _orig_wt(self, data, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.write_text = _wt

    _orig_rt = _pathlib_utf8.Path.read_text
    def _rt(self, encoding=None, errors=None, newline=None):
        return _orig_rt(self, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.read_text = _rt

    _orig_open = _builtins_utf8.open
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open_utf8
# --- end shim ---


API_URL = "https://www.warrenalpert.org/wp-json/winners/v1/info"
PRIZE_URL = "https://www.warrenalpert.org/prize/"
RECIPIENTS_URL = "https://www.warrenalpert.org/prize-recipients/"

FUNDER_ID = 4320307125
FUNDER_DISPLAY_NAME = "Warren Alpert Foundation"

PROVENANCE = "warren_alpert_prize"
CURRENCY = "USD"
COHORT_AMOUNT_USD = 500_000.0

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/warren_alpert_prize/warren_alpert_prize_recipients.parquet"

USER_AGENT = "openalex-walden-warren-alpert-prize-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.35

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 30) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/html,*/*;q=0.8",
        })
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.get(url, timeout=timeout, allow_redirects=True)
    _last_request_t = time.monotonic()
    return resp


def html_to_text(value: str | None) -> Optional[str]:
    if not value:
        return None
    text = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def slugify(text: str, max_len: int = 90) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return (text or "unknown")[:max_len].strip("-") or "unknown"


def year_bounds(label: str) -> tuple[int, int]:
    years = [int(y) for y in re.findall(r"(?:19|20)\d{2}", str(label))]
    if not years:
        raise RuntimeError(f"Could not parse year from cohort label: {label!r}")
    return years[0], years[-1]


def positions_to_text(positions) -> Optional[str]:
    if not isinstance(positions, list):
        return None
    parts = []
    for item in positions:
        if not isinstance(item, dict):
            continue
        title = (item.get("title") or "").strip()
        location = (item.get("location") or "").strip()
        text = " - ".join(p for p in [title, location] if p)
        if text:
            parts.append(text)
    return "; ".join(parts) or None


def first_affiliation(positions) -> Optional[str]:
    if not isinstance(positions, list):
        return None
    for item in positions:
        if not isinstance(item, dict):
            continue
        location = (item.get("location") or "").strip()
        if location:
            return location
    return None


def fetch_raw_rows(limit: Optional[int] = None) -> list[dict]:
    print("\n" + "=" * 60)
    print("Step 1: Download Warren Alpert Prize JSON endpoint")
    print("=" * 60)
    resp = _http_get(API_URL)
    resp.raise_for_status()
    cohorts = resp.json()
    if not isinstance(cohorts, list):
        raise RuntimeError("Expected winners/v1/info to return a list")
    print(f"  parsed {len(cohorts):,} cohorts")

    rows: list[dict] = []
    for cohort in cohorts:
        label = str(cohort.get("label") or cohort.get("value") or "").strip()
        start_year, end_year = year_bounds(label)
        winners = cohort.get("winners") or []
        if not winners:
            continue
        per_recipient_amount = COHORT_AMOUNT_USD / len(winners)
        cohort_description = html_to_text(cohort.get("description"))
        for pos, winner in enumerate(winners, start=1):
            first = (winner.get("first_name") or "").strip()
            last = (winner.get("last_name") or "").strip()
            full_name = " ".join(p for p in [first, last] if p).strip()
            if not full_name:
                full_name = (winner.get("title") or "").strip()
            award_id = "warren-alpert-prize-{}-{}-{}".format(
                label.replace("-", "_"), pos, slugify(full_name)
            )
            rows.append({
                "funder_award_id": award_id,
                "source_id": str(winner.get("id") or ""),
                "cohort_label": label,
                "source_year": str(end_year),
                "start_year_raw": str(start_year),
                "end_year_raw": str(end_year),
                "recipient_position": str(pos),
                "cohort_recipient_count": str(len(winners)),
                "recipient_name": full_name,
                "given_name": first or None,
                "family_name": last or None,
                "display_name": f"Warren Alpert Foundation Prize {label} - {full_name}",
                "description": cohort_description,
                "bio": html_to_text(winner.get("bio")),
                "positions_raw": positions_to_text(winner.get("positions")),
                "affiliation_raw": first_affiliation(winner.get("positions")),
                "headshot_url": winner.get("headshot_url"),
                "amount": str(per_recipient_amount),
                "currency": CURRENCY,
                "funder_scheme": "Warren Alpert Foundation Prize",
                "start_date": f"{start_year}-01-01",
                "end_date": f"{end_year}-12-31",
                "landing_page_url": RECIPIENTS_URL,
                "source_api_url": API_URL,
                "amount_source_url": PRIZE_URL,
                "provenance": PROVENANCE,
            })
            if limit and len(rows) >= limit:
                print(f"  [LIMIT] keeping first {len(rows):,} rows")
                return rows
    print(f"  parsed {len(rows):,} recipient rows")
    return rows


def load_cached_rows(output_dir: Path) -> list[dict]:
    raw_path = output_dir / "warren_alpert_prize_raw.json"
    with raw_path.open("r", encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list):
        raise RuntimeError(f"Cached JSON should be a list of records: {raw_path}")
    print(f"  [OK] loaded {len(rows):,} cached rows from {raw_path}")
    return rows


def normalize_rows(rows: list[dict], full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Normalize rows")
    print("=" * 60)
    if full_run and len(rows) < 80:
        raise RuntimeError(f"Expected at least 80 Warren Alpert rows, got {len(rows):,}")
    df = pd.DataFrame.from_records(rows)
    if df["funder_award_id"].duplicated().any():
        raise RuntimeError("Duplicate funder_award_id values found")
    print(f"  rows: {len(df):,}")
    print(f"  year range: {df['source_year'].astype(int).min()}-{df['source_year'].astype(int).max()}")
    for col in ["display_name", "source_year", "description", "bio", "affiliation_raw",
                "amount", "currency"]:
        count = int(df[col].notna().sum())
        print(f"  {col:18s}: {count:,}/{len(df):,} ({count * 100 / len(df):.1f}%)")
    print(f"  amount tiers: {sorted(df['amount'].dropna().unique(), key=lambda x: float(x))}")
    df = df.astype("string")
    return df


def write_outputs(rows: list[dict], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_path = output_dir / "warren_alpert_prize_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw rows to {raw_path}")
    parquet_path = output_dir / "warren_alpert_prize_recipients.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    print(f"  [OK] wrote {len(df):,} rows ({parquet_path.stat().st_size / 1024:.1f} KB) to {parquet_path}")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook section 1.4: refuse to overwrite S3 with a smaller corpus."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for the runbook section 1.4 shrink-check; "
            "rerun with --skip-upload for local validation."
        ) from exc
    client = boto3.client("s3")
    print(f"  Re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("    no existing parquet: first ingest, no shrink check needed.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest.")
        return True
    prev_path = output_dir / "_prev_warren_alpert_prize.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as exc:
        print(f"    [ERROR] could not read existing parquet ({exc}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)
    print(f"    previous count: {prev_count:,}   new count: {new_count:,}")
    if new_count < prev_count and not allow_shrink:
        print(f"\n[ERROR] Refusing to shrink Warren Alpert corpus ({prev_count:,} -> {new_count:,}).")
        return False
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path,
                 allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 4: Upload to S3")
    print("=" * 60)
    if not check_no_shrink(len(df), allow_shrink, output_dir):
        return False
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    try:
        subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
        print(f"  [OK] uploaded to {s3_uri}")
        return True
    except FileNotFoundError:
        print("[ERROR] aws CLI not found.")
        return False
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] aws s3 cp failed (exit {exc.returncode}).")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Download Warren Alpert Prize recipients.")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/warren_alpert_prize"))
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("Warren Alpert Foundation Prize -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Source:     {API_URL}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.skip_download:
        print("\nStep 1: Reuse cached raw JSON")
        rows = load_cached_rows(args.output_dir)
        if args.limit:
            rows = rows[:args.limit]
            print(f"  [LIMIT] keeping first {len(rows):,} cached rows")
    else:
        rows = fetch_raw_rows(limit=args.limit)

    df = normalize_rows(rows, full_run=args.limit is None)
    parquet_path = write_outputs(rows, df, args.output_dir)
    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading to S3.")
        print(f"Local parquet ready: {parquet_path}")
        return
    ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
