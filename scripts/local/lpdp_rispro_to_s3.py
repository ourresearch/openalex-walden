#!/usr/bin/env python3
"""
LPDP RISPRO grantees -> S3 Data Pipeline
========================================

Downloads RISPRO research grantee/project records from LPDP's official public
Gatsby page-data JSON and writes a parquet file for the OpenAlex awards
pipeline.

Source authority
----------------
The source is first-party. Lembaga Pengelola Dana Pendidikan (LPDP) publishes
the RISPRO grantee list at:

    https://lpdp.kemenkeu.go.id/grantees/daftar-grantees/

The Gatsby page data behind that official page exposes the full structured
list, including rows not visible on the first SSR page:

    https://lpdp.kemenkeu.go.id/page-data/grantees/daftar-grantees/page-data.json

Amount handling
---------------
The page-data source publishes project title, head of research, institution,
research focus, RISPRO category, and contract year. It does not publish exact
per-project amounts or currencies. This pipeline therefore preserves the
source fields and intentionally leaves OpenAlex amount/currency NULL for a
section 6.7 waiver in the notebook and PR.

Output
------
s3://openalex-ingest/awards/lpdp_rispro/lpdp_rispro_grantees.parquet

Usage
-----
    python lpdp_rispro_to_s3.py --skip-upload
    python lpdp_rispro_to_s3.py --limit 10 --skip-upload
    python lpdp_rispro_to_s3.py --skip-download --skip-upload
    python lpdp_rispro_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import subprocess
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests

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

    def _open_utf8(
        file, mode="r", buffering=-1, encoding=None, errors=None,
        newline=None, closefd=True, opener=None,
    ):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)

    _builtins_utf8.open = _open_utf8
# --- end shim ---


# =============================================================================
# Configuration
# =============================================================================

SOURCE_PAGE_URL = "https://lpdp.kemenkeu.go.id/grantees/daftar-grantees/"
PAGE_DATA_URL = "https://lpdp.kemenkeu.go.id/page-data/grantees/daftar-grantees/page-data.json"

FUNDER_ID = 4320328515
FUNDER_DISPLAY_NAME = "Lembaga Pengelola Dana Pendidikan"
PROVENANCE = "lpdp_rispro_grantees"
FUNDER_SCHEME_PREFIX = "RISPRO"

AMOUNT_DECISION = (
    "OpenAlex amount/currency intentionally NULL: the official LPDP grantee "
    "page-data publishes no exact per-project amount or currency."
)

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/lpdp_rispro/lpdp_rispro_grantees.parquet"

USER_AGENT = "openalex-walden-lpdp-rispro-ingest/1.0 (+https://openalex.org)"
REQUEST_DELAY_S = 0.2
EXPECTED_MIN_FULL_ROWS = 700


_session: Optional[requests.Session] = None
_last_request_t = 0.0


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = html.unescape(str(value))
    text = re.sub(r"\s+", " ", text).strip(" \t\r\n\u00a0")
    return text or None


def slugify(value: str, max_len: int = 70) -> str:
    ascii_text = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_text.lower()).strip("-")
    slug = re.sub(r"-+", "-", slug)
    return slug[:max_len].strip("-") or "unknown"


def source_hash(parts: list[Optional[str]], length: int = 12) -> str:
    payload = "\u001f".join((part or "").strip().lower() for part in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:length]


def split_person_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    text = clean_text(name)
    if not text:
        return None, None
    text = re.sub(r",.*$", "", text)
    prefix_re = re.compile(
        r"^(Prof\\.?|Professor|Dr\\.?|dr\\.?|Ir\\.?|Apt\\.?|Apt|H\\.?|Hj\\.?)\\s+",
        flags=re.IGNORECASE,
    )
    previous = None
    while previous != text:
        previous = text
        text = prefix_re.sub("", text).strip()
    tokens = [t.strip(",") for t in text.split() if t.strip(",")]
    suffixes = {"PhD", "MD", "MSc", "M.Si", "M.T", "S.T", "DPhil", "Jr.", "Sr.", "II", "III", "IV"}
    while tokens and tokens[-1].rstrip(".") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def json_string(value: Any) -> Optional[str]:
    if value in (None, "", [], {}):
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/html,application/xhtml+xml,*/*",
            "Accept-Language": "id,en-US;q=0.9,en;q=0.8",
        })
    return _session


def http_get(url: str, *, timeout: int = 60, max_attempts: int = 4) -> requests.Response:
    global _last_request_t
    session = get_session()
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < REQUEST_DELAY_S:
            time.sleep(REQUEST_DELAY_S - elapsed)
        try:
            response = session.get(url, timeout=timeout)
            _last_request_t = time.monotonic()
            log(f"GET {response.url} -> HTTP {response.status_code} ({len(response.content):,} bytes)")
            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 20)
                log(f"  retrying in {sleep_s}s after transient HTTP {response.status_code}")
                time.sleep(sleep_s)
                continue
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 20)
            log(f"  request failed ({exc}); retrying in {sleep_s}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def fetch_records(limit: Optional[int] = None) -> tuple[list[dict[str, Any]], dict[str, Optional[str]]]:
    print("\n" + "=" * 60)
    print("Step 1: Download LPDP RISPRO grantee records")
    print("=" * 60)

    page_resp = http_get(SOURCE_PAGE_URL)
    page_text = clean_text(re.sub(r"<[^>]+>", " ", page_resp.text)) or ""
    if "Daftar Grantee" not in page_text or "Ketua Riset" not in page_text:
        raise RuntimeError("Official LPDP grantee page did not contain expected visible labels")

    data_resp = http_get(PAGE_DATA_URL)
    payload = data_resp.json()
    try:
        grantee_blob = payload["result"]["data"]["granteeListOfGrantees"]
        records = grantee_blob["grantees"]
        page_setting = grantee_blob.get("page_setting") or {}
    except (KeyError, TypeError) as exc:
        raise RuntimeError("LPDP page-data JSON did not contain expected granteeListOfGrantees shape") from exc

    if not isinstance(records, list):
        raise RuntimeError(f"Expected grantees list from page-data JSON, got {type(records).__name__}")
    log(f"Page-data returned {len(records):,} RISPRO grantee records")

    if limit:
        records = records[:limit]
        log(f"[LIMIT] keeping first {len(records):,} records")
    if not records:
        raise RuntimeError("No RISPRO records returned from LPDP page-data")

    notes = {
        "source_page_url": SOURCE_PAGE_URL,
        "page_data_url": PAGE_DATA_URL,
        "source_page_title": clean_text(page_setting.get("title")) or "Daftar Grantee",
        "source_banner_image": clean_text(page_setting.get("banner_image")),
        "amount_decision": AMOUNT_DECISION,
    }
    return records, notes


def normalize_records(
    records: list[dict[str, Any]],
    notes: dict[str, Optional[str]],
    *,
    full_run: bool,
) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Normalize records")
    print("=" * 60)

    retrieved_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Optional[str]]] = []

    for rec in records:
        contract_year = clean_text(rec.get("contract_year"))
        category = clean_text(rec.get("rispro_category"))
        title = clean_text(rec.get("proposal_title"))
        head_name = clean_text(rec.get("head_of_research_name"))
        institution = clean_text(rec.get("institution"))
        research_focus = clean_text(rec.get("research_focus"))

        if not title:
            raise ValueError(f"Record missing proposal_title: {rec!r}")

        row_hash = source_hash([contract_year, category, title, head_name, institution, research_focus])
        title_slug = slugify(title)
        given_name, family_name = split_person_name(head_name)
        start_date = f"{contract_year}-01-01" if contract_year and re.match(r"^\d{4}$", contract_year) else None
        description = clean_text(
            f"{category or 'RISPRO'}; Research focus: {research_focus or 'not published'}; "
            f"Head of research: {head_name or 'not published'}; Institution: {institution or 'not published'}."
        )

        rows.append({
            "funder_award_id": f"lpdp-rispro-{contract_year or 'unknown'}-{title_slug}-{row_hash}",
            "source_row_hash": row_hash,
            "display_name": title,
            "description": description,
            "head_of_research_name": head_name,
            "lead_investigator_given_name": given_name,
            "lead_investigator_family_name": family_name,
            "institution": institution,
            "research_focus": research_focus,
            "rispro_category": category,
            "contract_year": contract_year,
            "start_date": start_date,
            "start_year": contract_year if contract_year and re.match(r"^\d{4}$", contract_year) else None,
            "funding_type": "research",
            "funder_scheme": category,
            "amount": None,
            "currency": None,
            "amount_decision": notes.get("amount_decision"),
            "landing_page_url": notes.get("source_page_url"),
            "source_page_url": notes.get("source_page_url"),
            "page_data_url": notes.get("page_data_url"),
            "source_page_title": notes.get("source_page_title"),
            "source_banner_image": notes.get("source_banner_image"),
            "created_at": clean_text(rec.get("created_at")),
            "raw_record_json": json_string(rec),
            "retrieved_at": retrieved_at,
        })

    df = pd.DataFrame(rows)
    validate_dataframe(df, full_run=full_run)

    # Runbook section 1.2: force Spark-friendly strings before parquet write.
    df = df.astype("string")
    return df


def validate_dataframe(df: pd.DataFrame, *, full_run: bool) -> None:
    total = len(df)
    if total == 0:
        raise RuntimeError("No LPDP RISPRO rows after normalization")

    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")

    if full_run and total < EXPECTED_MIN_FULL_ROWS:
        raise RuntimeError(
            f"Full LPDP RISPRO run returned only {total:,} rows; expected at least "
            f"{EXPECTED_MIN_FULL_ROWS:,}."
        )

    required_cols = [
        "display_name",
        "description",
        "head_of_research_name",
        "institution",
        "research_focus",
        "rispro_category",
        "contract_year",
        "landing_page_url",
    ]
    for col in required_cols:
        coverage = df[col].notna().mean()
        print(f"  {col:24s}: {df[col].notna().sum():,}/{total:,} ({coverage * 100:.1f}%)")
        if coverage < 0.95:
            raise RuntimeError(f"Unexpectedly low coverage for {col}: {coverage * 100:.1f}%")

    amount_coverage = df["amount"].notna().mean()
    print(f"  {'amount':24s}: {df['amount'].notna().sum():,}/{total:,} ({amount_coverage * 100:.1f}%)")
    print(f"  {'currency':24s}: {df['currency'].notna().sum():,}/{total:,} ({df['currency'].notna().mean() * 100:.1f}%)")
    print("  Amount decision: exact per-project amounts are not published in the official page-data.")
    print(f"  Year range: {df['start_year'].min()} - {df['start_year'].max()}")
    print(f"  RISPRO category distribution: {df['rispro_category'].value_counts(dropna=False).to_dict()}")
    print(f"  Top research focus values: {df['research_focus'].value_counts(dropna=False).head(20).to_dict()}")


def write_outputs(
    records: list[dict[str, Any]],
    notes: dict[str, Optional[str]],
    df: pd.DataFrame,
    output_dir: Path,
) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "lpdp_rispro_grantees_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump({"source_notes": notes, "records": records}, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw page-data payload to {raw_path}")

    parquet_path = output_dir / "lpdp_rispro_grantees.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df):,} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def load_cached_payload(output_dir: Path) -> tuple[list[dict[str, Any]], dict[str, Optional[str]]]:
    raw_path = output_dir / "lpdp_rispro_grantees_raw.json"
    if not raw_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {raw_path}")
    with raw_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, list):
        records = payload
        notes = {
            "source_page_url": SOURCE_PAGE_URL,
            "page_data_url": PAGE_DATA_URL,
            "source_page_title": "Daftar Grantee",
            "source_banner_image": None,
            "amount_decision": AMOUNT_DECISION,
        }
    else:
        records = payload.get("records")
        notes = payload.get("source_notes") or {}
    if not isinstance(records, list):
        raise RuntimeError(f"Cached JSON should contain a records list: {raw_path}")
    print(f"  [OK] loaded {len(records):,} cached records from {raw_path}")
    return records, notes


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

    prev_path = output_dir / "_prev_lpdp_rispro_grantees.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as exc:
        print(f"    [ERROR] could not read existing parquet ({exc}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)

    print(f"    previous count: {prev_count:,}   new count: {new_count:,}")
    if new_count < prev_count:
        if allow_shrink:
            print("    [OVERRIDE] new corpus is smaller but --allow-shrink was set.")
            return True
        print(f"\n[ERROR] Refusing to shrink LPDP RISPRO corpus ({prev_count:,} -> {new_count:,}).")
        return False
    print("    [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path, allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 4: Upload to S3")
    print("=" * 60)
    if not check_no_shrink(len(df), allow_shrink, output_dir):
        return False

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  Uploading {parquet_path} -> {s3_uri}")
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
    parser = argparse.ArgumentParser(
        description="Download LPDP RISPRO grantee records and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/lpdp_rispro"))
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for smoke testing")
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Reuse lpdp_rispro_grantees_raw.json from output-dir",
    )
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("LPDP RISPRO grantees -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Page data:  {PAGE_DATA_URL}")
    print(f"  Source page:{SOURCE_PAGE_URL}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        print("\nStep 1: Reuse cached raw JSON")
        records, notes = load_cached_payload(args.output_dir)
        if args.limit:
            records = records[:args.limit]
            print(f"  [LIMIT] keeping first {len(records):,} cached records")
    else:
        records, notes = fetch_records(limit=args.limit)

    df = normalize_records(records, notes, full_run=args.limit is None)
    parquet_path = write_outputs(records, notes, df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading to S3.")
        print(f"Local parquet ready: {parquet_path}")
        return

    ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
