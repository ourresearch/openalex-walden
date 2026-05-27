#!/usr/bin/env python3
"""
Heineken Prizes -> S3 Data Pipeline
===================================

Downloads laureate rows for the international Heineken Prizes and the
Heineken Young Scientists Awards from the official Heineken Prizes WordPress
API, normalizes them to string-typed parquet, and optionally uploads the file
to S3 for the Databricks notebook.

Source authority
----------------
The source is first-party Heineken Prizes:

    https://www.heinekenprizes.org/list-of-laureates/
    https://www.heinekenprizes.org/wp-json/wp/v2/avada_portfolio
    https://www.heinekenprizes.org/heineken-prizes/
    https://www.heinekenprizes.org/heineken-young-scientists-awards/

The "List of Laureates" page is rendered from Avada portfolio records. The
portfolio category taxonomy separates real laureate records from interviews,
jury pages, and other editorial content.

OpenAlex funder mapping
-----------------------
The Heineken Prizes are awarded under the responsibility of the Royal
Netherlands Academy of Arts and Sciences (KNAW), which exists in OpenAlex as
F4320320934. The source site names the Alfred Heineken funds as the financial
source, but OpenAlex has no separate Heineken prize-fund funder row; KNAW is
the awarding/selection body and is the auditable OpenAlex funder for this
ingest.

Amount handling
---------------
The official prize-information pages state the current public rules:

* scientific Heineken Prizes: USD 250,000
* Dr A.H. Heineken Prize for Art: EUR 100,000 total, half reserved for a
  publication/exhibition or similar project
* Heineken Young Scientists Awards: EUR 15,000

The laureate archive does not expose a historical per-row amount field, so the
script applies those official program rules consistently and documents that
choice in the notebook.

Output
------
s3://openalex-ingest/awards/heineken_prizes/heineken_prizes_laureates.parquet
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


BASE_URL = "https://www.heinekenprizes.org"
WP_API = f"{BASE_URL}/wp-json/wp/v2"
LIST_URL = f"{BASE_URL}/list-of-laureates/"
HEINEKEN_PRIZES_URL = f"{BASE_URL}/heineken-prizes/"
HYSA_URL = f"{BASE_URL}/heineken-young-scientists-awards/"

FUNDER_ID = 4320320934
FUNDER_DISPLAY_NAME = "Koninklijke Nederlandse Akademie van Wetenschappen"
PROVENANCE = "heineken_prizes_wp"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/heineken_prizes/heineken_prizes_laureates.parquet"

USER_AGENT = "openalex-walden-heineken-prizes-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25

HEINEKEN_PRIZE_CATEGORY_IDS = {
    12: "Biochemistry and Biophysics",
    13: "Art",
    14: "Medicine",
    15: "History",
    16: "Environmental Sciences",
    17: "Cognitive Science",
}

HYSA_CATEGORY_IDS = {
    301: "Humanities",
    302: "Social Sciences",
    303: "Medical/Biomedical Sciences",
    304: "Natural Sciences",
}

ALL_AWARD_CATEGORY_IDS = set(HEINEKEN_PRIZE_CATEGORY_IDS) | set(HYSA_CATEGORY_IDS)

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


def get_json(url: str) -> object:
    resp = _http_get(url)
    resp.raise_for_status()
    return resp.json()


def clean_text(value: str | None) -> Optional[str]:
    if value is None:
        return None
    value = value.replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def html_to_text(value: str | None) -> Optional[str]:
    if not value:
        return None
    if "<" not in value and ">" not in value:
        return clean_text(value)
    return clean_text(BeautifulSoup(value, "html.parser").get_text(" ", strip=True))


def slugify(text: str, max_len: int = 90) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return (text or "unknown")[:max_len].strip("-") or "unknown"


def split_name(name: str | None) -> tuple[Optional[str], Optional[str]]:
    name = clean_text(name)
    if not name:
        return None, None
    name = re.sub(r"^(Sir|Dame|Prof\.?|Professor|Dr\.?)\s+", "", name, flags=re.I)
    parts = name.split()
    if len(parts) == 1:
        return None, parts[0]
    if len(parts) >= 4 and " ".join(p.lower() for p in parts[-3:-1]) == "van der":
        return " ".join(parts[:-3]), " ".join(parts[-3:])
    if len(parts) >= 3 and parts[-2].lower() in {"de", "le", "ten", "ter", "van", "von"}:
        return " ".join(parts[:-2]), " ".join(parts[-2:])
    return " ".join(parts[:-1]), parts[-1]


def fetch_paginated(endpoint: str, params: Optional[dict] = None) -> list[dict]:
    params = dict(params or {})
    params.setdefault("per_page", 100)
    rows: list[dict] = []
    page = 1
    total_pages = None
    while True:
        url = f"{WP_API}/{endpoint}?per_page={params['per_page']}&page={page}"
        for key, value in params.items():
            if key not in {"per_page", "page"}:
                url += f"&{key}={value}"
        resp = _http_get(url)
        if resp.status_code == 400 and page > 1:
            break
        resp.raise_for_status()
        if total_pages is None:
            total_pages = int(resp.headers.get("X-WP-TotalPages", "1") or "1")
            total = int(resp.headers.get("X-WP-Total", "0") or "0")
            print(f"  {endpoint}: {total:,} records across {total_pages:,} page(s)")
        batch = resp.json()
        if not isinstance(batch, list):
            raise RuntimeError(f"Expected list response from {url}")
        rows.extend(batch)
        print(f"    page {page:,}/{total_pages:,}: {len(batch):,} records")
        if page >= total_pages:
            break
        page += 1
    return rows


def load_category_map() -> dict[int, dict]:
    categories = fetch_paginated("portfolio_category")
    return {
        int(row["id"]): {
            "slug": row.get("slug"),
            "name": html_to_text(row.get("name")),
            "count": row.get("count"),
        }
        for row in categories
    }


def infer_award_family(category_ids: set[int]) -> Optional[str]:
    if category_ids & set(HEINEKEN_PRIZE_CATEGORY_IDS):
        return "heineken_prize"
    if category_ids & set(HYSA_CATEGORY_IDS):
        return "heineken_young_scientists_award"
    return None


def infer_field(category_ids: set[int], category_map: dict[int, dict]) -> Optional[str]:
    for category_id in sorted(category_ids):
        if category_id in HEINEKEN_PRIZE_CATEGORY_IDS:
            return category_map.get(category_id, {}).get("name") or HEINEKEN_PRIZE_CATEGORY_IDS[category_id]
        if category_id in HYSA_CATEGORY_IDS:
            return category_map.get(category_id, {}).get("name") or HYSA_CATEGORY_IDS[category_id]
    return None


def parse_scheme_and_year(excerpt: str) -> tuple[str, int]:
    excerpt = clean_text(excerpt) or ""
    match = re.search(r"\b((?:19|20)\d{2})\s*$", excerpt)
    if not match:
        match = re.search(r"\b((?:19|20)\d{2})\b", excerpt)
    if not match:
        raise RuntimeError(f"Could not parse award year from excerpt: {excerpt!r}")
    year = int(match.group(1))
    scheme = clean_text(excerpt[:match.start()]) or excerpt
    return scheme, year


def amount_for_row(award_family: str, field: str | None) -> tuple[str, str, str]:
    if award_family == "heineken_young_scientists_award":
        return "15000.0", "EUR", HYSA_URL
    if field == "Art":
        return "100000.0", "EUR", HEINEKEN_PRIZES_URL
    return "250000.0", "USD", HEINEKEN_PRIZES_URL


def normalize_portfolio_record(record: dict, category_map: dict[int, dict]) -> Optional[dict]:
    category_ids = set(int(c) for c in (record.get("portfolio_category") or []))
    if not category_ids & ALL_AWARD_CATEGORY_IDS:
        return None
    award_family = infer_award_family(category_ids)
    if not award_family:
        return None
    name = html_to_text(record.get("title", {}).get("rendered"))
    excerpt = html_to_text(record.get("excerpt", {}).get("rendered"))
    content = html_to_text(record.get("content", {}).get("rendered"))
    if not name or not excerpt:
        raise RuntimeError(f"Missing title/excerpt on portfolio record {record.get('id')}")
    scheme, year = parse_scheme_and_year(excerpt)
    field = infer_field(category_ids, category_map)
    amount, currency, amount_source_url = amount_for_row(award_family, field)
    given, family = split_name(name)
    post_id = str(record.get("id"))
    native_id = f"heineken-{year}-{slugify(scheme)}-{slugify(name)}"
    return {
        "funder_award_id": native_id,
        "source_post_id": post_id,
        "source_post_slug": record.get("slug"),
        "display_name": f"{scheme} {year} - {name}",
        "description": content,
        "source_year": str(year),
        "recipient_name": name,
        "given_name": given,
        "family_name": family,
        "award_family": award_family,
        "funder_scheme": scheme,
        "research_field": field,
        "amount": amount,
        "currency": currency,
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "landing_page_url": record.get("link"),
        "source_list_url": LIST_URL,
        "amount_source_url": amount_source_url,
        "provenance": PROVENANCE,
    }


def fetch_raw_rows(limit: Optional[int] = None) -> list[dict]:
    print("\n" + "=" * 60)
    print("Step 1: Download official Heineken Prizes WordPress records")
    print("=" * 60)
    category_map = load_category_map()
    portfolio_records = fetch_paginated("avada_portfolio")
    rows: list[dict] = []
    for record in portfolio_records:
        normalized = normalize_portfolio_record(record, category_map)
        if normalized:
            rows.append(normalized)
    rows = sorted(rows, key=lambda r: (int(r["source_year"]), r["funder_scheme"], r["recipient_name"]))
    print(f"  filtered award/laureate rows: {len(rows):,}")
    if limit:
        rows = rows[:limit]
        print(f"  [LIMIT] keeping first {len(rows):,} rows")
    return rows


def load_cached_rows(output_dir: Path) -> list[dict]:
    raw_path = output_dir / "heineken_prizes_raw.json"
    with raw_path.open("r", encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list):
        raise RuntimeError(f"Cached JSON should be a list of records: {raw_path}")
    print(f"  [OK] loaded {len(rows):,} cached rows from {raw_path}")
    return rows


def normalize_rows(rows: list[dict], full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Validate and normalize rows")
    print("=" * 60)
    if full_run and len(rows) < 140:
        raise RuntimeError(f"Expected at least 140 Heineken laureate rows, got {len(rows):,}")
    seen: set[str] = set()
    for row in rows:
        award_id = row.get("funder_award_id")
        if not award_id:
            raise RuntimeError("Missing funder_award_id")
        if award_id in seen:
            raise RuntimeError(f"Duplicate funder_award_id would be emitted: {award_id}")
        seen.add(award_id)
    df = pd.DataFrame.from_records(rows)
    print(f"  rows: {len(df):,}")
    print(f"  year range: {df['source_year'].astype(int).min()}-{df['source_year'].astype(int).max()}")
    for col in ["display_name", "source_year", "recipient_name", "funder_scheme",
                "research_field", "description", "amount", "currency", "landing_page_url"]:
        count = int(df[col].notna().sum())
        print(f"  {col:18s}: {count:,}/{len(df):,} ({count * 100 / len(df):.1f}%)")
    print(f"  unique funder_award_id: {df['funder_award_id'].nunique():,}")
    print("  award families:")
    print(df["award_family"].value_counts().to_string())
    print("  currencies:")
    print(df["currency"].value_counts().to_string())
    df = df.astype("string")
    return df


def write_outputs(rows: list[dict], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_path = output_dir / "heineken_prizes_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw rows to {raw_path}")
    parquet_path = output_dir / "heineken_prizes_laureates.parquet"
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
    prev_path = output_dir / "_prev_heineken_prizes.parquet"
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
        print(f"\n[ERROR] Refusing to shrink Heineken corpus ({prev_count:,} -> {new_count:,}).")
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
    parser = argparse.ArgumentParser(description="Download Heineken Prizes laureates.")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/heineken_prizes"))
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("Heineken Prizes -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Source:     {LIST_URL}")
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
