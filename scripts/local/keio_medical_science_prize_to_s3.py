#!/usr/bin/env python3
"""
Keio Medical Science Prize -> S3 Data Pipeline
==============================================

Downloads laureate rows for the Keio Medical Science Prize from the official
Keio University Medical Science Fund website and writes a staging parquet file.

Source authority
----------------
The source is first-party:

    https://www.ms-fund.keio.ac.jp/en/prize/
    https://www.ms-fund.keio.ac.jp/en/prize/list.html

The past-laureates table lists every cohort from 1996 through 2025. Detail
pages are linked for 1999 onward and expose the laureate names, positions /
affiliations, and "Reason for Selection" text. The first three cohorts
(1996-1998) are present on the official roster but do not have detail pages;
those rows keep description and affiliation fields NULL rather than being
backfilled from third-party sources.

Amount handling
---------------
The official 2026 nomination PDF says: "Each laureate receives a certificate
of merit, a medal, and a monetary award of 10 million yen." The same PDF
contains the complete historical laureate roster through 2025, so this ingest
ships JPY 10,000,000 per laureate for all listed rows and documents the rule in
the notebook.

OpenAlex funder mapping
-----------------------
The awarding program is the Keio University Medical Science Fund. OpenAlex has
Keio University as F4320320909 (country JP; no DOI/ROR on the public funder
row). The official prize page states the award ceremony is held at Keio
University and the Fund is a Keio University program, so this ingest maps to
F4320320909.

Output
------
s3://openalex-ingest/awards/keio_medical_science_prize/keio_medical_science_prize_laureates.parquet

Usage
-----
    python keio_medical_science_prize_to_s3.py --skip-upload
    python keio_medical_science_prize_to_s3.py --limit 10 --skip-upload
    python keio_medical_science_prize_to_s3.py --skip-download --skip-upload
    python keio_medical_science_prize_to_s3.py --allow-shrink
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
from urllib.parse import urljoin

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


BASE_URL = "https://www.ms-fund.keio.ac.jp"
PRIZE_URL = f"{BASE_URL}/en/prize/"
LIST_URL = f"{BASE_URL}/en/prize/list.html"
AMOUNT_SOURCE_URL = f"{BASE_URL}/en/news/a4246b3e370ad2125f9f720963fda4963f374cad.pdf"

FUNDER_ID = 4320320909
FUNDER_DISPLAY_NAME = "Keio University"

PROVENANCE = "keio_medical_science_prize"
CURRENCY = "JPY"
AMOUNT_JPY = 10_000_000.0

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/keio_medical_science_prize/keio_medical_science_prize_laureates.parquet"

USER_AGENT = "openalex-walden-keio-medical-science-prize-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.35

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 30) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/pdf,*/*;q=0.8",
        })
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.get(url, timeout=timeout, allow_redirects=True)
    _last_request_t = time.monotonic()
    if "text/html" in resp.headers.get("content-type", "").lower():
        # The Keio site declares UTF-8 in the HTML meta tag but omits charset
        # from the HTTP header; requests otherwise guesses ISO-8859-1.
        resp.encoding = "utf-8-sig"
    return resp


def normalize_space(text: str | None) -> Optional[str]:
    if text is None:
        return None
    cleaned = re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()
    return cleaned or None


def slugify(text: str, max_len: int = 80) -> str:
    text = re.sub(r"\(\*+[^)]*\)", "", text)
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return (text or "unknown")[:max_len].strip("-") or "unknown"


def clean_laureate_name(raw: str) -> tuple[Optional[str], Optional[str]]:
    raw = normalize_space(raw) or ""
    notes = " ".join(re.findall(r"\(\*+[^)]*\)", raw)) or None
    name = normalize_space(re.sub(r"\(\*+[^)]*\)", "", raw))
    return name, notes


def strip_degree_suffix(name: str | None) -> Optional[str]:
    if not name:
        return None
    out = re.sub(r",?\s*(M\.?D\.?|Ph\.?D\.?|D\.?V\.?M\.?|Dr\.?)\.?", "", name, flags=re.I)
    return normalize_space(out)


def split_name(name: str | None) -> tuple[Optional[str], Optional[str]]:
    name = strip_degree_suffix(name)
    if not name:
        return None, None
    parts = name.split()
    if len(parts) == 1:
        return None, parts[0]
    return " ".join(parts[:-1]), parts[-1]


def detail_url_from_row(row) -> Optional[str]:
    hrefs = []
    for a in row.find_all("a", href=True):
        href = a["href"].replace(".//", "./")
        hrefs.append(urljoin(LIST_URL, href))
    return hrefs[0] if hrefs else None


def parse_laureate_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="prizewinners")
    if table is None:
        raise RuntimeError("Could not find table.prizewinners on Keio laureate list")

    rows: list[dict] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 3:
            continue
        year_text = normalize_space(tds[0].get_text(" ", strip=True))
        if not year_text:
            continue
        m = re.search(r"(19|20)\d{2}", year_text)
        if not m:
            continue
        year = int(m.group(0))
        detail_url = detail_url_from_row(tr)
        for position, td in enumerate(tds[1:3], start=1):
            raw_name = normalize_space(td.get_text(" ", strip=True))
            name, notes = clean_laureate_name(raw_name or "")
            if not name:
                continue
            rows.append({
                "source_year": year,
                "laureate_position": position,
                "laureate_name": name,
                "nobel_note": notes,
                "detail_url": detail_url,
                "source_list_url": LIST_URL,
            })
    return rows


def section_text_after_heading(h3) -> dict:
    section = {
        "source_detail_name": normalize_space(h3.get_text(" ", strip=True)),
        "affiliation_raw": None,
        "reason_for_selection": None,
        "comments": None,
    }
    raw_lines: list[str] = []
    for sib in h3.find_next_siblings():
        if getattr(sib, "name", None) == "h3":
            break
        text = sib.get_text("\n", strip=True)
        for line in text.splitlines():
            cleaned = normalize_space(line)
            if cleaned:
                raw_lines.append(cleaned)

    mode = "intro"
    reason_parts: list[str] = []
    comment_parts: list[str] = []
    for line in raw_lines:
        lower = line.lower()
        if lower == "reason for selection":
            mode = "reason"
            continue
        if lower == "background":
            mode = None
            continue
        if lower == "comments":
            mode = "comments"
            continue

        if mode == "intro":
            if section["affiliation_raw"] is None and not lower.startswith("website"):
                section["affiliation_raw"] = line
        elif mode == "reason":
            reason_parts.append(line)
        elif mode == "comments":
            comment_parts.append(line)

    section["reason_for_selection"] = normalize_space(" ".join(reason_parts))
    section["comments"] = normalize_space(" ".join(comment_parts))
    return section


def parse_detail_page(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main") or soup
    return [section_text_after_heading(h3) for h3 in main.find_all("h3")]


def fetch_raw_rows(limit: Optional[int] = None) -> list[dict]:
    print("\n" + "=" * 60)
    print("Step 1: Download Keio Medical Science Prize source pages")
    print("=" * 60)
    resp = _http_get(LIST_URL)
    resp.raise_for_status()
    rows = parse_laureate_table(resp.text)
    print(f"  parsed {len(rows):,} laureate rows from list page")
    if limit:
        rows = rows[:limit]
        print(f"  [LIMIT] keeping first {len(rows):,} rows")

    detail_cache: dict[str, list[dict]] = {}
    for row in rows:
        detail_url = row.get("detail_url")
        if not detail_url or detail_url in detail_cache:
            continue
        detail_resp = _http_get(detail_url)
        detail_resp.raise_for_status()
        detail_cache[detail_url] = parse_detail_page(detail_resp.text)
        print(f"  {detail_url.rsplit('/', 1)[-1]}: {len(detail_cache[detail_url])} detail sections")

    for row in rows:
        detail_url = row.get("detail_url")
        details = detail_cache.get(detail_url or "", [])
        detail = details[row["laureate_position"] - 1] if len(details) >= row["laureate_position"] else {}
        row.update({
            "source_detail_name": strip_degree_suffix(detail.get("source_detail_name")),
            "affiliation_raw": detail.get("affiliation_raw"),
            "reason_for_selection": detail.get("reason_for_selection"),
            "comments": detail.get("comments"),
            "amount_source_url": AMOUNT_SOURCE_URL,
        })
    return rows


def load_cached_rows(output_dir: Path) -> list[dict]:
    raw_path = output_dir / "keio_medical_science_prize_raw.json"
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
    if full_run and len(rows) < 50:
        raise RuntimeError(f"Expected at least 50 Keio laureate rows, got {len(rows):,}")

    out: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        year = int(row["source_year"])
        name = row["laureate_name"]
        given, family = split_name(name)
        award_id = f"keio-medical-science-prize-{year}-{row['laureate_position']}-{slugify(name)}"
        if award_id in seen:
            raise RuntimeError(f"Duplicate funder_award_id would be emitted: {award_id}")
        seen.add(award_id)

        description = row.get("reason_for_selection")
        landing_url = row.get("detail_url") or row.get("source_list_url") or LIST_URL
        out.append({
            "funder_award_id": award_id,
            "display_name": f"Keio Medical Science Prize {year} - {name}",
            "description": description,
            "source_year": str(year),
            "start_date": f"{year}-01-01",
            "end_date": f"{year}-12-31",
            "laureate_name": name,
            "given_name": given,
            "family_name": family,
            "nobel_note": row.get("nobel_note"),
            "laureate_position": str(row["laureate_position"]),
            "source_detail_name": row.get("source_detail_name"),
            "affiliation_raw": row.get("affiliation_raw"),
            "comments": row.get("comments"),
            "amount": str(AMOUNT_JPY),
            "currency": CURRENCY,
            "funder_scheme": "Keio Medical Science Prize",
            "landing_page_url": landing_url,
            "source_list_url": row.get("source_list_url") or LIST_URL,
            "amount_source_url": row.get("amount_source_url") or AMOUNT_SOURCE_URL,
            "provenance": PROVENANCE,
        })

    df = pd.DataFrame.from_records(out)
    print(f"  rows: {len(df):,}")
    print(f"  year range: {df['source_year'].astype(int).min()}-{df['source_year'].astype(int).max()}")
    for col in ["display_name", "source_year", "landing_page_url", "description",
                "affiliation_raw", "amount", "currency"]:
        count = int(df[col].notna().sum())
        print(f"  {col:18s}: {count:,}/{len(df):,} ({count * 100 / len(df):.1f}%)")
    print(f"  unique funder_award_id: {df['funder_award_id'].nunique():,}")
    if df["funder_award_id"].duplicated().any():
        raise RuntimeError("Duplicate funder_award_id values found")

    df = df.astype("string")
    return df


def write_outputs(rows: list[dict], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_path = output_dir / "keio_medical_science_prize_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw rows to {raw_path}")
    parquet_path = output_dir / "keio_medical_science_prize_laureates.parquet"
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

    prev_path = output_dir / "_prev_keio_medical_science_prize.parquet"
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
        print(f"\n[ERROR] Refusing to shrink Keio corpus ({prev_count:,} -> {new_count:,}).")
        return False
    print("    [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path,
                 allow_shrink: bool) -> bool:
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
        description="Download Keio Medical Science Prize laureates and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/keio_medical_science_prize"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse keio_medical_science_prize_raw.json from output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("Keio Medical Science Prize -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Source:     {LIST_URL}")
    print(f"  Amount src: {AMOUNT_SOURCE_URL}")
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
