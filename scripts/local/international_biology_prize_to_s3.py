#!/usr/bin/env python3
"""
International Prize for Biology -> S3 Data Pipeline
===================================================

Downloads recipient rows for the International Prize for Biology from the
Japan Society for the Promotion of Science (JSPS) official English pages and
writes a staging parquet file.

Source authority
----------------
The source is first-party JSPS:

    https://www.jsps.go.jp/english/e-biol/02_recipients.html
    https://www.jsps.go.jp/english/e-biol/01_outline.html

The public recipient table currently lists 1985-2023. Official detail pages
for 2024 and 2025 already exist at:

    https://www.jsps.go.jp/english/e-biol/02_recipients/awardee2024.html
    https://www.jsps.go.jp/english/e-biol/02_recipients/awardee2025.html

This script includes those official pages so the ingest is not stale simply
because the summary table lags the detail pages.

Amount handling
---------------
The official "About the Prize" page states that the prize consists of a medal
and a prize of ten million (10,000,000) yen. The prize has one recipient per
edition, so every row ships amount=10,000,000 and currency=JPY.

OpenAlex funder mapping
-----------------------
The prize is a JSPS award of recognition. OpenAlex has Japan Society for the
Promotion of Science as F4320334764 (country JP; no DOI/ROR on the public
funder row). This is the same funder family as the existing KAKEN ingest, but
the prize is an operationally separate awards program with its own provenance.

Output
------
s3://openalex-ingest/awards/international_biology_prize/international_biology_prize_recipients.parquet
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


BASE_URL = "https://www.jsps.go.jp"
LIST_URL = f"{BASE_URL}/english/e-biol/02_recipients.html"
ABOUT_URL = f"{BASE_URL}/english/e-biol/01_outline.html"
DETAIL_PATTERN = f"{BASE_URL}/english/e-biol/02_recipients/awardee{{year}}.html"

FUNDER_ID = 4320334764
FUNDER_DISPLAY_NAME = "Japan Society for the Promotion of Science"

PROVENANCE = "international_biology_prize"
CURRENCY = "JPY"
AMOUNT_JPY = 10_000_000.0

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/international_biology_prize/international_biology_prize_recipients.parquet"

USER_AGENT = "openalex-walden-international-biology-prize-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.35

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 30) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/pdf,*/*;q=0.8",
        })
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.get(url, timeout=timeout, allow_redirects=True)
    _last_request_t = time.monotonic()
    if "text/html" in resp.headers.get("content-type", "").lower():
        resp.encoding = "utf-8"
    return resp


def clean_text(value: str | None) -> Optional[str]:
    if value is None:
        return None
    value = value.replace("\u3000", " ").replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def html_text(html: str | None) -> Optional[str]:
    if not html:
        return None
    return clean_text(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))


def slugify(text: str, max_len: int = 90) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return (text or "unknown")[:max_len].strip("-") or "unknown"


def clean_person_name(raw: str | None) -> Optional[str]:
    raw = clean_text(raw)
    if not raw:
        return None
    raw = re.sub(r"^(Dr\.?|Prof\.?|Professor)\s*", "", raw, flags=re.I)
    raw = re.sub(r"^(Sir|Dame)\s+", "", raw, flags=re.I)
    raw = re.sub(r"\s+(FRS|FBA|MD|PhD|Ph\\.D\\.)$", "", raw)
    raw = raw.strip(" ,;")
    return clean_text(raw)


def split_name(name: str | None) -> tuple[Optional[str], Optional[str]]:
    name = clean_person_name(name)
    if not name:
        return None, None
    parts = name.split()
    if len(parts) == 1:
        return None, parts[0]
    if parts[0].isupper() and len(parts) >= 2:
        return " ".join(parts[1:]), parts[0].title()
    return " ".join(parts[:-1]), parts[-1]


def parse_recipient_line(line: str) -> tuple[Optional[str], Optional[str]]:
    line = re.sub(r"^Recipient\s*:\s*", "", line, flags=re.I)
    if " / " in line:
        name, affiliation = line.split(" / ", 1)
    else:
        name, affiliation = line, None
    return clean_person_name(name), clean_text(affiliation)


def first_about_link(panel) -> Optional[str]:
    for a in panel.find_all("a", href=True):
        text = clean_text(a.get_text(" ", strip=True)) or ""
        if "About the" in text and "Recipient" in text:
            return urljoin(LIST_URL, a["href"])
    return None


def parse_list_page(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []
    for panel in soup.find_all("div", class_="panel"):
        title = panel.find("p", class_="title")
        year_label = clean_text(title.get_text(" ", strip=True) if title else None)
        if not year_label:
            continue
        m = re.search(r"((?:19|20)\d{2})\s+\((\d+)(?:st|nd|rd|th)\)", year_label)
        if not m:
            continue
        year = int(m.group(1))
        edition = int(m.group(2))
        recipient_name = None
        affiliation = None
        research_field = None
        for label in panel.find_all("div", class_="label"):
            text = clean_text(label.get_text(" ", strip=True))
            if not text:
                continue
            if text.lower().startswith("recipient"):
                recipient_name, affiliation = parse_recipient_line(text)
            elif text.lower().startswith("research field"):
                research_field = clean_text(re.sub(r"^Research field\s*:\s*", "", text, flags=re.I))
        if not recipient_name:
            continue
        rows.append({
            "source_year": str(year),
            "edition": str(edition),
            "recipient_name": recipient_name,
            "affiliation_raw": affiliation,
            "research_field": research_field,
            "landing_page_url": first_about_link(panel) or LIST_URL,
            "source_list_url": LIST_URL,
        })
    return rows


def parse_detail_intro(text: str, year: int) -> tuple[Optional[str], Optional[str], Optional[str]]:
    text = clean_text(text) or ""
    m = re.search(
        rf"awards the {year} Prize in the field of [\"“](.+?)[\"”]\s+to\s+(.+)",
        text,
        flags=re.I,
    )
    if not m:
        return None, None, None
    field = clean_text(m.group(1))
    tail = clean_text(m.group(2)) or ""
    tail = tail.replace("Dr.", "Dr. ").replace("Prof.", "Prof. ")
    tail = clean_text(tail) or ""
    tail_no_title = clean_person_name(tail) or tail
    for marker in [" Emeritus Professor", " Professor", " Senior", " Director", " Distinguished"]:
        idx = tail_no_title.find(marker)
        if idx > 0:
            return clean_person_name(tail_no_title[:idx]), clean_text(tail_no_title[idx + 1:]), field
    if "," in tail_no_title and tail_no_title.index(",") < 80:
        name, affiliation = tail_no_title.split(",", 1)
        return clean_person_name(name), clean_text(affiliation), field
    parts = tail_no_title.split()
    if len(parts) >= 2:
        return clean_person_name(" ".join(parts[:2])), clean_text(" ".join(parts[2:])), field
    return clean_person_name(tail_no_title), None, field


def extract_research_achievements(soup: BeautifulSoup) -> Optional[str]:
    heading = None
    for h in soup.find_all(["h2", "h3", "h4"]):
        if "research" in h.get_text(" ", strip=True).lower() and "achievement" in h.get_text(" ", strip=True).lower():
            heading = h
            break
    if heading is None:
        return None
    parts: list[str] = []
    for sib in heading.find_next_siblings():
        if getattr(sib, "name", None) in ("h2", "h3", "h4"):
            break
        text = clean_text(sib.get_text(" ", strip=True)) if hasattr(sib, "get_text") else None
        if text:
            parts.append(text)
    return clean_text(" ".join(parts))


def parse_detail_page(url: str, year: int) -> Optional[dict]:
    resp = _http_get(url)
    if resp.status_code != 200:
        return None
    ctype = resp.headers.get("content-type", "").lower()
    if "html" not in ctype or "404 Page not found" in resp.text:
        return {"landing_page_url": url}
    soup = BeautifulSoup(resp.text, "html.parser")
    main = soup.find("main") or soup
    intro_text = None
    for div in main.find_all("div", class_="copy-0002"):
        text = clean_text(div.get_text(" ", strip=True))
        if text and f"awards the {year} Prize" in text:
            intro_text = text
            break
    if intro_text is None:
        text = clean_text(main.get_text(" ", strip=True)) or ""
        idx = text.find(f"awards the {year} Prize")
        intro_text = text[idx:idx + 600] if idx != -1 else None
    name, affiliation, field = parse_detail_intro(intro_text or "", year)
    return {
        "recipient_name": name,
        "affiliation_raw": affiliation,
        "research_field": field,
        "detail_description": extract_research_achievements(soup),
        "landing_page_url": url,
    }


def enrich_with_detail(rows: list[dict]) -> None:
    for row in rows:
        url = row.get("landing_page_url")
        if not url or not str(url).endswith(".html"):
            continue
        detail = parse_detail_page(str(url), int(row["source_year"]))
        if not detail:
            continue
        row["detail_description"] = detail.get("detail_description")


def add_newer_detail_rows(rows: list[dict]) -> None:
    seen_years = {int(row["source_year"]) for row in rows}
    max_listed = max(seen_years)
    current_year = datetime.now(timezone.utc).year
    for year in range(max_listed + 1, current_year + 1):
        url = DETAIL_PATTERN.format(year=year)
        detail = parse_detail_page(url, year)
        if not detail or not detail.get("recipient_name"):
            continue
        rows.append({
            "source_year": str(year),
            "edition": str(year - 1984),
            "recipient_name": detail["recipient_name"],
            "affiliation_raw": detail.get("affiliation_raw"),
            "research_field": detail.get("research_field"),
            "detail_description": detail.get("detail_description"),
            "landing_page_url": url,
            "source_list_url": LIST_URL,
        })


def fetch_raw_rows(limit: Optional[int] = None) -> list[dict]:
    print("\n" + "=" * 60)
    print("Step 1: Download JSPS International Prize for Biology pages")
    print("=" * 60)
    resp = _http_get(LIST_URL)
    resp.raise_for_status()
    rows = parse_list_page(resp.text)
    print(f"  parsed {len(rows):,} listed recipient rows from {LIST_URL}")
    enrich_with_detail(rows)
    add_newer_detail_rows(rows)
    rows = sorted(rows, key=lambda r: int(r["source_year"]))
    print(f"  total official rows after newer detail pages: {len(rows):,}")
    if limit:
        rows = rows[:limit]
        print(f"  [LIMIT] keeping first {len(rows):,} rows")
    return rows


def load_cached_rows(output_dir: Path) -> list[dict]:
    raw_path = output_dir / "international_biology_prize_raw.json"
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
    if full_run and len(rows) < 40:
        raise RuntimeError(f"Expected at least 40 International Biology rows, got {len(rows):,}")
    out: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        year = int(row["source_year"])
        name = clean_person_name(row.get("recipient_name")) or ""
        given, family = split_name(name)
        award_id = f"international-biology-prize-{year}-{slugify(name)}"
        if award_id in seen:
            raise RuntimeError(f"Duplicate funder_award_id would be emitted: {award_id}")
        seen.add(award_id)
        field = clean_text(row.get("research_field"))
        description = clean_text(row.get("detail_description")) or (
            f"International Prize for Biology in the field of {field}." if field else None
        )
        out.append({
            "funder_award_id": award_id,
            "display_name": f"International Prize for Biology {year} - {name}",
            "description": description,
            "source_year": str(year),
            "edition": row.get("edition"),
            "recipient_name": name,
            "given_name": given,
            "family_name": family,
            "affiliation_raw": clean_text(row.get("affiliation_raw")),
            "research_field": field,
            "amount": str(AMOUNT_JPY),
            "currency": CURRENCY,
            "funder_scheme": field or "International Prize for Biology",
            "start_date": f"{year}-01-01",
            "end_date": f"{year}-12-31",
            "landing_page_url": row.get("landing_page_url") or LIST_URL,
            "source_list_url": row.get("source_list_url") or LIST_URL,
            "amount_source_url": ABOUT_URL,
            "provenance": PROVENANCE,
        })
    df = pd.DataFrame.from_records(out)
    print(f"  rows: {len(df):,}")
    print(f"  year range: {df['source_year'].astype(int).min()}-{df['source_year'].astype(int).max()}")
    for col in ["display_name", "source_year", "recipient_name", "research_field",
                "affiliation_raw", "description", "amount", "currency"]:
        count = int(df[col].notna().sum())
        print(f"  {col:18s}: {count:,}/{len(df):,} ({count * 100 / len(df):.1f}%)")
    print(f"  unique funder_award_id: {df['funder_award_id'].nunique():,}")
    df = df.astype("string")
    return df


def write_outputs(rows: list[dict], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_path = output_dir / "international_biology_prize_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw rows to {raw_path}")
    parquet_path = output_dir / "international_biology_prize_recipients.parquet"
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
    prev_path = output_dir / "_prev_international_biology_prize.parquet"
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
        print(f"\n[ERROR] Refusing to shrink International Biology corpus ({prev_count:,} -> {new_count:,}).")
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
    parser = argparse.ArgumentParser(description="Download International Prize for Biology recipients.")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/international_biology_prize"))
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("International Prize for Biology -> S3 pipeline")
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
