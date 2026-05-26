#!/usr/bin/env python3
"""
Marianne and Marcus Wallenberg Foundation grants -> S3 Data Pipeline
====================================================================

Downloads grant rows from the official Marianne and Marcus Wallenberg
Foundation (MMW) Drupal site and writes a parquet file for the OpenAlex
awards pipeline.

Source authority
----------------
The source is first-party:

    https://mmw.wallenberg.org/en/sitemap.xml

The sitemap exposes static English yearly grant pages for 2019-2025 and
project detail pages under /en/project/. The yearly pages are the canonical
grant list because they include all rows with award year and SEK amount; the
project detail pages are used only to enrich rows where linked (PI,
institution, description, and funding text).

OpenAlex funder mapping note
----------------------------
OpenAlex search for "Marianne Marcus Wallenberg" returns one relevant funder
row, F4320327344. Its display name is the Swedish MAW label, but alternate
titles include "Marianne and Marcus Wallenberg Foundation", "Marianne och
Marcus Wallenbergs Stiftelse", and "MMW". No separate MMW funder row was
found, so this script uses F4320327344 and documents the ambiguity in the
tracker and notebook.

Output
------
s3://openalex-ingest/awards/mmw/mmw_grants.parquet

Usage
-----
    python mmw_to_s3.py --skip-upload
    python mmw_to_s3.py --limit 10 --skip-upload
    python mmw_to_s3.py --skip-download --skip-upload
    python mmw_to_s3.py --allow-shrink

Requirements
------------
    pip install beautifulsoup4 pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

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


# =============================================================================
# Configuration
# =============================================================================

BASE_URL = "https://mmw.wallenberg.org"
SITEMAP_URL = f"{BASE_URL}/en/sitemap.xml"

FUNDER_ID = 4320327344
FUNDER_DISPLAY_NAME = "Marianne and Marcus Wallenberg Foundation"
PROVENANCE = "mmw_wallenberg_grants"
CURRENCY = "SEK"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/mmw/mmw_grants.parquet"

USER_AGENT = "openalex-walden-mmw-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.35


_session: Optional[requests.Session] = None
_last_request_t = 0.0


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
    return _session


def polite_get(url: str, timeout: int = 60, max_attempts: int = 4) -> requests.Response:
    global _last_request_t
    session = get_session()
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < MIN_REQUEST_INTERVAL_S:
            time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
        try:
            resp = session.get(url, timeout=timeout)
            _last_request_t = time.monotonic()
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 20)
                print(f"  [WARN] HTTP {resp.status_code}; retrying in {sleep_s}s")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 20)
            print(f"  [WARN] request failed ({exc}); retrying in {sleep_s}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def strip_outer_quotes(value: Optional[str]) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    return clean_text(text.strip(" \"'\u201c\u201d"))


def split_person_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    text = clean_text(name)
    if not text:
        return None, None
    text = re.sub(
        r"^(Professor|Associate Professor|Assistant Professor|Dr|Doctor)\s+",
        "",
        text,
        flags=re.IGNORECASE,
    )
    tokens = [t for t in text.split() if t]
    suffixes = {"PhD", "MD", "DPhil", "Jr.", "Sr.", "II", "III", "IV"}
    while tokens and tokens[-1].rstrip(",") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def slugify(text: str, max_len: int = 80) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return (text[:max_len].strip("-") or "row")


def parse_amount_sek(text: str) -> tuple[Optional[int], Optional[str]]:
    """Return whole SEK from phrases like 'SEK 9 900 000' or 'SEK 9.6 million'."""
    amount_match = re.search(r"SEK\s+([0-9][0-9\s.,]*)(?:\s*(million|m))?", text, re.IGNORECASE)
    if not amount_match:
        return None, None
    raw_number = amount_match.group(1)
    number = raw_number.replace(" ", "").replace(",", ".")
    try:
        value = float(number)
    except ValueError:
        return None, amount_match.group(0)
    if amount_match.group(2):
        value *= 1_000_000
    return int(round(value)), clean_text(amount_match.group(0))


def extract_project_title(text: str) -> Optional[str]:
    match = re.search(
        r"project(?:\s+entitled|:)?\s*[\u201c\u201d\"]([^\"\u201c\u201d]+)[\u201c\u201d\"]",
        text,
        re.IGNORECASE,
    )
    return clean_text(match.group(1)) if match else None


def extract_institution_and_pi(text: str) -> tuple[Optional[str], Optional[str]]:
    before_amount = re.split(
        r"\b(?:grant|funding|awarded grant|yearly funding|Grant)\b",
        text,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    before_amount = clean_text(before_amount.strip(" ,")) or ""
    parts = [p for p in (clean_text(p) for p in before_amount.split(",")) if p]
    if not parts:
        return None, None

    title_re = re.compile(r"^(Professor|Associate Professor|Assistant Professor|Dr|Doctor)\b", re.IGNORECASE)
    institution: Optional[str] = None
    pi: Optional[str] = None
    if title_re.search(parts[0]):
        pi = parts[0]
        institution = parts[1] if len(parts) > 1 else None
    else:
        institution = parts[0]
        if len(parts) > 1 and title_re.search(parts[1]):
            pi = parts[1]

    if institution:
        institution = clean_text(re.sub(r"\s+Project\s*:?.*$", "", institution, flags=re.IGNORECASE))
    return institution, pi


def discover_grant_year_urls() -> list[str]:
    print("\n" + "=" * 60)
    print("Step 1: Discover grant-year pages")
    print("=" * 60)
    resp = polite_get(SITEMAP_URL)
    soup = BeautifulSoup(resp.text, "xml")
    urls = []
    for loc in soup.find_all("loc"):
        url = clean_text(loc.get_text())
        if url and re.search(r"/en/grants/\d{4}$", url):
            urls.append(url)
    urls = sorted(set(urls), key=lambda u: int(u.rsplit("/", 1)[1]))
    if not urls:
        raise RuntimeError("No /en/grants/YYYY pages found in MMW sitemap")
    print(f"  Found {len(urls)} grant-year pages: {', '.join(u.rsplit('/', 1)[1] for u in urls)}")
    return urls


def is_aggregate_summary(text: str) -> bool:
    return bool(re.match(r"In\s+\d{4}.*awarded grants? of SEK", text, re.IGNORECASE))


def extract_detail_link(paragraph) -> Optional[str]:
    for anchor in paragraph.find_all("a"):
        href = anchor.get("href")
        if not href:
            continue
        absolute = urljoin(BASE_URL, href.strip())
        if "/en/project/" in absolute or "/node/" in absolute:
            return absolute
    return None


def parse_grant_year_page(url: str) -> list[dict[str, Any]]:
    year = int(url.rsplit("/", 1)[1])
    print(f"  Fetching {year}: {url}")
    resp = polite_get(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    main = soup.find("main") or soup
    rows: list[dict[str, Any]] = []
    current_section: Optional[str] = None

    for tag in main.find_all(["h2", "p"]):
        text = clean_text(tag.get_text(" ", strip=True))
        if not text:
            continue
        if tag.name == "h2":
            current_section = text
            if current_section in ("Earlier grants", "Projects"):
                current_section = "__stop__"
            continue
        if current_section == "__stop__":
            continue
        if "SEK" not in text or not re.search(r"\b(grant|funding|awarded)\b", text, re.IGNORECASE):
            continue
        if is_aggregate_summary(text):
            continue

        amount, amount_text = parse_amount_sek(text)
        if amount is None:
            print(f"    [WARN] amount not parsed for {year}: {text[:160]}")
            continue

        detail_url = extract_detail_link(tag)
        link_text = None
        first_anchor = tag.find("a")
        if first_anchor:
            link_text = clean_text(first_anchor.get_text(" ", strip=True))

        project_title = extract_project_title(text)
        institution, pi = extract_institution_and_pi(text)
        display_name = (
            (link_text if detail_url else None)
            or project_title
            or strip_outer_quotes(link_text)
            or text[:140]
        )
        source_hash = hashlib.sha1(f"{year}|{text}".encode("utf-8")).hexdigest()[:12]

        rows.append({
            "source_year": str(year),
            "source_section": None if current_section in (None, "__stop__") else current_section,
            "source_page_url": url,
            "source_text": text,
            "source_amount_text": amount_text,
            "amount": str(amount),
            "currency": CURRENCY,
            "detail_url": detail_url,
            "display_name": display_name,
            "project_title": project_title,
            "institution": institution,
            "lead_investigator_name": pi,
            "source_row_hash": source_hash,
        })
    print(f"    parsed {len(rows)} grant rows")
    return rows


def parse_detail_page(url: str) -> dict[str, Optional[str]]:
    resp = polite_get(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    main = soup.find("main") or soup
    h1 = main.find("h1")
    detail_title = clean_text(h1.get_text(" ", strip=True)) if h1 else None

    detail: dict[str, Optional[str]] = {
        "detail_title": detail_title,
        "detail_description": None,
        "detail_project_title": None,
        "detail_lead_investigator_name": None,
        "detail_co_investigators_raw": None,
        "detail_institution": None,
        "detail_amount": None,
        "detail_amount_text": None,
    }

    description_parts: list[str] = []
    co_parts: list[str] = []
    in_co_block = False
    key_prefixes = (
        "Project:",
        "Principal Investigator:",
        "Co-Investigator:",
        "Institution:",
        "Funding Awarded:",
    )

    for p in main.find_all("p"):
        text = clean_text(p.get_text(" ", strip=True))
        if not text:
            continue
        if text.startswith("Project:"):
            detail["detail_project_title"] = clean_text(text.split(":", 1)[1])
            in_co_block = False
            continue
        if text.startswith("Principal Investigator:"):
            detail["detail_lead_investigator_name"] = clean_text(text.split(":", 1)[1])
            in_co_block = False
            continue
        if text.startswith("Co-Investigator:"):
            co_text = clean_text(text.split(":", 1)[1])
            if co_text:
                co_parts.append(co_text)
            in_co_block = True
            continue
        if text.startswith("Institution:"):
            detail["detail_institution"] = clean_text(text.split(":", 1)[1])
            in_co_block = False
            continue
        if text.startswith("Funding Awarded:"):
            amount, amount_text = parse_amount_sek(text)
            detail["detail_amount"] = str(amount) if amount is not None else None
            detail["detail_amount_text"] = amount_text
            in_co_block = False
            continue
        if in_co_block and not text.startswith(key_prefixes):
            co_parts.append(text)
            continue
        if not text.startswith(key_prefixes):
            # Stop before footer/sidebar copy leaks into the description.
            if text in ("About", "Shortcuts", "Contact") or text.startswith("Marianne and Marcus Wallenberg Foundation is"):
                continue
            description_parts.append(text)

    if description_parts:
        detail["detail_description"] = "\n\n".join(description_parts)
    if co_parts:
        detail["detail_co_investigators_raw"] = "; ".join(co_parts)
    return detail


def detail_slug(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    path = urlparse(url).path.strip("/")
    if not path:
        return None
    return slugify(path.rsplit("/", 1)[-1])


def build_native_ids(rows: list[dict[str, Any]]) -> None:
    seen: dict[str, int] = {}
    for row in rows:
        base = detail_slug(row.get("detail_url"))
        if not base:
            base = slugify(row.get("display_name") or row.get("project_title") or row.get("institution") or "grant")
        native = f"mmw-{row['source_year']}-{base}"
        if native in seen:
            seen[native] += 1
            native = f"{native}-{row['source_row_hash']}"
        else:
            seen[native] = 1
        row["funder_award_id"] = native


def fetch_grant_rows(limit: Optional[int] = None) -> list[dict[str, Any]]:
    urls = discover_grant_year_urls()
    rows: list[dict[str, Any]] = []
    for url in urls:
        rows.extend(parse_grant_year_page(url))

    if limit:
        rows = rows[:limit]
        print(f"  [LIMIT] keeping first {len(rows):,} rows before detail enrichment")

    detail_urls = sorted({r["detail_url"] for r in rows if r.get("detail_url")})
    print(f"\n  Enriching {len(detail_urls):,} linked detail pages")
    detail_cache: dict[str, dict[str, Optional[str]]] = {}
    for i, url in enumerate(detail_urls, start=1):
        try:
            detail_cache[url] = parse_detail_page(url)
        except Exception as exc:
            print(f"    [WARN] failed detail page {url}: {exc}")
            detail_cache[url] = {}
        if i % 25 == 0 or i == len(detail_urls):
            print(f"    {i:,}/{len(detail_urls):,} detail pages")

    for row in rows:
        detail = detail_cache.get(row.get("detail_url") or "", {})
        row.update(detail)
        row["display_name"] = (
            detail.get("detail_title")
            or row.get("display_name")
            or detail.get("detail_project_title")
            or row.get("project_title")
        )
        row["project_title"] = detail.get("detail_project_title") or row.get("project_title")
        row["institution"] = detail.get("detail_institution") or row.get("institution")
        row["lead_investigator_name"] = (
            detail.get("detail_lead_investigator_name") or row.get("lead_investigator_name")
        )
        if detail.get("detail_amount"):
            row["amount"] = detail["detail_amount"]
            row["source_amount_text"] = detail.get("detail_amount_text") or row.get("source_amount_text")
        row["description"] = detail.get("detail_description")
        row["co_investigators_raw"] = detail.get("detail_co_investigators_raw")
        row["landing_page_url"] = row.get("detail_url") or row.get("source_page_url")

    build_native_ids(rows)
    if not limit and len(rows) < 90:
        raise RuntimeError(f"Full MMW run returned only {len(rows):,} rows; expected at least 90.")
    return rows


def normalize_rows(rows: list[dict[str, Any]], *, full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Normalize rows")
    print("=" * 60)
    retrieved_at = datetime.now(timezone.utc).isoformat()
    normalized = []
    for row in rows:
        year = clean_text(row.get("source_year"))
        pi_name = clean_text(row.get("lead_investigator_name"))
        pi_given, pi_family = split_person_name(pi_name)
        normalized.append({
            "funder_award_id": clean_text(row.get("funder_award_id")),
            "display_name": clean_text(row.get("display_name")),
            "description": clean_text(row.get("description")),
            "project_title": clean_text(row.get("project_title")),
            "source_year": year,
            "start_date": f"{year}-01-01" if year else None,
            "end_date": f"{year}-12-31" if year else None,
            "amount": clean_text(row.get("amount")),
            "currency": CURRENCY if row.get("amount") else None,
            "institution": clean_text(row.get("institution")),
            "lead_investigator_name": pi_name,
            "lead_investigator_given_name": pi_given,
            "lead_investigator_family_name": pi_family,
            "co_investigators_raw": clean_text(row.get("co_investigators_raw")),
            "source_section": clean_text(row.get("source_section")),
            "source_amount_text": clean_text(row.get("source_amount_text")),
            "source_page_url": clean_text(row.get("source_page_url")),
            "detail_url": clean_text(row.get("detail_url")),
            "landing_page_url": clean_text(row.get("landing_page_url")),
            "source_text": clean_text(row.get("source_text")),
            "source_row_hash": clean_text(row.get("source_row_hash")),
            "retrieved_at": retrieved_at,
        })

    df = pd.DataFrame(normalized)
    validate_dataframe(df, full_run=full_run)
    df = df.astype("string")
    return df


def validate_dataframe(df: pd.DataFrame, *, full_run: bool) -> None:
    total = len(df)
    if total == 0:
        raise RuntimeError("No MMW grant rows after normalization")
    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")
    if full_run and total < 90:
        raise RuntimeError(f"Full MMW run returned only {total:,} rows; expected at least 90.")

    required_cols = ["display_name", "source_year", "amount", "currency", "institution", "landing_page_url"]
    for col in required_cols:
        coverage = df[col].notna().mean()
        print(f"  {col:24s}: {df[col].notna().sum():,}/{total:,} ({coverage * 100:.1f}%)")
        if coverage < 0.90:
            raise RuntimeError(f"Unexpectedly low coverage for {col}: {coverage * 100:.1f}%")
    print(f"  {'lead_investigator_name':24s}: {df['lead_investigator_name'].notna().sum():,}/{total:,} ({df['lead_investigator_name'].notna().mean() * 100:.1f}%)")
    print(f"  {'description':24s}: {df['description'].notna().sum():,}/{total:,} ({df['description'].notna().mean() * 100:.1f}%)")
    print(f"  {'detail_url':24s}: {df['detail_url'].notna().sum():,}/{total:,} ({df['detail_url'].notna().mean() * 100:.1f}%)")

    amount_numeric = pd.to_numeric(df["amount"], errors="coerce")
    print(f"  Total SEK amount: {amount_numeric.sum():,.0f}")
    print(f"  Year range: {df['source_year'].min()} - {df['source_year'].max()}")
    print(f"  Rows by year: {df['source_year'].value_counts().sort_index().to_dict()}")
    print(f"  Top sections: {df['source_section'].value_counts(dropna=False).head(10).to_dict()}")


def write_outputs(rows: list[dict[str, Any]], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "mmw_grants_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw rows to {raw_path}")

    parquet_path = output_dir / "mmw_grants.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df):,} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def load_cached_rows(output_dir: Path) -> list[dict[str, Any]]:
    raw_path = output_dir / "mmw_grants_raw.json"
    if not raw_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {raw_path}")
    with raw_path.open("r", encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list):
        raise RuntimeError(f"Cached JSON should be a list of records: {raw_path}")
    print(f"  [OK] loaded {len(rows):,} cached rows from {raw_path}")
    return rows


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

    prev_path = output_dir / "_prev_mmw_grants.parquet"
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
        print(f"\n[ERROR] Refusing to shrink MMW corpus ({prev_count:,} -> {new_count:,}).")
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
        description="Download MMW grants and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/mmw"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse mmw_grants_raw.json from output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("MMW grants -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Sitemap:    {SITEMAP_URL}")
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
        rows = fetch_grant_rows(limit=args.limit)

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
