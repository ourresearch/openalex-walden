#!/usr/bin/env python3
"""
Packard Fellowships for Science and Engineering -> S3.

Static-HTML scrape of the David and Lucile Packard Foundation's official
Fellows Directory:

    https://www.packard.org/approach/fellowships-for-science-engineering/fellows-directory/

The directory advertises 730 fellows across 1988-2025, paginated 28 per page.
Each card links to a per-fellow page with year, disciplines, current
institution, fellowship institution, profile text, and awards/achievements.

OpenAlex funder:
    F4320306079 - David and Lucile Packard Foundation

Output:
    s3://openalex-ingest/awards/packard/packard_fellows.parquet

Mapping notes:
    - One row per fellow page.
    - `funder_award_id = packard-{slug}-{year}` and duplicates raise.
    - `lead_investigator` in the notebook is the fellow; affiliation.name is
      the source's `Fellowship Institution`, not the fellow's later/current
      institution.
    - The program overview says current fellows receive USD 875,000 over five
      years, but the directory does not publish historical per-fellow amounts.
      The notebook leaves amount/currency NULL rather than applying a current
      program value to historical cohorts.
    - All parquet columns are strings by design; Databricks casts explicitly.

Usage:
    python scripts/local/packard_fellows_to_s3.py --skip-upload --max-pages 1
    python scripts/local/packard_fellows_to_s3.py --skip-upload
    python scripts/local/packard_fellows_to_s3.py --skip-download --skip-upload
    python scripts/local/packard_fellows_to_s3.py --allow-shrink

Requirements:
    pip install beautifulsoup4 pandas pyarrow requests boto3
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
from typing import Any
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup


# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). This
# crashes scrapers writing laureate names with non-ASCII chars (Polish ł,
# Turkish ğ, Greek μ, combining accents, zero-width spaces). Production
# runs on Linux/Databricks where UTF-8 is the default, but this fixes
# local validation on Windows without requiring contractors to set
# PYTHONUTF8=1 in their environment. See runbook §1.2.
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

BASE_URL = "https://www.packard.org"
DIRECTORY_URL = (
    "https://www.packard.org/approach/fellowships-for-science-engineering/"
    "fellows-directory/"
)
PROGRAM_URL = "https://www.packard.org/approach/fellowships-for-science-engineering/"
FUNDER_ID = 4320306079
FUNDER_DISPLAY_NAME = "David and Lucile Packard Foundation"
PROVENANCE = "packard_fellows_directory"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/packard/packard_fellows.parquet"

USER_AGENT = "openalex-walden-packard-fellows/1.0 (+https://openalex.org)"
REQUEST_TIMEOUT = (10, 60)
REQUEST_SLEEP_SECONDS = 0.15
EXPECTED_MIN_ROWS = 700


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).replace("\xa0", " ").split())
    return text or None


def slug_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    if not path.startswith("fellow/"):
        raise ValueError(f"Unexpected Packard fellow URL: {url}")
    return path.split("/")[-1]


def split_name(full_name: str | None) -> tuple[str | None, str | None]:
    full_name = clean_text(full_name)
    if not full_name:
        return None, None
    parts = full_name.split()
    if len(parts) == 1:
        return parts[0], None
    suffixes = {"Jr.", "Jr", "Sr.", "Sr", "II", "III", "IV"}
    if parts[-1] in suffixes and len(parts) > 2:
        return " ".join(parts[:-2]), " ".join(parts[-2:])
    return " ".join(parts[:-1]), parts[-1]


class PackardClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        )
        self._last_request = 0.0

    def get(self, url: str) -> str:
        elapsed = time.monotonic() - self._last_request
        if elapsed < REQUEST_SLEEP_SECONDS:
            time.sleep(REQUEST_SLEEP_SECONDS - elapsed)
        log(f"GET {url}")
        response = self.session.get(url, timeout=REQUEST_TIMEOUT)
        self._last_request = time.monotonic()
        log(f"  -> {response.status_code} {len(response.content):,} bytes")
        response.raise_for_status()
        return response.text


def directory_page_url(page: int) -> str:
    if page == 1:
        return DIRECTORY_URL
    return urljoin(DIRECTORY_URL, f"page/{page}/")


def parse_advertised_total(soup: BeautifulSoup) -> int | None:
    text = clean_text(soup.get_text(" ", strip=True)) or ""
    match = re.search(r"\b\d+\s*-\s*\d+\s+of\s+([\d,]+)\s+Fellows\b", text)
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


def parse_last_page(soup: BeautifulSoup, total: int | None) -> int:
    page_numbers: list[int] = []
    for link in soup.select("a.page-numbers"):
        text = clean_text(link.get_text(" ", strip=True))
        if text and text.isdigit():
            page_numbers.append(int(text))
    if page_numbers:
        return max(page_numbers)
    if total:
        return (total + 27) // 28
    return 1


def parse_directory_links(html: str) -> tuple[list[str], int | None, int]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for link in soup.select('a[href*="/fellow/"]'):
        href = link.get("href")
        if not href:
            continue
        url = urljoin(BASE_URL, href).split("#")[0]
        if url not in seen:
            seen.add(url)
            urls.append(url)
    total = parse_advertised_total(soup)
    last_page = parse_last_page(soup, total)
    return urls, total, last_page


def verify_program_amount_text(client: PackardClient) -> None:
    """Confirm the audit-only current amount note is still on Packard's site."""
    html = client.get(PROGRAM_URL)
    text = clean_text(BeautifulSoup(html, "html.parser").get_text(" ", strip=True)) or ""
    if "$875,000" not in text or "five years" not in text:
        raise RuntimeError(
            "Program overview no longer contains the expected '$875,000' / "
            "'five years' amount text; update program_current_amount_usd note "
            "before writing parquet."
        )
    log("Verified current program overview amount text: $875,000 over five years.")


def parse_meta(soup: BeautifulSoup) -> dict[str, str]:
    meta: dict[str, str] = {}
    for item in soup.select(".single-fellow__meta li"):
        label = item.find("strong")
        if not label:
            continue
        key = clean_text(label.get_text(" ", strip=True))
        if not key:
            continue
        value = item.get_text(" ", strip=True).replace(label.get_text(" ", strip=True), "", 1)
        meta[key.rstrip(":").strip()] = clean_text(value)
    return meta


def parse_profile_text(content: Any) -> str | None:
    if content is None:
        return None
    parts: list[str] = []
    for child in content.children:
        name = getattr(child, "name", None)
        if name == "h2" and clean_text(child.get_text(" ", strip=True)) == "Awards and Achievements":
            break
        if name == "p":
            text = clean_text(child.get_text(" ", strip=True))
            if text:
                parts.append(text)
    return "\n\n".join(parts) or None


def parse_achievements(content: Any) -> list[str]:
    if content is None:
        return []
    heading = None
    for h2 in content.find_all("h2"):
        if clean_text(h2.get_text(" ", strip=True)) == "Awards and Achievements":
            heading = h2
            break
    if heading is None:
        return []
    ul = heading.find_next("ul")
    if ul is None:
        return []
    return [
        text
        for text in (clean_text(li.get_text(" ", strip=True)) for li in ul.find_all("li"))
        if text
    ]


def parse_detail_page(url: str, html: str, downloaded_at: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.select_one("h1.single-fellow__title")
    full_name = clean_text(title.get_text(" ", strip=True) if title else None)
    meta = parse_meta(soup)

    year_text = meta.get("Year")
    if not year_text or not re.fullmatch(r"\d{4}", year_text):
        raise RuntimeError(f"{url} missing valid fellowship year: {year_text!r}")
    year = int(year_text)

    disciplines = [
        clean_text(part)
        for part in re.split(r",\s*", meta.get("Disciplines") or "")
        if clean_text(part)
    ]
    content = soup.select_one(".single-fellow__content")
    profile_text = parse_profile_text(content)
    achievements = parse_achievements(content)

    canonical = soup.find("link", rel="canonical")
    canonical_url = urljoin(BASE_URL, canonical.get("href")) if canonical and canonical.get("href") else url
    image = soup.find("meta", property="og:image")
    image_url = image.get("content") if image and image.get("content") else None
    slug = slug_from_url(canonical_url)
    given_name, family_name = split_name(full_name)

    source_description_parts = []
    if profile_text:
        source_description_parts.append(profile_text)
    if disciplines:
        source_description_parts.append("Disciplines: " + "; ".join(disciplines))
    current_institution = meta.get("Current Institution")
    fellowship_institution = meta.get("Fellowship Institution")
    if current_institution:
        source_description_parts.append(f"Current institution: {current_institution}")
    if fellowship_institution:
        source_description_parts.append(f"Fellowship institution: {fellowship_institution}")

    return {
        "funder_award_id": f"packard-{slug}-{year}",
        "slug": slug,
        "full_name": full_name,
        "given_name": given_name,
        "family_name": family_name,
        "fellowship_year": str(year),
        "disciplines": json.dumps(disciplines, ensure_ascii=False),
        "disciplines_text": "; ".join(disciplines) if disciplines else None,
        "current_institution": current_institution,
        "fellowship_institution": fellowship_institution,
        "profile_text": profile_text,
        "achievements": json.dumps(achievements, ensure_ascii=False),
        "achievements_text": "; ".join(achievements) if achievements else None,
        "display_name": f"Packard Fellowship for Science and Engineering: {full_name} ({year})",
        "description": "\n\n".join(source_description_parts) or None,
        "start_date": f"{year}-01-01",
        "end_date": f"{year + 4}-12-31",
        "amount": None,
        "currency": None,
        "program_current_amount_usd": "875000",
        "program_amount_note": "Current Packard program overview says each fellow receives USD 875,000 over five years; historical per-fellow amounts are not published in the directory, so OpenAlex amount/currency are left NULL.",
        "program_amount_source_url": PROGRAM_URL,
        "profile_url": canonical_url,
        "image_url": image_url,
        "source_html_title": clean_text(soup.title.get_text(" ", strip=True) if soup.title else None),
        "declined": "false",
        "downloaded_at": downloaded_at,
    }


def load_checkpoint(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    rows = json.loads(path.read_text())
    if not isinstance(rows, list):
        raise RuntimeError(f"Checkpoint {path} is not a JSON list")
    return {row["profile_url"]: row for row in rows if row.get("profile_url")}


def save_checkpoint(path: Path, rows_by_url: dict[str, dict[str, Any]]) -> None:
    ordered = sorted(rows_by_url.values(), key=lambda row: (row.get("fellowship_year") or "", row.get("slug") or ""))
    path.write_text(json.dumps(ordered, ensure_ascii=False, indent=2))


def download_all(args: argparse.Namespace) -> list[dict[str, Any]]:
    client = PackardClient()
    verify_program_amount_text(client)
    raw_path = args.output_dir / "packard_fellows_raw.json"
    rows_by_url = load_checkpoint(raw_path)
    if rows_by_url:
        log(f"Loaded checkpoint with {len(rows_by_url):,} parsed detail rows from {raw_path}")

    first_html = client.get(DIRECTORY_URL)
    first_links, advertised_total, last_page = parse_directory_links(first_html)
    if not advertised_total or advertised_total < EXPECTED_MIN_ROWS:
        raise RuntimeError(f"Unexpected advertised Packard fellow total: {advertised_total}")
    log(f"Directory advertises {advertised_total:,} fellows across {last_page} pages.")

    page_limit = min(last_page, args.max_pages) if args.max_pages else last_page
    all_urls: list[str] = []
    seen_urls: set[str] = set()
    for page in range(1, page_limit + 1):
        if page == 1:
            urls = first_links
        else:
            html = client.get(directory_page_url(page))
            urls, _, _ = parse_directory_links(html)
        log(f"Directory page {page}/{page_limit}: {len(urls)} fellow links")
        if not urls:
            raise RuntimeError(f"Directory page {page} had zero fellow links; refusing partial crawl.")
        for url in urls:
            if url not in seen_urls:
                seen_urls.add(url)
                all_urls.append(url)

    if args.max_pages is None and len(all_urls) != advertised_total:
        raise RuntimeError(
            f"Directory link count {len(all_urls):,} != advertised total {advertised_total:,}; "
            "inspect pagination before continuing."
        )

    downloaded_at = datetime.now(timezone.utc).isoformat()
    for idx, url in enumerate(all_urls, start=1):
        if url in rows_by_url:
            continue
        html = client.get(url)
        rows_by_url[url] = parse_detail_page(url, html, downloaded_at)
        if idx == 1 or idx % 25 == 0 or idx == len(all_urls):
            log(f"Parsed {idx:,}/{len(all_urls):,} detail pages")
        save_checkpoint(raw_path, rows_by_url)

    rows = [rows_by_url[url] for url in all_urls if url in rows_by_url]
    save_checkpoint(raw_path, {row["profile_url"]: row for row in rows})
    log(f"Cached {len(rows):,} rows to {raw_path}")
    return rows


def build_dataframe(rows: list[dict[str, Any]], full_run: bool) -> pd.DataFrame:
    if not rows:
        raise RuntimeError("No Packard fellow rows parsed.")
    df = pd.DataFrame(rows)

    required = ["funder_award_id", "full_name", "fellowship_year", "profile_url"]
    for column in required:
        missing = df[column].isna() | (df[column] == "")
        if missing.any():
            raise RuntimeError(f"{int(missing.sum())} rows missing required source field {column}")

    dup_mask = df["funder_award_id"].duplicated(keep=False)
    if dup_mask.any():
        raise RuntimeError(
            "Duplicate Packard funder_award_id values:\n"
            + df.loc[dup_mask, ["funder_award_id", "profile_url", "full_name"]].to_string(index=False)
        )

    years = pd.to_numeric(df["fellowship_year"], errors="coerce")
    if years.isna().any() or years.min() < 1988 or years.max() > datetime.now().year + 1:
        raise RuntimeError(f"Unexpected fellowship year range: {years.min()}-{years.max()}")

    if full_run and len(df) < EXPECTED_MIN_ROWS:
        raise RuntimeError(f"Full crawl produced only {len(df):,} rows; expected at least {EXPECTED_MIN_ROWS:,}.")

    return df.sort_values(["fellowship_year", "slug"]).astype("string")


def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    output_path = output_dir / "packard_fellows.parquet"
    df.to_parquet(output_path, index=False)
    log(f"Wrote {len(df):,} rows to {output_path} ({output_path.stat().st_size:,} bytes)")
    return output_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook §1.4: compare against the existing S3 parquet before upload."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError("boto3 is required for S3 upload; rerun with --skip-upload for local validation") from exc

    client = boto3.client("s3")
    log(f"§1.4 re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            log("No existing S3 parquet found; first ingest.")
            return True
        log(f"WARNING: S3 head_object failed with {code}; treating as first ingest.")
        return True

    prev_path = output_dir / "_prev_packard_fellows.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        previous_count = len(pd.read_parquet(prev_path))
    except Exception as exc:
        log(f"ERROR: couldn't read existing S3 parquet for shrink check: {exc}")
        return False
    finally:
        prev_path.unlink(missing_ok=True)

    log(f"Existing S3 rows: {previous_count:,}; new rows: {new_count:,}")
    if new_count < previous_count:
        if allow_shrink:
            log("WARNING: --allow-shrink set; proceeding despite smaller corpus.")
            return True
        log(
            f"ERROR: Runbook §1.4 violation: refusing to shrink Packard corpus "
            f"from {previous_count:,} to {new_count:,} rows."
        )
        return False
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path, allow_shrink: bool) -> bool:
    if not check_no_shrink(len(df), allow_shrink, output_dir):
        return False
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    log(f"Uploading {parquet_path} -> {s3_uri}")
    try:
        subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
    except FileNotFoundError:
        log("ERROR: aws CLI not found.")
        return False
    except subprocess.CalledProcessError as exc:
        log(f"ERROR: aws s3 cp failed with exit {exc.returncode}")
        return False
    log("Upload complete.")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Packard Fellows Directory -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/packard_fellows"))
    parser.add_argument("--skip-download", action="store_true", help="Reuse packard_fellows_raw.json")
    parser.add_argument("--skip-upload", action="store_true", help="Write local parquet only")
    parser.add_argument("--max-pages", type=int, default=None, help="Smoke-test mode: limit directory pages")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook §1.4 S3 shrink check")
    args = parser.parse_args()

    log("=" * 72)
    log("Packard Fellowships for Science and Engineering -> S3 starting")
    log(f"Source: {DIRECTORY_URL}")
    log(f"S3 target: s3://{S3_BUCKET}/{S3_KEY}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        raw_path = args.output_dir / "packard_fellows_raw.json"
        if not raw_path.exists():
            raise RuntimeError(f"--skip-download requested but {raw_path} does not exist")
        rows = json.loads(raw_path.read_text())
        log(f"Loaded {len(rows):,} cached raw rows from {raw_path}")
    else:
        rows = download_all(args)

    df = build_dataframe(rows, full_run=args.max_pages is None)
    log(
        "Coverage: "
        f"rows={len(df):,}, names={df['full_name'].notna().sum():,}, "
        f"years={df['fellowship_year'].notna().sum():,}, "
        f"disciplines={df['disciplines_text'].notna().sum():,}, "
        f"fellowship_institution={df['fellowship_institution'].notna().sum():,}, "
        f"profile_text={df['profile_text'].notna().sum():,}, "
        f"year_range={df['fellowship_year'].min()}-{df['fellowship_year'].max()}"
    )
    parquet_path = write_parquet(df, args.output_dir)

    if args.skip_upload:
        log("--skip-upload set; manual upload command:")
        log(f"aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")
        return

    if not upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink):
        sys.exit(7)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted.")
        sys.exit(130)
