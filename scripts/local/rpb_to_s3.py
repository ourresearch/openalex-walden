#!/usr/bin/env python3
"""
Research to Prevent Blindness grantees -> S3 parquet.

Source authority
----------------
Official Research to Prevent Blindness public grantee archive:

    https://www.rpbusa.org/grantees/

The page is a WordPress archive for individual RPB grantees. The static
archive page lists filters and the first page, while its own site JavaScript
uses the first-party WordPress AJAX endpoint for pagination:

    /wp-admin/admin-ajax.php?action=get_filtered_posts
        &post_type=individual-grantees&paged=N&pagination_id=0

This script follows that same public AJAX path. Each card exposes the
awardee name, institution, award name, award year, amount, research area, and
project/research description. The source does not publish native grant
numbers, so funder_award_id is a deterministic source-derived key built from
the visible award fields. Collision checks raise.

This ingest intentionally covers the historical Individual Grantees archive
only. The same page also shows current Departmental Grantees, but the site
labels that section as "current grant year only"; including it would mix an
incomplete current-only org panel into the historical individual-award corpus.

Output
------
    s3://openalex-ingest/awards/rpb/rpb_grantees.parquet

Usage
-----
    python scripts/local/rpb_to_s3.py --skip-upload
    python scripts/local/rpb_to_s3.py --limit 10 --skip-upload
    python scripts/local/rpb_to_s3.py --skip-download --skip-upload
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import sys
import time
from collections import Counter
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- Windows UTF-8 compatibility shim (fleet 2026-05-22) -----------------
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if sys.platform == "win32":
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
# --- end shim -------------------------------------------------------------


BASE_URL = "https://www.rpbusa.org"
ARCHIVE_URL = f"{BASE_URL}/grantees/"
AJAX_URL = f"{BASE_URL}/wp-admin/admin-ajax.php"

FUNDER_ID = 4320306811
FUNDER_DISPLAY_NAME = "Research to Prevent Blindness"
PROVENANCE = "rpb_grantees"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/rpb/rpb_grantees.parquet"

USER_AGENT = "Mozilla/5.0 (openalex-walden-rpb-ingest/1.0; +https://openalex.org)"
REQUEST_DELAY_S = 0.2
MAX_CONSECUTIVE_EMPTY = 3
MAX_CONSECUTIVE_NON200 = 5
DEFAULT_MAX_PAGES = 260
DEFAULT_CACHE = Path(".cache/rpb_grantees.json")

DEGREE_SUFFIX_RE = re.compile(
    r"(?:,?\s+\b(?:MD|M\.D\.|PhD|Ph\.D\.|DPhil|D\.Phil\.|DO|D\.O\.|OD|O\.D\.|DDS|DVM|MBBS|MHS|MPH|MPhil|M\.Phil\.|MSc|MS|M\.S\.|BA|B\.A\.|BS|B\.S\.|FRCS|FACS|ScD|Sc\.D\.|III|II|IV)\b\.?)+$",
    re.IGNORECASE,
)
WHITESPACE_RE = re.compile(r"\s+")
MONEY_RE = re.compile(r"-?\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)")


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = html.unescape(str(value))
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text or None


def slugify(value: str | None, max_len: int = 80) -> str:
    if not value:
        return "missing"
    text = html.unescape(value).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return (text[:max_len].strip("-") or "missing")


def parse_amount(value: str | None) -> str | None:
    if not value:
        return None
    match = MONEY_RE.search(value)
    if not match:
        return None
    try:
        amount = float(match.group(1).replace(",", ""))
    except ValueError:
        return None
    if amount <= 0:
        return None
    if amount.is_integer():
        return str(int(amount))
    return f"{amount:.2f}".rstrip("0").rstrip(".")


def strip_degrees(raw_name: str | None) -> str | None:
    if not raw_name:
        return None
    name = clean_text(raw_name)
    if not name:
        return None
    name = re.sub(r"\s+Opens third-party bio in new tab.*$", "", name, flags=re.IGNORECASE)
    if "/" in name:
        name = name.split("/", 1)[0]
    name = DEGREE_SUFFIX_RE.sub("", name).strip(" ,/")
    return clean_text(name)


def split_source_name(raw_name: str | None) -> tuple[str | None, str | None, str | None]:
    """RPB publishes names mostly as family-first: 'Alekseev, Oleg / MD, PhD'."""
    cleaned = strip_degrees(raw_name)
    if not cleaned:
        return None, None, None
    if "," in cleaned:
        family, given = cleaned.split(",", 1)
        family = clean_text(family)
        given = strip_degrees(given)
        return given, family, cleaned

    # Rare fallback for records like "Li, Wei PhD" after suffix stripping quirks or
    # old records without commas. Avoid overfitting: keep the raw cleaned name too.
    parts = cleaned.split()
    if len(parts) == 1:
        return None, parts[0], cleaned
    return " ".join(parts[:-1]), parts[-1], cleaned


class RPBClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Referer": ARCHIVE_URL,
        })
        self.last_request = 0.0

    def get(self, url: str, *, params: dict[str, Any] | None = None, timeout: int = 30) -> requests.Response:
        wait = REQUEST_DELAY_S - (time.monotonic() - self.last_request)
        if wait > 0:
            time.sleep(wait)
        response = self.session.get(url, params=params, timeout=timeout)
        self.last_request = time.monotonic()
        return response


def get_total_pages(client: RPBClient) -> int:
    response = client.get(ARCHIVE_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    pages = []
    for node in soup.select('.posts-pagination:not(.dept-grantees) .page-numbers'):
        text = clean_text(node.get_text(" ", strip=True))
        if text and text.isdigit():
            pages.append(int(text))
    total = max(pages) if pages else DEFAULT_MAX_PAGES
    log(f"Detected {total} individual-grantee pages from archive pagination")
    return total


def field_result(dialog: BeautifulSoup, css_class: str) -> str | None:
    box = dialog.select_one(f".{css_class}")
    if not box:
        return None
    return clean_text(box.select_one(".result").get_text(" ", strip=True) if box.select_one(".result") else box.get_text(" ", strip=True))


def visible_node_text(node: BeautifulSoup | None) -> str | None:
    if not node:
        return None
    copy = BeautifulSoup(str(node), "html.parser")
    for hidden in copy.select(".visually-hidden"):
        hidden.decompose()
    return clean_text(copy.get_text(" ", strip=True))


def parse_card(card: BeautifulSoup, page: int, idx: int) -> dict[str, Any] | None:
    dialog = card.select_one("dialog") or card
    raw_name = visible_node_text(dialog.select_one(".grantee-info > .text-h3") or card.select_one(".grantee"))
    given_name, family_name, cleaned_name = split_source_name(raw_name)
    institution_link = dialog.select_one("a.institution-link")
    institution = visible_node_text(institution_link)
    institution_url = institution_link.get("href") if institution_link else None
    award_name = field_result(dialog, "award-name") or clean_text(card.select_one(".grant-name").get_text(" ", strip=True) if card.select_one(".grant-name") else None)
    grant_type = field_result(dialog, "grant-type")
    award_year = field_result(dialog, "grant-year")
    amount_raw = field_result(dialog, "grant-amount")
    amount = parse_amount(amount_raw)
    research_areas = [clean_text(x.get_text(" ", strip=True)) for x in dialog.select(".research-area .area-item")]
    research_areas = [x for x in research_areas if x]
    description = clean_text(dialog.select_one(".description-research").get_text(" ", strip=True) if dialog.select_one(".description-research") else None)

    if not raw_name or not award_name or not award_year:
        return None
    try:
        award_year_int = int(award_year)
    except (TypeError, ValueError):
        award_year_int = None

    key_material = "|".join([
        cleaned_name or raw_name or "",
        award_name or "",
        str(award_year_int or award_year or ""),
        institution or "",
        amount or "",
        description or "",
    ])
    digest = hashlib.sha1(key_material.encode("utf-8")).hexdigest()[:16]
    funder_award_id = f"rpb-{award_year_int or 'unknown'}-{slugify(award_name, 40)}-{digest}"

    return {
        "source_page": page,
        "source_card_index": idx,
        "source_archive_url": ARCHIVE_URL,
        "landing_page_url": f"{ARCHIVE_URL}?pn={page}#card-{idx}",
        "funder_award_id": funder_award_id,
        "raw_awardee_name": raw_name,
        "awardee_name_clean": cleaned_name,
        "given_name": given_name,
        "family_name": family_name,
        "institution_name": institution,
        "institution_url": institution_url,
        "grant_type": grant_type,
        "award_name": award_name,
        "award_year": award_year_int,
        "amount": amount,
        "amount_raw": amount_raw,
        "currency": "USD" if amount else None,
        "research_area": "; ".join(research_areas) if research_areas else None,
        "description": description,
        "provenance": PROVENANCE,
    }


def fetch_page(client: RPBClient, page: int) -> tuple[int, str]:
    params = {
        "action": "get_filtered_posts",
        "post_type": "individual-grantees",
        "paged": page,
        "pagination_id": 0,
    }
    response = client.get(AJAX_URL, params=params)
    return response.status_code, response.text


def scrape_grantees(limit_pages: int | None = None) -> list[dict[str, Any]]:
    client = RPBClient()
    total_pages = get_total_pages(client)
    if limit_pages:
        total_pages = min(total_pages, limit_pages)
        log(f"Limit active: scraping first {total_pages} pages")

    rows: list[dict[str, Any]] = []
    consecutive_empty = 0
    consecutive_non200 = 0

    for page in range(1, total_pages + 1):
        status, body = fetch_page(client, page)
        if status != 200:
            consecutive_non200 += 1
            log(f"Page {page}: HTTP {status}; consecutive_non200={consecutive_non200}")
            if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError(f"Aborting after {consecutive_non200} consecutive non-200 pages")
            continue
        consecutive_non200 = 0

        soup = BeautifulSoup(body, "html.parser")
        cards = soup.select(".grantee-card")
        if not cards:
            consecutive_empty += 1
            log(f"Page {page}: 0 cards; consecutive_empty={consecutive_empty}")
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                raise RuntimeError(f"Aborting after {consecutive_empty} consecutive empty pages before expected end")
            continue
        consecutive_empty = 0

        parsed = [parse_card(card, page, idx) for idx, card in enumerate(cards)]
        parsed = [row for row in parsed if row]
        rows.extend(parsed)
        if page == 1 or page % 10 == 0 or page == total_pages:
            log(f"Page {page}/{total_pages}: +{len(parsed)} rows (total {len(rows):,})")

    return rows


def load_or_download(skip_download: bool, cache_path: Path, limit_pages: int | None) -> list[dict[str, Any]]:
    if skip_download:
        if not cache_path.exists():
            raise FileNotFoundError(f"--skip-download requested but cache is missing: {cache_path}")
        rows = json.loads(cache_path.read_text())
        if limit_pages:
            rows = [r for r in rows if int(r.get("source_page") or 0) <= limit_pages]
        log(f"Loaded {len(rows):,} rows from cache {cache_path}")
        return rows

    rows = scrape_grantees(limit_pages)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2))
    log(f"Wrote cache {cache_path} ({len(rows):,} rows)")
    return rows


def validate_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise RuntimeError("No RPB grantee rows parsed")

    n = len(rows)
    ids = [r["funder_award_id"] for r in rows if r.get("funder_award_id")]
    duplicate_ids = [k for k, v in Counter(ids).items() if v > 1]
    if duplicate_ids:
        raise RuntimeError(f"funder_award_id collisions: {duplicate_ids[:10]}")

    required = ["raw_awardee_name", "given_name", "family_name", "award_name", "award_year", "institution_name", "amount", "currency", "description"]
    for field in required:
        count = sum(1 for row in rows if row.get(field) not in (None, "", []))
        log(f"{field:<18} coverage {count:>5}/{n:<5} ({count * 100 / n:5.1f}%)")

    years = [int(r["award_year"]) for r in rows if r.get("award_year")]
    if years:
        log(f"Award year range: {min(years)}-{max(years)}")

    amount_values = [float(r["amount"]) for r in rows if r.get("amount")]
    if amount_values:
        log(f"Amount range: ${min(amount_values):,.0f}-${max(amount_values):,.0f}; total ${sum(amount_values):,.0f}")

    awards = Counter(r.get("award_name") for r in rows if r.get("award_name"))
    log("Top award types:")
    for award, count in awards.most_common(10):
        log(f"  {award}: {count}")

    name_counts = Counter((r.get("given_name"), r.get("family_name")) for r in rows if r.get("family_name"))
    prolific = [(name, count) for name, count in name_counts.most_common(20) if count > 5]
    if prolific:
        log("Top repeated awardee names for notebook §6.4a review:")
        for (given, family), count in prolific:
            log(f"  {given or ''} {family or ''}: {count}")


def build_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    validate_rows(rows)
    df = pd.DataFrame(rows)
    ordered_cols = [
        "source_page",
        "source_card_index",
        "source_archive_url",
        "landing_page_url",
        "funder_award_id",
        "raw_awardee_name",
        "awardee_name_clean",
        "given_name",
        "family_name",
        "institution_name",
        "institution_url",
        "grant_type",
        "award_name",
        "award_year",
        "amount",
        "amount_raw",
        "currency",
        "research_area",
        "description",
        "provenance",
    ]
    for col in ordered_cols:
        if col not in df.columns:
            df[col] = None
    return df[ordered_cols]


def check_no_shrink(new_count: int, allow_shrink: bool) -> None:
    if allow_shrink:
        log("--allow-shrink set; skipping S3 shrink guard")
        return
    try:
        import boto3

        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        old_df = pd.read_parquet(BytesIO(obj["Body"].read()))
        old_count = len(old_df)
    except Exception as exc:
        log(f"No prior S3 parquet readable for shrink check ({type(exc).__name__}); continuing")
        return

    if new_count < old_count:
        raise RuntimeError(
            f"Shrink guard blocked upload: new row count {new_count:,} < existing {old_count:,}. "
            "Re-run with --allow-shrink only after confirming the source truly shrank."
        )
    log(f"S3 shrink check passed: new {new_count:,} >= existing {old_count:,}")


def upload_to_s3(local_path: Path, skip_upload: bool, allow_shrink: bool) -> None:
    if skip_upload:
        log("--skip-upload set; not uploading to S3")
        return

    check_no_shrink(len(pd.read_parquet(local_path)), allow_shrink)

    import boto3

    s3 = boto3.client("s3")
    log(f"Uploading {local_path} to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(local_path), S3_BUCKET, S3_KEY)
    log("Upload complete")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download RPB grantee archive to parquet/S3")
    parser.add_argument("--limit", type=int, default=None, help="Limit to the first N source pages for smoke tests")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/rpb_awards"), help="Directory for parquet/cache outputs")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached JSON instead of fetching the source")
    parser.add_argument("--skip-upload", action="store_true", help="Write local parquet but do not upload to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Allow uploading fewer rows than the previous S3 parquet")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    cache_path = args.output_dir / DEFAULT_CACHE.name
    parquet_path = args.output_dir / "rpb_grantees.parquet"

    log(f"Starting RPB grantee ingest (funder_id={FUNDER_ID}, provenance={PROVENANCE})")
    rows = load_or_download(args.skip_download, cache_path, args.limit)
    df = build_dataframe(rows)

    # Required by runbook §1.2.5 immediately before parquet write.
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df):,} rows to {parquet_path}")

    upload_to_s3(parquet_path, args.skip_upload, args.allow_shrink)


if __name__ == "__main__":
    main()
