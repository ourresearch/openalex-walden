#!/usr/bin/env python3
"""
Royal Society Medals to S3 (PRIZE PATTERN)
==========================================

Fetches Royal Society medal winners from the official Royal Society website.
This follows the awards runbook prize-pattern source-authority rule: prize rows
come from the awarding body directly, not from Wikipedia, Wikidata, or third
party lists.

Official source pages:
  - https://royalsociety.org/medals-and-prizes/copley-medal/
  - https://royalsociety.org/medals-and-prizes/royal-medals/

The pages render the first winner page server-side and use the same official
Sitecore endpoint for pagination:
  - https://royalsociety.org/api/sitecore/PastWinner/PostPastWinner

Output:
  s3://openalex-ingest/awards/royal_society_medals/royal_society_medals.parquet

Awarding body in OpenAlex:
  Royal Society (F4320320006)

Amount note:
  These are medals/prizes where the official pages do not publish a per-winner
  monetary award. The prize-pattern waiver applies: amount and currency are NULL
  and this is documented in the notebook/tracker.

Parsing notes:
  Royal Medals have multiple winners per year. This script emits one row per
  official page-list item, so a multi-winner medal year intentionally produces
  multiple OpenAlex award rows.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://royalsociety.org/"
PAST_WINNER_ENDPOINT = urljoin(BASE_URL, "api/sitecore/PastWinner/PostPastWinner")

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/royal_society_medals/royal_society_medals.parquet"
OUTPUT_FILE = "royal_society_medals.parquet"

FUNDER_ID = "4320320006"
FUNDER_DISPLAY_NAME = "Royal Society"
PROVENANCE = "royal_society_medals"

HEADERS = {
    "User-Agent": "openalex-walden/1.0 (+https://openalex.org)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
}
REQUEST_DELAY = 0.75
RETRIES = 5

MEDAL_SOURCES = [
    {
        "prize_name": "Copley Medal",
        "slug": "copley_medal",
        "url": "https://royalsociety.org/medals-and-prizes/copley-medal/",
    },
    {
        "prize_name": "Royal Medals",
        "slug": "royal_medals",
        "url": "https://royalsociety.org/medals-and-prizes/royal-medals/",
    },
]

ORG_HINTS = {
    "academy",
    "association",
    "center",
    "centre",
    "committee",
    "consortium",
    "foundation",
    "group",
    "institute",
    "institution",
    "laboratory",
    "organisation",
    "organization",
    "society",
    "team",
    "university",
}


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def collapse_text(value: str | None) -> str | None:
    if not value:
        return None
    value = value.replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(r"\s+([,;.:])", r"\1", value)
    return value or None


def slugify(value: str | None) -> str:
    value = (value or "").lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "unknown"


def looks_like_org(name: str | None) -> bool:
    if not name:
        return False
    lowered = name.lower()
    return any(hint in lowered for hint in ORG_HINTS)


def split_name(name: str | None) -> tuple[str | None, str | None]:
    """Simple prize-ingest name splitter; organization names stay intact."""
    if not name:
        return None, None
    if looks_like_org(name):
        return None, name
    tokens = name.split()
    suffixes = {
        "ba",
        "cbe",
        "ch",
        "dbe",
        "dphil",
        "dsc",
        "freng",
        "frs",
        "fmedsci",
        "honfreng",
        "jr",
        "jr.",
        "kt",
        "md",
        "obe",
        "om",
        "phd",
        "scd",
        "sr",
        "sr.",
        "ii",
        "iii",
        "iv",
    }
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if tokens and tokens[0].lower().strip(",.") in {"sir", "dame", "lord", "professor", "prof"}:
        tokens = tokens[1:]
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def load_checkpoint(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    pages = payload.get("pages") if isinstance(payload, dict) else None
    if not isinstance(pages, dict):
        return {}
    return {str(k): str(v) for k, v in pages.items()}


def save_checkpoint(path: Path, pages: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump({"pages": pages}, handle, indent=2, sort_keys=True)


def request_get(session: requests.Session, url: str) -> str:
    last_err: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        started = time.time()
        try:
            response = session.get(url, headers=HEADERS, timeout=30)
            elapsed = time.time() - started
            log(f"GET {url} -> {response.status_code} {len(response.content)} bytes in {elapsed:.1f}s")
            response.raise_for_status()
            if not response.content:
                raise RuntimeError(f"Empty response from {url}")
            return response.text
        except Exception as exc:  # noqa: BLE001 - retry transport/status failures.
            last_err = exc
            if attempt < RETRIES:
                sleep_s = 2 ** (attempt - 1)
                log(f"  retrying after {sleep_s}s: {exc}")
                time.sleep(sleep_s)
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


def request_post(session: requests.Session, url: str, payload: dict[str, Any], referer: str) -> str:
    last_err: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        started = time.time()
        try:
            headers = {**HEADERS, "Referer": referer}
            response = session.post(url, data=payload, headers=headers, timeout=30)
            elapsed = time.time() - started
            log(f"POST {url} {payload} -> {response.status_code} {len(response.content)} bytes in {elapsed:.1f}s")
            response.raise_for_status()
            return response.text
        except Exception as exc:  # noqa: BLE001 - retry transport/status failures.
            last_err = exc
            if attempt < RETRIES:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                sleep_s = 10 * attempt if status in {403, 429} else 2 ** (attempt - 1)
                log(f"  retrying after {sleep_s}s: {exc}")
                time.sleep(sleep_s)
    raise RuntimeError(f"Failed to post to {url}: {last_err}")


def cached_get(session: requests.Session, url: str, checkpoint_file: Path, use_cache: bool) -> str:
    cache = load_checkpoint(checkpoint_file)
    key = f"GET {url}"
    if use_cache and key in cache:
        log(f"cached {url} ({len(cache[key].encode('utf-8'))} bytes)")
        return cache[key]
    html = request_get(session, url)
    cache[key] = html
    save_checkpoint(checkpoint_file, cache)
    time.sleep(REQUEST_DELAY)
    return html


def cached_post(
    session: requests.Session,
    url: str,
    payload: dict[str, Any],
    referer: str,
    checkpoint_file: Path,
    use_cache: bool,
) -> str:
    cache = load_checkpoint(checkpoint_file)
    key = f"POST {url} {json.dumps(payload, sort_keys=True)}"
    if use_cache and key in cache:
        log(f"cached POST page {payload.get('page')} ({len(cache[key].encode('utf-8'))} bytes)")
        return cache[key]
    html = request_post(session, url, payload, referer)
    cache[key] = html
    save_checkpoint(checkpoint_file, cache)
    time.sleep(REQUEST_DELAY)
    return html


def page_metadata(page_html: str, source_url: str) -> dict[str, str | None]:
    soup = BeautifulSoup(page_html, "html.parser")
    post_display = soup.select_one(".post-display")
    data_source = soup.select_one("#dataSourceId")
    if not post_display or not data_source or not data_source.get("value"):
        raise RuntimeError(f"Could not find Royal Society past-winner data source on {source_url}")
    endpoint_path = post_display.get("data-url") or "/api/sitecore/PastWinner/PostPastWinner"
    title = collapse_text(soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else None)
    meta_desc = soup.find("meta", attrs={"name": "description"})
    summary = collapse_text(meta_desc.get("content") if meta_desc and meta_desc.get("content") else None)
    return {
        "data_source_id": str(data_source["value"]),
        "endpoint_url": urljoin(BASE_URL, str(endpoint_path)),
        "page_title": title,
        "page_summary": summary,
    }


def parse_winner_items(html: str, source: dict[str, str], page_number: int, fetched_at: str) -> list[dict[str, str | None]]:
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("li.expandable-list__item")
    rows: list[dict[str, str | None]] = []
    for position, item in enumerate(items, start=1):
        if not isinstance(item, Tag):
            continue
        year_el = item.select_one(".expandable-list__award strong")
        title_el = item.select_one(".expandable-list__title")
        role_el = item.select_one(".expandable-list__position")
        desc_el = item.select_one(".js-expandableItemText")
        image = item.select_one("img")

        year = collapse_text(year_el.get_text(" ", strip=True) if year_el else None)
        laureate_name = collapse_text(title_el.get_text(" ", strip=True) if title_el else None)
        laureate_role = collapse_text(role_el.get_text(" ", strip=True) if role_el else None)
        citation = collapse_text(desc_el.get_text(" ", strip=True) if desc_el else None)
        image_url = urljoin(source["url"], image.get("src")) if image and image.get("src") else None
        image_alt = collapse_text(image.get("alt") if image and image.get("alt") else None)

        if not year or not re.fullmatch(r"(?:17|18|19|20)\d{2}", year):
            raise RuntimeError(f"Could not parse award year on {source['url']} page {page_number}: {year!r}")
        if not laureate_name:
            raise RuntimeError(f"Could not parse laureate name on {source['url']} page {page_number}")

        given_name, family_name = split_name(laureate_name)
        funder_award_id = "-".join(
            [
                "royal-society",
                source["slug"],
                year,
                slugify(laureate_name),
            ]
        )

        rows.append(
            {
                "source_url": source["url"],
                "endpoint_url": source["endpoint_url"],
                "data_source_id": source["data_source_id"],
                "source_page_number": str(page_number),
                "source_item_position": str(position),
                "source_item_id": item.get("data-id"),
                "prize_name": source["prize_name"],
                "prize_slug": source["slug"],
                "prize_page_title": source.get("page_title"),
                "prize_page_summary": source.get("page_summary"),
                "award_year": year,
                "laureate_name": laureate_name,
                "given_name": given_name,
                "family_name": family_name,
                "is_organization_laureate": "true" if looks_like_org(laureate_name) else "false",
                "laureate_role": laureate_role,
                "citation": citation,
                "image_url": image_url,
                "image_alt": image_alt,
                "source_award_amount": None,
                "currency": None,
                "amount_rule_note": "Official Royal Society medal pages do not publish per-winner monetary amounts; amount/currency NULL under prize-pattern waiver.",
                "funder_award_id": funder_award_id,
                "landing_page_url": source["url"],
                "source_fetched_at": fetched_at,
                "funder_id": FUNDER_ID,
                "funder_display_name": FUNDER_DISPLAY_NAME,
                "provenance": PROVENANCE,
            }
        )
    return rows


def fetch_medal_rows(
    session: requests.Session,
    source: dict[str, str],
    checkpoint: Path,
    use_cache: bool,
    max_pages: int,
    fetched_at: str,
) -> list[dict[str, str | None]]:
    page_html = cached_get(session, source["url"], checkpoint, use_cache)
    metadata = page_metadata(page_html, source["url"])
    source = {**source, **metadata}
    log(f"{source['prize_name']}: dataSourceId={source['data_source_id']}")

    rows: list[dict[str, str | None]] = []
    empty_pages = 0
    for page_number in range(1, max_pages + 1):
        payload = {"dataSourceId": source["data_source_id"], "page": page_number}
        html = cached_post(session, source["endpoint_url"], payload, source["url"], checkpoint, use_cache)
        page_rows = parse_winner_items(html, source, page_number, fetched_at)
        if not page_rows:
            empty_pages += 1
            log(f"{source['prize_name']}: page {page_number} returned 0 rows (empty streak {empty_pages})")
            if empty_pages >= 2 and rows:
                break
            continue
        empty_pages = 0
        rows.extend(page_rows)
        years = [row["award_year"] for row in page_rows]
        log(
            f"{source['prize_name']}: page {page_number} parsed {len(page_rows)} rows "
            f"({years[0]} to {years[-1]}), total {len(rows)}"
        )

    if not rows:
        raise RuntimeError(f"No rows parsed for {source['prize_name']} from {source['url']}")
    return rows


def build_rows(session: requests.Session, checkpoint: Path, use_cache: bool, max_pages: int) -> list[dict[str, str | None]]:
    fetched_at = utc_now()
    rows: list[dict[str, str | None]] = []
    for source in MEDAL_SOURCES:
        rows.extend(fetch_medal_rows(session, source, checkpoint, use_cache, max_pages, fetched_at))

    ids = [row["funder_award_id"] for row in rows]
    duplicates = [award_id for award_id, count in Counter(ids).items() if count > 1]
    if duplicates:
        raise RuntimeError(f"Duplicate funder_award_id values: {duplicates[:10]}")
    return rows


def write_parquet(rows: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / OUTPUT_FILE
    df = pd.DataFrame(rows)
    df = df.astype("string")
    df.to_parquet(output_path, index=False)
    log(f"Wrote {len(df)} rows to {output_path}")
    log(
        "Coverage: "
        f"{df['laureate_name'].notna().sum()}/{len(df)} names, "
        f"{df['award_year'].notna().sum()}/{len(df)} years, "
        f"{df['prize_name'].notna().sum()}/{len(df)} prize names, "
        f"{df['citation'].notna().sum()}/{len(df)} citations, "
        f"{df['laureate_role'].notna().sum()}/{len(df)} role/title strings, "
        f"{df['source_award_amount'].notna().sum()}/{len(df)} amounts, "
        f"{df['currency'].notna().sum()}/{len(df)} currencies"
    )
    return output_path


def upload_to_s3(path: Path) -> None:
    import boto3  # type: ignore[import-not-found]

    client = boto3.client("s3")
    client.upload_file(str(path), S3_BUCKET, S3_KEY)
    log(f"Uploaded {path} to s3://{S3_BUCKET}/{S3_KEY}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Royal Society medal winners for OpenAlex awards ingest.")
    parser.add_argument("--output-dir", default="/tmp/openalex-awards/royal_society_medals", help="Directory for parquet output")
    parser.add_argument("--checkpoint", default="/tmp/openalex-awards/royal_society_medals/checkpoint.json", help="HTML checkpoint path")
    parser.add_argument("--no-cache", action="store_true", help="Ignore checkpointed HTML and refetch all pages")
    parser.add_argument("--skip-upload", action="store_true", help="Write local parquet but do not upload to S3")
    parser.add_argument("--max-pages", type=int, default=100, help="Safety cap for official endpoint pagination per medal")
    args = parser.parse_args()

    session = requests.Session()
    rows = build_rows(session, Path(args.checkpoint), use_cache=not args.no_cache, max_pages=args.max_pages)
    output_path = write_parquet(rows, Path(args.output_dir))
    if args.skip_upload:
        log("Skipping S3 upload by request")
    else:
        upload_to_s3(output_path)


if __name__ == "__main__":
    main()
