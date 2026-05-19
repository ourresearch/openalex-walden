#!/usr/bin/env python3
"""
Dan David Prize to S3 (PRIZE PATTERN)
=====================================

Fetches current-format Dan David Prize winners from the awarding body's
official website. This follows the awards runbook's prize-pattern
source-authority rule: prize rows come from the awarding body directly, not
from Wikipedia, Wikidata, news articles, or third-party biographies.

Official source pages:
  - https://dandavidprize.org/our-winners/
  - https://dandavidprize.org/our-winners/?getby=cat&cat={year}
  - https://dandavidprize.org/                         (amount rule)

Output:
  s3://openalex-ingest/awards/dan_david_prize/dan_david_prize_winners.parquet

Awarding body in OpenAlex:
  Dan David Prize (F4320320949, DOI 10.13039/501100001741)

Scope note:
  The Dan David Prize site has a distinct "The Prize, 2001-2021" section for
  the legacy three-prize format. This script intentionally ingests only the
  current 2022-present "Our Winners" program because the official source
  exposes it as a structured, year-filtered winner list and the amount rule
  is unambiguous: up to nine annual prizes of $300,000 each. The legacy corpus
  should be handled in a separate follow-up if desired.

Parsing notes:
  The year-filtered winner pages render server-side. Each winner card exposes
  the winner name, year, research topic, institution/affiliation, and profile
  URL. The script also fetches each official profile page to preserve the
  richer biography text where available.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://dandavidprize.org/"
WINNERS_URL = urljoin(BASE_URL, "our-winners/")
AMOUNT_RULE_URL = BASE_URL

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/dan_david_prize/dan_david_prize_winners.parquet"
OUTPUT_FILE = "dan_david_prize_winners.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.2
RETRIES = 3

SOURCE_AWARD_AMOUNT = "300000"
CURRENCY = "USD"


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


def split_name(name: str | None) -> tuple[str | None, str | None]:
    """Canonical simple prize-ingest splitter used by nearby prize scripts."""
    if not name:
        return None, None
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def request_html(session: requests.Session, url: str) -> str:
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


def get_page(session: requests.Session, url: str, checkpoint_file: Path, use_cache: bool) -> str:
    cache = load_checkpoint(checkpoint_file)
    if use_cache and url in cache:
        log(f"cached {url} ({len(cache[url].encode('utf-8'))} bytes)")
        return cache[url]
    html = request_html(session, url)
    cache[url] = html
    save_checkpoint(checkpoint_file, cache)
    time.sleep(REQUEST_DELAY)
    return html


def discover_years(index_html: str) -> list[int]:
    """Read year filters from the official winners index page."""
    soup = BeautifulSoup(index_html, "html.parser")
    years: set[int] = set()
    for anchor in soup.find_all("a", href=True):
        href = urljoin(WINNERS_URL, anchor["href"])
        parsed = urlparse(href)
        query_years = parse_qs(parsed.query).get("cat", [])
        for raw in query_years:
            if re.fullmatch(r"20\d{2}", raw):
                years.add(int(raw))
        text = collapse_text(anchor.get_text(" ", strip=True))
        if text and re.fullmatch(r"20\d{2}", text):
            years.add(int(text))
    return sorted((year for year in years if year >= 2022), reverse=True)


def verify_amount_rule(home_html: str) -> None:
    text = BeautifulSoup(home_html, "html.parser").get_text(" ", strip=True)
    checks = [
        "$3 million" in text,
        "$300,000" in text,
        "up to nine prizes" in text.lower(),
    ]
    if not all(checks):
        raise RuntimeError(
            "Could not verify the current Dan David Prize amount rule on "
            f"{AMOUNT_RULE_URL}. Expected '$3 million', '$300,000', and "
            "'up to nine prizes' in the page text."
        )


def profile_slug(profile_url: str | None) -> str:
    if not profile_url:
        return "unknown"
    parsed = urlparse(profile_url)
    parts = [part for part in parsed.path.split("/") if part]
    return slugify(parts[-1] if parts else profile_url)


def parse_winner_cards(year: int, html: str, source_url: str) -> list[dict[str, str | None]]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(".c-post-card")
    log(f"Parsed {len(cards)} winner cards for {year}")
    rows: list[dict[str, str | None]] = []
    for position, card in enumerate(cards, start=1):
        if not isinstance(card, Tag):
            continue
        texts = [collapse_text(t) for t in card.stripped_strings]
        texts = [t for t in texts if t and t != "Read More"]
        if len(texts) < 4:
            log(f"  skipping card {position} on {year}: expected >=4 text parts, got {texts!r}")
            continue

        anchor = card.select_one('a[href*="/winners/"]')
        landing_page_url = urljoin(source_url, anchor["href"]) if anchor and anchor.get("href") else None

        # Official cards are name, year, topic, affiliation/institution.
        name = texts[0]
        card_year = texts[1] if len(texts) > 1 else str(year)
        topic = texts[2] if len(texts) > 2 else None
        affiliation = texts[3] if len(texts) > 3 else None
        if card_year != str(year):
            raise RuntimeError(f"Card year mismatch on {source_url}: expected {year}, got {card_year}")

        given_name, family_name = split_name(name)
        slug = profile_slug(landing_page_url)
        rows.append(
            {
                "funder_award_id": f"dan-david-prize-{year}-{slug}",
                "award_year": str(year),
                "laureate_name": name,
                "given_name": given_name,
                "family_name": family_name,
                "research_topic": topic,
                "affiliation": affiliation,
                "landing_page_url": landing_page_url,
                "profile_slug": slug,
                "winner_position": str(position),
                "source_award_amount": SOURCE_AWARD_AMOUNT,
                "currency": CURRENCY,
                "amount_rule_url": AMOUNT_RULE_URL,
                "source_url": source_url,
                "current_format_note": "Current 2022-present Dan David Prize format; legacy 2001-2021 format excluded.",
                "fetched_at": utc_now(),
            }
        )
    return rows


def parse_profile(profile_html: str, laureate_name: str, card_affiliation: str | None) -> dict[str, str | None]:
    soup = BeautifulSoup(profile_html, "html.parser")
    article = soup.find("article") or soup.find("main")
    if not article:
        return {"profile_title_affiliation": None, "profile_description": None}

    title_affiliation: str | None = None
    paragraphs: list[str] = []
    for element in article.find_all(["h1", "p"]):
        text = collapse_text(element.get_text(" ", strip=True))
        if not text:
            continue
        if element.name == "h1":
            if text.upper() == "OTHER WINNERS":
                break
            continue
        if not title_affiliation:
            title_affiliation = text
            continue
        if text == card_affiliation or text == laureate_name:
            continue
        if text.startswith("Stay up to date"):
            break
        paragraphs.append(text)

    description = collapse_text(" ".join(paragraphs))
    return {
        "profile_title_affiliation": title_affiliation,
        "profile_description": description,
    }


def fetch_rows(max_years: int | None, checkpoint_file: Path, use_cache: bool) -> list[dict[str, str | None]]:
    session = requests.Session()

    home_html = get_page(session, AMOUNT_RULE_URL, checkpoint_file, use_cache)
    verify_amount_rule(home_html)

    index_html = get_page(session, WINNERS_URL, checkpoint_file, use_cache)
    years = discover_years(index_html)
    if max_years is not None:
        years = years[:max_years]
    if not years:
        raise RuntimeError(f"No current-format Dan David Prize years found on {WINNERS_URL}")
    log(f"Discovered current-format years: {', '.join(str(y) for y in years)}")

    all_rows: list[dict[str, str | None]] = []
    for idx, year in enumerate(years, start=1):
        year_url = f"{WINNERS_URL}?getby=cat&cat={year}"
        html = get_page(session, year_url, checkpoint_file, use_cache)
        rows = parse_winner_cards(year, html, year_url)
        if not rows:
            raise RuntimeError(f"No winner rows parsed for {year} from {year_url}")
        log(f"Processed year {year}: {len(rows)} rows ({idx}/{len(years)} years)")
        all_rows.extend(rows)

    total = len(all_rows)
    for idx, row in enumerate(all_rows, start=1):
        landing_page_url = row.get("landing_page_url")
        if not landing_page_url:
            continue
        profile_html = get_page(session, landing_page_url, checkpoint_file, use_cache)
        row.update(parse_profile(profile_html, row["laureate_name"] or "", row.get("affiliation")))
        if idx % 10 == 0 or idx == total:
            log(f"Fetched profile details for {idx}/{total} winners")

    return all_rows


def validate_rows(rows: list[dict[str, str | None]]) -> None:
    if not rows:
        raise RuntimeError("No rows parsed from Dan David Prize source pages")
    award_ids = [row["funder_award_id"] for row in rows]
    duplicates = sorted({award_id for award_id in award_ids if award_ids.count(award_id) > 1})
    if duplicates:
        raise RuntimeError(f"Duplicate funder_award_id values detected: {duplicates[:10]}")

    required = ["award_year", "laureate_name", "research_topic", "landing_page_url", "source_award_amount", "currency"]
    for column in required:
        missing = [row["funder_award_id"] for row in rows if not row.get(column)]
        if missing:
            raise RuntimeError(f"Missing required source field {column} on {len(missing)} rows: {missing[:10]}")


def write_parquet(rows: list[dict[str, str | None]], output_path: Path) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df = df.sort_values(["award_year", "winner_position", "laureate_name"], ascending=[False, True, True])
    df = df.astype("string")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    log(f"Wrote {len(df)} rows to {output_path}")
    return df


def upload_to_s3(output_path: Path) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for S3 upload; rerun with --skip-upload for local validation") from exc
    log(f"Uploading {output_path} to s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(output_path), S3_BUCKET, S3_KEY)
    log("S3 upload complete")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Dan David Prize winners and write parquet for OpenAlex awards ingest.")
    parser.add_argument("--output-dir", default="/tmp/openalex-awards/dan_david_prize", help="Directory for parquet output")
    parser.add_argument("--checkpoint", default="/tmp/openalex-awards/dan_david_prize/checkpoint.json", help="HTML checkpoint path")
    parser.add_argument("--no-cache", action="store_true", help="Ignore checkpointed HTML and refetch all pages")
    parser.add_argument("--skip-upload", action="store_true", help="Write local parquet but do not upload to S3")
    parser.add_argument("--max-years", type=int, default=None, help="Optional smoke-test limit on number of official year pages")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = Path(args.output_dir) / OUTPUT_FILE
    checkpoint_file = Path(args.checkpoint)

    rows = fetch_rows(max_years=args.max_years, checkpoint_file=checkpoint_file, use_cache=not args.no_cache)
    validate_rows(rows)
    df = write_parquet(rows, output_path)

    log(
        "Coverage: "
        f"{df['laureate_name'].notna().sum()}/{len(df)} names, "
        f"{df['award_year'].notna().sum()}/{len(df)} years, "
        f"{df['research_topic'].notna().sum()}/{len(df)} topics, "
        f"{df['affiliation'].notna().sum()}/{len(df)} affiliations, "
        f"{df['profile_description'].notna().sum()}/{len(df)} profile descriptions, "
        f"{df['source_award_amount'].notna().sum()}/{len(df)} amounts, "
        f"{df['currency'].notna().sum()}/{len(df)} currencies"
    )

    if args.skip_upload:
        log("Skipping S3 upload by request")
    else:
        upload_to_s3(output_path)


if __name__ == "__main__":
    main()
