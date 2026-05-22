#!/usr/bin/env python3
"""
The Brain Prize to S3 (PRIZE PATTERN)
=====================================

Fetches The Brain Prize laureates from the official Brain Prize / Lundbeck
Foundation Drupal site. This follows the awards runbook's prize-pattern
source-authority rule: prize rows come from the awarding body's own site, not
from Wikipedia, Wikidata, news summaries, or other third-party biographies.

Official source pages:
  - https://brainprize.org/winners
  - https://brainprize.org/about-the-brain-prize
  - yearly winner pages and laureate autobiography/profile pages linked from
    the official winners page.

Output:
  s3://openalex-ingest/awards/brain_prize/brain_prize_laureates.parquet

Awarding body in OpenAlex:
  Lundbeckfonden (F4320321999, ROR https://ror.org/03hz8wd80,
  DOI 10.13039/501100003554)

Amount rule:
  The official "About The Brain Prize" page states that recipients receive a
  gold medal and share the prize of DKK 10m. The script stores the official
  yearly total as source_total_award_amount=10000000, the per-year
  laureate_count, and portion=1/laureate_count. The notebook maps amount to
  source_total_award_amount * portion.

Parsing notes:
  The /winners page contains the authoritative year/topic/laureate listing.
  Each yearly page links to laureate profile pages, which carry affiliation
  and biography text. Affiliation is preserved as source text; country is not
  separately mapped because the source does not publish a structured country
  field for all laureates.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://brainprize.org/"
WINNERS_URL = urljoin(BASE_URL, "winners")
AMOUNT_RULE_URL = urljoin(BASE_URL, "about-the-brain-prize")
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/brain_prize/brain_prize_laureates.parquet"
OUTPUT_FILE = "brain_prize_laureates.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.2
RETRIES = 3

SOURCE_TOTAL_AWARD_AMOUNT = "10000000"
CURRENCY = "DKK"


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def collapse_text(value: str | None) -> str | None:
    if not value:
        return None
    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(r"\s+([,;.:])", r"\1", value)
    return value or None


def slugify(value: str | None) -> str:
    value = (value or "").lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "unknown"


def split_name(name: str | None) -> tuple[str | None, str | None]:
    """Small, conservative splitter used by existing prize ingests."""
    if not name:
        return None, None
    tokens = name.split()
    prefixes = {"prof.", "professor", "dr.", "dr", "sir", "dame"}
    while tokens and tokens[0].lower().strip(",.") in prefixes:
        tokens.pop(0)
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def meta_content(soup: BeautifulSoup, name: str) -> str | None:
    tag = soup.find("meta", attrs={"name": name}) or soup.find("meta", attrs={"property": f"og:{name}"})
    if not tag:
        return None
    return collapse_text(tag.get("content"))


def load_checkpoint(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        return {}
    pages = payload.get("pages")
    if not isinstance(pages, dict):
        return {}
    return {str(k): str(v) for k, v in pages.items()}


def save_checkpoint(path: Path, pages: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump({"pages": pages}, handle, indent=2, sort_keys=True)


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
        except Exception as exc:  # noqa: BLE001 - retry transport/status/content failures.
            last_err = exc
            if attempt < RETRIES:
                sleep_s = 2 ** (attempt - 1)
                log(f"  retrying after {sleep_s}s: {exc}")
                time.sleep(sleep_s)
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


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


def parse_year_teasers(winners_html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(winners_html, "html.parser")
    blocks = soup.select("main article .node--type-brain-prize-year.node--view-mode-teaser")
    if not blocks:
        raise RuntimeError("No Brain Prize year teaser blocks found on /winners")

    years: list[dict[str, Any]] = []
    for block in blocks:
        year_text = collapse_text(block.select_one(".field-year").get_text(" ", strip=True) if block.select_one(".field-year") else None)
        title = block.select_one(".node-title a")
        if not year_text or not title:
            continue
        year_match = re.search(r"\b((?:19|20)\d{2})\b", year_text)
        if not year_match:
            continue
        winner_names = [
            collapse_text(item.get_text(" ", strip=True))
            for item in block.select(".field-winners-ref .node-title")
        ]
        winner_names = [name for name in winner_names if name]
        years.append({
            "award_year": year_match.group(1),
            "award_topic": collapse_text(title.get_text(" ", strip=True)),
            "year_page_url": urljoin(WINNERS_URL, title.get("href")),
            "winner_names": winner_names,
        })

    if not years:
        raise RuntimeError("No Brain Prize year rows parsed from /winners")
    return years


def parse_profile_links(year_url: str, year_html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(year_html, "html.parser")
    links: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for anchor in soup.select("main article .node--type-research-project-person a[href]"):
        label = collapse_text(anchor.get_text(" ", strip=True))
        href = urljoin(year_url, anchor.get("href"))
        if not label or label.lower().startswith("read "):
            continue
        if href in seen_urls:
            continue
        seen_urls.add(href)
        links.append({"laureate_name": label, "profile_url": href})

    return links


def parse_profile(profile_url: str, profile_html: str) -> dict[str, str | None]:
    soup = BeautifulSoup(profile_html, "html.parser")
    h1 = soup.select_one("h1")
    name = collapse_text(h1.get_text(" ", strip=True) if h1 else None) or meta_content(soup, "title")
    affiliation = collapse_text(
        soup.select_one(".field-applicant-institution").get_text(" ", strip=True)
        if soup.select_one(".field-applicant-institution")
        else None
    )
    intro = collapse_text(
        soup.select_one(".field-intro").get_text(" ", strip=True)
        if soup.select_one(".field-intro")
        else None
    )
    description = meta_content(soup, "description")
    given_name, family_name = split_name(name)
    return {
        "profile_url": profile_url,
        "laureate_name": name,
        "given_name": given_name,
        "family_name": family_name,
        "affiliation": affiliation,
        "profile_description": description,
        "profile_bio": intro,
    }


def verify_amount_rule(about_html: str) -> None:
    text = collapse_text(BeautifulSoup(about_html, "html.parser").get_text(" ", strip=True))
    if not text or "DKK 10m" not in text:
        raise RuntimeError("Could not verify official Brain Prize amount rule text on /about-the-brain-prize")


def build_rows(session: requests.Session, checkpoint_file: Path, use_cache: bool, limit: int | None) -> list[dict[str, str | None]]:
    winners_html = get_page(session, WINNERS_URL, checkpoint_file, use_cache)
    about_html = get_page(session, AMOUNT_RULE_URL, checkpoint_file, use_cache)
    verify_amount_rule(about_html)

    year_rows = parse_year_teasers(winners_html)
    log(f"Parsed {len(year_rows)} Brain Prize years from /winners")

    rows: list[dict[str, str | None]] = []
    for year_row in year_rows:
        year_url = str(year_row["year_page_url"])
        year_html = get_page(session, year_url, checkpoint_file, use_cache)
        year_soup = BeautifulSoup(year_html, "html.parser")
        year_description = meta_content(year_soup, "description")
        profile_links = parse_profile_links(year_url, year_html)

        expected_names = year_row["winner_names"]
        if len(profile_links) != len(expected_names):
            log(
                f"  warning: {year_row['award_year']} profile link count {len(profile_links)} "
                f"does not match /winners count {len(expected_names)}"
            )
        if not profile_links:
            profile_links = [
                {"laureate_name": name, "profile_url": year_url}
                for name in expected_names
            ]

        laureate_count = str(len(profile_links))
        if laureate_count == "0":
            raise RuntimeError(f"No laureates found for {year_url}")

        for link in profile_links:
            if limit is not None and len(rows) >= limit:
                log(f"--limit {limit} reached")
                return rows

            profile_url = link["profile_url"]
            profile = parse_profile(
                profile_url,
                get_page(session, profile_url, checkpoint_file, use_cache) if profile_url != year_url else year_html,
            )
            name = profile["laureate_name"] or link["laureate_name"]
            given_name, family_name = split_name(name)
            portion = f"{1 / int(laureate_count):.12g}"
            funder_award_id = "-".join([
                "brain-prize",
                str(year_row["award_year"]),
                slugify(str(year_row["award_topic"])),
                slugify(name),
            ])

            rows.append({
                "source_page_url": WINNERS_URL,
                "year_page_url": year_url,
                "profile_url": profile_url,
                "landing_page_url": profile_url,
                "award_year": str(year_row["award_year"]),
                "award_topic": str(year_row["award_topic"]),
                "laureate_name": name,
                "given_name": profile["given_name"] or given_name,
                "family_name": profile["family_name"] or family_name,
                "affiliation": profile["affiliation"],
                "profile_description": profile["profile_description"],
                "profile_bio": profile["profile_bio"],
                "year_description": year_description,
                "source_total_award_amount": SOURCE_TOTAL_AWARD_AMOUNT,
                "currency": CURRENCY,
                "amount_rule_url": AMOUNT_RULE_URL,
                "laureate_count": laureate_count,
                "portion": portion,
                "funder_award_id": funder_award_id,
                "downloaded_at": utc_now(),
            })

        log(f"Parsed {len(profile_links)} rows for {year_row['award_year']} - {year_row['award_topic']}")

    return rows


def validate_rows(rows: list[dict[str, str | None]]) -> None:
    if not rows:
        raise RuntimeError("No Brain Prize rows parsed; stopping before parquet write.")
    seen: set[str] = set()
    for row in rows:
        award_id = row.get("funder_award_id")
        if not award_id:
            raise RuntimeError(f"Missing funder_award_id for row: {row}")
        if award_id in seen:
            raise RuntimeError(f"Duplicate funder_award_id would be emitted: {award_id}")
        seen.add(award_id)


def log_summary(df: pd.DataFrame) -> None:
    log(f"DataFrame shape: {df.shape}")
    log(
        "Coverage: "
        f"name={df.laureate_name.notna().sum()}, "
        f"year={df.award_year.notna().sum()}, "
        f"topic={df.award_topic.notna().sum()}, "
        f"affiliation={df.affiliation.notna().sum()}, "
        f"profile_description={df.profile_description.notna().sum()}, "
        f"year_description={df.year_description.notna().sum()}, "
        f"amount={df.source_total_award_amount.notna().sum()}, "
        f"currency={df.currency.notna().sum()}"
    )
    log(f"Year range: {df['award_year'].min()}-{df['award_year'].max()}")
    log(f"Rows by year: {df['award_year'].value_counts().sort_index(ascending=False).to_dict()}")
    log(f"Currency distribution: {df['currency'].fillna('NULL').value_counts().to_dict()}")


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook §1.4 — refuse to overwrite if the new corpus is smaller."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for the §1.4 shrink-check; rerun with --skip-upload to bypass"
        ) from exc
    client = boto3.client("s3")
    log(f"§1.4 re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            log("  no existing parquet — first ingest, no shrink check.")
            return True
        log(f"  [WARN] head_object failed ({code}); treating as first ingest")
        return True
    prev_path = output_dir / "_prev_brain_prize_laureates.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as e:
        log(f"  [ERROR] couldn't read existing parquet ({e}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)
    log(f"  previous count: {prev_count}   new count: {new_count}")
    if new_count < prev_count:
        if allow_shrink:
            log(f"  [OVERRIDE] new < previous but --allow-shrink set; proceeding.")
            return True
        log(
            f"\n[ERROR] §1.4 violation: refusing to shrink corpus "
            f"({prev_count} -> {new_count}). Investigate first."
        )
        return False
    log(f"  [OK] new corpus not smaller; safe to overwrite.")
    return True


def upload_to_s3(local_path: Path) -> None:
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3

    s3 = boto3.client("s3")
    s3.upload_file(str(local_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="The Brain Prize -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--checkpoint-file", type=Path, default=Path("/tmp/brain_prize_checkpoint.json"))
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="Limit emitted laureate rows for smoke tests")
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Refetch official pages even if they exist in the checkpoint file",
    )
    parser.add_argument(
        "--allow-shrink",
        action="store_true",
        help="Override §1.4 shrink-check (refuse to overwrite if new < previous)",
    )
    args = parser.parse_args()

    log("=" * 72)
    log("The Brain Prize -> S3 starting")
    log(f"Official source: {WINNERS_URL}")
    log(f"S3 target: s3://{S3_BUCKET}/{S3_KEY}")

    session = requests.Session()
    rows = build_rows(session, args.checkpoint_file, use_cache=not args.no_cache, limit=args.limit)
    validate_rows(rows)

    df = pd.DataFrame(rows)
    if df["funder_award_id"].duplicated().any():
        examples = df.loc[df["funder_award_id"].duplicated(keep=False), ["funder_award_id", "landing_page_url"]]
        raise RuntimeError("Duplicate funder_award_id values found:\n" + examples.to_string(index=False))

    log_summary(df)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / OUTPUT_FILE
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {parquet_path}")

    if args.skip_upload:
        log("--skip-upload set; done.")
        return

    if not check_no_shrink(len(df), args.allow_shrink, args.output_dir):
        raise SystemExit(7)

    upload_to_s3(parquet_path)


if __name__ == "__main__":
    main()
