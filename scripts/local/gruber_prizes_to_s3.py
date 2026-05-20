#!/usr/bin/env python3
"""
Gruber Science Prizes to S3 (PRIZE PATTERN)
===========================================

Scrape official Gruber Prize laureate data from Yale-hosted Gruber
Foundation pages:

    https://gruber.yale.edu/year
    https://gruber.yale.edu/cosmology
    https://gruber.yale.edu/genetics
    https://gruber.yale.edu/neuroscience

Scope:
    Only the three science prize categories are included: Cosmology,
    Genetics, and Neuroscience. The same `/year` view also contains the
    archived Justice and Women's Rights prizes, but those are outside this
    science-prize ingest and are intentionally excluded.

Output:
    s3://openalex-ingest/awards/gruber_prizes/gruber_prizes_awards.parquet

Awarding body in OpenAlex:
    Gruber Foundation (F4320312392, DOI 10.13039/100010848)

Amount mapping:
    Each science category page states that the prize includes an
    unrestricted USD 500,000 cash award. The parquet keeps that official
    yearly/category total and a `laureate_count`; the Databricks notebook
    maps `amount = source_total_award_amount / laureate_count` so one row
    per (prize x laureate) does not multiply the total award.

The script preserves source text from the official site, leaves unsupported
fields (for example structured affiliation/country) to the notebook as NULL,
and fails before parquet write if synthetic funder_award_id values collide.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import tempfile
import time
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://gruber.yale.edu"
INDEX_URL = f"{BASE_URL}/year"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/gruber_prizes/gruber_prizes_awards.parquet"

SCIENCE_PRIZES = {
    "Cosmology": f"{BASE_URL}/cosmology",
    "Genetics": f"{BASE_URL}/genetics",
    "Neuroscience": f"{BASE_URL}/neuroscience",
}

OFFICIAL_TOTAL_AWARD_AMOUNT = 500000
OFFICIAL_CURRENCY = "USD"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY_SECONDS = 0.2
RETRIES = 3


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def clean_text(value: Any, max_chars: int | None = None) -> str | None:
    if value is None:
        return None
    text = html.unescape(str(value))
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\s+([,;.:])", r"\1", text)
    if not text:
        return None
    if max_chars and len(text) > max_chars:
        text = text[:max_chars].rstrip() + "..."
    return text


def slugify(value: str | None) -> str | None:
    if not value:
        return None
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", normalized.lower()).strip("-")
    return slug or None


def split_name(name: str | None) -> tuple[str | None, str | None]:
    """Split names using the repo's established prize-pattern convention."""
    if not name:
        return None, None
    if re.search(r"\bteam\b|&|\band the\b", name, flags=re.I):
        # Some Gruber laureates are named research teams or person+team
        # groups. There is no structured person name to split, so keep the
        # official source string intact in family_name rather than producing
        # misleading fragments like given_name='Planck', family_name='Team'.
        return None, name
    tokens = name.split()
    suffixes = {
        "phd", "ph.d.", "md", "m.d.", "dphil", "dsc", "scd",
        "jr", "jr.", "sr", "sr.", "ii", "iii", "iv",
    }
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def normalize_name_for_match(value: str | None) -> str | None:
    slug = slugify(value)
    return slug.replace("-", "") if slug else None


def fetch_url(session: requests.Session, url: str) -> str:
    last_error: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        try:
            log(f"GET {url}")
            response = session.get(url, headers=HEADERS, timeout=30)
            log(f"  status={response.status_code} bytes={len(response.content)}")
            response.raise_for_status()
            if not response.text.strip():
                raise RuntimeError(f"Empty response from {url}")
            return response.text
        except Exception as exc:
            last_error = exc
            if attempt == RETRIES:
                break
            sleep_seconds = 2 ** (attempt - 1)
            log(f"  attempt {attempt}/{RETRIES} failed: {exc}; retrying in {sleep_seconds}s")
            time.sleep(sleep_seconds)
    raise RuntimeError(f"Failed to fetch {url}: {last_error}")


def load_checkpoint(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {"detail_pages": {}, "recipient_pages": {}}
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    data.setdefault("detail_pages", {})
    data.setdefault("recipient_pages", {})
    return data


def save_checkpoint(path: Path | None, checkpoint: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(checkpoint, handle, ensure_ascii=False, indent=2, sort_keys=True)


def parse_prize_title(title: str | None) -> tuple[int | None, str | None]:
    if not title:
        return None, None
    match = re.match(r"^(?P<year>\d{4})\s+Gruber\s+(?P<category>.+?)\s+Prize$", title)
    if not match:
        return None, None
    return int(match.group("year")), clean_text(match.group("category"))


def parse_index_rows(index_html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(index_html, "html.parser")
    rows: list[dict[str, Any]] = []
    for row in soup.select(".view--content-prizes .view__row"):
        title_node = row.select_one(".views-field-title-1")
        laureate_node = row.select_one(".views-field-title a")
        teaser_node = row.select_one(".views-field-field-teaser-text")
        source_title = clean_text(title_node.get_text(" ", strip=True) if title_node else None)
        year, prize_category = parse_prize_title(source_title)
        if prize_category not in SCIENCE_PRIZES:
            continue
        laureate_name = clean_text(laureate_node.get_text(" ", strip=True) if laureate_node else None)
        given_name, family_name = split_name(laureate_name)
        prize_detail_url = urljoin(BASE_URL, laureate_node.get("href")) if laureate_node else None
        source_teaser = clean_text(teaser_node.get_text(" ", strip=True) if teaser_node else None, 4000)
        group_key = f"{year}:{prize_category}:{prize_detail_url}"
        funder_award_id = "-".join([
            "gruber",
            slugify(prize_category) or "unknown",
            str(year or "unknown"),
            slugify(laureate_name) or "unknown",
        ])
        rows.append({
            "source_title": source_title,
            "award_year": year,
            "prize_category": prize_category,
            "laureate_name": laureate_name,
            "laureate_given_name": given_name,
            "laureate_family_name": family_name,
            "prize_detail_url": prize_detail_url,
            "source_teaser": source_teaser,
            "prize_group_key": group_key,
            "funder_award_id": funder_award_id,
        })
    return rows


def parse_amount_rules(session: requests.Session) -> dict[str, str]:
    rules: dict[str, str] = {}
    amount_re = re.compile(r"\$500,?000|500,?000\s+cash", re.I)
    for category, url in SCIENCE_PRIZES.items():
        html_text = fetch_url(session, url)
        page_text = clean_text(BeautifulSoup(html_text, "html.parser").get_text(" ", strip=True), 8000)
        if not page_text or not amount_re.search(page_text):
            raise RuntimeError(
                f"Official amount rule not found on {url}; refusing to assume prize amount."
            )
        rules[category] = page_text
        time.sleep(REQUEST_DELAY_SECONDS)
    return rules


def parse_detail_page(html_text: str) -> dict[str, Any]:
    soup = BeautifulSoup(html_text, "html.parser")
    h1 = soup.select_one("h1")
    profile_node = soup.select_one(".node__laureate-profile")
    if profile_node is None:
        # Older pages can have a flatter layout; use the page content as
        # a conservative fallback rather than regexing the rendered page.
        profile_node = soup.select_one(".node__content")
    recipients: list[dict[str, str]] = []
    for link in soup.select('a[href^="/recipient/"]'):
        name = clean_text(link.get_text(" ", strip=True))
        href = link.get("href")
        if name and href:
            recipients.append({"name": name, "url": urljoin(BASE_URL, href)})

    # Deduplicate links while preserving order.
    seen: set[tuple[str, str]] = set()
    unique_recipients: list[dict[str, str]] = []
    for item in recipients:
        key = (item["name"], item["url"])
        if key not in seen:
            seen.add(key)
            unique_recipients.append(item)

    return {
        "detail_title": clean_text(h1.get_text(" ", strip=True) if h1 else None),
        "prize_profile_text": clean_text(profile_node.get_text(" ", strip=True) if profile_node else None, 8000),
        "recipient_links": unique_recipients,
    }


def parse_recipient_page(html_text: str) -> dict[str, Any]:
    soup = BeautifulSoup(html_text, "html.parser")
    h1 = soup.select_one("h1")
    bio_node = soup.select_one(".node__bio") or soup.select_one(".node__content")
    return {
        "recipient_profile_title": clean_text(h1.get_text(" ", strip=True) if h1 else None),
        "recipient_bio_text": clean_text(bio_node.get_text(" ", strip=True) if bio_node else None, 8000),
    }


def match_recipient_url(laureate_name: str | None, detail: dict[str, Any]) -> str | None:
    wanted = normalize_name_for_match(laureate_name)
    if not wanted:
        return None
    for item in detail.get("recipient_links", []):
        if normalize_name_for_match(item.get("name")) == wanted:
            return item.get("url")
    return None


def enrich_rows(
    rows: list[dict[str, Any]],
    session: requests.Session,
    checkpoint: dict[str, Any],
    checkpoint_file: Path | None,
) -> list[dict[str, Any]]:
    detail_pages = checkpoint.setdefault("detail_pages", {})
    recipient_pages = checkpoint.setdefault("recipient_pages", {})
    total = len(rows)
    for idx, row in enumerate(rows, start=1):
        detail_url = row.get("prize_detail_url")
        if detail_url and detail_url not in detail_pages:
            detail_pages[detail_url] = parse_detail_page(fetch_url(session, detail_url))
            save_checkpoint(checkpoint_file, checkpoint)
            time.sleep(REQUEST_DELAY_SECONDS)
        detail = detail_pages.get(detail_url, {}) if detail_url else {}
        recipient_url = match_recipient_url(row.get("laureate_name"), detail)
        row["recipient_profile_url"] = recipient_url
        row["detail_title"] = detail.get("detail_title")
        row["prize_profile_text"] = detail.get("prize_profile_text")
        row["recipient_links_json"] = json.dumps(detail.get("recipient_links", []), ensure_ascii=False, sort_keys=True)

        if recipient_url and recipient_url not in recipient_pages:
            recipient_pages[recipient_url] = parse_recipient_page(fetch_url(session, recipient_url))
            save_checkpoint(checkpoint_file, checkpoint)
            time.sleep(REQUEST_DELAY_SECONDS)
        recipient = recipient_pages.get(recipient_url, {}) if recipient_url else {}
        row["recipient_profile_title"] = recipient.get("recipient_profile_title")
        row["recipient_bio_text"] = recipient.get("recipient_bio_text")
        log(f"Enriched {idx}/{total}: {row.get('source_title')} - {row.get('laureate_name')}")
    return rows


def validate_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise RuntimeError("No Gruber science prize rows fetched; refusing to write parquet.")
    required = ["funder_award_id", "source_title", "award_year", "prize_category", "laureate_name"]
    for field in required:
        missing = [row for row in rows if not row.get(field)]
        if missing:
            raise RuntimeError(f"{len(missing)} rows missing {field}; refusing to write parquet.")
    counts = Counter(row["funder_award_id"] for row in rows)
    duplicates = sorted(value for value, count in counts.items() if count > 1)
    if duplicates:
        sample = ", ".join(duplicates[:10])
        raise RuntimeError(f"Duplicate funder_award_id values would collide downstream: {sample}")


def existing_s3_row_count(s3_client: Any, bucket: str, key: str) -> int | None:
    """Return existing parquet row count, or None if this is the first upload.

    This implements the repo's Step 1.4 fail-closed re-ingestion guard:
    never overwrite an existing S3 parquet with a smaller corpus unless the
    caller explicitly passes --allow-shrink.
    """
    from botocore.exceptions import ClientError
    import pandas as pd

    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        try:
            s3_client.download_file(bucket, key, tmp.name)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        return len(pd.read_parquet(tmp.name))


def main() -> None:
    parser = argparse.ArgumentParser(description="Gruber Science Prizes -> parquet -> S3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Smoke-test mode: only enrich/write the first N science rows.")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--checkpoint-file", type=Path, default=Path("/tmp/gruber_prizes_checkpoint.json"))
    parser.add_argument("--no-cache", action="store_true",
                        help="Ignore an existing checkpoint file.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and normalize only; skip pandas/parquet/S3.")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Write local parquet but do not upload to S3.")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Allow S3 upload even if row count shrinks versus the existing parquet.")
    args = parser.parse_args()

    if args.no_cache and args.checkpoint_file.exists():
        args.checkpoint_file.unlink()

    log("=" * 72)
    log("Gruber Science Prizes -> S3 starting")
    session = requests.Session()
    checkpoint = load_checkpoint(args.checkpoint_file)

    amount_rules = parse_amount_rules(session)
    log(f"Verified official USD 500,000 amount rule on {len(amount_rules)} category pages")

    index_html = fetch_url(session, INDEX_URL)
    rows = parse_index_rows(index_html)
    validate_rows(rows)
    group_counts = Counter(row["prize_group_key"] for row in rows)
    for row in rows:
        count = group_counts[row["prize_group_key"]]
        row["laureate_count"] = count
        row["portion"] = "1" if count == 1 else f"1/{count}"
        row["source_total_award_amount"] = OFFICIAL_TOTAL_AWARD_AMOUNT
        row["source_currency"] = OFFICIAL_CURRENCY
        row["amount_rule_source_url"] = SCIENCE_PRIZES[row["prize_category"]]
        row["amount_rule_text"] = amount_rules[row["prize_category"]]
        row["downloaded_at"] = datetime.now(timezone.utc).isoformat()

    rows.sort(key=lambda row: (-(row["award_year"] or 0), row["prize_category"], row["laureate_name"] or ""))
    if args.limit is not None:
        log(f"Applying --limit={args.limit} after full index/group-count parse")
        rows = rows[:args.limit]
    rows = enrich_rows(rows, session, checkpoint, args.checkpoint_file)
    validate_rows(rows)

    log(
        "Coverage: "
        f"rows={len(rows)}, "
        f"name={sum(1 for row in rows if row.get('laureate_name'))}, "
        f"given={sum(1 for row in rows if row.get('laureate_given_name'))}, "
        f"family={sum(1 for row in rows if row.get('laureate_family_name'))}, "
        f"year={sum(1 for row in rows if row.get('award_year'))}, "
        f"teaser={sum(1 for row in rows if row.get('source_teaser'))}, "
        f"profile={sum(1 for row in rows if row.get('prize_profile_text'))}, "
        f"recipient_url={sum(1 for row in rows if row.get('recipient_profile_url'))}, "
        f"bio={sum(1 for row in rows if row.get('recipient_bio_text'))}, "
        f"amount={sum(1 for row in rows if row.get('source_total_award_amount'))}"
    )
    log(f"Category counts: {dict(Counter(row['prize_category'] for row in rows))}")
    log(f"Year range: {min(row['award_year'] for row in rows)}-{max(row['award_year'] for row in rows)}")
    if args.dry_run:
        log(f"--dry-run set; fetched {len(rows)} rows and skipped parquet/S3.")
        log(f"Sample row: {json.dumps(rows[0], ensure_ascii=False, sort_keys=True)[:1500]}")
        return

    import pandas as pd

    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")
    df = df.astype("string")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "gruber_prizes_awards.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {parquet_path}")

    if args.skip_upload:
        log("--skip-upload set; done.")
        return

    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3
    s3 = boto3.client("s3")
    previous_count = existing_s3_row_count(s3, S3_BUCKET, S3_KEY)
    if previous_count is not None:
        log(f"Existing S3 parquet row count: {previous_count}")
        if len(df) < previous_count and not args.allow_shrink:
            raise RuntimeError(
                f"Refusing to shrink Gruber Science Prizes parquet from "
                f"{previous_count} to {len(df)} rows. Re-run with --allow-shrink "
                "only after confirming the source genuinely removed records."
            )
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    main()
