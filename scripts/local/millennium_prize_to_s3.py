#!/usr/bin/env python3
"""
Millennium Technology Prize to S3 (PRIZE PATTERN)
=================================================

Fetches Millennium Technology Prize winners from the official prize website.
This follows the awards runbook's prize-pattern source-authority rule: prize
rows come from the awarding body directly, not from Wikipedia, Wikidata, news
articles, or third-party biographies.

Official source pages:
  - https://millenniumprize.org/winners/
  - https://millenniumprize.org/prize/story/

Output:
  s3://openalex-ingest/awards/millennium_prize/millennium_prize_laureates.parquet

Awarding body in OpenAlex:
  Tekniikan Akatemia / Technology Academy Finland
  (F4320324443, DOI 10.13039/501100007456)

Amount rule:
  The official prize story calls the Millennium Technology Prize the
  pre-eminent EUR 1 million award and says the global one-million-euro prize
  was first awarded in 2004. The script stores one row per official laureate
  and divides EUR 1,000,000 among co-laureates for the same winning innovation.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

WINNERS_URL = "https://millenniumprize.org/winners/"
AMOUNT_RULE_URL = "https://millenniumprize.org/prize/story/"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/millennium_prize/millennium_prize_laureates.parquet"
OUTPUT_FILE = "millennium_prize_laureates.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.2
RETRIES = 3

TOTAL_AMOUNT = 1000000.0
CURRENCY = "EUR"


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
    if not name:
        return None, None
    name = re.sub(r"^(Grand Prize|Winner):\s*", "", name).strip()
    tokens = name.split()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def split_laureates(raw_name: str) -> list[str]:
    raw_name = collapse_text(raw_name) or ""
    raw_name = re.sub(r"^(Grand Prize|Winner):\s*", "", raw_name).strip()
    if " and " in raw_name:
        return [part.strip() for part in raw_name.split(" and ") if part.strip()]
    return [raw_name] if raw_name else []


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
        except Exception as exc:  # noqa: BLE001 - retry any transport/status failure.
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


def verify_amount_rule(story_html: str) -> None:
    text = BeautifulSoup(story_html, "html.parser").get_text(" ", strip=True)
    text = collapse_text(text) or ""
    checks = [
        "€1 million" in text,
        "one-million-euro prize" in text.lower(),
        "first awarded in 2004" in text.lower(),
    ]
    if not all(checks):
        raise RuntimeError(
            "Could not verify the Millennium Technology Prize amount rule on "
            f"{AMOUNT_RULE_URL}. Expected EUR 1 million and first-awarded-in-2004 language."
        )


def profile_slug(url: str | None) -> str:
    if not url:
        return "unknown"
    parts = [part for part in urlparse(url).path.split("/") if part]
    return slugify(parts[-1] if parts else url)


def parse_winners_index(index_html: str) -> list[dict[str, str | None]]:
    soup = BeautifulSoup(index_html, "html.parser")
    article = soup.find("article") or soup
    strings = [collapse_text(text) for text in article.stripped_strings]
    strings = [text for text in strings if text and text != "Read more ›"]

    detail_links = [
        anchor.get("href")
        for anchor in article.find_all("a", href=True)
        if "Read more" in anchor.get_text(" ", strip=True)
    ]
    blocks: list[tuple[str, str, str, str]] = []
    idx = 0
    while idx < len(strings):
        if re.fullmatch(r"\d{4}", strings[idx] or "") and idx + 2 < len(strings):
            year = strings[idx] or ""
            innovation = strings[idx + 1] or ""
            raw_laureates = strings[idx + 2] or ""
            link_index = len(blocks)
            if link_index >= len(detail_links):
                raise RuntimeError(f"Missing detail link for {year} {innovation}")
            blocks.append((year, innovation, raw_laureates, detail_links[link_index]))
            idx += 3
        else:
            idx += 1

    rows: list[dict[str, str | None]] = []
    for year, innovation, raw_laureates, detail_url in blocks:
        laureates = split_laureates(raw_laureates)
        if not laureates:
            raise RuntimeError(f"No laureates parsed for {year} {innovation}")
        for laureate_name in laureates:
            given_name, family_name = split_name(laureate_name)
            slug = f"{profile_slug(detail_url)}-{slugify(laureate_name)}"
            rows.append(
                {
                    "funder_award_id": f"millennium-prize-{year}-{slug}",
                    "award_year": year,
                    "innovation": innovation,
                    "raw_laureates": raw_laureates,
                    "laureate_name": laureate_name,
                    "given_name": given_name,
                    "family_name": family_name,
                    "landing_page_url": detail_url,
                    "profile_slug": slug,
                    "source_url": WINNERS_URL,
                    "amount_rule_url": AMOUNT_RULE_URL,
                    "fetched_at": utc_now(),
                }
            )
    log(f"Parsed {len(blocks)} winning innovations and {len(rows)} laureate rows")
    return rows


def parse_profile(profile_html: str) -> dict[str, str | None]:
    soup = BeautifulSoup(profile_html, "html.parser")
    article = soup.find("article") or soup
    strings = [collapse_text(text) for text in article.stripped_strings]
    strings = [text for text in strings if text]

    # Detail pages consistently render as:
    # back link, year, innovation, laureate name(s), country, description...
    country = strings[4] if len(strings) > 4 else None
    summary_parts: list[str] = []
    for text in strings[5:]:
        if text == "Share" or text.startswith("‹ Previous"):
            break
        if text in {"Show more", "Laureates", "All winners", "Next winner ›"}:
            break
        summary_parts.append(text)
    return {
        "country": country,
        "profile_description": collapse_text(" ".join(summary_parts)),
    }


def add_amounts(rows: list[dict[str, str | None]]) -> list[dict[str, str | None]]:
    share_counts = Counter((row["award_year"], row["innovation"]) for row in rows)
    for row in rows:
        key = (row["award_year"], row["innovation"])
        share_count = share_counts[key]
        amount = TOTAL_AMOUNT / share_count
        row["award_share_count"] = str(share_count)
        row["portion"] = f"1/{share_count}" if share_count > 1 else "1"
        row["award_total_amount"] = str(int(TOTAL_AMOUNT))
        row["source_award_amount"] = f"{amount:.6f}".rstrip("0").rstrip(".")
        row["currency"] = CURRENCY
        row["amount_note"] = "EUR 1,000,000 prize amount from official prize story, divided by laureate count for the same winning innovation."
    return rows


def fetch_rows(max_profiles: int | None, checkpoint_file: Path, use_cache: bool) -> list[dict[str, str | None]]:
    session = requests.Session()
    story_html = get_page(session, AMOUNT_RULE_URL, checkpoint_file, use_cache)
    verify_amount_rule(story_html)

    index_html = get_page(session, WINNERS_URL, checkpoint_file, use_cache)
    rows = add_amounts(parse_winners_index(index_html))
    total = len(rows if max_profiles is None else rows[:max_profiles])
    for idx, row in enumerate(rows):
        if max_profiles is not None and idx >= max_profiles:
            break
        url = row["landing_page_url"]
        if not url:
            continue
        profile_html = get_page(session, url, checkpoint_file, use_cache)
        row.update(parse_profile(profile_html))
        log(f"Fetched profile details for {idx + 1}/{total} laureates")
    return rows


def validate_rows(rows: list[dict[str, str | None]]) -> None:
    if not rows:
        raise RuntimeError("No rows parsed from Millennium Technology Prize source pages")
    award_ids = [row["funder_award_id"] for row in rows]
    duplicates = sorted({award_id for award_id in award_ids if award_ids.count(award_id) > 1})
    if duplicates:
        raise RuntimeError(f"Duplicate funder_award_id values detected: {duplicates[:10]}")
    required = ["award_year", "innovation", "laureate_name", "landing_page_url", "source_award_amount", "currency"]
    for column in required:
        missing = [row["funder_award_id"] for row in rows if not row.get(column)]
        if missing:
            raise RuntimeError(f"Missing required source field {column} on {len(missing)} rows: {missing[:10]}")


def write_parquet(rows: list[dict[str, str | None]], output_path: Path) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df = df.sort_values(["award_year", "innovation", "laureate_name"], ascending=[False, True, True])
    df = df.astype("string")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    log(f"Wrote {len(df)} rows to {output_path}")
    return df


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """
    Runbook §1.4 — never shrink the corpus on re-ingest. Read the existing
    S3 parquet's row count; if the new dataframe has fewer rows, abort.
    Returns True if it's safe to proceed; False if upload must be aborted.
    """
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
            log("  no existing parquet at S3 path — first ingest, no shrink check.")
            return True
        log(f"  [WARN] head_object failed ({code}); treating as first ingest")
        return True
    prev_path = output_dir / "_prev_millennium_prize_laureates.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        import pandas as pd
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as e:
        log(f"  [ERROR] couldn't read existing parquet ({e}); aborting upload "
            f"to avoid clobbering unknown data. Re-run with --allow-shrink if "
            f"you've verified the previous file is corrupt or empty.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)
    log(f"  previous count: {prev_count}   new count: {new_count}")
    if new_count < prev_count:
        if allow_shrink:
            log(f"  [OVERRIDE] new < previous but --allow-shrink set; proceeding.")
            return True
        log(
            f"  [ERROR] §1.4 violation: refusing to shrink corpus "
            f"({prev_count} -> {new_count}). Cause is almost always a "
            f"source-side partial outage, schema change, or pagination bug — "
            f"not a genuine retraction. Investigate first; re-run with "
            f"--allow-shrink if confirmed intentional."
        )
        return False
    log("  [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(output_path: Path) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for S3 upload; rerun with --skip-upload for local validation") from exc
    log(f"Uploading {output_path} to s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(output_path), S3_BUCKET, S3_KEY)
    log("S3 upload complete")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Millennium Technology Prize laureates and write parquet for OpenAlex awards ingest.")
    parser.add_argument("--output-dir", default="/tmp/openalex-awards/millennium_prize", help="Directory for parquet output")
    parser.add_argument("--checkpoint", default="/tmp/openalex-awards/millennium_prize/checkpoint.json", help="HTML checkpoint path")
    parser.add_argument("--no-cache", action="store_true", help="Ignore checkpointed HTML and refetch all pages")
    parser.add_argument("--skip-upload", action="store_true", help="Write local parquet but do not upload to S3")
    parser.add_argument(
        "--allow-shrink",
        action="store_true",
        help="Override the runbook §1.4 shrink-check. Only use after confirming a smaller corpus is intentional.",
    )
    parser.add_argument("--max-profiles", type=int, default=None, help="Optional smoke-test limit on detail pages")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = Path(args.output_dir) / OUTPUT_FILE
    checkpoint_file = Path(args.checkpoint)

    rows = fetch_rows(max_profiles=args.max_profiles, checkpoint_file=checkpoint_file, use_cache=not args.no_cache)
    validate_rows(rows)
    df = write_parquet(rows, output_path)
    log(
        "Coverage: "
        f"{df['laureate_name'].notna().sum()}/{len(df)} names, "
        f"{df['award_year'].notna().sum()}/{len(df)} years, "
        f"{df['innovation'].notna().sum()}/{len(df)} innovations, "
        f"{df['profile_description'].notna().sum()}/{len(df)} profile descriptions, "
        f"{df['source_award_amount'].notna().sum()}/{len(df)} amounts, "
        f"{df['currency'].notna().sum()}/{len(df)} currencies"
    )
    if args.skip_upload:
        log("Skipping S3 upload by request")
    else:
        if not check_no_shrink(len(df), args.allow_shrink, Path(args.output_dir)):
            raise SystemExit("§1.4 shrink-check failed. See above; re-run with --allow-shrink if intentional.")
        upload_to_s3(output_path)


if __name__ == "__main__":
    main()
