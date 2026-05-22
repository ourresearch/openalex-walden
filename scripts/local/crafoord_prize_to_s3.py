#!/usr/bin/env python3
"""
Crafoord Prize to S3 (PRIZE PATTERN)
====================================

Fetches Crafoord Prize laureates from the official Crafoord Prize website's
WordPress REST API. This follows the awards runbook's prize-pattern
source-authority rule: prize rows come from the awarding body directly, not
from Wikipedia, Wikidata, news articles, or third-party biographies.

Official source pages / APIs:
  - https://www.crafoordprize.se/about-the-prize/laureates/
  - https://www.crafoordprize.se/wp-json/wp/v2/award_winner
  - https://www.crafoordprize.se/news/the-prize-amount-for-the-crafoord-prize-has-been-increased-to-6-million-swedish-krona/

Output:
  s3://openalex-ingest/awards/crafoord_prize/crafoord_prize_laureates.parquet

Awarding body in OpenAlex:
  Royal Swedish Academy of Sciences (F4320320936,
  DOI 10.13039/501100001725)

Awarding-body note:
  The official Crafoord Prize site states that the prize is awarded in
  partnership between the Royal Swedish Academy of Sciences and the Crafoord
  Foundation in Lund, and that the Academy is responsible for selecting the
  laureates. OpenAlex has the Royal Swedish Academy of Sciences as a funder;
  a distinct "Crafoord Prize" funder was not found in OpenAlex.

Amount rule:
  The official 2015 press release says the prize amount was increased to SEK
  6 million from 2015. The script therefore populates per-laureate SEK amounts
  for 2015-present rows by dividing SEK 6,000,000 across all laureates in the
  same prize year/category. Pre-2015 amounts are left NULL rather than
  backfilled from an uncertain historical rule.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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

API_URL = "https://www.crafoordprize.se/wp-json/wp/v2/award_winner"
LAUREATES_URL = "https://www.crafoordprize.se/about-the-prize/laureates/"
AMOUNT_RULE_URL = (
    "https://www.crafoordprize.se/news/"
    "the-prize-amount-for-the-crafoord-prize-has-been-increased-to-6-million-swedish-krona/"
)

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/crafoord_prize/crafoord_prize_laureates.parquet"
OUTPUT_FILE = "crafoord_prize_laureates.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.2
RETRIES = 3

PRIZE_TERM_ID = 12
POST_2015_TOTAL_AMOUNT_SEK = 6000000.0
CURRENCY = "SEK"


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def collapse_text(value: str | None) -> str | None:
    if not value:
        return None
    value = html.unescape(value)
    if "<" in value and ">" in value:
        value = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
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


def request_get(session: requests.Session, url: str, *, params: dict[str, Any] | None = None) -> requests.Response:
    last_err: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        started = time.time()
        try:
            response = session.get(url, params=params, headers=HEADERS, timeout=30)
            elapsed = time.time() - started
            log(f"GET {response.url} -> {response.status_code} {len(response.content)} bytes in {elapsed:.1f}s")
            response.raise_for_status()
            if not response.content:
                raise RuntimeError(f"Empty response from {response.url}")
            return response
        except Exception as exc:  # noqa: BLE001 - retry transport/status failures.
            last_err = exc
            if attempt < RETRIES:
                sleep_s = 2 ** (attempt - 1)
                log(f"  retrying after {sleep_s}s: {exc}")
                time.sleep(sleep_s)
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


def load_checkpoint(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else {}


def save_checkpoint(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=False)


def verify_amount_rule(session: requests.Session, checkpoint: dict[str, Any], checkpoint_file: Path, use_cache: bool) -> None:
    if use_cache and "amount_rule_html" in checkpoint:
        html_text = str(checkpoint["amount_rule_html"])
        log(f"cached {AMOUNT_RULE_URL} ({len(html_text.encode('utf-8'))} bytes)")
    else:
        response = request_get(session, AMOUNT_RULE_URL)
        html_text = response.text
        checkpoint["amount_rule_html"] = html_text
        save_checkpoint(checkpoint_file, checkpoint)
        time.sleep(REQUEST_DELAY)

    text = collapse_text(html_text) or ""
    checks = [
        "SEK 6 million" in text or "6 million Swedish krona" in text,
        "increased from SEK 4 million to SEK 6 million" in text,
    ]
    if not all(checks):
        raise RuntimeError(
            "Could not verify the Crafoord Prize 2015-present amount rule on "
            f"{AMOUNT_RULE_URL}. Expected the page to mention SEK 6 million "
            "and the increase from SEK 4 million."
        )


def fetch_award_winner_pages(
    session: requests.Session,
    checkpoint_file: Path,
    use_cache: bool,
    max_pages: int | None,
) -> list[dict[str, Any]]:
    checkpoint = load_checkpoint(checkpoint_file)
    verify_amount_rule(session, checkpoint, checkpoint_file, use_cache)

    if use_cache and "api_pages" in checkpoint:
        log("cached WordPress award_winner API pages")
        pages = checkpoint["api_pages"]
    else:
        pages = []
        page = 1
        total_pages = None
        while True:
            params = {
                "per_page": 100,
                "page": page,
                "prize": PRIZE_TERM_ID,
                "lang": "en",
            }
            response = request_get(session, API_URL, params=params)
            payload = response.json()
            if not isinstance(payload, list):
                raise RuntimeError(f"Unexpected API payload on page {page}: {type(payload)}")
            total_pages = int(response.headers.get("X-WP-TotalPages", "1"))
            total_rows = int(response.headers.get("X-WP-Total", str(len(payload))))
            log(f"Fetched API page {page}/{total_pages}: {len(payload)} rows (reported total {total_rows})")
            pages.append(payload)
            if page >= total_pages:
                break
            if max_pages is not None and page >= max_pages:
                log(f"Stopping at --max-pages={max_pages}")
                break
            page += 1
            time.sleep(REQUEST_DELAY)
        checkpoint["api_pages"] = pages
        save_checkpoint(checkpoint_file, checkpoint)

    if max_pages is not None:
        pages = pages[:max_pages]
    rows = [item for page_payload in pages for item in page_payload]
    if not rows:
        raise RuntimeError("No Crafoord Prize rows returned by official WordPress API")
    return rows


def child_prize_terms(item: dict[str, Any]) -> list[dict[str, Any]]:
    terms = item.get("prizes") or []
    if not isinstance(terms, list):
        return []
    return [
        term
        for term in terms
        if isinstance(term, dict) and str(term.get("term_id")) != str(PRIZE_TERM_ID)
    ]


def normalize_item(item: dict[str, Any]) -> dict[str, str | None]:
    acf = item.get("acf") if isinstance(item.get("acf"), dict) else {}
    terms = child_prize_terms(item)
    category = terms[0] if terms else {}

    laureate_name = collapse_text((item.get("title") or {}).get("rendered"))
    given_name, family_name = split_name(laureate_name)
    award_year = collapse_text(acf.get("award_year"))
    category_name = collapse_text(category.get("name")) or None
    category_slug = collapse_text(category.get("slug")) or slugify(category_name)
    row_slug = collapse_text(item.get("slug")) or slugify(laureate_name)

    return {
        "funder_award_id": f"crafoord-prize-{award_year}-{category_slug}-{row_slug}",
        "award_year": award_year,
        "laureate_name": laureate_name,
        "given_name": given_name,
        "family_name": family_name,
        "laureate_title": collapse_text(acf.get("laureate_title")),
        "affiliation": collapse_text(acf.get("laureate_university")),
        "citation": collapse_text((item.get("content") or {}).get("rendered")),
        "prize_category": category_name,
        "prize_category_slug": category_slug,
        "prize_category_term_id": str(category.get("term_id")) if category.get("term_id") is not None else None,
        "wp_id": str(item.get("id")) if item.get("id") is not None else None,
        "wp_slug": row_slug,
        "landing_page_url": collapse_text(item.get("link")),
        "press_release_id": str(acf.get("laureate_press_release")) if acf.get("laureate_press_release") else None,
        "source_url": API_URL,
        "amount_rule_url": AMOUNT_RULE_URL,
        "fetched_at": utc_now(),
    }


def add_amounts(rows: list[dict[str, str | None]]) -> list[dict[str, str | None]]:
    share_counts = Counter((row["award_year"], row["prize_category_slug"]) for row in rows)
    for row in rows:
        key = (row["award_year"], row["prize_category_slug"])
        share_count = share_counts[key]
        row["award_share_count"] = str(share_count)
        row["portion"] = f"1/{share_count}" if share_count > 1 else "1"
        try:
            year = int(row["award_year"] or "")
        except ValueError:
            year = 0
        if year >= 2015:
            amount = POST_2015_TOTAL_AMOUNT_SEK / share_count
            row["award_total_amount"] = str(int(POST_2015_TOTAL_AMOUNT_SEK))
            row["source_award_amount"] = f"{amount:.6f}".rstrip("0").rstrip(".")
            row["currency"] = CURRENCY
            row["amount_note"] = "SEK 6,000,000 total prize amount from 2015 official press release, divided by laureate count for the year/category."
        else:
            row["award_total_amount"] = None
            row["source_award_amount"] = None
            row["currency"] = None
            row["amount_note"] = "Pre-2015 amount left NULL; official source used here only verifies the 2015-present SEK 6,000,000 rule."
    return rows


def fetch_rows(max_pages: int | None, checkpoint_file: Path, use_cache: bool) -> list[dict[str, str | None]]:
    session = requests.Session()
    raw_items = fetch_award_winner_pages(session, checkpoint_file, use_cache, max_pages)
    log(f"Normalizing {len(raw_items)} official award_winner records")
    rows = [normalize_item(item) for item in raw_items]
    rows = add_amounts(rows)
    return rows


def validate_rows(rows: list[dict[str, str | None]]) -> None:
    if not rows:
        raise RuntimeError("No rows parsed from Crafoord Prize source")
    award_ids = [row["funder_award_id"] for row in rows]
    duplicates = sorted({award_id for award_id in award_ids if award_ids.count(award_id) > 1})
    if duplicates:
        raise RuntimeError(f"Duplicate funder_award_id values detected: {duplicates[:10]}")

    required = ["award_year", "laureate_name", "prize_category", "landing_page_url"]
    for column in required:
        missing = [row["funder_award_id"] for row in rows if not row.get(column)]
        if missing:
            raise RuntimeError(f"Missing required source field {column} on {len(missing)} rows: {missing[:10]}")


def write_parquet(rows: list[dict[str, str | None]], output_path: Path) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df = df.sort_values(["award_year", "prize_category", "laureate_name"], ascending=[False, True, True])
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
    prev_path = output_dir / "_prev_crafoord_prize_laureates.parquet"
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
    parser = argparse.ArgumentParser(description="Fetch Crafoord Prize laureates and write parquet for OpenAlex awards ingest.")
    parser.add_argument("--output-dir", default="/tmp/openalex-awards/crafoord_prize", help="Directory for parquet output")
    parser.add_argument("--checkpoint", default="/tmp/openalex-awards/crafoord_prize/checkpoint.json", help="Checkpoint path")
    parser.add_argument("--no-cache", action="store_true", help="Ignore checkpointed API responses and refetch")
    parser.add_argument("--skip-upload", action="store_true", help="Write local parquet but do not upload to S3")
    parser.add_argument(
        "--allow-shrink",
        action="store_true",
        help="Override the runbook §1.4 shrink-check. Only use after confirming a smaller corpus is intentional.",
    )
    parser.add_argument("--max-pages", type=int, default=None, help="Optional smoke-test limit on API pages")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = Path(args.output_dir) / OUTPUT_FILE
    checkpoint_file = Path(args.checkpoint)

    rows = fetch_rows(max_pages=args.max_pages, checkpoint_file=checkpoint_file, use_cache=not args.no_cache)
    validate_rows(rows)
    df = write_parquet(rows, output_path)

    log(
        "Coverage: "
        f"{df['laureate_name'].notna().sum()}/{len(df)} names, "
        f"{df['award_year'].notna().sum()}/{len(df)} years, "
        f"{df['prize_category'].notna().sum()}/{len(df)} categories, "
        f"{df['affiliation'].notna().sum()}/{len(df)} affiliations, "
        f"{df['citation'].notna().sum()}/{len(df)} citations, "
        f"{df['source_award_amount'].notna().sum()}/{len(df)} post-2015 apportioned amounts, "
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
