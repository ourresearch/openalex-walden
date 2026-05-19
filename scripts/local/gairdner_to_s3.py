#!/usr/bin/env python3
"""
Gairdner Awards to S3 (PRIZE PATTERN)
=====================================

Gairdner publishes its laureate list through the official Sitefinity
OData API that backs https://www.gairdner.org/winners:

    https://www.gairdner.org/api/default/awardwinners

The API supports OData pagination and relation expansion. We fetch each
AwardWinner row with RelatedAward and RelatedWinner expanded, then emit
one row per (award x laureate). This is the prize-pattern shape used by
Nobel, Wolf, Lasker, Kavli, Fields, and Abel: the laureate is the
lead_investigator, funding_type is "prize", and funder_scheme is the
award program.

Output:
    s3://openalex-ingest/awards/gairdner/gairdner_awards.parquet

Awarding body in OpenAlex:
    Gairdner Foundation (F4320313415)

Amount note:
    Gairdner prizes are monetary, but the public API does not expose a
    reliable historical per-laureate amount. Older years changed value
    and some awards can be shared. The Databricks notebook leaves amount
    NULL, currency CAD, and explicitly waives the prize-pattern amount
    check, matching the conservative convention used by Wolf/Lasker.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

API_URL = "https://www.gairdner.org/api/default/awardwinners"
SITE_BASE = "https://www.gairdner.org"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/gairdner/gairdner_awards.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
PER_PAGE = 100
REQUEST_DELAY = 0.25
RETRIES = 3


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def html_to_text(value: str | None) -> str | None:
    """Strip simple HTML and normalize whitespace using only stdlib."""
    if not value:
        return None
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def year_from_award(record: dict[str, Any]) -> int | None:
    completed = clean_text(record.get("Completed"))
    if completed and re.match(r"^\d{4}", completed):
        return int(completed[:4])

    meta = clean_text(record.get("ContentMeta")) or ""
    match = re.search(r"year-(\d{4})", meta)
    if match:
        return int(match.group(1))
    return None


def first_related(record: dict[str, Any], key: str) -> dict[str, Any]:
    value = record.get(key) or []
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return value[0]
    return {}


def split_name(full_name: str | None) -> tuple[str | None, str | None]:
    """Fallback split for rows where Sitefinity name fields are incomplete."""
    if not full_name:
        return None, None
    tokens = full_name.split()
    suffixes = {
        "phd", "ph.d.", "md", "m.d.", "dphil", "frs", "jr", "jr.", "sr",
        "sr.", "ii", "iii", "iv",
    }
    while tokens and tokens[-1].lower().strip(",") in suffixes:
        tokens.pop()
    if len(tokens) <= 1:
        return None, tokens[0] if tokens else None
    return " ".join(tokens[:-1]), tokens[-1]


def laureate_name(winner: dict[str, Any], award_record: dict[str, Any]) -> str | None:
    return (
        clean_text(winner.get("Title"))
        or clean_text(award_record.get("Title"))
        or clean_text(award_record.get("UrlName"))
    )


def laureate_given_family(
    winner: dict[str, Any], award_record: dict[str, Any]
) -> tuple[str | None, str | None]:
    first = clean_text(winner.get("FirstName"))
    middle = clean_text(winner.get("MiddleName"))
    last = clean_text(winner.get("LastName"))
    if first or middle or last:
        given = " ".join(part for part in [first, middle] if part) or None
        return given, last
    return split_name(laureate_name(winner, award_record))


def winner_url(winner: dict[str, Any]) -> str | None:
    url_name = clean_text(winner.get("UrlName"))
    if url_name:
        return f"{SITE_BASE}/winner/{url_name}"
    item_default = clean_text(winner.get("ItemDefaultUrl"))
    if item_default:
        return f"{SITE_BASE}{item_default}"
    return None


def award_url(award: dict[str, Any]) -> str | None:
    url_name = clean_text(award.get("UrlName"))
    if url_name:
        return f"{SITE_BASE}/award/{url_name}"
    item_default = clean_text(award.get("ItemDefaultUrl"))
    if item_default:
        return f"{SITE_BASE}{item_default}"
    return None


def fetch_page(skip: int) -> tuple[list[dict[str, Any]], int | None]:
    params = {
        "$top": PER_PAGE,
        "$skip": skip,
        "$count": "true",
        "$expand": "RelatedAward,RelatedWinner",
    }

    last_err: Exception | None = None
    for attempt in range(RETRIES):
        try:
            log(f"GET {API_URL} skip={skip} top={PER_PAGE}")
            response = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
            log(f"  status={response.status_code} bytes={len(response.content)}")
            response.raise_for_status()
            payload = response.json()
            return payload.get("value") or [], payload.get("@odata.count")
        except Exception as exc:
            last_err = exc
            sleep_s = 2 ** attempt
            log(f"  attempt {attempt + 1}/{RETRIES} failed: {exc}; retrying in {sleep_s}s")
            time.sleep(sleep_s)

    raise RuntimeError(f"Failed to fetch skip={skip}: {last_err}")


def normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    award = first_related(record, "RelatedAward")
    winner = first_related(record, "RelatedWinner")

    name = laureate_name(winner, record)
    given_name, family_name = laureate_given_family(winner, record)
    year = year_from_award(record)
    content_meta = clean_text(record.get("ContentMeta"))
    award_name = (
        clean_text(award.get("FullTitle"))
        or clean_text(award.get("Title"))
        or (content_meta.split("|", 1)[0].strip() if content_meta else None)
    )
    citation = clean_text(winner.get("Quote"))
    award_summary = clean_text(award.get("Summary"))
    winner_bio = html_to_text(winner.get("Content"))

    return {
        "awardwinner_id": clean_text(record.get("Id")),
        "funder_award_id": clean_text(record.get("Id")),
        "awardwinner_url_name": clean_text(record.get("UrlName")),
        "awardwinner_title": clean_text(record.get("Title")),
        "completed": clean_text(record.get("Completed")),
        "year": year,
        "content_meta": content_meta,
        "award_id": clean_text(award.get("Id")),
        "award_name": award_name,
        "award_url_name": clean_text(award.get("UrlName")),
        "award_url": award_url(award),
        "award_summary": award_summary,
        "award_content_text": html_to_text(award.get("Content")),
        "laureate_id": clean_text(winner.get("Id")),
        "laureate_url_name": clean_text(winner.get("UrlName")),
        "laureate_url": winner_url(winner),
        "laureate_name": name,
        "laureate_given_name": given_name,
        "laureate_family_name": family_name,
        "laureate_prefix": clean_text(winner.get("Prefix")),
        "laureate_suffix": clean_text(winner.get("Suffix")),
        "laureate_designations": clean_text(winner.get("Designations")),
        "laureate_position_title": clean_text(winner.get("PositionTitle")),
        "laureate_memoriam": clean_text(winner.get("Memoriam")),
        "laureate_nobel_prize": winner.get("NobelPrize"),
        "citation": citation,
        "description": citation or award_summary or winner_bio,
        "winner_bio_text": winner_bio,
        # No Gairdner laureate has declined the award per the official Sitefinity feed;
        # kept for schema parity with Fields Medal / Abel Prize and to make the
        # description CASE in the notebook idempotent.
        "declined": False,
        "raw_awardwinner_json": json.dumps(record, ensure_ascii=False, sort_keys=True),
        "raw_award_json": json.dumps(award, ensure_ascii=False, sort_keys=True),
        "raw_winner_json": json.dumps(winner, ensure_ascii=False, sort_keys=True),
        "downloaded_at": datetime.utcnow().isoformat(),
    }


def fetch_all(limit_pages: int | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    skip = 0
    total: int | None = None
    page = 0

    while True:
        page += 1
        records, total = fetch_page(skip)
        log(f"  page={page} records={len(records)} running={len(rows) + len(records)}/{total or '?'}")
        rows.extend(normalize_record(record) for record in records)

        if limit_pages and page >= limit_pages:
            log(f"Stopping early because --limit-pages={limit_pages}")
            break
        if not records:
            break
        if total is not None and len(rows) >= total:
            break

        skip += len(records)
        time.sleep(REQUEST_DELAY)

    return rows


def validate_funder_award_ids(rows: list[dict[str, Any]]) -> None:
    """Fail before upload if native award IDs would collide downstream."""
    ids = [row.get("funder_award_id") for row in rows]
    missing = sum(1 for value in ids if not value)
    if missing:
        raise RuntimeError(f"{missing} rows are missing funder_award_id; refusing to write parquet.")

    counts = Counter(ids)
    duplicates = sorted(value for value, count in counts.items() if count > 1)
    if duplicates:
        sample = ", ".join(str(value) for value in duplicates[:10])
        raise RuntimeError(
            f"Duplicate funder_award_id values would collide downstream: {sample}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Gairdner Awards -> parquet -> S3")
    parser.add_argument("--limit-pages", type=int, default=None,
                        help="Smoke-test mode: only fetch the first N pages.")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and normalize only; skip pandas/parquet/S3.")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()

    log("=" * 60)
    log("Gairdner Awards -> S3 starting")
    rows = fetch_all(limit_pages=args.limit_pages)
    if not rows:
        raise RuntimeError("No Gairdner award rows fetched; stopping before parquet write.")
    validate_funder_award_ids(rows)

    log(
        "Coverage: "
        f"name={sum(1 for r in rows if r.get('laureate_name'))}, "
        f"given={sum(1 for r in rows if r.get('laureate_given_name'))}, "
        f"family={sum(1 for r in rows if r.get('laureate_family_name'))}, "
        f"year={sum(1 for r in rows if r.get('year'))}, "
        f"award={sum(1 for r in rows if r.get('award_name'))}, "
        f"citation={sum(1 for r in rows if r.get('citation'))}"
    )
    award_programs = sorted({r["award_name"] for r in rows if r.get("award_name")})
    log(f"Award programs: {award_programs}")
    if args.dry_run:
        log(f"--dry-run set; fetched {len(rows)} rows and skipped parquet/S3.")
        log(f"Sample row: {json.dumps(rows[0], ensure_ascii=False, sort_keys=True)[:1200]}")
        return

    import pandas as pd

    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")
    log(
        "Coverage: "
        f"name={df.laureate_name.notna().sum()}, "
        f"given={df.laureate_given_name.notna().sum()}, "
        f"family={df.laureate_family_name.notna().sum()}, "
        f"year={df.year.notna().sum()}, "
        f"award={df.award_name.notna().sum()}, "
        f"citation={df.citation.notna().sum()}"
    )
    log(f"Award programs: {sorted(df.award_name.dropna().unique().tolist())}")

    # Match current walden source-script convention and avoid Spark/PyArrow
    # type inference surprises on null-heavy columns.
    df = df.astype("string")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "gairdner_awards.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {parquet_path}")

    if args.skip_upload:
        log("--skip-upload set; done.")
        return

    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3
    s3 = boto3.client("s3")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    main()
