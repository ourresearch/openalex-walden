#!/usr/bin/env python3
"""
Health Research Board Ireland Grants Approved -> S3 Pipeline
============================================================

Downloads HRB Ireland grant records from the Health Research Board's own
"Grants approved" database.

Source authority
----------------
HRB publishes the grant database on its own website:

    https://www.hrb.ie/funding-category/research-funding/investments-impacts/grants-approved/

The archive reports 2,155 grant records and is backed by HRB's public
WordPress REST API at `/wp-json/wp/v2/grant-approved`. REST gives every grant
post, title, abstract, canonical URL, and stable WordPress post ID. The rendered
grant detail page carries the structured sidebar fields used here: award year,
duration, grant value, principal investigator, host institution, scheme, scheme
type, and HRB broad research area.

HRB does not publish a grant-number field on these pages. The stable
funder_award_id is therefore `hrb-{wp_post_id}` from the official source, and
the slug/link are retained for auditability.

OpenAlex funder
---------------
F4320312041 - Health Research Board (IE)
ROR: https://ror.org/003hb2249
DOI: 10.13039/100010414

Output
------
s3://openalex-ingest/awards/hrb_ireland/hrb_ireland_projects.parquet

Usage
-----
    python scripts/local/hrb_ireland_to_s3.py --limit 10 --skip-upload
    python scripts/local/hrb_ireland_to_s3.py --skip-upload
    python scripts/local/hrb_ireland_to_s3.py --skip-download --skip-upload
    python scripts/local/hrb_ireland_to_s3.py --allow-shrink
"""

from __future__ import annotations

import argparse
import concurrent.futures
import html
import json
import math
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup


# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
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
# --- end shim ---


SOURCE_PAGE_URL = "https://www.hrb.ie/funding-category/research-funding/investments-impacts/grants-approved/"
REST_ENDPOINT = "https://www.hrb.ie/wp-json/wp/v2/grant-approved"
FUNDER_ID = 4320312041
FUNDER_DISPLAY_NAME = "Health Research Board"
PROVENANCE = "hrb_grants_approved"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/hrb_ireland/hrb_ireland_projects.parquet"

DEFAULT_OUTPUT_DIR = Path("/tmp")
JSON_CACHE_FILENAME = "hrb_ireland_grants_approved.json"
PARQUET_FILENAME = "hrb_ireland_projects.parquet"
REQUEST_TIMEOUT = (10, 90)
MAX_RETRIES = 5
PER_PAGE = 100
EXPECTED_MIN_FULL_ROWS = 2000

HEADERS = {
    "User-Agent": "openalex-walden-hrb-ireland/1.0 (+https://openalex.org)",
    "Accept": "application/json,text/html,application/xhtml+xml",
}

PREFIX_TITLES = {
    "dr", "dr.", "prof", "prof.", "professor", "mr", "mr.", "mrs", "mrs.",
    "ms", "ms.", "miss", "sir", "dame",
}
SUFFIXES = {
    "phd", "md", "dphil", "dsc", "scd", "mph", "msc", "ma", "ba", "bsc",
    "mrcpi", "frcpi", "frcp", "frcs", "fmedsci", "jr", "jr.", "sr", "sr.",
    "ii", "iii", "iv",
}


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = html.unescape(str(value))
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[\u00a0\u2028\u2029 \t\r\n]+", " ", text).strip()
    return text or None


def html_to_text(value: Any) -> str | None:
    if value is None:
        return None
    soup = BeautifulSoup(str(value), "html.parser")
    return clean_text(soup.get_text(" ", strip=True))


def fetch_json(session: requests.Session, url: str, params: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, str]]:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                return response.json(), response.headers
            log(f"  REST page={params.get('page')}: HTTP {response.status_code} (attempt {attempt}/{MAX_RETRIES})")
        except requests.RequestException as exc:
            last_exc = exc
            log(f"  REST page={params.get('page')}: {exc} (attempt {attempt}/{MAX_RETRIES})")
        time.sleep(min(2 ** attempt, 30))
    raise RuntimeError(f"REST fetch failed for {url} params={params}: {last_exc}")


def fetch_text(url: str) -> str | None:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                response.encoding = "utf-8"
                return response.text
            log(f"  detail {url}: HTTP {response.status_code} (attempt {attempt}/{MAX_RETRIES})")
        except requests.RequestException as exc:
            last_exc = exc
            log(f"  detail {url}: {exc} (attempt {attempt}/{MAX_RETRIES})")
        time.sleep(min(2 ** attempt, 30))
    log(f"  GIVING UP detail {url}: {last_exc}")
    return None


def split_name(full_name: str | None) -> tuple[str | None, str | None]:
    """Canonical given/family split: strip titles/suffixes, last token is family."""
    name = clean_text(full_name)
    if not name:
        return None, None
    tokens = name.split()
    while tokens and tokens[0].lower().strip(",.") in PREFIX_TITLES:
        tokens.pop(0)
    while tokens and tokens[-1].lower().strip(",.") in SUFFIXES:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_amount_eur(raw: str | None) -> float | None:
    text = clean_text(raw)
    if not text:
        return None
    m = re.search(r"[-+]?€?\s*([0-9][0-9,]*(?:\.[0-9]+)?)", text)
    if not m:
        return None
    try:
        value = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    return value if value > 0 else None


def parse_year(raw: str | None) -> str | None:
    text = clean_text(raw)
    if not text:
        return None
    m = re.search(r"(19|20)\d{2}", text)
    if not m:
        return None
    year = int(m.group(0))
    return str(year) if 1800 <= year <= 2100 else None


def parse_duration_months(raw: str | None) -> str | None:
    text = clean_text(raw)
    if not text:
        return None
    years = 0
    months = 0
    m = re.search(r"(\d+)\s*years?", text, flags=re.I)
    if m:
        years = int(m.group(1))
    m = re.search(r"(\d+)\s*months?", text, flags=re.I)
    if m:
        months = int(m.group(1))
    total = years * 12 + months
    return str(total) if total > 0 else None


def classify_funding_type(scheme: str | None, scheme_type: str | None) -> str:
    haystack = " ".join(x for x in [scheme, scheme_type] if x).lower()
    if re.search(r"scholarship|fellowship|emerging investigator|clinician scientist|career", haystack):
        return "fellowship"
    if re.search(r"training|capacity building|doctoral|postdoctoral|summer student", haystack):
        return "training"
    return "research"


def extract_detail_cards(detail_html: str | None) -> dict[str, str | None]:
    if not detail_html:
        return {}
    soup = BeautifulSoup(detail_html, "html.parser")
    cards: dict[str, str | None] = {}
    for card in soup.select(".small-card"):
        title = card.select_one(".card-title")
        value = card.select_one(".card-value")
        if title and value:
            cards[clean_text(title.get_text(" ", strip=True)) or ""] = clean_text(value.get_text(" ", strip=True))
    return cards


def normalise_record(post: dict[str, Any], detail_html: str | None) -> dict[str, Any]:
    cards = extract_detail_cards(detail_html)
    post_id = post.get("id")
    title = html_to_text((post.get("title") or {}).get("rendered"))
    description = html_to_text((post.get("content") or {}).get("rendered"))
    pi_raw = cards.get("Principal Investigator")
    given_name, family_name = split_name(pi_raw)
    amount = parse_amount_eur(cards.get("Grant Value"))
    award_year = parse_year(cards.get("Award Year"))
    duration_months = parse_duration_months(cards.get("Duration"))
    scheme = cards.get("Scheme")
    scheme_type = cards.get("Scheme Type")
    return {
        "funder_award_id": f"hrb-{post_id}",
        "wp_post_id": str(post_id) if post_id is not None else None,
        "slug": clean_text(post.get("slug")),
        "display_name": title,
        "description": description,
        "award_year_raw": cards.get("Award Year"),
        "duration_raw": cards.get("Duration"),
        "duration_months": duration_months,
        "amount": f"{amount:.2f}" if amount is not None else None,
        "currency": "EUR" if amount is not None else None,
        "principal_investigator_raw": pi_raw,
        "lead_given_name": given_name,
        "lead_family_name": family_name,
        "host_institution": cards.get("Host Institution"),
        "funder_scheme": scheme,
        "scheme_type": scheme_type,
        "hrb_broad_research_area": cards.get("HRB Broad Research Area Classification"),
        "funding_type": classify_funding_type(scheme, scheme_type),
        "start_year": award_year,
        "end_year": None,
        "landing_page_url": clean_text(post.get("link")),
        "wp_date": clean_text(post.get("date")),
        "wp_modified": clean_text(post.get("modified")),
        "source_page_url": SOURCE_PAGE_URL,
        "provenance": PROVENANCE,
        "funder_id": str(FUNDER_ID),
        "funder_display_name": FUNDER_DISPLAY_NAME,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }


def enumerate_rest_posts(limit: int | None) -> list[dict[str, Any]]:
    session = requests.Session()
    session.headers.update(HEADERS)
    fields = "id,slug,link,title,content,date,modified"
    params = {"per_page": PER_PAGE, "page": 1, "_fields": fields}
    rows, headers = fetch_json(session, REST_ENDPOINT, params)
    total = int(headers.get("X-WP-Total") or len(rows))
    total_pages = int(headers.get("X-WP-TotalPages") or max(1, math.ceil(total / PER_PAGE)))
    log(f"  REST reports {total:,} grant-approved posts across {total_pages} pages")
    posts = list(rows)
    for page in range(2, total_pages + 1):
        if limit is not None and len(posts) >= limit:
            break
        rows, _ = fetch_json(session, REST_ENDPOINT, {"per_page": PER_PAGE, "page": page, "_fields": fields})
        if not rows:
            log(f"  page={page}: empty response; continuing because REST reported {total_pages} total pages")
            continue
        posts.extend(rows)
        if page % 5 == 0 or page == total_pages:
            log(f"  fetched REST page {page}/{total_pages}; posts={len(posts):,}")
    if limit is not None:
        posts = posts[:limit]
    deduped: list[dict[str, Any]] = []
    seen: dict[str, dict[str, Any]] = {}
    duplicate_count = 0
    for post in posts:
        post_id = str(post.get("id"))
        if post_id in seen:
            prior = seen[post_id]
            comparable = {
                "slug": post.get("slug"),
                "link": post.get("link"),
                "title": (post.get("title") or {}).get("rendered"),
            }
            prior_comparable = {
                "slug": prior.get("slug"),
                "link": prior.get("link"),
                "title": (prior.get("title") or {}).get("rendered"),
            }
            if comparable != prior_comparable:
                raise RuntimeError(f"Conflicting duplicate REST post id {post_id}: {comparable} vs {prior_comparable}")
            duplicate_count += 1
            continue
        seen[post_id] = post
        deduped.append(post)
    if duplicate_count:
        log(f"  dropped {duplicate_count:,} exact duplicate REST posts by WordPress post id")
    return deduped


def enrich_posts(posts: list[dict[str, Any]], workers: int) -> list[dict[str, Any]]:
    records: list[dict[str, Any] | None] = [None] * len(posts)

    def fetch_one(idx_post: tuple[int, dict[str, Any]]) -> tuple[int, dict[str, Any]]:
        idx, post = idx_post
        detail_html = fetch_text(post.get("link") or "")
        return idx, normalise_record(post, detail_html)

    log(f"  fetching {len(posts):,} detail pages with {workers} workers")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = [executor.submit(fetch_one, item) for item in enumerate(posts)]
        for done, future in enumerate(concurrent.futures.as_completed(futures), 1):
            idx, record = future.result()
            records[idx] = record
            if done % 100 == 0 or done == len(posts):
                log(f"  detail pages {done:,}/{len(posts):,}")
    return [r for r in records if r is not None]


def download_records(output_dir: Path, limit: int | None, workers: int) -> list[dict[str, Any]]:
    posts = enumerate_rest_posts(limit)
    if not posts:
        raise RuntimeError("No HRB grant posts found.")
    records = enrich_posts(posts, workers)
    cache_path = output_dir / JSON_CACHE_FILENAME
    cache_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"Wrote JSON cache {cache_path} ({cache_path.stat().st_size:,} bytes)")
    return records


def load_cached_records(output_dir: Path) -> list[dict[str, Any]]:
    cache_path = output_dir / JSON_CACHE_FILENAME
    if not cache_path.exists():
        raise FileNotFoundError(
            f"--skip-download set but {cache_path} does not exist. "
            "Run once without --skip-download first."
        )
    records = json.loads(cache_path.read_text(encoding="utf-8"))
    log(f"Loaded {len(records):,} cached records from {cache_path}")
    return records


def dedupe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: dict[str, dict[str, Any]] = {}
    duplicate_count = 0
    for record in records:
        key = record.get("funder_award_id")
        if key in seen:
            prior = dict(seen[key])
            current = dict(record)
            prior.pop("downloaded_at", None)
            current.pop("downloaded_at", None)
            if prior != current:
                raise RuntimeError(f"Conflicting duplicate record for funder_award_id={key}")
            duplicate_count += 1
            continue
        seen[key] = record
        deduped.append(record)
    if duplicate_count:
        log(f"  dropped {duplicate_count:,} exact duplicate enriched records by funder_award_id")
    return deduped


def build_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    expected_cols = [
        "funder_award_id", "wp_post_id", "slug", "display_name", "description",
        "award_year_raw", "duration_raw", "duration_months", "amount",
        "currency", "principal_investigator_raw", "lead_given_name",
        "lead_family_name", "host_institution", "funder_scheme", "scheme_type",
        "hrb_broad_research_area", "funding_type", "start_year", "end_year",
        "landing_page_url", "wp_date", "wp_modified", "source_page_url",
        "provenance", "funder_id", "funder_display_name", "downloaded_at",
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    return df[expected_cols]


def validate(df: pd.DataFrame) -> None:
    n = len(df)
    log(f"Local validation: {n:,} HRB grant awards")
    if n < EXPECTED_MIN_FULL_ROWS:
        log(f"  WARNING: row count {n:,} is below expected full-run floor {EXPECTED_MIN_FULL_ROWS:,}")

    def pct(col: str) -> None:
        count = int(df[col].notna().sum())
        pct_value = 100.0 * count / n if n else 0.0
        log(f"  {col:<28}{count:>7}/{n:<7} ({pct_value:5.1f}%)")

    for col in [
        "funder_award_id",
        "display_name",
        "description",
        "principal_investigator_raw",
        "lead_family_name",
        "host_institution",
        "amount",
        "currency",
        "start_year",
        "funder_scheme",
        "scheme_type",
    ]:
        pct(col)

    distinct_ids = df["funder_award_id"].nunique(dropna=False)
    log(f"  unique funder_award_id {distinct_ids:,}/{n:,}")
    if distinct_ids != n:
        raise RuntimeError("funder_award_id is not unique.")

    years = pd.to_numeric(df["start_year"], errors="coerce").dropna()
    if len(years):
        log(f"  start_year range {int(years.min())}-{int(years.max())}")
    durations = pd.to_numeric(df["duration_months"], errors="coerce").dropna()
    if len(durations):
        log(f"  duration_months range {int(durations.min())}-{int(durations.max())}")
    amounts = pd.to_numeric(df["amount"], errors="coerce").dropna()
    if len(amounts):
        log(
            f"  amount range EUR {amounts.min():,.0f}-{amounts.max():,.0f}; "
            f"total EUR {amounts.sum():,.0f}"
        )

    log("  funding_type distribution:")
    for value, count in df["funding_type"].fillna("(missing)").value_counts().head(12).items():
        log(f"    {value}: {count}")
    log("  top funder_scheme values:")
    for value, count in df["funder_scheme"].fillna("(missing)").value_counts().head(12).items():
        log(f"    {value}: {count}")


def check_no_shrink(local_path: Path, allow_shrink: bool, client: Any) -> None:
    new_rows = len(pd.read_parquet(local_path))
    log(f"Runbook section 1.4 shrink-check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        previous_path = local_path.with_suffix(".previous.parquet")
        client.download_file(S3_BUCKET, S3_KEY, str(previous_path))
        previous_rows = len(pd.read_parquet(previous_path))
        log(f"  previous S3 rows={previous_rows:,}, new rows={new_rows:,}")
        if new_rows < previous_rows and not allow_shrink:
            raise RuntimeError(
                f"Refusing to shrink: new {new_rows:,} < previous {previous_rows:,}. "
                "Use --allow-shrink to override after manual review."
            )
    except client.exceptions.ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            log("  No existing S3 parquet found; treating this as first ingest.")
            return
        raise


def upload_to_s3(local_path: Path, allow_shrink: bool) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for upload; use --skip-upload locally.") from exc
    client = boto3.client("s3")
    check_no_shrink(local_path, allow_shrink, client)
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    client.upload_file(str(local_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="HRB Ireland Grants Approved -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit", type=int, default=None, help="Smoke test: process first N grants")
    parser.add_argument("--skip-download", action="store_true", help="Use cached enriched JSON from --output-dir")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--detail-workers", type=int, default=6, help="Concurrent detail-page fetches")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    log("=" * 72)
    log("HRB Ireland Grants Approved ingest starting")
    log(f"source={SOURCE_PAGE_URL}")
    log(f"rest={REST_ENDPOINT}")
    log(f"funder_id=F{FUNDER_ID}")
    log(f"provenance={PROVENANCE}")

    if args.skip_download:
        records = load_cached_records(args.output_dir)
        if args.limit is not None:
            records = records[:args.limit]
    else:
        records = download_records(args.output_dir, args.limit, args.detail_workers)

    records = dedupe_records(records)
    df = build_dataframe(records)
    validate(df)

    output_path = args.output_dir / PARQUET_FILENAME
    df = df.astype("string")
    df.to_parquet(output_path, index=False)
    log(f"Wrote {output_path} ({output_path.stat().st_size:,} bytes)")

    if args.skip_upload:
        log("--skip-upload set; not uploading to S3.")
        return
    upload_to_s3(output_path, args.allow_shrink)


if __name__ == "__main__":
    main()
