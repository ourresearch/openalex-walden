#!/usr/bin/env python3
"""
Letten Prize laureates -> S3 Data Pipeline
=========================================

Downloads official Letten Prize WordPress REST records and writes a parquet
file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party: lettenprize.com is the official Letten Prize site.
It states that the Letten Foundation and the Young Academy of Norway launched
the prize, and exposes public WordPress REST records for the canonical winner
pages:

    https://lettenprize.com/wp-json/wp/v2/posts
    https://lettenprize.com/wp-json/wp/v2/pages

The OpenAlex funder is Letten Foundation (F4320328141). This ingest maps the
Letten Prize rows to that funder because the official site identifies the
Letten Foundation as the foundation partner and funder behind the prize.

Validation on 2026-05-28:
  - 4 laureate rows: 2018, 2021, 2023, 2025
  - 100% coverage: funder_award_id, laureate name, title, year, amount,
    currency, citation/description, landing_page_url
  - Amounts are NOK 2,000,000 for 2018/2021 and NOK 2,500,000 for 2023/2025,
    following the official winner/call/criteria pages

Output
------
s3://openalex-ingest/awards/letten_prize/letten_prize_laureates.parquet

Usage
-----
    python letten_prize_to_s3.py --skip-upload
    python letten_prize_to_s3.py --limit 2 --skip-upload
    python letten_prize_to_s3.py --skip-download --skip-upload
    python letten_prize_to_s3.py --allow-shrink

Requirements
------------
    pip install beautifulsoup4 pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import subprocess
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup


POSTS_API_URL = "https://lettenprize.com/wp-json/wp/v2/posts"
PAGES_API_URL = "https://lettenprize.com/wp-json/wp/v2/pages"
ABOUT_URL = "https://lettenprize.com/about-letten-prize/"
CRITERIA_URL = "https://lettenprize.com/criteria/"

# Awarding body/funder: Letten Foundation.
# Verified in OpenAlex as F4320328141 (NO).
FUNDER_ID = 4320328141
FUNDER_DISPLAY_NAME = "Letten Foundation"
PROVENANCE = "letten_prize"
FUNDER_SCHEME = "Letten Prize"
FUNDING_TYPE = "prize"
CURRENCY = "NOK"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/letten_prize/letten_prize_laureates.parquet"

USER_AGENT = "openalex-walden-letten-prize-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25

RAW_JSON_FILENAME = "letten_prize_wp_rest_raw.json"
PARQUET_FILENAME = "letten_prize_laureates.parquet"

# Canonical winner records exposed in the official site menu. Pages are used
# where present; 2025 currently exists as a post only.
CANONICAL_WINNERS = {
    2018: {"type": "page", "slug": "winner-of-the-2018-letten-prize"},
    2021: {"type": "page", "slug": "winner-of-the-2021-letten-prize"},
    2023: {"type": "page", "slug": "winner-of-the-2023-letten-prize"},
    2025: {"type": "post", "slug": "2025-letten-prize-laureate"},
}

_last_request_t = 0.0


def log(message: str) -> None:
    print(message, flush=True)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    raw = str(value)
    if "<" in raw or "&" in raw:
        text = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)
    else:
        text = raw
    text = html.unescape(text)
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_text.lower()).strip("-")
    return slug or "unknown"


def split_name(name: str) -> tuple[Optional[str], Optional[str]]:
    parts = [p for p in clean_text(name).split(" ") if p] if clean_text(name) else []
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return " ".join(parts[:-1]), parts[-1]


def polite_get_json(url: str, *, params: Optional[dict[str, Any]] = None,
                    timeout: int = 60, max_attempts: int = 4) -> Any:
    global _last_request_t
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Referer": "https://lettenprize.com/",
    }
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < MIN_REQUEST_INTERVAL_S:
            time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            _last_request_t = time.monotonic()
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 20)
                log(f"  [WARN] HTTP {resp.status_code}; retrying in {sleep_s}s")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 20)
            log(f"  [WARN] request failed ({exc}); retrying in {sleep_s}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def fetch_wp_collection(url: str) -> list[dict[str, Any]]:
    params = {
        "per_page": 100,
        "_fields": "id,date,slug,link,title,content,excerpt,modified,type",
    }
    data = polite_get_json(url, params=params)
    if not isinstance(data, list):
        raise RuntimeError(f"Expected list from WordPress REST endpoint: {url}")
    log(f"  [OK] fetched {len(data):,} records from {url}")
    return data


def fetch_sources() -> dict[str, Any]:
    log("\n" + "=" * 60)
    log("Step 1: Download official Letten Prize WordPress REST data")
    log("=" * 60)
    posts = fetch_wp_collection(POSTS_API_URL)
    pages = fetch_wp_collection(PAGES_API_URL)
    return {
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "posts_api_url": POSTS_API_URL,
        "pages_api_url": PAGES_API_URL,
        "posts": posts,
        "pages": pages,
    }


def find_item(payload: dict[str, Any], year: int, spec: dict[str, str]) -> dict[str, Any]:
    collection = payload["pages"] if spec["type"] == "page" else payload["posts"]
    matches = [item for item in collection if item.get("slug") == spec["slug"]]
    if len(matches) != 1:
        raise RuntimeError(f"Expected one {spec['type']} with slug {spec['slug']!r}; found {len(matches)}")
    item = dict(matches[0])
    item["source_type"] = spec["type"]
    item["award_year"] = year
    return item


def rendered_text(item: dict[str, Any]) -> str:
    return clean_text(item.get("content", {}).get("rendered")) or ""


def rendered_title(item: dict[str, Any]) -> str:
    return clean_text(item.get("title", {}).get("rendered")) or ""


def parse_laureate_name(item: dict[str, Any]) -> str:
    title = rendered_title(item)
    text = rendered_text(item)

    title_match = re.search(r"^(.+?) wins the Letten Prize$", title, flags=re.IGNORECASE)
    if title_match:
        return clean_text(title_match.group(1))

    to_match = re.search(
        r"awards the Letten Prize\s+\d{4}\s+to\s+([A-Z][A-Za-zÀ-ÖØ-öø-ÿ .'\-]+?)\s+for\s+",
        text,
    )
    if to_match:
        return clean_text(to_match.group(1))

    first_sentence_match = re.search(
        r"^[\"“]?\s*([A-Z][A-Za-zÀ-ÖØ-öø-ÿ .'\-]+?)\s+(?:hails|is affiliated|\(from)",
        text,
    )
    if first_sentence_match:
        return clean_text(first_sentence_match.group(1))

    soup = BeautifulSoup(item.get("content", {}).get("rendered") or "", "html.parser")
    for strong in soup.find_all("strong"):
        candidate = clean_text(strong.get_text(" ", strip=True))
        if not candidate:
            continue
        if re.search(r"Citation|Video|Winner|Prize|Get to|Responding", candidate, flags=re.IGNORECASE):
            continue
        if re.match(r"^[A-Z][A-Za-zÀ-ÖØ-öø-ÿ .'\-]+ [A-Z][A-Za-zÀ-ÖØ-öø-ÿ .'\-]+$", candidate):
            return candidate

    raise RuntimeError(f"Could not parse laureate name from {item.get('link')}")


def parse_affiliation(item: dict[str, Any], laureate_name: str) -> Optional[str]:
    text = rendered_text(item)
    year = item["award_year"]
    if year == 2025 and "Massachusetts Institute of Technology" in text:
        return "Massachusetts Institute of Technology"
    if year == 2023:
        match = re.search(r"currently\s+Assistant Professor at\s+(.+?)(?:,\s+where|\.|$)", text)
        if match:
            return clean_text(match.group(1))
    if year == 2021:
        match = re.search(r"affiliated with the\s+(.+?)\s+where", text)
        if match:
            return clean_text(match.group(1))
    if year == 2018:
        match = re.search(r"is affiliated with the\s+(.+?)\.\s+", text)
        if match:
            return clean_text(match.group(1))
    return None


def parse_description(item: dict[str, Any], laureate_name: str) -> Optional[str]:
    text = rendered_text(item)
    if not text:
        return None
    if len(text) <= 1200:
        return text
    # Keep the source citation/explanation concise for the awards table while
    # preserving the full official content in raw JSON.
    sentences = re.split(r"(?<=[.!?])\s+", text)
    selected: list[str] = []
    for sentence in sentences:
        if laureate_name.split()[-1] in sentence or "Letten Prize" in sentence or len(selected) < 2:
            selected.append(sentence)
        if len(" ".join(selected)) > 900:
            break
    return clean_text(" ".join(selected)) or text[:1200]


def amount_for_year(year: int) -> tuple[str, str]:
    if year <= 2021:
        return "2000000", "Official 2018/2021 winner pages state NOK 2,000,000."
    return "2500000", "Official 2023 call/current criteria state NOK 2,500,000."


def normalize_records(payload: dict[str, Any], *, full_run: bool) -> pd.DataFrame:
    log("\n" + "=" * 60)
    log("Step 2: Normalize laureate records")
    log("=" * 60)

    rows: list[dict[str, Any]] = []
    for year, spec in sorted(CANONICAL_WINNERS.items()):
        item = find_item(payload, year, spec)
        laureate = parse_laureate_name(item)
        given_name, family_name = split_name(laureate)
        amount, amount_note = amount_for_year(year)
        source_hash = hashlib.sha1(json.dumps(item, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()[:12]
        award_date = clean_text(item.get("date"))
        award_date_day = award_date[:10] if award_date else None
        rows.append({
            "funder_award_id": f"letten-prize-{year}-{slugify(laureate)}",
            "source_record_id": str(item.get("id")),
            "source_type": item.get("source_type"),
            "source_slug": item.get("slug"),
            "source_hash": source_hash,
            "display_name": f"Letten Prize {year} - {laureate}",
            "description": parse_description(item, laureate),
            "laureate_name": laureate,
            "given_name": given_name,
            "family_name": family_name,
            "affiliation": parse_affiliation(item, laureate),
            "award_year": str(year),
            "award_date": award_date_day,
            "amount": amount,
            "currency": CURRENCY,
            "amount_note": amount_note,
            "funder_scheme": FUNDER_SCHEME,
            "funding_type": FUNDING_TYPE,
            "landing_page_url": clean_text(item.get("link")),
            "source_title": rendered_title(item),
            "source_modified": clean_text(item.get("modified")),
            "about_url": ABOUT_URL,
            "criteria_url": CRITERIA_URL,
            "retrieved_at": payload["retrieved_at"],
        })

    if rows and full_run:
        rows = rows
    df = pd.DataFrame(rows)
    validate_dataframe(df, full_run=full_run)
    return df


def validate_dataframe(df: pd.DataFrame, *, full_run: bool) -> None:
    total = len(df)
    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")

    required_cols = [
        "funder_award_id",
        "display_name",
        "description",
        "laureate_name",
        "given_name",
        "family_name",
        "award_year",
        "award_date",
        "amount",
        "currency",
        "landing_page_url",
    ]
    for col in required_cols:
        covered = int(df[col].notna().sum())
        pct = covered / total * 100 if total else 0.0
        log(f"  {col:18s}: {covered:,}/{total:,} ({pct:.1f}%)")
        if covered != total:
            raise RuntimeError(f"Unexpected NULL values in required source column {col}")

    affiliation_covered = int(df["affiliation"].notna().sum())
    log(f"  {'affiliation':18s}: {affiliation_covered:,}/{total:,} ({affiliation_covered / total * 100 if total else 0:.1f}%)")

    if full_run and total != len(CANONICAL_WINNERS):
        raise RuntimeError(f"Full Letten Prize run returned {total:,} rows; expected {len(CANONICAL_WINNERS)}")

    amount_numeric = pd.to_numeric(df["amount"], errors="coerce")
    if amount_numeric.isna().any():
        raise RuntimeError("All Letten Prize rows should have numeric NOK amounts")

    log(f"  [OK] unique funder_award_id values: {df['funder_award_id'].nunique():,}")
    log(f"  Year range: {df['award_year'].min()} - {df['award_year'].max()}")
    log(f"  Total NOK amount: {amount_numeric.sum():,.0f}")
    log(f"  Amount distribution: {df['amount'].value_counts(dropna=False).to_dict()}")
    log(f"  Laureates: {df['laureate_name'].tolist()}")


def write_outputs(payload: dict[str, Any], df: pd.DataFrame, output_dir: Path) -> Path:
    log("\n" + "=" * 60)
    log("Step 3: Write raw JSON and parquet")
    log("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_json_path = output_dir / RAW_JSON_FILENAME
    raw_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"  [OK] wrote raw WordPress REST payload to {raw_json_path}")

    parquet_path = output_dir / PARQUET_FILENAME
    parquet_df = df.astype("string")
    parquet_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    size_kb = parquet_path.stat().st_size / 1024
    log(f"  [OK] wrote {len(parquet_df):,} rows ({size_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def load_cached_payload(output_dir: Path) -> dict[str, Any]:
    raw_json_path = output_dir / RAW_JSON_FILENAME
    if not raw_json_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {raw_json_path}")
    payload = json.loads(raw_json_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "posts" not in payload or "pages" not in payload:
        raise RuntimeError(f"Cached JSON does not look like a Letten Prize payload: {raw_json_path}")
    log(f"  [OK] loaded cached WordPress REST payload from {raw_json_path}")
    return payload


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook section 1.4: refuse to overwrite S3 with a smaller corpus."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for the runbook section 1.4 shrink-check; "
            "rerun with --skip-upload for local validation."
        ) from exc

    client = boto3.client("s3")
    log(f"  Re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            log("    no existing parquet: first ingest, no shrink check needed.")
            return True
        log(f"    [WARN] head_object failed ({code}); treating as first ingest.")
        return True

    prev_path = output_dir / "_prev_letten_prize_laureates.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as exc:
        log(f"    [ERROR] could not read existing parquet ({exc}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)

    log(f"    previous count: {prev_count:,}   new count: {new_count:,}")
    if new_count < prev_count:
        if allow_shrink:
            log("    [OVERRIDE] new corpus is smaller but --allow-shrink was set.")
            return True
        log(f"\n[ERROR] Refusing to shrink Letten Prize corpus ({prev_count:,} -> {new_count:,}).")
        return False
    log("    [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path, allow_shrink: bool) -> bool:
    log("\n" + "=" * 60)
    log("Step 4: Upload to S3")
    log("=" * 60)
    if not check_no_shrink(len(df), allow_shrink, output_dir):
        return False

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    log(f"  Uploading {parquet_path} -> {s3_uri}")
    try:
        subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
        log(f"  [OK] uploaded to {s3_uri}")
        return True
    except FileNotFoundError:
        log("[ERROR] aws CLI not found.")
        return False
    except subprocess.CalledProcessError as exc:
        log(f"[ERROR] aws s3 cp failed (exit {exc.returncode}).")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download Letten Prize laureates and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/letten_prize"))
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached WordPress REST payload from output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    log("=" * 60)
    log("Letten Prize laureates -> S3 pipeline")
    log("=" * 60)
    log(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    log(f"  Source:     {POSTS_API_URL} and {PAGES_API_URL}")
    log(f"  Output dir: {args.output_dir.absolute()}")
    log(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    log(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        log("\nStep 1: Reuse cached WordPress REST payload")
        payload = load_cached_payload(args.output_dir)
    else:
        payload = fetch_sources()

    df = normalize_records(payload, full_run=args.limit is None)
    if args.limit:
        df = df.head(args.limit).copy()
        log(f"  [LIMIT] keeping first {len(df):,} laureate rows")
        validate_dataframe(df, full_run=False)

    parquet_path = write_outputs(payload, df, args.output_dir)

    if args.skip_upload:
        log("\n[SKIP] --skip-upload set; not uploading to S3.")
        log(f"Local parquet ready: {parquet_path}")
        return

    ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
