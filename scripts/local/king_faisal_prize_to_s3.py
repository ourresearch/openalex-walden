#!/usr/bin/env python3
"""
King Faisal Prize to S3 (PRIZE PATTERN)
=======================================

Fetches King Faisal Prize laureates from the prize's official all-winners
table and official detail pages. This follows the awards runbook's
prize-pattern source-authority rule: prize rows come from the awarding body,
not from Wikipedia, Wikidata, or secondary biographies.

Official source pages:
  - https://kingfaisalprize.org/all-winners/
    Full all-time winners table.
  - https://kingfaisalprize.org/nominations/
    Current prize-components page, including the SAR 750,000 / USD 200,000
    amount rule.

Output:
  s3://openalex-ingest/awards/king_faisal_prize/king_faisal_prize_laureates.parquet

Awarding body in OpenAlex:
  King Faisal Foundation (F4320323301; ROR https://ror.org/00n60x364;
  DOI 10.13039/501100005418)

Parsing notes:
  The official table has one row per laureate, so the script does not need
  fragile human-name splitting. Shared prizes are inferred by grouping rows on
  (award_year, award_category), then the official SAR 750,000 prize amount is
  apportioned across that group.
"""

from __future__ import annotations

import argparse
import json
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

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

BASE_URL = "https://kingfaisalprize.org"
ALL_WINNERS_URL = "https://kingfaisalprize.org/all-winners/"
AMOUNT_RULE_URL = "https://kingfaisalprize.org/nominations/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/king_faisal_prize/king_faisal_prize_laureates.parquet"
OUTPUT_FILE = "king_faisal_prize_laureates.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.15
RETRIES = 3

SOURCE_PRIZE_AMOUNT_SAR = 750_000
CURRENCY = "SAR"

ORG_TERMS = {
    "academy",
    "association",
    "center",
    "centre",
    "foundation",
    "institute",
    "institution",
    "organization",
    "project",
    "society",
    "university",
}

TITLE_PREFIX_RE = re.compile(
    r"^(?:"
    r"Professor|Professsor|Prof\.|Doctor|Dr\.?|Mr\.?|Mrs\.?|Ms\.?|"
    r"Shaikh|Sheikh|Sayyid|Sir|Dame|President|Field Marshal|"
    r"H\.\s*E\.\s*Dr\.?|H\.\s*E\.|H\.E\.\s*Dr\.?|H\.E\.|"
    r"His Excellency|His Exellency|His Highness|His Majesty|HRH Prince|"
    r"The Honorable|Seri Dato|"
    r"Custodian of the Two Holy Mosques"
    r")(?:\s+|(?=[A-Z]))",
    flags=re.IGNORECASE,
)


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


def clean_laureate_name(name: str | None) -> str | None:
    name = collapse_text(name)
    if not name:
        return None
    name = re.sub(r"\b([A-Z])\.(?=[A-Z][a-z])", r"\1. ", name)
    previous = None
    while previous != name:
        previous = name
        name = collapse_text(TITLE_PREFIX_RE.sub("", name)) or ""
    return name or None


def is_organization_name(name: str | None) -> bool:
    if not name:
        return False
    lowered = name.lower()
    if any(term in lowered for term in ORG_TERMS):
        return True
    if re.fullmatch(r"[A-Z0-9&.\- ]{3,}", name) and " " not in name.strip("."):
        return True
    return False


def split_name(name: str | None, is_org: bool) -> tuple[str | None, str | None]:
    if not name:
        return None, None
    if is_org:
        return None, name
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
            if not response.text:
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


def parse_all_winners(html: str) -> list[dict[str, str | None]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        raise RuntimeError(f"No all-winners table found on {ALL_WINNERS_URL}")

    rows: list[dict[str, str | None]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) != 5:
            continue
        year = collapse_text(cells[0].get_text(" ", strip=True))
        category = collapse_text(cells[1].get_text(" ", strip=True))
        topic = collapse_text(cells[3].get_text(" ", strip=True))
        country = collapse_text(cells[4].get_text(" ", strip=True))
        name_anchor = cells[2].select_one("span.gold a")
        if not name_anchor:
            name_anchor = next((a for a in cells[2].find_all("a") if collapse_text(a.get_text(" ", strip=True))), None)
        if not (year and category and name_anchor and name_anchor.get("href")):
            raise RuntimeError(f"Could not parse required cells from row: {tr}")
        official_name = collapse_text(name_anchor.get_text(" ", strip=True))
        clean_name = clean_laureate_name(official_name)
        detail_url = urljoin(BASE_URL, name_anchor["href"])
        prize_title = f"King Faisal Prize in {category}"
        rows.append({
            "award_year": year,
            "award_category": category,
            "topic": topic,
            "country": country,
            "official_laureate_name": official_name,
            "laureate_name": clean_name,
            "detail_url": detail_url,
            "prize_title": prize_title,
            "source_url": ALL_WINNERS_URL,
        })

    if not rows:
        raise RuntimeError("No King Faisal Prize laureate rows parsed; stopping before detail fetch.")
    log(f"Parsed {len(rows)} laureate rows from official all-winners table")
    return rows


def parse_detail_page(html: str) -> dict[str, str | None]:
    soup = BeautifulSoup(html, "html.parser")
    h1 = collapse_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else None)
    quote = collapse_text(soup.find("h2").get_text(" ", strip=True) if soup.find("h2") else None)
    text = collapse_text(soup.get_text(" ", strip=True)) or ""

    topic_detail = None
    topic_match = re.search(r'Topic:\\s*"([^"]+)"', text)
    if topic_match:
        topic_detail = collapse_text(topic_match.group(1))

    citation = None
    marker = " was awarded the prize "
    marker_pos = text.lower().find(marker)
    if marker_pos >= 0:
        end_candidates = [
            idx for idx in [
                text.find(" Biography ", marker_pos),
                text.find(" Laureate's Honoring", marker_pos),
                text.find(" Laureate's Interview", marker_pos),
                text.find(" Laureate's Certificate", marker_pos),
                text.find(" Scientific Article", marker_pos),
            ]
            if idx >= 0
        ]
        end = min(end_candidates) if end_candidates else len(text)
        tail = text[marker_pos + len(marker):end]
        citation = collapse_text(tail[:2000])
        if citation:
            citation = citation[0].upper() + citation[1:]

    meta_description = None
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        meta_description = collapse_text(meta["content"])

    return {
        "detail_title": h1,
        "quote": quote,
        "topic_detail": topic_detail,
        "citation": citation,
        "meta_description": meta_description,
    }


def verify_amount_rule(html: str) -> None:
    text = collapse_text(BeautifulSoup(html, "html.parser").get_text(" ", strip=True)) or ""
    if "SR 750,000" not in text or "US$ 200,000" not in text:
        raise RuntimeError("Could not verify official King Faisal Prize amount rule on nominations page.")


def add_amounts_and_ids(rows: list[dict[str, str | None]]) -> list[dict[str, str | None]]:
    group_sizes: dict[tuple[str | None, str | None], int] = {}
    for row in rows:
        key = (row.get("award_year"), row.get("award_category"))
        group_sizes[key] = group_sizes.get(key, 0) + 1

    seen: set[str] = set()
    for row in rows:
        key = (row.get("award_year"), row.get("award_category"))
        group_size = group_sizes[key]
        amount = SOURCE_PRIZE_AMOUNT_SAR / group_size
        detail_slug = (row.get("detail_url") or "").rstrip("/").split("/")[-1]
        award_id = "-".join([
            "king-faisal-prize",
            str(row.get("award_year") or "unknown"),
            slugify(row.get("award_category")),
            slugify(detail_slug or row.get("laureate_name")),
        ])
        if award_id in seen:
            raise RuntimeError(f"Duplicate funder_award_id would be emitted: {award_id}")
        seen.add(award_id)

        row["source_prize_amount_sar"] = str(SOURCE_PRIZE_AMOUNT_SAR)
        row["source_award_amount"] = f"{amount:.2f}"
        row["currency"] = CURRENCY
        row["winner_count"] = str(group_size)
        row["portion"] = f"1/{group_size}" if group_size != 1 else "1"
        row["amount_rule_url"] = AMOUNT_RULE_URL
        row["funder_award_id"] = award_id
    return rows


def fetch_rows(
    session: requests.Session,
    checkpoint_file: Path,
    use_cache: bool,
    limit_detail_pages: int | None,
) -> list[dict[str, str | None]]:
    table_html = get_page(session, ALL_WINNERS_URL, checkpoint_file, use_cache=use_cache)
    amount_html = get_page(session, AMOUNT_RULE_URL, checkpoint_file, use_cache=use_cache)
    verify_amount_rule(amount_html)

    rows = parse_all_winners(table_html)
    if limit_detail_pages is not None:
        rows = rows[:limit_detail_pages]
        log(f"--limit-detail-pages set; detail fetch limited to {len(rows)} rows")

    for index, row in enumerate(rows, start=1):
        detail_html = get_page(session, row["detail_url"] or "", checkpoint_file, use_cache=use_cache)
        detail = parse_detail_page(detail_html)
        row.update(detail)
        row["topic"] = row.get("topic") or detail.get("topic_detail")
        row["landing_page_url"] = row["detail_url"]
        row["laureate_is_organization"] = str(is_organization_name(row.get("laureate_name"))).lower()
        given_name, family_name = split_name(row.get("laureate_name"), row["laureate_is_organization"] == "true")
        row["laureate_given_name"] = given_name
        row["laureate_family_name"] = family_name
        row["downloaded_at"] = utc_now()
        log(
            f"detail {index}/{len(rows)} parsed: {row.get('award_year')} "
            f"{row.get('award_category')} - {row.get('laureate_name')}"
        )

    return add_amounts_and_ids(rows)


def validate_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise RuntimeError("No King Faisal Prize rows parsed; stopping before parquet write.")
    seen: set[str] = set()
    for row in rows:
        award_id = row.get("funder_award_id")
        if not award_id:
            raise RuntimeError(f"Missing funder_award_id for row: {row}")
        if award_id in seen:
            raise RuntimeError(f"Duplicate funder_award_id would be emitted: {award_id}")
        seen.add(award_id)
        for column in ("award_year", "award_category", "laureate_name", "landing_page_url"):
            if not row.get(column):
                raise RuntimeError(f"Missing {column} for row: {row}")


def log_summary(df: pd.DataFrame) -> None:
    log(f"DataFrame shape: {df.shape}")
    log(
        "Coverage: "
        f"name={df.laureate_name.notna().sum()}, "
        f"year={df.award_year.notna().sum()}, "
        f"category={df.award_category.notna().sum()}, "
        f"topic={df.topic.notna().sum()}, "
        f"citation={df.citation.notna().sum()}, "
        f"amount={df.source_award_amount.notna().sum()}, "
        f"currency={df.currency.notna().sum()}"
    )
    log(f"Year range: {df['award_year'].min()}-{df['award_year'].max()}")
    log(f"Rows by category: {df['award_category'].value_counts().sort_index().to_dict()}")
    log(f"Rows by winner_count: {df['winner_count'].value_counts().sort_index().to_dict()}")


def existing_s3_row_count(s3_client: Any, bucket: str, key: str) -> int | None:
    """Return existing parquet row count, or None for first upload.

    This is the runbook Step 1.4 re-ingestion guard: do not overwrite an
    existing S3 parquet with a smaller corpus unless the caller explicitly
    passes --allow-shrink.
    """
    from botocore.exceptions import ClientError

    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        try:
            s3_client.download_file(bucket, key, tmp.name)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        return len(pd.read_parquet(tmp.name))


def upload_to_s3(local_path: Path, row_count: int, allow_shrink: bool) -> None:
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3

    s3 = boto3.client("s3")
    previous_count = existing_s3_row_count(s3, S3_BUCKET, S3_KEY)
    if previous_count is not None:
        log(f"Existing S3 parquet row count: {previous_count}")
        if row_count < previous_count and not allow_shrink:
            raise RuntimeError(
                f"Refusing to shrink King Faisal Prize parquet from {previous_count} "
                f"to {row_count} rows. Re-run with --allow-shrink only after "
                "confirming the source genuinely removed records."
            )
    s3.upload_file(str(local_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="King Faisal Prize -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--checkpoint-file", type=Path, default=Path("/tmp/king_faisal_prize_checkpoint.json"))
    parser.add_argument("--limit-detail-pages", type=int, default=None, help="Smoke-test: fetch only first N detail pages")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--no-cache", action="store_true", help="Refetch pages even when checkpoint cache exists")
    args = parser.parse_args()

    log("=" * 72)
    log("King Faisal Prize -> S3 starting")
    log(f"Official source: {ALL_WINNERS_URL}")
    log(f"S3 target: s3://{S3_BUCKET}/{S3_KEY}")

    session = requests.Session()
    rows = fetch_rows(
        session=session,
        checkpoint_file=args.checkpoint_file,
        use_cache=not args.no_cache,
        limit_detail_pages=args.limit_detail_pages,
    )
    validate_rows(rows)

    df = pd.DataFrame(rows)
    if df["funder_award_id"].duplicated().any():
        examples = df.loc[
            df["funder_award_id"].duplicated(keep=False),
            ["funder_award_id", "landing_page_url", "laureate_name"],
        ]
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

    upload_to_s3(parquet_path, len(df), args.allow_shrink)


if __name__ == "__main__":
    main()
