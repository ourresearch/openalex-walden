#!/usr/bin/env python3
"""
Japan Prize to S3 (PRIZE PATTERN)
=================================

Fetches Japan Prize laureates from the awarding body's official static
laureates-by-year pages. This follows the awards runbook's prize-pattern
source-authority rule: prize rows come from the awarding body directly, not
from Wikipedia, Wikidata, news articles, or third-party biographies.

Official source pages:
  - https://www.japanprize.jp/en/laureates_by_year.html      (2020s)
  - https://www.japanprize.jp/en/laureates_by_year2010.html  (2010s)
  - https://www.japanprize.jp/en/laureates_by_year2000.html  (2000s)
  - https://www.japanprize.jp/en/laureates_by_year1990.html  (1990s)
  - https://www.japanprize.jp/en/laureates_by_year1980.html  (1980s)
  - https://www.japanprize.jp/en/prize.html                  (amount rule)

Output:
  s3://openalex-ingest/awards/japan_prize/japan_prize_laureates.parquet

Awarding body in OpenAlex:
  Japan Prize Foundation (F4320322286, ROR https://ror.org/01z7s7j95,
  DOI 10.13039/501100004016)

Amount rule:
  The official Japan Prize "About us" page states that, as a general rule,
  each laureate receives a certificate, medal, and prize of 100 million yen.
  This script stores source_award_amount=100000000 and currency=JPY for every
  parsed laureate row, with amount_rule_url pointing to that official page.

Parsing notes:
  The laureates table uses rowspan-heavy HTML. The parser expands rowspans into
  a rectangular grid, emits only rows whose "Name (b.-d.)" column contains a
  laureate name, and reads the immediately following affiliation row for
  nationality and affiliation text. Some older rows do not link to profile
  pages; their landing_page_url falls back to the official decade table page.
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

BASE_URL = "https://www.japanprize.jp/en/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/japan_prize/japan_prize_laureates.parquet"
OUTPUT_FILE = "japan_prize_laureates.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.2
RETRIES = 3

AMOUNT_RULE_URL = urljoin(BASE_URL, "prize.html")
SOURCE_AWARD_AMOUNT = "100000000"
CURRENCY = "JPY"

LAUREATE_PAGES = [
    "laureates_by_year.html",
    "laureates_by_year2010.html",
    "laureates_by_year2000.html",
    "laureates_by_year1990.html",
    "laureates_by_year1980.html",
]


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
    """Canonical prize-ingest name splitter from Wolf/Kavli/Abel patterns."""
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


def request_html(session: requests.Session, path_or_url: str) -> str:
    url = path_or_url if path_or_url.startswith("http") else urljoin(BASE_URL, path_or_url)
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
            # The server omits a charset and requests guesses ISO-8859-1.
            # The pages are UTF-8; decode bytes explicitly to avoid mojibake.
            return response.content.decode("utf-8", errors="replace")
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


def get_page(session: requests.Session, path: str, checkpoint_file: Path, use_cache: bool) -> str:
    cache = load_checkpoint(checkpoint_file)
    url = urljoin(BASE_URL, path)
    if use_cache and url in cache:
        log(f"cached {url} ({len(cache[url].encode('utf-8'))} bytes)")
        return cache[url]
    html = request_html(session, url)
    cache[url] = html
    save_checkpoint(checkpoint_file, cache)
    time.sleep(REQUEST_DELAY)
    return html


def cell_payload(cell: Tag, source_url: str) -> dict[str, str | None]:
    anchor = cell.find("a", href=True)
    href = urljoin(source_url, anchor["href"]) if anchor else None
    return {
        "text": collapse_text(cell.get_text(" ", strip=True)),
        "href": href,
        "class": " ".join(cell.get("class", [])),
    }


def expand_rowspan_table(table: Tag, source_url: str) -> list[list[dict[str, str | None]]]:
    """Expand a rowspan-heavy HTML table into a rectangular-ish grid.

    Japan Prize uses rowspans for year, field, photo, and achievements. Each
    laureate row is followed by an affiliation row and often a lecture row.
    Expanding rowspans lets the parser treat each physical row as if it had
    Year / Field / Photo / Name-or-affiliation / Achievement columns.
    """
    grid: list[list[dict[str, str | None]]] = []
    spans: dict[tuple[int, int], tuple[dict[str, str | None], int]] = {}

    for row_idx, tr in enumerate(table.find_all("tr")):
        row: list[dict[str, str | None]] = []
        col_idx = 0

        for cell in tr.find_all(["th", "td"]):
            while (row_idx, col_idx) in spans:
                value, remaining = spans.pop((row_idx, col_idx))
                row.append(value)
                if remaining > 1:
                    spans[(row_idx + 1, col_idx)] = (value, remaining - 1)
                col_idx += 1

            value = cell_payload(cell, source_url)
            rowspan = int(cell.get("rowspan") or 1)
            colspan = int(cell.get("colspan") or 1)
            for offset in range(colspan):
                row.append(value)
                if rowspan > 1:
                    spans[(row_idx + 1, col_idx + offset)] = (value, rowspan - 1)
            col_idx += colspan

        while (row_idx, col_idx) in spans:
            value, remaining = spans.pop((row_idx, col_idx))
            row.append(value)
            if remaining > 1:
                spans[(row_idx + 1, col_idx)] = (value, remaining - 1)
            col_idx += 1

        grid.append(row)

    return grid


def normalize_year(year_text: str | None) -> str | None:
    match = re.search(r"\b((?:19|20)\d{2})\b", year_text or "")
    return match.group(1) if match else None


def clean_laureate_name(name_text: str | None) -> tuple[str | None, str | None, str | None]:
    if not name_text:
        return None, None, None
    raw = collapse_text(name_text)
    life_dates = None
    match = re.search(r"[（(]([^()（）]*\d{4}[^()（）]*)[）)]", raw or "")
    if match:
        life_dates = collapse_text(match.group(1))
    clean = re.sub(r"[（(][^()（）]*\d{4}[^()（）]*[）)]", "", raw or "")
    clean = re.sub(r"^(Prof\.|Dr\.|Mr\.|Ms\.|Sir|Dame)\s+", "", clean).strip()
    return raw, collapse_text(clean), life_dates


def parse_affiliation(value: str | None) -> tuple[str | None, str | None]:
    text = collapse_text(value)
    if not text:
        return None, None
    match = re.match(r"^\[([^\]]+)\]\s*(.*)$", text)
    if not match:
        return None, text
    return collapse_text(match.group(1)), collapse_text(match.group(2))


def parse_decade_page(path: str, html: str) -> list[dict[str, str | None]]:
    source_url = urljoin(BASE_URL, path)
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        raise RuntimeError(f"No laureates table found on {source_url}")

    grid = expand_rowspan_table(table, source_url)
    rows: list[dict[str, str | None]] = []

    for row_idx, row in enumerate(grid[2:], start=2):
        if len(row) < 5:
            continue
        year = normalize_year(row[0]["text"])
        field = collapse_text(row[1]["text"])
        name_raw, name, life_dates = clean_laureate_name(row[3]["text"])
        achievement = collapse_text(row[4]["text"])

        if not year or not field or not name:
            continue
        if name.startswith("[") or name.lower().startswith("lecture"):
            continue

        nationality = None
        affiliation = None
        if row_idx + 1 < len(grid) and len(grid[row_idx + 1]) >= 4:
            nationality, affiliation = parse_affiliation(grid[row_idx + 1][3]["text"])

        given_name, family_name = split_name(name)
        profile_url = row[3]["href"]
        field_url = row[1]["href"]
        landing_page_url = profile_url or source_url
        profile_slug = profile_url.rstrip("/").split("/")[-1].replace(".html", "") if profile_url else slugify(name)
        funder_award_id = "-".join([
            "japan-prize",
            year,
            slugify(field),
            slugify(profile_slug or name),
        ])

        rows.append({
            "source_page_url": source_url,
            "source_table_path": path,
            "profile_url": profile_url,
            "field_url": field_url,
            "landing_page_url": landing_page_url,
            "award_year": year,
            "award_field": field,
            "laureate_name_raw": name_raw,
            "laureate_name": name,
            "life_dates": life_dates,
            "given_name": given_name,
            "family_name": family_name,
            "nationality": nationality,
            "affiliation": affiliation,
            "achievement": achievement,
            "source_award_amount": SOURCE_AWARD_AMOUNT,
            "currency": CURRENCY,
            "amount_rule_url": AMOUNT_RULE_URL,
            "funder_award_id": funder_award_id,
            "downloaded_at": utc_now(),
        })

    log(f"Parsed {len(rows)} laureate rows from {source_url}")
    return rows


def fetch_rows(session: requests.Session, checkpoint_file: Path, use_cache: bool) -> list[dict[str, str | None]]:
    pages = ["prize.html", *LAUREATE_PAGES]
    html_by_path = {
        path: get_page(session, path, checkpoint_file, use_cache=use_cache)
        for path in pages
    }

    prize_text = collapse_text(BeautifulSoup(html_by_path["prize.html"], "html.parser").get_text(" ", strip=True))
    if not prize_text or "100 million yen" not in prize_text:
        raise RuntimeError("Could not verify official Japan Prize amount rule text on prize.html")

    rows: list[dict[str, str | None]] = []
    for path in LAUREATE_PAGES:
        rows.extend(parse_decade_page(path, html_by_path[path]))

    if not rows:
        raise RuntimeError("No Japan Prize laureate rows parsed; stopping before parquet write.")

    return rows


def validate_rows(rows: list[dict[str, str | None]]) -> None:
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
        f"field={df.award_field.notna().sum()}, "
        f"affiliation={df.affiliation.notna().sum()}, "
        f"achievement={df.achievement.notna().sum()}, "
        f"amount={df.source_award_amount.notna().sum()}, "
        f"currency={df.currency.notna().sum()}"
    )
    log(f"Rows by decade page: {df['source_table_path'].value_counts().sort_index().to_dict()}")
    log(f"Year range: {df['award_year'].min()}-{df['award_year'].max()}")
    log(f"Currency distribution: {df['currency'].fillna('NULL').value_counts().to_dict()}")


def upload_to_s3(local_path: Path) -> None:
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3

    s3 = boto3.client("s3")
    s3.upload_file(str(local_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Japan Prize -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--checkpoint-file", type=Path, default=Path("/tmp/japan_prize_checkpoint.json"))
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Refetch official pages even if they exist in the checkpoint file",
    )
    args = parser.parse_args()

    log("=" * 72)
    log("Japan Prize -> S3 starting")
    log("Official source: https://www.japanprize.jp/en/laureates_by_year.html")
    log(f"S3 target: s3://{S3_BUCKET}/{S3_KEY}")

    session = requests.Session()
    rows = fetch_rows(session, args.checkpoint_file, use_cache=not args.no_cache)
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

    upload_to_s3(parquet_path)


if __name__ == "__main__":
    main()
