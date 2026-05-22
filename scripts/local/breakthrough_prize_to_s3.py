#!/usr/bin/env python3
"""
Breakthrough Prize to S3 (PRIZE PATTERN)
========================================

Breakthrough Prize Foundation publishes laureates as static HTML on
breakthroughprize.org. The category/year listing pages expose detail links,
and each detail page exposes the laureate name, affiliation, year, prize name,
and citation.

This script uses the official site only. It does not backfill from Wikipedia,
Wikidata, or press coverage. Amounts are derived from the official prize pages:

- Breakthrough Prize / Special Breakthrough Prize: USD 3,000,000
- New Horizons prizes: USD 100,000
- Vera Rubin / Maryam Mirzakhani New Frontiers prizes: USD 50,000

Rows are one per (prize x laureate). The listing pages group winners inside
`ul.people` blocks; the script treats each block as one shared prize group and
apportions the official prize amount equally across that block. This is more
faithful than grouping by detail-page citation because some collaboration pages
carry distinct detail text while the official listing still presents them as a
single shared prize.

Output: s3://openalex-ingest/awards/breakthrough_prize/breakthrough_prize_laureates.parquet
Awarding body in OpenAlex: Breakthrough Prize Foundation (F4320315036)
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path
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

BASE_URL = "https://breakthroughprize.org"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/breakthrough_prize/breakthrough_prize_laureates.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.25
RETRIES = 3

CATEGORIES = {
    1: "Fundamental Physics",
    2: "Life Sciences",
    3: "Mathematics",
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def collapse_text(value: str | None) -> str | None:
    if not value:
        return None
    text = re.sub(r"\s+", " ", value).strip()
    return text or None


def slugify(value: str | None) -> str:
    value = (value or "").lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "unknown"


def split_name(name: str | None) -> tuple[str | None, str | None]:
    """Split 'Robert S. Langer' -> ('Robert S.', 'Langer').

    This is the canonical prize-ingest pattern used by Wolf/Kavli/HHMI. It
    strips common suffix tokens and treats the last remaining token as family
    name. Organizational laureates such as "CERN" become (NULL, "CERN").
    """
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
            return response.text
        except Exception as exc:  # noqa: BLE001 - log and retry any transport/status failure.
            last_err = exc
            if attempt < RETRIES:
                sleep_s = 2 ** (attempt - 1)
                log(f"  retrying after {sleep_s}s: {exc}")
                time.sleep(sleep_s)
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


def parse_prize_tabs(soup: BeautifulSoup, category_id: int) -> list[dict[str, str]]:
    tabs: list[dict[str, str]] = []
    seen: set[str] = set()
    for index, anchor in enumerate(soup.select("ul.laureates-prizes a"), start=1):
        label = collapse_text(anchor.get_text(" ", strip=True))
        if not label:
            continue
        href = anchor.get("href")
        if href:
            match = re.search(r"/P(\d+)", href)
            prize_code = f"P{match.group(1)}" if match else f"P{index}"
            path = href
        else:
            prize_code = "P1"
            path = f"/Laureates/{category_id}"
        key = f"{category_id}:{prize_code}"
        if key in seen:
            continue
        seen.add(key)
        tabs.append({
            "category_id": str(category_id),
            "category_name": CATEGORIES[category_id],
            "prize_code": prize_code,
            "prize_tab_label": label,
            "path": path,
        })
    if not tabs:
        tabs.append({
            "category_id": str(category_id),
            "category_name": CATEGORIES[category_id],
            "prize_code": "P1",
            "prize_tab_label": "Breakthrough Prize",
            "path": f"/Laureates/{category_id}",
        })
    return tabs


def parse_year_pages(soup: BeautifulSoup, base_path: str) -> list[dict[str, str]]:
    pages: list[dict[str, str]] = []
    seen: set[str] = set()
    for li in soup.select("ul.pointer li"):
        text = collapse_text(li.get_text(" ", strip=True))
        if not text:
            continue
        year_match = re.search(r"\b(19|20)\d{2}\b", text)
        if not year_match:
            continue
        year = year_match.group(0)
        anchor = li.find("a")
        href = anchor.get("href") if anchor and anchor.get("href") else base_path
        if href in seen:
            continue
        seen.add(href)
        pages.append({"year": year, "path": href})
    return pages


def parse_listing_people(soup: BeautifulSoup) -> list[dict[str, str]]:
    people: list[dict[str, str]] = []
    seen: set[str] = set()
    for group_index, people_group in enumerate(soup.select("ul.people"), start=1):
        items = people_group.select("li")
        group_size = len(items)
        for li in items:
            anchor = li.find("a", href=re.compile(r"^/Laureates/\d+/L\d+"))
            if not anchor:
                continue
            href = anchor.get("href")
            if not href or href in seen:
                continue
            seen.add(href)
            text_anchor = li.select_one("span.text a")
            img = li.find("img")
            name = collapse_text(text_anchor.get_text(" ", strip=True) if text_anchor else None)
            name = name or collapse_text(img.get("alt") if img else None)
            people.append({
                "detail_path": href,
                "listing_name": name or "",
                "listing_group_index": str(group_index),
                "listing_group_size": str(group_size),
            })
    return people


def discover_detail_pages(session: requests.Session) -> list[dict[str, str]]:
    details: dict[str, dict[str, str]] = {}
    for category_id in CATEGORIES:
        root_path = f"/Laureates/{category_id}"
        root_soup = BeautifulSoup(request_html(session, root_path), "html.parser")
        prize_tabs = parse_prize_tabs(root_soup, category_id)
        log(f"Category {category_id} {CATEGORIES[category_id]}: {len(prize_tabs)} prize tabs")
        for tab in prize_tabs:
            tab_soup = root_soup if tab["path"] == root_path else BeautifulSoup(request_html(session, tab["path"]), "html.parser")
            year_pages = parse_year_pages(tab_soup, tab["path"])
            log(f"  {tab['prize_tab_label']} {tab['prize_code']}: {len(year_pages)} year pages")
            for year_page in year_pages:
                page_soup = tab_soup if year_page["path"] == tab["path"] else BeautifulSoup(request_html(session, year_page["path"]), "html.parser")
                people = parse_listing_people(page_soup)
                log(f"    {year_page['year']} {year_page['path']}: {len(people)} detail links")
                for person in people:
                    detail_path = person["detail_path"]
                    details.setdefault(detail_path, {
                        **tab,
                        "listing_year": year_page["year"],
                        "listing_path": year_page["path"],
                        "listing_name": person["listing_name"],
                        "listing_group_index": person["listing_group_index"],
                        "listing_group_size": person["listing_group_size"],
                        "detail_path": detail_path,
                    })
                time.sleep(REQUEST_DELAY)
            time.sleep(REQUEST_DELAY)
    return list(details.values())


def parse_detail_page(html: str, meta: dict[str, str]) -> dict[str, str | None]:
    soup = BeautifulSoup(html, "html.parser")
    text_box = soup.select_one(".laureate .text")
    if not text_box:
        raise RuntimeError(f"Could not find laureate detail block for {meta['detail_path']}")

    name = collapse_text(text_box.select_one("h3").get_text(" ", strip=True) if text_box.select_one("h3") else None)
    affiliation = collapse_text(text_box.select_one("p.affiliation").get_text(" ", strip=True) if text_box.select_one("p.affiliation") else None)
    prize_text = collapse_text(text_box.select_one("h4.prize").get_text(" ", strip=True) if text_box.select_one("h4.prize") else None)
    year = None
    prize_title = None
    if prize_text:
        year_match = re.search(r"\b(19|20)\d{2}\b", prize_text)
        if year_match:
            year = year_match.group(0)
            prize_title = collapse_text(prize_text.replace(year, "", 1))
        else:
            prize_title = prize_text

    citation = None
    for paragraph in text_box.find_all("p", recursive=False):
        classes = paragraph.get("class") or []
        if "affiliation" in classes:
            continue
        candidate = collapse_text(paragraph.get_text(" ", strip=True))
        if candidate:
            citation = candidate
            break

    laureate_id_match = re.search(r"/L(\d+)$", meta["detail_path"])
    laureate_id = laureate_id_match.group(1) if laureate_id_match else meta["detail_path"].strip("/").replace("/", "-")
    given_name, family_name = split_name(name)
    source_url = urljoin(BASE_URL, meta["detail_path"])
    prize_title = prize_title or meta["prize_tab_label"]
    year = year or meta.get("listing_year")
    award_group_key = "|".join([
        meta["category_id"],
        meta["prize_code"],
        meta["listing_year"],
        meta["listing_path"],
        meta["listing_group_index"],
    ])

    return {
        "category_id": meta["category_id"],
        "category_name": meta["category_name"],
        "prize_code": meta["prize_code"],
        "prize_tab_label": meta["prize_tab_label"],
        "listing_year": meta["listing_year"],
        "listing_path": urljoin(BASE_URL, meta["listing_path"]),
        "listing_group_index": meta["listing_group_index"],
        "listing_group_size": meta["listing_group_size"],
        "detail_path": source_url,
        "laureate_id": laureate_id,
        "listing_name": meta.get("listing_name"),
        "laureate_name": name,
        "laureate_given_name": given_name,
        "laureate_family_name": family_name,
        "affiliation": affiliation,
        "award_year": year,
        "prize_title": prize_title,
        "citation": citation,
        "award_group_key": award_group_key,
        # No Breakthrough Prize laureate has declined the award per the official
        # site; kept for schema parity with Fields Medal / Abel Prize and to make
        # the description CASE in the notebook idempotent.
        "declined": False,
        "downloaded_at": datetime.utcnow().isoformat(timespec="seconds"),
    }


def prize_amount_usd(prize_title: str | None) -> int | None:
    title = (prize_title or "").lower()
    if "vera rubin" in title or "maryam mirzakhani" in title or "new frontiers" in title:
        return 50_000
    if "new horizons" in title:
        return 100_000
    if "breakthrough prize" in title:
        return 3_000_000
    return None


def add_award_ids_and_amounts(rows: list[dict[str, str | None]]) -> list[dict[str, str | None]]:
    group_sizes: dict[str, int] = {}
    for row in rows:
        group_key = row["award_group_key"] or row["detail_path"] or ""
        group_sizes[group_key] = group_sizes.get(group_key, 0) + 1

    seen_award_ids: set[str] = set()
    for row in rows:
        full_amount = prize_amount_usd(row.get("prize_title"))
        group_key = row["award_group_key"] or row["detail_path"] or ""
        group_size = group_sizes[group_key]
        amount = (full_amount / group_size) if full_amount is not None and group_size else None
        award_id = "-".join([
            "breakthrough",
            slugify(row.get("prize_title")),
            str(row.get("award_year") or "unknown"),
            str(row.get("laureate_id") or slugify(row.get("laureate_name"))),
        ])
        if award_id in seen_award_ids:
            raise RuntimeError(f"Duplicate funder_award_id would be emitted: {award_id}")
        seen_award_ids.add(award_id)
        row["funder_award_id"] = award_id
        row["source_prize_amount_usd"] = str(full_amount) if full_amount is not None else None
        row["award_group_size"] = str(group_size)
        row["portion"] = f"1/{group_size}" if group_size != 1 else "1"
        row["amount_usd"] = f"{amount:.2f}" if amount is not None else None
        row["currency"] = "USD" if full_amount is not None else None
    return rows


def load_checkpoint(path: Path) -> dict[str, dict[str, str | None]]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    rows = payload.get("rows") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return {}
    return {str(row["detail_path"]): row for row in rows if isinstance(row, dict) and row.get("detail_path")}


def save_checkpoint(path: Path, rows: list[dict[str, str | None]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump({"rows": rows}, fh, indent=2, sort_keys=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Breakthrough Prize -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--checkpoint-file", type=Path, default=Path("/tmp/breakthrough_prize_checkpoint.json"))
    parser.add_argument("--limit-detail-pages", type=int, default=None,
                        help="Smoke-test: fetch only the first N laureate detail pages after listing discovery")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()

    session = requests.Session()
    log("=" * 60)
    log("Breakthrough Prize -> S3 starting")
    log("Discovering official listing/detail pages")
    detail_meta = discover_detail_pages(session)
    if args.limit_detail_pages is not None:
        detail_meta = detail_meta[:args.limit_detail_pages]
        log(f"--limit-detail-pages set; detail fetch limited to {len(detail_meta)} pages")
    if not detail_meta:
        raise RuntimeError("No Breakthrough Prize detail pages discovered; stopping before parquet write.")

    checkpoint = load_checkpoint(args.checkpoint_file)
    rows_by_path: dict[str, dict[str, str | None]] = {}
    for idx, meta in enumerate(detail_meta, start=1):
        detail_url = urljoin(BASE_URL, meta["detail_path"])
        if detail_url in checkpoint:
            rows_by_path[detail_url] = checkpoint[detail_url]
            rows_by_path[detail_url].update({
                "category_id": meta["category_id"],
                "category_name": meta["category_name"],
                "prize_code": meta["prize_code"],
                "prize_tab_label": meta["prize_tab_label"],
                "listing_year": meta["listing_year"],
                "listing_path": urljoin(BASE_URL, meta["listing_path"]),
                "listing_name": meta.get("listing_name"),
                "listing_group_index": meta["listing_group_index"],
                "listing_group_size": meta["listing_group_size"],
                "award_group_key": "|".join([
                    meta["category_id"],
                    meta["prize_code"],
                    meta["listing_year"],
                    meta["listing_path"],
                    meta["listing_group_index"],
                ]),
            })
            log(f"detail {idx}/{len(detail_meta)} cached: {detail_url}")
            continue
        html = request_html(session, meta["detail_path"])
        row = parse_detail_page(html, meta)
        rows_by_path[detail_url] = row
        save_checkpoint(args.checkpoint_file, list(rows_by_path.values()))
        log(f"detail {idx}/{len(detail_meta)} parsed: {row.get('award_year')} {row.get('prize_title')} - {row.get('laureate_name')}")
        time.sleep(REQUEST_DELAY)

    rows = add_award_ids_and_amounts(list(rows_by_path.values()))
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No Breakthrough Prize rows parsed; stopping before parquet write.")
    log(f"DataFrame shape: {df.shape}")
    log(
        "Coverage: "
        f"name={df.laureate_name.notna().sum()}, "
        f"year={df.award_year.notna().sum()}, "
        f"prize={df.prize_title.notna().sum()}, "
        f"citation={df.citation.notna().sum()}, "
        f"affiliation={df.affiliation.notna().sum()}, "
        f"amount={df.amount_usd.notna().sum()}"
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "breakthrough_prize_laureates.parquet"
    df = df.astype("string")
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
