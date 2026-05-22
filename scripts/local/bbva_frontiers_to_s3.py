#!/usr/bin/env python3
"""
BBVA Foundation Frontiers of Knowledge Awards to S3 (PRIZE PATTERN)
===================================================================

Fetches BBVA Foundation Frontiers of Knowledge Award laureates from the
award body's official English laureates page and official detail pages. This
follows the awards runbook's prize-pattern source-authority rule: prize rows
come from the awarding body directly, not from Wikipedia, Wikidata, news
articles, or third-party biographies.

Official source pages:
  - https://www.frontiersofknowledgeawards-fbbva.es/laureates/
  - https://www.frontiersofknowledgeawards-fbbva.es/conditions/

Output:
  s3://openalex-ingest/awards/bbva_frontiers/bbva_frontiers_laureates.parquet

Awarding body in OpenAlex:
  Fundación BBVA (F4320309764, DOI 10.13039/100007406)

Amount rule:
  The official conditions page states that each category consists of 400,000
  euros, a diploma, and a commemorative artwork, and that the monetary amount
  is divided when an award is shared. This script stores one row per official
  laureate card and divides EUR 400,000 by the number of laureates in the same
  edition year and category.

Organization laureates:
  A small number of official laureates are organizations. The awards schema has
  person-shaped lead_investigator fields, so the script preserves the full
  official laureate name and emits best-effort given/family fields only when
  the name looks person-like. Organization-like names keep given_name NULL and
  family_name equal to the full official laureate name.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

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

LAUREATES_URL = "https://www.frontiersofknowledgeawards-fbbva.es/laureates/"
CONDITIONS_URL = "https://www.frontiersofknowledgeawards-fbbva.es/conditions/"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/bbva_frontiers/bbva_frontiers_laureates.parquet"
OUTPUT_FILE = "bbva_frontiers_laureates.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.15
RETRIES = 3

AWARD_TOTAL_AMOUNT = 400000.0
CURRENCY = "EUR"

ORG_HINTS = {
    "academy",
    "agency",
    "alliance",
    "association",
    "center",
    "centre",
    "council",
    "development",
    "foundation",
    "fund",
    "initiative",
    "institute",
    "international",
    "laboratory",
    "lab",
    "network",
    "organization",
    "programme",
    "program",
    "project",
    "university",
}


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


def decode_html(response: requests.Response) -> str:
    return response.content.decode("utf-8", errors="replace")


def slugify(value: str | None) -> str:
    value = (value or "").lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "unknown"


def is_org_like(name: str | None) -> bool:
    if not name:
        return False
    lower = name.lower()
    if lower.startswith(("the ", "un ", "una ", "el ", "la ")):
        return True
    if any(char in name for char in ["&", "/", "("]):
        return True
    tokens = set(re.findall(r"[a-z]+", lower))
    return bool(tokens & ORG_HINTS)


def split_name(name: str | None) -> tuple[str | None, str | None, str]:
    if not name:
        return None, None, "unknown"
    if is_org_like(name):
        return None, name, "organization_or_group"
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, name, "unknown"
    if len(tokens) == 1:
        return None, tokens[0], "person"
    return " ".join(tokens[:-1]), tokens[-1], "person"


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
            return decode_html(response)
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
        json.dump({"pages": pages}, handle, indent=2, sort_keys=True, ensure_ascii=False)


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


def verify_amount_rule(conditions_html: str) -> None:
    text = BeautifulSoup(conditions_html, "html.parser").get_text(" ", strip=True)
    text = collapse_text(text) or ""
    checks = [
        "400,000 euros" in text,
        "divided" in text.lower(),
        "shared by more than one person" in text.lower() or "award is shared" in text.lower(),
    ]
    if not all(checks):
        raise RuntimeError(
            "Could not verify the BBVA Frontiers amount rule on "
            f"{CONDITIONS_URL}. Expected 400,000 euros and shared-award division language."
        )


def profile_slug(url: str | None) -> str:
    if not url:
        return "unknown"
    path_parts = [part for part in urlparse(url).path.split("/") if part]
    return slugify(path_parts[-1] if path_parts else url)


def parse_edition_label(label: str | None) -> tuple[str | None, str | None]:
    label = collapse_text(label)
    if not label:
        return None, None
    year_match = re.search(r"\((\d{4})\)", label)
    edition_match = re.search(r"^([^()]+)", label)
    return (
        year_match.group(1) if year_match else None,
        collapse_text(edition_match.group(1)) if edition_match else label,
    )


def parse_laureates_index(index_html: str) -> list[dict[str, str | None]]:
    soup = BeautifulSoup(index_html, "html.parser")
    rows: list[dict[str, str | None]] = []
    tabs = soup.select('div.tab[id^="edition_"]')
    log(f"Parsed {len(tabs)} edition tabs from official laureates page")
    for tab in tabs:
        if not isinstance(tab, Tag):
            continue
        year_from_id = (tab.get("id") or "").replace("edition_", "")
        header = tab.select_one("h5")
        label = collapse_text(header.get_text(" ", strip=True)) if header else None
        label_year, edition_label = parse_edition_label(label)
        award_year = label_year or year_from_id
        cards = tab.select(".awarded-grid a")
        log(f"Edition {award_year}: {len(cards)} laureate cards")
        for position, anchor in enumerate(cards, start=1):
            href = anchor.get("href")
            name_el = anchor.select_one(".name")
            theme_el = anchor.select_one(".theme")
            laureate_name = collapse_text(name_el.get_text(" ", strip=True) if name_el else None)
            category = collapse_text(theme_el.get_text(" ", strip=True) if theme_el else None)
            if not href or not laureate_name or not category:
                raise RuntimeError(f"Incomplete laureate card in edition {award_year}: {anchor}")
            given_name, family_name, laureate_type = split_name(laureate_name)
            slug = profile_slug(href)
            rows.append(
                {
                    "funder_award_id": f"bbva-frontiers-{award_year}-{slug}",
                    "award_year": award_year,
                    "edition_label": edition_label,
                    "winner_position": str(position),
                    "laureate_name": laureate_name,
                    "laureate_type": laureate_type,
                    "given_name": given_name,
                    "family_name": family_name,
                    "award_category": category,
                    "landing_page_url": href,
                    "profile_slug": slug,
                    "source_url": LAUREATES_URL,
                    "amount_rule_url": CONDITIONS_URL,
                    "fetched_at": utc_now(),
                }
            )
    return rows


def parse_profile(profile_html: str) -> dict[str, str | None]:
    soup = BeautifulSoup(profile_html, "html.parser")
    main = soup.find("main") or soup
    name = collapse_text(main.find("h1").get_text(" ", strip=True)) if main.find("h1") else None

    profile_category: str | None = None
    profile_edition: str | None = None
    award_summary: str | None = None
    bio_parts: list[str] = []
    contribution_parts: list[str] = []
    mode: str | None = None

    for element in main.find_all(["h1", "h2", "h3", "p"]):
        text = collapse_text(element.get_text(" ", strip=True))
        if not text:
            continue
        upper = text.upper()
        if upper in {"NOMINEES", "DIGITAL ARCHIPELAGO"}:
            continue
        if upper == "BIO":
            mode = "bio"
            continue
        if upper == "CONTRIBUTION":
            mode = "contribution"
            continue
        if upper in {"INTERVIEW", "MORE"}:
            mode = None
            continue
        if text.startswith("With the collaboration of") or text.startswith("Edificio San"):
            break
        if element.name == "h1":
            continue
        if element.name == "h2" and "FRONTERAS DEL CONOCIMIENTO" in upper:
            continue
        if profile_category is None and element.name == "p" and text in {
            "Basic Sciences",
            "Biology and Biomedicine",
            "Information and Communication Technologies",
            "Climate Change and Environmental Sciences",
            "Economics, Finance and Management",
            "Social Sciences",
            "Humanities",
            "Music and Opera",
            "Development Cooperation",
            "Arts",
        }:
            profile_category = text
            continue
        if profile_edition is None and element.name == "p" and re.match(r"\d+(st|nd|rd|th) Edition$", text):
            profile_edition = text
            continue
        if award_summary is None and element.name == "p" and "Frontiers of Knowledge Award" in text:
            award_summary = text
            continue
        if mode == "bio" and element.name == "p":
            bio_parts.append(text)
        elif mode == "contribution" and element.name == "p":
            contribution_parts.append(text)

    return {
        "profile_name": name,
        "profile_category": profile_category,
        "profile_edition": profile_edition,
        "award_summary": award_summary,
        "bio": collapse_text(" ".join(bio_parts)),
        "contribution": collapse_text(" ".join(contribution_parts)),
    }


def add_amounts(rows: list[dict[str, str | None]]) -> list[dict[str, str | None]]:
    share_counts = Counter((row["award_year"], row["award_category"]) for row in rows)
    for row in rows:
        key = (row["award_year"], row["award_category"])
        share_count = share_counts[key]
        amount = AWARD_TOTAL_AMOUNT / share_count
        row["award_share_count"] = str(share_count)
        row["portion"] = f"1/{share_count}" if share_count > 1 else "1"
        row["award_total_amount"] = str(int(AWARD_TOTAL_AMOUNT))
        row["source_award_amount"] = f"{amount:.6f}".rstrip("0").rstrip(".")
        row["currency"] = CURRENCY
        row["amount_note"] = "EUR 400,000 category amount from official conditions page, divided by laureate count for edition/category."
    return rows


def fetch_rows(max_profiles: int | None, checkpoint_file: Path, use_cache: bool) -> list[dict[str, str | None]]:
    session = requests.Session()
    conditions_html = get_page(session, CONDITIONS_URL, checkpoint_file, use_cache)
    verify_amount_rule(conditions_html)

    index_html = get_page(session, LAUREATES_URL, checkpoint_file, use_cache)
    rows = parse_laureates_index(index_html)
    rows = add_amounts(rows)
    if not rows:
        raise RuntimeError(f"No laureate cards found on {LAUREATES_URL}")

    total = len(rows if max_profiles is None else rows[:max_profiles])
    for idx, row in enumerate(rows):
        if max_profiles is not None and idx >= max_profiles:
            break
        url = row["landing_page_url"]
        if not url:
            continue
        profile_html = get_page(session, url, checkpoint_file, use_cache)
        row.update(parse_profile(profile_html))
        if (idx + 1) % 25 == 0 or (idx + 1) == total:
            log(f"Fetched profile details for {idx + 1}/{total} laureates")

    return rows


def validate_rows(rows: list[dict[str, str | None]]) -> None:
    if not rows:
        raise RuntimeError("No rows parsed from BBVA Frontiers source pages")
    award_ids = [row["funder_award_id"] for row in rows]
    duplicates = sorted({award_id for award_id in award_ids if award_ids.count(award_id) > 1})
    if duplicates:
        raise RuntimeError(f"Duplicate funder_award_id values detected: {duplicates[:10]}")
    required = ["award_year", "laureate_name", "award_category", "landing_page_url", "source_award_amount", "currency"]
    for column in required:
        missing = [row["funder_award_id"] for row in rows if not row.get(column)]
        if missing:
            raise RuntimeError(f"Missing required source field {column} on {len(missing)} rows: {missing[:10]}")


def write_parquet(rows: list[dict[str, str | None]], output_path: Path) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df = df.sort_values(["award_year", "award_category", "winner_position"], ascending=[False, True, True])
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
    prev_path = output_dir / "_prev_bbva_frontiers_laureates.parquet"
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
    parser = argparse.ArgumentParser(description="Fetch BBVA Frontiers laureates and write parquet for OpenAlex awards ingest.")
    parser.add_argument("--output-dir", default="/tmp/openalex-awards/bbva_frontiers", help="Directory for parquet output")
    parser.add_argument("--checkpoint", default="/tmp/openalex-awards/bbva_frontiers/checkpoint.json", help="HTML checkpoint path")
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
        f"{df['award_category'].notna().sum()}/{len(df)} categories, "
        f"{df['award_summary'].notna().sum()}/{len(df)} summaries, "
        f"{df['bio'].notna().sum()}/{len(df)} bios, "
        f"{df['contribution'].notna().sum()}/{len(df)} contributions, "
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
