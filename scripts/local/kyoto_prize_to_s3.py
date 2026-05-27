#!/usr/bin/env python3
"""
Kyoto Prize to S3 (PRIZE PATTERN)
=================================

Fetches Kyoto Prize laureates from the official Kyoto Prize website published
by the Inamori Foundation. This follows the awards runbook's prize-pattern
source-authority rule: prize rows come from the awarding body directly, not
from Wikipedia, Wikidata, or third-party biographies.

Official source pages:
  - https://www.kyotoprize.org/en/laureates/
  - https://www.kyotoprize.org/en/laureates/{laureate_slug}/
  - https://www.kyotoprize.org/en/about/             (current amount rule)
  - https://www.kyotoprize.org/wp-content/uploads/2019/08/rita_everlasting_en.pdf
                                                        (2018 increase statement)

Output:
  s3://openalex-ingest/awards/kyoto_prize/kyoto_prize_laureates.parquet

Awarding body in OpenAlex:
  Inamori Foundation (F4320322210)

Amount note:
  The official about page states the current prize money is 100 million yen
  per category. The official 2018 "Rita Everlasting" statement says the
  monetary value was increased to 100 million yen per category on April 12,
  2018, so this script populates apportioned JPY amounts for 2018-present rows
  and leaves pre-2018 rows NULL rather than guessing older historical rules.

Parsing notes:
  The laureates index renders server-side and exposes all laureate cards.
  Detail pages expose the official citation, achievement digest, role, field,
  and affiliation text where available.
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
from urllib.parse import urljoin, urlparse

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

BASE_URL = "https://www.kyotoprize.org/"
LAUREATES_URL = urljoin(BASE_URL, "en/laureates/")
AMOUNT_RULE_URL = urljoin(BASE_URL, "en/about/")
AMOUNT_HISTORY_URL = "https://www.kyotoprize.org/wp-content/uploads/2019/08/rita_everlasting_en.pdf"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/kyoto_prize/kyoto_prize_laureates.parquet"
OUTPUT_FILE = "kyoto_prize_laureates.parquet"

FUNDER_ID = "4320322210"
FUNDER_DISPLAY_NAME = "Inamori Foundation"
PRIZE_NAME = "Kyoto Prize"
PROVENANCE = "kyoto_prize"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.15
RETRIES = 3

CURRENT_CATEGORY_AMOUNT = 100000000.0
CURRENT_AMOUNT_START_YEAR = 2018
CURRENCY = "JPY"

CATEGORY_BY_CLASS = {
    "laureate--green": "Advanced Technology",
    "laureate--purple": "Basic Sciences",
    "laureate--red": "Arts and Philosophy",
}

ORG_HINTS = {
    "academy",
    "association",
    "center",
    "centre",
    "foundation",
    "institute",
    "institution",
    "laboratory",
    "organisation",
    "organization",
    "society",
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
    value = value.replace("\u3000", " ")
    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(r"\s+([,;.:])", r"\1", value)
    return value or None


def slugify(value: str | None) -> str:
    value = (value or "").lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "unknown"


def looks_like_org(name: str | None) -> bool:
    if not name:
        return False
    lowered = name.lower()
    return any(hint in lowered for hint in ORG_HINTS)


def split_name(name: str | None) -> tuple[str | None, str | None]:
    """Simple prize-ingest name splitter; organization names stay intact."""
    if not name:
        return None, None
    if looks_like_org(name):
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
            if not response.content:
                raise RuntimeError(f"Empty response from {url}")
            return response.text
        except Exception as exc:  # noqa: BLE001 - retry transport/status failures.
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


def normalize_laureate_url(href: str | None) -> str | None:
    if not href:
        return None
    url = urljoin(LAUREATES_URL, href)
    # Some old links on the official page contain /en/en/. The canonical detail
    # URL is /en/laureates/{slug}/.
    url = url.replace("/en/en/laureates/", "/en/laureates/")
    return url


def category_from_card(card: Tag) -> str | None:
    classes = set(card.get("class") or [])
    for class_name, category in CATEGORY_BY_CLASS.items():
        if class_name in classes:
            return category
    return None


def parse_index_cards(index_html: str) -> list[dict[str, str | None]]:
    soup = BeautifulSoup(index_html, "html.parser")
    cards = soup.select("ul.laureates li.laureate")
    log(f"Parsed {len(cards)} laureate cards from official index")
    rows: list[dict[str, str | None]] = []
    for position, card in enumerate(cards, start=1):
        if not isinstance(card, Tag):
            continue
        name_el = card.select_one(".laureate__name")
        center_el = card.select_one(".laureate__text__center")
        field_el = card.select_one(".laureate__department")
        anchor = card.find("a", href=True)

        name = collapse_text(name_el.get_text(" ", strip=True) if name_el else None)
        center_text = collapse_text(center_el.get_text(" ", strip=True) if center_el else None)
        field = collapse_text(field_el.get_text(" ", strip=True).strip("[]") if field_el else None)
        year_match = re.search(r"(?:19|20)\d{2}", center_text or "")
        if not name or not year_match:
            raise RuntimeError(f"Could not parse laureate card {position}: name={name!r}, center={center_text!r}")
        year = year_match.group(0)
        role = collapse_text((center_text or "").replace(year, "", 1))
        landing_page_url = normalize_laureate_url(anchor.get("href") if anchor else None)
        if not landing_page_url:
            raise RuntimeError(f"Missing detail URL for laureate card {position}: {name}")

        rows.append(
            {
                "source_url": LAUREATES_URL,
                "landing_page_url": landing_page_url,
                "award_year": year,
                "prize_name": PRIZE_NAME,
                "prize_category": category_from_card(card),
                "prize_field": field,
                "laureate_name": name,
                "laureate_role": role,
                "index_position": str(position),
            }
        )
    if not rows:
        raise RuntimeError(f"No Kyoto Prize laureate cards parsed from {LAUREATES_URL}")
    return rows


def section_text(soup: BeautifulSoup, section_id: str, selector: str = ".widziwig") -> str | None:
    section = soup.select_one(f"#{section_id}")
    if not section:
        return None
    target = section.select_one(selector) or section
    return collapse_text(target.get_text(" ", strip=True))


def parse_detail_page(html: str, url: str) -> dict[str, str | None]:
    soup = BeautifulSoup(html, "html.parser")
    name = collapse_text(soup.select_one(".human__name").get_text(" ", strip=True) if soup.select_one(".human__name") else None)
    role = collapse_text(soup.select_one(".human__job").get_text(" ", strip=True) if soup.select_one(".human__job") else None)
    if role:
        role = collapse_text(role.replace("/", " "))
    year_text = collapse_text(soup.select_one(".human__year").get_text(" ", strip=True) if soup.select_one(".human__year") else None)
    year_match = re.search(r"(?:19|20)\d{2}", year_text or "")
    category = collapse_text(soup.select_one(".department").get_text(" ", strip=True) if soup.select_one(".department") else None)
    field = collapse_text(soup.select_one(".field").get_text(" ", strip=True) if soup.select_one(".field") else None)
    lifespan = collapse_text(soup.select_one(".human__birthday").get_text(" ", strip=True) if soup.select_one(".human__birthday") else None)
    affiliation = collapse_text(soup.select_one(".human__belongs").get_text(" ", strip=True) if soup.select_one(".human__belongs") else None)
    achievement_title = collapse_text(soup.select_one("#section-introduction .mini-title").get_text(" ", strip=True) if soup.select_one("#section-introduction .mini-title") else None)
    achievement_digest = section_text(soup, "section-introduction")
    citation = section_text(soup, "section-reason")
    biography = section_text(soup, "section-profile")
    meta_desc = soup.find("meta", attrs={"name": "description"})
    profile_description = collapse_text(meta_desc.get("content") if meta_desc and meta_desc.get("content") else None)
    image = soup.select_one(".human__image img")
    image_url = urljoin(url, image.get("src")) if image and image.get("src") else None

    return {
        "detail_award_year": year_match.group(0) if year_match else None,
        "detail_prize_category": category,
        "detail_prize_field": field,
        "detail_laureate_name": name,
        "detail_laureate_role": role,
        "lifespan": lifespan,
        "affiliation": affiliation,
        "achievement_title": achievement_title,
        "achievement_digest": achievement_digest,
        "citation": citation,
        "biography": biography,
        "profile_description": profile_description,
        "image_url": image_url,
    }


def verify_amount_rule(about_html: str) -> None:
    text = BeautifulSoup(about_html, "html.parser").get_text(" ", strip=True)
    normalized = text.lower().replace(",", "")
    if "prize money" not in normalized or "100 million yen" not in normalized:
        raise RuntimeError(
            "Could not verify current Kyoto Prize amount rule on "
            f"{AMOUNT_RULE_URL}. Expected 'prize money' and '100 million yen'."
        )


def apply_amounts(rows: list[dict[str, str | None]]) -> None:
    counts = Counter((row["award_year"], row["prize_field"]) for row in rows)
    for row in rows:
        year = int(row["award_year"] or 0)
        count = counts[(row["award_year"], row["prize_field"])]
        portion = 1.0 / count if count else 1.0
        row["laureate_count_for_year_field"] = str(count)
        row["portion"] = f"{portion:.12g}"
        row["category_award_amount"] = f"{CURRENT_CATEGORY_AMOUNT:.0f}" if year >= CURRENT_AMOUNT_START_YEAR else None
        row["source_award_amount"] = f"{CURRENT_CATEGORY_AMOUNT * portion:.12g}" if year >= CURRENT_AMOUNT_START_YEAR else None
        row["currency"] = CURRENCY if year >= CURRENT_AMOUNT_START_YEAR else None
        row["amount_rule_url"] = AMOUNT_RULE_URL
        row["amount_rule_note"] = (
            f"Official current amount is {CURRENT_CATEGORY_AMOUNT:.0f} {CURRENCY} per category; "
            f"{AMOUNT_HISTORY_URL} states the increase to this amount was decided on April 12, 2018; "
            f"populated for {CURRENT_AMOUNT_START_YEAR}-present and apportioned by year/field laureate count. "
            "Pre-2018 amount/currency left NULL rather than guessing older historical rules."
        )


def build_rows(session: requests.Session, checkpoint: Path, use_cache: bool, max_profiles: int | None) -> list[dict[str, str | None]]:
    amount_html = get_page(session, AMOUNT_RULE_URL, checkpoint, use_cache)
    verify_amount_rule(amount_html)
    index_html = get_page(session, LAUREATES_URL, checkpoint, use_cache)
    rows = parse_index_cards(index_html)
    if max_profiles is not None:
        rows = rows[:max_profiles]
        log(f"Applying max profile limit: {len(rows)} rows")

    fetched_at = utc_now()
    for idx, row in enumerate(rows, start=1):
        url = row["landing_page_url"]
        if not url:
            raise RuntimeError(f"Missing landing_page_url for row {idx}: {row}")
        detail_html = get_page(session, url, checkpoint, use_cache)
        detail = parse_detail_page(detail_html, url)

        detail_name = detail.get("detail_laureate_name")
        if detail_name and detail_name != row["laureate_name"]:
            log(f"  detail name differs for {url}: index={row['laureate_name']!r}, detail={detail_name!r}")
        if detail.get("detail_award_year") and detail["detail_award_year"] != row["award_year"]:
            raise RuntimeError(
                f"Detail year mismatch for {url}: index={row['award_year']}, detail={detail['detail_award_year']}"
            )

        row["detail_source_url"] = url
        row["prize_category"] = detail.get("detail_prize_category") or row.get("prize_category")
        row["prize_field"] = detail.get("detail_prize_field") or row.get("prize_field")
        row["laureate_role"] = detail.get("detail_laureate_role") or row.get("laureate_role")
        row["affiliation"] = detail.get("affiliation")
        row["lifespan"] = detail.get("lifespan")
        row["achievement_title"] = detail.get("achievement_title")
        row["achievement_digest"] = detail.get("achievement_digest")
        row["citation"] = detail.get("citation")
        row["biography"] = detail.get("biography")
        row["profile_description"] = detail.get("profile_description")
        row["image_url"] = detail.get("image_url")

        given_name, family_name = split_name(row["laureate_name"])
        row["given_name"] = given_name
        row["family_name"] = family_name
        row["is_organization_laureate"] = "true" if looks_like_org(row["laureate_name"]) else "false"
        row["funder_award_id"] = "-".join(
            [
                "kyoto-prize",
                row["award_year"] or "unknown-year",
                slugify(row.get("prize_field")),
                slugify(row.get("laureate_name")),
            ]
        )
        row["source_fetched_at"] = fetched_at
        row["funder_id"] = FUNDER_ID
        row["funder_display_name"] = FUNDER_DISPLAY_NAME
        row["provenance"] = PROVENANCE

        if idx % 25 == 0 or idx == len(rows):
            log(f"Fetched profile details for {idx}/{len(rows)} laureates")

    ids = [row["funder_award_id"] for row in rows]
    duplicates = [award_id for award_id, count in Counter(ids).items() if count > 1]
    if duplicates:
        raise RuntimeError(f"Duplicate funder_award_id values: {duplicates[:10]}")

    apply_amounts(rows)
    return rows


def write_parquet(rows: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / OUTPUT_FILE
    df = pd.DataFrame(rows)
    df = df.astype("string")
    df.to_parquet(output_path, index=False)
    log(f"Wrote {len(df)} rows to {output_path}")
    log(
        "Coverage: "
        f"{df['laureate_name'].notna().sum()}/{len(df)} names, "
        f"{df['award_year'].notna().sum()}/{len(df)} years, "
        f"{df['prize_category'].notna().sum()}/{len(df)} categories, "
        f"{df['prize_field'].notna().sum()}/{len(df)} fields, "
        f"{df['affiliation'].notna().sum()}/{len(df)} affiliations, "
        f"{df['citation'].notna().sum()}/{len(df)} citations, "
        f"{df['source_award_amount'].notna().sum()}/{len(df)} amounts, "
        f"{df['currency'].notna().sum()}/{len(df)} currencies"
    )
    return output_path


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
    prev_path = output_dir / "_prev_kyoto_prize_laureates.parquet"
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


def upload_to_s3(path: Path) -> None:
    import boto3  # type: ignore[import-not-found]

    client = boto3.client("s3")
    client.upload_file(str(path), S3_BUCKET, S3_KEY)
    log(f"Uploaded {path} to s3://{S3_BUCKET}/{S3_KEY}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Kyoto Prize laureates and write parquet for OpenAlex awards ingest.")
    parser.add_argument("--output-dir", default="/tmp/openalex-awards/kyoto_prize", help="Directory for parquet output")
    parser.add_argument("--checkpoint", default="/tmp/openalex-awards/kyoto_prize/checkpoint.json", help="HTML checkpoint path")
    parser.add_argument("--no-cache", action="store_true", help="Ignore checkpointed HTML and refetch all pages")
    parser.add_argument("--skip-upload", action="store_true", help="Write local parquet but do not upload to S3")
    parser.add_argument(
        "--allow-shrink",
        action="store_true",
        help="Override the runbook §1.4 shrink-check. Only use after confirming a smaller corpus is intentional.",
    )
    parser.add_argument("--max-profiles", type=int, default=None, help="Optional smoke-test limit on detail pages")
    args = parser.parse_args()

    session = requests.Session()
    rows = build_rows(session, Path(args.checkpoint), use_cache=not args.no_cache, max_profiles=args.max_profiles)
    output_path = write_parquet(rows, Path(args.output_dir))
    if args.skip_upload:
        log("Skipping S3 upload by request")
    else:
        if not check_no_shrink(len(rows), args.allow_shrink, Path(args.output_dir)):
            raise SystemExit("§1.4 shrink-check failed. See above; re-run with --allow-shrink if intentional.")
        upload_to_s3(output_path)


if __name__ == "__main__":
    main()
