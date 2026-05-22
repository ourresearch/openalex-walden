#!/usr/bin/env python3
"""
Balzan Prize to S3 (PRIZE PATTERN)
==================================

Fetches Balzan Prize winners from the awarding body's official prizewinner
archive. This follows the awards runbook's prize-pattern source-authority rule:
rows come from balzan.org directly, not from Wikipedia, Wikidata, or third-party
biographies.

Official source pages:
  - https://www.balzan.org/en/prizewinners
  - https://www.balzan.org/en/prizewinners?quinquennio={period}
  - Per-prizewinner detail pages linked from the archive

Output:
  s3://openalex-ingest/awards/balzan_prize/balzan_prize_laureates.parquet

Awarding body in OpenAlex:
  Fondazione Internazionale Premio Balzan (F4320310930, ROR
  https://ror.org/01xc6az48, DOI 10.13039/100008995)

Amount rule:
  Balzan is a monetary prize, but the official archive does not expose a stable
  historical per-prizewinner amount in the five-year archive or detail pages.
  This script records source_award_amount as NULL and currency as CHF with an
  explicit amount_note so the Databricks notebook can apply the prize-pattern
  amount waiver rather than fabricating historical values.

Parsing notes:
  The archive is server-rendered. Each selected `quinquennio` period lists up
  to five years of prizewinner cards. Some cards contain shared awards like
  "A and B" or "A, B, C"; these are split into one row per laureate, with
  winner_count and portion preserved in the raw parquet. Organization winners
  such as The Nobel Foundation are kept as a single laureate row.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

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

BASE_URL = "https://www.balzan.org"
ARCHIVE_URL = f"{BASE_URL}/en/prizewinners"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/balzan_prize/balzan_prize_laureates.parquet"
OUTPUT_FILE = "balzan_prize_laureates.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.25
RETRIES = 3

PERIODS = [
    "2025",
    "2020",
    "2015",
    "2010",
    "2005",
    "2000",
    "1995",
    "1990",
    "1985",
    "1980",
    "1975",
    "1970",
    "1965",
    "1960",
]

AMOUNT_NOTE = (
    "Official Balzan archive does not expose stable historical per-prizewinner "
    "amounts in the archive/detail pages; amount left NULL for prize-pattern "
    "waiver."
)
CURRENCY = "CHF"


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


def archive_url_for_period(period: str) -> str:
    return f"{ARCHIVE_URL}?quinquennio={period}"


def parse_award_field(award_title: str | None, year: str | None) -> str | None:
    if not award_title:
        return None
    field = award_title
    if year:
        field = re.sub(rf"^{re.escape(year)}\s+", "", field)
    field = re.sub(r"^Balzan Prize for\s+", "", field, flags=re.IGNORECASE)
    return collapse_text(field)


def is_organization_winner(name: str) -> bool:
    lowered = name.lower()
    org_terms = [
        "foundation",
        "agency",
        "committee",
        "organization",
        "organisation",
        "nations",
        "refugee",
        "red cross",
        "research group",
        "center",
        "centre",
        "john xxiii",
        "mother teresa",
    ]
    return any(term in lowered for term in org_terms)


def normalize_person_name(name: str) -> str:
    """Clean compact initial strings and surrounding whitespace."""
    name = collapse_text(name) or ""
    # Source has historical strings like D.H.Matthews and F.J.Vine.
    name = re.sub(r"((?:[A-Z]\.){2,})(?=[A-Z][a-z])", r"\1 ", name)
    return collapse_text(name) or ""


def split_laureates(name: str) -> list[str]:
    """Split obvious shared-winner strings without splitting organization names."""
    clean = collapse_text(name) or ""
    if not clean:
        return []
    if is_organization_winner(clean):
        return [clean]

    # Pattern: "Peter and Rosemary Grant" means Peter Grant + Rosemary Grant.
    # Likewise "Aleida and Jan Assmann". If both names had full surnames, the
    # left side would contain a space already and the generic split below works.
    inherited_surname = re.match(r"^([A-Z][A-Za-zÀ-ÖØ-öø-ÿ'’-]+) and ([A-Z][A-Za-zÀ-ÖØ-öø-ÿ'’-]+) ([A-Z][A-Za-zÀ-ÖØ-öø-ÿ'’-]+)$", clean)
    if inherited_surname:
        first_given, second_given, surname = inherited_surname.groups()
        return [f"{first_given} {surname}", f"{second_given} {surname}"]

    normalized = re.sub(r"\s+and\s+", ", ", clean)
    parts = [normalize_person_name(part) for part in normalized.split(",")]
    parts = [part for part in parts if part]
    # Avoid over-splitting initials-only punctuation accidents.
    if len(parts) > 1 and all(len(part) >= 2 for part in parts):
        return parts
    return [clean]


def first_nonempty_link(box: Tag) -> str | None:
    for anchor in box.find_all("a", href=True):
        text = collapse_text(anchor.get_text(" ", strip=True)) or ""
        href = anchor["href"]
        if text.lower() == "project":
            continue
        if href:
            return urljoin(BASE_URL, href)
    return None


def project_link(box: Tag) -> str | None:
    for anchor in box.find_all("a", href=True):
        text = (collapse_text(anchor.get_text(" ", strip=True)) or "").lower()
        if text == "project":
            return urljoin(BASE_URL, anchor["href"])
    return None


def parse_detail_page(html: str) -> dict[str, str | None]:
    soup = BeautifulSoup(html, "html.parser")
    motivation = soup.select_one(".motivazione")
    return {
        "detail_title": collapse_text(soup.title.get_text(" ", strip=True) if soup.title else None),
        "citation": collapse_text(motivation.get_text(" ", strip=True) if motivation else None),
    }


def parse_archive_page(period: str, html: str) -> list[dict[str, str | None]]:
    source_url = archive_url_for_period(period)
    soup = BeautifulSoup(html, "html.parser")
    boxes = soup.select(".box.archivio")
    rows: list[dict[str, str | None]] = []

    for box in boxes:
        year = collapse_text(box.select_one(".anno").get_text(" ", strip=True) if box.select_one(".anno") else None)
        country = collapse_text(box.select_one(".nazione").get_text(" ", strip=True) if box.select_one(".nazione") else None)
        name = collapse_text(box.select_one("h3").get_text(" ", strip=True) if box.select_one("h3") else None)
        award_title = collapse_text(box.select_one(".materia").get_text(" ", strip=True) if box.select_one(".materia") else None)
        detail_url = first_nonempty_link(box)
        research_project_url = project_link(box)

        if not year or not name or not award_title:
            log(f"  [WARN] Skipping incomplete archive card on {source_url}: {box.get_text(' ', strip=True)[:200]}")
            continue

        laureates = split_laureates(name)
        winner_count = len(laureates)
        for idx, laureate_name in enumerate(laureates, start=1):
            given, family = split_name(laureate_name)
            award_field = parse_award_field(award_title, year)
            funder_award_id = f"balzan:{year}:{slugify(award_field)}:{slugify(laureate_name)}"
            rows.append(
                {
                    "funder_award_id": funder_award_id,
                    "award_year": year,
                    "award_title": award_title,
                    "award_field": award_field,
                    "laureate_name": laureate_name,
                    "laureate_given_name": given,
                    "laureate_family_name": family,
                    "source_laureate_text": name,
                    "winner_count": str(winner_count),
                    "winner_index": str(idx),
                    "portion": str(1 / winner_count),
                    "source_country": country,
                    "landing_page_url": detail_url or source_url,
                    "detail_page_url": detail_url,
                    "research_project_url": research_project_url,
                    "source_url": source_url,
                    "source_award_amount": None,
                    "currency": CURRENCY,
                    "amount_note": AMOUNT_NOTE,
                    "source_type": "balzan_prize_archive",
                    "fetched_at": utc_now(),
                }
            )

    log(f"parsed {len(rows)} laureate rows from {source_url} ({len(boxes)} cards)")
    return rows


def fetch_rows(output_dir: Path, use_cache: bool) -> list[dict[str, str | None]]:
    checkpoint_file = output_dir / "balzan_prize_checkpoint.json"
    session = requests.Session()
    rows: list[dict[str, str | None]] = []

    for period in PERIODS:
        url = archive_url_for_period(period)
        html = get_page(session, url, checkpoint_file, use_cache)
        rows.extend(parse_archive_page(period, html))

    detail_urls = sorted({row["detail_page_url"] for row in rows if row.get("detail_page_url")})
    log(f"fetching {len(detail_urls)} unique detail pages")
    details: dict[str, dict[str, str | None]] = {}
    for index, url in enumerate(detail_urls, start=1):
        log(f"detail {index}/{len(detail_urls)}")
        html = get_page(session, str(url), checkpoint_file, use_cache)
        details[str(url)] = parse_detail_page(html)

    for row in rows:
        detail_url = row.get("detail_page_url")
        detail = details.get(str(detail_url), {})
        row.update(detail)

    return rows


def validate_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise RuntimeError("No Balzan Prize rows parsed")

    ids = [row.get("funder_award_id") for row in rows]
    duplicates = sorted({value for value in ids if ids.count(value) > 1})
    if duplicates:
        raise RuntimeError(f"Duplicate funder_award_id values would merge rows: {duplicates[:20]}")

    missing_core = [
        row
        for row in rows
        if not row.get("funder_award_id")
        or not row.get("award_year")
        or not row.get("award_field")
        or not row.get("laureate_name")
    ]
    if missing_core:
        raise RuntimeError(f"{len(missing_core)} rows missing core fields")


def write_outputs(rows: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    validate_rows(rows)
    df = pd.DataFrame(rows)

    log("summary:")
    log(f"  rows: {len(df):,}")
    log(f"  distinct funder_award_id: {df['funder_award_id'].nunique():,}")
    log(f"  year range: {df['award_year'].min()}-{df['award_year'].max()}")
    log(f"  rows with citation: {df['citation'].notna().sum():,}")
    log(f"  rows with research project URL: {df['research_project_url'].notna().sum():,}")
    log(f"  shared-prize rows: {(pd.to_numeric(df['winner_count'], errors='coerce') > 1).sum():,}")

    output_path = output_dir / OUTPUT_FILE
    log(f"writing {output_path}")
    df = df.astype("string")
    df.to_parquet(output_path, index=False)
    log(f"wrote {output_path.stat().st_size / (1024 * 1024):.2f} MB")
    return output_path


def find_aws_cli() -> str | None:
    import shutil

    aws_path = shutil.which("aws")
    if aws_path:
        return aws_path

    for path in [
        Path.home() / "Library/Python/3.9/bin/aws",
        Path.home() / "Library/Python/3.10/bin/aws",
        Path.home() / "Library/Python/3.11/bin/aws",
        Path.home() / "Library/Python/3.12/bin/aws",
        Path("/usr/local/bin/aws"),
        Path("/opt/homebrew/bin/aws"),
    ]:
        if path.exists():
            return str(path)

    return None


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
    prev_path = output_dir / "_prev_balzan_prize_laureates.parquet"
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


def upload_to_s3(local_path: Path) -> bool:
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    log(f"uploading {local_path} -> {s3_uri}")
    aws_cmd = find_aws_cli()
    if not aws_cmd:
        log("[ERROR] AWS CLI not found. Install with: pip install awscli")
        return False

    try:
        subprocess.run(
            [aws_cmd, "s3", "cp", str(local_path), s3_uri],
            capture_output=True,
            text=True,
            check=True,
        )
        log("upload complete")
        return True
    except subprocess.CalledProcessError as exc:
        log(f"[ERROR] upload failed: {exc.stderr}")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Balzan Prize winners and upload parquet to S3")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./balzan_prize_data"),
        help="Directory for checkpoint and parquet output",
    )
    parser.add_argument(
        "--use-cache",
        action="store_true",
        help="Reuse cached HTML in the checkpoint file",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip S3 upload after writing parquet",
    )
    parser.add_argument(
        "--allow-shrink",
        action="store_true",
        help="Override the runbook §1.4 shrink-check. Only use after confirming a smaller corpus is intentional.",
    )
    args = parser.parse_args()

    rows = fetch_rows(args.output_dir, use_cache=args.use_cache)
    parquet_path = write_outputs(rows, args.output_dir)

    upload_success = True
    if not args.skip_upload:
        import pandas as pd
        new_count = len(pd.read_parquet(parquet_path))
        if not check_no_shrink(new_count, args.allow_shrink, args.output_dir):
            raise SystemExit("§1.4 shrink-check failed. See above; re-run with --allow-shrink if intentional.")
        upload_success = upload_to_s3(parquet_path)
        if not upload_success:
            log(f"manual upload command: aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")

    if not upload_success:
        sys.exit(1)

    log("pipeline complete")
    log("next step: run notebooks/awards/CreateBalzanPrizeAwards.ipynb in Databricks")


if __name__ == "__main__":
    main()
