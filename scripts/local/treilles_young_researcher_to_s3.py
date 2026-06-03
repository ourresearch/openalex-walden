#!/usr/bin/env python3
"""
Fondation des Treilles Prix jeune chercheur -> S3 Data Pipeline
==============================================================

Downloads the official Fondation des Treilles young-researcher laureate table
and writes a parquet file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party: Fondation des Treilles publishes a structured table
of all young-researcher prize laureates/beneficiaries with name, discipline,
and prize year:

    https://www.fondationdestreilles.com/la-recherche/le-prix-jeune-chercheur/les-laureats-du-prix-jeune-chercheur/

The official program page states that the Prix jeune chercheur is awarded by
Fondation des Treilles and has an amount of 6,000 euros:

    https://www.fondationdestreilles.com/la-recherche/le-prix-jeune-chercheur/

Validation on 2026-05-28:
  - 612 award rows from the official laureate table
  - 100% coverage: funder_award_id, name, award year, amount, currency,
    display_name, landing_page_url; 611/612 rows include discipline
  - year range 1983-2026; amount is EUR 6,000 per row per official program page

Output
------
s3://openalex-ingest/awards/treilles/treilles_young_researcher.parquet

Usage
-----
    python treilles_young_researcher_to_s3.py --skip-upload
    python treilles_young_researcher_to_s3.py --limit 10 --skip-upload
    python treilles_young_researcher_to_s3.py --skip-download --skip-upload
    python treilles_young_researcher_to_s3.py --allow-shrink

Requirements
------------
    pip install beautifulsoup4 lxml pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
import unicodedata
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass


LAUREATES_URL = (
    "https://www.fondationdestreilles.com/la-recherche/le-prix-jeune-chercheur/"
    "les-laureats-du-prix-jeune-chercheur/"
)
PROGRAM_URL = "https://www.fondationdestreilles.com/la-recherche/le-prix-jeune-chercheur/"

# Awarding body: Fondation des Treilles.
# Verified in OpenAlex as F4320327761 (FR).
FUNDER_ID = 4320327761
FUNDER_DISPLAY_NAME = "Fondation des Treilles"
PROVENANCE = "treilles_prix_jeune_chercheur"
FUNDER_SCHEME = "Prix jeune chercheur"
FUNDING_TYPE = "prize"
AMOUNT_EUR = "6000"
CURRENCY = "EUR"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/treilles/treilles_young_researcher.parquet"

USER_AGENT = "openalex-walden-treilles-young-researcher-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25

RAW_JSON_FILENAME = "treilles_young_researcher_raw.json"
RAW_HTML_FILENAME = "treilles_young_researcher_laureates.html"
PROGRAM_HTML_FILENAME = "treilles_young_researcher_program.html"
PARQUET_FILENAME = "treilles_young_researcher.parquet"

_last_request_t = 0.0

NAME_SUFFIXES = {
    "phd",
    "ph.d",
    "ph.d.",
    "md",
    "m.d",
    "m.d.",
    "dphil",
    "dsc",
    "d.sc",
    "d.sc.",
    "scd",
    "sc.d",
    "sc.d.",
    "jr",
    "jr.",
    "sr",
    "sr.",
    "ii",
    "iii",
    "iv",
}

FAMILY_PARTICLES = {
    "al",
    "d",
    "de",
    "del",
    "della",
    "des",
    "di",
    "do",
    "dos",
    "du",
    "el",
    "la",
    "le",
    "les",
    "van",
    "von",
}

ORG_MARKERS = {
    "association",
    "federation",
    "fondation",
    "institut",
    "laboratoire",
    "societe",
}


def log(message: str) -> None:
    print(message, flush=True)


def clean_text(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    text = str(value).replace("\u00a0", " ")
    text = BeautifulSoup(text, "html.parser").get_text(" ", strip=True) if "<" in text else text
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_text.lower()).strip("-")
    return slug or "unknown"


def normalize_name_token(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return ascii_text.lower().strip(" .,-'’")


def is_family_particle(value: str) -> bool:
    raw = value.lower().strip(" .,-")
    token = normalize_name_token(value)
    return token in FAMILY_PARTICLES or raw.startswith(("d'", "d’"))


def looks_non_person_name(name: str, tokens: list[str]) -> bool:
    normalized = " ".join(normalize_name_token(token) for token in tokens)
    if "(" in name or ")" in name:
        return True
    if any(marker in normalized.split() for marker in ORG_MARKERS):
        return True
    if normalized == "la main a la pate":
        return True
    return False


def split_nom_prenom(value: str) -> tuple[Optional[str], Optional[str]]:
    """Parse the source's family-name-then-given-name value.

    The Fondation table labels this column "Nom et prénom". Most rows are
    "Family Given", but French particles can make the family name span multiple
    tokens, e.g. "de Brito José Marquès" or "Le Bohec Céline".
    """
    name = clean_text(value)
    if not name:
        return None, None
    tokens = [token for token in re.split(r"\s+", name) if token]
    while tokens and normalize_name_token(tokens[-1]) in NAME_SUFFIXES:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1 or looks_non_person_name(name, tokens):
        return None, name

    family_end = 2 if is_family_particle(tokens[0]) and len(tokens) > 2 else 1
    while family_end < len(tokens) - 1 and is_family_particle(tokens[family_end]):
        family_end += 1
        while family_end < len(tokens) - 1 and is_family_particle(tokens[family_end]):
            family_end += 1
        if family_end < len(tokens) - 1:
            family_end += 1

    family = clean_text(" ".join(tokens[:family_end]))
    given = clean_text(" ".join(tokens[family_end:]))
    return given, family


def polite_get_text(url: str, *, timeout: int = 90, max_attempts: int = 4) -> str:
    global _last_request_t
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.fondationdestreilles.com/",
    }
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < MIN_REQUEST_INTERVAL_S:
            time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            _last_request_t = time.monotonic()
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 20)
                log(f"  [WARN] HTTP {resp.status_code}; retrying in {sleep_s}s")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp.text
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 20)
            log(f"  [WARN] request failed ({exc}); retrying in {sleep_s}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def download_sources() -> dict[str, Any]:
    log("\n" + "=" * 60)
    log("Step 1: Download official Fondation des Treilles source pages")
    log("=" * 60)
    laureates_html = polite_get_text(LAUREATES_URL)
    program_html = polite_get_text(PROGRAM_URL)
    log(f"  [OK] downloaded laureate page: {len(laureates_html):,} characters")
    log(f"  [OK] downloaded program page: {len(program_html):,} characters")
    return {
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "laureates_url": LAUREATES_URL,
        "program_url": PROGRAM_URL,
        "laureates_html": laureates_html,
        "program_html": program_html,
    }


def source_page_title(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    return clean_text(soup.title.get_text(" ", strip=True) if soup.title else None)


def parse_laureate_table(payload: dict[str, Any]) -> list[dict[str, Any]]:
    tables = pd.read_html(StringIO(payload["laureates_html"]))
    if len(tables) != 1:
        raise RuntimeError(f"Expected exactly one laureate table; found {len(tables)}")
    table = tables[0]
    expected_cols = {"Nom et prénom", "Discipline", "Année du prix"}
    if set(table.columns) != expected_cols:
        raise RuntimeError(f"Unexpected table columns: {table.columns.tolist()}")

    laureates_title = source_page_title(payload["laureates_html"])
    program_title = source_page_title(payload["program_html"])
    rows: list[dict[str, Any]] = []
    for idx, row in table.iterrows():
        name = clean_text(row["Nom et prénom"])
        discipline = clean_text(row["Discipline"])
        year = clean_text(row["Année du prix"])
        if name == "Name" and discipline == "Field of Study" and year == "Award Year":
            continue
        if not (name and year):
            raise RuntimeError(f"Missing required table value at row {idx}: {row.to_dict()}")
        if not re.fullmatch(r"\d{4}", year):
            raise RuntimeError(f"Unexpected prize year {year!r} at row {idx}")
        source_key = f"{year}|{name}|{discipline or 'no-discipline'}"
        source_hash = hashlib.sha1(source_key.encode("utf-8")).hexdigest()[:10]
        given_name, family_name = split_nom_prenom(name)
        rows.append({
            "source_row_number": str(idx + 1),
            "source_hash": source_hash,
            "name": name,
            "given_name": given_name,
            "family_name": family_name,
            "discipline": discipline,
            "award_year": year,
            "amount": AMOUNT_EUR,
            "currency": CURRENCY,
            "amount_note": "Official program page states the Prix jeune chercheur amount is 6000 euros.",
            "funder_scheme": FUNDER_SCHEME,
            "funding_type": FUNDING_TYPE,
            "landing_page_url": LAUREATES_URL,
            "program_url": PROGRAM_URL,
            "source_page_title": laureates_title,
            "program_page_title": program_title,
            "retrieved_at": payload["retrieved_at"],
        })
    return rows


def normalize_records(payload: dict[str, Any], *, full_run: bool) -> pd.DataFrame:
    log("\n" + "=" * 60)
    log("Step 2: Normalize laureate table")
    log("=" * 60)
    records = parse_laureate_table(payload)
    rows: list[dict[str, Any]] = []
    for rec in records:
        award_id = (
            f"treilles-young-researcher-{rec['award_year']}-"
            f"{slugify(rec['name'])}-{rec['source_hash']}"
        )
        discipline_phrase = (
            f"; source discipline: {rec['discipline']}"
            if rec["discipline"]
            else "; source discipline not provided"
        )
        rows.append({
            "funder_award_id": award_id,
            "display_name": f"Prix jeune chercheur {rec['award_year']} - {rec['name']}",
            "description": (
                f"{rec['name']} is listed by Fondation des Treilles as a Prix jeune chercheur "
                f"laureate/beneficiary for {rec['award_year']}{discipline_phrase}."
            ),
            **rec,
        })
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
        "name",
        "award_year",
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

    discipline_covered = int(df["discipline"].notna().sum())
    log(f"  {'discipline':18s}: {discipline_covered:,}/{total:,} ({discipline_covered / total * 100 if total else 0:.1f}%)")

    given_covered = int(df["given_name"].notna().sum())
    family_covered = int(df["family_name"].notna().sum())
    log(f"  {'given_name':18s}: {given_covered:,}/{total:,} ({given_covered / total * 100 if total else 0:.1f}%)")
    log(f"  {'family_name':18s}: {family_covered:,}/{total:,} ({family_covered / total * 100 if total else 0:.1f}%)")
    family_norm = df["family_name"].dropna().map(normalize_name_token)
    particle_only = sorted(set(family_norm[family_norm.isin(FAMILY_PARTICLES)]))
    if particle_only:
        raise RuntimeError(f"Particle-only parsed family names found: {particle_only}")
    multi_token_families = df["family_name"].dropna()[df["family_name"].dropna().str.contains(r"\s")]
    log(f"  {'multi-token family':18s}: {len(multi_token_families):,}/{family_covered:,}")
    if not multi_token_families.empty:
        log(f"  Multi-token family examples: {multi_token_families.head(12).tolist()}")
    log(f"  Top family names: {df['family_name'].dropna().value_counts().head(12).to_dict()}")

    if full_run and total < 600:
        raise RuntimeError(f"Full Fondation des Treilles run returned {total:,} rows; expected at least 600")

    years = pd.to_numeric(df["award_year"], errors="coerce")
    amount_numeric = pd.to_numeric(df["amount"], errors="coerce")
    if years.isna().any() or amount_numeric.isna().any():
        raise RuntimeError("Year and amount fields must parse numerically")

    log(f"  [OK] unique funder_award_id values: {df['funder_award_id'].nunique():,}")
    log(f"  Year range: {int(years.min())} - {int(years.max())}")
    log(f"  Total EUR amount: {amount_numeric.sum():,.0f}")
    log(f"  Top disciplines: {df['discipline'].value_counts().head(10).to_dict()}")


def cache_payload_for_json(payload: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
    return {
        "retrieved_at": payload["retrieved_at"],
        "laureates_url": LAUREATES_URL,
        "program_url": PROGRAM_URL,
        "laureates_html_sha256": hashlib.sha256(payload["laureates_html"].encode("utf-8")).hexdigest(),
        "program_html_sha256": hashlib.sha256(payload["program_html"].encode("utf-8")).hexdigest(),
        "records": df.to_dict("records"),
    }


def write_outputs(payload: dict[str, Any], df: pd.DataFrame, output_dir: Path) -> Path:
    log("\n" + "=" * 60)
    log("Step 3: Write raw files and parquet")
    log("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    (output_dir / RAW_HTML_FILENAME).write_text(payload["laureates_html"], encoding="utf-8")
    (output_dir / PROGRAM_HTML_FILENAME).write_text(payload["program_html"], encoding="utf-8")
    raw_json_path = output_dir / RAW_JSON_FILENAME
    raw_json_path.write_text(json.dumps(cache_payload_for_json(payload, df), ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"  [OK] wrote raw/cache files to {output_dir}")

    parquet_path = output_dir / PARQUET_FILENAME
    parquet_df = df.astype("string")
    parquet_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    size_kb = parquet_path.stat().st_size / 1024
    log(f"  [OK] wrote {len(parquet_df):,} rows ({size_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def load_cached_payload(output_dir: Path) -> dict[str, Any]:
    laureates_path = output_dir / RAW_HTML_FILENAME
    program_path = output_dir / PROGRAM_HTML_FILENAME
    if not laureates_path.exists() or not program_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cached HTML is missing in {output_dir}")
    payload = {
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "laureates_url": LAUREATES_URL,
        "program_url": PROGRAM_URL,
        "laureates_html": laureates_path.read_text(encoding="utf-8"),
        "program_html": program_path.read_text(encoding="utf-8"),
    }
    log(f"  [OK] loaded cached HTML from {output_dir}")
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

    prev_path = output_dir / "_prev_treilles_young_researcher.parquet"
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
        log(f"\n[ERROR] Refusing to shrink Treilles corpus ({prev_count:,} -> {new_count:,}).")
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
        description="Download Fondation des Treilles young-researcher laureates and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/treilles_young_researcher"))
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached source HTML from output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    log("=" * 60)
    log("Fondation des Treilles Prix jeune chercheur -> S3 pipeline")
    log("=" * 60)
    log(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    log(f"  Source:     {LAUREATES_URL}")
    log(f"  Output dir: {args.output_dir.absolute()}")
    log(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    log(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        log("\nStep 1: Reuse cached source HTML")
        payload = load_cached_payload(args.output_dir)
    else:
        payload = download_sources()

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
