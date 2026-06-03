#!/usr/bin/env python3
"""
Royal Zoological Society of New South Wales research grants -> S3 pipeline.

Downloads official Royal Zoological Society of New South Wales (RZS NSW)
research-grant awardee pages, extracts recipient-level rows for the Paddy
Pallin Science Grants and Ethel Mary Read Research Grants, and writes a
parquet file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party: RZS NSW publishes the grant program pages and the
current/past awardee tables on rzsnsw.org.au. The Paddy Pallin program page
states that the grants are offered by the Paddy Pallin Foundation and RZS NSW
and administered through RZS NSW; per-row funding partners are preserved in
source_funding_partner. The Ethel Mary Read page describes the fund as a RZS
NSW council-established grant fund.

Validation target on 2026-05-28:
  - 82 official awardee rows from RZS NSW tables
  - 47 Paddy Pallin Science Grant rows, 2014-2025
  - 35 Ethel Mary Read Research Grant rows, 2013-2025
  - amount/currency NULL because the official pages publish maximum grants
    (AUD 10,000 and AUD 3,000) rather than exact per-recipient awards

Output
------
s3://openalex-ingest/awards/rzsnsw/rzsnsw_research_grants.parquet

Usage
-----
    python rzsnsw_research_grants_to_s3.py --skip-upload
    python rzsnsw_research_grants_to_s3.py --limit 10 --skip-upload
    python rzsnsw_research_grants_to_s3.py --skip-download --skip-upload
    python rzsnsw_research_grants_to_s3.py --allow-shrink

Requirements
------------
    pip install beautifulsoup4 pandas pyarrow requests boto3
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
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass


FUNDER_ID = 4320331891
FUNDER_DISPLAY_NAME = "Royal Zoological Society of New South Wales"
FUNDER_DOI = "10.13039/501100022897"
PROVENANCE = "rzsnsw_research_grants"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/rzsnsw/rzsnsw_research_grants.parquet"

PADDY_SCHEME = "Paddy Pallin Science Grant"
EMR_SCHEME = "Ethel Mary Read Research Grant"

PROGRAM_SOURCES = {
    PADDY_SCHEME: {
        "program_url": "https://www.rzsnsw.org.au/Paddy-Pallin",
        "awardees_url": "https://www.rzsnsw.org.au/page-18104",
        "source_key": "paddy_pallin",
        "program_amount_text": "official program page says grants are worth up to AUD 10,000 each",
        "amount_note": (
            "RZS NSW publishes an up-to-AUD-10,000 program cap and staged payment schedule, "
            "but not exact per-recipient award amounts; amount/currency left NULL."
        ),
    },
    EMR_SCHEME: {
        "program_url": "https://www.rzsnsw.org.au/EMR",
        "awardees_url": "https://www.rzsnsw.org.au/page-18112",
        "source_key": "ethel_mary_read",
        "program_amount_text": "official program page says the maximum grant awarded is AUD 3,000 per year",
        "amount_note": (
            "RZS NSW publishes an up-to-AUD-3,000 maximum grant, but not exact "
            "per-recipient award amounts; amount/currency left NULL."
        ),
    },
}

USER_AGENT = "openalex-walden-rzsnsw-research-grants-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25

RAW_JSON_FILENAME = "rzsnsw_research_grants_raw.json"
PARQUET_FILENAME = "rzsnsw_research_grants.parquet"
HTML_DIRNAME = "html"

_last_request_t = 0.0


def log(message: str) -> None:
    print(message, flush=True)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).replace("\u00a0", " ")
    text = BeautifulSoup(text, "html.parser").get_text(" ", strip=True) if "<" in text else text
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_text.lower()).strip("-")
    return slug or "unknown"


def stable_hash(*parts: Optional[str], length: int = 12) -> str:
    joined = "\n".join(part or "" for part in parts)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()[:length]


def strip_name_title(value: str) -> str:
    name = clean_text(value) or ""
    name = re.sub(r"^(Drs?\.?|Dra\.?|Mr\.?|Ms\.?|Mrs\.?|Prof\.?)\s+", "", name)
    name = re.sub(
        r",?\s*(Ph\.?D\.?|M\.?D\.?|M\.?Sc\.?|M\.?S\.?|B\.?Sc\.?|DPhil|Jr\.?|Sr\.?|II|III|IV)\.?$",
        "",
        name,
        flags=re.IGNORECASE,
    )
    return clean_text(name.strip(" ,")) or ""


def split_person_name(value: str) -> tuple[Optional[str], Optional[str]]:
    name = strip_name_title(value)
    parts = name.split()
    if not parts:
        return None, None
    if len(parts) == 1:
        return None, parts[0]
    return " ".join(parts[:-1]), parts[-1]


def polite_get_text(url: str, *, timeout: int = 60, max_attempts: int = 4) -> str:
    global _last_request_t
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.rzsnsw.org.au/Grants-Awards",
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


def page_title(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    return clean_text(soup.title.get_text(" ", strip=True) if soup.title else None)


def detail_lines(cell: Any) -> list[str]:
    lines = [
        clean_text(el.get_text(" ", strip=True))
        for el in cell.find_all(["h3", "p"], recursive=False)
    ]
    lines = [line for line in lines if line]
    if len(lines) >= 3:
        return lines
    fallback = [clean_text(text) for text in cell.stripped_strings]
    return [line for line in fallback if line]


def extract_links(cell: Any, base_url: str) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for anchor in cell.find_all("a"):
        href = anchor.get("href")
        label = clean_text(anchor.get_text(" ", strip=True))
        if href and label:
            links.append({"label": label, "url": urljoin(base_url, href)})
    return links


def funding_partner(lines: list[str]) -> Optional[str]:
    for line in lines:
        if line.startswith("Funded by "):
            return clean_text(line.replace("Funded by ", "", 1))
    return None


def make_description(
    *,
    scheme: str,
    year: str,
    recipient_name: str,
    affiliation: str,
    project_title: str,
    source_funding_partner: Optional[str],
) -> str:
    description = (
        f"{FUNDER_DISPLAY_NAME} {scheme} awarded in {year} to {recipient_name}"
        f" at {affiliation} for project: {project_title}."
    )
    if source_funding_partner:
        description += f" Source lists funding partner: {source_funding_partner}."
    return description


def make_record(
    *,
    source_key: str,
    scheme: str,
    year: str,
    project_title: str,
    recipient_name: str,
    affiliation: str,
    source_funding_partner: Optional[str],
    source_links: list[dict[str, str]],
    source_table_url: str,
    source_program_url: str,
    source_page_title: Optional[str],
    program_amount_text: str,
    amount_note: str,
    source_order: int,
) -> dict[str, Optional[str]]:
    given_name, family_name = split_person_name(recipient_name)
    recipient_clean = strip_name_title(recipient_name)
    project_clean = clean_text(project_title) or ""
    affiliation_clean = clean_text(affiliation) or ""
    link_url = source_links[0]["url"] if source_links else source_table_url
    source_hash = stable_hash(scheme, year, recipient_clean, affiliation_clean, project_clean)
    award_id = f"rzsnsw-{slugify(scheme)}-{year}-{slugify(recipient_clean)}-{source_hash[:8]}"
    return {
        "funder_id": str(FUNDER_ID),
        "funder_display_name": FUNDER_DISPLAY_NAME,
        "funder_doi": FUNDER_DOI,
        "provenance": PROVENANCE,
        "source_key": source_key,
        "source_order": str(source_order),
        "source_hash": source_hash,
        "source_page_title": source_page_title,
        "source_table_url": source_table_url,
        "source_program_url": source_program_url,
        "source_links_json": json.dumps(source_links, ensure_ascii=False),
        "landing_page_url": link_url,
        "funder_award_id": award_id,
        "display_name": f"{recipient_clean} - {scheme} ({year})",
        "description": make_description(
            scheme=scheme,
            year=year,
            recipient_name=recipient_clean,
            affiliation=affiliation_clean,
            project_title=project_clean,
            source_funding_partner=source_funding_partner,
        ),
        "project_title": project_clean,
        "recipient_name": recipient_clean,
        "given_name": given_name,
        "family_name": family_name,
        "affiliation": affiliation_clean,
        "country": None,
        "award_year": year,
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "funder_scheme": scheme,
        "funding_type": "research",
        "amount": None,
        "currency": None,
        "program_amount_text": program_amount_text,
        "amount_note": amount_note,
        "source_funding_partner": source_funding_partner,
        "doi": None,
    }


def parse_awardee_page(
    *,
    source_key: str,
    scheme: str,
    awardees_url: str,
    program_url: str,
    html: str,
    program_amount_text: str,
    amount_note: str,
) -> list[dict[str, Optional[str]]]:
    soup = BeautifulSoup(html, "html.parser")
    title = page_title(html)
    rows: list[dict[str, Optional[str]]] = []
    current_year: Optional[str] = None
    source_order = 0

    for tr in soup.find_all("tr"):
        cells = tr.find_all("td", recursive=False)
        if not cells:
            continue
        first_text = clean_text(cells[0].get_text(" ", strip=True))
        if first_text and re.fullmatch(r"(19|20)\d{2}", first_text):
            current_year = first_text
            continue
        if not current_year or len(cells) < 2:
            continue

        detail = cells[-1]
        links = extract_links(detail, awardees_url)
        lines = detail_lines(detail)
        if len(lines) < 3:
            continue
        if lines[0].startswith("The Royal Zoological Society of NSW aims to"):
            continue

        project_title = lines[0]
        recipient_name = lines[1]
        affiliation = lines[2]
        source_order += 1
        rows.append(
            make_record(
                source_key=source_key,
                scheme=scheme,
                year=current_year,
                project_title=project_title,
                recipient_name=recipient_name,
                affiliation=affiliation,
                source_funding_partner=funding_partner(lines[3:]),
                source_links=links,
                source_table_url=awardees_url,
                source_program_url=program_url,
                source_page_title=title,
                program_amount_text=program_amount_text,
                amount_note=amount_note,
                source_order=source_order,
            )
        )

    log(f"  [OK] parsed {len(rows):2d} rows from {source_key}")
    return rows


def download_sources() -> dict[str, Any]:
    log("\n" + "=" * 60)
    log("Step 1: Download official RZS NSW source pages")
    log("=" * 60)
    pages: dict[str, str] = {}
    urls: dict[str, str] = {}
    for scheme, spec in PROGRAM_SOURCES.items():
        for kind in ("program_url", "awardees_url"):
            key = f"{spec['source_key']}_{kind}"
            url = str(spec[kind])
            html = polite_get_text(url)
            pages[key] = html
            urls[key] = url
            log(f"  [OK] {scheme} {kind}: {len(html):,} characters")
    return {
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "pages": pages,
        "urls": urls,
    }


def load_cached_payload(output_dir: Path) -> dict[str, Any]:
    raw_json_path = output_dir / RAW_JSON_FILENAME
    if not raw_json_path.exists():
        raise FileNotFoundError(f"--skip-download requested but {raw_json_path} is missing")
    cached = json.loads(raw_json_path.read_text(encoding="utf-8"))
    html_dir = output_dir / HTML_DIRNAME
    pages: dict[str, str] = {}
    for key, filename in cached["html_files"].items():
        pages[key] = (html_dir / filename).read_text(encoding="utf-8")
    log(f"  [OK] loaded cached HTML from {html_dir}")
    return {
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "pages": pages,
        "urls": cached["urls"],
    }


def normalize_records(payload: dict[str, Any], *, full_run: bool) -> pd.DataFrame:
    log("\n" + "=" * 60)
    log("Step 2: Normalize RZS NSW research grant rows")
    log("=" * 60)
    pages = payload["pages"]
    rows: list[dict[str, Optional[str]]] = []
    for scheme, spec in PROGRAM_SOURCES.items():
        source_key = str(spec["source_key"])
        rows.extend(
            parse_awardee_page(
                source_key=source_key,
                scheme=scheme,
                awardees_url=str(spec["awardees_url"]),
                program_url=str(spec["program_url"]),
                html=pages[f"{source_key}_awardees_url"],
                program_amount_text=str(spec["program_amount_text"]),
                amount_note=str(spec["amount_note"]),
            )
        )

    df = pd.DataFrame(rows)
    df["retrieved_at"] = payload["retrieved_at"]
    validate_dataframe(df, full_run=full_run)
    return df


def validate_dataframe(df: pd.DataFrame, *, full_run: bool) -> None:
    total = len(df)
    if total == 0:
        raise RuntimeError("No RZS NSW award rows parsed")

    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")

    required_cols = [
        "funder_award_id",
        "display_name",
        "description",
        "recipient_name",
        "given_name",
        "family_name",
        "affiliation",
        "project_title",
        "award_year",
        "funder_scheme",
        "landing_page_url",
    ]
    for col in required_cols:
        covered = int(df[col].notna().sum())
        pct = covered / total * 100 if total else 0.0
        log(f"  {col:22s}: {covered:,}/{total:,} ({pct:.1f}%)")
        if covered != total:
            raise RuntimeError(f"Unexpected NULL values in required source column {col}")

    for col in ["amount", "currency", "source_funding_partner", "program_amount_text"]:
        covered = int(df[col].notna().sum())
        pct = covered / total * 100 if total else 0.0
        log(f"  {col:22s}: {covered:,}/{total:,} ({pct:.1f}%)")

    if full_run and total != 82:
        raise RuntimeError(f"Full RZS NSW run returned {total:,} rows; expected 82")

    years = pd.to_numeric(df["award_year"], errors="coerce")
    if years.isna().any():
        raise RuntimeError("Award years must parse numerically")

    amount_numeric = pd.to_numeric(df["amount"], errors="coerce")
    if amount_numeric.notna().any():
        raise RuntimeError("RZS NSW parser unexpectedly populated exact amounts from capped program text")

    log(f"  [OK] unique funder_award_id values: {df['funder_award_id'].nunique():,}")
    log(f"  Year range: {int(years.min())} - {int(years.max())}")
    log(f"  Scheme distribution: {df['funder_scheme'].value_counts().to_dict()}")
    log(f"  Exact amount coverage: 0/{total:,} (0.0%) - waived because source publishes caps only")


def write_outputs(payload: dict[str, Any], df: pd.DataFrame, output_dir: Path) -> Path:
    log("\n" + "=" * 60)
    log("Step 3: Write raw files and parquet")
    log("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)
    html_dir = output_dir / HTML_DIRNAME
    html_dir.mkdir(parents=True, exist_ok=True)

    html_files: dict[str, str] = {}
    for key, html in payload["pages"].items():
        filename = f"{key}.html"
        (html_dir / filename).write_text(html, encoding="utf-8")
        html_files[key] = filename

    raw_json = {
        "retrieved_at": payload["retrieved_at"],
        "urls": payload["urls"],
        "html_files": html_files,
        "records": df.to_dict("records"),
    }
    raw_json_path = output_dir / RAW_JSON_FILENAME
    raw_json_path.write_text(json.dumps(raw_json, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"  [OK] wrote raw/cache files to {output_dir}")

    parquet_path = output_dir / PARQUET_FILENAME
    parquet_df = df.astype("string")
    parquet_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    size_kb = parquet_path.stat().st_size / 1024
    log(f"  [OK] wrote {len(parquet_df):,} rows ({size_kb:.1f} KB) to {parquet_path}")
    return parquet_path


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

    prev_path = output_dir / "_prev_rzsnsw_research_grants.parquet"
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
        log(f"\n[ERROR] Refusing to shrink RZS NSW corpus ({prev_count:,} -> {new_count:,}).")
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
        description="Download RZS NSW research grants and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/rzsnsw_research_grants"))
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached source HTML from output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    log("=" * 60)
    log("Royal Zoological Society of New South Wales research grants -> S3 pipeline")
    log("=" * 60)
    log(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    log(f"  Source:     {PROGRAM_SOURCES[PADDY_SCHEME]['awardees_url']}")
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
        log(f"  [LIMIT] keeping first {len(df):,} award rows")
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
