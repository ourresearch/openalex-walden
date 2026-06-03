#!/usr/bin/env python3
"""
American Ornithological Society Kessel/LACCR research awards -> S3 pipeline.

Downloads official American Ornithological Society (AOS) announcement pages for
Kessel Fellowships and Latin American/Caribbean Conservation Research Grants,
extracts recipient-level research award rows, and writes a parquet file for the
OpenAlex awards pipeline.

Source authority
----------------
The source is first-party: AOS publishes the announcement posts and program
pages on americanornithology.org. The OpenAlex funder row used here is the
current American Ornithological Society row, F4320313553. OpenAlex also has
legacy American Ornithologists' Union and Cooper Ornithological Society rows,
but these post-2016 awards are issued by AOS.

Validation target on 2026-05-28:
  - 35 rows from official AOS announcement pages
  - 19 Kessel Fellowship rows with exact USD amounts from AOS announcements
  - 16 LACCR Grant rows with amount/currency NULL because exact per-recipient
    amounts are not published; the official "up to $5,000" cap is preserved
  - year range 2020-2026

Output
------
s3://openalex-ingest/awards/aos/aos_research_awards.parquet

Usage
-----
    python aos_research_awards_to_s3.py --skip-upload
    python aos_research_awards_to_s3.py --limit 10 --skip-upload
    python aos_research_awards_to_s3.py --skip-download --skip-upload
    python aos_research_awards_to_s3.py --allow-shrink

Requirements
------------
    pip install beautifulsoup4 pandas pyarrow requests boto3
"""

from __future__ import annotations

# --- Windows UTF-8 compatibility shim (runbook §1.2 #7; no-op on Linux/Databricks) ---
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

import pandas as pd
import requests
from bs4 import BeautifulSoup


FUNDER_ID = 4320313553
FUNDER_DISPLAY_NAME = "American Ornithological Society"
PROVENANCE = "aos_kessel_laccr_research"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/aos/aos_research_awards.parquet"

SITEMAP_URL = "https://americanornithology.org/post-sitemap.xml"
PROGRAM_URLS = {
    "kessel": "https://americanornithology.org/awards-grants/research-grants/kessel-fellowships/",
    "laccr": (
        "https://americanornithology.org/awards-grants/research-grants/"
        "latin-american-caribbean-conservation-research-grant/"
    ),
}

SOURCE_URLS = {
    "kessel_2020": "https://americanornithology.org/2020-kessel-research-fellowship-awardees/",
    "kessel_2022": "https://americanornithology.org/2022-aos-kessel-research-fellowship-awardees/",
    "kessel_2023": (
        "https://americanornithology.org/"
        "aos-awards-four-2023-kessel-fellowships-for-ornithological-research/"
    ),
    "kessel_2024": (
        "https://americanornithology.org/"
        "aos-awards-four-2024-kessel-fellowships-for-ornithological-research/"
    ),
    "combined_2025": (
        "https://americanornithology.org/"
        "aos-announces-2025-kessel-fellows-latin-american-caribbean-research-grantees/"
    ),
    "laccr_2023": (
        "https://americanornithology.org/"
        "aos-announces-2023-latin-america-caribbean-conservation-research-awards-winners/"
    ),
    "laccr_2026": (
        "https://americanornithology.org/"
        "aos-announces-2026-latin-american-caribbean-conservation-research-grantees/"
    ),
}

KESSEL_SCHEME = "Kessel Fellowship"
LACCR_SCHEME = "Latin American/Caribbean Conservation Research Grant"

USER_AGENT = "openalex-walden-aos-research-awards-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25

RAW_JSON_FILENAME = "aos_research_awards_raw.json"
PARQUET_FILENAME = "aos_research_awards.parquet"
HTML_DIRNAME = "html"

_last_request_t = 0.0


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


def strip_name_title(value: str) -> str:
    name = clean_text(value) or ""
    name = re.sub(r"^(Drs?\.?|Dra\.?|Mr\.?|Ms\.?)\s+", "", name)
    name = re.sub(r",?\s*(Ph\.D\.|M\.Sc\.|M\.S\.|MSc|MS|PhD)\.?$", "", name)
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
        "Referer": "https://americanornithology.org/awards-grants/",
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


def page_nodes(html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main") or soup
    nodes: list[tuple[str, str]] = []
    for el in main.find_all(["h1", "h2", "h3", "p"]):
        text = clean_text(el.get_text(" ", strip=True))
        if text:
            nodes.append((el.name, text))
    return nodes


def source_page_title(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    return clean_text(soup.title.get_text(" ", strip=True) if soup.title else None)


def publication_date(nodes: list[tuple[str, str]]) -> Optional[str]:
    for _tag, text in nodes[:12]:
        if re.fullmatch(r"\d{1,2}\s+[A-Za-z]+\s+\d{4}", text):
            return text
    return None


def year_from_url_title(url: str, title: Optional[str]) -> str:
    m = re.search(r"(20\d{2})", f"{url} {title or ''}")
    if not m:
        raise RuntimeError(f"Could not infer award year from {url}")
    return m.group(1)


def project_from_text(text: str) -> Optional[str]:
    match = re.search(
        r"(?:Joint project title|Project title|Project)\s*:\s*[“\"]?(.+?)(?:[”\"]?\s+Abstract\s*:|[”\"]?$)",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        return clean_text(match.group(1).strip(" “”\""))
    return None


def record(
    *,
    name: str,
    affiliation: Optional[str],
    project_title: Optional[str],
    funder_scheme: str,
    award_year: str,
    amount: Optional[str],
    currency: Optional[str],
    amount_note: str,
    program_amount_text: Optional[str],
    landing_page_url: str,
    source_page_title_value: Optional[str],
    source_publication_date: Optional[str],
    source_order: int,
) -> dict[str, Any]:
    clean_name = strip_name_title(name)
    given_name, family_name = split_person_name(clean_name)
    source_key = f"{award_year}|{funder_scheme}|{clean_name}|{project_title or ''}"
    source_hash = hashlib.sha1(source_key.encode("utf-8")).hexdigest()[:10]
    award_id = f"aos-{slugify(funder_scheme)}-{award_year}-{slugify(clean_name)}-{source_hash}"
    description_parts = [
        f"{clean_name} is listed by the American Ornithological Society as a {funder_scheme} recipient for {award_year}."
    ]
    if project_title:
        description_parts.append(f"Project: {project_title}.")
    if affiliation:
        description_parts.append(f"Source affiliation/role: {affiliation}.")
    return {
        "funder_award_id": award_id,
        "display_name": f"{funder_scheme} {award_year} - {clean_name}",
        "description": " ".join(description_parts),
        "recipient_name": clean_name,
        "given_name": given_name,
        "family_name": family_name,
        "affiliation": clean_text(affiliation),
        "project_title": clean_text(project_title),
        "award_year": award_year,
        "amount": amount,
        "currency": currency,
        "amount_note": amount_note,
        "program_amount_text": program_amount_text,
        "funder_scheme": funder_scheme,
        "funding_type": "fellowship" if funder_scheme == KESSEL_SCHEME else "research",
        "landing_page_url": landing_page_url,
        "program_url": PROGRAM_URLS["kessel"] if funder_scheme == KESSEL_SCHEME else PROGRAM_URLS["laccr"],
        "source_page_title": source_page_title_value,
        "source_publication_date": source_publication_date,
        "source_order": str(source_order),
        "source_hash": source_hash,
    }


def parse_heading_page(
    key: str,
    url: str,
    html: str,
    *,
    default_scheme: str,
) -> list[dict[str, Any]]:
    nodes = page_nodes(html)
    title = nodes[0][1]
    year = year_from_url_title(url, title)
    page_title = source_page_title(html)
    pub_date = publication_date(nodes)
    rows: list[dict[str, Any]] = []
    current_scheme = default_scheme
    heading_tags = {"h2", "h3"}
    skip_heads = {
        "Related Posts",
        "Leave a Reply Cancel reply",
        "Kessel Fellowships for Ornithological Research",
        "Latin American/Caribbean Conservation Research Grants",
    }
    i = 0
    while i < len(nodes):
        tag, text = nodes[i]
        if tag == "h2" and text in skip_heads:
            if "Kessel" in text:
                current_scheme = KESSEL_SCHEME
            elif "Latin American/Caribbean" in text:
                current_scheme = LACCR_SCHEME
            i += 1
            continue

        if tag in heading_tags and text not in skip_heads and not text.startswith("2025 Winners"):
            name = strip_name_title(text)
            if len(name.split()) < 2 or "American Ornithological Society announces" in name:
                i += 1
                continue
            affiliation = None
            project_title = None
            j = i + 1
            if j < len(nodes) and nodes[j][0] == "p":
                affiliation = nodes[j][1]
                j += 1
            while j < len(nodes) and nodes[j][0] not in heading_tags:
                found_project = project_from_text(nodes[j][1])
                if found_project and not project_title:
                    project_title = found_project
                j += 1

            if current_scheme == KESSEL_SCHEME:
                amount = "30000" if year == "2025" and "Teresa Pegan" in name else "15000"
                currency = "USD"
                amount_note = (
                    "AOS announcement states one 2025 Arctic Kessel Fellowship was $30,000 and "
                    "other Kessel Fellowships in this corpus were $15,000."
                )
                program_amount_text = "Kessel announcements publish exact USD amounts."
            else:
                amount = None
                currency = None
                amount_note = (
                    "AOS LACCR pages publish an up-to-$5,000 USD program cap but not exact "
                    "per-recipient award amounts; amount/currency left NULL by source authority."
                )
                program_amount_text = "up to $5,000 USD"

            rows.append(record(
                name=name,
                affiliation=affiliation,
                project_title=project_title,
                funder_scheme=current_scheme,
                award_year=year,
                amount=amount,
                currency=currency,
                amount_note=amount_note,
                program_amount_text=program_amount_text,
                landing_page_url=url,
                source_page_title_value=page_title,
                source_publication_date=pub_date,
                source_order=len(rows) + 1,
            ))
            i = j
            continue
        i += 1
    log(f"  [OK] parsed {len(rows):2d} rows from {key}")
    return rows


def parse_paragraph_kessel_page(key: str, url: str, html: str) -> list[dict[str, Any]]:
    nodes = page_nodes(html)
    title = nodes[0][1]
    year = year_from_url_title(url, title)
    page_title = source_page_title(html)
    pub_date = publication_date(nodes)
    rows: list[dict[str, Any]] = []
    i = 0
    while i < len(nodes):
        tag, text = nodes[i]
        if tag == "p" and re.match(r"Dr\.\s+", text) and "," in text and not text.startswith("Dr. Brina"):
            name_part, affiliation = text.split(",", 1)
            project_title = project_from_text(nodes[i + 1][1]) if i + 1 < len(nodes) else None
            if project_title:
                rows.append(record(
                    name=name_part,
                    affiliation=affiliation,
                    project_title=project_title,
                    funder_scheme=KESSEL_SCHEME,
                    award_year=year,
                    amount="15000",
                    currency="USD",
                    amount_note="AOS announcement states these Kessel Fellowships were $15,000 each.",
                    program_amount_text="$15,000",
                    landing_page_url=url,
                    source_page_title_value=page_title,
                    source_publication_date=pub_date,
                    source_order=len(rows) + 1,
                ))
                i += 2
                continue
        i += 1
    log(f"  [OK] parsed {len(rows):2d} rows from {key}")
    return rows


def parse_laccr_2023(key: str, url: str, html: str) -> list[dict[str, Any]]:
    nodes = page_nodes(html)
    page_title = source_page_title(html)
    pub_date = publication_date(nodes)
    text_values = [text for _tag, text in nodes]
    joint_project = next((project_from_text(text) for text in text_values if text.startswith("Joint project title")), None)
    individual_project = next((project_from_text(text) for text in text_values if text.startswith("Project title")), None)
    specs = [
        (
            "Valentina Gómez-Bahamón",
            "Eberly Postdoctoral Fellow in the Toews Lab at The Pennsylvania State University",
            joint_project,
        ),
        (
            "Sergio Estrada Villegas",
            "Junior Assistant Professor at the Universidad del Rosario, Bogotá, Colombia",
            joint_project,
        ),
        (
            "María Emilia Rebollo",
            "Postdoctoral Fellow at CONICET, Argentina; ColBEC (FCEyN, UNLPam); INCITAP (CONICET, UNLPam)",
            individual_project,
        ),
    ]
    rows = [
        record(
            name=name,
            affiliation=affiliation,
            project_title=project_title,
            funder_scheme=LACCR_SCHEME,
            award_year="2023",
            amount=None,
            currency=None,
            amount_note=(
                "AOS LACCR pages publish an up-to-$5,000 USD program cap but not exact "
                "per-recipient award amounts; amount/currency left NULL by source authority."
            ),
            program_amount_text="up to $5,000 USD",
            landing_page_url=url,
            source_page_title_value=page_title,
            source_publication_date=pub_date,
            source_order=i + 1,
        )
        for i, (name, affiliation, project_title) in enumerate(specs)
    ]
    log(f"  [OK] parsed {len(rows):2d} rows from {key}")
    return rows


def download_sources() -> dict[str, Any]:
    log("\n" + "=" * 60)
    log("Step 1: Download official AOS source pages")
    log("=" * 60)
    pages: dict[str, str] = {}
    all_urls = {"post_sitemap": SITEMAP_URL, **PROGRAM_URLS, **SOURCE_URLS}
    for key, url in all_urls.items():
        html = polite_get_text(url)
        pages[key] = html
        log(f"  [OK] {key:14s} {len(html):,} characters")
    return {
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "pages": pages,
        "urls": all_urls,
    }


def load_cached_payload(output_dir: Path) -> dict[str, Any]:
    raw_json_path = output_dir / RAW_JSON_FILENAME
    if not raw_json_path.exists():
        raise FileNotFoundError(f"--skip-download requested but {raw_json_path} is missing")
    cached = json.loads(raw_json_path.read_text(encoding="utf-8"))
    pages: dict[str, str] = {}
    html_dir = output_dir / HTML_DIRNAME
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
    log("Step 2: Normalize AOS research award rows")
    log("=" * 60)
    pages = payload["pages"]
    rows: list[dict[str, Any]] = []
    rows.extend(parse_paragraph_kessel_page("kessel_2020", SOURCE_URLS["kessel_2020"], pages["kessel_2020"]))
    rows.extend(parse_paragraph_kessel_page("kessel_2022", SOURCE_URLS["kessel_2022"], pages["kessel_2022"]))
    rows.extend(parse_heading_page("kessel_2023", SOURCE_URLS["kessel_2023"], pages["kessel_2023"], default_scheme=KESSEL_SCHEME))
    rows.extend(parse_heading_page("kessel_2024", SOURCE_URLS["kessel_2024"], pages["kessel_2024"], default_scheme=KESSEL_SCHEME))
    rows.extend(parse_heading_page("combined_2025", SOURCE_URLS["combined_2025"], pages["combined_2025"], default_scheme=KESSEL_SCHEME))
    rows.extend(parse_laccr_2023("laccr_2023", SOURCE_URLS["laccr_2023"], pages["laccr_2023"]))
    rows.extend(parse_heading_page("laccr_2026", SOURCE_URLS["laccr_2026"], pages["laccr_2026"], default_scheme=LACCR_SCHEME))

    df = pd.DataFrame(rows)
    df["retrieved_at"] = payload["retrieved_at"]
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
        "recipient_name",
        "family_name",
        "project_title",
        "award_year",
        "funder_scheme",
        "landing_page_url",
    ]
    for col in required_cols:
        covered = int(df[col].notna().sum())
        pct = covered / total * 100 if total else 0.0
        log(f"  {col:18s}: {covered:,}/{total:,} ({pct:.1f}%)")
        if covered != total:
            raise RuntimeError(f"Unexpected NULL values in required source column {col}")

    for col in ["given_name", "affiliation", "amount", "currency", "program_amount_text"]:
        covered = int(df[col].notna().sum())
        pct = covered / total * 100 if total else 0.0
        log(f"  {col:18s}: {covered:,}/{total:,} ({pct:.1f}%)")

    if full_run and total != 35:
        raise RuntimeError(f"Full AOS run returned {total:,} rows; expected 35")

    years = pd.to_numeric(df["award_year"], errors="coerce")
    if years.isna().any():
        raise RuntimeError("Award years must parse numerically")
    amount_numeric = pd.to_numeric(df["amount"], errors="coerce")
    exact_amount_rows = int(amount_numeric.notna().sum())
    if full_run and exact_amount_rows < 19:
        raise RuntimeError(f"Expected at least 19 exact-amount Kessel rows; found {exact_amount_rows}")

    log(f"  [OK] unique funder_award_id values: {df['funder_award_id'].nunique():,}")
    log(f"  Year range: {int(years.min())} - {int(years.max())}")
    log(f"  Scheme distribution: {df['funder_scheme'].value_counts().to_dict()}")
    log(f"  Exact amount total USD: {amount_numeric.sum():,.0f}")


def write_outputs(payload: dict[str, Any], df: pd.DataFrame, output_dir: Path) -> Path:
    log("\n" + "=" * 60)
    log("Step 3: Write raw files and parquet")
    log("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)
    html_dir = output_dir / HTML_DIRNAME
    html_dir.mkdir(parents=True, exist_ok=True)

    html_files: dict[str, str] = {}
    for key, html in payload["pages"].items():
        filename = f"{key}.html" if key != "post_sitemap" else "post_sitemap.xml"
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

    prev_path = output_dir / "_prev_aos_research_awards.parquet"
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
        log(f"\n[ERROR] Refusing to shrink AOS corpus ({prev_count:,} -> {new_count:,}).")
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
        description="Download AOS Kessel/LACCR research awards and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/aos_research_awards"))
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached source HTML from output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    log("=" * 60)
    log("American Ornithological Society Kessel/LACCR research awards -> S3 pipeline")
    log("=" * 60)
    log(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    log(f"  Source:     {SITEMAP_URL}")
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
