#!/usr/bin/env python3
"""
Stanley Medical Research Institute treatment trials -> S3.

Source authority
----------------
The source is SMRI's own Awarded Treatment Trials table and linked detail
pages:

    https://www.stanleyresearch.org/awarded-treatment-trials/

The static table lists one row per awarded trial with a real SMRI Grant ID,
illness, intervention, status, investigator, sample size, state/province, and
country. Each Grant ID links to a detail page with PI name, institution, center,
address/city/country, outcome measures, results, publication, and study link.

OpenAlex funder
---------------
F4320309530 - Stanley Medical Research Institute
ROR https://ror.org/01pj5nn22, DOI 10.13039/100007123.

Output
------
s3://openalex-ingest/awards/smri_treatment_trials/smri_treatment_trials.parquet

Usage
-----
    python scripts/local/smri_treatment_trials_to_s3.py --limit 10 --skip-upload
    python scripts/local/smri_treatment_trials_to_s3.py --skip-upload
    python scripts/local/smri_treatment_trials_to_s3.py
    python scripts/local/smri_treatment_trials_to_s3.py --allow-shrink
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup


# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
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

    def _open_utf8(
        file,
        mode="r",
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        closefd=True,
        opener=None,
    ):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)

    _builtins_utf8.open = _open_utf8
# --- end shim ---


FUNDER_ID = 4320309530
FUNDER_DISPLAY_NAME = "Stanley Medical Research Institute"
PROVENANCE = "smri_treatment_trials"

BASE_URL = "https://www.stanleyresearch.org"
INDEX_URL = f"{BASE_URL}/awarded-treatment-trials/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/smri_treatment_trials/smri_treatment_trials.parquet"

DEFAULT_OUTPUT_DIR = Path("/tmp/smri_treatment_trials")
INDEX_HTML = "awarded-treatment-trials.html"
DETAILS_DIR = "details"
OUTPUT_PARQUET_NAME = "smri_treatment_trials.parquet"
EXPECTED_MIN_FULL_ROWS = 350
REQUEST_TIMEOUT = (10, 90)
MAX_RETRIES = 5

HEADERS = {
    "User-Agent": "openalex-walden-smri-treatment-trials/1.0 (+https://openalex.org)",
    "Accept": "text/html,application/xhtml+xml",
}

SPACE_RE = re.compile(r"\s+")
SUFFIX_RE = re.compile(
    r"(?:,\s*)?(?:Ph\\.?D\\.?|M\\.?D\\.?|DPhil|Jr\\.?|Sr\\.?|II|III|IV)$",
    flags=re.I,
)
PREFIX_RE = re.compile(r"^(?:Dr\\.?|Prof\\.?|Professor|Mr\\.?|Mrs\\.?|Ms\\.?|Miss)\\s+", re.I)

COUNTRY_CODES = {
    "Australia": "AU",
    "Austria": "AT",
    "Belgium": "BE",
    "Brazil": "BR",
    "Canada": "CA",
    "China": "CN",
    "Denmark": "DK",
    "Ethiopia": "ET",
    "Finland": "FI",
    "France": "FR",
    "Germany": "DE",
    "India": "IN",
    "Ireland": "IE",
    "Israel": "IL",
    "Italy": "IT",
    "Japan": "JP",
    "Korea": "KR",
    "Netherlands": "NL",
    "New Zealand": "NZ",
    "Norway": "NO",
    "Peru": "PE",
    "Romania": "RO",
    "Russia": "RU",
    "Serbia": "RS",
    "South Africa": "ZA",
    "South Korea": "KR",
    "Spain": "ES",
    "Sweden": "SE",
    "The Netherlands": "NL",
    "Turkey": "TR",
    "U.S.A.": "US",
    "USA": "US",
    "United Kingdom": "GB",
    "United States": "US",
}


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).replace("\u00a0", " ").strip()
    if not text or text.lower() in {"nan", "none", "n/a", "na", "-"}:
        return None
    return SPACE_RE.sub(" ", text)


def json_string(value: Any) -> Optional[str]:
    if value in (None, [], {}):
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def fetch_url(url: str, output_path: Path) -> str:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code == 404:
                raise FileNotFoundError(f"404 detail page not found: {url}")
            r.raise_for_status()
            r.encoding = "utf-8"
            output_path.write_text(r.text, encoding="utf-8")
            return r.text
        except requests.RequestException as exc:
            last_exc = exc
            sleep = min(2 ** attempt, 30)
            log(f"  retry {attempt}/{MAX_RETRIES} for {url} after {exc} (sleep {sleep}s)")
            time.sleep(sleep)
    raise RuntimeError(f"Failed to fetch {url}: {last_exc}") from last_exc


def load_or_fetch(url: str, output_path: Path, skip_download: bool) -> str:
    if skip_download:
        if not output_path.exists():
            raise RuntimeError(f"--skip-download set but cached HTML is missing: {output_path}")
        return output_path.read_text(encoding="utf-8")
    if output_path.exists():
        log(f"[CACHE] using {output_path}")
        return output_path.read_text(encoding="utf-8")
    log(f"[FETCH] {url}")
    return fetch_url(url, output_path)


def parse_index(html: str) -> list[dict[str, Optional[str]]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table#awarded_trials")
    if table is None:
        raise RuntimeError("Could not find table#awarded_trials")
    rows: list[dict[str, Optional[str]]] = []
    for tr in table.select("tbody tr"):
        cells = tr.find_all("td")
        if len(cells) != 9:
            continue
        grant_link = cells[0].find("a")
        grant_id = clean_text(cells[0].get_text(" ", strip=True))
        href = grant_link.get("href") if grant_link else None
        rows.append(
            {
                "grant_id": grant_id,
                "index_illness": clean_text(cells[1].get_text(" ", strip=True)),
                "index_primary_drug": clean_text(cells[2].get_text(" ", strip=True)),
                "index_secondary_drug": clean_text(cells[3].get_text(" ", strip=True)),
                "index_status": clean_text(cells[4].get_text(" ", strip=True)),
                "index_investigator": clean_text(cells[5].get_text(" ", strip=True)),
                "index_sample_size": clean_text(cells[6].get_text(" ", strip=True)),
                "index_state_province": clean_text(cells[7].get_text(" ", strip=True)),
                "index_country": clean_text(cells[8].get_text(" ", strip=True)),
                "landing_page_url": href or (f"{BASE_URL}/{grant_id}" if grant_id else None),
            }
        )
    return [r for r in rows if r.get("grant_id")]


def page_lines(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    return [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]


DETAIL_LABELS = {
    "Grant ID",
    "Illness",
    "Primary Drug/Intervention",
    "Primary Dosage",
    "Secondary Drug Intervention",
    "Secondary Dosage",
    "Other Drug/Intervention",
    "Other Dosage",
    "Status",
    "Investigator",
    "Sample Size",
    "Duration of Study Period for Each Subject",
    "Outcome Measurements",
    "Results",
    "Publication",
    "Link",
    "PI Name",
    "Degree",
    "Center",
    "Institution",
    "Address",
    "City or Town",
    "State or Province",
    "Zip or Postal Code",
    "Country",
    "Email Address",
}


def parse_detail(html: str) -> dict[str, Optional[str]]:
    lines = page_lines(html)
    out: dict[str, Optional[str]] = {}
    for i, line in enumerate(lines):
        if line in DETAIL_LABELS and i + 1 < len(lines):
            value = clean_text(lines[i + 1])
            out[line] = value
    return out


def grant_year(grant_id: Optional[str]) -> Optional[int]:
    text = clean_text(grant_id)
    if not text:
        return None
    m = re.match(r"^(\d{2})", text)
    if not m:
        return None
    yy = int(m.group(1))
    return 1900 + yy if yy >= 90 else 2000 + yy


def normalize_person_name(name: Optional[str]) -> Optional[str]:
    text = clean_text(name)
    if not text:
        return None
    text = PREFIX_RE.sub("", text)
    text = SUFFIX_RE.sub("", text).strip(" ,")
    return clean_text(text)


def split_people(name: Optional[str]) -> list[str]:
    text = clean_text(name)
    if not text:
        return []
    text = text.replace("Drs.", "Dr.")
    text = re.sub(r"\s+and\s+", ", ", text)
    text = re.sub(r"\s*,\s*(?=(?:Dr\.?|Prof\.?|Professor)\s+)", "||", text)
    parts = [normalize_person_name(p) for p in text.split("||")]
    return [p for p in parts if p]


def split_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    text = normalize_person_name(name)
    if not text:
        return None, None
    parts = [p for p in text.replace(",", " ").split() if p]
    while parts and re.fullmatch(r"(Ph\\.?D\\.?|M\\.?D\\.?)", parts[-1], re.I):
        parts.pop()
    if not parts:
        return None, None
    if len(parts) == 1:
        return None, parts[0]
    return " ".join(parts[:-1]), parts[-1]


def person_payload(person: str) -> dict[str, Optional[str]]:
    given, family = split_name(person)
    return {"name": person, "given_name": given, "family_name": family}


def normalize_country(value: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    country = clean_text(value)
    if not country:
        return None, None
    return country, COUNTRY_CODES.get(country)


def description_from_fields(record: dict[str, Optional[str]]) -> Optional[str]:
    bits = []
    for label, prefix in [
        ("Illness", "Illness"),
        ("Primary Drug/Intervention", "Primary drug/intervention"),
        ("Secondary Drug Intervention", "Secondary drug/intervention"),
        ("Other Drug/Intervention", "Other drug/intervention"),
        ("Status", "Status"),
        ("Sample Size", "Sample size"),
        ("Duration of Study Period for Each Subject", "Study-period duration"),
        ("Outcome Measurements", "Outcome measurements"),
        ("Results", "Results"),
        ("Publication", "Publication"),
        ("Link", "Link"),
    ]:
        value = clean_text(record.get(label))
        if value:
            bits.append(f"{prefix}: {value}")
    return "; ".join(bits) if bits else None


def normalize_records(index_rows: list[dict[str, Optional[str]]], output_dir: Path, skip_download: bool, limit: int | None) -> pd.DataFrame:
    if limit is not None:
        index_rows = index_rows[:limit]
    records = []
    details_dir = output_dir / DETAILS_DIR
    for idx, row in enumerate(index_rows, 1):
        grant_id = row["grant_id"]
        detail_url = row.get("landing_page_url") or f"{BASE_URL}/{grant_id}"
        detail_path = details_dir / f"{grant_id}.html"
        try:
            html = load_or_fetch(detail_url, detail_path, skip_download=skip_download)
            detail_found = bool(html.strip())
            detail = parse_detail(html) if detail_found else {}
        except FileNotFoundError as exc:
            log(f"  [WARN] {exc}; keeping index-table fields only for {grant_id}")
            detail_path.parent.mkdir(parents=True, exist_ok=True)
            detail_path.write_text("", encoding="utf-8")
            detail = {}
            detail_found = False
        illness = clean_text(detail.get("Illness")) or row.get("index_illness")
        primary_drug = clean_text(detail.get("Primary Drug/Intervention")) or row.get("index_primary_drug")
        status = clean_text(detail.get("Status")) or row.get("index_status")
        pi_raw = clean_text(detail.get("PI Name")) or clean_text(detail.get("Investigator")) or row.get("index_investigator")
        people = split_people(pi_raw)
        lead = person_payload(people[0]) if people else {"name": pi_raw, "given_name": None, "family_name": None}
        co_lead = person_payload(people[1]) if len(people) > 1 else None
        others = [person_payload(p) for p in people[2:]]
        country_raw, country_code = normalize_country(clean_text(detail.get("Country")) or row.get("index_country"))
        display_parts = [f"SMRI Treatment Trial {grant_id}"]
        if primary_drug and illness:
            display_parts.append(f"{primary_drug} for {illness}")
        elif illness:
            display_parts.append(illness)
        display_name = ": ".join(display_parts)
        records.append(
            {
                "funder_award_id": grant_id,
                "grant_id": grant_id,
                "display_name": display_name,
                "description": description_from_fields({**row, **detail}),
                "illness": illness,
                "primary_drug": primary_drug,
                "primary_dosage": clean_text(detail.get("Primary Dosage")),
                "secondary_drug": clean_text(detail.get("Secondary Drug Intervention")) or row.get("index_secondary_drug"),
                "secondary_dosage": clean_text(detail.get("Secondary Dosage")),
                "other_drug": clean_text(detail.get("Other Drug/Intervention")),
                "other_dosage": clean_text(detail.get("Other Dosage")),
                "status": status,
                "sample_size": clean_text(detail.get("Sample Size")) or row.get("index_sample_size"),
                "study_period_duration": clean_text(detail.get("Duration of Study Period for Each Subject")),
                "outcome_measurements": clean_text(detail.get("Outcome Measurements")),
                "results": clean_text(detail.get("Results")),
                "publication_raw": clean_text(detail.get("Publication")),
                "study_link_raw": clean_text(detail.get("Link")),
                "lead_name": lead["name"],
                "lead_given_name": lead["given_name"],
                "lead_family_name": lead["family_name"],
                "co_lead_name": co_lead["name"] if co_lead else None,
                "co_lead_given_name": co_lead["given_name"] if co_lead else None,
                "co_lead_family_name": co_lead["family_name"] if co_lead else None,
                "other_investigators": json_string(others),
                "pi_name_raw": pi_raw,
                "degree": clean_text(detail.get("Degree")),
                "center": clean_text(detail.get("Center")),
                "institution": clean_text(detail.get("Institution")),
                "address": clean_text(detail.get("Address")),
                "city": clean_text(detail.get("City or Town")),
                "state_province": clean_text(detail.get("State or Province")) or row.get("index_state_province"),
                "postal_code": clean_text(detail.get("Zip or Postal Code")),
                "country": country_raw,
                "country_code": country_code,
                "email": clean_text(detail.get("Email Address")),
                "award_year": grant_year(grant_id),
                "amount": None,
                "currency": None,
                "landing_page_url": detail_url,
                "source_index_url": INDEX_URL,
                "detail_page_found": str(detail_found),
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                "provenance": PROVENANCE,
                "funder_id": str(FUNDER_ID),
                "funder_display_name": FUNDER_DISPLAY_NAME,
            }
        )
        if idx % 50 == 0:
            log(f"  fetched + parsed {idx:,}/{len(index_rows):,} detail pages")
        time.sleep(0.05)

    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=["funder_award_id"], keep="first")
    return df.sort_values("funder_award_id", kind="stable").reset_index(drop=True)


def validate(df: pd.DataFrame, source_rows: int) -> None:
    n = len(df)
    print("\n" + "=" * 72)
    print("Local validation")
    print("=" * 72)
    print(f"  Source table rows: {source_rows:,}")
    print(f"  Award rows: {n:,}")
    if n == 0:
        raise RuntimeError("No award rows after normalization")

    def coverage(col: str) -> None:
        c = int(df[col].notna().sum())
        pct = 100.0 * c / n if n else 0.0
        print(f"  {col:<32} {c:>6,}/{n:<6,} ({pct:5.1f}%)")

    for col in [
        "funder_award_id",
        "display_name",
        "description",
        "illness",
        "primary_drug",
        "status",
        "lead_name",
        "lead_family_name",
        "institution",
        "country",
        "country_code",
        "sample_size",
        "outcome_measurements",
        "results",
        "award_year",
        "landing_page_url",
    ]:
        coverage(col)
    unique_ids = int(df["funder_award_id"].nunique())
    print(f"  unique funder_award_id        {unique_ids:>6,}/{n:<6,}")
    if unique_ids != n:
        raise RuntimeError("funder_award_id values are not unique")
    if n < EXPECTED_MIN_FULL_ROWS:
        print(f"  [WARN] row count {n:,} below expected full-run minimum {EXPECTED_MIN_FULL_ROWS:,}")

    years = pd.to_numeric(df["award_year"], errors="coerce").dropna()
    if len(years):
        print(f"  award_year range: {int(years.min())}-{int(years.max())}")
    print("\n  Status distribution:")
    print(df["status"].value_counts(dropna=False).head(20).to_string())
    print("\n  Illness distribution:")
    print(df["illness"].value_counts(dropna=False).head(20).to_string())
    print("\n  Country distribution:")
    print(df["country"].value_counts(dropna=False).head(20).to_string())


def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / OUTPUT_PARQUET_NAME
    # Runbook §1.2.5: force string dtype immediately before parquet write.
    df = df.astype("string")
    df.to_parquet(output_path, index=False)
    print(f"\n[OK] wrote {len(df):,} rows ({output_path.stat().st_size / 1024 / 1024:.2f} MB) to {output_path}")
    return output_path


def check_no_shrink(local_path: Path, allow_shrink: bool) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for S3 upload; use --skip-upload locally") from exc

    client = boto3.client("s3")
    new_rows = len(pd.read_parquet(local_path))
    print(f"\nRunbook §1.4 shrink check: s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except client.exceptions.ClientError:
        print("  No existing S3 parquet found; first upload is allowed.")
        return

    previous_path = local_path.with_suffix(".previous.parquet")
    client.download_file(S3_BUCKET, S3_KEY, str(previous_path))
    previous_rows = len(pd.read_parquet(previous_path))
    print(f"  Previous rows: {previous_rows:,}; new rows: {new_rows:,}")
    if new_rows < previous_rows and not allow_shrink:
        raise RuntimeError(
            f"Refusing to shrink {S3_KEY}: new {new_rows:,} < previous {previous_rows:,}. "
            "Use --allow-shrink only after confirming the source legitimately shrank."
        )


def upload_to_s3(local_path: Path, allow_shrink: bool) -> None:
    check_no_shrink(local_path, allow_shrink)
    import boto3

    print(f"\nUploading {local_path} -> s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(local_path), S3_BUCKET, S3_KEY)
    print("[OK] upload complete")


def main() -> None:
    parser = argparse.ArgumentParser(description="SMRI awarded treatment trials -> parquet -> S3")
    parser.add_argument("--limit", type=int, default=None, help="Smoke-test: keep only the first N trials")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--skip-download", action="store_true", help="Use cached HTML files in --output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Allow row-count shrink vs existing S3 parquet")
    args = parser.parse_args()

    print("=" * 72)
    print("SMRI awarded treatment trials -> S3 pipeline")
    print("=" * 72)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Provenance: {PROVENANCE}")
    print(f"  Source:     {INDEX_URL}")
    print(f"  Output dir: {args.output_dir}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    index_path = args.output_dir / INDEX_HTML
    try:
        index_html = load_or_fetch(INDEX_URL, index_path, skip_download=args.skip_download)
    except RuntimeError as exc:
        print(f"[ERROR] {exc}")
        sys.exit(2)

    index_rows = parse_index(index_html)
    print(f"[OK] parsed {len(index_rows):,} rows from awarded trials table")
    df = normalize_records(index_rows, args.output_dir, args.skip_download, args.limit)
    validate(df, source_rows=len(index_rows))
    output_path = write_parquet(df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading to S3.")
        print(f"Local parquet ready: {output_path}")
        return

    upload_to_s3(output_path, args.allow_shrink)


if __name__ == "__main__":
    main()
