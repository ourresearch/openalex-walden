#!/usr/bin/env python3
"""
SEPM science awards -> S3 Data Pipeline
=======================================

Downloads Society for Sedimentary Geology (SEPM) official science-award
recipient pages and writes a parquet file for the OpenAlex awards pipeline.

Source authority
----------------
The sources are first-party:

    https://www.sepm.org/Past-Winners
    https://www.sepm.org/2024awardees
    https://www.sepm.org/2026awardees

The past-winners page publishes historical SEPM science award and honor
recipients through 2023. The 2024 and 2026 awardee pages add current award
cohorts not yet folded into the past-winners page.

Output
------
s3://openalex-ingest/awards/sepm/sepm_science_awards.parquet

Usage
-----
    python sepm_science_awards_to_s3.py --skip-upload
    python sepm_science_awards_to_s3.py --limit 10 --skip-upload
    python sepm_science_awards_to_s3.py --skip-download --skip-upload
    python sepm_science_awards_to_s3.py --allow-shrink

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


FUNDER_ID = 4320312534
FUNDER_DISPLAY_NAME = "Society for Sedimentary Geology"
PROVENANCE = "sepm_science_awards"

PAST_WINNERS_URL = "https://www.sepm.org/Past-Winners"
AWARDEES_2024_URL = "https://www.sepm.org/2024awardees"
AWARDEES_CURRENT_URL = "https://www.sepm.org/2026awardees"

SOURCE_URLS = [PAST_WINNERS_URL, AWARDEES_2024_URL, AWARDEES_CURRENT_URL]

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/sepm/sepm_science_awards.parquet"

USER_AGENT = "openalex-walden-sepm-awards-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25
MIN_EXPECTED_FULL_ROWS = 350

PAST_SECTION_MAP = {
    "TWENHOFEL MEDALISTS": "William H. Twenhofel Medal",
    "DISTINGUISHED SERVICE AWARDS": "SEPM Distinguished Service Award",
    "HONORARY MEMBERS": "SEPM Honorary Membership",
    "MOORE MEDALISTS": "Raymond C. Moore Medal",
    "SHEPARD MEDALISTS": "Francis P. Shepard Medal",
    "PETTIJOHN MEDALISTS": "Francis P. Pettijohn Medal",
    "William R. Dickinson Medal": "William R. Dickinson Medal",
    "JAMES LEE WILSON AWARD": "James Lee Wilson Award",
}

AWARD_LABEL_MAP = {
    "twenhofel": "William H. Twenhofel Medal",
    "shepard": "Francis P. Shepard Medal",
    "pettijohn": "Francis P. Pettijohn Medal",
    "wilson": "James Lee Wilson Award",
    "moore": "Raymond C. Moore Medal",
    "dickinson": "William R. Dickinson Medal",
    "honorary membership": "SEPM Honorary Membership",
}

COUNTRY_NORMALIZATION = {
    "canada": "CA",
    "usa": "US",
    "united states": "US",
}

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
    return _session


def polite_get(url: str, timeout: int = 45, max_attempts: int = 4) -> requests.Response:
    global _last_request_t
    session = get_session()
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < MIN_REQUEST_INTERVAL_S:
            time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
        try:
            print(f"  GET {url}")
            resp = session.get(url, timeout=timeout)
            _last_request_t = time.monotonic()
            print(f"    -> HTTP {resp.status_code}, {len(resp.content):,} bytes")
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 20)
                print(f"  [WARN] HTTP {resp.status_code}; retrying in {sleep_s}s")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 20)
            print(f"  [WARN] request failed ({exc}); retrying in {sleep_s}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def slugify(value: Optional[str], max_len: int = 60) -> str:
    text = clean_text(value) or "unknown"
    text = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return (text or "unknown")[:max_len].strip("-") or "unknown"


def short_hash(*values: Any) -> str:
    joined = "|".join("" if value is None else str(value) for value in values)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()[:10]


def normalize_name(value: Optional[str]) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    text = re.sub(r"^(Dr|Prof|Professor)\.?\s+", "", text, flags=re.I)
    text = re.sub(r"\s*,\s*[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\s*$", "", text)
    return clean_text(text)


def clean_affiliation(value: Optional[str]) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    text = re.sub(r"\s*,?\s*[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\s*$", "", text)
    return clean_text(text)


def split_name(full_name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    name = normalize_name(full_name)
    if not name:
        return None, None
    suffix_pattern = re.compile(
        r",?\s+(Ph\.?D\.?|M\.?D\.?|DPhil|Jr\.?|Sr\.?|II|III|IV)$",
        flags=re.I,
    )
    while suffix_pattern.search(name):
        name = suffix_pattern.sub("", name).strip()
    parts = name.split()
    if len(parts) == 1:
        return parts[0], None
    return " ".join(parts[:-1]), parts[-1]


def award_from_label(label: Optional[str]) -> Optional[str]:
    text = (clean_text(label) or "").lower()
    for key, award_name in AWARD_LABEL_MAP.items():
        if key in text:
            return award_name
    return None


def normalize_country_from_affiliation(value: Optional[str]) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    lower = text.lower()
    for key, country in COUNTRY_NORMALIZATION.items():
        if re.search(rf"\b{re.escape(key)}\b", lower):
            return country
    return None


def make_record(
    *,
    award_year: str,
    award_name: str,
    recipient_name: str,
    source_url: str,
    downloaded_at: str,
    source_record_type: str,
    affiliation_name: Optional[str] = None,
    email: Optional[str] = None,
    source_note: Optional[str] = None,
) -> dict[str, Any]:
    normalized_name = normalize_name(recipient_name)
    if not normalized_name:
        raise RuntimeError(f"Missing recipient name for {award_name} {award_year} from {source_url}")
    given_name, family_name = split_name(normalized_name)
    award_id = (
        f"sepm-{award_year}-{slugify(award_name, 42)}-"
        f"{slugify(normalized_name, 42)}-{short_hash(award_year, award_name, normalized_name)}"
    )
    description = source_note or f"{award_name} recipient."
    return {
        "funder_award_id": award_id,
        "display_name": f"{award_name} ({award_year}) - {normalized_name}",
        "description": description,
        "recipient_name": normalized_name,
        "recipient_given_name": given_name,
        "recipient_family_name": family_name,
        "affiliation_name": clean_affiliation(affiliation_name),
        "affiliation_country": normalize_country_from_affiliation(affiliation_name),
        "email": clean_text(email),
        "award_name": award_name,
        "award_year": award_year,
        "amount": None,
        "currency": None,
        "source_record_type": source_record_type,
        "source_url": source_url,
        "landing_page_url": source_url,
        "provenance": PROVENANCE,
        "downloaded_at": downloaded_at,
    }


def fetch_source_html() -> dict[str, str]:
    print("\n" + "=" * 60)
    print("Step 1: Download official SEPM award pages")
    print("=" * 60)
    html_by_url = {}
    for url in SOURCE_URLS:
        resp = polite_get(url)
        if "SEPM" not in resp.text:
            raise RuntimeError(f"Unexpected page body for {url}")
        html_by_url[url] = resp.text
    return html_by_url


def parse_records(html_by_url: dict[str, str], limit: Optional[int]) -> list[dict[str, Any]]:
    print("\n" + "=" * 60)
    print("Step 2: Parse SEPM science award recipients")
    print("=" * 60)
    downloaded_at = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    records.extend(parse_past_winners(html_by_url[PAST_WINNERS_URL], downloaded_at))
    records.extend(parse_2024_awardees(html_by_url[AWARDEES_2024_URL], downloaded_at))
    records.extend(parse_current_awardees(html_by_url[AWARDEES_CURRENT_URL], downloaded_at))

    # De-dupe exact same award/year/name if a current page is later folded into Past-Winners.
    deduped = {}
    for rec in records:
        key = (rec["award_year"], rec["award_name"], rec["recipient_name"])
        deduped.setdefault(key, rec)
    records = sorted(deduped.values(), key=lambda r: (r["award_year"], r["award_name"], r["recipient_name"]))
    if limit is not None:
        records = records[:limit]
        print(f"  [LIMIT] keeping first {len(records):,} rows")
    print(f"  [OK] parsed {len(records):,} SEPM science award recipient rows")
    return records


def parse_past_winners(html: str, downloaded_at: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    records: list[dict[str, Any]] = []
    current_award: Optional[str] = None
    for tag in soup.find_all(["h2", "p"]):
        text = clean_text(tag.get_text(" ", strip=True))
        if not text:
            continue
        if tag.name == "h2":
            current_award = PAST_SECTION_MAP.get(text)
            continue
        if not current_award or text.lower().startswith("year -"):
            continue
        for year, name in re.findall(r"\b((?:19|20)\d{2})\s*-\s*(.*?)(?=\s+(?:19|20)\d{2}\s*-|$)", text):
            recipient = normalize_name(name)
            if recipient:
                records.append(make_record(
                    award_year=year,
                    award_name=current_award,
                    recipient_name=recipient,
                    source_url=PAST_WINNERS_URL,
                    downloaded_at=downloaded_at,
                    source_record_type="past_winners_page",
                ))
    print(f"  Past winners page: {len(records):,} rows")
    return records


def parse_2024_awardees(html: str, downloaded_at: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    paragraphs = [p for p in soup.find_all("p") if clean_text(p.get_text(" ", strip=True))]
    records: list[dict[str, Any]] = []
    for idx, p in enumerate(paragraphs):
        label = clean_text(p.get_text(" ", strip=True))
        award_name = award_from_label(label)
        if not award_name:
            continue
        name = None
        affiliation = None
        email = None
        for lookahead in paragraphs[idx + 1:idx + 5]:
            text = clean_text(lookahead.get_text(" ", strip=True))
            if not text:
                continue
            if award_from_label(text):
                break
            if name is None and lookahead.find(["strong", "em"]):
                name = normalize_name(lookahead.get_text("", strip=True))
                continue
            if name and affiliation is None:
                affiliation = text
                mail = lookahead.find("a", href=re.compile(r"^mailto:", re.I))
                if mail:
                    email = re.sub(r"^mailto:", "", mail.get("href", ""), flags=re.I)
                break
        if name:
            records.append(make_record(
                award_year="2024",
                award_name=award_name,
                recipient_name=name,
                affiliation_name=affiliation,
                email=email,
                source_url=AWARDEES_2024_URL,
                downloaded_at=downloaded_at,
                source_record_type="current_awardee_page",
                source_note=label,
            ))
    print(f"  2024 awardees page: {len(records):,} rows")
    return records


def parse_current_awardees(html: str, downloaded_at: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    heading_text = " ".join(
        clean_text(tag.get_text(" ", strip=True)) or ""
        for tag in soup.find_all(["h1", "h2"])
    )
    year_match = re.search(r"SEPM\s+((?:19|20)\d{2})\s+Awardees", heading_text)
    award_year = year_match.group(1) if year_match else "2026"
    records: list[dict[str, Any]] = []
    for li in soup.find_all("li"):
        text = clean_text(li.get_text(" ", strip=True))
        if not text or "not awarded" in text.lower():
            continue
        award_name = award_from_label(text)
        if not award_name or ":" not in text:
            continue
        after_colon = text.split(":", 1)[1]
        recipient = re.split(r",\s*[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", after_colon)[0]
        mail = li.find("a", href=re.compile(r"^mailto:", re.I))
        email = re.sub(r"^mailto:", "", mail.get("href", ""), flags=re.I) if mail else None
        records.append(make_record(
            award_year=award_year,
            award_name=award_name,
            recipient_name=recipient,
            email=email,
            source_url=AWARDEES_CURRENT_URL,
            downloaded_at=downloaded_at,
            source_record_type="current_awardee_page",
        ))
    print(f"  Current awardees page ({award_year}): {len(records):,} rows")
    return records


def load_cached_records(output_dir: Path) -> list[dict[str, Any]]:
    raw_path = output_dir / "sepm_science_awards_raw.json"
    if not raw_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {raw_path}")
    with raw_path.open("r", encoding="utf-8") as f:
        records = json.load(f)
    if not isinstance(records, list):
        raise RuntimeError(f"Cached JSON should be a list of records: {raw_path}")
    print(f"  [OK] loaded {len(records):,} cached records from {raw_path}")
    return records


def normalize_records(records: list[dict[str, Any]], *, full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 3: Normalize and validate")
    print("=" * 60)
    df = pd.DataFrame(records)

    expected_cols = [
        "funder_award_id",
        "display_name",
        "description",
        "recipient_name",
        "recipient_given_name",
        "recipient_family_name",
        "affiliation_name",
        "affiliation_country",
        "email",
        "award_name",
        "award_year",
        "amount",
        "currency",
        "source_record_type",
        "source_url",
        "landing_page_url",
        "provenance",
        "downloaded_at",
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    df = df[expected_cols]

    validate_dataframe(df, full_run=full_run)

    # Prevent pyarrow type drift on sparse nullable fields.
    df = df.astype("string")
    return df


def validate_dataframe(df: pd.DataFrame, *, full_run: bool) -> None:
    total = len(df)
    print(f"  Rows: {total:,}")
    if total == 0:
        raise RuntimeError("No SEPM award rows parsed")

    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")
    print("  funder_award_id duplicates: 0")

    required_thresholds = {
        "funder_award_id": 1.00,
        "display_name": 1.00,
        "recipient_name": 1.00,
        "recipient_given_name": 0.98,
        "recipient_family_name": 0.98,
        "award_name": 1.00,
        "award_year": 1.00,
        "landing_page_url": 1.00,
    }
    for col, min_coverage in required_thresholds.items():
        non_null = df[col].notna().sum()
        coverage = non_null / total if total else 0
        print(f"  {col:24s}: {non_null:,}/{total:,} ({coverage * 100:.1f}%)")
        if coverage < min_coverage:
            raise RuntimeError(f"Unexpectedly low coverage for {col}: {coverage * 100:.1f}%")

    if df["amount"].notna().any() or df["currency"].notna().any():
        raise RuntimeError("SEPM prize-pattern rows should not populate amount/currency")

    if full_run and total < MIN_EXPECTED_FULL_ROWS:
        raise RuntimeError(
            f"Full SEPM run returned only {total:,} rows; expected at least "
            f"{MIN_EXPECTED_FULL_ROWS:,} from official science-award pages."
        )

    years = pd.to_numeric(df["award_year"], errors="coerce")
    print(f"  Award year range: {int(years.min())} - {int(years.max())}")
    print(f"  Source type distribution: {df['source_record_type'].value_counts(dropna=False).to_dict()}")
    print(f"  Award distribution: {df['award_name'].value_counts(dropna=False).to_dict()}")
    print(f"  Year distribution: {df['award_year'].value_counts(dropna=False).sort_index().to_dict()}")
    print(f"  Affiliation coverage: {df['affiliation_name'].notna().sum():,}/{total:,}")
    print("  Amount/currency: NULL by prize-pattern waiver")


def write_outputs(records: list[dict[str, Any]], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 4: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "sepm_science_awards_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw parsed records to {raw_path}")

    parquet_path = output_dir / "sepm_science_awards.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df):,} rows ({sz_kb:.1f} KB) to {parquet_path}")
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
    print(f"  Re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("    no existing parquet: first ingest, no shrink check needed.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest.")
        return True

    prev_path = output_dir / "_prev_sepm_science_awards.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as exc:
        print(f"    [ERROR] could not read existing parquet ({exc}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)

    print(f"    previous count: {prev_count:,}   new count: {new_count:,}")
    if new_count < prev_count:
        if allow_shrink:
            print("    [OVERRIDE] new corpus is smaller but --allow-shrink was set.")
            return True
        print(
            f"\n[ERROR] Refusing to shrink SEPM awards corpus "
            f"({prev_count:,} -> {new_count:,}). Investigate before upload."
        )
        return False
    print("    [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path,
                 allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 5: Upload to S3")
    print("=" * 60)
    if not check_no_shrink(len(df), allow_shrink, output_dir):
        return False

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  Uploading {parquet_path} -> {s3_uri}")
    try:
        subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
        print(f"  [OK] uploaded to {s3_uri}")
        return True
    except FileNotFoundError:
        print("[ERROR] aws CLI not found.")
        return False
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] aws s3 cp failed (exit {exc.returncode}).")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download SEPM science award recipients and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/sepm_science_awards"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit award rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse sepm_science_awards_raw.json from output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("SEPM science awards -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Provenance: {PROVENANCE}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        print("\nStep 1: Reuse cached raw JSON")
        records = load_cached_records(args.output_dir)
        if args.limit:
            records = records[:args.limit]
            print(f"  [LIMIT] keeping first {len(records):,} cached records")
    else:
        html_by_url = fetch_source_html()
        records = parse_records(html_by_url, limit=args.limit)

    df = normalize_records(records, full_run=args.limit is None)
    parquet_path = write_outputs(records, df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading to S3.")
        print(f"Local parquet ready: {parquet_path}")
        return

    ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
