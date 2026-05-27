#!/usr/bin/env python3
"""
Smithsonian Artist Research Fellowship (SARF) awards to S3.

Source authority
----------------
The Office of Academic Appointments and Internships publishes the official
Smithsonian Artist Research Fellowship recipient list at:

    https://fellowships.si.edu/SARFawards

The related program page documents the fellowship and stipend policy at:

    https://fellowships.si.edu/SARF

No third-party data is used. The recipient page is a static Drupal page with
one paragraph per fellowship recipient under year headings.

Amount handling
---------------
The program page says current SARF fellows receive a stipend of US$5,000 per
month, but also says exact dates, stipend amount, and allowances are specified
in each award letter. The recipient list does not publish each recipient's
tenure dates or award-letter amount. This pipeline therefore preserves the
current program stipend text in source columns and intentionally leaves
amount/currency NULL for OpenAlex, with a section 6.7 waiver documented in the
notebook and PR.

Output
------
    s3://openalex-ingest/awards/smithsonian_sarf/smithsonian_sarf_awards.parquet

Usage
-----
    python smithsonian_sarf_to_s3.py --skip-upload
    python smithsonian_sarf_to_s3.py --limit 10 --skip-upload
    python smithsonian_sarf_to_s3.py --skip-download --skip-upload
    python smithsonian_sarf_to_s3.py --allow-shrink

Requirements
------------
    pip install beautifulsoup4 pandas pyarrow requests boto3
"""

import argparse
import json
import re
import subprocess
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

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
        file, mode="r", buffering=-1, encoding=None, errors=None,
        newline=None, closefd=True, opener=None,
    ):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)

    _builtins_utf8.open = _open_utf8
# --- end shim ---


# =============================================================================
# Configuration
# =============================================================================

RECIPIENTS_URL = "https://fellowships.si.edu/SARFawards"
PROGRAM_URL = "https://fellowships.si.edu/SARF"

FUNDER_ID = 7230414656
FUNDER_DISPLAY_NAME = "Office of Fellowships, Smithsonian Institution"
PROVENANCE = "smithsonian_sarf"
FUNDER_SCHEME = "Smithsonian Artist Research Fellowship"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/smithsonian_sarf/smithsonian_sarf_awards.parquet"

USER_AGENT = "openalex-walden-smithsonian-sarf-ingest/1.0 (+https://openalex.org)"
REQUEST_DELAY_S = 0.2
EXPECTED_MIN_FULL_ROWS = 90

CURRENT_STIPEND_RAW = "Stipend: $5,000 per month"
AMOUNT_DECISION = (
    "Per-recipient amount/currency NULL: recipient page omits tenure dates and "
    "award-letter amounts; SARF program page publishes only current stipend policy."
)

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def clean_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip(" \t\r\n\u00a0")
    return text or None


def slugify(value: str, max_len: int = 80) -> str:
    ascii_text = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_text.lower()).strip("-")
    slug = re.sub(r"-+", "-", slug)
    return slug[:max_len].strip("-") or "unknown"


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def _http_get(url: str, timeout: int = 30) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
    elapsed = time.monotonic() - _last_request_t
    if elapsed < REQUEST_DELAY_S:
        time.sleep(REQUEST_DELAY_S - elapsed)
    response = _session.get(url, timeout=timeout)
    _last_request_t = time.monotonic()
    response.raise_for_status()
    return response


def split_source_name(source_name: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse SARF's source format, usually 'Family, Given'."""
    source_name = clean_text(source_name)
    if not source_name:
        return None, None, None
    if "," in source_name:
        family, given = [clean_text(part) for part in source_name.split(",", 1)]
        display = clean_text(f"{given} {family}") if given and family else source_name
        return given, family, display
    parts = source_name.split()
    if len(parts) == 1:
        return None, source_name, source_name
    return " ".join(parts[:-1]), parts[-1], source_name


def fallback_project_title(raw_text: str, source_name: str) -> Optional[str]:
    escaped_name = re.escape(source_name)
    match = re.match(rf"^{escaped_name}\s*,\s*(.*?)\s+at\s+", raw_text)
    if match:
        return clean_text(match.group(1).strip(' "\u201c\u201d.'))
    return None


def extract_host_and_advisors(raw_text: str, project_title: Optional[str], source_name: str) -> tuple[Optional[str], Optional[str]]:
    tail = raw_text
    if project_title and project_title in tail:
        tail = tail.split(project_title, 1)[1]
    elif source_name and source_name in tail:
        tail = tail.split(source_name, 1)[1]
    match = re.search(r"\bat\s+(.+?)(?:\s+with\s+(.+))?\.?$", tail, flags=re.IGNORECASE)
    if not match:
        return None, None
    host = clean_text(match.group(1).strip(' "\u201c\u201d,.'))
    advisors = clean_text(match.group(2).strip(' "\u201c\u201d,.')) if match.group(2) else None
    return host, advisors


def extract_program_stipend(program_html: str) -> Optional[str]:
    text = BeautifulSoup(program_html, "html.parser").get_text("\n", strip=True)
    match = re.search(r"Stipend\s*\n\s*(\$5,000 per month)", text, flags=re.IGNORECASE)
    if match:
        return f"Stipend: {match.group(1)}"
    if "$5,000 per month" in text:
        return CURRENT_STIPEND_RAW
    return None


def parse_recipients_page(html: str, limit: Optional[int] = None) -> list[dict[str, Optional[str]]]:
    soup = BeautifulSoup(html, "html.parser")
    content = soup.select_one(".text-block__content")
    if content is None:
        raise RuntimeError("Could not find SARF recipient content block")

    rows: list[dict[str, Optional[str]]] = []
    current_year: Optional[int] = None
    row_in_year = 0
    for child in content.children:
        if not isinstance(child, Tag):
            continue
        label = clean_text(child.get_text(" ", strip=True))
        if child.name == "h2" and label and re.fullmatch(r"(19|20)\d{2}", label):
            current_year = int(label)
            row_in_year = 0
            continue
        if child.name != "p" or current_year is None:
            continue

        raw_text = clean_text(child.get_text(" ", strip=True))
        name_tag = child.find("strong")
        source_name = clean_text(name_tag.get_text(" ", strip=True)) if name_tag else None
        if not raw_text or not source_name:
            continue

        title_tag = child.find("em")
        project_title = clean_text(title_tag.get_text(" ", strip=True)) if title_tag else None
        if not project_title:
            project_title = fallback_project_title(raw_text, source_name)

        host_unit, advisors = extract_host_and_advisors(raw_text, project_title, source_name)
        given_name, family_name, recipient_name = split_source_name(source_name)
        row_in_year += 1
        title_or_name = project_title or recipient_name or source_name
        display_name = f"{title_or_name} (Smithsonian Artist Research Fellowship)"
        award_slug = slugify(f"{current_year}-{source_name}-{project_title or row_in_year}")

        rows.append({
            "funder_award_id": f"sarf-{award_slug}",
            "source_year": str(current_year),
            "source_sequence": str(row_in_year),
            "display_name": display_name,
            "project_title": project_title,
            "description": raw_text,
            "recipient_name": recipient_name or source_name,
            "source_name": source_name,
            "given_name": given_name,
            "family_name": family_name,
            "host_unit": host_unit,
            "smithsonian_advisors": advisors,
            "amount": None,
            "currency": None,
            "current_program_stipend_raw": CURRENT_STIPEND_RAW,
            "amount_decision": AMOUNT_DECISION,
            "funder_scheme": FUNDER_SCHEME,
            "start_date": f"{current_year}-01-01",
            "end_date": f"{current_year}-12-31",
            "landing_page_url": RECIPIENTS_URL,
            "source_url": RECIPIENTS_URL,
            "program_url": PROGRAM_URL,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "declined": False,
        })
        if limit and len(rows) >= limit:
            break
    return rows


def smoke_test() -> None:
    log("Smoke test: fetch SARF recipient and program pages")
    recipients_response = _http_get(RECIPIENTS_URL)
    recipients = parse_recipients_page(recipients_response.text, limit=10)
    if not recipients:
        raise RuntimeError("SARF smoke parse returned zero rows")
    program_response = _http_get(PROGRAM_URL)
    stipend = extract_program_stipend(program_response.text)
    if stipend != CURRENT_STIPEND_RAW:
        log(f"  [WARN] expected stipend text not found exactly; parsed={stipend!r}")
    sample = recipients[0]
    log(
        "  sample: "
        f"year={sample.get('source_year')} recipient={sample.get('recipient_name')} "
        f"title={sample.get('project_title')} host={sample.get('host_unit')}"
    )
    required = ["source_year", "recipient_name", "display_name", "host_unit", "funder_award_id"]
    missing = [field for field in required if not sample.get(field)]
    if missing:
        raise RuntimeError(f"SARF smoke row missing required fields: {missing}")


def download_source(output_dir: Path, limit: Optional[int]) -> Path:
    log("Step 1: download official SARF pages")
    recipients_response = _http_get(RECIPIENTS_URL)
    program_response = _http_get(PROGRAM_URL)
    parsed_stipend = extract_program_stipend(program_response.text)
    if parsed_stipend:
        log(f"  program stipend text: {parsed_stipend}")
    records = parse_recipients_page(recipients_response.text, limit=limit)
    if limit:
        log(f"  [LIMIT] parsed first {len(records)} recipients")
    else:
        log(f"  parsed {len(records)} recipients")
    raw = {
        "source": {
            "recipients_url": RECIPIENTS_URL,
            "program_url": PROGRAM_URL,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "program_stipend_raw": parsed_stipend,
        },
        "records": records,
    }
    raw_path = output_dir / "smithsonian_sarf_raw.json"
    raw_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2))
    log(f"  cached source JSON to {raw_path}")
    return raw_path


def validate_dataframe(df: pd.DataFrame, full_run: bool) -> None:
    if df.empty:
        raise RuntimeError("No Smithsonian SARF recipient rows parsed")
    if df["funder_award_id"].duplicated().any():
        dupes = df.loc[df["funder_award_id"].duplicated(), "funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values: {dupes}")
    if full_run and len(df) < EXPECTED_MIN_FULL_ROWS:
        raise RuntimeError(
            f"Full SARF corpus unexpectedly small: {len(df)} rows; expected at least {EXPECTED_MIN_FULL_ROWS}"
        )

    checks = {
        "display_name": 1.00,
        "recipient_name": 1.00,
        "source_year": 1.00,
        "host_unit": 0.95,
        "landing_page_url": 1.00,
    }
    for column, threshold in checks.items():
        coverage = df[column].notna().mean()
        log(f"  coverage {column}: {coverage:.1%}")
        if coverage < threshold:
            raise RuntimeError(f"Coverage for {column} is {coverage:.1%}, below required {threshold:.0%}")


def build_dataframe(raw_path: Path, limit: Optional[int]) -> pd.DataFrame:
    log("Step 2: build staging dataframe")
    raw = json.loads(raw_path.read_text())
    records = raw.get("records", [])
    if limit:
        records = records[:limit]
    df = pd.DataFrame.from_records(records)
    validate_dataframe(df, full_run=(limit is None))

    log(f"  rows: {len(df)}")
    year_counts = df.groupby("source_year").size().sort_index()
    log("  rows by year:")
    for year, count in year_counts.items():
        log(f"    {year}: {count}")
    log(f"  project_title coverage: {df['project_title'].notna().mean():.1%}")
    log("  amount/currency: intentionally NULL; exact SARF recipient amounts are not published")
    return df


def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    log("Step 3: write parquet")
    parquet_path = output_dir / "smithsonian_sarf_awards.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    log(f"  wrote {len(df)} rows to {parquet_path} ({parquet_path.stat().st_size / 1024:.1f} KB)")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook section 1.4: refuse to overwrite S3 with a smaller corpus."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError("boto3 is required for upload; rerun with --skip-upload for local validation") from exc

    client = boto3.client("s3")
    log(f"  shrink-check against s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            log("    no existing object; first ingest")
            return True
        log(f"    [WARN] head_object failed with {code}; treating as first ingest")
        return True

    previous_path = output_dir / "_previous_smithsonian_sarf_awards.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(previous_path))
        previous_count = len(pd.read_parquet(previous_path))
    finally:
        previous_path.unlink(missing_ok=True)

    log(f"    previous count: {previous_count}; new count: {new_count}")
    if new_count < previous_count and not allow_shrink:
        log("[ERROR] refusing to shrink existing corpus; rerun with --allow-shrink after review")
        return False
    if new_count < previous_count:
        log("    [OVERRIDE] --allow-shrink permits smaller replacement")
    else:
        log("    [OK] new corpus is not smaller")
    return True


def upload_to_s3(parquet_path: Path, row_count: int, output_dir: Path, allow_shrink: bool) -> bool:
    log("Step 4: upload to S3")
    if not check_no_shrink(row_count, allow_shrink, output_dir):
        return False
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    try:
        subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
    except FileNotFoundError:
        log("[ERROR] aws CLI not found")
        return False
    except subprocess.CalledProcessError as exc:
        log(f"[ERROR] aws s3 cp failed with exit code {exc.returncode}")
        return False
    log(f"  uploaded to {s3_uri}")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Download Smithsonian SARF recipients and write staging parquet")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/smithsonian_sarf"))
    parser.add_argument("--skip-download", action="store_true", help="Reuse smithsonian_sarf_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--limit", type=int, default=None, help="Limit recipient rows for smoke testing")
    parser.add_argument("--allow-shrink", action="store_true", help="Override the runbook shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    log("Smithsonian SARF awards pipeline")
    log(f"  funder: {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    log(f"  provenance: {PROVENANCE}")
    log(f"  output dir: {args.output_dir.absolute()}")
    log(f"  S3 dest: s3://{S3_BUCKET}/{S3_KEY}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "smithsonian_sarf_raw.json"
        if not raw_path.exists():
            log(f"[ERROR] --skip-download set but {raw_path} does not exist")
            sys.exit(2)
        log(f"[SKIP] reusing {raw_path}")
    else:
        raw_path = download_source(args.output_dir, args.limit)

    df = build_dataframe(raw_path, args.limit)
    parquet_path = write_parquet(df, args.output_dir)

    if args.skip_upload:
        log("[SKIP] --skip-upload set; no S3 action taken")
        log(f"  admin upload command: aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")
    else:
        if not upload_to_s3(parquet_path, len(df), args.output_dir, args.allow_shrink):
            sys.exit(3)

    log("Pipeline complete")
    log("Next: admin runs notebooks/awards/CreateSmithsonianSARFAwards.ipynb in Databricks")


if __name__ == "__main__":
    main()
