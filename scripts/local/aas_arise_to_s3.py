#!/usr/bin/env python3
"""
African Academy of Sciences ARISE grantees -> S3 Data Pipeline
==============================================================

Downloads grantee/project records from the official African Research
Initiative for Scientific Excellence (ARISE) portal and writes a parquet file
for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party. ARISE is an African Academy of Sciences (AAS)
programme implemented by AAS with AU/EU partners. The public ARISE site exposes
the grantee list through its JSON API:

    https://arise-api.aasciences.app/api/grantees

The public grantee pages live at:

    https://arise.aasciences.app/grantees/{slug}

Amount handling
---------------
The official ARISE news page says the programme supports 47 research teams with
grants of up to EUR 500,000 per team. The grantee API and public grantee pages
do not publish exact per-team budgets. This script preserves the programme
ceiling in source columns but intentionally leaves OpenAlex amount/currency
NULL for a section 6.7 waiver in the notebook and PR.

Output
------
s3://openalex-ingest/awards/aas_arise/aas_arise_grantees.parquet

Usage
-----
    python aas_arise_to_s3.py --skip-upload
    python aas_arise_to_s3.py --limit 10 --skip-upload
    python aas_arise_to_s3.py --skip-download --skip-upload
    python aas_arise_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
import html
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

API_URL = "https://arise-api.aasciences.app/api/grantees"
APP_BASE_URL = "https://arise.aasciences.app"
IMAGE_BASE_URL = "https://arise-api.aasciences.app"
PROGRAM_URL = "https://arise.aasciences.app/"
AMOUNT_SOURCE_URL = (
    "https://arise.aasciences.app/news/"
    "major-contributions-announced-for-arise-programme-to-advance-scientific-excellence-in-africa"
)

FUNDER_ID = 4320327323
FUNDER_DISPLAY_NAME = "African Academy of Sciences"
PROVENANCE = "aas_arise_grantees"
FUNDER_SCHEME = "African Research Initiative for Scientific Excellence (ARISE)"

PROGRAM_CEILING_AMOUNT_EUR = "500000"
PROGRAM_CEILING_CURRENCY = "EUR"
PROGRAM_AMOUNT_RAW = "ARISE grants are up to EUR 500,000 per research team"
AMOUNT_DECISION = (
    "OpenAlex amount/currency intentionally NULL: ARISE publishes only a "
    "programme ceiling, not exact per-team awarded budgets."
)

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/aas_arise/aas_arise_grantees.parquet"

USER_AGENT = "openalex-walden-aas-arise-ingest/1.0 (+https://openalex.org)"
REQUEST_DELAY_S = 0.2
EXPECTED_MIN_FULL_ROWS = 45


_session: Optional[requests.Session] = None
_last_request_t = 0.0


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = html.unescape(str(value))
    text = re.sub(r"\s+", " ", text).strip(" \t\r\n\u00a0")
    return text or None


def slugify(value: str, max_len: int = 90) -> str:
    ascii_text = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_text.lower()).strip("-")
    slug = re.sub(r"-+", "-", slug)
    return slug[:max_len].strip("-") or "unknown"


def split_person_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    text = clean_text(name)
    if not text:
        return None, None
    text = re.sub(
        r"^(Dr\.?|Prof\.?|Professor|Mr\.?|Mrs\.?|Ms\.?)\s+",
        "",
        text,
        flags=re.IGNORECASE,
    )
    tokens = [t.strip(",") for t in text.split() if t.strip(",")]
    suffixes = {"PhD", "MD", "DPhil", "Jr.", "Sr.", "II", "III", "IV"}
    while tokens and tokens[-1].rstrip(".") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def absolute_url(base: str, value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    return urljoin(base, text)


def json_string(value: Any) -> Optional[str]:
    if value in (None, "", [], {}):
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/html,application/xhtml+xml,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        })
    return _session


def http_get(url: str, *, timeout: int = 60, max_attempts: int = 4) -> requests.Response:
    global _last_request_t
    session = get_session()
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < REQUEST_DELAY_S:
            time.sleep(REQUEST_DELAY_S - elapsed)
        try:
            response = session.get(url, timeout=timeout)
            _last_request_t = time.monotonic()
            log(f"GET {response.url} -> HTTP {response.status_code} ({len(response.content):,} bytes)")
            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 20)
                log(f"  retrying in {sleep_s}s after transient HTTP {response.status_code}")
                time.sleep(sleep_s)
                continue
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 20)
            log(f"  request failed ({exc}); retrying in {sleep_s}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def fetch_program_notes() -> dict[str, Optional[str]]:
    print("\n" + "=" * 60)
    print("Step 1a: Verify programme source pages")
    print("=" * 60)

    program_resp = http_get(PROGRAM_URL)
    amount_resp = http_get(AMOUNT_SOURCE_URL)
    program_text = clean_text(re.sub(r"<[^>]+>", " ", program_resp.text)) or ""
    amount_text = clean_text(re.sub(r"<[^>]+>", " ", amount_resp.text)) or ""

    if "African Research Initiative for Scientific Excellence" not in program_text:
        raise RuntimeError("ARISE programme page did not contain expected programme name")
    if "47 research teams" not in amount_text or "500,000" not in amount_text:
        raise RuntimeError("ARISE amount source page did not contain expected 47-team/EUR 500,000 note")

    log("Verified official programme and amount-note pages")
    return {
        "program_url": PROGRAM_URL,
        "amount_source_url": AMOUNT_SOURCE_URL,
        "program_amount_raw": PROGRAM_AMOUNT_RAW,
        "program_ceiling_amount_eur": PROGRAM_CEILING_AMOUNT_EUR,
        "program_ceiling_currency": PROGRAM_CEILING_CURRENCY,
        "amount_decision": AMOUNT_DECISION,
    }


def fetch_grantee_records(limit: Optional[int] = None) -> tuple[list[dict[str, Any]], dict[str, Optional[str]]]:
    print("\n" + "=" * 60)
    print("Step 1b: Download ARISE grantee records")
    print("=" * 60)

    notes = fetch_program_notes()
    response = http_get(API_URL)
    records = response.json()
    if not isinstance(records, list):
        raise RuntimeError(f"Expected list from ARISE API, got {type(records).__name__}")
    log(f"API returned {len(records):,} grantee records")

    if limit:
        records = records[:limit]
        log(f"[LIMIT] keeping first {len(records):,} records")
    if not records:
        raise RuntimeError("No grantee records returned from ARISE API")
    return records, notes


def normalize_records(
    records: list[dict[str, Any]],
    notes: dict[str, Optional[str]],
    *,
    full_run: bool,
) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Normalize records")
    print("=" * 60)

    retrieved_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Optional[str]]] = []

    for rec in records:
        slug = clean_text(rec.get("slug"))
        grantee_name = clean_text(rec.get("name"))
        research_title = clean_text(rec.get("research_title"))
        if not slug:
            slug = slugify(" ".join(x for x in [grantee_name, research_title] if x))
        if not slug:
            raise ValueError(f"Record missing usable slug/name/title: {rec!r}")

        given_name, family_name = split_person_name(grantee_name)
        registered_year = clean_text(rec.get("registered_year"))
        thematic_area = clean_text(rec.get("thematic_area"))
        cohort = clean_text(rec.get("cohort"))
        country = clean_text(rec.get("country"))
        institution = clean_text(rec.get("institution"))
        description_parts = [
            clean_text(rec.get("problem_statement")),
            clean_text(rec.get("progress_highlights")),
            clean_text(rec.get("key_findings")),
            clean_text(rec.get("potential_impact")),
        ]

        rows.append({
            "funder_award_id": f"aas-arise-{slug}",
            "source_slug": slug,
            "display_name": research_title,
            "description": "\n\n".join(part for part in description_parts if part) or None,
            "grantee_name": grantee_name,
            "lead_investigator_given_name": given_name,
            "lead_investigator_family_name": family_name,
            "institution": institution,
            "country": country,
            "cohort": cohort,
            "cohort_slug": clean_text(rec.get("cohort_slug")),
            "thematic_area": thematic_area,
            "discipline_slug": clean_text(rec.get("discipline_slug")),
            "registered_year": registered_year,
            "start_date": registered_year if registered_year else None,
            "start_year": registered_year[:4] if registered_year and re.match(r"^\d{4}", registered_year) else None,
            "landing_page_url": f"{APP_BASE_URL}/grantees/{slug}",
            "project_link": clean_text(rec.get("project_link")),
            "image_url": absolute_url(IMAGE_BASE_URL, rec.get("image")),
            "funder_scheme": " - ".join(
                part for part in [FUNDER_SCHEME, cohort, thematic_area] if part
            ),
            "funding_type": "grant",
            "amount": None,
            "currency": None,
            "program_amount_raw": notes.get("program_amount_raw"),
            "program_ceiling_amount_eur": notes.get("program_ceiling_amount_eur"),
            "program_ceiling_currency": notes.get("program_ceiling_currency"),
            "amount_decision": notes.get("amount_decision"),
            "program_url": notes.get("program_url"),
            "amount_source_url": notes.get("amount_source_url"),
            "problem_statement": clean_text(rec.get("problem_statement")),
            "progress_highlights": clean_text(rec.get("progress_highlights")),
            "key_findings": clean_text(rec.get("key_findings")),
            "potential_impact": clean_text(rec.get("potential_impact")),
            "raw_record_json": json_string(rec),
            "retrieved_at": retrieved_at,
        })

    df = pd.DataFrame(rows)
    validate_dataframe(df, full_run=full_run)

    # Runbook section 1.2: force Spark-friendly strings before parquet write.
    df = df.astype("string")
    return df


def validate_dataframe(df: pd.DataFrame, *, full_run: bool) -> None:
    total = len(df)
    if total == 0:
        raise RuntimeError("No ARISE rows after normalization")

    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")

    if full_run and total < EXPECTED_MIN_FULL_ROWS:
        raise RuntimeError(
            f"Full ARISE run returned only {total:,} rows; expected at least "
            f"{EXPECTED_MIN_FULL_ROWS:,}."
        )

    required_cols = [
        "display_name",
        "description",
        "grantee_name",
        "institution",
        "country",
        "thematic_area",
        "registered_year",
        "landing_page_url",
    ]
    for col in required_cols:
        coverage = df[col].notna().mean()
        print(f"  {col:24s}: {df[col].notna().sum():,}/{total:,} ({coverage * 100:.1f}%)")
        if coverage < 0.95:
            raise RuntimeError(f"Unexpectedly low coverage for {col}: {coverage * 100:.1f}%")

    amount_coverage = df["amount"].notna().mean()
    print(f"  {'amount':24s}: {df['amount'].notna().sum():,}/{total:,} ({amount_coverage * 100:.1f}%)")
    print(f"  {'currency':24s}: {df['currency'].notna().sum():,}/{total:,} ({df['currency'].notna().mean() * 100:.1f}%)")
    print("  Amount decision: per-team exact budgets are not published; preserving programme ceiling only.")
    print(f"  Year range: {df['start_year'].min()} - {df['start_year'].max()}")
    print(f"  Thematic distribution: {df['thematic_area'].value_counts(dropna=False).to_dict()}")
    print(f"  Cohort distribution: {df['cohort'].value_counts(dropna=False).to_dict()}")
    print(f"  Countries represented: {df['country'].nunique(dropna=True):,}")


def write_outputs(
    records: list[dict[str, Any]],
    notes: dict[str, Optional[str]],
    df: pd.DataFrame,
    output_dir: Path,
) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "aas_arise_grantees_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump({"program_notes": notes, "records": records}, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw API payload to {raw_path}")

    parquet_path = output_dir / "aas_arise_grantees.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df):,} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def load_cached_payload(output_dir: Path) -> tuple[list[dict[str, Any]], dict[str, Optional[str]]]:
    raw_path = output_dir / "aas_arise_grantees_raw.json"
    if not raw_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {raw_path}")
    with raw_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, list):
        records = payload
        notes = {
            "program_url": PROGRAM_URL,
            "amount_source_url": AMOUNT_SOURCE_URL,
            "program_amount_raw": PROGRAM_AMOUNT_RAW,
            "program_ceiling_amount_eur": PROGRAM_CEILING_AMOUNT_EUR,
            "program_ceiling_currency": PROGRAM_CEILING_CURRENCY,
            "amount_decision": AMOUNT_DECISION,
        }
    else:
        records = payload.get("records")
        notes = payload.get("program_notes") or {}
    if not isinstance(records, list):
        raise RuntimeError(f"Cached JSON should contain a records list: {raw_path}")
    print(f"  [OK] loaded {len(records):,} cached records from {raw_path}")
    return records, notes


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

    prev_path = output_dir / "_prev_aas_arise_grantees.parquet"
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
        print(f"\n[ERROR] Refusing to shrink AAS ARISE corpus ({prev_count:,} -> {new_count:,}).")
        return False
    print("    [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path, allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 4: Upload to S3")
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
        description="Download AAS ARISE grantee records and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/aas_arise"))
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for smoke testing")
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Reuse aas_arise_grantees_raw.json from output-dir",
    )
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("AAS ARISE grantees -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  API:        {API_URL}")
    print(f"  Programme:  {PROGRAM_URL}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        print("\nStep 1: Reuse cached raw JSON")
        records, notes = load_cached_payload(args.output_dir)
        if args.limit:
            records = records[:args.limit]
            print(f"  [LIMIT] keeping first {len(records):,} cached records")
    else:
        records, notes = fetch_grantee_records(limit=args.limit)

    df = normalize_records(records, notes, full_run=args.limit is None)
    parquet_path = write_outputs(records, notes, df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading to S3.")
        print(f"Local parquet ready: {parquet_path}")
        return

    ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
