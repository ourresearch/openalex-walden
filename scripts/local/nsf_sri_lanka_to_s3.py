#!/usr/bin/env python3
"""
National Science Foundation of Sri Lanka Research & Technology Grants -> S3
=========================================================================

Downloads public Research & Technology Grant detail pages from the official
National Science Foundation (NSF) of Sri Lanka Grant Management Information
System (GMIS) and writes a parquet file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party:

    https://gmis.nsf.gov.lk/
    https://gmis.nsf.gov.lk/homedashboard
    https://gmis.nsf.gov.lk/rtgrainfodetailView/{numeric_id}

The GMIS homepage links users to a Research & Technology Grant search form,
and the dashboard publishes official awarded-grant counts. The public detail
route exposes one grant per page with a native NSF grant number, title,
principal investigator text, subject, and optional keywords. Broad blank
searches are slow from this environment, so this downloader deterministically
enumerates the public detail route and validates the resulting count against
the dashboard-scale corpus.

Amount/currency note
--------------------
GMIS detail pages do not publish per-grant award amounts or currencies, so
amount and currency are intentionally NULL under the runbook's amount-waiver
rule. The script preserves grant numbers, subjects, keywords, and source URLs
for audit.

Output
------
s3://openalex-ingest/awards/nsf_sri_lanka/nsf_sri_lanka_grants.parquet

Usage
-----
    python nsf_sri_lanka_to_s3.py --skip-upload
    python nsf_sri_lanka_to_s3.py --limit 10 --skip-upload
    python nsf_sri_lanka_to_s3.py --skip-download --skip-upload
    python nsf_sri_lanka_to_s3.py --allow-shrink

Requirements
------------
    pip install beautifulsoup4 pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
import concurrent.futures
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
import urllib3
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
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open_utf8
# --- end shim ---


# =============================================================================
# Configuration
# =============================================================================

BASE_URL = "https://gmis.nsf.gov.lk"
HOME_URL = f"{BASE_URL}/"
DASHBOARD_URL = f"{BASE_URL}/homedashboard"
DETAIL_URL_TEMPLATE = f"{BASE_URL}/rtgrainfodetailView/{{detail_id}}"

DEFAULT_MAX_DETAIL_ID = 2300
CHUNK_SIZE = 200
MAX_WORKERS = 16

FUNDER_ID = 4320335353
FUNDER_DISPLAY_NAME = "National Science Foundation of Sri Lanka"
PROVENANCE = "nsf_sri_lanka_gmis"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/nsf_sri_lanka/nsf_sri_lanka_grants.parquet"

USER_AGENT = "openalex-walden-nsf-sri-lanka-ingest/1.0 (+https://openalex.org)"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def slugify(text: str, max_len: int = 90) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return (text[:max_len].strip("-") or "grant")


def polite_get(url: str, *, timeout: int = 30, max_attempts: int = 3) -> requests.Response:
    last_exc: Optional[Exception] = None
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout, verify=False)
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 15)
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 15)
            time.sleep(sleep_s)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def fetch_dashboard_counts() -> dict[str, Optional[int]]:
    print("  Fetching GMIS dashboard counts")
    html = polite_get(DASHBOARD_URL).text
    counts: dict[str, Optional[int]] = {
        "research_grant_count": None,
        "technology_grant_count": None,
        "dashboard_awarded_total": None,
    }
    match = re.search(
        r"labels:\s*\['Research Grant',\s*'Technology Grant'\].*?"
        r"data:\s*\[\s*(\d+),\s*(\d+)",
        html,
        flags=re.S,
    )
    if match:
        research = int(match.group(1))
        technology = int(match.group(2))
        counts["research_grant_count"] = research
        counts["technology_grant_count"] = technology
        counts["dashboard_awarded_total"] = research + technology
        print(f"  Dashboard reports {research:,} research + {technology:,} technology grants")
    else:
        print("  [WARN] Could not parse dashboard grant count chart")
    return counts


def infer_year_from_grant_number(grant_number: Optional[str]) -> Optional[str]:
    text = clean_text(grant_number)
    if not text:
        return None
    match = re.search(r"/(\d{4}|\d{2})(?:/|$)", text)
    if not match:
        return None
    year = int(match.group(1))
    if year < 100:
        # GMIS has legacy grants such as RGB/81/026 and RG/83/035; two-digit
        # years up to the current era are treated as 2000s, older as 1900s.
        year = 2000 + year if year <= 26 else 1900 + year
    return str(year)


def first_investigator_name(principal_investigator_text: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    text = clean_text(principal_investigator_text)
    if not text:
        return None, None, None
    first = clean_text(text.split(";")[0])
    if not first:
        return None, None, None
    name = first
    for marker in [
        ", Department", ", Dept.", ", Faculty", ", Institute", ", Senior Lecturer",
        ", Professor", ", University", ", National", ", Veterinary",
    ]:
        idx = name.lower().find(marker.lower())
        if idx > 0:
            name = name[:idx]
            break
    name = clean_text(re.sub(r"^(Dr\.?|Prof\.?|Professor|Mr\.?|Mrs\.?|Ms\.?)\s*", "", name))
    if not name:
        return None, None, first
    tokens = [t.strip(",") for t in name.split() if t.strip(",")]
    if len(tokens) >= 2 and not any("," in t for t in tokens):
        return " ".join(tokens[:-1]), tokens[-1], first
    return None, name, first


def text_for_label(soup: BeautifulSoup, label: str) -> Optional[str]:
    label_tag = soup.find(
        ["h4", "h5", "h6"],
        string=lambda s: bool(s and clean_text(s).lower().startswith(label.lower())),
    )
    if not label_tag:
        return None
    node = label_tag.find_next_sibling()
    chunks: list[str] = []
    while node is not None:
        if getattr(node, "name", None) in {"h2", "h3", "h4", "h5", "h6"}:
            break
        if getattr(node, "name", None) in {"p", "ul", "ol", "div"}:
            text = clean_text(node.get_text(" ", strip=True))
            if text:
                chunks.append(text)
        node = node.find_next_sibling()
    return clean_text(" ".join(chunks))


def parse_detail_page(detail_id: int, html: str) -> Optional[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    heading = soup.find(
        "h2",
        string=lambda s: bool(s and "Research & Technology Grants" in s),
    )
    if not heading:
        return None

    grant_number_tag = heading.find_next("p")
    title_tag = grant_number_tag.find_next("h2") if grant_number_tag else None
    pi_tag = title_tag.find_next("p") if title_tag else None
    grant_number = clean_text(grant_number_tag.get_text(" ", strip=True)) if grant_number_tag else None
    title = clean_text(title_tag.get_text(" ", strip=True)) if title_tag else None
    if not grant_number or not title or not re.search(r"\d", grant_number):
        return None

    principal_investigators = clean_text(pi_tag.get_text(" ", strip=True)) if pi_tag else None
    given_name, family_name, first_investigator_raw = first_investigator_name(principal_investigators)
    year = infer_year_from_grant_number(grant_number)
    landing_page_url = DETAIL_URL_TEMPLATE.format(detail_id=detail_id)
    # Some legacy GMIS pages reuse a native grant number across multiple public
    # detail records. Include the public detail ID to keep the OpenAlex native
    # key stable and unique while preserving the official grant number.
    funder_award_id = f"nsf-sri-lanka-{detail_id}-{slugify(grant_number)}"

    return {
        "detail_id": str(detail_id),
        "funder_award_id": funder_award_id,
        "grant_number": grant_number,
        "display_name": title,
        "principal_investigators": principal_investigators,
        "lead_investigator_raw": first_investigator_raw,
        "lead_investigator_given_name": given_name,
        "lead_investigator_family_name": family_name,
        "subject": text_for_label(soup, "Subject"),
        "keywords": text_for_label(soup, "Keywords"),
        "collaborators": text_for_label(soup, "Collaborators"),
        "abstract": text_for_label(soup, "Abstract"),
        "key_research_findings": text_for_label(soup, "Key Research Findings"),
        "source_year": year,
        "start_date": f"{year}-01-01" if year else None,
        "end_date": f"{year}-12-31" if year else None,
        "amount": None,
        "currency": None,
        "landing_page_url": landing_page_url,
        "source_url": landing_page_url,
    }


def fetch_one_detail(detail_id: int) -> Optional[dict[str, Any]]:
    url = DETAIL_URL_TEMPLATE.format(detail_id=detail_id)
    try:
        html = polite_get(url, timeout=20, max_attempts=2).text
    except RuntimeError:
        return None
    return parse_detail_page(detail_id, html)


def fetch_grant_records(limit: Optional[int], max_detail_id: int) -> list[dict[str, Any]]:
    print("\n" + "=" * 60)
    print("Step 1: Download NSF Sri Lanka GMIS grant detail pages")
    print("=" * 60)
    print(f"  Detail route: {DETAIL_URL_TEMPLATE}")
    print(f"  Enumerating IDs 1..{max_detail_id:,}")
    if limit:
        print(f"  [LIMIT] stopping after {limit:,} valid grant pages")

    records: list[dict[str, Any]] = []
    for start in range(1, max_detail_id + 1, CHUNK_SIZE):
        stop = min(start + CHUNK_SIZE - 1, max_detail_id)
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(fetch_one_detail, detail_id) for detail_id in range(start, stop + 1)]
            chunk_records = [future.result() for future in concurrent.futures.as_completed(futures)]
        chunk_valid = [record for record in chunk_records if record]
        chunk_valid.sort(key=lambda record: int(record["detail_id"]))
        records.extend(chunk_valid)
        print(f"  IDs {start:,}-{stop:,}: {len(chunk_valid):,} valid grants (running {len(records):,})")
        if limit and len(records) >= limit:
            records = records[:limit]
            break

    if not records:
        raise RuntimeError("No GMIS Research & Technology Grant detail pages parsed")
    return records


def normalize_records(records: list[dict[str, Any]], dashboard_counts: dict[str, Optional[int]],
                      *, full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Normalize and validate records")
    print("=" * 60)
    retrieved_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    for record in records:
        row = dict(record)
        row.update({
            "funder_id": str(FUNDER_ID),
            "funder_display_name": FUNDER_DISPLAY_NAME,
            "provenance": PROVENANCE,
            "source_authority": "National Science Foundation of Sri Lanka GMIS",
            "dashboard_research_grant_count": (
                str(dashboard_counts["research_grant_count"])
                if dashboard_counts.get("research_grant_count") is not None else None
            ),
            "dashboard_technology_grant_count": (
                str(dashboard_counts["technology_grant_count"])
                if dashboard_counts.get("technology_grant_count") is not None else None
            ),
            "dashboard_awarded_total": (
                str(dashboard_counts["dashboard_awarded_total"])
                if dashboard_counts.get("dashboard_awarded_total") is not None else None
            ),
            "retrieved_at": retrieved_at,
        })
        rows.append(row)

    df = pd.DataFrame(rows)
    validate_dataframe(df, dashboard_counts=dashboard_counts, full_run=full_run)
    df = df.astype("string")
    return df


def validate_dataframe(df: pd.DataFrame, *, dashboard_counts: dict[str, Optional[int]],
                       full_run: bool) -> None:
    total = len(df)
    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")

    required_cols = ["grant_number", "display_name", "principal_investigators", "landing_page_url"]
    for col in required_cols:
        count = int(df[col].notna().sum())
        coverage = count / total if total else 0
        print(f"  {col:26s}: {count:,}/{total:,} ({coverage * 100:.1f}%)")
        if coverage < 0.95:
            raise RuntimeError(f"Unexpectedly low coverage for {col}: {coverage * 100:.1f}%")

    year_count = int(df["source_year"].notna().sum())
    print(f"  {'source_year':26s}: {year_count:,}/{total:,} ({(year_count / total if total else 0) * 100:.1f}%)")
    print(f"  {'subject':26s}: {int(df['subject'].notna().sum()):,}/{total:,} ({df['subject'].notna().mean() * 100:.1f}%)")
    print(f"  {'keywords':26s}: {int(df['keywords'].notna().sum()):,}/{total:,} ({df['keywords'].notna().mean() * 100:.1f}%)")
    print(f"  {'amount':26s}: {int(df['amount'].notna().sum()):,}/{total:,} (0.0%; source does not publish amounts)")
    print(f"  Distinct official grant numbers: {df['grant_number'].nunique():,}")

    if full_run and total < 2000:
        raise RuntimeError(f"Full GMIS run returned only {total:,} rows; expected at least 2,000.")
    if full_run and dashboard_counts.get("dashboard_awarded_total"):
        expected = int(dashboard_counts["dashboard_awarded_total"] or 0)
        if abs(total - expected) > 150:
            raise RuntimeError(
                f"Full GMIS run returned {total:,} rows, but dashboard reports {expected:,}. "
                "Investigate before upload."
            )
        print(f"  [OK] corpus size is close to dashboard total ({total:,} vs {expected:,})")
    elif not full_run:
        print(f"  [WARN] limited run has {total:,} rows; do not treat this as a full corpus.")

    print(f"  Year range: {df['source_year'].dropna().min()} - {df['source_year'].dropna().max()}")
    print(f"  Subject sample distribution: {df['subject'].value_counts(dropna=False).head(10).to_dict()}")


def write_outputs(records: list[dict[str, Any]], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "nsf_sri_lanka_grants_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw parsed records to {raw_path}")

    parquet_path = output_dir / "nsf_sri_lanka_grants.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df):,} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def load_cached_records(output_dir: Path) -> list[dict[str, Any]]:
    raw_path = output_dir / "nsf_sri_lanka_grants_raw.json"
    if not raw_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {raw_path}")
    with raw_path.open("r", encoding="utf-8") as f:
        records = json.load(f)
    if not isinstance(records, list):
        raise RuntimeError(f"Cached JSON should be a list of records: {raw_path}")
    print(f"  [OK] loaded {len(records):,} cached records from {raw_path}")
    return records


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

    prev_path = output_dir / "_prev_nsf_sri_lanka_grants.parquet"
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
            f"\n[ERROR] Refusing to shrink NSF Sri Lanka corpus "
            f"({prev_count:,} -> {new_count:,}). Investigate before upload."
        )
        return False
    print("    [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path,
                 allow_shrink: bool) -> bool:
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
        description="Download NSF Sri Lanka GMIS grants and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/nsf_sri_lanka"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit records for smoke testing")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse nsf_sri_lanka_grants_raw.json from output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook section 1.4 shrink-check")
    parser.add_argument("--max-detail-id", type=int, default=DEFAULT_MAX_DETAIL_ID,
                        help="Highest public rtgrainfodetailView numeric ID to enumerate")
    args = parser.parse_args()

    print("=" * 60)
    print("NSF Sri Lanka GMIS grants -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Source:     {HOME_URL}")
    print(f"  Dashboard:  {DASHBOARD_URL}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    dashboard_counts = fetch_dashboard_counts()

    if args.skip_download:
        print("\nStep 1: Reuse cached raw JSON")
        records = load_cached_records(args.output_dir)
        if args.limit:
            records = records[:args.limit]
            print(f"  [LIMIT] keeping first {len(records):,} cached records")
    else:
        records = fetch_grant_records(limit=args.limit, max_detail_id=args.max_detail_id)

    df = normalize_records(records, dashboard_counts, full_run=args.limit is None)
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
