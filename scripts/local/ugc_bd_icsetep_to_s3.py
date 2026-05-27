#!/usr/bin/env python3
"""
UGC Bangladesh ICSETEP R&D Grant sub-projects -> S3 Data Pipeline
=================================================================

Downloads the official ICSETEP Research & Development Grant approved
sub-project PDF published under the University Grants Commission of Bangladesh
domain and writes a parquet file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party:

    https://icsetep.ugc.gov.bd/research-and-development-grant/
    https://icsetep.ugc.gov.bd/wp-content/uploads/2026/01/ASP-Details.pdf

ICSETEP is implemented by the University Grants Commission of Bangladesh. The
source page links the "List of Approved Sub-Projects (APP) from 1st Round".
The PDF contains one row per approved R&D grant sub-project with title, focus
area, principal investigator, designation, and affiliation.

Output
------
s3://openalex-ingest/awards/ugc_bd_icsetep/ugc_bd_icsetep_projects.parquet

Usage
-----
    python ugc_bd_icsetep_to_s3.py --skip-upload
    python ugc_bd_icsetep_to_s3.py --limit 10 --skip-upload
    python ugc_bd_icsetep_to_s3.py --skip-download --skip-upload
    python ugc_bd_icsetep_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests boto3
    system command: pdftotext (Poppler)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

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


SOURCE_PAGE_URL = "https://icsetep.ugc.gov.bd/research-and-development-grant/"
APPROVED_PDF_URL = "https://icsetep.ugc.gov.bd/wp-content/uploads/2026/01/ASP-Details.pdf"

FUNDER_ID = 4320316035
FUNDER_DISPLAY_NAME = "University Grants Commission of Bangladesh"
PROVENANCE = "ugc_bd_icsetep_rdg"
FUNDER_SCHEME = "ICSETEP Research and Development Grant - Round 1"
SOURCE_YEAR = "2026"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ugc_bd_icsetep/ugc_bd_icsetep_projects.parquet"

USER_AGENT = "openalex-walden-ugc-bd-icsetep-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25

AREA_LABELS = {
    "A": "Academy-Industry Collaboration",
    "B": "Interdisciplinary R&D",
    "C": "Cutting Edge CSE/IT Research",
    "D": "IT Solutions Addressing Disability Issues",
}
AFFIL_PREFIXES = (
    "Professor,",
    "Associate Professor,",
    "Assistant Professor,",
    "Lecturer,",
)
AFFIL_WORDS = (
    "Professor",
    "Department",
    "University",
    "Institute",
    "Technology",
    "Engineering",
    "Communication",
    "Mathematics",
    "BRAC",
    "Daffodil",
    "Jashore",
    "Pabna",
    "Rajshahi",
    "Varendra",
    "Hajee",
    "Bangladesh Agricultural",
    "Mawlana Bhashani",
)

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/pdf,*/*;q=0.8",
        })
    return _session


def polite_get(url: str, timeout: int = 90, max_attempts: int = 4) -> requests.Response:
    global _last_request_t
    session = get_session()
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < MIN_REQUEST_INTERVAL_S:
            time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
        try:
            resp = session.get(url, timeout=timeout)
            _last_request_t = time.monotonic()
            print(f"  GET {url} -> HTTP {resp.status_code}, {len(resp.content):,} bytes")
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
    text = str(value).replace("\ufeff", "").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def normalize_text(value: Any) -> str:
    return clean_text(value) or ""


def split_person_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    text = clean_text(name)
    if not text:
        return None, None
    tokens = [token.strip(",") for token in text.split() if token.strip(",")]
    suffixes = {"PhD", "MD", "DPhil", "Jr", "Jr.", "Sr", "Sr.", "II", "III", "IV"}
    while tokens and tokens[-1] in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def discover_pdf_url(source_html: str) -> str:
    pdfs = re.findall(r"https?://[^\"'<>\s]+\.pdf", source_html, flags=re.I)
    for url in pdfs:
        if "ASP-Details.pdf" in url:
            return url
    print("  [WARN] approved-subprojects PDF was not discoverable in page HTML; using documented URL.")
    return APPROVED_PDF_URL


def pdf_to_raw_text(pdf_path: Path, txt_path: Path) -> str:
    pdftotext = shutil.which("pdftotext")
    if not pdftotext:
        raise RuntimeError("pdftotext is required but was not found on PATH.")
    result = subprocess.run(
        [pdftotext, "-raw", str(pdf_path), str(txt_path)],
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"pdftotext failed: {result.stderr.strip()}")
    text = txt_path.read_text(encoding="utf-8", errors="replace")
    if len(text.strip()) < 1000:
        raise RuntimeError(f"pdftotext extracted only {len(text.strip())} characters from {pdf_path}")
    return text


def download_sources(output_dir: Path) -> tuple[str, dict[str, Any]]:
    print("\n" + "=" * 60)
    print("Step 1: Download official ICSETEP approved sub-project PDF")
    print("=" * 60)
    page = polite_get(SOURCE_PAGE_URL)
    pdf_url = discover_pdf_url(page.text)
    pdf = polite_get(pdf_url, timeout=120)
    pdf_path = output_dir / "ASP-Details.pdf"
    txt_path = output_dir / "ASP-Details.txt"
    pdf_path.write_bytes(pdf.content)
    raw_text = pdf_to_raw_text(pdf_path, txt_path)
    manifest = {
        "source_page_url": SOURCE_PAGE_URL,
        "pdf_url": pdf_url,
        "pdf_bytes": len(pdf.content),
        "text_chars": len(raw_text),
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
    (output_dir / "ugc_bd_icsetep_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"  [OK] PDF text extracted: {len(raw_text):,} chars")
    return raw_text, manifest


def load_cached_sources(output_dir: Path) -> tuple[str, dict[str, Any]]:
    txt_path = output_dir / "ASP-Details.txt"
    manifest_path = output_dir / "ugc_bd_icsetep_manifest.json"
    if not txt_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {txt_path}")
    raw_text = txt_path.read_text(encoding="utf-8", errors="replace")
    manifest = {
        "source_page_url": SOURCE_PAGE_URL,
        "pdf_url": APPROVED_PDF_URL,
        "downloaded_at": None,
    }
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    print(f"  [OK] loaded cached PDF text: {txt_path} ({len(raw_text):,} chars)")
    return raw_text, manifest


def split_affiliation(rest: str) -> tuple[str, str]:
    for prefix in AFFIL_PREFIXES:
        idx = rest.find(prefix)
        if idx != -1:
            return rest[:idx].strip(), rest[idx:].strip()
    return rest.strip(), ""


def looks_like_name(text: str) -> bool:
    tokens = [token.strip(",") for token in text.split()]
    if not 1 <= len(tokens) <= 6:
        return False
    bad_tokens = {"and", "or", "in", "for", "of", "the", "with", "from", "to", "based", "enabled"}
    for token in tokens:
        base = token.strip(".()")
        if not base:
            continue
        if "-" in base or ":" in base:
            return False
        if base.lower() in bad_tokens:
            return False
        if not base[0].isupper():
            return False
    return True


def split_marker_line(line: str) -> Optional[dict[str, Optional[str]]]:
    text = normalize_text(line)
    match = re.match(r"^([A-D])\s+(.+)$", text)
    if match:
        area = match.group(1)
        pi, affiliation = split_affiliation(match.group(2))
        if looks_like_name(pi):
            return {"title_before": None, "area": area, "pi": pi, "affiliation": affiliation}
        return None

    match = re.match(r"^(.+?)\s+([A-D])\s+(.+)$", text)
    if match:
        title_before = match.group(1)
        area = match.group(2)
        pi, affiliation = split_affiliation(match.group(3))
        if looks_like_name(pi):
            return {"title_before": title_before, "area": area, "pi": pi, "affiliation": affiliation}
    return None


def is_affiliation_line(line: str, current_affiliation_lines: list[str]) -> bool:
    text = normalize_text(line)
    if not text:
        return False
    if any(word in text for word in AFFIL_WORDS):
        return True
    if current_affiliation_lines and (text.lower().startswith(("of ", "and ")) or len(text.split()) <= 4):
        return True
    return False


def parse_pdf_text(raw_text: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    title_lines: list[str] = []
    current: Optional[dict[str, Any]] = None

    for raw_line in raw_text.splitlines():
        line = normalize_text(raw_line)
        if not line:
            continue
        if line.startswith("Sub-Project Title") or line.startswith("List of Approved"):
            continue

        marker = split_marker_line(line)
        if marker:
            if current is not None:
                records.append(current)
            if marker["title_before"]:
                title_lines.append(marker["title_before"] or "")
            current = {
                "title_lines": title_lines,
                "area": marker["area"],
                "pi": marker["pi"],
                "affiliation_lines": [],
            }
            if marker["affiliation"]:
                current["affiliation_lines"].append(marker["affiliation"])
            title_lines = []
            continue

        if current is not None and is_affiliation_line(line, current["affiliation_lines"]):
            current["affiliation_lines"].append(line)
            continue

        if current is not None:
            records.append(current)
            current = None
        title_lines.append(line)

    if current is not None:
        records.append(current)

    parsed: list[dict[str, Any]] = []
    for idx, record in enumerate(records, start=1):
        title = clean_text(" ".join(record["title_lines"]))
        pi = clean_text(record["pi"])
        affiliation = clean_text(" ".join(record["affiliation_lines"]))
        area = clean_text(record["area"])
        if not title or not pi or not affiliation or not area:
            raise RuntimeError(f"Could not parse complete row {idx}: {record}")
        parsed.append({
            "source_order": idx,
            "title": title,
            "area": area,
            "area_label": AREA_LABELS.get(area),
            "principal_investigator": pi,
            "affiliation": affiliation,
        })
    return parsed


def source_row_json(row: dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True)


def award_id_for(title: str) -> str:
    digest = hashlib.sha1(normalize_text(title).lower().encode("utf-8")).hexdigest()[:12]
    return f"icsetep-rdg-r1-{digest}"


def normalize(raw_text: str, manifest: dict[str, Any], limit: Optional[int], *, full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Parse, normalize, and validate")
    print("=" * 60)
    rows = parse_pdf_text(raw_text)
    print(f"  Parsed official PDF: {len(rows):,} approved sub-project rows")
    if limit is not None:
        rows = rows[:limit]
        print(f"  [LIMIT] keeping first {len(rows):,} rows")

    downloaded_at = manifest.get("downloaded_at") or datetime.now(timezone.utc).isoformat()
    pdf_url = manifest.get("pdf_url") or APPROVED_PDF_URL
    records: list[dict[str, Any]] = []
    for row in rows:
        given, family = split_person_name(row["principal_investigator"])
        award_id = award_id_for(row["title"])
        source_row = {
            **row,
            "pdf_url": pdf_url,
            "source_page_url": SOURCE_PAGE_URL,
        }
        records.append({
            "funder_award_id": award_id,
            "display_name": row["title"],
            "description": (
                f"Approved ICSETEP R&D Grant Round 1 sub-project in area "
                f"{row['area']} ({row['area_label']}). Principal investigator: "
                f"{row['principal_investigator']}."
            ),
            "principal_investigator": row["principal_investigator"],
            "pi_given_name": given,
            "pi_family_name": family,
            "pi_affiliation": row["affiliation"],
            "area": row["area"],
            "area_label": row["area_label"],
            "funder_scheme": FUNDER_SCHEME,
            "funding_type": "research",
            "amount": None,
            "currency": None,
            "source_year": SOURCE_YEAR,
            "source_order": str(row["source_order"]),
            "landing_page_url": SOURCE_PAGE_URL,
            "source_pdf_url": pdf_url,
            "source_page_url": SOURCE_PAGE_URL,
            "source_row_json": source_row_json(source_row),
            "downloaded_at": downloaded_at,
        })

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No UGC Bangladesh ICSETEP rows were normalized")

    for col in ["funder_award_id", "display_name", "principal_investigator", "pi_affiliation", "area", "area_label"]:
        missing = int(out[col].isna().sum())
        if missing:
            raise RuntimeError(f"Required column {col} has {missing:,} missing values")

    dupes = int(out["funder_award_id"].duplicated().sum())
    if dupes:
        raise RuntimeError(f"Duplicate funder_award_id values: {dupes:,}")

    if full_run and len(out) != 20:
        raise RuntimeError(f"Expected exactly 20 approved ICSETEP Round 1 rows; got {len(out):,}")

    print(f"  Rows: {len(out):,}")
    print(f"  Unique funder_award_id: {out['funder_award_id'].nunique():,}")
    print(f"  Title coverage: {out['display_name'].notna().mean() * 100:.1f}%")
    print(f"  PI coverage: {out['principal_investigator'].notna().mean() * 100:.1f}%")
    print(f"  Affiliation coverage: {out['pi_affiliation'].notna().mean() * 100:.1f}%")
    print(f"  Amount coverage: 0.0% (official PDF does not publish per-sub-project amounts)")
    print(f"  Area distribution: {out['area'].value_counts().to_dict()}")

    return out.astype("string")


def write_outputs(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "ugc_bd_icsetep_projects.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    print(f"  [OK] wrote {len(df):,} rows ({parquet_path.stat().st_size / 1024:.1f} KB) to {parquet_path}")
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

    prev_path = output_dir / "_prev_ugc_bd_icsetep_projects.parquet"
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
        print(f"\n[ERROR] Refusing to shrink UGC Bangladesh ICSETEP corpus ({prev_count:,} -> {new_count:,}).")
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
        description="Download UGC Bangladesh ICSETEP R&D Grant sub-projects and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/ugc_bd_icsetep"))
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached PDF text from output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("UGC Bangladesh ICSETEP R&D Grant sub-projects -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Source:     {SOURCE_PAGE_URL}")
    print(f"  Provenance: {PROVENANCE}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        print("\nStep 1: Reuse cached official PDF text")
        raw_text, manifest = load_cached_sources(args.output_dir)
    else:
        raw_text, manifest = download_sources(args.output_dir)

    df = normalize(raw_text, manifest, args.limit, full_run=args.limit is None)
    parquet_path = write_outputs(df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading to S3.")
        print(f"Local parquet ready: {parquet_path}")
        return

    ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
