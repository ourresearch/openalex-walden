#!/usr/bin/env python3
"""
TUBITAK TRDizin projects -> S3 pipeline
=======================================

Downloads completed TUBITAK-funded project records from TRDizin's public
search API and writes a parquet staging file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party / official. TUBITAK says that ARDEB-funded projects
completed since 1965 are published through ULAKBIM's "TUBITAK Destekli
Projeler Veri Tabani" and can be searched by project number, title,
principal/researcher/consultant names, year, and keywords:

    https://tubitak.gov.tr/tr/duyuru/tubitak-destekli-projeler-veri-tabani

The current TRDizin Angular bundle exposes the public Elasticsearch-backed
endpoint used below. The historical `/api/defaultSearch/project` route is not
available; projects are enumerated by filtering publication search to
`facet-documentType=PROJECT`:

    https://search.trdizin.gov.tr/api/defaultSearch/publication/?q=&order=publicationYear-DESC&page=1&limit=100&facet-documentType=PROJECT

Validation on 2026-06-02 source probe:
  - API reported 27,828 PROJECT records.
  - Pagination worked through page 279 at limit=100; page 280 was empty.
  - Records include native project number, title/abstract, project group,
    start/end dates, year, subject tags, and named contributors with roles.
  - Amount/currency and institution are not published in these API records.

Output
------
s3://openalex-ingest/awards/tubitak/tubitak_trdizin_projects.parquet

Usage
-----
    python tubitak_to_s3.py --limit 10 --skip-upload
    python tubitak_to_s3.py --skip-upload
    python tubitak_to_s3.py --skip-download --skip-upload
    python tubitak_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
import builtins
import json
import math
import re
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

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
    import pathlib as _pathlib_utf8

    _orig_wt = _pathlib_utf8.Path.write_text
    def _wt(self, data, encoding=None, errors=None, newline=None):
        return _orig_wt(self, data, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.write_text = _wt

    _orig_rt = _pathlib_utf8.Path.read_text
    def _rt(self, encoding=None, errors=None, newline=None):
        return _orig_rt(self, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.read_text = _rt

    _orig_open = builtins.open
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    builtins.open = _open_utf8
# --- end shim ---


FUNDER_ID = 4320322626
FUNDER_DISPLAY_NAME = "Türkiye Bilimsel ve Teknolojik Araştırma Kurumu"
PROVENANCE = "trdizin_tubitak_projects"

SOURCE_NOTICE_URL = "https://tubitak.gov.tr/tr/duyuru/tubitak-destekli-projeler-veri-tabani"
SEARCH_PAGE_URL = "https://search.trdizin.gov.tr/tr/proje/ara"
API_URL = "https://search.trdizin.gov.tr/api/defaultSearch/publication/"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/tubitak/tubitak_trdizin_projects.parquet"

USER_AGENT = "openalex-walden-tubitak-ingest/1.0 (+https://openalex.org)"
PAGE_SIZE = 100
EXPECTED_MIN_FULL_ROWS = 27000

SPACE_RE = re.compile(r"\s+")
SUFFIX_RE = re.compile(
    r"\b(?:Ph\.?D\.?|MD|M\.?D\.?|Dr\.?|Prof\.?|Assoc\.?|Doç\.?|Jr\.?|Sr\.?|II|III|IV)\b\.?",
    re.IGNORECASE,
)


def log(message: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\r", " ").replace("\n", " ")
    text = SPACE_RE.sub(" ", text).strip()
    if not text or text.lower() in {"none", "null", "nan", "-"}:
        return None
    return text


def date_part(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    if "T" in text:
        text = text.split("T", 1)[0]
    if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
        if text.startswith("0001-") or text == "1900-01-01":
            return None
        return text
    return None


def int_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return str(int(value))
    except (TypeError, ValueError):
        text = clean_text(value)
        return text


def json_string(value: Any) -> Optional[str]:
    if value in (None, [], {}):
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def pick_localized(items: Iterable[dict[str, Any]], key: str, language: str) -> Optional[str]:
    language = language.upper()
    for item in items:
        if clean_text(item.get("language")) == language:
            return clean_text(item.get(key))
    return None


def split_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Canonical given/family split for given-name ... family-name sources."""
    text = clean_text(name)
    if not text:
        return None, None
    text = SUFFIX_RE.sub("", text)
    text = SPACE_RE.sub(" ", text).strip(" ,")
    if not text:
        return None, None
    parts = text.split()
    if len(parts) == 1:
        return None, parts[0]
    return " ".join(parts[:-1]), parts[-1]


def normalize_project_number(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    return SPACE_RE.sub("", text).upper()


def contributor_from_author(author: dict[str, Any], start_date: Optional[str]) -> dict[str, Optional[str]]:
    name = clean_text(author.get("name")) or clean_text(author.get("inPublicationName"))
    given, family = split_name(name)
    duty = clean_text(author.get("duty"))
    order = int_string(author.get("order"))
    institution = author.get("institution") if isinstance(author.get("institution"), dict) else {}
    return {
        "name": name,
        "given_name": given,
        "family_name": family,
        "orcid": clean_text(author.get("orcid")),
        "duty": duty,
        "order": order,
        "role_start": start_date,
        "institution_name": clean_text(author.get("institutionName")),
        "institution_title": clean_text((institution or {}).get("title")),
        "institution_root_title": clean_text((institution or {}).get("rootTitle")),
        "institution_country": clean_text((institution or {}).get("country")),
    }


def sorted_contributors(authors: list[dict[str, Any]], start_date: Optional[str]) -> list[dict[str, Optional[str]]]:
    contributors = [contributor_from_author(author, start_date) for author in authors or []]
    return sorted(contributors, key=lambda c: int(c.get("order") or "999999"))


def select_lead(contributors: list[dict[str, Optional[str]]]) -> tuple[Optional[dict[str, Optional[str]]], list[dict[str, Optional[str]]]]:
    managers = [c for c in contributors if c.get("duty") == "MANAGER"]
    lead = managers[0] if managers else (contributors[0] if contributors else None)
    if lead is None:
        return None, []
    others = [c for c in contributors if c is not lead]
    return lead, others


def title_slug(title: Optional[str]) -> str:
    title = clean_text(title) or "project"
    slug = re.sub(r"[^0-9A-Za-z]+", "-", title.lower()).strip("-")
    return slug[:80] or "project"


def project_quality(row: dict[str, Any]) -> tuple[int, int, int, int, str, str]:
    authors = row.get("authors") or []
    abstracts = row.get("abstracts") or []
    has_manager = any(a.get("duty") == "MANAGER" for a in authors)
    has_title = any(clean_text(a.get("title")) for a in abstracts)
    has_abstract = any(clean_text(a.get("abstract")) for a in abstracts)
    return (
        1 if has_manager else 0,
        1 if has_title else 0,
        1 if has_abstract else 0,
        len(authors),
        clean_text(row.get("firstIndexDate")) or "",
        str(row.get("id") or ""),
    )


def api_params(page: int, page_size: int) -> dict[str, str]:
    return {
        "q": "",
        "order": "publicationYear-DESC",
        "page": str(page),
        "limit": str(page_size),
        "facet-documentType": "PROJECT",
    }


def fetch_page(session: requests.Session, page: int, page_size: int, retries: int = 3) -> dict[str, Any]:
    params = api_params(page, page_size)
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(API_URL, params=params, timeout=90)
            log(f"GET page={page} limit={page_size} -> HTTP {resp.status_code}; {len(resp.content):,} bytes")
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_error = exc
            log(f"  [WARN] page {page} attempt {attempt}/{retries} failed: {exc}")
            time.sleep(min(2 ** attempt, 10))
    raise RuntimeError(f"Failed to fetch TRDizin page {page}") from last_error


def fetch_records(output_dir: Path, limit: Optional[int]) -> tuple[list[dict[str, Any]], int, Path]:
    print("\n" + "=" * 60)
    print("Step 1: Download TRDizin PROJECT records")
    print("=" * 60)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

    output_dir.mkdir(parents=True, exist_ok=True)
    raw_path = output_dir / "tubitak_trdizin_projects_raw.jsonl"
    target_page_size = min(PAGE_SIZE, limit) if limit else PAGE_SIZE
    first = fetch_page(session, 1, target_page_size)
    total = int(first.get("hits", {}).get("total", {}).get("value") or 0)
    if total <= 0:
        raise RuntimeError("TRDizin PROJECT search returned zero total records.")
    if limit:
        expected_pages = math.ceil(min(limit, total) / target_page_size)
        expected_records = min(limit, total)
    else:
        expected_pages = math.ceil(total / target_page_size)
        expected_records = total
    log(f"TRDizin reports {total:,} PROJECT records; fetching {expected_records:,} records across {expected_pages:,} pages.")

    records: list[dict[str, Any]] = []

    def add_hits(payload: dict[str, Any]) -> None:
        for hit in payload.get("hits", {}).get("hits", []) or []:
            source = hit.get("_source") or {}
            source["_trdizin_hit_id"] = hit.get("_id")
            source["_trdizin_index"] = hit.get("_index")
            source["_trdizin_score"] = hit.get("_score")
            records.append(source)

    add_hits(first)
    for page in range(2, expected_pages + 1):
        payload = fetch_page(session, page, target_page_size)
        hits = payload.get("hits", {}).get("hits", []) or []
        if not hits and page <= expected_pages:
            log(f"  [WARN] empty page {page} before expected end; continuing per runbook no-first-empty rule.")
        add_hits(payload)
        if limit and len(records) >= limit:
            break

    if limit:
        records = records[:limit]

    with raw_path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    log(f"Wrote {len(records):,} raw TRDizin records to {raw_path}")
    return records, total, raw_path


def load_cached_records(output_dir: Path, limit: Optional[int]) -> tuple[list[dict[str, Any]], int, Path]:
    print("\n" + "=" * 60)
    print("Step 1: Reuse cached TRDizin JSONL")
    print("=" * 60)
    raw_path = output_dir / "tubitak_trdizin_projects_raw.jsonl"
    if not raw_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {raw_path}")
    records: list[dict[str, Any]] = []
    with raw_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            records.append(json.loads(line))
            if limit and len(records) >= limit:
                break
    log(f"Loaded {len(records):,} cached records from {raw_path}")
    return records, len(records), raw_path


def dedupe_by_project_number(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    missing_project_number = 0
    for record in records:
        project_number = normalize_project_number(record.get("projectnumber"))
        if not project_number:
            missing_project_number += 1
            continue
        groups[project_number].append(record)

    deduped: list[dict[str, Any]] = []
    duplicate_groups = 0
    duplicate_records = 0
    for project_number, candidates in groups.items():
        if len(candidates) > 1:
            duplicate_groups += 1
            duplicate_records += len(candidates) - 1
        best = sorted(candidates, key=project_quality, reverse=True)[0]
        best["_duplicate_trdizin_ids"] = [
            clean_text(c.get("_trdizin_hit_id")) or clean_text(c.get("id"))
            for c in candidates
            if c is not best
        ]
        best["_duplicate_source_count"] = len(candidates)
        deduped.append(best)

    stats = {
        "missing_project_number": missing_project_number,
        "duplicate_project_number_groups": duplicate_groups,
        "duplicate_project_number_extra_records": duplicate_records,
    }
    return sorted(deduped, key=lambda r: (int(r.get("publicationYear") or 0), normalize_project_number(r.get("projectnumber")) or "")), stats


def normalize_records(records: list[dict[str, Any]], reported_total: int, raw_path: Path, limit: Optional[int]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Normalize TRDizin records")
    print("=" * 60)

    deduped, dedupe_stats = dedupe_by_project_number(records)
    log(
        "Native project-number cleanup: "
        f"{len(records):,} source records -> {len(deduped):,} award rows; "
        f"{dedupe_stats['missing_project_number']:,} missing project number; "
        f"{dedupe_stats['duplicate_project_number_groups']:,} duplicate groups."
    )

    retrieved_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    for record in deduped:
        project_number = normalize_project_number(record.get("projectnumber"))
        if not project_number:
            continue

        abstracts = record.get("abstracts") or []
        authors = record.get("authors") or []
        subjects = record.get("subjects") or []
        start_date = date_part(record.get("startedDate"))
        end_date = date_part(record.get("endDate"))
        contributors = sorted_contributors(authors, start_date)
        lead, others = select_lead(contributors)

        title_tr = pick_localized(abstracts, "title", "TUR")
        title_en = pick_localized(abstracts, "title", "ENG")
        abstract_tr = pick_localized(abstracts, "abstract", "TUR")
        abstract_en = pick_localized(abstracts, "abstract", "ENG")
        keywords_tr = pick_localized(abstracts, "keywords", "TUR")
        keywords_en = pick_localized(abstracts, "keywords", "ENG")
        display_name = title_tr or title_en or clean_text(record.get("orderTitle")) or f"TUBITAK project {project_number}"
        description = abstract_tr or abstract_en
        trdizin_id = clean_text(record.get("_trdizin_hit_id")) or int_string(record.get("id"))
        landing_page_url = f"https://search.trdizin.gov.tr/tr/yayin/detay/{trdizin_id}" if trdizin_id else SEARCH_PAGE_URL
        subject_names = [clean_text(s.get("name")) for s in subjects if clean_text(s.get("name"))]
        subject_roots = [clean_text(s.get("rootName")) for s in subjects if clean_text(s.get("rootName"))]

        rows.append({
            "funder_award_id": project_number,
            "trdizin_id": trdizin_id,
            "display_name": display_name,
            "description": description,
            "title_tr": title_tr,
            "title_en": title_en,
            "abstract_tr": abstract_tr,
            "abstract_en": abstract_en,
            "keywords_tr": keywords_tr,
            "keywords_en": keywords_en,
            "project_number": project_number,
            "project_group": clean_text(record.get("projectGroup")),
            "publication_year": int_string(record.get("publicationYear")),
            "start_date": start_date,
            "end_date": end_date,
            "lead_name": lead.get("name") if lead else None,
            "lead_given_name": lead.get("given_name") if lead else None,
            "lead_family_name": lead.get("family_name") if lead else None,
            "lead_orcid": lead.get("orcid") if lead else None,
            "lead_role": lead.get("duty") if lead else None,
            "contributors_json": json_string(contributors),
            "other_investigators_json": json_string(others),
            "contributor_count": int_string(len(contributors)),
            "subject_names": "; ".join(subject_names) if subject_names else None,
            "subject_roots": "; ".join(sorted(set(subject_roots))) if subject_roots else None,
            "subjects_json": json_string(subjects),
            "access_type": clean_text(record.get("accessType")),
            "language": clean_text(record.get("language")),
            "report_pages": int_string(record.get("endPage")),
            "pdf_id": clean_text(record.get("pdf")),
            "view_count": int_string(record.get("viewCount")),
            "download_count": int_string(record.get("downloadCount")),
            "first_index_date": date_part(record.get("firstIndexDate")),
            "index_date": date_part(record.get("indexDate")),
            "source_doc_type": clean_text(record.get("docType")),
            "source_raw_path": str(raw_path),
            "source_notice_url": SOURCE_NOTICE_URL,
            "search_page_url": SEARCH_PAGE_URL,
            "api_url": API_URL,
            "landing_page_url": landing_page_url,
            "duplicate_trdizin_ids_json": json_string(record.get("_duplicate_trdizin_ids")),
            "duplicate_source_count": int_string(record.get("_duplicate_source_count")),
            "reported_total": int_string(reported_total),
            "retrieved_at": retrieved_at,
        })

    df = pd.DataFrame(rows)
    validate_dataframe(df, full_run=limit is None)
    return df


def coverage(df: pd.DataFrame, column: str) -> tuple[int, float]:
    total = len(df)
    count = int(df[column].notna().sum()) if column in df else 0
    pct = count / total * 100 if total else 0.0
    return count, pct


def validate_dataframe(df: pd.DataFrame, *, full_run: bool) -> None:
    total = len(df)
    print(f"  Rows: {total:,}")
    if total == 0:
        raise RuntimeError("Normalized dataframe is empty.")

    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(20).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values after dedupe: {dupes}")
    print(f"  Distinct funder_award_id values: {df['funder_award_id'].nunique():,}")

    for column in [
        "display_name", "description", "project_number", "project_group",
        "publication_year", "start_date", "end_date", "lead_name", "lead_orcid",
        "contributors_json", "subject_names", "report_pages", "pdf_id",
    ]:
        count, pct = coverage(df, column)
        print(f"  {column:24s}: {count:,}/{total:,} ({pct:.1f}%)")

    required_thresholds = {
        "display_name": 0.99,
        "project_number": 1.00,
        "project_group": 0.95,
        "publication_year": 0.95,
        "start_date": 0.90,
        "end_date": 0.90,
        "lead_name": 0.95,
        "contributors_json": 0.95,
    }
    for column, threshold in required_thresholds.items():
        pct = df[column].notna().mean()
        if pct < threshold:
            raise RuntimeError(f"Unexpectedly low {column} coverage: {pct * 100:.1f}%")

    years = pd.to_numeric(df["publication_year"], errors="coerce")
    print(f"  Year range: {int(years.min())}-{int(years.max())}")
    print(f"  Top project groups: {df['project_group'].value_counts(dropna=False).head(12).to_dict()}")
    roles = Counter()
    for value in df["contributors_json"].dropna():
        for contributor in json.loads(value):
            roles[contributor.get("duty")] += 1
    print(f"  Contributor role distribution: {dict(roles.most_common())}")

    if full_run and total < EXPECTED_MIN_FULL_ROWS:
        raise RuntimeError(
            f"Full TUBITAK run returned only {total:,} rows; expected at least {EXPECTED_MIN_FULL_ROWS:,}."
        )


def write_outputs(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / "tubitak_trdizin_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    size_mb = parquet_path.stat().st_size / (1024 * 1024)
    print(f"  [OK] wrote {len(df):,} rows ({size_mb:.2f} MB) to {parquet_path}")
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

    prev_path = output_dir / "_prev_tubitak_trdizin_projects.parquet"
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
        print(f"\n[ERROR] Refusing to shrink TUBITAK corpus ({prev_count:,} -> {new_count:,}).")
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
        description="Download TUBITAK TRDizin project records and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/tubitak"))
    parser.add_argument("--limit", type=int, default=None, help="Limit records for smoke testing")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached JSONL from output-dir")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("TUBITAK TRDizin projects -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Provenance: {PROVENANCE}")
    print(f"  Source:     {SEARCH_PAGE_URL}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.skip_download:
        records, reported_total, raw_path = load_cached_records(args.output_dir, args.limit)
    else:
        records, reported_total, raw_path = fetch_records(args.output_dir, args.limit)

    df = normalize_records(records, reported_total, raw_path, args.limit)
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
