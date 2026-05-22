#!/usr/bin/env python3
"""
IDRC (International Development Research Centre) to S3 Data Pipeline
====================================================================

Downloads IDRC's IATI Activity XML files from open.canada.ca, parses them,
and uploads a single parquet file to S3 for Databricks ingestion.

Data source: https://open.canada.ca/data/organization/idrc-crdi
            (CKAN package id 7188a59d-4d90-4759-b16c-1c5f24df6910)
Output: s3://openalex-ingest/awards/idrc/idrc_projects.parquet

The package contains one XML per fiscal year of closed activities plus
one for currently active activities. URLs change as new years are
published, so we discover them dynamically via the CKAN API rather than
hardcoding.

Source fidelity: each `iati-activity` becomes one row. Most multi-language
or multi-row sub-elements (budget, transaction, participating-org,
sector, recipient-region) are preserved as JSON strings so the
Databricks notebook can decide how to flatten them.

Requirements:
    pip install pandas pyarrow requests
    AWS CLI configured with write access to s3://openalex-ingest/awards/idrc/
"""

import argparse
import io
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). This
# crashes scrapers writing laureate names with non-ASCII chars (Polish ł,
# Turkish ğ, Greek μ, combining accents, zero-width spaces). Production
# runs on Linux/Databricks where UTF-8 is the default, but this fixes
# local validation on Windows without requiring contractors to set
# PYTHONUTF8=1 in their environment. See runbook §1.2.
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

CKAN_PACKAGE_URL = (
    "https://open.canada.ca/data/api/action/package_show"
    "?id=7188a59d-4d90-4759-b16c-1c5f24df6910"
)
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/idrc/idrc_projects.parquet"

# idrc-crdi.ca returns 403 to the default requests user-agent
HTTP_HEADERS = {
    "User-Agent": (
        "openalex-walden/1.0 (+https://openalex.org; contact: team@openalex.org)"
    ),
}

XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def discover_xml_urls() -> list[dict]:
    """Hit CKAN to enumerate every IATI activity XML in the IDRC package.

    Skip the organization XML (separate package) and HTML glossary entries.
    """
    log(f"Fetching CKAN package metadata: {CKAN_PACKAGE_URL}")
    resp = requests.get(CKAN_PACKAGE_URL, timeout=30, headers=HTTP_HEADERS)
    resp.raise_for_status()
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"CKAN error: {body.get('error')}")

    out = []
    for r in body["result"].get("resources", []):
        if (r.get("format") or "").upper() != "XML":
            continue
        name = (r.get("name") or "").lower()
        if "organization" in name:
            continue
        out.append({"name": r.get("name"), "url": r.get("url")})
    log(f"Found {len(out)} IATI activity XML files")
    return out


def get_narratives(elem: ET.Element) -> dict[str, str]:
    """Collect language → narrative-text for an element with `narrative` children."""
    out: dict[str, str] = {}
    for n in elem.findall("narrative"):
        lang = n.attrib.get(XML_LANG, "und")
        if n.text and lang not in out:
            out[lang] = n.text.strip()
    return out


def en_or_first(narratives: dict[str, str]) -> str | None:
    if not narratives:
        return None
    return narratives.get("en") or next(iter(narratives.values()))


def parse_activity(act: ET.Element, source_url: str) -> dict:
    """Map one iati-activity element to a flat-ish dict.

    Lossy nothing important: budgets, transactions, participating-orgs,
    sectors, recipient-regions all get serialised to JSON so the
    notebook can drill in.
    """
    iati_id_el = act.find("iati-identifier")
    iati_id = iati_id_el.text.strip() if iati_id_el is not None and iati_id_el.text else None

    title_n = get_narratives(act.find("title")) if act.find("title") is not None else {}

    # Description type=1 is "general"; preserve all types as separate fields
    descs: dict[str, dict[str, str]] = {}
    for d in act.findall("description"):
        descs[d.attrib.get("type", "1")] = get_narratives(d)
    desc_general = descs.get("1", {})

    # Activity dates: type 1 planned-start, 2 actual-start, 3 planned-end, 4 actual-end
    dates: dict[str, str | None] = {"1": None, "2": None, "3": None, "4": None}
    for d in act.findall("activity-date"):
        t = d.attrib.get("type")
        iso = d.attrib.get("iso-date")
        if t in dates and iso:
            dates[t] = iso

    # Budgets: list of {period_start, period_end, value, currency, value_date, status, type}
    budgets = []
    for b in act.findall("budget"):
        v = b.find("value")
        budgets.append({
            "status": b.attrib.get("status"),
            "type": b.attrib.get("type"),
            "period_start": (b.find("period-start").attrib.get("iso-date")
                             if b.find("period-start") is not None else None),
            "period_end": (b.find("period-end").attrib.get("iso-date")
                           if b.find("period-end") is not None else None),
            "value": (v.text.strip() if v is not None and v.text else None),
            "currency": (v.attrib.get("currency") if v is not None else None),
            "value_date": (v.attrib.get("value-date") if v is not None else None),
        })

    # Transactions
    transactions = []
    for t in act.findall("transaction"):
        v = t.find("value")
        tt = t.find("transaction-type")
        td = t.find("transaction-date")
        transactions.append({
            "type": tt.attrib.get("code") if tt is not None else None,
            "date": td.attrib.get("iso-date") if td is not None else None,
            "value": v.text.strip() if v is not None and v.text else None,
            "currency": v.attrib.get("currency") if v is not None else None,
        })

    # Participating organisations (role 1=funding, 2=accountable, 3=extending, 4=implementing)
    participating = []
    for po in act.findall("participating-org"):
        narr = get_narratives(po)
        participating.append({
            "role": po.attrib.get("role"),
            "type": po.attrib.get("type"),
            "ref": po.attrib.get("ref"),
            "name_en": narr.get("en"),
            "name_fr": narr.get("fr"),
            "name_es": narr.get("es"),
        })

    # Recipient countries / regions
    recipient_countries = [
        {"code": rc.attrib.get("code"), "percentage": rc.attrib.get("percentage")}
        for rc in act.findall("recipient-country")
    ]
    recipient_regions = [
        {"code": rr.attrib.get("code"), "percentage": rr.attrib.get("percentage"),
         "vocabulary": rr.attrib.get("vocabulary")}
        for rr in act.findall("recipient-region")
    ]

    sectors = [
        {"code": s.attrib.get("code"), "vocabulary": s.attrib.get("vocabulary"),
         "percentage": s.attrib.get("percentage")}
        for s in act.findall("sector")
    ]

    status_el = act.find("activity-status")
    activity_status_code = status_el.attrib.get("code") if status_el is not None else None
    scope_el = act.find("activity-scope")
    activity_scope_code = scope_el.attrib.get("code") if scope_el is not None else None

    return {
        # Source-fidelity identifiers
        "iati_identifier": iati_id,
        "title_en": title_n.get("en"),
        "title_fr": title_n.get("fr"),
        "title_es": title_n.get("es"),
        "description_en": desc_general.get("en"),
        "description_fr": desc_general.get("fr"),
        "description_es": desc_general.get("es"),
        # Dates kept as ISO strings so Spark parses them with TRY_TO_DATE
        "planned_start": dates["1"],
        "actual_start": dates["2"],
        "planned_end": dates["3"],
        "actual_end": dates["4"],
        # Status / scope
        "activity_status_code": activity_status_code,
        "activity_scope_code": activity_scope_code,
        # Money (preserved as JSON arrays — notebook will sum)
        "budgets_json": json.dumps(budgets, ensure_ascii=False),
        "transactions_json": json.dumps(transactions, ensure_ascii=False),
        # Orgs / geography / sectors
        "participating_orgs_json": json.dumps(participating, ensure_ascii=False),
        "recipient_countries_json": json.dumps(recipient_countries, ensure_ascii=False),
        "recipient_regions_json": json.dumps(recipient_regions, ensure_ascii=False),
        "sectors_json": json.dumps(sectors, ensure_ascii=False),
        # Provenance
        "source_xml_url": source_url,
        "downloaded_at": datetime.utcnow().isoformat(),
    }


def fetch_xml(url: str, retries: int = 3) -> bytes:
    last_err = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=120, headers=HTTP_HEADERS)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            last_err = e
            log(f"  retry {attempt + 1}/{retries} after error: {e}")
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


def parse_xml_to_rows(xml_bytes: bytes, source_url: str) -> list[dict]:
    root = ET.fromstring(xml_bytes)
    activities = root.findall("iati-activity")
    return [parse_activity(a, source_url) for a in activities]


def main() -> None:
    p = argparse.ArgumentParser(description="IDRC IATI XML -> parquet -> S3")
    p.add_argument("--limit-files", type=int, default=None,
                   help="For smoke-test: only process the first N XML files")
    p.add_argument("--limit-rows", type=int, default=None,
                   help="For smoke-test: stop after N total activities")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"),
                   help="Local output directory")
    p.add_argument("--skip-upload", action="store_true",
                   help="Skip S3 upload (smoke-test only)")
    args = p.parse_args()

    log("=" * 60)
    log("IDRC -> S3 pipeline starting")

    urls = discover_xml_urls()
    if args.limit_files:
        urls = urls[: args.limit_files]
        log(f"Smoke-test mode: limited to first {len(urls)} XML files")

    all_rows: list[dict] = []
    for i, ent in enumerate(urls, 1):
        log(f"[{i}/{len(urls)}] {ent['name']}")
        xml_bytes = fetch_xml(ent["url"])
        log(f"  downloaded {len(xml_bytes):,} bytes")
        rows = parse_xml_to_rows(xml_bytes, ent["url"])
        log(f"  parsed {len(rows):,} activities (running total: {len(all_rows) + len(rows):,})")
        all_rows.extend(rows)
        if args.limit_rows and len(all_rows) >= args.limit_rows:
            log(f"Smoke-test row limit hit ({args.limit_rows}); stopping")
            all_rows = all_rows[: args.limit_rows]
            break

    log(f"Total activities: {len(all_rows):,}")
    df = pd.DataFrame(all_rows)
    # All source columns are strings (IATI text fields + JSON-serialized substructures).
    # Force string dtype to prevent pyarrow inferring null-heavy columns (e.g. title_es,
    # description_es, planned_start) as int — which breaks COALESCE/CAST in the notebook.
    df = df.astype("string")
    log(f"DataFrame shape: {df.shape}")
    log(f"Columns: {list(df.columns)}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "idrc_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {parquet_path}")

    if args.skip_upload:
        log("--skip-upload set; done.")
        return

    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3  # imported here so smoke-tests don't require boto3
    s3 = boto3.client("s3")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    main()
