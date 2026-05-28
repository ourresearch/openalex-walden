#!/usr/bin/env python3
"""
DOST-PCAARRD Grants-in-Aid projects -> S3 Data Pipeline
=======================================================

Downloads the official DOST-PCAARRD Transparency Seal PDFs for yearly
Grants-in-Aid (GIA) projects and writes a deduplicated project-level parquet
for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party: the Philippine Council for Agriculture, Aquatic and
Natural Resources Research and Development (DOST-PCAARRD) publishes yearly
Transparency Seal PDFs titled "DOST-PCAARRD List of Grants in Aid (GIA)
Projects" at:

    https://www.pcaarrd.dost.gov.ph/index.php/transparency-seal

The PDFs are Microsoft Excel exports printed to PDF. They include project
titles, dates, status, total project cost, and the annual PCAARRD GIA amount.

Scope and deduplication
-----------------------
Each yearly PDF is a disclosure snapshot. Ongoing projects repeat across
multiple years, so the script deduplicates by normalized project title,
implementing agency, and start/end dates, keeping the latest disclosure row and
preserving all source years plus per-year GIA amounts in JSON columns.

Output
------
s3://openalex-ingest/awards/pcaarrd/pcaarrd_gia_projects.parquet

Usage
-----
    python pcaarrd_to_s3.py --skip-upload
    python pcaarrd_to_s3.py --limit 10 --skip-upload
    python pcaarrd_to_s3.py --skip-download --skip-upload
    python pcaarrd_to_s3.py --allow-shrink

Requirements
------------
    pip install beautifulsoup4 pandas pyarrow requests
    poppler's `pdftotext` executable must be available on PATH.
"""

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
from urllib.parse import urljoin

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
        file, mode="r", buffering=-1, encoding=None, errors=None,
        newline=None, closefd=True, opener=None,
    ):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)

    _builtins_utf8.open = _open_utf8
# --- end shim ---


TRANSPARENCY_URL = "https://www.pcaarrd.dost.gov.ph/index.php/transparency-seal"

# Awarding body: Philippine Council for Agriculture, Aquatic and Natural
# Resources Research and Development. Verified in OpenAlex as F4320336119 (PH).
FUNDER_ID = 4320336119
FUNDER_DISPLAY_NAME = (
    "Philippine Council for Agriculture, Aquatic and Natural Resources "
    "Research and Development"
)
PROVENANCE = "pcaarrd_gia_projects"
CURRENCY = "PHP"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/pcaarrd/pcaarrd_gia_projects.parquet"

USER_AGENT = "openalex-walden-pcaarrd-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25
EXPECTED_MIN_FULL_ROWS = 900

DATE_AMOUNT_RE = re.compile(
    r"(?P<start>\d{1,2}-[A-Z][a-z]{2}-\d{2,4})\s+"
    r"(?P<end>\d{1,2}-[A-Z][a-z]{2}-\d{2,4})\s+"
    r"(?P<status>[A-Za-z][A-Za-z /-]*?)\s+"
    r"(?P<total>-|\d[\d,]*(?:\.\d+)?)\s+"
    r"(?P<gia>-|\d[\d,]*(?:\.\d+)?)\s*$"
)

KRA_START_RE = re.compile(
    r"^(?:KRA\s*\d+|Rapid\b|Poverty\b|Integrity\s+of\s+the\s+environment|"
    r"Global\s+competitiveness|Competitive\b|Human\s+capital|"
    r"Science,\s*technology|Environmental\s+integrity|Sustainable\b|"
    r"Good\s+governance|Climate\b)",
    re.IGNORECASE,
)

DESCRIPTION_START_RE = re.compile(
    r"^(?:The\s+(?:project|program|study)|This\s+(?:project|program|study)|"
    r"The\s+general\s+objective|General:|Specifically,|To\s+)",
    re.IGNORECASE,
)

OUTPUT_START_RE = re.compile(
    r"^(?:Publication|Publications|Patent|Patents|Product|Products|People|"
    r"Place|Places|Policy|Expected|Y1|Year\s+\d|Total:)",
    re.IGNORECASE,
)

AGENCY_KEYWORDS = re.compile(
    r"\b(?:University|College|Institute|Center|Centre|Department|Bureau|"
    r"School|Council|Authority|Corporation|Agency|Foundation|Research|"
    r"DOST|UPLB|MMSU|ASSCAT|BSU|VSU|USM|CLSU|NVSU|PCA|RMC|Inc\.|Corp\.)\b",
    re.IGNORECASE,
)

BENEFICIARY_BOUNDARY_RE = re.compile(
    r"\b(?:Farmers|Researchers|Students|LGU|LGUs|Policy|Industry|"
    r"Beneficiaries|Extension|Stakeholders|Communities|Women|Men)\b"
    r"|\b(?:farmers|researchers|students|stakeholders|processors|traders)\b",
    re.IGNORECASE,
)


_session: Optional[requests.Session] = None
_last_request_t = 0.0


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
        })
    return _session


def polite_get(url: str, *, timeout: int = 120, max_attempts: int = 4) -> requests.Response:
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
    text = str(value)
    replacements = {
        "\u00c3\u00a2\u00e2\u201a\u00ac\u00e2\u201e\u00a2": "'",
        "\u00c3\u00a2\u00e2\u201a\u00ac\u0153": '"',
        "\u00c3\u00b1": "n",
        "\u20ac\u00a2": "-",
        "\ufb01": "fi",
        "\ufb02": "fl",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = text.replace("\x0c", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def normalize_key(value: Optional[str]) -> str:
    text = clean_text(value) or ""
    text = text.casefold()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_money(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if value.strip() == "-":
        return None
    cleaned = value.replace(",", "").strip()
    try:
        return f"{float(cleaned):.2f}".rstrip("0").rstrip(".")
    except ValueError:
        return clean_text(value)


def parse_source_date(value: str) -> Optional[str]:
    value = value.strip()
    for fmt in ("%d-%b-%y", "%d-%b-%Y"):
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def extract_gia_links(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict[str, str]] = []
    seen: set[int] = set()
    for anchor in soup.find_all("a", href=True):
        text = clean_text(anchor.get_text(" ", strip=True)) or ""
        match = re.search(r"(20\d{2}) DOST-PCAARRD List of Grants in Aid \(GIA\) Projects", text, re.I)
        if not match:
            continue
        year = int(match.group(1))
        if year in seen:
            continue
        seen.add(year)
        out.append({
            "source_year": str(year),
            "title": text,
            "url": urljoin(TRANSPARENCY_URL, anchor["href"]),
        })
    out.sort(key=lambda item: int(item["source_year"]))
    if not out:
        raise RuntimeError("No PCAARRD GIA PDF links discovered on Transparency Seal page")
    return out


def discover_sources(output_dir: Path) -> list[dict[str, str]]:
    print("\n" + "=" * 60)
    print("Step 1: Discover official PCAARRD GIA PDFs")
    print("=" * 60)
    resp = polite_get(TRANSPARENCY_URL)
    html = resp.text
    (output_dir / "pcaarrd_transparency.html").write_text(html, encoding="utf-8")
    links = extract_gia_links(html)
    for item in links:
        print(f"  {item['source_year']}: {item['url']}")
    return links


def download_pdf(item: dict[str, str], source_dir: Path) -> Path:
    year = item["source_year"]
    path = source_dir / f"pcaarrd_gia_{year}.pdf"
    if path.exists() and path.stat().st_size > 0:
        return path
    resp = polite_get(item["url"], timeout=180)
    content_type = resp.headers.get("Content-Type", "")
    if "pdf" not in content_type.lower() and not resp.content.startswith(b"%PDF"):
        raise RuntimeError(f"Expected PDF for {year}, got Content-Type={content_type!r}")
    path.write_bytes(resp.content)
    print(f"  downloaded {year}: {len(resp.content):,} bytes")
    return path


def pdf_to_raw_text(pdf_path: Path, text_path: Path) -> str:
    if text_path.exists() and text_path.stat().st_size > 0:
        return text_path.read_text(encoding="utf-8", errors="replace")
    if not shutil.which("pdftotext"):
        raise RuntimeError("pdftotext is required to parse PCAARRD's PDF exports")
    subprocess.run(["pdftotext", "-raw", str(pdf_path), str(text_path)], check=True)
    return text_path.read_text(encoding="utf-8", errors="replace")


def strip_header_footer(lines: list[str]) -> list[str]:
    out: list[str] = []
    for raw_line in lines:
        line = clean_text(raw_line)
        if not line:
            continue
        if line.startswith("FY ") and "PCAARRD LIST" in line:
            continue
        if "Program Title Project Title Key Result Areas" in line:
            continue
        if line in {
            "December 31,",
            "Total Project",
            "Cost",
            "GIA",
        }:
            continue
        if re.match(r"^20\d{2}'?$", line):
            continue
        if re.match(r"^Page \d+ of \d+$", line):
            continue
        out.append(line)
    return out


def split_program_project(pre_kra_lines: list[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    lines = [line for line in pre_kra_lines if line]
    if not lines:
        return None, None, None

    project_idx: Optional[int] = None
    for idx, line in enumerate(lines):
        if re.match(r"^(?:Project|Study|Component|Sub-?project)\b", line, re.I):
            project_idx = idx
            break

    def strip_embedded_kra(text: Optional[str]) -> Optional[str]:
        if not text:
            return text
        stripped = re.split(
            r"\b(?:Poverty\s+reduction|Rapid,?\s+inclusive|Integrity\s+of\s+the\s+environment|"
            r"Global\s+competitiveness|Competitive\s+and\s+sustainable|Human\s+capital|"
            r"Science,\s*technology|Good\s+governance)\b",
            text,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]
        return clean_text(stripped)

    if project_idx is not None:
        program = clean_text(" ".join(lines[:project_idx]))
        project = strip_embedded_kra(clean_text(" ".join(lines[project_idx:])))
    else:
        program = None
        project = strip_embedded_kra(clean_text(" ".join(lines)))

    combined = clean_text(" - ".join([x for x in [program, project] if x]))
    return program, project, combined


def split_kra_and_description(lines: list[str], kra_idx: int) -> tuple[Optional[str], Optional[str], list[str]]:
    tail = lines[kra_idx:]
    if not tail:
        return None, None, []

    desc_idx: Optional[int] = None
    for idx, line in enumerate(tail):
        if idx == 0:
            continue
        if DESCRIPTION_START_RE.search(line):
            desc_idx = idx
            break
    if desc_idx is None:
        desc_idx = min(2, len(tail))

    kra = clean_text(" ".join(tail[:desc_idx]))
    remainder = tail[desc_idx:]

    output_idx: Optional[int] = None
    for idx, line in enumerate(remainder):
        if OUTPUT_START_RE.search(line):
            output_idx = idx
            break
    if output_idx is None:
        output_idx = min(len(remainder), 8)

    description = clean_text(" ".join(remainder[:output_idx]))
    after_description = remainder[output_idx:]
    return kra, description, after_description


def extract_agency_and_beneficiaries(lines: list[str]) -> tuple[Optional[str], Optional[str]]:
    if not lines:
        return None, None

    # Search near the end because raw PDF extraction lists expected outputs
    # before implementing agency / beneficiaries.
    tail = [line for line in lines[-35:] if line]
    best_idx: Optional[int] = None
    for idx in range(len(tail) - 1, -1, -1):
        line = tail[idx]
        if OUTPUT_START_RE.search(line):
            continue
        if AGENCY_KEYWORDS.search(line) or re.match(r"^[A-Z][A-Z&./ -]{1,25}\b", line):
            best_idx = idx
            break

    if best_idx is None:
        return None, clean_text(" ".join(tail[-6:]))

    agency_parts = [tail[best_idx]]
    for line in tail[best_idx + 1:best_idx + 3]:
        if line.startswith(("—", "-", "€¢", "•")):
            break
        if BENEFICIARY_BOUNDARY_RE.search(line):
            break
        if OUTPUT_START_RE.search(line):
            break
        if len(clean_text(" ".join(agency_parts + [line])) or "") <= 180:
            agency_parts.append(line)

    agency = clean_text(" ".join(agency_parts))
    if agency:
        acronym_match = re.match(r"^([A-Z][A-Z&./-]{1,20})\s+(.+)$", agency)
        if acronym_match and BENEFICIARY_BOUNDARY_RE.search(acronym_match.group(2)):
            agency = acronym_match.group(1)
        agency = re.split(r"\s+[—•]\s+|\s+€¢\s+", agency, maxsplit=1)[0]
        agency = re.split(BENEFICIARY_BOUNDARY_RE, agency, maxsplit=1)[0]
        agency = clean_text(agency)
        if agency and (
            len(agency) > 180
            or re.search(r"\b(?:findings|policies|programs|services|support sustainable|use the study)\b", agency, re.I)
        ):
            agency = None

    beneficiary_lines = tail[best_idx + len(agency_parts):]
    beneficiaries = clean_text(" ".join(beneficiary_lines[-10:]))
    return agency, beneficiaries


def parse_raw_text(text: str, source: dict[str, str]) -> list[dict[str, Any]]:
    source_year = int(source["source_year"])
    lines = strip_header_footer(text.splitlines())

    blocks: list[tuple[dict[str, str], list[str]]] = []
    current: list[str] = []
    for line in lines:
        match = DATE_AMOUNT_RE.search(line)
        if match:
            current.append(line[:match.start()].rstrip())
            blocks.append((match.groupdict(), current))
            current = []
        else:
            current.append(line)

    rows: list[dict[str, Any]] = []
    for row_number, (meta, block_lines) in enumerate(blocks, start=1):
        block_lines = strip_header_footer(block_lines)
        kra_idx: Optional[int] = None
        for idx, line in enumerate(block_lines):
            if KRA_START_RE.search(line):
                kra_idx = idx
                break
        if kra_idx is None:
            # Keep the row but use the pre-date text as a title so validation
            # can surface any unusual layouts.
            pre_kra = block_lines[:]
            kra = None
            description = None
            after_description: list[str] = []
        else:
            pre_kra = block_lines[:kra_idx]
            kra, description, after_description = split_kra_and_description(block_lines, kra_idx)

        program_title, project_title, combined_title = split_program_project(pre_kra)
        agency, beneficiaries = extract_agency_and_beneficiaries(after_description)
        display_name = project_title or combined_title
        if not display_name:
            continue

        start_date = parse_source_date(meta["start"])
        end_date = parse_source_date(meta["end"])
        dedupe_key = "|".join([
            normalize_key(display_name),
            normalize_key(agency),
            start_date or "",
            end_date or "",
        ])
        award_hash = hashlib.sha1(dedupe_key.encode("utf-8")).hexdigest()[:16]

        rows.append({
            "funder_award_id": f"pcaarrd-{award_hash}",
            "source_year": str(source_year),
            "source_row_number": str(row_number),
            "source_title": source["title"],
            "source_url": source["url"],
            "source_pdf_url": source["url"],
            "display_name": display_name,
            "program_title": program_title,
            "project_title": project_title,
            "program_project_text": combined_title,
            "key_result_area": kra,
            "description": description,
            "source_implementing_agency": agency,
            "beneficiaries": beneficiaries,
            "start_date": start_date,
            "end_date": end_date,
            "start_date_raw": meta["start"],
            "end_date_raw": meta["end"],
            "status": clean_text(meta["status"]),
            "amount": parse_money(meta["total"]),
            "currency": CURRENCY,
            "total_project_cost": parse_money(meta["total"]),
            "pcaarrd_gia_for_source_year": parse_money(meta["gia"]),
            "source_years": str(source_year),
            "gia_by_source_year_json": json.dumps(
                {str(source_year): parse_money(meta["gia"])},
                ensure_ascii=False,
                sort_keys=True,
            ),
            "funder_id": str(FUNDER_ID),
            "funder_display_name": FUNDER_DISPLAY_NAME,
            "provenance": PROVENANCE,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        })
    print(f"  parsed {source_year}: {len(rows):,} disclosure rows")
    return rows


def dedupe_project_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row["funder_award_id"], []).append(row)

    deduped: list[dict[str, Any]] = []
    for award_id, group in grouped.items():
        group = sorted(group, key=lambda r: (int(r["source_year"]), int(r["source_row_number"])))
        latest = dict(group[-1])
        years = sorted({r["source_year"] for r in group}, key=int)
        gia_by_year = {
            r["source_year"]: r["pcaarrd_gia_for_source_year"]
            for r in group
            if r.get("pcaarrd_gia_for_source_year")
        }
        latest["source_years"] = ",".join(years)
        latest["gia_by_source_year_json"] = json.dumps(gia_by_year, ensure_ascii=False, sort_keys=True)
        latest["source_row_count_before_dedupe"] = str(len(group))
        latest["funder_award_id"] = award_id
        deduped.append(latest)

    deduped.sort(key=lambda r: (r.get("start_date") or "", r.get("display_name") or "", r["funder_award_id"]))
    return deduped


def fetch_and_parse(output_dir: Path, *, limit: Optional[int]) -> pd.DataFrame:
    source_dir = output_dir / "source"
    source_dir.mkdir(parents=True, exist_ok=True)

    links = discover_sources(output_dir)
    all_rows: list[dict[str, Any]] = []
    for item in links:
        pdf_path = download_pdf(item, source_dir)
        text_path = source_dir / f"pcaarrd_gia_{item['source_year']}.txt"
        text = pdf_to_raw_text(pdf_path, text_path)
        all_rows.extend(parse_raw_text(text, item))

    if not all_rows:
        raise RuntimeError("No PCAARRD rows parsed from official PDFs")

    print(f"  raw disclosure rows before project dedupe: {len(all_rows):,}")
    rows = dedupe_project_rows(all_rows)
    print(f"  project rows after dedupe: {len(rows):,}")

    if limit:
        rows = rows[:limit]
        print(f"  [LIMIT] keeping first {len(rows):,} deduped rows")

    return pd.DataFrame(rows)


def load_cached(output_dir: Path, *, limit: Optional[int]) -> pd.DataFrame:
    cache_path = output_dir / "pcaarrd_gia_projects_raw.json"
    if not cache_path.exists():
        raise FileNotFoundError(f"Missing cache file: {cache_path}")
    rows = json.loads(cache_path.read_text(encoding="utf-8"))
    if limit:
        rows = rows[:limit]
    return pd.DataFrame(rows)


def validate(df: pd.DataFrame, *, full_run: bool) -> None:
    print("\n" + "=" * 60)
    print("Validation")
    print("=" * 60)
    rows = len(df)
    print(f"  rows: {rows:,}")
    if rows == 0:
        raise RuntimeError("No rows to write")
    if full_run and rows < EXPECTED_MIN_FULL_ROWS:
        raise RuntimeError(f"Full run parsed only {rows:,} rows; expected at least {EXPECTED_MIN_FULL_ROWS:,}")

    duplicate_ids = int(df["funder_award_id"].duplicated().sum())
    print(f"  duplicate funder_award_id values: {duplicate_ids:,}")
    if duplicate_ids:
        dupes = df.loc[df["funder_award_id"].duplicated(keep=False), "funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")

    required = ["display_name", "start_date", "amount", "currency", "source_pdf_url"]
    for col in required:
        coverage = float(df[col].notna().mean() * 100.0)
        print(f"  {col} coverage: {coverage:.1f}%")
    if df["display_name"].isna().any():
        raise RuntimeError("display_name coverage must be 100%")
    amount_missing = float(df["amount"].isna().mean())
    if full_run and amount_missing > 0.02:
        raise RuntimeError("More than 2% of rows are missing total project cost")
    if not full_run and amount_missing > 0.50:
        raise RuntimeError("Smoke sample has less than 50% total project cost coverage")

    start_years = df["start_date"].dropna().astype(str).str[:4]
    year_min = start_years.min() if not start_years.empty else "NULL"
    year_max = start_years.max() if not start_years.empty else "NULL"
    print(f"  year range: {year_min}-{year_max}")
    print(f"  total project cost ({CURRENCY}): {pd.to_numeric(df['amount'], errors='coerce').sum():,.2f}")
    print("  top statuses:")
    print(df["status"].value_counts(dropna=False).head(10).to_string())
    print("  source years represented:")
    print(df["source_year"].value_counts().sort_index().to_string())


def write_outputs(df: pd.DataFrame, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_path = output_dir / "pcaarrd_gia_projects_raw.json"
    cache_path.write_text(json.dumps(df.to_dict(orient="records"), ensure_ascii=False, indent=2), encoding="utf-8")

    parquet_path = output_dir / "pcaarrd_gia_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    print(f"\nWrote {len(df):,} rows to {parquet_path}")
    return parquet_path


def check_no_shrink(parquet_path: Path, *, allow_shrink: bool) -> None:
    if allow_shrink:
        print("  [ALLOW-SHRINK] Skipping S3 shrink check by explicit request")
        return
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    tmp_path = parquet_path.with_name("existing_pcaarrd_gia_projects.parquet")
    print(f"  Checking existing S3 object for shrink guard: {s3_uri}")
    result = subprocess.run(["aws", "s3", "cp", s3_uri, str(tmp_path)], capture_output=True, text=True)
    if result.returncode != 0:
        stderr = result.stderr or ""
        if "404" in stderr or "NoSuchKey" in stderr or "Not Found" in stderr:
            print("  No existing S3 parquet found; shrink check passes for first upload")
            return
        raise RuntimeError(f"Could not fetch existing S3 parquet for shrink check:\n{stderr}")
    old_rows = len(pd.read_parquet(tmp_path))
    new_rows = len(pd.read_parquet(parquet_path))
    print(f"  existing rows: {old_rows:,}; new rows: {new_rows:,}")
    if new_rows < old_rows:
        raise RuntimeError(
            f"Refusing to upload shrinking corpus ({new_rows:,} < {old_rows:,}). "
            "Use --allow-shrink only after reviewing the source change."
        )


def upload_to_s3(parquet_path: Path, *, allow_shrink: bool) -> None:
    print("\n" + "=" * 60)
    print("S3 Upload")
    print("=" * 60)
    check_no_shrink(parquet_path, allow_shrink=allow_shrink)
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
    print(f"Uploaded to {s3_uri}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download official PCAARRD GIA projects to parquet")
    parser.add_argument("--limit", type=int, help="Limit deduped rows for smoke tests")
    parser.add_argument("--output-dir", default="data/pcaarrd", help="Local output/cache directory")
    parser.add_argument("--skip-download", action="store_true", help="Reuse cached JSON instead of downloading")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true", help="Allow a shrinking corpus during upload")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)

    if args.skip_download:
        df = load_cached(output_dir, limit=args.limit)
    else:
        df = fetch_and_parse(output_dir, limit=args.limit)

    full_run = not args.limit
    validate(df, full_run=full_run)
    parquet_path = write_outputs(df, output_dir)

    if args.skip_upload:
        print("\nSkipping S3 upload (--skip-upload)")
    else:
        upload_to_s3(parquet_path, allow_shrink=args.allow_shrink)


if __name__ == "__main__":
    main()
