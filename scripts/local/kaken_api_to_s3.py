#!/usr/bin/env python3
"""
KAKEN (JSPS Grants-in-Aid) official-API to S3 Data Pipeline
===========================================================

Replaces the page-scraping pipeline in kaken_to_s3.py with NII's official
KAKEN OpenSearch API. Same destination and a superset of the same parquet
schema, so CreateKAKENAwards.ipynb / CreateKAKENWorkAwards.ipynb run
unchanged.

Data Source: https://kaken.nii.ac.jp/opensearch/  (format=xml)
API docs:    https://support.nii.ac.jp/en/kaken/api/api_outline
XML schema:  https://bitbucket.org/niijp/KAKEN_Definition (v4.4.0)
Output:      s3://openalex-ingest/awards/kaken/kaken_projects.parquet

Why the API instead of scraping:
- 500 records/request (rw=500) vs 1 page fetch per grant: ~874k grants come
  down in ~1,750 requests (hours) instead of ~1M page fetches (days).
- The XML is the master format behind the site and carries fields the HTML
  scrape never had: all project members with roles (not just the PI),
  direct/indirect cost breakdown, project status, and per-product
  peer-review/open-access/acknowledgement flags plus PMID/ISSN/NAID/WoS/
  Scopus identifiers with an `authenticated` attribute.
- Deterministic paging by fiscal year makes re-runs cheap (the funder
  pipeline gained features after the first KAKEN ingest; re-ingests should
  not cost a week of scraping).

Requirements:
    pip install pandas pyarrow requests lxml

    KAKEN_APPID environment variable (free registration:
    https://support.nii.ac.jp/en/cinii/api/developer). Also read from
    ../../.env (openalex-walden/.env) if not set in the environment.

    AWS CLI configured with write access to s3://openalex-ingest/awards/kaken/

Usage:
    python kaken_api_to_s3.py                       # full harvest + upload
    python kaken_api_to_s3.py --resume              # skip already-saved pages
    python kaken_api_to_s3.py --years 2023-2025 --skip-upload   # validation
    python kaken_api_to_s3.py --parse-only          # re-parse cached XML only

Author: OpenAlex Team
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from lxml import etree

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# See kaken_to_s3.py / runbook §1.2 for rationale.
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

API_URL = "https://kaken.nii.ac.jp/opensearch/"
ROWS_PER_REQUEST = 500          # documented maximum
REQUEST_DELAY = 1.0             # polite pacing; whole harvest is only ~1,750 requests
MAX_RETRIES = 5
RETRY_BACKOFF = 3.0
FIRST_FISCAL_YEAR = 1964        # KAKEN coverage starts here
USER_AGENT = "OpenAlex-KAKEN-Ingest/2.0 (research data aggregator; contact@openalex.org)"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/kaken/kaken_projects.parquet"

WALDEN_ENV = Path(__file__).resolve().parents[2] / ".env"


def get_appid() -> str:
    """KAKEN_APPID from the environment, falling back to openalex-walden/.env."""
    appid = os.getenv("KAKEN_APPID", "").strip()
    if not appid and WALDEN_ENV.exists():
        for line in WALDEN_ENV.read_text().splitlines():
            m = re.match(r"\s*(?:export\s+)?KAKEN_APPID\s*=\s*['\"]?([^'\"#\s]+)", line)
            if m:
                appid = m.group(1)
                break
    if not appid:
        sys.exit(
            "[ERROR] KAKEN_APPID not set (env var or openalex-walden/.env).\n"
            "        Register a free CiNii Web API application ID at\n"
            "        https://support.nii.ac.jp/en/cinii/api/developer"
        )
    return appid


# =============================================================================
# Harvest (raw XML pages, cached to disk, resumable)
# =============================================================================

def api_get(params: dict, appid: str) -> bytes:
    """One API request with retry/backoff. Returns raw XML bytes."""
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(
                API_URL,
                params={"appid": appid, "format": "xml", "lang": "en", **params},
                headers={"User-Agent": USER_AGENT},
                timeout=120,
            )
            if resp.status_code == 200 and resp.content.lstrip().startswith(b"<?xml"):
                return resp.content
            last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
            # 403 = bad appid: no point retrying
            if resp.status_code == 403:
                sys.exit(f"[ERROR] API rejected the appid: {last_err}")
        except requests.RequestException as e:
            last_err = f"{type(e).__name__}: {e}"
        time.sleep(RETRY_BACKOFF ** (attempt + 1))
    raise RuntimeError(f"API request failed after {MAX_RETRIES} attempts: {last_err}")


def get_year_total(year: int, appid: str) -> int:
    """Total research projects whose period starts in the given fiscal year.

    rw only accepts 20/50/100/200/500 -- other values (e.g. rw=1) silently
    return totalResults=0, so the count probe pays for a small real page.
    """
    xml = api_get({"s1": year, "s2": year, "o1": 1, "rw": 20, "st": 1}, appid)
    root = etree.fromstring(xml)
    # OpenSearch-style totalResults: tolerate either an attribute on the root
    # or a namespaced/plain child element, since the response envelope is not
    # publicly documented without an appid.
    if root.get("totalResults"):
        return int(root.get("totalResults"))
    for el in root.iter():
        tag = etree.QName(el).localname if el.tag is not etree.Comment else ""
        if tag == "totalResults" and (el.text or "").strip().isdigit():
            return int(el.text.strip())
    # Fallback: count grantAward elements in this single-row response and let
    # the pager run until an empty page.
    return -1


def harvest_year(year: int, appid: str, xml_dir: Path, resume: bool) -> int:
    """Download all pages for one fiscal year. Returns number of records saved."""
    total = get_year_total(year, appid)
    if total == 0:
        return 0
    n_seen = 0
    st = 1
    page = 1
    while True:
        out = xml_dir / f"{year}_{st:07d}.xml"
        if resume and out.exists():
            content = out.read_bytes()
        else:
            time.sleep(REQUEST_DELAY)
            content = api_get(
                {"s1": year, "s2": year, "o1": 1, "rw": ROWS_PER_REQUEST, "st": st},
                appid,
            )
            out.write_bytes(content)
        n_page = content.count(b"<grantAward ")
        n_seen += n_page
        if total >= 0:
            print(f"    {year} page {page}: {n_page} records ({n_seen:,}/{total:,})")
        else:
            print(f"    {year} page {page}: {n_page} records ({n_seen:,})")
        if n_page < ROWS_PER_REQUEST:
            break
        if total >= 0 and n_seen >= total:
            break
        st += ROWS_PER_REQUEST
        page += 1
    return n_seen


def harvest_all(years: range, appid: str, xml_dir: Path, resume: bool) -> None:
    print(f"\n{'='*60}\nPhase 1: Harvesting KAKEN via OpenSearch API\n{'='*60}")
    xml_dir.mkdir(parents=True, exist_ok=True)
    grand_total = 0
    t0 = time.time()
    for year in years:
        n = harvest_year(year, appid, xml_dir, resume)
        grand_total += n
        elapsed = time.time() - t0
        print(f"  [{year}] {n:,} records (cumulative {grand_total:,}, {elapsed/60:.1f} min)")
    print(f"\n  [DONE] {grand_total:,} records harvested")


# =============================================================================
# Parse (XML -> rows matching the kaken_projects.parquet schema)
# =============================================================================

def _lang_pick(elements, lang: str = "en") -> Optional[str]:
    """Pick text of the element in the requested language, else the first."""
    if not elements:
        return None
    for el in elements:
        if el.get("{http://www.w3.org/XML/1998/namespace}lang") == lang:
            text = (el.text or "").strip()
            if text:
                return text
    text = (elements[0].text or "").strip()
    return text or None


def _txt(parent, path: str) -> Optional[str]:
    el = parent.find(path)
    if el is not None and el.text and el.text.strip():
        return el.text.strip()
    return None


def _member_names(m) -> tuple:
    """(family, given) from a <member>'s personalName."""
    name_el = m.find("personalName")
    if name_el is None:
        return None, None
    family = _txt(name_el, "familyName")
    given = _txt(name_el, "givenName")
    full = _txt(name_el, "fullName")
    if full and not (family or given):
        parts = full.split()
        if len(parts) >= 2:
            return parts[0], " ".join(parts[1:])
        return full, None
    return family, given


def parse_grant_award(ga) -> Optional[dict]:
    """Map one <grantAward> element to a parquet row (scraper-compatible).

    Live-response shape (validated against 20H00001, 2026-07-07): the
    Japanese <summary> is the master record (all members, periodOfAward,
    keywordList, overallAwardAmount); the English <summary> is a sparse
    overlay carrying romanized names and translated titles/categories.
    Merge accordingly: display strings prefer EN, structure comes from JA.
    """
    award_number = ga.get("awardNumber")
    if not award_number:
        return None

    XMLLANG = "{http://www.w3.org/XML/1998/namespace}lang"
    summaries = ga.findall("summary")
    if not summaries:
        return None
    en = next((s for s in summaries if s.get(XMLLANG) == "en"), None)
    ja = next((s for s in summaries if s.get(XMLLANG) == "ja"), None)
    master = ja if ja is not None else (en if en is not None else summaries[0])
    display = en if en is not None else master

    def _from_both(path):
        """Element from the display (EN) summary, falling back to master."""
        el = display.find(path)
        if el is None or (el.text is not None and not el.text.strip() and len(el) == 0):
            fb = master.find(path)
            if fb is not None:
                return fb
        return el if el is not None else master.find(path)

    title = _txt(display, "title") or _txt(master, "title")
    # EN category text is English ("Grant-in-Aid for ...") — required by the
    # downstream funding_type CASE mapping in CreateKAKENAwards.
    category = _txt(display, "category") or _txt(master, "category")

    # Period (JA summary only in live responses)
    period = master.find("periodOfAward")
    if period is None and en is not None:
        period = en.find("periodOfAward")
    start_date = end_date = None
    if period is not None:
        start_date = _txt(period, "startDate")
        end_date = _txt(period, "endDate")
        if not start_date:
            fy = _txt(period, "startFiscalYear") or period.get("searchStartFiscalYear")
            if fy:
                start_date = f"{fy}-04-01"
        if not end_date:
            fy = _txt(period, "endFiscalYear") or period.get("searchEndFiscalYear")
            if fy:
                end_date = f"{fy}-03-31"

    # Budget: overall totals; keep direct/indirect as additive columns
    amount = amount_direct = amount_indirect = None
    overall = master.find("overallAwardAmount")
    if overall is None and en is not None:
        overall = en.find("overallAwardAmount")
    if overall is not None:
        def _num(tag):
            v = _txt(overall, tag)
            if v:
                v = re.sub(r"[^\d.]", "", v)
                return float(v) if v else None
            return None
        amount = _num("totalCost") or _num("convertedJpyTotalCost")
        amount_direct = _num("directCost")
        amount_indirect = _num("indirectCost")

    # Members: roster from the master (JA) summary; romanized names overlaid
    # from the EN summary by eradCode/researcherNumber (fallback: sequence).
    def _member_key(m):
        return m.get("researcherNumber") or m.get("eradCode") or f"seq{m.get('sequence')}"

    en_names = {}
    if en is not None:
        for m in en.findall("member"):
            en_names[_member_key(m)] = _member_names(m)

    pi_given = pi_family = pi_affil = pi_nrid = None
    members = []
    for m in master.findall("member"):
        role = m.get("role")
        # researcher number: @researcherNumber in the schema, but live records
        # often carry it only as @eradCode (same 8-digit KAKEN number).
        rn = m.get("researcherNumber") or m.get("eradCode")
        family, given = _member_names(m)
        en_fam, en_giv = en_names.get(_member_key(m), (None, None))
        # Prefer romanized names (matches the scraper's /en/ page output).
        family = en_fam or family
        given = en_giv or given
        affil_bits = []
        aff = m.find("affiliation")
        for tag in ("institution", "department", "jobTitle"):
            # affiliation/* is current; direct member/* children are the
            # deprecated-but-still-populated fallback.
            t = (_txt(aff, tag) if aff is not None else None) or _txt(m, tag)
            if t:
                affil_bits.append(t)
        affiliation = ", ".join(affil_bits) or None
        inst_code = None
        for el in ((aff.find("institution") if aff is not None else None), m.find("institution")):
            if el is not None and el.get("niiCode"):
                inst_code = el.get("niiCode")
                break
        members.append({
            "role": role,
            "family_name": family,
            "given_name": given,
            "researcher_number": rn,
            "erad_code": m.get("eradCode"),
            "affiliation": affiliation,
            "institution_nii_code": inst_code,
        })
        if role == "principal_investigator" and pi_family is None:
            pi_family, pi_given, pi_affil = family, given, affiliation
            # NRID = '1000' + researcher number zero-padded to 9 digits
            # (matches the nrid.nii.ac.jp URLs the scraper harvested, e.g.
            # researcher 50287950 -> NRID 1000050287950).
            pi_nrid = f"1000{rn.zfill(9)}" if rn else None

    # Institution (project host): EN display name, niiCode from either
    institution = None
    institution_nii_code = None
    for s in (display, master):
        el = s.find("institution")
        if el is not None:
            if institution is None and el.text and el.text.strip():
                institution = el.text.strip()
            if institution_nii_code is None:
                institution_nii_code = el.get("niiCode")

    # Keywords (JA summary only in live responses)
    keywords = None
    for s in (display, master):
        kws = [
            (k.text or "").strip()
            for k in s.findall("keywordList/keyword")
            if k.text and k.text.strip()
        ]
        if kws:
            keywords = ", ".join(kws)
            break

    # Abstract: paragraphList typed purpose/abstract/achievement, EN preferred
    abstract = None
    for s in (display, master):
        for want in ("abstract", "purpose", "outline", "achievement"):
            for pl in s.findall("paragraphList"):
                if (pl.get("type") or "").lower().startswith(want):
                    texts = [
                        (p.text or "").strip()
                        for p in pl.findall("paragraph")
                        if p.text and p.text.strip()
                    ]
                    if texts:
                        abstract = "\n".join(texts)
                        break
            if abstract:
                break
        if abstract:
            break

    # productListEnriched = KAKEN's own (funder-side) identifier resolution.
    # Index enriched DOIs by <ref id> so DOI-less self-reports can be
    # backfilled, explicitly labeled enriched_doi (provenance stays visible).
    enriched_doi = {}
    ple = ga.find("productListEnriched")
    if ple is not None:
        for p in ple.findall("product"):
            ref = p.find("ref")
            doi = _txt(p, "doi")
            if ref is not None and ref.get("id") and doi:
                enriched_doi[ref.get("id")] = doi

    # Research products (self-reported grant -> output links). Superset of the
    # scraper's {type,title,year,doi,naid}; all values strings so the junction
    # notebook's array<map<string,string>> parse keeps working.
    products = []
    pl_el = ga.find("productList")
    for p in (pl_el.findall("product") if pl_el is not None else []):
        def _ident(tag):
            el = p.find(tag)
            if el is None or not (el.text or "").strip():
                return None, None
            return el.text.strip(), el.get("authenticated")
        doi, doi_auth = _ident("doi")
        naid, _ = _ident("naid")
        pmid, _ = _ident("pmid")
        issn, _ = _ident("issn")
        prod = {
            "type": p.get("type"),
            "title": _lang_pick(p.findall("title")),
            "year": _txt(p, "year"),
            "doi": doi,
            "naid": naid,
            "pmid": pmid,
            "issn": issn,
            "doi_authenticated": doi_auth,
            "journal_title": _lang_pick(p.findall("journalTitle")),
            "reviewed": p.get("reviewed"),
            "open_access": p.get("openAccess"),
            "acknowledgement": p.get("acknowledgement"),
        }
        if not doi and p.get("id") in enriched_doi:
            prod["enriched_doi"] = enriched_doi[p.get("id")]
        if any(prod.get(k) for k in ("title", "doi", "naid", "pmid")):
            products.append({k: v for k, v in prod.items() if v is not None})

    return {
        "project_id": award_number,
        "title": title,
        "abstract": abstract,
        "category": category,
        "start_date": start_date,
        "end_date": end_date,
        "amount": amount,
        "currency": "JPY",
        "pi_given_name": pi_given,
        "pi_family_name": pi_family,
        "pi_affiliation": pi_affil,
        "pi_nrid": pi_nrid,
        "institution": institution,
        "keywords": keywords,
        "products_json": json.dumps(products, ensure_ascii=False) if products else None,
        "landing_page_url": f"https://kaken.nii.ac.jp/grant/KAKENHI-PROJECT-{award_number}/",
        # -- additive columns (not consumed by current notebooks) --
        "amount_direct": amount_direct,
        "amount_indirect": amount_indirect,
        "project_type": ga.get("projectType"),
        "project_status": (
            master.find("projectStatus").get("statusCode")
            if master.find("projectStatus") is not None else None
        ),
        "institution_nii_code": institution_nii_code,
        "members_json": json.dumps(members, ensure_ascii=False) if members else None,
    }


def strip_namespaces(root) -> None:
    """Drop any default XML namespace in place so element paths stay simple.

    The response envelope is not publicly documented (format=xml is
    appid-gated), so tolerate both namespaced and plain markup. xml:lang
    attributes are unaffected.
    """
    for el in root.iter():
        if isinstance(el.tag, str) and el.tag.startswith("{"):
            el.tag = etree.QName(el).localname
    etree.cleanup_namespaces(root)


def parse_all(xml_dir: Path) -> pd.DataFrame:
    print(f"\n{'='*60}\nPhase 2: Parsing XML\n{'='*60}")
    files = sorted(xml_dir.glob("*.xml"))
    if not files:
        sys.exit(f"[ERROR] No XML files in {xml_dir}; run the harvest first")
    rows = []
    for i, f in enumerate(files):
        try:
            root = etree.fromstring(f.read_bytes())
            strip_namespaces(root)
        except etree.XMLSyntaxError as e:
            print(f"  [WARN] {f.name}: XML parse error ({e}); skipping")
            continue
        for ga in root.iter("grantAward"):
            row = parse_grant_award(ga)
            if row:
                rows.append(row)
        if (i + 1) % 100 == 0:
            print(f"  [{i+1}/{len(files)}] {len(rows):,} projects")
    df = pd.DataFrame(rows)
    before = len(df)
    # Re-runs of a fiscal year can overlap page boundaries; keep first.
    df = df.drop_duplicates(subset=["project_id"], keep="first")
    print(f"  Parsed {before:,} rows -> {len(df):,} unique projects")
    return df


# =============================================================================
# Save + upload
# =============================================================================

def save_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    schema = pa.schema([
        ("project_id", pa.string()),
        ("title", pa.string()),
        ("abstract", pa.string()),
        ("category", pa.string()),
        ("start_date", pa.string()),
        ("end_date", pa.string()),
        ("amount", pa.float64()),
        ("currency", pa.string()),
        ("pi_given_name", pa.string()),
        ("pi_family_name", pa.string()),
        ("pi_affiliation", pa.string()),
        ("pi_nrid", pa.string()),
        ("institution", pa.string()),
        ("keywords", pa.string()),
        ("products_json", pa.string()),
        ("landing_page_url", pa.string()),
        ("amount_direct", pa.float64()),
        ("amount_indirect", pa.float64()),
        ("project_type", pa.string()),
        ("project_status", pa.string()),
        ("institution_nii_code", pa.string()),
        ("members_json", pa.string()),
        ("ingested_at", pa.string()),
    ])
    out = output_dir / "kaken_projects.parquet"
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, out)
    print(f"\n  [SAVE] {out} ({out.stat().st_size/1e6:.1f} MB)")

    print(f"\n  Summary:")
    print(f"    - Projects: {len(df):,}")
    for col in ("title", "start_date", "amount", "pi_family_name", "products_json", "members_json", "abstract"):
        print(f"    - With {col}: {df[col].notna().sum():,}")
    return out


def upload_to_s3(local_path: Path) -> bool:
    import shutil
    aws = shutil.which("aws")
    if not aws:
        print("  [ERROR] AWS CLI not found")
        return False
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"\n  [UPLOAD] {local_path.name} -> {s3_uri}")
    try:
        subprocess.run([aws, "s3", "cp", str(local_path), s3_uri],
                       capture_output=True, text=True, check=True)
        print("  [SUCCESS] Upload complete")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] Upload failed: {e.stderr}")
        return False


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="KAKEN official-API harvest to S3")
    parser.add_argument("--output-dir", type=Path, default=Path("./kaken_data"))
    parser.add_argument("--resume", action="store_true",
                        help="Skip year/page XML files already on disk")
    parser.add_argument("--years", type=str, default=None,
                        help="Fiscal-year range like 2023-2025 (default: 1964-current)")
    parser.add_argument("--parse-only", action="store_true",
                        help="Skip harvest; parse cached XML")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()

    this_fy = datetime.now().year  # close enough; FY overlap is harmless
    if args.years:
        m = re.match(r"(\d{4})-(\d{4})$", args.years)
        if not m:
            sys.exit("[ERROR] --years must look like 2023-2025")
        years = range(int(m.group(1)), int(m.group(2)) + 1)
    else:
        years = range(FIRST_FISCAL_YEAR, this_fy + 1)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    xml_dir = args.output_dir / "xml"

    print("=" * 60)
    print("KAKEN official-API to S3 Pipeline")
    print("=" * 60)
    print(f"Years: {years.start}-{years.stop - 1}")
    print(f"Output: {args.output_dir.absolute()}")

    if not args.parse_only:
        appid = get_appid()
        harvest_all(years, appid, xml_dir, resume=args.resume)

    df = parse_all(xml_dir)
    parquet_path = save_parquet(df, args.output_dir)

    if not args.skip_upload:
        if not upload_to_s3(parquet_path):
            print(f"\n[WARNING] Manual upload: aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")

    print("\nNext step: run notebooks/awards/CreateKAKENAwards.ipynb, then CreateKAKENWorkAwards.ipynb")


if __name__ == "__main__":
    main()
