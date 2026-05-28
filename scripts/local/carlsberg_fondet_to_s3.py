#!/usr/bin/env python3
"""
Carlsbergfondet grants to S3 Data Pipeline
==========================================

Downloads grant pages from the official Carlsberg Foundation website and
normalizes them to the OpenAlex awards staging parquet.

Source authority
----------------
`carlsbergfondet.dk` is the awarding body's own public website. Its sitemap
at `https://www.carlsbergfondet.dk/en/sitemap.xml` exposes the grant detail
pages under `/en/what-we-have-funded/{slug}/`. Each detail page contains
labeled fields for applicant, institution, amount, year, and type of grant.
No third-party source is used.

Amount and currency
-------------------
The site publishes per-grant amounts as labels such as `DKK 327,181`. These
are parsed to numeric `amount` values with `currency='DKK'` when present.

Output
------
s3://openalex-ingest/awards/carlsberg_fondet/carlsberg_fondet_grants.parquet

Usage
-----
    python carlsberg_fondet_to_s3.py                    # full run
    python carlsberg_fondet_to_s3.py --skip-upload      # local dev
    python carlsberg_fondet_to_s3.py --skip-download    # reuse cached JSON
    python carlsberg_fondet_to_s3.py --limit 10         # smoke test
    python carlsberg_fondet_to_s3.py --allow-shrink     # override runbook shrink-check

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
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

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


# =============================================================================
# Configuration
# =============================================================================

SITE_BASE = "https://www.carlsbergfondet.dk"
SITEMAP_URL = f"{SITE_BASE}/en/sitemap.xml"

# Awarding body: Carlsbergfondet.
# Verified in OpenAlex as F4320321504, country DK, ROR 01kpjmx04,
# DOI 10.13039/501100002808.
FUNDER_ID = 4320321504
FUNDER_DISPLAY_NAME = "Carlsbergfondet"
PROVENANCE = "carlsberg_fondet"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/carlsberg_fondet/carlsberg_fondet_grants.parquet"

USER_AGENT = "openalex-walden-carlsbergfondet-ingest/1.0 (+https://openalex.org)"

# The full crawl is 3,164 pages; 5 requests/sec is polite for this static site.
MIN_REQUEST_INTERVAL_S = 0.20
EXPECTED_MIN_FULL_ROWS = 3100

LABEL_MAP = {
    "name of applicant": "applicant_name",
    "institution": "institution",
    "amount": "amount_raw",
    "year": "year_raw",
    "type of grant": "type_of_grant",
}


# =============================================================================
# HTTP helper
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 60) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT})
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.get(url, timeout=timeout)
    _last_request_t = time.monotonic()
    return resp


def clean_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = unescape(str(value))
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


# =============================================================================
# Sitemap + page parsing
# =============================================================================


def extract_grant_urls(sitemap_xml: str) -> list[str]:
    """Return unique Carlsberg grant detail URLs from the official sitemap."""
    urls: list[str] = []
    try:
        root = ET.fromstring(sitemap_xml.encode("utf-8"))
        for loc in root.findall(".//{*}loc"):
            if loc.text:
                urls.append(loc.text.strip())
    except ET.ParseError:
        urls = re.findall(r"<loc>([^<]+)</loc>", sitemap_xml)

    out: list[str] = []
    seen: set[str] = set()
    for raw_url in urls:
        url = clean_text(raw_url)
        if not url:
            continue
        if "/en/what-we-have-funded/" not in url:
            continue
        slug = slug_from_url(url)
        if not slug or slug == "what-we-have-funded":
            continue
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out


def slug_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    return path.rsplit("/", 1)[-1]


def extract_labeled_facts(soup: BeautifulSoup) -> dict[str, Optional[str]]:
    """Extract tag-agnostic label/value pairs from a Carlsberg detail page."""
    facts: dict[str, Optional[str]] = {v: None for v in LABEL_MAP.values()}
    for node in soup.find_all(string=True):
        label_text = clean_text(node)
        if not label_text:
            continue
        key = LABEL_MAP.get(label_text.casefold())
        if not key or facts.get(key):
            continue

        label_tag = getattr(node, "parent", None)
        if label_tag is None:
            continue

        # The current template is <div><p>Label</p><p>Value</p></div>.
        # Walk upward a few levels and choose the smallest container that
        # starts with this label and has exactly one following value.
        container = label_tag.parent
        for _ in range(4):
            if container is None:
                break
            texts = [clean_text(t) for t in container.stripped_strings]
            texts = [t for t in texts if t]
            if texts and texts[0].casefold() == label_text.casefold() and len(texts) >= 2:
                facts[key] = texts[1]
                break
            container = container.parent
    return facts


def parse_amount(amount_raw: Optional[str]) -> tuple[Optional[float], Optional[str]]:
    if not amount_raw:
        return None, None
    text = clean_text(amount_raw) or ""
    currency_match = re.search(r"\b([A-Z]{3})\b", text)
    currency = currency_match.group(1) if currency_match else None
    number_match = re.search(r"\d[\d,.\s]*", text)
    if not number_match:
        return None, currency
    number_text = number_match.group(0).strip()
    # English Carlsberg pages use commas as thousands separators.
    normalized = number_text.replace(",", "").replace(" ", "")
    try:
        return float(normalized), currency
    except ValueError:
        return None, currency


def parse_year(year_raw: Optional[str]) -> Optional[int]:
    if not year_raw:
        return None
    m = re.search(r"\b(19|20)\d{2}\b", year_raw)
    return int(m.group(0)) if m else None


_DEGREE_SUFFIXES = {
    "PhD", "Ph.D.", "MD", "M.D.", "DPhil", "Dr.phil.", "Jr.", "Sr.", "II", "III", "IV"
}
_PREFIX_TITLES = {"Dr", "Dr.", "Prof", "Prof.", "Professor", "Associate", "Assistant"}

# Tokens that indicate the applicant_name is an institution rather than a
# person. Used to prevent the §6.4 PI bug seen on 2026-05-27 where
# "Carlsbergfondet", "Det Danske Institut i Rom", and "Videnskabernes Selskab /
# Royal Academy" all got split into given/family columns (oxjobs #267 sibling).
# Detection is token-level (case-insensitive, punctuation/slash stripped).
_INSTITUTIONAL_NAME_MARKERS = {
    "carlsbergfondet", "fondet", "institut", "institute", "instituttet",
    "universitet", "university", "universiteit", "selskab", "selskabs",
    "selskabet", "society", "akademi", "academy", "royal", "kgl",
    "kongelige", "foundation", "fond", "fonden", "fonds", "center", "centre",
    "centeret", "centret", "museum", "museet", "school", "skole", "skolen",
    "forskning", "forskningsråd", "forskningsraad", "forum", "hospital",
    "sygehus", "biblioteket", "bibliotek", "library", "council", "ràd",
    "raad", "association", "forening", "foreningen", "danske", "danish",
    "polytechnic", "company", "institution",
}


def is_institutional_name(name: Optional[str]) -> bool:
    """Return True if `name` looks like an institution, not a person."""
    if not name:
        return False
    # Split on whitespace AND `/` (joins like "Royal / Academy").
    raw_tokens = re.split(r"[\s/]+", name)
    tokens = {t.lower().strip(".,;:()[]") for t in raw_tokens if t}
    return bool(tokens & _INSTITUTIONAL_NAME_MARKERS)


def split_name(full_name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Conservative applicant-name split for OpenAlex investigator structs."""
    if not full_name:
        return None, None
    tokens = full_name.replace(",", " ").split()
    prefix_tokens = {t.rstrip(".") for t in _PREFIX_TITLES}
    while tokens and tokens[0].rstrip(".") in prefix_tokens:
        tokens.pop(0)
    suffix_tokens = {s.rstrip(".,") for s in _DEGREE_SUFFIXES}
    while tokens and tokens[-1].rstrip(".,") in suffix_tokens:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_grant_page(html: str, url: str, http_status: int) -> dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title = clean_text(h1.get_text(" ", strip=True)) if h1 else None
    if not title and soup.title:
        title = clean_text(re.sub(r"\s*\|\s*Carlsbergfondet.*$", "", soup.title.get_text(" ", strip=True)))

    canonical = None
    canonical_tag = soup.find("link", rel=lambda rel: rel and "canonical" in rel)
    if canonical_tag and canonical_tag.get("href"):
        canonical = clean_text(canonical_tag["href"])

    facts = extract_labeled_facts(soup)
    amount, currency = parse_amount(facts.get("amount_raw"))
    year = parse_year(facts.get("year_raw"))
    applicant_name = facts.get("applicant_name")
    # Org-level grants (e.g. Postdoctoral Networking Day to "Carlsbergfondet"
    # itself, or grants to "Det Danske Institut i Rom") should leave given/
    # family NULL so we don't pollute the PI columns with institution names.
    # The institution slot still gets populated below via facts["institution"].
    if is_institutional_name(applicant_name):
        given_name, family_name = None, None
    else:
        given_name, family_name = split_name(applicant_name)

    return {
        "source_url": url,
        "landing_page_url": canonical or url,
        "slug": slug_from_url(url),
        "http_status": str(http_status),
        "title": title,
        "applicant_name": facts.get("applicant_name"),
        "given_name": given_name,
        "family_name": family_name,
        "institution": facts.get("institution"),
        "amount_raw": facts.get("amount_raw"),
        "amount": amount,
        "currency": currency,
        "year_raw": facts.get("year_raw"),
        "year": year,
        "type_of_grant": facts.get("type_of_grant"),
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }


def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: sitemap reachable + one grant parses")
    print("=" * 60)
    r = _http_get(SITEMAP_URL)
    r.raise_for_status()
    urls = extract_grant_urls(r.text)
    print(f"  grant detail URLs: {len(urls)}")
    if len(urls) < EXPECTED_MIN_FULL_ROWS:
        print(f"  [WARN] detail URL count {len(urls)} below expected {EXPECTED_MIN_FULL_ROWS}")
    if not urls:
        print("[ERROR] sitemap returned 0 grant detail URLs")
        sys.exit(3)
    page = _http_get(urls[0])
    page.raise_for_status()
    rec = parse_grant_page(page.text, urls[0], page.status_code)
    print(f"  sample: {urls[0]}")
    print(f"  title={bool(rec.get('title'))} applicant={bool(rec.get('applicant_name'))} "
          f"amount={rec.get('amount_raw')} year={rec.get('year')} "
          f"type={rec.get('type_of_grant')}")
    missing = [k for k in ("title", "applicant_name", "amount", "year", "type_of_grant") if not rec.get(k)]
    if missing:
        print(f"[ERROR] sample parse missed required fields: {missing}")
        sys.exit(4)


# =============================================================================
# Download + normalization
# =============================================================================


def download_grants(output_dir: Path, limit: Optional[int]) -> Path:
    print("\n" + "=" * 60)
    print("Step 1: Download sitemap + scrape grant detail pages")
    print("=" * 60)
    r = _http_get(SITEMAP_URL)
    r.raise_for_status()
    urls = extract_grant_urls(r.text)
    print(f"  {len(urls)} grant detail URLs")
    if limit:
        urls = urls[:limit]
        print(f"  [LIMIT] only scraping first {limit}")

    rows: list[dict[str, Optional[str]]] = []
    for i, url in enumerate(urls, 1):
        try:
            page = _http_get(url)
            if page.status_code != 200:
                print(f"  [{i}/{len(urls)}] {url} -> HTTP {page.status_code}; URL-only row")
                rec = {
                    "source_url": url,
                    "landing_page_url": url,
                    "slug": slug_from_url(url),
                    "http_status": str(page.status_code),
                    "downloaded_at": datetime.now(timezone.utc).isoformat(),
                }
            else:
                rec = parse_grant_page(page.text, url, page.status_code)
        except Exception as exc:
            print(f"  [{i}/{len(urls)}] {url} -> error {exc}; URL-only row")
            rec = {
                "source_url": url,
                "landing_page_url": url,
                "slug": slug_from_url(url),
                "http_status": "error",
                "parse_error": str(exc),
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
            }
        rows.append(rec)
        if i % 100 == 0 or i == len(urls):
            n_title = sum(1 for row in rows if row.get("title"))
            n_applicant = sum(1 for row in rows if row.get("applicant_name"))
            n_amount = sum(1 for row in rows if row.get("amount") is not None)
            n_year = sum(1 for row in rows if row.get("year") is not None)
            print(
                f"  [{i}/{len(urls)}] coverage so far: "
                f"title={n_title} applicant={n_applicant} amount={n_amount} year={n_year}"
            )

    raw_path = output_dir / "carlsberg_fondet_raw.json"
    raw_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2))
    print(f"\n  cached {len(rows)} records to {raw_path}")
    return raw_path


def validate_dataframe(df: pd.DataFrame, full_run: bool) -> None:
    if df.empty:
        raise RuntimeError("No rows built from Carlsberg raw JSON")

    if df["funder_award_id"].duplicated().any():
        dupes = df.loc[df["funder_award_id"].duplicated(), "funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values: {dupes}")

    if full_run and len(df) < EXPECTED_MIN_FULL_ROWS:
        raise RuntimeError(
            f"Full Carlsberg corpus unexpectedly small: {len(df)} rows "
            f"(expected at least {EXPECTED_MIN_FULL_ROWS})"
        )

    checks = {
        "display_name": 0.95,
        "applicant_name": 0.80,
        "amount": 0.80,
        "year": 0.95,
        "type_of_grant": 0.95,
        "landing_page_url": 1.00,
    }
    for col, min_cov in checks.items():
        coverage = df[col].notna().mean()
        print(f"  coverage {col}: {coverage:.1%}")
        if coverage < min_cov:
            raise RuntimeError(
                f"Coverage for {col} is {coverage:.1%}, below required {min_cov:.0%}; "
                "parser/source changed or this is not safe to ship"
            )


def build_dataframe(raw_path: Path, full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Build flat DataFrame")
    print("=" * 60)
    records = json.loads(raw_path.read_text())
    rows: list[dict[str, object]] = []
    skipped_test_rows = 0
    for rec in records:
        slug = rec.get("slug")
        if not slug:
            continue
        title = rec.get("title")
        amount = rec.get("amount")
        if (
            isinstance(title, str)
            and title.upper().startswith("TEST ")
            and amount in (0, 0.0, "0", "0.0")
        ):
            # Official sitemap currently includes cf22-0030, a DKK 0 test page.
            skipped_test_rows += 1
            continue
        year = rec.get("year")
        start_date = f"{year}-01-01" if year else None
        end_date = f"{year}-12-31" if year else None
        # Re-apply the institutional-name guard at build time so cached raw
        # JSONs from before the fix (2026-05-27 oxjobs #267 sibling) are also
        # cleaned: if applicant_name is an institution, null given/family.
        applicant_name = rec.get("applicant_name")
        if is_institutional_name(applicant_name):
            given_name, family_name = None, None
        else:
            given_name = rec.get("given_name")
            family_name = rec.get("family_name")
        rows.append({
            "funder_award_id": f"carlsberg-fondet-{slug}",
            "slug": slug,
            "display_name": title,
            "description": None,
            "applicant_name": applicant_name,
            "given_name": given_name,
            "family_name": family_name,
            "institution": rec.get("institution"),
            "amount": amount,
            "currency": rec.get("currency"),
            "amount_raw": rec.get("amount_raw"),
            "year": year,
            "start_date": start_date,
            "end_date": end_date,
            "type_of_grant": rec.get("type_of_grant"),
            "landing_page_url": rec.get("landing_page_url") or rec.get("source_url"),
            "source_url": rec.get("source_url"),
            "http_status": rec.get("http_status"),
            "year_raw": rec.get("year_raw"),
            "downloaded_at": rec.get("downloaded_at"),
            "declined": False,
        })
    df = pd.DataFrame.from_records(rows)
    if skipped_test_rows:
        print(f"  skipped {skipped_test_rows} official sitemap test row(s)")
    validate_dataframe(df, full_run=full_run)

    print(f"\n  rows: {len(df)}")
    print("\n  By type_of_grant:")
    print(df.groupby("type_of_grant").size().sort_values(ascending=False).to_string())
    print("\n  By year:")
    print(df.groupby("year").size().sort_index().tail(15).to_string())

    # Runbook write-site safety: cast everything to pandas string before parquet.
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with runbook shrink-check)
# =============================================================================


def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "carlsberg_fondet_grants.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    size_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df)} rows ({size_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook section 1.4: refuse to overwrite S3 with a smaller corpus."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for the shrink-check; rerun with --skip-upload to bypass"
        ) from exc

    client = boto3.client("s3")
    print(f"  Re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("    no existing parquet: first ingest, no shrink check")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest")
        return True

    prev_path = output_dir / "_prev_carlsberg_fondet_grants.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as exc:
        print(f"    [ERROR] could not read existing parquet ({exc}); aborting upload")
        return False
    finally:
        prev_path.unlink(missing_ok=True)

    print(f"    previous count: {prev_count}   new count: {new_count}")
    if new_count < prev_count:
        if allow_shrink:
            print("    [OVERRIDE] new corpus is smaller but --allow-shrink is set")
            return True
        print(
            f"\n[ERROR] refusing to shrink corpus ({prev_count} -> {new_count}). "
            "Investigate first or rerun with --allow-shrink."
        )
        return False
    print("    [OK] new corpus is not smaller; safe to overwrite")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path, allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 4: Upload to S3 (with shrink check)")
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
        print("[ERROR] aws CLI not found")
        return False
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] aws s3 cp failed (exit {exc.returncode})")
        return False


# =============================================================================
# Main
# =============================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/carlsberg_fondet"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse carlsberg_fondet_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Do not push parquet to S3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only scrape first N grant pages for smoke testing")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override the runbook shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Carlsbergfondet -> S3 Pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "carlsberg_fondet_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        print(f"\n[SKIP] reusing {raw_path}")
    else:
        raw_path = download_grants(args.output_dir, args.limit)

    full_run = args.limit is None
    df = build_dataframe(raw_path, full_run=full_run)
    parquet_path = write_parquet(df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload; manual upload command for admin:")
        print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")
    else:
        ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
        if not ok:
            sys.exit(7)

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print("Next: notebooks/awards/CreateCarlsbergFondetAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
