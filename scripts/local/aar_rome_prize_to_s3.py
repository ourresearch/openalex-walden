#!/usr/bin/env python3
"""
American Academy in Rome Rome Prize Fellows to S3
=================================================

Downloads Rome Prize fellow records from the American Academy in Rome's
official Drupal people directory. The all-years listing gives one profile row
per fellow profile, grouped by discipline and source year; each detail page
adds the named Rome Prize, fellowship dates, profession/affiliation text,
project title, and project description.

Source authority
----------------
aarome.org is the awarding body's own site. The relevant public directory is:

    https://www.aarome.org/people/rome-prize-fellows?field_affil_year_target_id=All

The listing is server-rendered Drupal HTML. Detail pages are linked directly
from the official directory and are also server-rendered.

Amount and currency
-------------------
The source does not publish per-fellow award amounts in the directory or on
fellow profile pages. Amount and currency ship as NULL under the prize /
fellowship-pattern section 6.7 waiver, matching existing non-monetary or
undisclosed prize/fellowship ingests.

Output
------
s3://openalex-ingest/awards/aar_rome_prize/aar_rome_prize_fellows.parquet

Usage
-----
    python aar_rome_prize_to_s3.py --skip-upload
    python aar_rome_prize_to_s3.py --limit 10 --skip-upload
    python aar_rome_prize_to_s3.py --skip-download --skip-upload
    python aar_rome_prize_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests beautifulsoup4 boto3
"""

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

# Windows UTF-8 compatibility shim, matching the awards scraper fleet.
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


# =============================================================================
# Configuration
# =============================================================================

SITE_BASE = "https://www.aarome.org"
LISTING_URL = f"{SITE_BASE}/people/rome-prize-fellows?field_affil_year_target_id=All"

FUNDER_ID = 4320320895
FUNDER_DISPLAY_NAME = "American Academy in Rome"
PROVENANCE = "aar_rome_prize_fellows"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/aar_rome_prize/aar_rome_prize_fellows.parquet"

USER_AGENT = "openalex-walden-aar-rome-prize-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25


# =============================================================================
# HTTP helper
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 60) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.get(url, timeout=timeout)
    _last_request_t = time.monotonic()
    return resp


def _clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = unescape(re.sub(r"\s+", " ", value)).strip()
    return text or None


def _slugify(value: str) -> str:
    value = unescape(value).lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


# =============================================================================
# Listing parser
# =============================================================================

def parse_listing(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.select_one(".people-current")
    if not container:
        raise RuntimeError("Could not find .people-current listing container")

    rows: list[dict] = []
    for heading in container.find_all("h3", recursive=False):
        discipline = _clean_text(heading.get_text(" ", strip=True))
        profile_container = heading.find_next_sibling("div", class_="profile-container")
        if not discipline or profile_container is None:
            continue
        for profile in profile_container.select(":scope > .profile"):
            title_el = profile.select_one(".title")
            year_el = profile.select_one(".field-affil-year")
            link_el = profile.select_one(".card-link a[href]") or profile.select_one("a[href]")
            name = _clean_text(title_el.get_text(" ", strip=True) if title_el else None)
            source_year = _clean_text(year_el.get_text(" ", strip=True) if year_el else None)
            href = link_el.get("href") if link_el else None
            if not name or not source_year or not href:
                continue
            rows.append({
                "source_discipline": discipline,
                "source_year": source_year,
                "recipient_name": name,
                "profile_path": href,
                "landing_page_url": urljoin(SITE_BASE, href),
            })

    if not rows:
        raise RuntimeError("Listing parser found zero Rome Prize fellow profiles")
    return rows


# =============================================================================
# Detail-page parser
# =============================================================================

def _field_text(soup: BeautifulSoup, selector: str) -> Optional[str]:
    el = soup.select_one(selector)
    if not el:
        return None
    return _clean_text(el.get_text(" ", strip=True))


def parse_detail(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    profile = soup.select_one("article.profile.wrapper") or soup.select_one(".profile.wrapper")
    if not profile:
        profile = soup

    name = _field_text(soup, "article.profile.wrapper h2 span") or _field_text(soup, "article.profile.wrapper h2")
    fellowship = _field_text(soup, ".field-people-affiliations > .field__item .field-affil-fellowship-name .field__item")
    date_range = _field_text(soup, ".field-people-affiliations > .field__item .field-affil-dates .field__item")
    profession = _field_text(soup, ".field-people-professions .field__item")
    project_title = _field_text(soup, ".field-people-project-title .field__item")
    project_description = _field_text(soup, ".field-people-project-description .field__item")

    if project_description and len(project_description) > 2500:
        project_description = project_description[:2500].rsplit(" ", 1)[0]

    return {
        "detail_name": name,
        "fellowship_name": fellowship,
        "date_range": date_range,
        "profession": profession,
        "project_title": project_title,
        "project_description": project_description,
    }


MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


def _parse_one_date(text: str, default_year: Optional[int]) -> Optional[str]:
    text = _clean_text(text)
    if not text:
        return None
    m = re.match(r"([A-Za-z]+)\s+(\d{1,2})(?:,\s*(\d{4}))?$", text)
    if not m:
        return None
    month = MONTHS.get(m.group(1).lower())
    day = int(m.group(2))
    year = int(m.group(3)) if m.group(3) else default_year
    if not month or not year:
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


def parse_date_range(date_range: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not date_range:
        return None, None
    value = date_range.replace("\u2013", "-").replace("\u2014", "-")
    parts = [p.strip() for p in value.split("-", 1)]
    if len(parts) == 1:
        one = _parse_one_date(parts[0], None)
        return one, one
    end_year_match = re.search(r"(\d{4})\s*$", parts[1])
    end_year = int(end_year_match.group(1)) if end_year_match else None
    start = _parse_one_date(parts[0], end_year)
    end = _parse_one_date(parts[1], None)
    return start, end


# =============================================================================
# Smoke test and download
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: AAR all-years listing and one detail page")
    print("=" * 60)
    resp = _http_get(LISTING_URL)
    print(f"  listing HTTP {resp.status_code}; {len(resp.text):,} bytes")
    resp.raise_for_status()
    rows = parse_listing(resp.text)
    print(f"  listing profiles: {len(rows)}")
    if len(rows) < 500:
        print("[ERROR] expected 500+ Rome Prize profiles; selector or query may have changed")
        sys.exit(3)
    sample = rows[0]
    d_resp = _http_get(sample["landing_page_url"])
    print(f"  sample detail HTTP {d_resp.status_code}; {len(d_resp.text):,} bytes")
    d_resp.raise_for_status()
    detail = parse_detail(d_resp.text)
    print(
        "  sample: "
        f"{sample['recipient_name']} / {sample['source_year']} / "
        f"{sample['source_discipline']} / {detail.get('fellowship_name')}"
    )
    if not detail.get("fellowship_name"):
        print("[ERROR] detail page did not expose fellowship name")
        sys.exit(4)


def download_records(output_dir: Path, limit: Optional[int]) -> Path:
    print("\n" + "=" * 60)
    print("Step 1: Download AAR listing + fellow detail pages")
    print("=" * 60)
    listing_resp = _http_get(LISTING_URL)
    listing_resp.raise_for_status()
    listing_rows = parse_listing(listing_resp.text)
    if limit:
        listing_rows = listing_rows[:limit]
        print(f"  [LIMIT] only fetching first {limit} fellow profiles")
    print(f"  detail pages to fetch: {len(listing_rows)}")

    records: list[dict] = []
    for i, row in enumerate(listing_rows, 1):
        try:
            resp = _http_get(row["landing_page_url"])
            if resp.status_code != 200:
                print(f"  [{i}/{len(listing_rows)}] HTTP {resp.status_code}: {row['landing_page_url']}")
                detail = {}
            else:
                detail = parse_detail(resp.text)
        except Exception as exc:
            print(f"  [{i}/{len(listing_rows)}] error {exc}: {row['landing_page_url']}")
            detail = {}
        records.append({**row, **detail})
        if i <= 5 or i % 25 == 0 or i == len(listing_rows):
            print(
                f"  [{i}/{len(listing_rows)}] "
                f"{row['source_year']} {row['source_discipline']}: {row['recipient_name']}"
            )

    raw_path = output_dir / "aar_rome_prize_raw.json"
    raw_path.write_text(json.dumps(records, ensure_ascii=False, indent=2))
    print(f"\n  cached {len(records)} raw records to {raw_path}")
    return raw_path


# =============================================================================
# Build DataFrame
# =============================================================================

_DEGREE_SUFFIXES = {"PhD", "Ph.D.", "Ph.D", "MD", "M.D.", "DPhil", "ScD",
                    "Jr.", "Jr", "Sr.", "Sr", "II", "III", "IV"}
_HONORIFIC_PREFIXES_RE = re.compile(
    r"^(?:Dr\.?|Prof\.?|Professor|Mr\.?|Ms\.?|Mrs\.?|Sir|Dame)\s+",
    re.I,
)


def split_name(full_name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not full_name:
        return None, None
    name = _HONORIFIC_PREFIXES_RE.sub("", full_name).strip()
    parts = [p.strip() for p in name.replace(";", ",").split(",")]
    name = parts[0].strip()
    toks = name.split()
    suffixes = {s.rstrip(".") for s in _DEGREE_SUFFIXES}
    while toks and toks[-1].rstrip(".") in suffixes:
        toks.pop()
    if not toks:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


def split_recipients(name: str) -> list[str]:
    parts = re.split(r"\s+(?:&|and)\s+", name)
    return [_clean_text(p) for p in parts if _clean_text(p)]


def build_dataframe(raw_path: Path) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Build flat DataFrame")
    print("=" * 60)
    records = json.loads(raw_path.read_text())
    rows: list[dict] = []
    seen_ids: set[str] = set()
    for r in records:
        recipient_name = r.get("detail_name") or r.get("recipient_name")
        source_year = r.get("source_year")
        profile_path = r.get("profile_path") or ""
        profile_slug = profile_path.rstrip("/").rsplit("/", 1)[-1] or _slugify(recipient_name or "")
        discipline_slug = _slugify(r.get("source_discipline") or "rome-prize")
        funder_award_id = f"aar-rome-prize-{source_year}-{discipline_slug}-{profile_slug}"
        if funder_award_id in seen_ids:
            raise RuntimeError(f"funder_award_id collision: {funder_award_id}")
        seen_ids.add(funder_award_id)

        start_date, end_date = parse_date_range(r.get("date_range"))
        recipients = split_recipients(recipient_name or "")
        lead_name = recipients[0] if recipients else recipient_name
        co_name = recipients[1] if len(recipients) > 1 else None
        lead_given, lead_family = split_name(lead_name)
        co_given, co_family = split_name(co_name)

        project_title = r.get("project_title")
        display_name = (
            project_title
            or f"Rome Prize {source_year} - {recipient_name}"
        )
        fellowship_name = r.get("fellowship_name")
        description_parts = []
        if r.get("project_description"):
            description_parts.append(r["project_description"])
        if fellowship_name:
            description_parts.append(f"Fellowship: {fellowship_name}.")
        description = " ".join(description_parts) or None

        rows.append({
            "funder_award_id": funder_award_id,
            "provenance": PROVENANCE,
            "source_url": LISTING_URL,
            "source_year": source_year,
            "source_discipline": r.get("source_discipline"),
            "profile_slug": profile_slug,
            "recipient_name": recipient_name,
            "recipient_count": str(len(recipients) if recipients else 1),
            "lead_name": lead_name,
            "lead_given_name": lead_given,
            "lead_family_name": lead_family,
            "co_lead_name": co_name,
            "co_lead_given_name": co_given,
            "co_lead_family_name": co_family,
            "fellowship_name": fellowship_name,
            "profession": r.get("profession"),
            "project_title": project_title,
            "project_description": r.get("project_description"),
            "display_name": display_name,
            "description": description,
            "date_range": r.get("date_range"),
            "start_date": start_date,
            "end_date": end_date,
            "amount": None,
            "currency": None,
            "funding_type": "fellowship",
            "landing_page_url": r.get("landing_page_url"),
        })

    df = pd.DataFrame.from_records(rows)
    if df.empty:
        raise RuntimeError("Built zero rows")

    def pct(col: str) -> str:
        n = df[col].notna().sum()
        return f"{n}/{len(df)} ({n * 100 / len(df):.1f}%)"

    print(f"  rows: {len(df)}")
    print(f"  unique funder_award_id: {df['funder_award_id'].nunique()}")
    print(f"  source year range: {df['source_year'].min()}-{df['source_year'].max()}")
    print(f"  discipline count: {df['source_discipline'].nunique()}")
    print(f"  project_title coverage: {pct('project_title')}")
    print(f"  project_description coverage: {pct('project_description')}")
    print(f"  profession coverage: {pct('profession')}")
    print(f"  fellowship_name coverage: {pct('fellowship_name')}")
    print(f"  start_date coverage: {pct('start_date')}")
    print(f"  multi-recipient rows: {(df['recipient_count'] != '1').sum()}")
    print("\n  Discipline distribution:")
    print(df["source_discipline"].value_counts().sort_index().to_string())
    print("\n  Source year distribution:")
    print(df["source_year"].value_counts().sort_index().tail(10).to_string())

    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "aar_rome_prize_fellows.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    print(f"  [OK] wrote {len(df)} rows to {parquet_path} ({parquet_path.stat().st_size / 1024:.1f} KB)")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for the runbook section 1.4 shrink check; "
            "rerun with --skip-upload for local validation"
        ) from exc

    client = boto3.client("s3")
    print(f"  section 1.4 shrink check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            print("    no existing parquet - first ingest")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest")
        return True

    previous_path = output_dir / "_previous_aar_rome_prize.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(previous_path))
        previous_count = len(pd.read_parquet(previous_path))
    except Exception as exc:
        print(f"    [ERROR] could not read existing parquet ({exc}); aborting upload")
        return False
    finally:
        previous_path.unlink(missing_ok=True)

    print(f"    previous count: {previous_count}; new count: {new_count}")
    if new_count < previous_count:
        if allow_shrink:
            print("    [OVERRIDE] new corpus is smaller but --allow-shrink is set")
            return True
        print(f"    [ERROR] refusing to shrink corpus ({previous_count} -> {new_count})")
        return False
    print("    [OK] new corpus is not smaller")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path, allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 4: Upload to S3")
    print("=" * 60)
    if not check_no_shrink(len(df), allow_shrink, output_dir):
        return False
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    try:
        subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
        print(f"  [OK] uploaded to {s3_uri}")
        return True
    except FileNotFoundError:
        print("[ERROR] aws CLI not found")
        return False
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] aws s3 cp failed with exit {exc.returncode}")
        return False


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download American Academy in Rome Rome Prize fellows to parquet/S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--limit", type=int, default=None, help="Limit detail pages for smoke tests")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/aar_rome_prize"))
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("American Academy in Rome Rome Prize Fellows to S3")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Provenance: {PROVENANCE}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    raw_path = args.output_dir / "aar_rome_prize_raw.json"
    if args.skip_download:
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} is missing")
            sys.exit(6)
        print(f"\n[SKIP] reusing cached raw JSON at {raw_path}")
    else:
        raw_path = download_records(args.output_dir, args.limit)

    df = build_dataframe(raw_path)
    parquet_path = write_parquet(df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; no S3 action performed.")
        print(f"  Admin upload command: aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")
    else:
        ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
        if not ok:
            sys.exit(7)

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print("Next: run notebooks/awards/CreateAARRomePrizeAwards.ipynb in Databricks.")
    print("=" * 60)


if __name__ == "__main__":
    main()
