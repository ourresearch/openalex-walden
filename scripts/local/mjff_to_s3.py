#!/usr/bin/env python3
"""
Michael J. Fox Foundation Funded Studies -> S3 data pipeline.

This is a grant/research-pattern ingest for the Michael J. Fox Foundation for
Parkinson's Research (OpenAlex funder F4320306136, DOI 10.13039/100000864).

Source authority
----------------
The source is the foundation's own Funded Studies database:

    https://www.michaeljfox.org/funded-studies

The listing page calls a first-party Drupal AJAX endpoint:

    https://www.michaeljfox.org/ajax/funded-grants?page=N

Each AJAX page returns six rendered grant cards plus a "Load More" link when
another page exists. Each card links to an official `/grant/{slug}` detail page,
and those detail pages link to official `/researcher/{slug}` profile pages.

Why this source is acceptable:
  - It is first-party MJFF content, not a third-party index.
  - The endpoint exposes the same cards a browser receives, without auth.
  - Detail/profile pages expose richer text and location/affiliation data than
    the listing cards, so the script follows those links rather than shipping
    listing snippets only.

Amount handling
---------------
The public funded-study pages do not expose per-grant award amounts. MJFF
publishes aggregate funding totals and occasional news-round amounts, but those
are not row-level amounts. Following the runbook source-authority rule and the
existing HHMI/CIFAR/Damon Runyon/Schmidt precedent, amount and currency remain
NULL with a notebook waiver.

Output
------
    s3://openalex-ingest/awards/mjff/mjff_funded_studies.parquet

Usage
-----
    python scripts/local/mjff_to_s3.py --limit 12 --skip-upload
    python scripts/local/mjff_to_s3.py --skip-upload
    python scripts/local/mjff_to_s3.py --skip-download --skip-upload
    python scripts/local/mjff_to_s3.py --allow-shrink

Requirements
------------
    pip install pandas pyarrow requests beautifulsoup4 boto3
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup, NavigableString, Tag

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


BASE_URL = "https://www.michaeljfox.org"
LISTING_URL = f"{BASE_URL}/funded-studies"
AJAX_URL = f"{BASE_URL}/ajax/funded-grants"

FUNDER_ID = 4320306136
FUNDER_DISPLAY_NAME = "Michael J. Fox Foundation for Parkinson's Research"
FUNDER_DOI = "10.13039/100000864"
PROVENANCE = "mjff_funded_studies"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/mjff/mjff_funded_studies.parquet"

USER_AGENT = "openalex-walden-mjff-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.20
MAX_RETRIES = 4
REQUEST_TIMEOUT_S = 45


_session: Optional[requests.Session] = None
_last_request_t = 0.0


def clean_text(value: Any) -> Optional[str]:
    """Normalize whitespace and HTML entities without inventing data."""
    if value is None:
        return None
    text = unescape(str(value)).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def slug_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    return path.rsplit("/", 1)[-1]


def full_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    return urljoin(BASE_URL, url)


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/json,application/xhtml+xml,*/*;q=0.8",
                "Referer": LISTING_URL,
            }
        )
    return _session


def fetch_url(url: str, *, expect_json: bool = False) -> requests.Response:
    """Rate-limited GET with retries and chatty logging for Step 1 safety."""
    global _last_request_t
    session = _get_session()
    for attempt in range(1, MAX_RETRIES + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < MIN_REQUEST_INTERVAL_S:
            time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
        started = time.monotonic()
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT_S, allow_redirects=True)
            _last_request_t = time.monotonic()
            size = len(response.content)
            print(f"  GET {response.status_code} {size:>8} bytes {url}")
            if response.status_code >= 500:
                raise requests.HTTPError(f"{response.status_code} server response", response=response)
            response.raise_for_status()
            if expect_json:
                response.json()
            return response
        except Exception as exc:
            wait = min(2**attempt, 20)
            took = time.monotonic() - started
            print(f"  [retry {attempt}/{MAX_RETRIES}] {url} failed after {took:.1f}s: {exc}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(wait)
    raise RuntimeError(f"unreachable retry loop for {url}")


def cache_path(cache_dir: Path, prefix: str, url_or_key: str, suffix: str) -> Path:
    digest = hashlib.sha1(url_or_key.encode("utf-8")).hexdigest()[:16]
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "-", slug_from_url(url_or_key) or prefix).strip("-")[:80]
    return cache_dir / f"{prefix}_{safe}_{digest}.{suffix}"


def read_or_fetch_text(url: str, cache_dir: Path) -> str:
    path = cache_path(cache_dir, "html", url, "html")
    if path.exists():
        return path.read_text(encoding="utf-8")
    response = fetch_url(url)
    text = response.text
    path.write_text(text, encoding="utf-8")
    return text


def read_or_fetch_json(url: str, cache_dir: Path) -> dict[str, Any]:
    path = cache_path(cache_dir, "json", url, "json")
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    response = fetch_url(url, expect_json=True)
    data = response.json()
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return data


def parse_program_year(value: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    text = clean_text(value)
    if not text:
        return None, None
    match = re.match(r"^(?P<program>.+?),\s*(?P<year>\d{4})$", text)
    if match:
        return clean_text(match.group("program")), match.group("year")
    year = re.search(r"\b(19|20)\d{2}\b", text)
    program = clean_text(re.sub(r",?\s*\b(19|20)\d{2}\b", "", text))
    return program, year.group(0) if year else None


def parse_card(card: Tag, page: int, position: int) -> dict[str, Any]:
    meta = clean_text(card.select_one(".wideCard-section_main .txt_colorSlate5").get_text(" ", strip=True)) if card.select_one(".wideCard-section_main .txt_colorSlate5") else None
    program, year = parse_program_year(meta)

    title_el = card.select_one(".wideCard-section_main .txt_colorBlue5")
    title = clean_text(title_el.get_text(" ", strip=True) if title_el else None)

    summary_el = card.select_one(".wideCard-section_main .txt_serif")
    listing_summary = clean_text(summary_el.get_text(" ", strip=True) if summary_el else None)

    detail_link = card.select_one('a.btn[href^="/grant/"], a.btn_primary[href^="/grant/"]')
    detail_url = full_url(detail_link.get("href")) if detail_link else None

    researcher_links = []
    for a in card.select('a[href^="/researcher/"]'):
        href = full_url(a.get("href"))
        name = clean_text(a.get_text(" ", strip=True))
        if href and name:
            researcher_links.append({"name": name, "url": href, "slug": slug_from_url(href)})

    if not detail_url:
        raise ValueError(f"page {page} card {position} has no grant detail link")
    if not title:
        raise ValueError(f"page {page} card {position} has no title")

    return {
        "source_page": str(page),
        "source_page_position": str(position),
        "source_program_year": meta,
        "source_program": program,
        "source_year": year,
        "title": title,
        "listing_summary": listing_summary,
        "grant_url": detail_url,
        "grant_slug": slug_from_url(detail_url),
        "listing_researchers_json": json.dumps(researcher_links, ensure_ascii=False),
    }


def discover_listing_records(cache_dir: Path, limit: Optional[int] = None, max_pages: Optional[int] = None) -> list[dict[str, Any]]:
    print("\n" + "=" * 72)
    print("Step 1 discovery: MJFF funded-grants AJAX pages")
    print("=" * 72)
    records: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    page = 0

    while True:
        if max_pages is not None and page >= max_pages:
            print(f"Reached --max-pages={max_pages}; stopping discovery.")
            break
        url = f"{AJAX_URL}?page={page}"
        data = read_or_fetch_json(url, cache_dir)
        markup = data.get("markup") or ""
        soup = BeautifulSoup(markup, "html.parser")
        cards = soup.select("li.wideCards-item")
        next_link = soup.select_one('a[rel="next"][href*="page="]')
        print(f"  page {page}: {len(cards)} cards; next={'yes' if next_link else 'no'}")

        for i, card in enumerate(cards, start=1):
            row = parse_card(card, page, i)
            if row["grant_url"] in seen_urls:
                raise RuntimeError(f"duplicate grant URL in listing: {row['grant_url']}")
            seen_urls.add(row["grant_url"])
            records.append(row)
            if limit is not None and len(records) >= limit:
                print(f"Reached --limit={limit}; stopping discovery after {len(records)} records.")
                return records

        if not next_link:
            print("No next link present; reached endpoint terminus.")
            break
        page += 1

    if not records:
        raise RuntimeError("MJFF AJAX discovery returned zero grant cards")
    print(f"Discovered {len(records)} unique grant cards.")
    return records


def extract_labeled_sections(container: Tag) -> dict[str, str]:
    """Parse MJFF prose blocks where labels are <strong>Label</strong> nodes."""
    sections: dict[str, str] = {}
    for strong in container.find_all("strong"):
        label = clean_text(strong.get_text(" ", strip=True))
        if not label:
            continue
        parts: list[str] = []
        for sib in strong.next_siblings:
            if isinstance(sib, Tag) and sib.name == "strong":
                break
            if isinstance(sib, NavigableString):
                parts.append(str(sib))
            elif isinstance(sib, Tag):
                if sib.name == "br":
                    parts.append(" ")
                else:
                    parts.append(sib.get_text(" ", strip=True))
        value = clean_text(" ".join(parts).lstrip(": "))
        if value:
            key = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
            sections[key] = value
    return sections


def find_section_after_heading(soup: BeautifulSoup, heading_text: str) -> Optional[str]:
    for h in soup.find_all(["h2", "h3", "h4"]):
        if clean_text(h.get_text(" ", strip=True)) == heading_text:
            parent = h.find_parent(class_=re.compile(r"quarantine|tier|container")) or h.parent
            if parent:
                paragraphs = []
                for p in parent.find_all("p"):
                    text = clean_text(p.get_text(" ", strip=True))
                    if text and text != heading_text:
                        paragraphs.append(text)
                return clean_text(" ".join(paragraphs))
    return None


def parse_researchers_from_grant(soup: BeautifulSoup) -> list[dict[str, Any]]:
    researchers: list[dict[str, Any]] = []
    for heading in soup.find_all(["h2", "h3"]):
        if clean_text(heading.get_text(" ", strip=True)) != "Researchers":
            continue
        dynamic = heading.find_parent(class_="dynamicList")
        if not dynamic:
            continue
        for item in dynamic.select(".dynamicListItem"):
            name = clean_text(item.find("h4").get_text(" ", strip=True)) if item.find("h4") else None
            location = clean_text(item.select_one(".txt_semiBold").get_text(" ", strip=True)) if item.select_one(".txt_semiBold") else None
            link = item.select_one('a[href^="/researcher/"]')
            url = full_url(link.get("href")) if link else None
            if name:
                researchers.append(
                    {
                        "name": name,
                        "url": url,
                        "slug": slug_from_url(url) if url else None,
                        "location": location,
                    }
                )
        break
    return researchers


def parse_tags(soup: BeautifulSoup) -> dict[str, list[str]]:
    tags: dict[str, list[str]] = {}
    for category in soup.select(".tags-category"):
        title = category.select_one(".tags-category-title")
        label = clean_text(title.get_text(" ", strip=True)) if title else None
        if not label:
            continue
        key = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
        values = [clean_text(a.get_text(" ", strip=True)) for a in category.select("a.tags-link")]
        tags[key] = [v for v in values if v]
    return tags


def parse_grant_detail(url: str, cache_dir: Path) -> dict[str, Any]:
    html = read_or_fetch_text(url, cache_dir)
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main") or soup
    h1 = main.find("h1")
    detail_title = clean_text(h1.get_text(" ", strip=True) if h1 else None)

    sections: dict[str, str] = {}
    for quarantine in main.select(".quarantine"):
        sections.update(extract_labeled_sections(quarantine))

    final_outcome = find_section_after_heading(soup, "Final Outcome")
    if final_outcome:
        sections["final_outcome"] = final_outcome

    researchers = parse_researchers_from_grant(soup)
    tags = parse_tags(soup)
    same_program = (tags.get("within_the_same_program") or [None])[0]
    same_year = (tags.get("within_the_same_funding_year") or [None])[0]

    return {
        "detail_title": detail_title,
        "detail_sections_json": json.dumps(sections, ensure_ascii=False, sort_keys=True),
        "objective_rationale": sections.get("objective_rationale") or sections.get("study_rationale"),
        "project_description": sections.get("project_description"),
        "relevance_to_diagnosis_treatment": sections.get("relevance_to_diagnosis_treatment_of_parkinson_s_disease"),
        "anticipated_outcome": sections.get("anticipated_outcome"),
        "final_outcome": sections.get("final_outcome"),
        "publication_based_on_mjff_funding": sections.get("publication_based_on_mjff_funding"),
        "detail_researchers_json": json.dumps(researchers, ensure_ascii=False),
        "detail_researcher_count": str(len(researchers)),
        "detail_program": same_program,
        "detail_year": same_year,
        "keywords_json": json.dumps(tags.get("search_by_related_keywords") or [], ensure_ascii=False),
        "tags_json": json.dumps(tags, ensure_ascii=False, sort_keys=True),
    }


def parse_researcher_profile(url: Optional[str], cache_dir: Path) -> dict[str, Any]:
    if not url:
        return {}
    html = read_or_fetch_text(url, cache_dir)
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main") or soup
    h1 = main.find("h1")
    name = clean_text(h1.get_text(" ", strip=True) if h1 else None)

    profile_lines = []
    for div in main.select(".hero-content-inner .txt_medium"):
        text = clean_text(div.get_text(" ", strip=True))
        if text:
            profile_lines.append(text)

    position_affiliation = None
    location = None
    for line in profile_lines:
        if line.lower().startswith("location:"):
            location = clean_text(line.replace("Location:", "", 1))
        elif position_affiliation is None:
            position_affiliation = line

    bio = None
    q = main.select_one(".main-primary .quarantine_full")
    if q:
        bio = clean_text(q.get_text(" ", strip=True))

    return {
        "lead_profile_name": name,
        "lead_profile_url": url,
        "lead_position_affiliation": position_affiliation,
        "lead_location": location,
        "lead_bio": bio,
    }


def enrich_records(records: list[dict[str, Any]], cache_dir: Path, skip_researcher_pages: bool = False) -> list[dict[str, Any]]:
    print("\n" + "=" * 72)
    print("Step 1 detail fetch: grant pages and official researcher profiles")
    print("=" * 72)
    researcher_cache: dict[str, dict[str, Any]] = {}
    enriched: list[dict[str, Any]] = []

    total = len(records)
    for idx, row in enumerate(records, start=1):
        if idx == 1 or idx % 25 == 0 or idx == total:
            print(f"  detail progress {idx}/{total} grants")
        detail = parse_grant_detail(row["grant_url"], cache_dir)
        row.update(detail)

        detail_researchers = json.loads(row.get("detail_researchers_json") or "[]")
        listing_researchers = json.loads(row.get("listing_researchers_json") or "[]")
        researchers = detail_researchers or listing_researchers
        lead = researchers[0] if researchers else {}
        row["lead_investigator_raw"] = lead.get("name")
        row["lead_researcher_url"] = lead.get("url")
        row["all_researchers_json"] = json.dumps(researchers, ensure_ascii=False)

        if not skip_researcher_pages and lead.get("url"):
            profile_url = lead["url"]
            if profile_url not in researcher_cache:
                researcher_cache[profile_url] = parse_researcher_profile(profile_url, cache_dir)
            row.update(researcher_cache[profile_url])
        else:
            row.update(
                {
                    "lead_profile_name": None,
                    "lead_profile_url": lead.get("url"),
                    "lead_position_affiliation": None,
                    "lead_location": lead.get("location"),
                    "lead_bio": None,
                }
            )

        row["source_program"] = row.get("detail_program") or row.get("source_program") or "MJFF Funded Study"
        row["source_year"] = row.get("detail_year") or row.get("source_year")
        row["source_year_missing_note"] = (
            "MJFF public page does not expose funding year" if not row.get("source_year") else None
        )
        row["display_name"] = row.get("detail_title") or row.get("title")
        row["description"] = (
            row.get("objective_rationale")
            or row.get("project_description")
            or row.get("final_outcome")
            or row.get("listing_summary")
        )
        row["funder_award_id"] = f"mjff-{row['grant_slug']}"
        row["funder_id"] = str(FUNDER_ID)
        row["funder_display_name"] = FUNDER_DISPLAY_NAME
        row["funder_doi"] = FUNDER_DOI
        row["provenance"] = PROVENANCE
        row["funding_type"] = "research"
        row["amount"] = None
        row["currency"] = None
        row["landing_page_url"] = row["grant_url"]
        row["downloaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        enriched.append(row)

    return enriched


def validate_rows(df: pd.DataFrame) -> None:
    print("\n" + "=" * 72)
    print("Validation")
    print("=" * 72)
    if df.empty:
        raise RuntimeError("no rows generated")
    dupes = df[df["funder_award_id"].duplicated(keep=False)]
    if not dupes.empty:
        sample = dupes[["funder_award_id", "landing_page_url"]].head(10).to_dict("records")
        raise RuntimeError(f"duplicate funder_award_id values would collapse rows: {sample}")
    missing = {}
    for col in ["funder_award_id", "display_name", "source_year", "source_program", "landing_page_url"]:
        missing[col] = int(df[col].isna().sum() + (df[col].astype(str).str.len() == 0).sum())
    print(f"  rows: {len(df):,}")
    print(f"  years: {df['source_year'].dropna().min()}-{df['source_year'].dropna().max()}")
    print(f"  unique award ids: {df['funder_award_id'].nunique():,}")
    print(f"  missing required-like source fields: {missing}")
    print("  top source programs:")
    print(df["source_program"].value_counts(dropna=False).head(12).to_string())
    if missing["funder_award_id"] or missing["display_name"] or missing["landing_page_url"]:
        raise RuntimeError(f"missing critical source fields: {missing}")
    # A handful of official MJFF detail pages omit funding-year tags entirely.
    # Preserve them with NULL dates, but fail if the parser ever loses broad year coverage.
    if missing["source_year"] > max(5, int(len(df) * 0.01)):
        raise RuntimeError(f"unexpectedly low source_year coverage: {missing}")


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook §1.4: refuse to overwrite S3 if the new corpus is smaller."""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("s3")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("No previous S3 object found; shrink check passes for first ingest.")
            return True
        print(f"[WARN] Could not inspect previous S3 object ({code}); continuing without shrink baseline.")
        return True

    prev_path = output_dir / "_prev_mjff_funded_studies.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    finally:
        prev_path.unlink(missing_ok=True)

    print(f"Shrink check: previous={prev_count:,}, new={new_count:,}, allow_shrink={allow_shrink}")
    if new_count < prev_count and not allow_shrink:
        raise SystemExit(
            f"Refusing to upload smaller corpus ({new_count:,} < {prev_count:,}). "
            "Rerun with --allow-shrink only after reviewing the source change."
        )
    return True


def upload_to_s3(path: Path) -> None:
    import boto3

    print(f"Uploading {path} to s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(path), S3_BUCKET, S3_KEY)


def run(args: argparse.Namespace) -> pd.DataFrame:
    global MIN_REQUEST_INTERVAL_S
    MIN_REQUEST_INTERVAL_S = args.request_interval

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = output_dir / "raw"
    cache_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / "mjff_funded_studies.parquet"

    if args.skip_download:
        if not parquet_path.exists():
            raise SystemExit(f"--skip-download requested but {parquet_path} does not exist")
        print(f"Reading existing parquet from {parquet_path}")
        df = pd.read_parquet(parquet_path)
    else:
        # Fast source sanity check before the full crawl.
        first_page = read_or_fetch_json(f"{AJAX_URL}?page=0", cache_dir)
        first_cards = BeautifulSoup(first_page.get("markup") or "", "html.parser").select("li.wideCards-item")
        if not first_cards:
            raise RuntimeError("source sanity check failed: page 0 returned zero grant cards")
        print(f"Source sanity check: page 0 returned {len(first_cards)} grant cards.")

        records = discover_listing_records(cache_dir, limit=args.limit, max_pages=args.max_pages)
        enriched = enrich_records(records, cache_dir, skip_researcher_pages=args.skip_researcher_pages)
        df = pd.DataFrame(enriched)
        validate_rows(df)
        df = df.astype("string")
        df.to_parquet(parquet_path, index=False)
        print(f"Wrote {len(df):,} rows to {parquet_path}")

    validate_rows(df)

    if args.skip_upload:
        print("--skip-upload set; not uploading to S3.")
    else:
        check_no_shrink(len(df), args.allow_shrink, output_dir)
        upload_to_s3(parquet_path)

    return df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download MJFF funded studies and upload parquet to S3.")
    parser.add_argument("--output-dir", default="data/mjff", help="Local cache/output directory.")
    parser.add_argument("--limit", type=int, default=None, help="Limit grants for smoke tests.")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit AJAX listing pages for debugging.")
    parser.add_argument("--skip-download", action="store_true", help="Reuse existing local parquet.")
    parser.add_argument("--skip-upload", action="store_true", help="Do not upload to S3.")
    parser.add_argument("--skip-researcher-pages", action="store_true", help="Do not fetch linked researcher profile pages.")
    parser.add_argument(
        "--request-interval",
        type=float,
        default=MIN_REQUEST_INTERVAL_S,
        help="Minimum seconds between HTTP requests. Default is conservative; local full QA may lower this.",
    )
    parser.add_argument("--allow-shrink", action="store_true", help="Allow replacing S3 object with fewer rows.")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        run(parse_args())
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        raise
