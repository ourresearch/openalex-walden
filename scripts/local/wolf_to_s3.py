#!/usr/bin/env python3
"""
Wolf Prize to S3 (PRIZE PATTERN)
=================================

Wolf Foundation publishes all Wolf Prize winners as WordPress posts in
the "the-wolf-prize-winners" category (id 27). The category has 391
posts as of 2026-05.

Each post follows a "{Year} - {Field} - {Name}" title pattern, with
the body containing biography + citation. Years span 1978-present
across six fields: Agriculture, Chemistry, Mathematics, Medicine,
Physics, and Arts (~6 winners/year).

Output: s3://openalex-ingest/awards/wolf_prize/wolf_prize_winners.parquet

Awarding body in OpenAlex: Wolf Foundation (F4320320951)
"""

import argparse
import html as html_mod
import json
import re
import time
from datetime import datetime
from pathlib import Path

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

API = "https://wolffund.org.il/wp-json/wp/v2/posts"
CATS_API = "https://wolffund.org.il/wp-json/wp/v2/categories"
WINNERS_CATEGORY = 27  # "the-wolf-prize-winners"
# English field categories (Hebrew duplicates exist with different IDs)
FIELD_CATEGORIES = {
    85: "Physics",
    81: "Mathematics",
    87: "Medicine",
    83: "Chemistry",
    91: "Agriculture",
    107: "Architecture",
    114: "Music",
    124: "Painting & Sculpture",
    47: "Leadership",
}
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/wolf_prize/wolf_prize_winners.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
PER_PAGE = 100
REQUEST_DELAY = 0.3
RETRIES = 3


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_page(page: int) -> tuple[list[dict], int]:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(API, params={
                "per_page": PER_PAGE, "page": page,
                "categories": WINNERS_CATEGORY,
                "_fields": "id,slug,date,modified,link,title,excerpt,content,categories",
            }, headers=HEADERS, timeout=30, verify=False)
            r.raise_for_status()
            return r.json(), int(r.headers.get("X-WP-Total", 0))
        except Exception as e:
            last_err = e
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Page {page} failed: {last_err}")


def html_to_text(html: str) -> str:
    """Strip tags, decode HTML entities, collapse whitespace."""
    if not html:
        return ""
    text = re.sub(r"<[^>]+>", " ", html)
    text = html_mod.unescape(text)  # &nbsp; &quot; &#8217; etc.
    return re.sub(r"\s+", " ", text).strip()


def split_name(name: str) -> tuple[str | None, str | None]:
    """Split 'James P. Eisenstein' -> ('James P.', 'Eisenstein').

    Strips trailing degree/suffix tokens (PhD, MD, Jr., Sr., II, III) before
    splitting. Last whitespace-separated token = family name; rest = given.
    """
    if not name:
        return None, None
    # Drop trailing degree/suffix tokens
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


# Affiliation typically follows "Affiliation at the time of the award:" in the content
AFFIL_RE = re.compile(
    r"Affiliation at the time of the award\s*:?\s*(.{1,300}?)(?:Award citation|Prize share|Citation|$)",
    re.I | re.S,
)
CITATION_RE = re.compile(
    r"Award citation\s*:?\s*[“\"']?(.{1,1000}?)[”\"']?\s*(?:Prize share|Affiliation|Bibliography|$)",
    re.I | re.S,
)


def parse_content(html: str) -> tuple[str | None, str | None]:
    text = html_to_text(html)  # already entity-decoded
    affil = None
    citation = None
    m = AFFIL_RE.search(text)
    if m:
        affil = m.group(1).strip().rstrip(":").strip() or None
        # Trim chrome that sometimes follows ("Award citation:" label without value)
        affil = re.split(r"(?=Award citation|Prize share|Bibliography)", affil)[0].strip()
        affil = affil or None
    m = CITATION_RE.search(text)
    if m:
        citation = m.group(1).strip() or None
    return affil, citation


def normalise(post: dict) -> dict:
    title_html = (post.get("title") or {}).get("rendered") or ""
    name = html_to_text(title_html)
    given_name, family_name = split_name(name)

    # Field = first known English field category
    cats = post.get("categories") or []
    field = next((FIELD_CATEGORIES[c] for c in cats if c in FIELD_CATEGORIES), None)

    # Year: parse from post date
    year = None
    d = post.get("date") or ""
    if len(d) >= 4 and d[:4].isdigit():
        year = int(d[:4])

    affil, citation = parse_content((post.get("content") or {}).get("rendered", ""))
    # Trim trailing punctuation that often hangs off the citation in the source
    if citation:
        citation = citation.strip().strip(".").strip("”").strip("\"").strip("'").strip(":").strip()

    return {
        "wp_post_id": post.get("id"),
        "slug": post.get("slug"),
        "url": post.get("link"),
        "wp_date": post.get("date"),
        "name": name,
        "given_name": given_name,
        "family_name": family_name,
        "year": year,
        "field": field,
        "affiliation": affil,
        "citation": citation,
        "categories": cats,
        "downloaded_at": datetime.utcnow().isoformat(),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Wolf Prize -> parquet -> S3")
    p.add_argument("--limit-pages", type=int, default=None,
                   help="Smoke-test: only fetch first N pages")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    args = p.parse_args()

    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    log("=" * 60)
    log("Wolf Prize -> S3 starting")

    rows: list[dict] = []
    page = 1
    total = None
    while True:
        posts, total = fetch_page(page)
        if not posts:
            break
        for p_ in posts:
            rows.append(normalise(p_))
        log(f"  page {page}: +{len(posts)} (running: {len(rows)}/{total})")
        if args.limit_pages and page >= args.limit_pages:
            break
        if len(rows) >= total:
            break
        page += 1
        time.sleep(REQUEST_DELAY)

    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")
    if not df.empty:
        log(f"Coverage: name={df.name.notna().sum()}, year={df.year.notna().sum()}, field={df.field.notna().sum()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "wolf_prize_winners.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {parquet_path}")

    if args.skip_upload:
        log("--skip-upload set; done.")
        return
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3
    s3 = boto3.client("s3")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    main()
