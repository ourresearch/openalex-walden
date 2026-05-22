#!/usr/bin/env python3
"""
Lasker Awards to S3 (PRIZE PATTERN)
====================================

Lasker Foundation publishes its winners as a `winners` custom post
type. Two kinds of posts coexist:
  - Year wrappers ("2025 Winners") — skip these
  - Award posts (one per award title) — these have a `winners_name`
    taxonomy with N laureates per award

We use `?_embed=1` to inline the term details (award category, year,
laureate names) so we don't have to do separate term lookups.

Output: s3://openalex-ingest/awards/lasker/lasker_awards.parquet
Awarding body in OpenAlex: Lasker Foundation (F4320311370)

Schema: one row per (award × laureate). Apportioned amount is N/A
(Lasker doesn't publish a per-laureate share); we leave amount NULL.
"""

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

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

API = "https://laskerfoundation.org/wp-json/wp/v2/winners"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/lasker/lasker_awards.parquet"

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
                "per_page": PER_PAGE, "page": page, "_embed": 1,
                "_fields": "id,slug,date,link,title,content,award,year_of_award,winners_name,_embedded,_links",
            }, headers=HEADERS, timeout=30, verify=False)
            r.raise_for_status()
            return r.json(), int(r.headers.get("X-WP-Total", 0))
        except Exception as e:
            last_err = e
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Page {page}: {last_err}")


def html_to_text(html: str) -> str:
    if not html:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()


def parse_affiliations_from_content(content_html: str) -> dict[str, str]:
    """Build a {family_name_lower: affiliation} map from the award post's content.

    The post's content has structured laureate cards: each is a
    `<p class="aw-name">Name</p>` followed by a `<p class="aw-work">Affiliation</p>`.
    We key by the LAST whitespace-separated token of aw-name (family name)
    so we can match against the WP taxonomy term whose name is "Family, Given".
    Some aw-name values include parenthetical nicknames or middle initials —
    those are dropped via the last-token rule.
    """
    if not content_html:
        return {}
    soup = BeautifulSoup(content_html, "html.parser")
    out: dict[str, str] = {}
    names = soup.select("p.aw-name")
    works = soup.select("p.aw-work")
    # Pair them by document order (they should be 1:1)
    for n, w in zip(names, works):
        full_name = re.sub(r"\s+", " ", n.get_text(" ", strip=True))
        affiliation = re.sub(r"\s+", " ", w.get_text(" ", strip=True))
        if not full_name or not affiliation:
            continue
        # Last token = family name (works for "Michael J. Welsh", "Jesús González",
        # "Doris Ying Tsao", etc. — multi-word family names like "van der Berg"
        # would surface as "Berg" only, which still keys uniquely within an award)
        family = full_name.split()[-1]
        out[family.lower()] = affiliation
    return out


def expand_award(post: dict) -> list[dict]:
    """One award post -> N rows (one per laureate)."""
    embedded = (post.get("_embedded") or {}).get("wp:term") or []
    award_terms: list[dict] = []
    year_terms: list[dict] = []
    laureate_terms: list[dict] = []
    for term_list in embedded:
        if not term_list:
            continue
        tax = (term_list[0] or {}).get("taxonomy")
        if tax == "award":
            award_terms = term_list
        elif tax == "year_of_award":
            year_terms = term_list
        elif tax == "winners_name":
            laureate_terms = term_list

    if not laureate_terms:
        return []  # year-wrapper posts have no laureates

    award_name = (award_terms[0].get("name") if award_terms else None)
    year = None
    if year_terms:
        y_str = (year_terms[0].get("name") or year_terms[0].get("slug") or "").strip()
        if y_str.isdigit():
            year = int(y_str)

    title_html = (post.get("title") or {}).get("rendered") or ""
    title = html_to_text(title_html)
    content_html = (post.get("content") or {}).get("rendered", "")
    excerpt = html_to_text(content_html)[:1500]
    landing = post.get("link")

    # Per-laureate affiliations are in the post body's <p.aw-name>+<p.aw-work> pairs
    affil_by_family = parse_affiliations_from_content(content_html)

    out = []
    for t in laureate_terms:
        # Term name is "Family, Given" — split on first comma
        name_disp = (t.get("name") or "").strip()
        given_name: str | None = None
        family_name: str | None = None
        if "," in name_disp:
            family_name, given_name = [s.strip() for s in name_disp.split(",", 1)]
        else:
            # No comma: assume single name or already given-first; last token = family
            tokens = name_disp.split()
            family_name = tokens[-1] if tokens else None
            given_name = " ".join(tokens[:-1]) if len(tokens) > 1 else None

        affiliation = affil_by_family.get((family_name or "").lower())

        out.append({
            "wp_post_id": post.get("id"),
            "wp_slug": post.get("slug"),
            "url": landing,
            "wp_date": post.get("date"),
            "award_name": award_name,        # BASIC / CLINICAL / SPECIAL ACHIEVEMENT / etc.
            "year": year,
            "achievement_title": title,      # e.g. "Triple-drug therapy for cystic fibrosis"
            "laureate_name": name_disp,
            "laureate_given_name": given_name,
            "laureate_family_name": family_name,
            "laureate_term_id": t.get("id"),
            "laureate_slug": t.get("slug"),
            "affiliation": affiliation,
            "description": excerpt,
            "downloaded_at": datetime.utcnow().isoformat(),
        })
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Lasker -> parquet -> S3")
    p.add_argument("--limit-pages", type=int, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    args = p.parse_args()

    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    log("=" * 60)
    log("Lasker -> S3 starting")

    rows: list[dict] = []
    skipped_wrappers = 0
    page = 1
    while True:
        posts, total = fetch_page(page)
        if not posts:
            break
        for p_ in posts:
            expanded = expand_award(p_)
            if not expanded:
                skipped_wrappers += 1
            rows.extend(expanded)
        log(f"  page {page}: posts={len(posts)} (running rows: {len(rows)}, wrappers skipped: {skipped_wrappers})")
        if args.limit_pages and page >= args.limit_pages:
            break
        if page * PER_PAGE >= total:
            break
        page += 1
        time.sleep(REQUEST_DELAY)

    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")
    if not df.empty:
        log(f"Coverage: name={df.laureate_name.notna().sum()}, "
            f"year={df.year.notna().sum()}, "
            f"award={df.award_name.notna().sum()}, "
            f"affiliation={df.affiliation.notna().sum()}")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "lasker_awards.parquet"
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
