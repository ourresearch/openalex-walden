#!/usr/bin/env python3
"""
National Breast Cancer Foundation (NBCF, Australia) research projects to S3
==========================================================================

NBCF publishes its funded research projects on its WordPress site:

    current projects   https://nbcf.org.au/research/research-projects/            (post type "project")
    completed projects https://nbcf.org.au/research/research-projects-completed/  (post type "project-completed")

Both post types are enumerated by the Yoast sitemaps (project-sitemap.xml,
project-completed-sitemap.xml). Each project page carries the title, a
person card (institution, position, lead researcher name) and a free-text
description. The funding year and grant type are not on the project page;
they are the listing filters, served as static archive URLs
(.../year/{yyyy}/page/{n}/ and .../grant/{type}/page/{n}/), so the script
walks every filter value and maps each project URL to its year / grant type.
The research category (Prevent / Detect / Treat / Quality Of Life ...) comes
from the listing cards. Method 5 (static HTML) on the runbook ladder.

NBCF publishes no grant number (the IIRS-19-016 / PF-16-011 / ECF-17-002
codes researchers cite are not on the site) and no per-project amount.

robots.txt (2026-10-01): User-agent * disallows only /nbcf-user/. Cloudflare
answers a bare "Mozilla/5.0" UA with a block page; the honest
openalex-walden UA below is served normally.

Output: s3://openalex-ingest/awards/nbcf/nbcf_projects.parquet
"""

import argparse
import hashlib
import html
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). See runbook §1.2.
# (grep anchor for the §4.0 self-check: sys.stdout.reconfigure)
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

BASE = "https://nbcf.org.au"
LISTINGS = {
    "project": f"{BASE}/research/research-projects",
    "project-completed": f"{BASE}/research/research-projects-completed",
}
SITEMAPS = {
    "project": f"{BASE}/project-sitemap.xml",
    "project-completed": f"{BASE}/project-completed-sitemap.xml",
}
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/nbcf/nbcf_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 1.0
RETRIES = 3
MAX_PAGES = 40
MAX_CONSECUTIVE_EMPTY = 3


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, cache_dir: Path | None = None) -> str:
    cache = None
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        # short name + URL hash: long project slugs blow the Windows MAX_PATH
        name = re.sub(r"[^a-zA-Z0-9]+", "_", url.split("//", 1)[1])[:50]
        cache = cache_dir / f"{name}_{hashlib.md5(url.encode()).hexdigest()[:10]}.html"
        if cache.exists():
            return cache.read_text()
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code == 404:
                log(f"GET {url} -> 404")
                return ""
            r.raise_for_status()
            r.encoding = "utf-8"
            if "Attention Required! | Cloudflare" in r.text:
                raise RuntimeError("Cloudflare block page")
            time.sleep(REQUEST_DELAY)
            if cache:
                cache.write_text(r.text)
            return r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            log(f"GET {url} failed ({e}); retry {attempt + 1}/{RETRIES}")
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace(" ", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def slug_of(url: str) -> str:
    return url.rstrip("/").rsplit("/", 1)[-1].lower()


def listing_items(page: str) -> list[tuple[str, str | None]]:
    """(project URL, category) for each card inside the archive items grid."""
    i = page.find('data-archive="items"')
    if i < 0:
        return []
    j = page.find('data-archive="pagination"', i)
    seg = page[i: j if j > 0 else len(page)]
    out = []
    for m in re.finditer(r'<a href="(https://nbcf\.org\.au/project(?:-completed)?/[^"]+)">(.*?)</a>', seg, re.S):
        cat = re.search(r'card-research__category[^>]*>(.*?)</span>', m.group(2), re.S)
        out.append((m.group(1), text(cat.group(1)) if cat else None))
    return out


def filter_values(page: str, name: str) -> list[str]:
    m = re.search(rf'<select[^>]*name="{name}"[^>]*>(.*?)</select>', page, re.S)
    return [v for v in re.findall(r'<option value="([^"]+)"', m.group(1))] if m else []


def walk_listing(base: str, path: str, cache_dir: Path | None) -> tuple[dict[str, str | None], int]:
    """All (url -> category) under base/path, paginated; returns (items, total-count)."""
    items, total, empty = {}, None, 0
    for n in range(1, MAX_PAGES + 1):
        url = f"{base}/{path}" + (f"page/{n}/" if n > 1 else "")
        page = get(url, cache_dir)
        if total is None:
            m = re.search(r'data-archive="total-count">\s*(\d+)', page)
            total = int(m.group(1)) if m else 0
        got = listing_items(page)
        new = [u for u, _ in got if u not in items]
        for u, c in got:
            items.setdefault(u, c)
        if not new:
            empty += 1
            if empty >= MAX_CONSECUTIVE_EMPTY or len(items) >= total:
                break
            continue
        empty = 0
        if len(items) >= total:
            break
    if total and len(items) < total:
        log(f"  WARNING {base}/{path}: collected {len(items)} of {total}")
    return items, total or 0


HONORIFIC_RE = re.compile(
    r"^(?:(?:dr|prof|professor|associate professor|assoc\.? prof\.?|a/prof\.?|mr|mrs|ms|miss)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py) plus a leading-honorific strip."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                "ao", "am", "oam", "faa", "fahms"}  # Australian honours ("John Hopper AO")
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_project(url: str, page: str) -> dict:
    title = re.search(r'<h1 class="title title--page">(.*?)</h1>', page, re.S)
    card = re.search(r'<div class="card-person__about">(.*?)</div>', page, re.S)
    inst = pos = name = None
    if card:
        c = card.group(1)
        inst = text((re.search(r'<span class="pre-title[^"]*">(.*?)</span>', c, re.S) or [None, None])[1])
        pos = text((re.search(r'<span class="position">(.*?)</span>', c, re.S) or [None, None])[1])
        name = text((re.search(r'<h3 class="title">(.*?)</h3>', c, re.S) or [None, None])[1])
    body = re.search(r'<div class="is-editable">(.*?)(?:<aside class="share"|<div class="card-person")', page, re.S)
    desc = None
    if body:
        paras = [text(p) for p in re.findall(r"<p[^>]*>(.*?)</p>", body.group(1), re.S)]
        desc = "\n\n".join(p for p in paras if p) or None
    if not name:
        # No person on the card: it is an image caption (one 2026 equipment /
        # centre grant: "Image courtesy of Siemens"), not an institution.
        if inst or pos:
            log(f"  {url}: card has no person ({pos!r} / {inst!r}); institution not used")
        inst = pos = None
    published = re.search(r"Published:\s*([0-9/]+)", page)
    given, family = split_name(name) if name else (None, None)
    return {
        "title": text(title.group(1)) if title else None,
        "institution": inst,
        "lead_position": pos,
        "lead_name": name,
        "lead_given_name": given,
        "lead_family_name": family,
        "description": desc,
        "published": published.group(1) if published else None,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="NBCF research projects -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only fetch the first N project pages")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    # 1. project URLs from the sitemaps
    urls = {}
    for ptype, sm in SITEMAPS.items():
        locs = re.findall(r"<loc>([^<]+)</loc>", get(sm))
        locs = [u.strip() for u in locs if f"/{ptype}/" in u]
        log(f"Sitemap {ptype}: {len(locs)} URLs")
        for u in locs:
            urls.setdefault(slug_of(u), (ptype, u))

    # 2. year / grant type / category from the listing filters
    year_of, grant_of, cat_of = {}, {}, {}
    for ptype, base in LISTINGS.items():
        first = get(base + "/", args.cache_dir)
        items, total = walk_listing(base, "", args.cache_dir)
        log(f"Listing {ptype}: {len(items)} of {total} cards")
        for u, c in items.items():
            cat_of.setdefault(slug_of(u), c)
        years = filter_values(first, "year")
        grants = filter_values(first, "grant")
        for y in years:
            items, total = walk_listing(base, f"year/{y}/", args.cache_dir)
            for u in items:
                year_of.setdefault(slug_of(u), set()).add(int(y))
        for g in grants:
            items, total = walk_listing(base, f"grant/{g}/", args.cache_dir)
            for u in items:
                grant_of.setdefault(slug_of(u), set()).add(g)
        log(f"  {ptype}: years {years}, grant types {grants}")

    # 3. project pages
    keys = sorted(urls)
    if args.limit:
        keys = keys[: args.limit]
    rows = []
    for i, k in enumerate(keys, 1):
        ptype, url = urls[k]
        page = get(url, args.cache_dir)
        if not page:
            log(f"  skipped (404): {url}")
            continue
        rec = parse_project(url, page)
        ys = sorted(year_of.get(k, []))
        rec.update({
            "slug": k,
            "post_type": ptype,
            "landing_page_url": url,
            "award_year": str(ys[0]) if ys else None,
            "listed_years": ",".join(map(str, ys)) or None,
            "grant_type": ",".join(sorted(grant_of.get(k, []))) or None,
            "category": cat_of.get(k),
        })
        rows.append(rec)
        if i % 25 == 0:
            log(f"  {i}/{len(keys)} project pages")

    df = pd.DataFrame(rows)
    df["funder_award_id"] = "NBCF-" + df["slug"]
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    log(f"Parsed {len(df)} projects ({(df['post_type'] == 'project').sum()} current, "
        f"{(df['post_type'] == 'project-completed').sum()} completed)")
    for c in ["title", "lead_family_name", "institution", "description", "award_year", "grant_type", "category"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    multi = df["listed_years"].fillna("").str.contains(",")
    log(f"  projects listed under >1 year: {multi.sum()}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "nbcf_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_nbcf_projects.parquet"
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(df)}")
        if len(df) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
