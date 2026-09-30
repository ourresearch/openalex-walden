#!/usr/bin/env python3
"""
McKnight Foundation (research grants) to S3 Data Pipeline
=========================================================

McKnight's grantmaking is mostly non-research (arts, Midwest climate,
communities; searchable at mcknight.org/grants/search-our-grants, org-level
rows with no investigators). Its research grantmaking is:

1. The Collaborative Crop Research Program (CCRP), now "Global Collaboration
   for Resilient Food Systems": agroecology research grants to universities
   and research organisations in Africa and the Andes. Every grant has a public
   page at https://www.ccrp.org/grants/<slug>/ (WordPress, all listed in the
   site's Yoast sitemap) with title, lead organisation, partner organisations,
   community of practice, countries, duration (MM/YYYY - MM/YYYY), overview,
   grant aims and outputs/outcomes. THIS SCRIPT INGESTS THESE.
2. The McKnight Endowment Fund for Neuroscience (MEFN) scholar / technology /
   brain-disorder awards (mcknight.org/programs/the-mcknight-endowment-fund-
   for-neuroscience/*/awardees/). MEFN is a separate OpenAlex funder
   (F4320306144), so those awards are NOT ingested here under McKnight
   Foundation; see the notebook header.

ccrp.org publishes no grant numbers, PIs or amounts. funder_award_id is
"CCRP-<WordPress post id>".

Output: s3://openalex-ingest/awards/mcknight/mcknight_projects.parquet
"""

import argparse
import calendar
import html
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
import sys
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if sys.platform == "win32":
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

SITEMAP_INDEX = "https://www.ccrp.org/sitemap_index.xml"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/mcknight/mcknight_projects.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"}
REQUEST_DELAY = 0.7
RETRIES = 3
MAX_CONSECUTIVE_NON200 = 5


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> tuple[int, str]:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            log(f"GET {url} -> {r.status_code} ({len(r.content)} bytes)")
            r.encoding = "utf-8"
            return r.status_code, r.text
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 * (attempt + 1))
    log(f"GET {url} failed: {last}")
    return 0, ""


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<\?xml[^>]*\?>", "", fragment)
    t = re.sub(r"<(li|br|p)[^>]*>", " ", t)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def grant_urls() -> list[str]:
    status, idx = get(SITEMAP_INDEX)
    if status != 200:
        raise RuntimeError(f"sitemap index HTTP {status}")
    locs = re.findall(r"<loc>([^<]+)</loc>", idx)
    # Yoast: either a sitemap index of sub-sitemaps, or (as on ccrp.org) a flat urlset
    subs = [u for u in locs if u.endswith(".xml")]
    for sm in subs:
        st, xml = get(sm)
        if st == 200:
            locs += re.findall(r"<loc>([^<]+)</loc>", xml)
    return sorted({u.strip() for u in locs if re.match(r"https://www\.ccrp\.org/grants/[^/]+/?$", u.strip())})


def month_date(s: str | None, end: bool = False) -> str | None:
    # "11/2020" -> 2020-11-01 (start) / 2020-11-30 (end)
    if not s:
        return None
    m = re.fullmatch(r"(\d{1,2})/(\d{4})", s.strip())
    if not m or not 1 <= int(m.group(1)) <= 12:
        y = re.fullmatch(r"(\d{4})", s.strip())
        if y:
            return f"{y.group(1)}-12-31" if end else f"{y.group(1)}-01-01"
        return None
    yr, mo = int(m.group(2)), int(m.group(1))
    if not end:
        return f"{yr:04d}-{mo:02d}-01"
    return f"{yr:04d}-{mo:02d}-{calendar.monthrange(yr, mo)[1]:02d}"


def parse_grant(url: str, page: str) -> dict | None:
    title = re.search(r'<h1 class="main__title">(.*?)</h1>', page, re.S)
    post = re.search(r"\bpostid-(\d+)\b", page)
    if not title or not post:
        return None
    main = re.search(r'<main class="main">(.*?)</main>', page, re.S)
    body = main.group(1) if main else page
    # detail blocks: <h2 class="detail-heading">Label:</h2> followed by content up to the next heading
    parts = re.split(r'<h2 class="detail-heading">\s*(.*?)\s*</h2>', body, flags=re.S)
    fields = {}
    for label, content in zip(parts[1::2], parts[2::2]):
        content = re.split(r"<!-- end of \.main__inner -->", content)[0]
        fields[text(label).rstrip(":").strip()] = text(content)
    dur = fields.get("Duration") or ""
    d = [x.strip() for x in re.split(r"\s*[—–-]\s*", dur) if x.strip()]
    return {
        "post_id": post.group(1),
        "slug": url.rstrip("/").rsplit("/", 1)[-1],
        "title": text(title.group(1)),
        "lead_organization": fields.get("Lead Organization"),
        "partner_organizations": fields.get("Partner Organizations"),
        "community_of_practice": fields.get("Community of Practice"),
        "countries": fields.get("Countries"),
        "duration_text": dur or None,
        "start_date": month_date(d[0]) if d else None,
        "end_date": month_date(d[1], end=True) if len(d) > 1 else None,
        "overview": fields.get("Overview"),
        "grant_aims": fields.get("Grant Aims"),
        "outputs_outcomes": fields.get("Outputs and Outcomes"),
        "other_fields": json.dumps({k: v for k, v in fields.items() if k not in {
            "Lead Organization", "Partner Organizations", "Community of Practice", "Countries",
            "Duration", "Overview", "Grant Aims", "Outputs and Outcomes"}}, ensure_ascii=False),
        "landing_page_url": url,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="McKnight CCRP/GCRFS grant pages (ccrp.org) -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None)
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the section 1.4 shrink guard")
    args = p.parse_args()

    urls = grant_urls()
    log(f"Sitemap: {len(urls)} ccrp.org/grants/ pages")
    if args.limit:
        urls = urls[: args.limit]
    rows, skipped, non200 = [], [], 0
    t0 = time.time()
    for i, url in enumerate(urls, 1):
        cache = args.cache_dir / (url.rstrip("/").rsplit("/", 1)[-1][:80] + ".html") if args.cache_dir else None
        if cache and cache.exists():
            status, page = 200, cache.read_text()
        else:
            status, page = get(url)
            time.sleep(REQUEST_DELAY)
        if status != 200:
            non200 += 1
            skipped.append(url)
            if non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError("too many consecutive non-200 pages; refusing to truncate")
            continue
        non200 = 0
        if cache:
            args.cache_dir.mkdir(parents=True, exist_ok=True)
            cache.write_text(page)
        rec = parse_grant(url, page)
        if rec is None:
            skipped.append(url)
        else:
            rows.append(rec)
        if i % 50 == 0:
            el = time.time() - t0
            log(f"[{el:5.0f}s] {i}/{len(urls)} pages, {len(rows)} grants, ETA {el / i * (len(urls) - i):4.0f}s")
    log(f"Parsed {len(rows)} grants, {len(skipped)} pages skipped")
    for u in skipped[:20]:
        log(f"  skipped: {u}")

    df = pd.DataFrame(rows)
    # No public grant number on ccrp.org (McKnight's internal "15-473" style
    # numbers appear only in acknowledgements), so the award id is the grant
    # page's WordPress post id: stable, unique, printed in the page markup.
    df["funder_award_id"] = "CCRP-" + df["post_id"]
    # ~some grant pages are published untitled (h1 empty, <title>"CRFS | Untitled");
    # compose a display title from the lead organisation and start year.
    untitled = df["title"].isna() | df["title"].str.fullmatch(r"(?i)untitled|\d+")
    df["display_title"] = df["title"].where(~untitled, None)
    df.loc[untitled, "display_title"] = [
        "McKnight CCRP grant" + (f": {o}" if isinstance(o, str) and o else "")
        + (f" ({d[:4]})" if isinstance(d, str) and d else "")
        for o, d in zip(df.loc[untitled, "lead_organization"], df.loc[untitled, "start_date"])
    ]
    df["title_is_composed"] = untitled.map({True: "true", False: "false"})
    log(f"  untitled pages (composed display_title): {int(untitled.sum())}")
    dupes = df["funder_award_id"].duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    for c in ["title", "lead_organization", "start_date", "end_date", "overview", "grant_aims", "countries"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "mcknight_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_mcknight_projects.parquet"
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
