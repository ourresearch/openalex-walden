#!/usr/bin/env python3
"""
Rufford Foundation to S3 Data Pipeline
======================================

The Rufford Foundation (UK charity 1117270; OpenAlex F4320309818) funds nature
conservation projects in developing countries through the Rufford Small Grants
programme (first, second and booster grants; most are field research /
monitoring projects led by early-career conservationists). Every grant has a
public page in the foundation's Project Directory:

    https://www.rufford.org/projects/            (paginated, ~20 per page)
    https://www.rufford.org/projects/<grantee-slug>/<project-slug>/

The directory is server-rendered Django (static HTML). There is no bulk export,
no API, no 360Giving file and no Europe PMC (GRIST) feed for Rufford (checked
2026-10-01), so this script walks the listing pages and then every project page.
robots.txt carries only Cloudflare's content-signal preamble with no rules.

Per project we keep: grantee (lead), project title, award/publication date,
location (place, ISO country code, continent), categories (species/themes),
summary + body text, and the project reference number.

Project reference (funder_award_id, runbook §2.1.1)
---------------------------------------------------
Rufford numbers grants ``NNNNN-S`` (application number + grant sequence: ``-1``
first grant, ``-2`` second, ``-B`` booster, ...; e.g. ``33202-1``, ``23387-B``),
and older (pre-~2011) grants ``N.MM.YY`` (e.g. ``130.07.04``). That is the form
grantees cite (OpenAlex citation stubs for Rufford: 634 distinct ``NNNNN-S``
references, plus a few ``N.MM.YY``). The pages do not print the number as text,
but every uploaded report PDF and most project photos are filed under it
(``media/project_reports/33202-1_June_2021.pdf``,
``media/project_images/33683-BHeader/...``). We take the reference from the
report file names first, then from image folder names, and only when it is
unique across the directory. Projects with no recoverable reference get a
stable synthetic key ``RUFFORD-<grantee-slug>/<project-slug>`` (the page path).

Amounts are not published per grant (Rufford Small Grants are up to GBP 6,000 /
12,000 / 15,000 by round), so ``amount`` is NULL.

Output: s3://openalex-ingest/awards/rufford/rufford_projects.parquet
"""

import argparse
import hashlib
import html
import json
import re
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (TWCF-style shim; it renames sys, so for the §4.0 grep: sys.stdout.reconfigure)
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

BASE = "https://www.rufford.org"
LIST_URL = BASE + "/projects/{page}/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/rufford/rufford_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.4
RETRIES = 4
MAX_CONSECUTIVE_EMPTY = 3

MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}

# reference shapes
# application numbers run to 5 digits (cited max ~49,400); a 6-digit "376161-2" is a file-name typo
REF_NEW = r"\d{4,5}-(?:[A-Z]|\d{1,2})"
REF_OLD = r"\d{1,3}\.\d{1,2}\.\d{2}"
REPORT_REF_RE = re.compile(rf"^(?:RSG[\s_-]*)?({REF_NEW}|{REF_OLD})(?=[\s_.\-]|$)", re.I)
IMAGE_REF_RE = re.compile(rf"^(\d{{4,5}}-(?:[A-Z]|\d(?![0-9]))|{REF_OLD})", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> tuple[int, str]:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code == 404:
                return 404, ""
            if r.status_code != 200:
                last_err = f"HTTP {r.status_code}"
                time.sleep(3 * (attempt + 1))
                continue
            r.encoding = "utf-8"
            return 200, r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(3 * (attempt + 1))
    log(f"  GET {url} failed after {RETRIES} tries: {last_err}")
    return -1, ""


def cached_get(url: str, cache_dir: Path | None, refresh: bool = False) -> tuple[int, str]:
    cache = None
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        # full-URL hash: slug truncation made two long slugs share a cache file on Alberta Innovates
        cache = cache_dir / (hashlib.sha1(url.encode("utf-8")).hexdigest() + ".html")
        if cache.exists() and not refresh:
            return 200, cache.read_text()
    status, page = get(url)
    if cache and status == 200 and page:
        cache.write_text(page)
    time.sleep(REQUEST_DELAY)
    return status, page


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<(script|style)\b.*?</\1>", " ", fragment, flags=re.S | re.I)
    t = re.sub(r"<p class=\"caption\">.*?</p>", " ", t, flags=re.S)  # photo captions
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("\xa0", " ").replace("​", "").replace("﻿", "")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py), verbatim."""
    if not name:
        return None, None
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


TITLE_PREFIX_RE = re.compile(r"^(?:dr|prof|professor|mr|mrs|ms|miss)\.?\s+", re.I)


def parse_date(s: str | None) -> str | None:
    if not s:
        return None
    m = re.match(r"(\d{1,2})\s+([A-Za-z]{3})[a-z]*\s+(\d{4})", s.strip())
    if not m or m.group(2).lower() not in MONTHS:
        return None
    return f"{int(m.group(3)):04d}-{MONTHS[m.group(2).lower()]:02d}-{int(m.group(1)):02d}"


def parse_listing(page: str) -> tuple[list[dict], int | None]:
    m = re.search(r"Page\s+\d+\s+of\s+(\d+)", page)
    total = int(m.group(1)) if m else None
    items = []
    for art in re.findall(r'<article class="project-listing-item">(.*?)</article>', page, re.S):
        href = re.search(r'<div class="project-link">\s*<a[^>]+href="(/projects/[^"]+/[^"]+/)"', art, re.S)
        leader = re.search(r'<h4 class="project-leader">(.*?)<span class="project-date">(.*?)</span>', art, re.S)
        title = re.search(r'<p class="project-title">(.*?)</p>', art, re.S)
        country = re.search(r'<div class="project-country">(.*?)</div>', art, re.S)
        if not href:
            continue
        items.append({
            "path": href.group(1),
            "list_leader": text(leader.group(1)) if leader else None,
            "list_date": text(leader.group(2)) if leader else None,
            "list_title": text(title.group(1)) if title else None,
            "list_country": text(country.group(1)) if country else None,
        })
    return items, total


def _not_a_year(ref: str | None) -> str | None:
    # "2019-1 report.pdf" is a year, not grant 2019-1 (real 4-digit references exist, e.g. 8953-2)
    if ref and re.fullmatch(r"(199\d|20[0-3]\d)-.*", ref):
        return None
    return ref


def ref_from_report(fname: str) -> str | None:
    m = REPORT_REF_RE.match(unquote(fname).strip())
    return _not_a_year(m.group(1).upper()) if m else None


def ref_from_image(folder: str) -> str | None:
    m = IMAGE_REF_RE.match(unquote(folder).strip())
    return _not_a_year(m.group(1).upper()) if m else None


def parse_project(page: str) -> dict:
    hero = re.search(r'<section class="hero.*?</section>', page, re.S)
    hero = hero.group(0) if hero else ""
    title = re.search(r'<h2 class="subtitle">(.*?)</h2>', hero, re.S)
    date = re.search(r'<span class="project-date">(.*?)</span>', hero, re.S)
    loc = re.search(r'<span class="project-location[^"]*">(.*?)<span class="project-categories', hero, re.S) \
        or re.search(r'<span class="project-location[^"]*">(.*?)</p>', hero, re.S)
    loc_html = loc.group(1) if loc else ""
    country_code = re.search(r'href="/projects/country/([A-Za-z]{2})/"', loc_html)
    country_name = re.search(r'href="/projects/country/[A-Za-z]{2}/"[^>]*>(.*?)</a>', loc_html, re.S)
    continent = re.search(r'href="/projects/continent/([a-z_]+)/"', loc_html)
    # place = text before the country link
    place = None
    if country_code:
        before = loc_html[: loc_html.find('href="/projects/country/')]
        before = re.sub(r"<a\b[^>]*$", "", before, flags=re.S)
        place = (text(before) or "").strip(" ,") or None
    cats_html = re.search(r'<span class="project-categories.*?</span>\s*</p>', hero, re.S)
    cats = re.findall(r'href="/projects/category/([^"/]+)/"[^>]*>\s*(.*?)\s*</a>', cats_html.group(0) if cats_html else "", re.S)
    leader = re.search(r'<div class="small-headline">\s*<h4[^>]*>(.*?)</h4>', page, re.S)
    summary = re.search(r'<div class="summary content">(.*?)</div>', page, re.S)
    body = re.search(r'<div class="body content">(.*?)</div>\s*(?:<a name="updates"|<h3|</div>)', page, re.S)
    reports = re.findall(r'media/project_reports/([^"]+?)"', page)
    images = re.findall(r'media/(?:cached/)?project_images/([^/"]+)/', page)
    rep_refs = Counter(r for r in (ref_from_report(f) for f in reports) if r)
    img_refs = Counter(r for r in (ref_from_image(f) for f in images) if r)
    if rep_refs:
        ref, ref_source = rep_refs.most_common(1)[0][0], "report_file"
    elif img_refs:
        ref, ref_source = img_refs.most_common(1)[0][0], "image_folder"
    else:
        ref, ref_source = None, None
    return {
        "title": text(title.group(1)) if title else None,
        "date_text": text(date.group(1)) if date else None,
        "place": place,
        "country_code": country_code.group(1).upper() if country_code else None,
        "country_name": (text(country_name.group(1)) or "").strip(" ,") or None if country_name else None,
        "continent": continent.group(1) if continent else None,
        "categories": [text(c[1]) for c in cats],
        "leader": text(leader.group(1)) if leader else None,
        "summary": text(summary.group(1)) if summary else None,
        "body": text(body.group(1)) if body else None,
        "ref": ref,
        "ref_source": ref_source,
        "ref_candidates": sorted(set(rep_refs) | set(img_refs)),
        "n_report_files": len(set(reports)),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--output-dir", type=Path, default=Path("./rufford_out"))
    ap.add_argument("--cache-dir", type=Path, default=None, help="cache fetched pages here (resumable)")
    ap.add_argument("--limit", type=int, default=None, help="only fetch the first N project pages (smoke test)")
    ap.add_argument("--max-pages", type=int, default=400, help="safety cap on listing pages")
    ap.add_argument("--workers", type=int, default=1, help="parallel project-page fetchers (needs --cache-dir)")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    args = ap.parse_args()

    # ---- 1. listing pages (always re-fetched: page boundaries shift as projects are added)
    status, first = get(LIST_URL.format(page=1).replace("/1/", "/"))
    if status != 200:
        raise SystemExit(f"listing page 1: HTTP {status}")
    items, total_pages = parse_listing(first)
    if not total_pages:
        raise SystemExit("could not read 'Page 1 of N' from the listing")
    log(f"listing: {total_pages} pages")
    seen = {i["path"] for i in items}
    consecutive_empty = 0
    last_page = min(total_pages, args.max_pages)
    if args.limit:
        last_page = min(last_page, args.limit // 20 + 1)
    for p in range(2, last_page + 1):
        status, page = get(LIST_URL.format(page=p))
        time.sleep(REQUEST_DELAY)
        got, _ = parse_listing(page) if status == 200 else ([], None)
        if not got:
            consecutive_empty += 1
            log(f"  listing page {p}: no items (HTTP {status}) ({consecutive_empty}/{MAX_CONSECUTIVE_EMPTY}); continuing")
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                raise SystemExit(f"{MAX_CONSECUTIVE_EMPTY} consecutive empty listing pages before page {total_pages}; aborting")
            continue
        consecutive_empty = 0
        for it in got:
            if it["path"] not in seen:
                seen.add(it["path"])
                items.append(it)
        if p % 25 == 0:
            log(f"  listing page {p}/{last_page}: {len(items)} projects so far")
    log(f"listing done: {len(items)} distinct project pages")
    if not args.limit and len(items) < (total_pages - 1) * 20 * 0.95:
        raise SystemExit(f"only {len(items)} projects for {total_pages} pages; listing walk looks truncated")
    if args.limit:
        items = items[: args.limit]

    # ---- 2. project pages (optionally prefetched into the cache by a few polite workers)
    if args.workers > 1 and args.cache_dir:
        from concurrent.futures import ThreadPoolExecutor
        urls = [BASE + it["path"] for it in items]
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for i, _ in enumerate(pool.map(lambda u: cached_get(u, args.cache_dir), urls), 1):
                if i % 200 == 0:
                    el = time.time() - t0
                    log(f"  prefetched {i}/{len(urls)} project pages - ETA {el / i * (len(urls) - i) / 60:.0f} min")
    rows = []
    failed = []
    t0 = time.time()
    for i, it in enumerate(items, 1):
        url = BASE + it["path"]
        status, page = cached_get(url, args.cache_dir)
        if status != 200 or not page:
            failed.append((url, status))
            det = {}
        else:
            det = parse_project(page)
        parts = [unquote(x) for x in it["path"].strip("/").split("/")]
        leader = det.get("leader") or it["list_leader"]
        given, family = split_name(TITLE_PREFIX_RE.sub("", leader or "").strip())
        date_iso = parse_date(det.get("date_text") or it["list_date"])
        desc = " ".join(x for x in [det.get("summary"), det.get("body")] if x) or None
        rows.append({
            "project_ref": det.get("ref"),
            "project_ref_source": det.get("ref_source"),
            "project_ref_candidates": json.dumps(det.get("ref_candidates") or []),
            "page_path": "/".join(parts[1:]),
            "title": det.get("title") or it["list_title"],
            "summary": det.get("summary"),
            "description": desc,
            "award_date": date_iso,
            "award_year": date_iso[:4] if date_iso else None,
            "lead_name": leader,
            "lead_given_name": given,
            "lead_family_name": family,
            "place": det.get("place"),
            "country_code": det.get("country_code"),
            "country_name": det.get("country_name") or it["list_country"],
            "continent": det.get("continent"),
            "categories": json.dumps(det.get("categories") or [], ensure_ascii=False),
            "n_report_files": det.get("n_report_files"),
            "landing_page_url": url,
        })
        if i % 100 == 0:
            el = time.time() - t0
            eta = el / i * (len(items) - i)
            log(f"  {i}/{len(items)} project pages ({i/len(items):.1%}) - ETA {eta/60:.0f} min; refs so far {sum(1 for r in rows if r['project_ref'])}")
    if failed:
        log(f"{len(failed)} project pages failed: {failed[:10]}")
        if len(failed) > max(5, 0.01 * len(items)):
            raise SystemExit("too many failed project pages; rerun (cache makes it resumable)")

    df = pd.DataFrame(rows)

    # ---- 3. reference uniqueness: a reference claimed by >1 page is ambiguous unless exactly
    # one claimant got it from its own report files
    claims = df[df["project_ref"].notna()].groupby("project_ref").size()
    multi = set(claims[claims > 1].index)
    if multi:
        for ref in multi:
            idx = df.index[df["project_ref"] == ref]
            rep = [j for j in idx if df.at[j, "project_ref_source"] == "report_file"]
            keep = rep[0] if len(rep) == 1 else None
            for j in idx:
                if j != keep:
                    df.at[j, "project_ref"] = None
                    df.at[j, "project_ref_source"] = "ambiguous:" + ref
        log(f"{len(multi)} references were claimed by >1 page; kept only unambiguous report-file claims")
    df["funder_award_id"] = df["project_ref"].where(df["project_ref"].notna(), "RUFFORD-" + df["page_path"])
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()[:20]}")

    log(f"{len(df)} awards; reference source: {df['project_ref_source'].fillna('none').str.replace(r':.*', '', regex=True).value_counts().to_dict()}")
    for c in ["title", "description", "award_date", "lead_family_name", "country_code", "project_ref"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "rufford_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload or args.limit:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_rufford_projects.parquet"
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
