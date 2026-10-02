#!/usr/bin/env python3
"""
Wilhelm Sander-Stiftung (WSS) to S3 Data Pipeline
=================================================

The Wilhelm Sander-Stiftung (Munich; cancer and medical research) publishes
its funded-projects database at https://www.wilhelm-sander-stiftung.de/forschungsprojekte/
(map + search, a WordPress plugin "foerderprojekte-manager"). The page reads
its data from the plugin's public WordPress REST namespace:

    GET /wp-json/foerderprojekte/v1/stats                      ({"total_projects": ...})
    GET /wp-json/foerderprojekte/v1/filter-options             (universities, types, years)
    GET /wp-json/foerderprojekte/v1/projects/by-university/ID  (all projects of one university)
    GET /wp-json/foerderprojekte/v1/search?search=...          (map/search box)
    GET /wp-json/wp/v2/foerderprojekte                         (core WP list: every post id)

Each project carries: WordPress post id, title, funding year (pyear),
summary, institute, applicant {title, firstname, lastname}, categories
(organ system), type (research line), funding_amount ("69.360 €") and, for
~10%, funding_period ("01.03.2026 - 01.03.2027"). The by-university endpoint
is complete per university; the few projects with no university are found
via /search on their title. Completeness is checked against the core WP
post list and the plugin's stats total. Method 2 (WordPress REST) on the
runbook ladder. robots.txt (checked 2026-10-01) disallows only /wp-admin/.

The site does not publish the WSS grant number that citing works quote
("2017.009.1"), and project detail pages redirect to the home page, so
funder_award_id is the synthetic, stable key "WSS-{post id}".

Output: s3://openalex-ingest/awards/wilhelm_sander/wilhelm_sander_projects.parquet
"""

import argparse
import html
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Same block as twcf_to_s3.py; the grep in runbook §4.0 looks for
# sys.stdout.reconfigure (this shim calls it via the renamed module).
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

BASE = "https://www.wilhelm-sander-stiftung.de"
API = f"{BASE}/wp-json/foerderprojekte/v1"
LANDING = f"{BASE}/forschungsprojekte/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/wilhelm_sander/wilhelm_sander_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5
RETRIES = 4


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get_json(url: str, params: dict | None = None):
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=60)
            log(f"GET {r.url} -> {r.status_code} ({len(r.content)} bytes)")
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} {params} failed: {last_err}")


C1 = re.compile(r"[\x80-\x9f]")


def clean(s) -> str | None:
    """Strip tags/entities and repair stray cp1252 C1 characters that the
    plugin stores as latin-1 code points (U+0096 for an en dash, etc.)."""
    if s is None:
        return None
    t = str(s)
    t = C1.sub(lambda m: bytes([ord(m.group(0))]).decode("cp1252", errors="replace"), t)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("​", "").replace("﻿", "")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def cached_json(name: str, url: str, params: dict | None, cache_dir: Path | None):
    cache = cache_dir / name if cache_dir else None
    if cache and cache.exists():
        return json.loads(cache.read_text())
    d = get_json(url, params)
    if cache:
        cache.write_text(json.dumps(d, ensure_ascii=False))
    time.sleep(REQUEST_DELAY)
    return d


def list_posts(cache_dir: Path | None) -> dict[int, str]:
    """Every published foerderprojekt post (id -> title) from the core WP REST
    API; the authoritative enumeration the plugin endpoints are checked against."""
    out, page, total_pages = {}, 1, None
    while total_pages is None or page <= total_pages:
        url = f"{BASE}/wp-json/wp/v2/foerderprojekte"
        params = {"per_page": 100, "page": page, "_fields": "id,title", "orderby": "id", "order": "asc"}
        r = requests.get(url, params=params, headers=HEADERS, timeout=90)
        log(f"GET {r.url} -> {r.status_code}")
        if r.status_code != 200:
            raise RuntimeError(f"wp/v2/foerderprojekte page {page}: HTTP {r.status_code}")
        total_pages = int(r.headers.get("X-WP-TotalPages", "1"))
        for x in r.json():
            out[int(x["id"])] = clean(x["title"]["rendered"]) or ""
        page += 1
        time.sleep(REQUEST_DELAY)
    return out


def fetch_all(limit: int | None, cache_dir: Path | None) -> tuple[list[dict], int]:
    """1) /projects/by-university/{id} for every university in filter-options:
    complete per university (no pagination; total == len) and the only
    endpoint that returns funding_period. 2) Projects with no university are
    found by searching their WP title on /search and matching the post id.
    (The /search endpoint alone cannot enumerate the corpus: it orders by year
    only, so offset pages overlap; a plain offset walk returned 2,301 of 2,799.)"""
    total = int(get_json(f"{API}/stats")["total_projects"])
    posts = list_posts(cache_dir)
    opts = get_json(f"{API}/filter-options")
    unis = [(i.strip(), u["title"]) for u in opts["universities"] for i in str(u["id"]).split(",") if i.strip()]
    log(f"stats total {total}; wp/v2 posts {len(posts)}; {len(unis)} university ids")
    seen: dict[int, dict] = {}
    for n, (uid, utitle) in enumerate(unis, 1):
        d = cached_json(f"uni_{uid}.json", f"{API}/projects/by-university/{uid}", None, cache_dir)
        projects = d.get("projects") or []
        if int(d.get("total") or 0) != len(projects):
            raise RuntimeError(f"university {uid}: total {d.get('total')} != {len(projects)} returned")
        for p in projects:
            seen.setdefault(int(p["id"]), {**p, "university": {"id": uid, "title": utitle}, "_source": "by_university"})
        if n % 10 == 0:
            log(f"  {n}/{len(unis)} universities: {len(seen)} projects")
        if limit and len(seen) >= limit:
            return list(seen.values()), total
    missing = sorted(set(posts) - set(seen))
    log(f"{len(seen)} projects via universities; {len(missing)} posts without a university, searching by title")
    for pid in missing:
        # search on the longest plain-ASCII-word run of the title: the plugin stores some
        # punctuation as cp1252 C1 code points, which never match the WP title's en dash
        words = re.findall(r"[A-Za-z0-9ÄÖÜäöüß\-]{3,}", posts[pid])
        q = " ".join(words[:6]) or posts[pid]
        d = cached_json(f"search_post_{pid}.json", f"{API}/search", {"search": q}, cache_dir)
        hits = [p for c in (d.get("results") or {}).values() if isinstance(c, dict)
                for ps in c.values() for p in ps if int(p["id"]) == pid]
        if hits:
            h = hits[0]
            seen[pid] = {**h, "pyear": h.get("year"), "funding_period": None, "_source": "search"}
        else:
            log(f"  post {pid} not found by title search: {posts[pid][:80]}")
    return list(seen.values()), total


def proper(name: str | None) -> str | None:
    """Pre-2000 records store applicants in capitals ("GEBICKE-HAERTER",
    "GERHARD U."); title-case those only. Mixed-case names are left as is."""
    if not name or not name.isupper():
        return name
    return re.sub(r"(Von|Van|Der|Den|De|Zu|Und)", lambda m: m.group(1).lower(), name.title())


def parse_amount(v) -> float | None:
    """by-university: '69.360 €' (German thousands dots); search: 69360 (int)."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v) if v > 0 else None
    s = re.sub(r"[^\d.,]", "", str(v)).replace(".", "").replace(",", ".")
    try:
        x = float(s)
    except ValueError:
        return None
    return x if x > 0 else None


def parse_period(v) -> tuple[str | None, str | None]:
    """'01.03.2026 - 01.03.2027' or a single '01.03.2026' -> ISO dates."""
    ds = re.findall(r"(\d{2})\.(\d{2})\.(\d{4})", str(v or ""))
    iso = [f"{y}-{m}-{d}" for d, m, y in ds]
    return (iso[0] if iso else None), (iso[1] if len(iso) > 1 else None)


def to_row(p: dict) -> dict:
    a = p.get("applicant") or {}
    u = p.get("university") or {}
    t = p.get("type") or {}
    cats = p.get("categories") or []
    amt = p.get("funding_amount")
    amount = parse_amount(amt)
    start, end = parse_period(p.get("funding_period"))
    return {
        "wss_post_id": str(p["id"]),
        "funder_award_id": f"WSS-{p['id']}",
        "title": clean(p.get("title")),
        "year": str(p["pyear"]) if p.get("pyear") else None,
        "funding_period": clean(p.get("funding_period")),
        "start_date": start,
        "end_date": end,
        "summary": clean(p.get("summary")),
        "institute": clean(p.get("institute")),
        "applicant_title": clean(a.get("title")),
        "applicant_firstname_raw": clean(a.get("firstname")),
        "applicant_lastname_raw": clean(a.get("lastname")),
        "lead_given_name": proper(clean(a.get("firstname"))),
        "lead_family_name": proper(clean(a.get("lastname"))),
        "university_id": str(u.get("id")) if isinstance(u, dict) and u.get("id") else None,
        "university": clean(u.get("title")) if isinstance(u, dict) else None,
        "type_id": str(t.get("id")) if isinstance(t, dict) and t.get("id") else None,
        "type": clean(t.get("title")) if isinstance(t, dict) else None,
        "categories": "; ".join(c for c in (clean(c.get("title")) for c in cats) if c) or None,
        "funding_amount_raw": None if amt is None else str(amt),
        "amount": amount,
        "currency": "EUR" if amount is not None else None,
        "source_endpoint": p.get("_source"),
        "landing_page_url": LANDING,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Wilhelm Sander-Stiftung projects -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--cache-dir", type=Path, default=None, help="cache raw API pages here")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = ap.parse_args()
    if args.cache_dir:
        args.cache_dir.mkdir(parents=True, exist_ok=True)

    projects, total = fetch_all(args.limit, args.cache_dir)
    if args.limit:
        projects = projects[: args.limit]
    elif len(projects) < total:
        raise SystemExit(f"only {len(projects)} of {total} projects fetched; refusing to write a partial corpus")
    df = pd.DataFrame([to_row(p) for p in projects])
    # ~170 projects exist as two consecutive WP posts with identical content
    # (e.g. 15397/15398: same title, applicant, year, amount, institute) -- a
    # double import on the site. Keep the lower post id; record the other.
    key = ["title", "lead_given_name", "lead_family_name", "year", "amount", "institute", "university"]
    df["_pid"] = df["wss_post_id"].astype(int)
    df = df.sort_values("_pid")
    grp = df[key].astype("string").fillna("<null>").agg("|".join, axis=1)
    df["duplicate_post_ids"] = grp.map(
        df.groupby(grp)["wss_post_id"].agg(lambda s: ",".join(s.iloc[1:]) or None))
    before = len(df)
    df = df[~grp.duplicated(keep="first")].drop(columns="_pid")
    log(f"dropped {before - len(df)} duplicate posts (identical {'/'.join(key)}); {len(df)} distinct projects")
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")

    log(f"{len(df)} projects (stats total {total})")
    for c in ["title", "year", "start_date", "end_date", "summary", "lead_family_name", "university", "institute", "type", "amount"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  years {df['year'].min()}-{df['year'].max()}; total EUR {df['amount'].sum():,.0f}")
    log("  types: " + "; ".join(f"{k}={v}" for k, v in df["type"].value_counts(dropna=False).items()))

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "wilhelm_sander_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_wilhelm_sander_projects.parquet"
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
