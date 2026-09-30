#!/usr/bin/env python3
"""
NordForsk to S3 Data Pipeline
=============================

NordForsk (Nordic Council of Ministers' research funding body, Oslo) publishes
every funded project on its Drupal site at https://www.nordforsk.org/projects/<slug>.
The project list is enumerated by the Drupal Simple XML Sitemap
(sitemap.xml?page=1..2; ~310 English project pages plus Swedish copies). Each
project page carries an <aside class="article__facts"> block:

    Project number   (e.g. 82845 -- the number grantees cite; Crossref
                      funding metadata carries the same bare 5-6 digit form)
    Project leader   ("Name, Institution" / "Name (Institution)" / "Name")
    Call, Programme, Research area, Project website

plus a year span tag ("2017 - 2021") and a free-text description. NordForsk
publishes NO grant amounts on project pages, so amount/currency ship NULL.
Method 5 (static HTML) on the runbook ladder; no export/API exists.

Swedish (/sv/) pages are fetched only when no English page has the same slug,
then de-duplicated on project number (English wins).

Output: s3://openalex-ingest/awards/nordforsk/nordforsk_projects.parquet
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
# (runbook §1.2 item 7; equivalent to sys.stdout.reconfigure(encoding="utf-8") + open() default utf-8)
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

BASE = "https://www.nordforsk.org"
SITEMAPS = [f"{BASE}/sitemap.xml?page={i}" for i in (1, 2)]
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/nordforsk/nordforsk_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.3
RETRIES = 3


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code == 404:
                return ""
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def project_urls() -> list[str]:
    locs = []
    for sm in SITEMAPS:
        xml = get(sm)
        found = re.findall(r"<loc>([^<]+)</loc>", xml)
        log(f"  {sm}: {len(found)} urls")
        locs += found
    en = sorted({u.strip() for u in locs if re.search(r"nordforsk\.org/projects/[^/]+$", u)})
    en_slugs = {u.rsplit("/", 1)[-1] for u in en}
    sv = sorted({u.strip() for u in locs if re.search(r"nordforsk\.org/sv/projects/[^/]+$", u)
                 and u.rsplit("/", 1)[-1] not in en_slugs})
    log(f"Sitemap: {len(en)} English project pages, {len(sv)} Swedish-only slugs")
    if len(en) < 250:
        raise RuntimeError(f"sitemap returned only {len(en)} English project pages; expected ~310")
    return en + sv


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py) plus a leading honorific strip."""
    if not name:
        return None, None
    tokens = re.sub(r"^(?:(?:dr|prof|professor|docent)\.?\s+)+", "", name.strip(), flags=re.I).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_leader(s: str | None) -> list[dict]:
    """'Name, Institution[, Country]' / 'Name (Institution)' / 'Name' /
    'A and B' -> list of {name, given_name, family_name, institution}."""
    if not s:
        return []
    s = s.strip(" ,;")
    inst = None
    m = re.match(r"^([^,(]+?)\s*\((.+)\)\s*$", s)
    if m:
        name, inst = m.group(1), m.group(2)
    elif "," in s:
        name, inst = s.split(",", 1)
    else:
        name = s
    out = []
    for nm in re.split(r"\s+(?:and|og|och|&)\s+", name.strip()):
        nm = nm.strip(" ,")
        if not nm:
            continue
        g, f = split_name(nm)
        out.append({"name": nm, "given_name": g, "family_name": f,
                    "institution": inst.strip(" ,") if inst else None})
    return out


def parse_project(url: str, page: str) -> dict | None:
    aside = re.search(r'<aside class="article__facts">(.*?)</aside>', page, re.S)
    if not aside:
        return None
    facts = {}
    for k, v in re.findall(r"<h4>(.*?)</h4>\s*(.*?)(?=<h4>|$)", aside.group(1), re.S):
        key = (text(k) or "").lower().rstrip(":").strip()
        if key and key not in facts:
            facts[key] = v
    num = text(facts.get("project number"))
    num = re.sub(r"\D", "", num) if num else None
    title = re.search(r'<h1 class="article__title">(.*?)</h1>', page, re.S)
    years = re.search(r'icon-calendar"></i>(.*?)</span>', page, re.S)
    ys = re.findall(r"(?:19|20)\d{2}", text(years.group(1)) or "") if years else []
    body = re.search(r'<div class="article__body">(.*?)(?:<div class="[^"]*contacts|<footer|Sign up to our newsletter)', page, re.S)
    call = re.search(r"<a[^>]*>(.*?)</a>", facts.get("call", ""), re.S)
    area_html = facts.get("research area") or facts.get("research areas") or ""
    areas = [text(a) for a in re.findall(r"<a[^>]*>(.*?)</a>", area_html, re.S)] or ([text(area_html)] if text(area_html) else [])
    website = re.search(r'href="(https?://[^"]+)"', facts.get("project website") or facts.get("project homepage") or facts.get("website") or "")
    leader_raw = text(facts.get("project leader"))
    people = parse_leader(leader_raw)
    lead = people[0] if people else None
    return {
        "project_number": num or None,
        "title": text(title.group(1)) if title else None,
        "start_year": ys[0] if ys else None,
        "end_year": ys[-1] if len(ys) > 1 else None,
        "leader_raw": leader_raw,
        "lead_name": lead["name"] if lead else None,
        "lead_given_name": lead["given_name"] if lead else None,
        "lead_family_name": lead["family_name"] if lead else None,
        "lead_institution": lead["institution"] if lead else None,
        "people_json": json.dumps(people, ensure_ascii=False) if people else None,
        "call": text(call.group(1)) if call else text(facts.get("call")),
        "programme": text(facts.get("programme")) or text(facts.get("initiative")),
        "research_area": "; ".join(a for a in areas if a) or None,
        "project_website": website.group(1) if website else None,
        "description": text(body.group(1)) if body else None,
        "landing_page_url": url,
        "language": "sv" if "/sv/projects/" in url else "en",
    }


def main() -> None:
    p = argparse.ArgumentParser(description="NordForsk projects -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    urls = project_urls()
    if args.limit:
        urls = urls[: args.limit]

    rows, skipped = [], []
    t0 = time.time()
    for i, url in enumerate(urls, 1):
        cache = None
        if args.cache_dir:
            args.cache_dir.mkdir(parents=True, exist_ok=True)
            prefix = "sv__" if "/sv/" in url else ""
            cache = args.cache_dir / (prefix + url.rsplit("/", 1)[-1][:80] + ".html")
        if cache and cache.exists():
            page = cache.read_text()
        else:
            page = get(url)
            if cache and page:
                cache.write_text(page)
            time.sleep(REQUEST_DELAY)
        rec = parse_project(url, page) if page else None
        if rec is None or not rec["project_number"]:
            skipped.append(url)
        else:
            rows.append(rec)
        if i % 50 == 0:
            el = time.time() - t0
            log(f"  {i}/{len(urls)} fetched, {len(rows)} parsed, ETA {el / i * (len(urls) - i):.0f}s")

    df = pd.DataFrame(rows)
    before = len(df)
    df = df.sort_values("language").drop_duplicates(subset=["project_number"], keep="first")  # en before sv
    log(f"Parsed {before} pages -> {len(df)} unique project numbers; {len(skipped)} pages without a project number")
    for u in skipped:
        log(f"  skipped (no project number): {u}")
    df["funder_award_id"] = df["project_number"]
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    for c in ["title", "start_year", "end_year", "lead_family_name", "lead_institution",
              "call", "programme", "research_area", "description"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "nordforsk_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_nordforsk_projects.parquet"
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
