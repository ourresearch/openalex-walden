#!/usr/bin/env python3
"""
Breast Cancer Research Foundation (BCRF) to S3 Data Pipeline
============================================================

BCRF publishes its funded investigators in the "Meet Our Researchers" directory
at https://www.bcrf.org/researchers/ (WordPress custom post type, not exposed
through wp-json; every profile is listed in
https://www.bcrf.org/researchers-sitemap.xml, ~315 profiles). Method 5 (static
HTML). Each profile carries:

    name + degrees (h1), institution and city/region/country (subtitle),
    Titles and Affiliations, Research area (a one-line statement of what the
    BCRF grant funds, then Impact / Progress Thus Far / What's next prose),
    Biography, "BCRF Investigator Since" (year), Areas of Focus.

There is no grant number, no amount and no grant dates on the profile, so one
row = one BCRF-funded investigator (the investigator's BCRF research grant),
keyed by a synthetic `BCRF-INV-{profile slug}`.

BCRF also registers grant DOIs with Crossref itself (prefix 10.63055, award
numbers BCRF-YY-NNN / CONS-YY-NNN / SPEC-YY-NNN, ~107 deposits, mostly the 2026
cycle), which are already in OpenAlex as `crossref_work` award rows with full
title, abstract, amount, dates and PI ORCID. A profile whose investigator is
the lead investigator of one of those deposits (matched on family name + first
given initial) is therefore NOT shipped again: it is written to the parquet with
`excluded=1, exclusion_reason=covered_by_crossref_grant` and the deposit's award
number in `crossref_award`, so the directory row cannot duplicate (or, at a
higher priority, overwrite) BCRF's richer Crossref record.

Output: s3://openalex-ingest/awards/bcrf/bcrf_projects.parquet
"""

import argparse
import html
import json
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O. See runbook §1.2. (grep anchor: sys.stdout.reconfigure)
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

BASE = "https://www.bcrf.org"
SITEMAP = f"{BASE}/researchers-sitemap.xml"
FUNDER_DOI = "10.13039/100001006"  # BCRF, OpenAlex F4320306260
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/bcrf/bcrf_projects.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 openalex-walden/1.0"}
REQUEST_DELAY = 0.5
RETRIES = 3

US_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut", "delaware",
    "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky",
    "louisiana", "maine", "maryland", "massachusetts", "michigan", "minnesota", "mississippi",
    "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey", "new mexico",
    "new york", "north carolina", "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania",
    "rhode island", "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont",
    "virginia", "washington", "west virginia", "wisconsin", "wyoming", "district of columbia",
    "washington, dc", "dc", "d.c.", "puerto rico", "usa", "united states", "hawai'i", "hawai’i",
}


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


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<br\s*/?>", "\n", fragment)
    t = re.sub(r"</p>\s*<p[^>]*>", "\n\n", t)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = "\n".join(re.sub(r"[ \t]+", " ", line).strip() for line in t.split("\n"))
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    return t or None


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|sir|dame|mr|mrs|ms)\.?\s+)+", re.I)
DEGREE_RE = re.compile(r",.*$")  # "Adrian Lee, PhD" / "Melissa Troester, PhD, MPH"


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py / twcf_to_s3.py), with a
    leading-honorific strip; degrees after a comma are removed before calling."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                "mph", "facp", "frcpc", "frcp", "facs", "faacr", "ms", "msc", "mba", "rn", "do", "mbbs"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def fold(s: str | None) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"[^a-z]", "", s.lower())


COUNTRY_ISO = {
    "united kingdom": "GB", "uk": "GB", "england": "GB", "scotland": "GB", "wales": "GB",
    "australia": "AU", "italy": "IT", "canada": "CA", "switzerland": "CH", "spain": "ES",
    "belgium": "BE", "israel": "IL", "france": "FR", "argentina": "AR", "rwanda": "RW",
    "germany": "DE", "netherlands": "NL", "the netherlands": "NL", "japan": "JP", "china": "CN",
    "singapore": "SG", "sweden": "SE", "denmark": "DK", "norway": "NO", "austria": "AT",
    "ireland": "IE", "portugal": "PT", "mexico": "MX", "brazil": "BR", "south korea": "KR",
    "korea": "KR", "india": "IN", "nigeria": "NG", "kenya": "KE", "greece": "GR", "finland": "FI",
    "hong kong": "HK", "taiwan": "TW", "chile": "CL", "colombia": "CO", "peru": "PE",
    "south africa": "ZA", "uganda": "UG", "ghana": "GH", "poland": "PL", "czech republic": "CZ",
}
CA_PROVINCES = {"ontario", "quebec", "québec", "british columbia", "alberta", "manitoba",
                "nova scotia", "saskatchewan", "new brunswick"}


def country_of(location: str | None) -> str | None:
    """ISO-3166 alpha-2 from the profile's "City, State" / "City, Country" line."""
    if not location:
        return None
    last = location.split(",")[-1].strip().lower()
    if last in US_STATES or location.strip().lower() in US_STATES:
        return "US"
    if last in CA_PROVINCES:
        return "CA"
    return COUNTRY_ISO.get(last)


def h2_sections(content: str) -> dict[str, str]:
    """Split the profile body on its <h2> headings. Two layouts exist (Vue blocks
    with div wrappers, and a flat h2/p layout for e.g. Conquer Cancer fellows);
    splitting on h2 regardless of div nesting reads both."""
    parts = re.split(r"<h2[^>]*>(.*?)</h2>", content, flags=re.S)
    return {(text(parts[i]) or "").lower(): parts[i + 1] for i in range(1, len(parts) - 1, 2)}


def parse_profile(url: str, page: str) -> dict | None:
    start = page.find('<h1 class="name')
    if start < 0:
        return None
    end = page.find("</section>", page.find('class="single-researcher"', start))
    body = page[start:end if end > 0 else None]
    h1 = re.search(r"<h1[^>]*>(.*?)</h1>", body, re.S)
    full = text(h1.group(1)) if h1 else None
    name = DEGREE_RE.sub("", full).strip() if full else None
    given, family = split_name(name)
    sub = re.search(r'<div class="subtitle">\s*<p>(.*?)</p>', body, re.S)
    sub_lines = [l.strip() for l in (text(sub.group(1)) or "").split("\n") if l.strip()] if sub else []
    # usually "Institution / City, Region"; a few wrap the institution over two lines
    if len(sub_lines) > 1 and "," in sub_lines[-1]:
        institution, location = " ".join(sub_lines[:-1]), sub_lines[-1]
    else:
        institution, location = (" ".join(sub_lines) or None), None
    ec = body.find("researcher-entry-content")
    sidebar = body.find("researcher-sidebar")
    secs = h2_sections(body[ec:sidebar if sidebar > 0 else None] if ec >= 0 else "")
    titles = secs.get("titles and affiliations")
    ra_html = secs.get("research area") or ""
    ra_inner = re.search(r'<div class="research-content"[^>]*>(.*)', ra_html, re.S)
    if ra_inner:
        ra_html = ra_inner.group(1)
    first_p = re.search(r"<p[^>]*>(.*?)</p>", ra_html, re.S)
    research_area = text(first_p.group(1)) if first_p else None
    # Impact / Progress Thus Far / What's next: the grant's own narrative
    sections = re.split(r"<h3[^>]*>(.*?)</h3>", ra_html)
    narrative = []
    for i in range(1, len(sections) - 1, 2):
        head, chunk = text(sections[i]), text(sections[i + 1])
        if chunk:
            narrative.append(f"{head}: {chunk}" if head else chunk)
    since = re.search(r'Investigator Since</h3>\s*<p class="year">\s*(\d{4})', body)
    aof = [text(a) for a in re.findall(r'class="aof-link">(.*?)</a>', body, re.S)]
    return {
        "slug": url.rstrip("/").rsplit("/", 1)[-1],
        "name_full": full,
        "name": name,
        "given_name": given,
        "family_name": family,
        "institution": institution,
        "location": location,
        "country": country_of(location),
        "titles_affiliations": text(titles) if titles else None,
        "research_area": research_area,
        "description": "\n\n".join(narrative) or None,
        "investigator_since": since.group(1) if since else None,
        "areas_of_focus": json.dumps([a for a in aof if a], ensure_ascii=False),
        "landing_page_url": url,
    }


def crossref_grants() -> list[dict]:
    """BCRF's own Crossref grant deposits (award number, lead investigator)."""
    out, cursor = [], "*"
    while True:
        r = requests.get(
            "https://api.crossref.org/works",
            params={"filter": f"type:grant,award.funder:{FUNDER_DOI}", "rows": 1000,
                    "cursor": cursor, "mailto": "team@ourresearch.org"},
            headers=HEADERS, timeout=120,
        )
        r.raise_for_status()
        msg = r.json()["message"]
        for item in msg["items"]:
            for proj in item.get("project", []):
                for li in proj.get("lead-investigator", []):
                    out.append({"award": (item.get("award") or "").strip(), "doi": item["DOI"].lower(),
                                "given": li.get("given"), "family": li.get("family")})
        if not msg["items"]:
            return out
        cursor = msg["next-cursor"]


def main() -> None:
    p = argparse.ArgumentParser(description="BCRF researchers directory -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    urls = sorted({u.strip() for u in re.findall(r"<loc>([^<]+)</loc>", get(SITEMAP))
                   if re.search(r"/researchers/[^/]+/?$", u)})
    log(f"Sitemap: {len(urls)} researcher profiles")
    if args.limit:
        urls = urls[: args.limit]

    rows, skipped = [], []
    for i, url in enumerate(urls, 1):
        cache = None
        if args.cache_dir:
            args.cache_dir.mkdir(parents=True, exist_ok=True)
            cache = args.cache_dir / (url.rstrip("/").rsplit("/", 1)[-1][:80] + ".html")
        if cache and cache.exists() and cache.stat().st_size > 0:
            page = cache.read_text()
        else:
            page = get(url)
            if cache:
                cache.write_text(page)
            time.sleep(REQUEST_DELAY)
        rec = parse_profile(url, page) if page else None
        if rec is None or not rec["family_name"]:
            skipped.append(url)
        else:
            rows.append(rec)
        if i % 50 == 0:
            log(f"  {i}/{len(urls)} fetched, {len(rows)} parsed")
    df = pd.DataFrame(rows)
    log(f"Parsed {len(df)} profiles, {len(skipped)} skipped")
    for u in skipped[:20]:
        log(f"  skipped: {u}")

    xref = crossref_grants()
    log(f"Crossref: {len(xref)} BCRF grant deposits")
    idx = {}
    for g in xref:
        key = (fold(g["family"]), fold(g["given"])[:1])
        idx.setdefault(key, []).append(g["award"])
    df["crossref_award"] = [
        "; ".join(sorted(set(idx.get((fold(f), fold(g)[:1]), [])))) or None
        for f, g in zip(df["family_name"], df["given_name"])
    ]
    df["funder_award_id"] = "BCRF-INV-" + df["slug"].str.lower()
    df["exclusion_reason"] = df["crossref_award"].map(lambda a: "covered_by_crossref_grant" if a else None)
    df["excluded"] = df["exclusion_reason"].notna().map(lambda b: "1" if b else "0")
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    kept = df[df["excluded"] == "0"]
    log(f"  covered by a Crossref deposit (not shipped): {(df['excluded'] == '1').sum()}; to ship: {len(kept)}")
    for c in ["research_area", "description", "institution", "country", "investigator_since", "titles_affiliations"]:
        log(f"  {c:20s} {kept[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "bcrf_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_bcrf_projects.parquet"
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
