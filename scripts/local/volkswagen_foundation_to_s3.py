#!/usr/bin/env python3
"""
Volkswagen Foundation (VolkswagenStiftung) to S3 Data Pipeline
==============================================================

The foundation publishes its funded projects since 2004 in the public
"Projektdatenbank" at https://projektdatenbank.volkswagenstiftung.de/
(ASP.NET Blazor Server app; every page is server-prerendered, so plain HTTP
works). robots.txt allows everything and points at sitemap.xml, which
enumerates every project (/projekt/<Projektnummer>, ~4,300), person and
institution page. No export/CSV/API exists (ladder item 0 checked 2026-09-30).

Per project page (prerendered):
    Projektnummer, Zusammenfassung (summary; the prerender truncates it at
    ~750 chars with "..."; the full text needs an interactive click),
    Keywords, Status, Startdatum, Enddatum, Foerdersumme (EUR),
    Profilbereich, Foerderinitiative, Ausschreibung (call), Fachgebiet
    (subject, applications from 2025), Projektleitung (one or more project
    leads: person link, optional ORCID, institution).

The project TITLE is not in the detail-page prerender; it is on the project
cards of the list views. We take it from the project leads' /person/ pages
(each person page lists that person's project cards, title + number), which
are also prerendered.

Method 5 (static HTML) on the runbook ladder.

Output: s3://openalex-ingest/awards/volkswagen_foundation/volkswagen_foundation_projects.parquet
"""

import argparse
import html
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). See runbook §1.2.
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

BASE = "https://projektdatenbank.volkswagenstiftung.de"
SLUG = "volkswagen_foundation"
S3_BUCKET = "openalex-ingest"
S3_KEY = f"awards/{SLUG}/{SLUG}_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org; awards ingest)"}
WORKERS = 4
REQUEST_DELAY = 0.25  # per worker
RETRIES = 4

_session = requests.Session()
_session.headers.update(HEADERS)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, cache: Path | None = None) -> str:
    if cache is not None and cache.exists():
        return cache.read_text()
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = _session.get(url, timeout=60)
            if r.status_code == 404:
                return ""
            r.raise_for_status()
            r.encoding = "utf-8"
            page = r.text
            if cache is not None:
                cache.write_text(page)
            time.sleep(REQUEST_DELAY)
            return page
        except Exception as e:  # noqa: BLE001
            last_err = e
            log(f"  retry {attempt + 1}/{RETRIES} {url}: {e}")
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("​", "").replace("﻿", "")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def parse_date(s: str | None) -> str | None:
    m = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", s or "")
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else None


def parse_amount(s: str | None) -> float | None:
    # "2.182.150 €" / "94.400,50 €"
    if not s:
        return None
    digits = re.sub(r"[^\d,]", "", s).replace(",", ".")
    try:
        return float(digits) if digits else None
    except ValueError:
        return None


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|priv\.-doz|pd|sir|dame|mr|mrs|ms)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py) with a leading-honorific strip."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def sitemap_ids() -> list[str]:
    xml = get(f"{BASE}/sitemap.xml")
    ids = re.findall(r"<loc>https://projektdatenbank\.volkswagenstiftung\.de/projekt/([^<]+)</loc>", xml)
    return sorted(set(i.strip() for i in ids))


def parse_people(seg: str) -> list[dict]:
    people = []
    for block in re.split(r'<div class="mud-grid-item mb-\d">', seg)[1:]:
        pm = re.search(r'<a href="/person/([0-9a-f-]+)"[^>]*>(.*?)</a>', block, re.S)
        if not pm:
            continue
        orcid = re.search(r"orcid\.org/(\d{4}-\d{4}-\d{4}-\d{3}[\dX])", block)
        inst = re.search(r'<a href="/institution/([0-9a-f-]+)"[^>]*>(.*?)</a>', block, re.S)
        name = text(pm.group(2))
        given, family = split_name(name)
        people.append({
            "person_id": pm.group(1),
            "name": name,
            "given_name": given,
            "family_name": family,
            "orcid": orcid.group(1) if orcid else None,
            "institution": text(inst.group(2)) if inst else None,
            "institution_id": inst.group(1) if inst else None,
        })
    return people


def parse_project(pid: str, page: str) -> dict | None:
    m = re.search(r">Projektnummer</h3>\s*<p[^>]*>(.*?)</p>", page, re.S)
    if not m:
        return None
    number = text(m.group(1))
    summ = re.search(r">Zusammenfassung</h3>\s*<p[^>]*>(.*?)</p>", page, re.S)
    kw = re.search(r">Keywords</h3>\s*<p[^>]*>(.*?)</p>", page, re.S)
    info = {}
    for label, value in re.findall(
            r'<p class="mud-typography mud-typography-body2">([^<]+):</p>\s*(?:<a [^>]*>)?\s*<p[^>]*>(.*?)</p>',
            page, re.S):
        info[text(label)] = text(value)
    call_id = re.search(r'href="/ausschreibung/([^"]+)"', page)
    lead_seg = ""
    i = page.find(">Projektleitung</h3>")
    if i >= 0:
        j = page.find("<footer", i)
        lead_seg = page[i: j if j > 0 else None]
    people = parse_people(lead_seg)
    summary = text(summ.group(1)) if summ else None
    return {
        "project_number": number,
        "summary": summary,
        "summary_truncated": bool(summary and summary.endswith("...")),
        "keywords": text(kw.group(1)) if kw else None,
        "status": info.get("Status"),
        "start_date": parse_date(info.get("Startdatum")),
        "end_date": parse_date(info.get("Enddatum")),
        "amount_text": info.get("Fördersumme"),
        "amount": parse_amount(info.get("Fördersumme")),
        "currency": "EUR" if info.get("Fördersumme") else None,
        "profile_area": info.get("Profilbereich"),
        "funding_initiative": info.get("Förderinitiative"),
        "call": info.get("Ausschreibung"),
        "call_id": call_id.group(1) if call_id else None,
        "subject": info.get("Fachgebiet"),
        "people": people,
        "lead_name": people[0]["name"] if people else None,
        "lead_given_name": people[0]["given_name"] if people else None,
        "lead_family_name": people[0]["family_name"] if people else None,
        "lead_orcid": people[0]["orcid"] if people else None,
        "lead_institution": people[0]["institution"] if people else None,
        "landing_page_url": f"{BASE}/projekt/{pid}",
    }


CARD_RE = re.compile(r'<a href="/projekt/([^"]+)"[^>]*>\s*<h3[^>]*>(.*?)</h3>', re.S)


def main() -> None:
    p = argparse.ArgumentParser(description="VolkswagenStiftung Projektdatenbank -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    ids = sitemap_ids()
    log(f"Sitemap: {len(ids)} project URLs")
    if len(ids) < 1000 and not args.limit:
        raise SystemExit(f"sitemap lists only {len(ids)} projects; expected ~4,300 -- refusing")
    if args.limit:
        ids = ids[: args.limit]

    pcache = qcache = None
    if args.cache_dir:
        pcache = args.cache_dir / "projekt"
        qcache = args.cache_dir / "person"
        pcache.mkdir(parents=True, exist_ok=True)
        qcache.mkdir(parents=True, exist_ok=True)

    def fetch_project(pid: str):
        page = get(f"{BASE}/projekt/{pid}", pcache / f"{pid}.html" if pcache else None)
        return pid, (parse_project(pid, page) if page else None)

    rows, skipped = [], []
    t0 = time.time()
    with ThreadPoolExecutor(WORKERS) as ex:
        for n, (pid, rec) in enumerate(ex.map(fetch_project, ids), 1):
            if rec is None:
                skipped.append(pid)
            else:
                rows.append(rec)
            if n % 200 == 0:
                el = time.time() - t0
                log(f"  projects {n}/{len(ids)} ({len(rows)} parsed) ETA {el / n * (len(ids) - n) / 60:.1f} min")
    log(f"Parsed {len(rows)} project pages, {len(skipped)} skipped")

    # Titles from the project leads' person pages (project cards).
    titles: dict[str, str] = {}
    persons = sorted({pp["person_id"] for r in rows for pp in r["people"]})
    log(f"Fetching {len(persons)} person pages for project titles")

    def fetch_person(uuid: str):
        return get(f"{BASE}/person/{uuid}", qcache / f"{uuid}.html" if qcache else None)

    t0 = time.time()
    with ThreadPoolExecutor(WORKERS) as ex:
        for n, page in enumerate(ex.map(fetch_person, persons), 1):
            for pid, t in CARD_RE.findall(page or ""):
                if text(t):
                    titles.setdefault(pid, text(t))
            if n % 500 == 0:
                el = time.time() - t0
                log(f"  persons {n}/{len(persons)} ({len(titles)} titles) ETA {el / n * (len(persons) - n) / 60:.1f} min")

    for r in rows:
        r["title"] = titles.get(r["landing_page_url"].rsplit("/", 1)[-1])
    df = pd.DataFrame(rows)
    before = len(df)
    df = df.drop_duplicates(subset=["project_number"], keep="first")
    log(f"{before} rows, {before - len(df)} duplicate project numbers dropped")
    dupes = df["project_number"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate project_number: {df.loc[dupes, 'project_number'].tolist()}")
    for u in skipped[:20]:
        log(f"  skipped: {u}")
    for c in ["title", "summary", "start_date", "end_date", "amount", "funding_initiative",
              "lead_name", "lead_orcid", "lead_institution"]:
        log(f"  {c:20s} {df[c].notna().mean():6.1%}")
    log(f"  summary truncated   {df['summary_truncated'].mean():6.1%}")
    log(f"  total amount EUR {df['amount'].sum():,.0f}")

    df["people"] = df["people"].map(lambda v: v if v else [])
    people_col = df.pop("people")
    df = df.astype("string")
    df["people"] = people_col  # nested list<struct> kept as-is for the notebook

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / f"{SLUG}_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / f"_previous_{SLUG}_projects.parquet"
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
