#!/usr/bin/env python3
"""
Templeton World Charity Foundation (TWCF) to S3 Data Pipeline
=============================================================

TWCF publishes every funded project on its Craft CMS site at
https://www.templetonworldcharity.org/projects-resources/project-database/<slug>.
The full project list is enumerated by the Craft sitemap
(sitemaps-2-section-projects-2-sitemap-p{1,2}.xml, ~684 projects), and each
project page carries a clean <dl class="project-details"> block:

    TWCF Number, Project Duration, Core Funding Area, Priority (initiative),
    Region, Amount Awarded (USD), Grant DOI (10.54224/<n>, Crossref-registered)

plus Director / coDirector blocks (name, institution, optional ORCID) and a
free-text description. Method 5 (static HTML) on the runbook ladder.

Most TWCF grants with a DOI are already in OpenAlex via Crossref Awards
(provenance crossref_work). This ingest adds the projects Crossref is missing
and makes the funder's own site the authoritative source.

Output: s3://openalex-ingest/awards/twcf/twcf_projects.parquet
"""

import argparse
import html
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

BASE = "https://www.templetonworldcharity.org"
SITEMAPS = [f"{BASE}/sitemaps-2-section-projects-2-sitemap-p{i}.xml" for i in (1, 2)]
FUNDER_DOI = "10.13039/501100011730"  # TWCF, OpenAlex F4320327239
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/twcf/twcf_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5
RETRIES = 3

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], 1)}


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
    urls = []
    for sm in SITEMAPS:
        urls += re.findall(r"<loc>([^<]+)</loc>", get(sm))
    return sorted({u.strip() for u in urls if "/project-database/" in u})


def number_key(s: str) -> str:
    """Core 5-digit project number, the same as the grant DOI suffix:
    "2021-20707" / "TWCF-2021-20707" / "TWCF0314" / "0646" / "34009-2"
    -> "20707" / "20707" / "20314" / "20646" / "34009".
    Older projects show 4 digits on the site with the leading "2" dropped
    (#0646 <-> 10.54224/20646; holds for 166 of 167 such pages)."""
    s = re.sub(r"^TWCF-?", "", s.strip(), flags=re.I)
    s = re.sub(r"^(19|20)\d{2}-", "", s)
    s = re.sub(r"-\d+$", "", s)
    return "2" + s if len(s) == 4 and s.isdigit() else s


def crossref_awards() -> dict[str, tuple[str, str]]:
    """number_key -> (award number, grant DOI) from TWCF's own Crossref grant
    deposits (prefix 10.54224). Crossref's award number is "TWCF-{year}-{n}",
    but newer project pages show only "{n}", so the year can't be rebuilt from
    the site. Reusing TWCF's deposited string makes our rows hash to the same
    award id as the existing crossref_work rows, so this ingest upgrades them
    in CreateAwards dedup instead of duplicating them.

    Keyed on the project number, not the DOI: the site links at least one
    project to another project's DOI (#0264 -> 10.54224/30264), and one
    deposit carries another project's award number (DOI 30294 ->
    TWCF-2022-30066). Deposits whose award number names a different project
    than their own DOI are dropped."""
    out, cursor = {}, "*"
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
            award, doi = (item.get("award") or "").strip(), item["DOI"].lower()
            if not award:
                continue
            key = number_key(award)
            suffix = doi.rsplit("/", 1)[-1]
            # a 5-digit suffix naming a different project is a bad deposit;
            # malformed suffixes (10.54224/200567 for TWCF-2020-20567) are typos
            if re.fullmatch(r"\d{5}", suffix) and suffix != key:
                log(f"  Crossref deposit {doi} carries award {award}; ignored")
                continue
            out[key] = (award, doi)
        if not msg["items"]:
            return out
        cursor = msg["next-cursor"]


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("​", "").replace("﻿", "")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def parse_date(s: str | None) -> str | None:
    # "September 1 / 2026"
    if not s:
        return None
    m = re.search(r"([A-Za-z]+)\s+(\d{1,2})\s*/\s*(\d{4})", s)
    if not m or m.group(1).lower() not in MONTHS:
        return None
    return f"{m.group(3)}-{MONTHS[m.group(1).lower()]:02d}-{int(m.group(2)):02d}"


HONORIFIC_RE = re.compile(r"^(?:(?:rev|revd|pr|fr|dr|prof|professor|sir|dame|mr|mrs|ms)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py), plus UK post-nominals and a leading
    honorific strip for names like "Rev. Pr. Thierry Magnin"."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                "frs", "frse", "fba", "fmedsci", "obe", "cbe", "mbe"}  # UK post-nominals ("Simon Conway Morris FRS")
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_people(page: str) -> list[dict]:
    """Director / coDirector blocks: eyebrow role, collaborator name,
    institution, and an optional ORCID link inside the same block."""
    people = []
    for b in re.split(r'<div class="eyebrow">', page)[1:]:
        role = text(b[: b.find("</div>")])
        if role is None or "director" not in role.lower():
            continue
        name = re.search(r'class="description collaborator">(.*?)</div>', b, re.S)
        if not name:
            continue
        inst = re.search(r'class="institution"><strong>Institution</strong>(.*?)</div>', b, re.S)
        orcid = re.search(r'orcid\.org/(\d{4}-\d{4}-\d{4}-\d{3}[\dX])', b)
        given, family = split_name(text(name.group(1)))
        people.append({
            "role": role,
            "name": text(name.group(1)),
            "given_name": given,
            "family_name": family,
            "institution": text(inst.group(1)) if inst else None,
            "orcid": orcid.group(1) if orcid else None,
        })
    return people


def parse_project(url: str, page: str) -> dict | None:
    dl = re.search(r'<dl class="project-details">(.*?)</dl>', page, re.S)
    if not dl:
        return None
    fields = {text(k): v for k, v in re.findall(r"<dt>(.*?)</dt>\s*<dd>(.*?)</dd>", dl.group(1), re.S)}
    title = re.search(r'mod-projectDetails.*?<div class="lead anim">(.*?)</div>', page, re.S)
    dparts = re.findall(r'<div class="text-nowrap">(.*?)</div>', fields.get("Project Duration") or "", re.S)
    amount_txt = text(fields.get("Amount Awarded"))
    digits = re.sub(r"[^\d.]", "", amount_txt or "")
    amount = float(digits) if digits else None
    doi_html = fields.get("Grant DOI*") or fields.get("Grant DOI") or ""
    doi = re.search(r"doi\.org/(10\.\d+/[^\"<\s]+)", doi_html)
    rest = page[dl.end():]
    note = re.search(r'<div class="note">(.*?)</div>', rest, re.S)
    # people blocks sit between the details list and the description
    people = parse_people(rest[: note.start()] if note else rest)
    lead = next((p for p in people if p["role"].lower() == "director"), people[0] if people else None)
    return {
        "twcf_number": text(fields.get("TWCF Number")),
        "title": text(title.group(1)) if title else None,
        "start_date": parse_date(text(dparts[0])) if dparts else None,
        "end_date": parse_date(text(dparts[1])) if len(dparts) > 1 else None,
        "core_funding_area": text(fields.get("Core Funding Area")),
        "priority": text(fields.get("Priority")),
        "region": text(fields.get("Region")),
        "amount_text": amount_txt,
        "amount": amount,
        "currency": "USD" if amount is not None else None,
        "grant_doi": doi.group(1).lower() if doi else None,
        "lead_name": lead["name"] if lead else None,
        "lead_given_name": lead["given_name"] if lead else None,
        "lead_family_name": lead["family_name"] if lead else None,
        "lead_institution": lead["institution"] if lead else None,
        "lead_orcid": lead["orcid"] if lead else None,
        "people": people,
        "description": text(note.group(1)) if note else None,
        "landing_page_url": url,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="TWCF project database -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    urls = project_urls()
    log(f"Sitemap: {len(urls)} project URLs")
    if args.limit:
        urls = urls[: args.limit]

    rows, skipped = [], []
    for i, url in enumerate(urls, 1):
        cache = None
        if args.cache_dir:
            args.cache_dir.mkdir(parents=True, exist_ok=True)
            # slug truncated: some slugs are full project titles and blow Windows MAX_PATH
            cache = args.cache_dir / (url.rstrip("/").rsplit("/", 1)[-1][:80] + ".html")
        if cache and cache.exists():
            page = cache.read_text()
        else:
            page = get(url)
            if cache:
                cache.write_text(page)
            time.sleep(REQUEST_DELAY)
        rec = parse_project(url, page) if page else None
        if rec is None or not rec["twcf_number"]:
            skipped.append(url)
        else:
            rows.append(rec)
        if i % 50 == 0:
            log(f"  {i}/{len(urls)} fetched, {len(rows)} parsed")

    df = pd.DataFrame(rows)
    before = len(df)
    df = df.drop_duplicates(subset=["twcf_number"], keep="first")
    log(f"Parsed {before} projects ({before - len(df)} duplicate TWCF numbers dropped), {len(skipped)} pages skipped")

    xref = crossref_awards()
    log(f"Crossref: {len(xref)} usable TWCF grant deposits")
    keys = df["twcf_number"].map(number_key)
    matched = keys.map(xref)
    site_doi = df["grant_doi"]
    # No Crossref deposit: old 4-digit projects use "TWCF0646", the form most
    # citing works write in acknowledgements (94 of 285 crossref_work_funders
    # rows), so these also upgrade the existing priority-0 citation shells.
    df["funder_award_id"] = [
        m[0] if isinstance(m, tuple) else (f"TWCF{n}" if re.fullmatch(r"\d{4}", n) else f"TWCF-{n}")
        for m, n in zip(matched, df["twcf_number"])
    ]
    df["grant_doi"] = [
        m[1] if isinstance(m, tuple) else (d if d and d.rsplit("/", 1)[-1] == k else None)
        for m, d, k in zip(matched, site_doi, keys)
    ]
    wrong = site_doi.notna() & (site_doi != df["grant_doi"])
    for n, d in zip(df.loc[wrong, "twcf_number"], site_doi[wrong]):
        log(f"  site links #{n} to {d}, which is another project's DOI; dropped")
    log(f"  matched to Crossref: {matched.notna().sum()}, site-only: {matched.isna().sum()}, "
        f"Crossref grants not on site: {len(set(xref) - set(keys))}")
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    for u in skipped[:20]:
        log(f"  skipped: {u}")
    for c in ["title", "start_date", "end_date", "amount", "grant_doi",
              "lead_name", "lead_institution", "lead_orcid", "description"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  total amount USD {df['amount'].sum():,.0f}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "twcf_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_twcf_projects.parquet"
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
