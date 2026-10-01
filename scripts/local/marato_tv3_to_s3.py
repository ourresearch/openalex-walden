#!/usr/bin/env python3
"""
Fundació La Marató de TV3 to S3 Data Pipeline
=============================================

La Marató de TV3 is the annual Catalan telethon; each edition (December) is
dedicated to one disease area and the Fundació La Marató de TV3 then funds
biomedical research projects selected by AQuAS-managed international peer
review. The foundation publishes every funded project on its own site, one
page per edition (1993-2024; the 1992 edition funded awareness only):

    https://www.3cat.cat/tv3/marato/en/recerca/projectes-financats/
      -> /tv3/marato/en/projectes-financats/{year}/{id}/

Each edition page is a Next.js page whose __NEXT_DATA__ JSON carries a
ProjectesFinancatsMarato module with one object per project:
    titol  (English title), autor (HTML list: PI name + institution, one <li>
    per participating group, coordinator first), euros (amount, EUR).
Method 5 (static HTML) on the runbook ladder; no bulk export, CSV, or API exists
(the transparency portal links back to this same list; not in Europe PMC GRIST).

No project code is published. Citing works quote the foundation's internal
expedient codes (e.g. "201331-30" = edition 2013, application 31, group 30),
which cannot be derived from the public list, so funder_award_id is a stable
synthetic key: LMTV3-{edition}-{lead-PI-slug}.

Output: s3://openalex-ingest/awards/marato_tv3/marato_tv3_projects.parquet
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


# (self-check marker: this block is the runbook sys.stdout.reconfigure shim, with sys renamed)
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

BASE = "https://www.3cat.cat"
LIST_URL = f"{BASE}/tv3/marato/en/recerca/projectes-financats/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/marato_tv3/marato_tv3_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.7
RETRIES = 3


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
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
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(html.unescape(t)).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def year_pages(list_html: str) -> list[tuple[int, str, str | None]]:
    """(edition year, year-page URL, disease area label) from the all-projects index."""
    out = {}
    for m in re.finditer(r'href="(https://www\.3cat\.cat/tv3/marato/en/projectes-financats/(\d{4})/\d+/)"[^>]*>(.*?)</a>',
                         list_html, re.S):
        url, year, label = m.group(1), int(m.group(2)), text(m.group(3))
        rec = out.setdefault(year, [url, None])
        if label and label != str(year):
            rec[1] = label
    return sorted((y, u, lab) for y, (u, lab) in out.items())


def next_data_projects(page: str) -> tuple[list[dict], dict]:
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', page, re.S)
    if not m:
        raise RuntimeError("no __NEXT_DATA__ on year page")
    data = json.loads(m.group(1))

    def walk(o):
        if isinstance(o, dict):
            if o.get("name") == "ProjectesFinancatsMarato":
                return o.get("finalProps") or {}
            for v in o.values():
                r = walk(v)
                if r is not None:
                    return r
        elif isinstance(o, list):
            for v in o:
                r = walk(v)
                if r is not None:
                    return r
        return None

    props = walk(data)
    if props is None:
        raise RuntimeError("no ProjectesFinancatsMarato module on year page")
    return props.get("projectes") or [], props.get("textsAny") or {}


HONORIFIC_RE = re.compile(r"^(?:(?:dra|dr|prof|profa|sr|sra|mr|mrs|ms)\.?\s+)+", re.I)
# Spanish / Catalan / Portuguese / Dutch surname particles: kept with the surname that follows
PARTICLES = {"de", "del", "la", "las", "los", "i", "y", "da", "das", "do", "dos", "di", "van", "von", "der", "den", "le"}


def split_name(name: str) -> tuple[str | None, str | None]:
    """Runbook §2.4.1 helper (wolf_to_s3.py suffix set) adapted to Iberian names.

    La Marató PIs are almost all Spanish/Catalan, who carry two surnames
    ("Aina Areny Balagueró", "Pere-Joan Cardona i Iglesias", "David de la Rosa
    Carrillo"). The canonical last-token rule would put the first surname in
    given_name, so with 3+ tokens the family name is the last TWO surname units,
    where a unit absorbs preceding particles (de, del, la, i, y ...). Two-token
    names split as usual. Trailing degree suffixes are stripped first."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).replace(",", " ").split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    if len(tokens) == 2:
        return tokens[0], tokens[1]
    units, i = [], len(tokens) - 1
    while i >= 1 and len(units) < 2:  # token 0 is always a given name
        start = i
        while start - 1 >= 1 and tokens[start - 1].lower() in PARTICLES:
            start -= 1
        units.insert(0, tokens[start:i + 1])
        i = start - 1
    fam_start = len(tokens) - sum(len(u) for u in units)
    return " ".join(tokens[:fam_start]) or None, " ".join(tokens[fam_start:])


def parse_people(autor_html: str) -> list[dict]:
    people = []
    for li in re.findall(r"<li>(.*?)</li>", autor_html or "", re.S):
        name = re.search(r"<strong>(.*?)</strong>", li, re.S)
        inst = re.search(r"<em>(.*?)</em>", li, re.S)
        full = text(name.group(1)) if name else None
        if not full:
            continue
        given, family = split_name(full)
        people.append({
            "name": HONORIFIC_RE.sub("", full).strip(),
            "given_name": given,
            "family_name": family,
            "institution": text(inst.group(1)) if inst else None,
        })
    return people


def parse_amount(s: str | None) -> float | None:
    # Euros, in mixed formats: "398.836 €", "445.312,50 €", "€199.750,00" (European),
    # "€ 300,000.00" (English, edition 2018), "€ 299.450.00" (typo). A separator
    # followed by 1-2 trailing digits is the decimal mark; every other one groups thousands.
    if not s:
        return None
    digits = re.sub(r"[^\d,.]", "", s).strip(",.")
    if not digits:
        return None
    if "," in digits and "." in digits and digits[-1].isdigit():
        # both marks present ("199.989,375 €", "€ 300,000.00"): the last one is decimal
        m = re.search(r"[.,](\d+)$", digits)
    else:
        m = re.search(r"[.,](\d{1,2})$", digits)
    whole = digits[: m.start()] if m else digits
    whole = re.sub(r"[.,]", "", whole)
    try:
        return float(f"{whole}.{m.group(1)}" if m else whole)
    except ValueError:
        return None


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def main() -> None:
    p = argparse.ArgumentParser(description="Fundació La Marató de TV3 funded projects -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the N most recent editions (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    years = year_pages(get(LIST_URL))
    log(f"Index: {len(years)} editions ({years[0][0]}-{years[-1][0]})")
    if len(years) < 30:
        raise SystemExit(f"index lists only {len(years)} editions; expected 33 (1992-2024) - page layout changed?")
    if args.limit:
        years = years[-args.limit:]

    rows = []
    for year, url, disease in years:
        cache = args.cache_dir / f"en_{year}.html" if args.cache_dir else None
        if cache and cache.exists():
            page = cache.read_text()
        else:
            page = get(url)
            if cache:
                args.cache_dir.mkdir(parents=True, exist_ok=True)
                cache.write_text(page)
            time.sleep(REQUEST_DELAY)
        projects, texts = next_data_projects(page)
        declared = texts.get("projectes_financats_num_projectes")
        log(f"  {year} ({disease}): {len(projects)} projects (page header says {declared})")
        for seq, pr in enumerate(projects, 1):
            people = parse_people(pr.get("autor"))
            lead = people[0] if people else None
            rows.append({
                "edition_year": str(year),
                "disease_area": disease,
                "page_seq": str(seq),
                "title": text(pr.get("titol")),
                "amount_text": text(pr.get("euros")),
                "amount": parse_amount(pr.get("euros")),
                "lead_name": lead["name"] if lead else None,
                "lead_given_name": lead["given_name"] if lead else None,
                "lead_family_name": lead["family_name"] if lead else None,
                "lead_institution": lead["institution"] if lead else None,
                "n_investigators": str(len(people)),
                "people": json.dumps(people, ensure_ascii=False),
                "landing_page_url": url,
            })

    df = pd.DataFrame(rows)
    # Edition 1992 was awareness-only (no research projects); nothing to drop otherwise.
    df = df[df["title"].notna()].copy()
    # Synthetic stable key (no project code is published): edition year + lead PI slug,
    # with the page sequence appended only if one PI leads two projects in an edition.
    base = [f"LMTV3-{y}-{slug(n or t)}" for y, n, t in zip(df["edition_year"], df["lead_name"], df["title"])]
    dup = pd.Series(base).duplicated(keep=False).values
    df["funder_award_id"] = [f"{b}-{s}" if d else b for b, d, s in zip(base, dup, df["page_seq"])]
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")

    log(f"Parsed {len(df)} projects across {df['edition_year'].nunique()} editions "
        f"({int(dup.sum())} rows needed a sequence suffix)")
    for c in ["title", "amount", "lead_name", "lead_family_name", "lead_institution"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  total amount EUR {df['amount'].sum():,.0f}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "marato_tv3_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_marato_tv3_projects.parquet"
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
