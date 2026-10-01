#!/usr/bin/env python3
"""
European Foundation for the Study of Diabetes (EFSD) to S3 Data Pipeline
========================================================================

EFSD (Duesseldorf; the foundation of EASD; OpenAlex F4320320870) publishes the
recipients of each of its grant and fellowship programmes at
https://www.europeandiabetesfoundation.org/recipients/ : one static HTML page
per programme, listing each award year and, per recipient, name, institution -
city/country and project title (for the mentorship programme: the mentor).

Most EFSD programmes are co-funded with an industry or foundation partner
(Lilly, Novo Nordisk / Novo Nordisk Foundation, Boehringer Ingelheim, MSD,
Sanofi, AstraZeneca, JDRF, JDS, EUDF). EFSD runs the call, review and award,
so every award ships under EFSD, with the programme name (which names the
partner) as funder_scheme and the partner in its own column.

Not published: amounts, start/end dates (only the award year) and EFSD's
internal grant numbers, so funder_award_id is the synthetic
"EFSD-<programme>-<year>-<recipient>" key.

Scope: all programmes on the recipients page, including the training ones
(Rising Star fellowships, Future Leaders Mentorship, JDS reciprocal travel
fellowships). Nothing filtered.

Output: s3://openalex-ingest/awards/efsd/efsd_projects.parquet
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

BASE = "https://www.europeandiabetesfoundation.org"
INDEX = f"{BASE}/recipients/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/efsd/efsd_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5
RETRIES = 3

PARTNERS = [  # order matters: "Novo Nordisk Foundation" before "Novo Nordisk"
    ("Boehringer Ingelheim", "Boehringer Ingelheim"), ("JDRF", "JDRF"), ("Lilly", "Lilly"),
    ("Novo Nordisk Foundation", "Novo Nordisk Foundation"), ("Novo Nordisk", "Novo Nordisk"),
    ("MSD", "MSD"), ("Sanofi", "Sanofi"), ("AstraZeneca", "AstraZeneca"), ("JDS", "Japan Diabetes Society"),
    ("EUDF", "EUDF"),
]
# source typos / variants -> one spelling
COUNTRY_FIX = {"UK": "United Kingdom", "The Netherlands": "Netherlands", "Schweden": "Sweden",
               "Fance": "France", "Spai": "Spain"}
TRAINING = ("mentorship",)
FELLOWSHIP = ("fellowship", "rising star", "young investigator", "future leaders award")


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code == 404:
                return ""
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def cached_get(url: str, cache_dir: Path | None) -> str:
    cache = None
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache = cache_dir / (url.rstrip("/").rsplit("/", 1)[-1][:100] + ".html")
        if cache.exists():
            return cache.read_text()
    page = get(url)
    if cache and page:
        cache.write_text(page)
    time.sleep(REQUEST_DELAY)
    return page


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
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


PARTICLES = {"van", "von", "de", "der", "den", "da", "di", "del", "le", "la", "du", "ten", "ter"}


def split_person(name: str) -> tuple[str | None, str | None]:
    """split_name, keeping a lowercase particle with the family name
    ("Marleen van Greevenbroek" -> "Marleen" / "van Greevenbroek")."""
    given, family = split_name(name)
    parts = (given or "").split()
    while len(parts) > 1 and parts[-1] in PARTICLES:
        family = parts.pop() + " " + family
    return (" ".join(parts) or None) if given else None, family


def slugify(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def parse_index(page: str) -> list[tuple[str, str]]:
    main = page[page.find("<main"): page.find("</main>")]
    out = []
    for href, label in re.findall(r'<a[^>]+href="(recipients/[a-z0-9-]+/)"[^>]*>(.*?)</a>', main, re.S):
        if href.rstrip("/").endswith(("reports", "research-impact")):
            continue
        out.append((f"{BASE}/{href}", text(label)))
    return list(dict.fromkeys(out))


def parse_entry(chunk: str) -> dict | None:
    """'<b>Name:</b> Inst - City, Country<br>Project title' (variants: colon
    outside the <b>, entries separated by <br><br> instead of </p><p>)."""
    m = re.match(r"\s*<b>(.*?)</b>(.*)", chunk, re.S)
    if not m:
        return None
    name = (text(m.group(1)) or "").strip(" :")
    lines = [text(x) for x in re.split(r"<br\s*/?>|</p>\s*<p[^>]*>", m.group(2))]
    lines = [x.strip(" :") for x in lines if x and x.strip(" :")]
    if not name or not lines:
        return None
    # normalise "Inst – Country", "Inst -City, Country" to the " - " separator
    inst_line = re.sub(r"\s+[–—-]\s*|\s*[–—]\s+|\s+-(?=[A-Z])", " - ", lines[0])
    title = " ".join(lines[1:]) or None
    inst, _, place = inst_line.rpartition(" - ") if " - " in inst_line else (inst_line, "", "")
    country = place.rsplit(",", 1)[-1].strip() if place else None
    if not place and "," in inst_line:  # "Steno Diabetes Center Copenhagen, Denmark"
        inst, country = inst_line.rsplit(",", 1)[0], inst_line.rsplit(",", 1)[1].strip()
    country = COUNTRY_FIX.get(country, country) if country else None
    mentor = None
    if title and title.lower().startswith("mentor:"):
        mentor, title = title.split(":", 1)[1].strip(), None
    given, family = split_person(name)
    return {"name": name, "given_name": given, "family_name": family,
            "institution": inst.strip() or None, "location": place.strip() or None,
            "country": country, "title": title, "mentor": mentor}


def parse_programme(page: str) -> list[dict]:
    main = page[page.find("<main"): page.find("</main>")]
    out = []
    # each year: a 25%-width column holding <b>YYYY</b>, then a 75%-width column of entries
    for blk in re.split(r'<div class="layout-column layout-column-width-25 col-md-3">', main)[1:]:
        y = re.search(r"<b>\s*((?:19|20)\d{2})\s*</b>", blk)
        if not y:
            continue
        right = blk.split('<div class="layout-column layout-column-width-75 col-md-9">', 1)
        if len(right) < 2:
            continue
        body = right[1]
        for chunk in re.split(r"(?=<b>)", body)[1:]:
            e = parse_entry(chunk)
            if e:
                out.append({**e, "year": int(y.group(1))})
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="EFSD recipients -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the first N programmes (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    progs = parse_index(get(INDEX))
    log(f"Recipients index: {len(progs)} programmes")
    if len(progs) < 15:
        raise SystemExit(f"only {len(progs)} programme links; page layout changed?")
    if args.limit:
        progs = progs[: args.limit]

    rows = []
    for url, label in progs:
        entries = parse_programme(cached_get(url, args.cache_dir))
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        partners = [name for key, name in PARTNERS if re.search(rf"\b{re.escape(key)}\b", label)]
        if "Novo Nordisk Foundation" in partners and "Novo Nordisk" in partners:
            partners.remove("Novo Nordisk")
        low = label.lower()
        ftype = "training" if any(k in low for k in TRAINING) else (
            "fellowship" if any(k in low for k in FELLOWSHIP) else "research")
        log(f"  {len(entries):3d} awards  {label}")
        if not entries:
            raise SystemExit(f"no recipients parsed from {url}")
        for e in entries:
            rows.append({
                "programme": label,
                "programme_slug": slug,
                "partner": "; ".join(partners) or None,
                "funding_type": ftype,
                "award_year": e["year"],
                "funder_award_id": f"EFSD-{slug.removeprefix('efsd-').removeprefix('efsd')[:40].strip('-')}-{e['year']}-{slugify(e['name'])}",
                "title": e["title"],
                "mentor": e["mentor"],
                "name": e["name"],
                "given_name": e["given_name"],
                "family_name": e["family_name"],
                "institution": e["institution"],
                "location": e["location"],
                "country": e["country"],
                "landing_page_url": url,
            })

    df = pd.DataFrame(rows)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    log(f"{len(df)} awards {df['award_year'].min()}-{df['award_year'].max()}; "
        f"types {df['funding_type'].value_counts().to_dict()}")
    for c in ["title", "family_name", "institution", "country", "partner"]:
        log(f"  {c:12s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "efsd_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_efsd_projects.parquet"
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
