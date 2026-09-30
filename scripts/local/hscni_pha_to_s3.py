#!/usr/bin/env python3
"""
Public Health Agency (Northern Ireland) — HSC R&D Division awards to S3
=======================================================================

The HSC Research & Development Division is the research-funding arm of the
Public Health Agency (Northern Ireland) (OpenAlex F4320320853, ROR 03ek62e72,
Crossref Funder 10.13039/501100001626). It publishes its funded awards on
research.hscni.net as one Drupal page per scheme, each with an HTML table
(title, chief investigator / fellow, dates/status, HSC R&D cost, and for some
schemes co-funders or an RRG theme). No bulk export or API exists (checked
2026-09-30). Method 5 (static HTML) on the runbook ladder; robots.txt asks for
Crawl-delay 10, which this script honours.

Scraped pages (see PAGES): Fellowship Awards, Enabling Research Awards,
Opportunity-Led Commissioned Research, Recognised Research Group awards,
US-Ireland R&D Partnership, PPI small grants, COVID-19 Rapid Response, GPARTS,
Cochrane Fellowships, NILS awards.

Not scraped: the "Project portfolio by year" page (titles only, no PI/cost),
the per-call commissioned pages (Bamford, Suicide Prevention, Investing in
Health, etc.; ~40 awards written as free prose in one cell, not table rows),
Workshops & Conferences support (event sponsorship) and the NIHR awards page
(NIHR-funded, not HSC R&D).

Output: s3://openalex-ingest/awards/hscni_pha/hscni_pha_projects.parquet
"""

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (sys.stdout.reconfigure + utf-8 file I/O defaults, under the _sys_utf8 alias)
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

import argparse
import hashlib
import html
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

BASE = "https://research.hscni.net"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/hscni_pha/hscni_pha_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
CRAWL_DELAY = 10  # robots.txt Crawl-delay: 10
RETRIES = 3

# Award-portfolio pages that publish one table row per award with a
# title + chief investigator/fellow + cost header. code -> (slug, scheme label)
PAGES = {
    "FEL": ("hsc-fellowship-awards", None),  # scheme comes from the SCHEME column
    "ERA": ("enabling-research-awards-0", "Enabling Research Award"),
    "OPP": ("opportunity-led-commissioned-research-awards", "Opportunity-Led Commissioned Research Award"),
    "RRG": ("recognised-research-group-rrg-awards-portfolio", "Recognised Research Group Award"),
    "USI": ("us-ireland-rd-partnership-awards-portfolio", "US-Ireland R&D Partnership Programme"),
    "PPI": ("ppi-research-support-small-grant-awards", "PPI in Research Support Small Grant"),
    "COV": ("covid-19-rapid-response-funding-call", "COVID-19 Rapid Response Funding Call"),
    "GPA": ("general-practice-academic-research-training-scheme-gparts-awards", "General Practice Academic Research Training Scheme (GPARTS)"),
    "COC": ("cochrane-fellowship-awards", "Cochrane Fellowship Award"),
    "NILS": ("ni-longitudinal-study-awards-portfolio", "NI Longitudinal Study (NILS) Award"),
}

HEADER_MAP = [  # (field, regex on lower-cased header text) - first match wins
    ("scheme", r"^scheme$|rrg theme"),
    ("title", r"title|award summary|^award$"),
    ("pi", r"investigator|fellow|^pi$"),
    ("status", r"^status"),
    ("dates", r"^dates"),
    ("cost", r"cost|funding"),
    ("co_funders", r"co-fund"),
]
STATUS_RE = re.compile(r"\b(Active|Complete|Completed|Research complete|Withdrawn|Terminated|Closed|Ongoing)\b", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(CRAWL_DELAY * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def tx(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


HONORIFIC_RE = re.compile(
    r"^(?:(?:assist\.?\s+prof|assoc\.?\s+prof|associate\s+professor|assistant\s+professor|professor|prof|dr|mr|mrs|ms|miss|mx|sir|dame)\.?\s+)+",
    re.I,
)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py), with a leading-honorific
    strip and UK post-nominals, as in twcf_to_s3.py."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                "frs", "frse", "fba", "fmedsci", "obe", "cbe", "mbe"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_pi(cell: str | None) -> dict:
    """'Ms Kirsty Jerrard (QUB/BHSCT)' -> name, affiliation. For multi-person
    cells (US-Ireland: NI lead first, then partners) keep the first person."""
    out = {"pi_raw": cell, "pi_name": None, "pi_affiliation": None}
    if not cell or cell.upper() in {"N/A", "TBC"}:
        return out
    m = re.match(r"^\s*([^()]+?)\s*\(([^()]*)\)", cell)
    name, aff = (m.group(1), m.group(2)) if m else (cell, None)
    name = re.split(r"\s+(?:and|&)\s+|,|;|/", name)[0].strip()
    has_title = bool(HONORIFIC_RE.match(name))
    words = re.split(r"\s+", name)
    # "BSO (Infrastructure Award)" / org acronyms are not people
    if not has_title and (len(words) < 2 or any(w.isupper() and len(w) > 1 for w in words)):
        return out
    out["pi_name"] = name
    out["pi_affiliation"] = aff.strip() if aff and aff.strip() else None
    return out


def yr(s: str) -> int | None:
    y = int(s)
    if y < 100:
        y += 2000 if y < 50 else 1900
    return y if 1980 <= y <= 2035 else None


def parse_dates(s: str | None) -> dict:
    out = {"start_date": None, "end_date": None, "start_year": None, "end_year": None}
    if not s:
        return out
    d = re.findall(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})", s)
    if d:
        full = []
        for dd, mm, yy in d:
            y = yr(yy)
            if y and 1 <= int(mm) <= 12 and 1 <= int(dd) <= 31:
                full.append(f"{y}-{int(mm):02d}-{int(dd):02d}")
        if full:
            out["start_date"] = full[0]
            out["start_year"] = int(full[0][:4])
            if len(full) > 1:
                out["end_date"] = full[1]
                out["end_year"] = int(full[1][:4])
        return out
    m = re.search(r"\b(\d{4}|\d{2})\s*[-–]\s*(\d{4}|\d{2})?\b", s)
    if m:
        out["start_year"] = yr(m.group(1))
        if m.group(2):
            e = yr(m.group(2))
            if e and out["start_year"] and e >= out["start_year"]:
                out["end_year"] = e
        return out
    m = re.search(r"\b(19|20)\d{2}\b", s)
    if m:
        out["start_year"] = int(m.group(0))
    return out


def gbp(s: str | None) -> float | None:
    if not s:
        return None
    m = re.search(r"£?\s*([\d,]+(?:\.\d+)?)\s*([kKmM](?![a-z]))?", s)
    if not m:
        return None
    try:
        v = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    mult = {"k": 1e3, "m": 1e6}.get((m.group(2) or "").lower(), 1)  # "£10K"
    v *= mult
    return v if v > 0 else None


def main_content(page: str) -> str:
    i = page.find('id="main-content"')
    e = page.find('id="footer', i)
    return page[i: e if e > 0 else None]


def parse_page(code: str, slug: str, scheme_label: str | None, page: str) -> list[dict]:
    m = main_content(page)
    rows = []
    pos, section_year = 0, None
    for tm in re.finditer(r"<table.*?</table>", m, re.S):
        # a bare year heading between tables ("2026", "2025") dates the next table
        between = tx(m[pos: tm.start()]) or ""
        ys = re.findall(r"\b(20\d{2}|19\d{2})\b", between)
        if ys and len(between) < 400:
            section_year = int(ys[-1])
        pos = tm.end()
        trs = re.findall(r"<tr.*?</tr>", tm.group(0), re.S)
        cols = None
        for tr in trs:
            cells = re.findall(r"<t([dh])[^>]*>(.*?)</t[dh]>", tr, re.S)
            texts = [tx(c) or "" for _, c in cells]
            low = [t.lower() for t in texts]
            # header row: short label cells only (a data row titled "Title-..." with
            # scheme "HSC Doctoral Fellowship" must not be mistaken for a header)
            is_header_like = all(len(t) <= 45 for t in texts)
            if is_header_like and any(re.search(r"title|award summary", t) for t in low) and any(
                    re.search(r"investigator|fellow|^pi$", t) for t in low):
                cols = {}
                for idx, h in enumerate(low):
                    for field, rx in HEADER_MAP:
                        if field not in cols and re.search(rx, h):
                            cols[field] = idx
                            break
                continue
            if cols is None:
                continue
            get_c = lambda f: texts[cols[f]] if f in cols and cols[f] < len(texts) else None  # noqa: E731
            title = get_c("title")
            if title:
                # trailing link text appended to titles on the COVID/OPP pages:
                # "... **Case Study**", "... Click here for Short Report**", "Evidence Brief , ..."
                cut = re.search(r"\s*(\*\*|click here|evidence brief|executive summary|final report|publications?\s*:|report\s*-)", title, re.I)
                if cut and cut.start() > 20:
                    title = title[: cut.start()]
                title = re.sub(r"\s+", " ", title.replace("*", " ")).strip(" ,") or None
            if not title:
                continue
            pi_cell, dates, cost = get_c("pi"), get_c("dates"), get_c("cost")
            status = get_c("status")
            if not pi_cell and not dates and not cost:
                # PPI page: a description row under the award row
                if rows and code == "PPI" and not rows[-1].get("description"):
                    rows[-1]["description"] = title
                continue
            # one GPARTS <tr> holds several awards jammed into single cells; the
            # rows can't be re-aligned reliably, so skip them (logged).
            if len(STATUS_RE.findall(status or "")) > 1 or re.match(r"^(TBC\s+){2,}", title):
                log(f"  [{code}] skipped merged multi-award row: {title[:60]}")
                continue
            if status is None and dates and STATUS_RE.search(dates):  # "2024- 2027 Active"
                status = STATUS_RE.search(dates).group(0)
            title_link = None
            if "title" in cols and cols["title"] < len(cells):
                a = re.search(r'href="([^"]+)"', cells[cols["title"]][1])
                title_link = a.group(1) if a else None
            rec = {
                "page_code": code,
                "page_slug": slug,
                "scheme": get_c("scheme") or scheme_label,
                "title": title,
                "status": status,
                "dates_text": dates,
                "section_year": section_year,
                "cost_text": cost,
                "amount": gbp(cost),
                "co_funders": get_c("co_funders"),
                "title_link": title_link,
                "description": None,
                "landing_page_url": f"{BASE}/{slug}",
            }
            rec.update(parse_pi(pi_cell))
            rec.update(parse_dates(dates))
            if rec["start_year"] is None and section_year:
                rec["start_year"] = section_year
            g, f = split_name(rec["pi_name"]) if rec["pi_name"] else (None, None)
            rec["lead_given_name"], rec["lead_family_name"] = g, f
            rows.append(rec)
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description="HSC R&D Division (Public Health Agency NI) award portfolios -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only fetch the first N portfolio pages")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    items = list(PAGES.items())[: args.limit] if args.limit else list(PAGES.items())
    rows = []
    for code, (slug, label) in items:
        cache = args.cache_dir / f"{slug}.html" if args.cache_dir else None
        if cache and cache.exists():
            page = cache.read_text()
        else:
            page = get(f"{BASE}/{slug}")
            if cache:
                args.cache_dir.mkdir(parents=True, exist_ok=True)
                cache.write_text(page)
            time.sleep(CRAWL_DELAY)
        got = parse_page(code, slug, label, page)
        if not got:
            raise SystemExit(f"{slug}: no award rows parsed - page layout changed?")
        log(f"{code:4s} {slug}: {len(got)} awards")
        rows += got

    df = pd.DataFrame(rows)
    df["_k"] = (df["page_code"] + "|" + df["title"].str.lower().str.replace(r"\W+", " ", regex=True).str.strip()
                + "|" + df["lead_family_name"].fillna("").str.lower())
    before = len(df)
    df = df.drop_duplicates(subset=["_k"], keep="first")
    if before != len(df):
        log(f"Dropped {before - len(df)} exact duplicate rows (same page, title and PI)")
    # No citable award reference is published on these pages (the HSC R&D
    # references researchers cite, e.g. COM/5596/20, EAT/4735/12, appear only
    # in a few PDF file names), so the id is synthetic and stable:
    # HSCRD-{page code}-{sha1(page|title|PI family)[:10]}.
    df["funder_award_id"] = [
        f"HSCRD-{c}-{hashlib.sha1(k.encode('utf-8')).hexdigest()[:10]}" for c, k in zip(df["page_code"], df["_k"])
    ]
    df = df.drop(columns="_k")
    if df["funder_award_id"].str.lower().duplicated().any():
        raise SystemExit("duplicate funder_award_id")
    df["currency"] = "GBP"
    for c in ["title", "pi_name", "pi_affiliation", "start_year", "end_year", "amount", "status", "description"]:
        log(f"  {c:16s} {df[c].notna().mean():6.1%}")
    log(f"  total amount GBP {df['amount'].sum():,.0f} over {len(df)} awards")

    for c in ["start_year", "end_year", "section_year"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")  # "2024", not "2024.0"
    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "hscni_pha_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_hscni_pha_projects.parquet"
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
