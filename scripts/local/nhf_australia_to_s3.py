#!/usr/bin/env python3
"""
National Heart Foundation of Australia to S3 Data Pipeline
==========================================================

The Heart Foundation (OpenAlex F4320320442, AU) publishes one "Research Grant
Recipients {year}" page per funding round on its own site:

    https://www.heartfoundation.org.au/research/research-award-recipients-{year}

(linked from https://www.heartfoundation.org.au/research/grant-recipients).
Pages exist for the 2018-2025 rounds; earlier rounds are not online. Each page
is a set of accordions, one per scheme (Future Leader Fellowship levels,
Postdoctoral Fellowship, PhD / Postgraduate Scholarship, Vanguard Grant,
First Nations / Aboriginal and Torres Strait Islander grant, Collaboration and
Exchange, ...), each holding a recipient table. Method 5 (static HTML), except
the 2024 page, whose tables are Canva embeds; the Canva view page ships the
table cells as JSON, parsed here.

Columns vary by year:
  2018-2019  Institution, First/Last Name, Funded Amount (AUD), Scientific
             Title, Duration (Years), State
  2020-2021  Institution, Name, Project Title, Project length, State
  2022-2025  Award/Grant ID, Researcher, Administering Institute, State,
             Research Category, Project Title
so amounts exist only for 2018-2019 and grant numbers only for 2022-2025.

Excluded (not research money / not new awards): the "Innovation Awards" /
named-award accordions (Shirley E Freeman Award, Paul Korner Innovation Award,
Ross Hohnen Award), which are honours given to fellows already listed above,
and "(Honorary Future Leader Fellowship)" rows (2021; unfunded).

``funder_award_id`` (runbook §2.1.1): citing works carry the bare 6-digit
Heart Foundation application/award number (``106654``, ``102068``; Crossref
funder metadata on F4320320442), which is the numeric part of the page's
Grant ID (``110333-2025_FLF`` -> ``110333``). Rows without a published number
(2018-2021) get a synthetic ``NHFA-{year}-{sha1(scheme|name|title)[:10]}``.

Output: s3://openalex-ingest/awards/nhf_australia/nhf_australia_projects.parquet
"""

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

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (renamed-sys variant; equivalent to sys.stdout.reconfigure(...) — runbook §1.2)
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). No-op on
# Linux/Databricks. See runbook §1.2.
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

BASE = "https://www.heartfoundation.org.au"
HUB = f"{BASE}/research/grant-recipients"
YEAR_URL = BASE + "/research/research-award-recipients-{year}"
FIRST_YEAR = 2010
SLUG = "nhf_australia"
S3_BUCKET = "openalex-ingest"
S3_KEY = f"awards/{SLUG}/{SLUG}_projects.parquet"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 openalex-walden/1.0"}
REQUEST_DELAY = 0.5
RETRIES = 4

EXCLUDE_SECTION = re.compile(r"innovation award|freeman award|korner|hohnen|award for research excellence", re.I)
STATES = {"nsw": "New South Wales", "vic": "Victoria", "qld": "Queensland", "wa": "Western Australia",
          "sa": "South Australia", "tas": "Tasmania", "act": "Australian Capital Territory",
          "nt": "Northern Territory"}
ID_SUFFIX_SCHEME = {"FLF": "Future Leader Fellowship", "PDF": "Postdoctoral Fellowship",
                    "PGS": "Postgraduate Scholarship", "FNC": "First Nations CVD Grant",
                    "VG": "Vanguard Grant", "C&E": "Collaboration and Exchange Grant"}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> requests.Response:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            log(f"  GET {url[-75:]} -> {r.status_code} ({len(r.content)} bytes)")
            if r.status_code == 404:
                return r
            r.raise_for_status()
            r.encoding = "utf-8"
            return r
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after {RETRIES} tries: {last_err}")


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<br\s*/?>", " ", fragment)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|assoc|associate|a/prof|a/professor|mr|mrs|ms|miss|sir|dame)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py) + honorific strip."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                "am", "ao", "ac", "facc", "fracp"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def norm_header(h: str) -> str:
    h = (h or "").lower()
    if "id" in h.split() or h.startswith("award id") or h.startswith("grant id"):
        return "grant_id"
    if "first" in h or h.startswith("fulle"):  # 2019 typo "Fulle Name"
        return "first_name"
    if "last" in h:
        return "last_name"
    if h in {"researcher", "awardee", "name"}:
        return "researcher"
    if "institut" in h:
        return "institution"
    if "amount" in h:
        return "amount"
    if "title" in h:
        return "title"
    if "duration" in h or "duartion" in h or "length" in h:
        return "duration"
    if "state" in h:
        return "state"
    if "category" in h:
        return "research_category"
    return re.sub(r"\W+", "_", h).strip("_")


def html_table_rows(table: str) -> list[list[str | None]]:
    rows = []
    for tr in re.findall(r"<tr.*?</tr>", table, re.S):
        rows.append([text(c) for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", tr, re.S)])
    return rows


CANVA_CELL = re.compile(r'"([A-Z])(\d+)":\{"A":\{"C":"#[0-9a-fA-F]+"\},"B":\{"A\?":"A","A":\{"C":\{"A":(\[(?:"(?:[^"\\]|\\.)*",?)*\])')


def canva_rows(page: str) -> list[list[str | None]]:
    """Canva 'view?embed' pages carry the design's table cells as JSON keyed
    like spreadsheet refs (A1, B1, ...), text in escaped string arrays."""
    cells = {}
    for col, row, arr in CANVA_CELL.findall(page):
        t = " ".join(re.findall(r'"((?:[^"\\]|\\.)*)"', arr))
        t = re.sub(r"\\+[nt]", " ", t)
        t = re.sub(r"\\+u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), t)
        t = re.sub(r"\s+", " ", t).strip()
        cells.setdefault((int(row), col), t or None)
    rows = sorted({r for r, _ in cells})
    cols = sorted({c for _, c in cells})
    return [[cells.get((r, c)) for c in cols] for r in rows]


TOKEN = re.compile(
    r'MuiAccordionSummary-content[^>]*>\s*<h\d[^>]*>(?P<acc>.*?)</h\d>'
    r'|(?P<table><table.*?</table>)'
    r'|(?P<canva>https://www\.canva\.com/design/[^"\'\\\s<>]+?/view\?embed)',
    re.S)


def parse_year(year: int, page: str, cache_dir: Path | None) -> list[dict]:
    body = page.split('<script id="__NEXT_DATA__"', 1)[0]
    # the first (expanded) accordion has emotion <style> blocks between its
    # summary div and its <h6> title; strip all inline styles first
    body = re.sub(r"<style[^>]*>.*?</style>", "", body, flags=re.S)
    url = YEAR_URL.format(year=year)
    section, rows, seen_canva = None, [], set()
    for m in TOKEN.finditer(body):
        if m.group("acc") is not None:
            section = text(m.group("acc"))
            continue
        if m.group("canva"):
            curl = html.unescape(m.group("canva"))
            if curl in seen_canva:
                continue
            seen_canva.add(curl)
            cache = cache_dir / f"canva_{hashlib.sha1(curl.encode()).hexdigest()[:12]}.html" if cache_dir else None
            if cache and cache.exists():
                cpage = cache.read_text()
            else:
                cpage = get(curl).text
                if cache:
                    cache_dir.mkdir(parents=True, exist_ok=True)
                    cache.write_text(cpage)
                time.sleep(REQUEST_DELAY)
            table = canva_rows(cpage)
            source = "canva"
        else:
            table = html_table_rows(m.group("table"))
            source = "html"
        if not table or len(table) < 2:
            continue
        if section and EXCLUDE_SECTION.search(section):
            log(f"  {year} skip section '{section}' ({len(table) - 1} rows; honours, not grants)")
            continue
        header = [norm_header(h or "") for h in table[0]]
        for cells in table[1:]:
            rec = {h: c for h, c in zip(header, cells) if h}
            if not any(rec.values()):
                continue
            rows.append({"award_year": str(year), "scheme_section": section, "table_source": source,
                         "landing_page_url": url, **rec})
    return rows


def scheme_name(section: str | None, grant_id: str | None, year: str) -> str | None:
    s = section or ""
    s = re.sub(r"^(explore\s+)|(\s+funded)?\s+projects$", "", s.strip(), flags=re.I)
    s = re.sub(rf"^{year}\s+", "", s).strip()
    m = re.search(r"_([A-Z&]+)\s*$", grant_id or "")
    if m and m.group(1) in ID_SUFFIX_SCHEME and not s:
        s = ID_SUFFIX_SCHEME[m.group(1)]
    if re.fullmatch(r"level \d", s, re.I):
        s = f"Future Leader Fellowship - {s.title()}"
    s = re.sub(r"\s+", " ", s.replace("–", "-").replace("&", "and")).strip()
    s = re.sub(r"(Grant|Award)s$", r"\1", s)  # "Vanguard Grants" -> "Vanguard Grant"
    return s or None


def funding_type(scheme: str | None) -> str:
    s = (scheme or "").lower()
    if "fellowship" in s:
        return "fellowship"
    if "scholarship" in s or "phd" in s:
        return "training"
    return "research"


def build(rows: list[dict]) -> pd.DataFrame:
    out = []
    for r in rows:
        name = r.get("researcher")
        if r.get("last_name"):
            given, family = r.get("first_name"), r.get("last_name")
            name = " ".join(x for x in [given, family] if x)
        else:
            given, family = split_name(name or "")
        title = r.get("title")
        # 2021 titles carry "(Honorary Future Leader Fellowship)" etc.
        honorary = bool(title and re.search(r"\(honorary[^)]*\)", title, re.I))
        if honorary:  # honorary fellowships carry no Heart Foundation money
            log(f"  skip honorary award: {name} ({r['award_year']})")
            continue
        gid = r.get("grant_id")
        num = re.match(r"\s*(\d{5,7})", gid or "")
        scheme = scheme_name(r.get("scheme_section"), gid, r["award_year"])
        if num:
            fa_id = num.group(1)
        else:
            key = "|".join([scheme or "", (name or "").lower(), (title or "").lower()])
            fa_id = f"NHFA-{r['award_year']}-{hashlib.sha1(key.encode()).hexdigest()[:10]}"
        amount = None
        if r.get("amount"):
            digits = re.sub(r"[^\d.]", "", r["amount"])
            amount = float(digits) if digits else None
        state = r.get("state")
        out.append({
            "funder_award_id": fa_id,
            "grant_id_published": gid,
            "award_year": r["award_year"],
            "scheme": scheme,
            "scheme_section": r.get("scheme_section"),
            "funding_type": funding_type(scheme),
            "title": title,
            "honorary": "true" if honorary else "false",
            "researcher_name": name,
            "lead_given_name": given,
            "lead_family_name": family,
            "lead_institution": r.get("institution"),
            "state": STATES.get((state or "").lower(), state),
            "research_category": r.get("research_category"),
            "duration": r.get("duration"),
            "amount": amount,
            "currency": "AUD" if amount is not None else None,
            "landing_page_url": r["landing_page_url"],
            "table_source": r["table_source"],
        })
    df = pd.DataFrame(out)
    before = len(df)
    df = df[df["title"].notna() | df["lead_family_name"].notna()]
    df = df.drop_duplicates(subset=["funder_award_id"], keep="first")
    log(f"Dedup: {before} table rows -> {len(df)} awards (repeated accordion sets / tables dropped)")
    return df


def discover_years() -> list[int]:
    hub = get(HUB).text
    years = {int(y) for y in re.findall(r"research-award-recipients-(\d{4})", hub)}
    top = max(years | {datetime.now().year})
    for y in range(FIRST_YEAR, top + 2):
        if y not in years:
            r = get(YEAR_URL.format(year=y))
            if r.status_code == 200 and "Research Grant Recipients" in r.text:
                years.add(y)
            time.sleep(REQUEST_DELAY)
    return sorted(years)


def main() -> None:
    ap = argparse.ArgumentParser(description="Heart Foundation (AU) grant recipients -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None, help="smoke test: only the N most recent rounds")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = ap.parse_args()

    years = discover_years()
    log(f"Rounds online: {years}")
    if args.limit:
        years = years[-args.limit:]
    rows = []
    for y in years:
        cache = args.cache_dir / f"nhf_{y}.html" if args.cache_dir else None
        if cache and cache.exists():
            page = cache.read_text()
        else:
            page = get(YEAR_URL.format(year=y)).text
            if cache:
                args.cache_dir.mkdir(parents=True, exist_ok=True)
                cache.write_text(page)
        yr = parse_year(y, page, args.cache_dir)
        log(f"  {y}: {len(yr)} table rows")
        if not yr:
            raise RuntimeError(f"no recipient rows parsed for {y}; page layout changed?")
        rows += yr
    df = build(rows)
    log(f"{len(df)} awards; by year {df['award_year'].value_counts().sort_index().to_dict()}")
    for c in ["title", "lead_family_name", "lead_institution", "amount", "grant_id_published", "research_category"]:
        log(f"  {c:20s} {df[c].notna().mean():6.1%}")
    log(f"  schemes {df['scheme'].value_counts().to_dict()}")

    df["amount"] = df["amount"].map(lambda v: None if v is None or pd.isna(v) else repr(float(v)))
    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / f"{SLUG}_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload or args.limit:
        log("Upload skipped")
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / f"_previous_{SLUG}_projects.parquet"
    try:  # runbook §1.4: never shrink the corpus on re-ingest
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
