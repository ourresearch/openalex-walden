#!/usr/bin/env python3
"""
KidneyCure (ASN Foundation for Kidney Research) recipients to S3
================================================================

KidneyCure is the American Society of Nephrology's research foundation
(ASN Foundation for Kidney Research, US 501(c)3, est. 2012; the older
programmes were run by ASN itself). It publishes one static HTML recipient
list per grant programme at https://www.kidneycure.org/pages/recipients.aspx:

    /grants/tig/recipients.aspx?app=<CODE>          (Transition to Independence family)
    /grants/fellowships/recipients.aspx?app=<CODE>  (research / pre-doctoral fellowships)

Each list is grouped by award year (<h4>), optionally by named sub-award
(<h6>, e.g. "Donald E. Wesson Research Fellow" on the Lipps page), and each
recipient is one <p>: "Name, degrees<br /><i>Project title</i>" (pre-~2004
entries carry the name only). The lists carry no grant number, amount or
institution; NULL is the correct value for those. Method 5 (static HTML) on
the runbook ladder; 14 pages, no detail pages.

robots.txt (2026-10-01): User-agent * disallows /api/*, /js/*, /thumb/* etc.;
the recipient pages themselves are allowed and server-rendered.

Output: s3://openalex-ingest/awards/kidneycure/kidneycure_projects.parquet
"""

import argparse
import html
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). See runbook §1.2.
# (grep anchor for the §4.0 self-check: sys.stdout.reconfigure)
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

BASE = "https://www.kidneycure.org"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/kidneycure/kidneycure_projects.parquet"

# (app code, path, funding_type). Order matters for cross-page dedup: a
# person listed on both a programme page and the Lipps page's sub-headings
# (e.g. "KidneyCure Pre-Doctoral Fellow") is kept from the earlier page.
PAGES = [
    ("PREDOC", "/grants/fellowships/recipients.aspx?app=PREDOC", "fellowship"),
    ("LIPPS", "/grants/fellowships/recipients.aspx?app=LIPPS", "fellowship"),
    ("GOT", "/grants/tig/recipients.aspx?app=GOT", "research"),
    ("MERRILL", "/grants/tig/recipients.aspx?app=MERRILL", "research"),
    ("SIEGEL", "/grants/tig/recipients.aspx?app=SIEGEL", "research"),
    ("BONVENTRE", "/grants/tig/recipients.aspx?app=BONVENTRE", "research"),
    ("COP", "/grants/tig/recipients.aspx?app=COP", "research"),
    ("DEIJ", "/grants/tig/recipients.aspx?app=DEIJ", "research"),
    ("AAIM", "/grants/tig/recipients.aspx?app=AAIM", "research"),
    ("AKF", "/grants/tig/recipients.aspx?app=AKF", "research"),
    ("HALPIN", "/grants/tig/recipients.aspx?app=HALPIN", "research"),
    ("NEPHCURE", "/grants/tig/recipients.aspx?app=NEPHCURE", "research"),
    ("OHF", "/grants/tig/recipients.aspx?app=OHF", "research"),
    ("BENNETT", "/grants/tig/recipients.aspx?app=BENNETT", "research"),
    # TIG umbrella page: currently empty (recipients are listed under the named grants);
    # kept so new entries are picked up if KidneyCure starts listing there.
    ("TIG", "/grants/tig/recipients.aspx?app=TIG", "research"),
]

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"}
REQUEST_DELAY = 1.0
RETRIES = 3


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            log(f"GET {url} -> {r.status_code} ({len(r.content)} bytes)")
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("​", "").replace("﻿", "")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|mr|mrs|ms)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py) plus a leading-honorific strip."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                "mph", "ms", "msc", "mbbs", "mbchb", "do", "fasn"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_person(raw: str) -> tuple[str | None, str | None]:
    """'Laurence H. Beck, Jr., MD, PhD' -> ('Laurence H. Beck Jr.', 'MD, PhD').
    The list writes 'Name, degree, degree'; the name is the first comma chunk
    (plus a Jr./Sr./II/III chunk if one follows). Parenthesised nicknames
    ('Jing (Jason) O Wu') are dropped."""
    if not raw:
        return None, None
    chunks = [c.strip() for c in raw.split(",") if c.strip()]
    name = chunks[0]
    rest = chunks[1:]
    if rest and rest[0].lower().strip(".") in {"jr", "sr", "ii", "iii", "iv"}:
        name = f"{name} {rest.pop(0)}"
    name = re.sub(r"\s*\([^)]*\)\s*", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name or None, (", ".join(rest) or None)


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def parse_page(app: str, url: str, page: str, funding_type: str) -> list[dict]:
    content = page[page.find('id="content"'):]
    programme = text(re.search(r"<h3>(.*?)</h3>", content, re.S).group(1))
    programme = re.sub(r"\s*Recipients$", "", programme)
    start = page.find('<div id="recipient_list">')
    if start < 0:
        raise RuntimeError(f"{url}: no recipient_list block (page layout changed?)")
    seg = page[start: page.find("end #content", start)]
    rows, year, sub = [], None, None
    for m in re.finditer(r"<h4>(.*?)</h4>|<h6>(.*?)</h6>|<p>(.*?)</p>", seg, re.S):
        if m.group(1) is not None:
            year, sub = text(m.group(1)), None
            continue
        if m.group(2) is not None:
            sub = text(m.group(2))
            continue
        p = m.group(3)
        title_m = re.search(r"<i>(.*?)</i>", p, re.S)
        person_raw = text(re.split(r"<br\s*/?>", p, maxsplit=1)[0])
        name, degrees = parse_person(person_raw)
        title = text(title_m.group(1)) if title_m else None
        if title:
            title = title.strip(' "“”').strip() or None
        given, family = split_name(name)
        rows.append({
            "app": app,
            "programme": programme,
            "sub_programme": sub,
            "award_year": year,
            "recipient_raw": person_raw,
            "lead_name": name,
            "lead_degrees": degrees,
            "lead_given_name": given,
            "lead_family_name": family,
            "title": title,
            "funding_type": funding_type,
            "landing_page_url": url,
        })
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description="KidneyCure recipient lists -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only fetch the first N programme pages")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    pages = PAGES[: args.limit] if args.limit else PAGES
    rows = []
    for app, path, ftype in pages:
        url = BASE + path
        recs = parse_page(app, url, get(url), ftype)
        log(f"  {app:10s} {len(recs):4d} recipients")
        rows += recs
        time.sleep(REQUEST_DELAY)

    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("no recipients parsed")
    bad_year = ~df["award_year"].fillna("").str.fullmatch(r"(19|20)\d{2}")
    if bad_year.any():
        raise SystemExit(f"unparseable year headings: {df.loc[bad_year, ['app', 'award_year']].drop_duplicates().values.tolist()}")

    # The same award can be listed twice (a programme page and a Lipps-page
    # sub-heading, or a repeated <p>): keep the first listing.
    df["_key"] = df["award_year"] + "|" + df["lead_name"].map(slug)
    before = len(df)
    dup = df[df["_key"].duplicated(keep=False)]
    for k, g in dup.groupby("_key"):
        log(f"  duplicate listing {k}: {g['app'].tolist()} -> keeping {g['app'].iloc[0]}")
    df = df.drop_duplicates(subset="_key", keep="first").drop(columns="_key")
    log(f"Parsed {before} listings, {before - len(df)} duplicate listings dropped -> {len(df)} awards")

    # Synthetic, stable key: KidneyCure publishes no grant number, and the
    # existing citation stubs on ASN (F4320306605) / ASN Foundation
    # (F4320311111) carry co-funders' numbers (NIH, AHA), not KidneyCure
    # ones, so there is no citable form to match.
    df["funder_award_id"] = ("KIDNEYCURE-" + df["app"] + "-" + df["award_year"] + "-"
                             + df["lead_name"].map(slug))
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")

    for c in ["title", "lead_family_name", "lead_given_name", "sub_programme"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  years {df['award_year'].min()}-{df['award_year'].max()}")
    log("  by programme: " + ", ".join(f"{k}={v}" for k, v in df["app"].value_counts().items()))

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "kidneycure_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_kidneycure_projects.parquet"
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
