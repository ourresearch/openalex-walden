#!/usr/bin/env python3
"""
Georgia Clinical and Translational Science Alliance (Georgia CTSA) pilot grants to S3
=====================================================================================

Georgia CTSA (Emory / Morehouse School of Medicine / Georgia Tech / University of
Georgia) is an NIH CTSA hub, funded by NCATS award UL1TR002378 (predecessor:
Atlanta Clinical and Translational Science Institute, ACTSI, UL1TR000454 /
UL1RR025008). Its pilot grants are sub-awards paid from that NIH grant.

Source: https://georgiactsa.org/funding/pilot-grant-recipients/index.html lists
every pilot-grant "round" (#34-#148) run through the Georgia CTSA / Emory pilot
grant office, one static page per round (grants-<n>.html). Each page lists the
awards: project title (usually with the amount in parentheses) and a nested list
of investigators ("PI: Name, degrees, position, department, institution").

SCOPE FILTER: the same office administers rounds for many OTHER sponsors (Emory
School of Medicine I3, Healthcare Innovation Program, Emory University Research
Committee, Regenerative Medicine, CFAR, ADRC, Morningside Center, Gordon GCRC,
...). Only rounds whose heading names the Georgia CTSA or its predecessor ACTSI
("Georgia CTSA", "Georgia CTSA (BERD)", "(Informatics)", "REM Program",
"Community Engagement", "MSM/ACTSI Pilot", "Georgia CTSA and James J Hills and
Wanda S Hills") are ingested. Rounds #133 and #139 (Georgia CTSA) publish only
the count of awards with no award list, so they cannot be ingested.

No award numbers are published: funder_award_id = "GACTSA-R<round>-<nn>"
(round number + 1-based position of the award on the round page).

Output: s3://openalex-ingest/awards/georgia_ctsa/georgia_ctsa_projects.parquet
"""

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
import sys
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if sys.platform == "win32":
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

INDEX_URL = "https://georgiactsa.org/funding/pilot-grant-recipients/index.html"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/georgia_ctsa/georgia_ctsa_projects.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"}
REQUEST_DELAY = 0.7
RETRIES = 4
MAX_CONSECUTIVE_NON200 = 5

IN_SCOPE_RE = re.compile(r"Georgia CTSA|ACTSI", re.I)
MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], 1)}
ROLE_RE = re.compile(  # "PI:", "Co-PI:", "Consultant:", "Co-Investigator::", "PI - "
    r"^\s*(?:((?:Co-?|Contact |Multi-?)?(?:PI|I|Investigators?))\s*(?::+|\s[-–]\s)|([A-Z][A-Za-z/ -]{1,30}?)\s*:+)\s*", re.I)
AMOUNT_RE = re.compile(r"\(\s*\$\s*([0-9][0-9,]*(?:\.\d+)?)[^()$]*\)", re.I)  # "($40,000)", "($39,971.60 funded by ...)"
# Partner / local institutions, normalised; the first one named in an investigator line wins.
INSTITUTIONS = [
    (r"Georgia Institute of Technology|Georgia Tech\b", "Georgia Institute of Technology"),
    (r"Morehouse School of Medicine|\bMSM\b", "Morehouse School of Medicine"),
    (r"University of Georgia|\bUGA\b", "University of Georgia"),
    (r"Georgia State University", "Georgia State University"),
    (r"Kennesaw State", "Kennesaw State University"),
    (r"Augusta University|Medical College of Georgia", "Augusta University"),
    (r"Children'?s Healthcare of Atlanta", "Children's Healthcare of Atlanta"),
    (r"Atlanta VA|VA Medical Center|Veterans Affairs", "Atlanta VA Medical Center"),
    (r"Centers for Disease Control|\bCDC\b", "Centers for Disease Control and Prevention"),
    (r"Emory|Rollins School of Public Health|Nell Hodgson Woodruff|Winship|Yerkes", "Emory University"),
]
NOT_A_NAME_RE = re.compile(
    r"Professor|Department|Division|School|University|Director|Fellow|Scientist|Instructor|Lecturer|"
    r"Student|Resident|Postdoc|Program|Center|Institute", re.I)
DEGREE_TOKENS = {
    "phd", "ph.d", "ph.d.", "md", "m.d", "m.d.", "msc", "mph", "mba", "rn", "dvm", "pharmd",
    "mstat", "msw", "faan", "fnp-bc", "dnp", "bsn", "msn", "mhs", "dphil", "dsc", "scd", "drph",
    "mscr", "psyd", "mbbs", "mmsc", "ms", "ma",  # only stripped when >2 tokens remain ("Tianwen Ma" kept)
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> tuple[int, str]:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            log(f"GET {url} -> {r.status_code} ({len(r.content)} bytes)")
            if r.status_code in (502, 503, 504):  # transient gateway errors seen on this host
                time.sleep(3 * (attempt + 1))
                continue
            r.encoding = "utf-8"
            return r.status_code, r.text
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(3 * (attempt + 1))
    log(f"GET {url} failed: {last}")
    return 0, ""


def clean(s: str | None) -> str | None:
    if s is None:
        return None
    s = s.replace("\xa0", " ").replace("​", "").replace("﻿", "").replace("�", "'")
    s = re.sub(r"\s+", " ", s).strip(" ,;:-")
    return s or None


def round_links(index_html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(index_html, "lxml")
    out = {}
    for a in soup.select("main a[href]"):
        href = a["href"]
        if re.search(r"grants-x?\d+\.html$", href):
            out[urljoin(INDEX_URL, href)] = clean(a.get_text(" "))
    return sorted(out.items())


def parse_round_heading(h1: str) -> dict:
    num = re.search(r"Round\s*#?\s*(\d+)", h1)
    d = None
    m = re.search(r"\((\d{1,2})/(\d{4})", h1)
    if m:
        d = f"{int(m.group(2)):04d}-{int(m.group(1)):02d}-01"
    else:
        m = re.search(r"\(?\s*(" + "|".join(MONTHS) + r")\s*(\d{4})", h1, re.I)
        if m:
            d = f"{int(m.group(2)):04d}-{MONTHS[m.group(1).lower()]:02d}-01"
    program = re.sub(r"^Pilot Grant Recipients?\s*[-–�']*\s*", "", h1)
    program = re.sub(r"Round\s*#?\s*\d+", "", program)
    program = re.sub(r"\([^)]*\d{4}[^)]*\)|\(?\s*(" + "|".join(MONTHS) + r")\s*\d{4}\)?", "", program, flags=re.I)
    program = clean(re.sub(r"\s*-\s*-\s*|\s+-\s*$|^\s*-\s+", " ", program))
    return {"round_number": num.group(1) if num else None, "round_date": d, "round_program": program}


def split_person(text: str) -> dict | None:
    """'PI: Jessica Alvarez, PhD, RD, Assoc. Professor, Dept. Medicine, Emory University'."""
    t = clean(text) or ""
    role = None
    m = ROLE_RE.match(t)
    if m:
        role = m.group(1) or m.group(2)
        t = t[m.end():]
    amt = AMOUNT_RE.search(t)
    t = AMOUNT_RE.sub("", t).strip()
    parts = [p.strip() for p in re.split(r",|;", t) if p.strip()]
    if not parts:
        return None
    name = parts[0]
    # "Lance Waller PhD" / "Jeffrey Skolnick, Ph.D." -> drop degree tokens on the name segment
    toks = name.split()
    while len(toks) > 2 and toks[-1].lower().strip(",") in DEGREE_TOKENS:  # "Lance Waller PhD"
        toks.pop()
    name = " ".join(toks)
    if not name or NOT_A_NAME_RE.search(name) or len(name.split()) > 5:
        name = None  # line with no person name (e.g. a bare position/affiliation line)
    rest = ", ".join(parts[1:])
    hits = [(m.start(), canon) for pat, canon in INSTITUTIONS for m in [re.search(pat, rest)] if m]
    inst = min(hits)[1] if hits else None  # first institution named after the person's name
    given, family = split_name(name) if name else (None, None)
    return {
        "role": role, "name": name, "given_name": given, "family_name": family,
        "institution": inst, "raw": clean(text),
        "amount": amt.group(1).replace(",", "") if amt else None,
    }


def split_name(name: str) -> tuple[str | None, str | None]:
    """Split 'James P. Eisenstein' -> ('James P.', 'Eisenstein') (wolf_to_s3.py canonical)."""
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


def parse_round(url: str, page: str) -> tuple[dict, list[dict]]:
    soup = BeautifulSoup(page, "lxml")
    h1 = clean(soup.find("h1").get_text(" ")) if soup.find("h1") else ""
    info = parse_round_heading(h1)
    info.update({"round_heading": h1, "round_url": url})
    w = soup.select_one("main .wysiwyg")
    if w is None or not IN_SCOPE_RE.search(h1):
        return info, []
    intro = clean(w.get_text(" "))[:300] if w.get_text(strip=True) else ""
    info["round_intro"] = intro
    each = re.search(r"\(\s*\$\s*([0-9][0-9,]*)\s*each\s*\)", intro or "", re.I)
    awards = []
    outer = [li for li in w.find_all("li") if li.find("ul") and not li.find_parent("li")]
    for li in outer:
        nested = li.find("ul")
        people_li = nested.find_all("li")
        nested.extract()
        title_raw = clean(li.get_text(""))  # "" not " ": some titles are split across inline spans
        amt = AMOUNT_RE.search(title_raw or "")
        title = clean(AMOUNT_RE.sub("", title_raw or ""))
        people = [p for p in (split_person(x.get_text(" ")) for x in people_li) if p]
        named = [p for p in people if p["name"]]
        amount = amt.group(1).replace(",", "") if amt else None
        if amount is None:
            amount = next((p["amount"] for p in people if p["amount"]), None)
        if amount is None and each:
            amount = each.group(1).replace(",", "")
        lead = next((p for p in named if p["role"] and p["role"].lower() in ("pi", "contact pi", "mpi", "multi-pi")), None) \
            or (named[0] if named else None)
        co = [p for p in named if p is not lead]
        awards.append({
            "title": title,
            "amount": amount,
            "pi_name": lead["name"] if lead else None,
            "pi_given_name": lead["given_name"] if lead else None,
            "pi_family_name": lead["family_name"] if lead else None,
            "pi_institution": lead["institution"] if lead else None,
            "pi_raw": lead["raw"] if lead else None,
            "co_investigators": json.dumps(
                [{k: p[k] for k in ("role", "name", "given_name", "family_name", "institution", "raw")} for p in co],
                ensure_ascii=False),
            "n_investigators": str(len(named)),
        })
    return info, awards


def main() -> None:
    p = argparse.ArgumentParser(description="Georgia CTSA pilot grant recipients (georgiactsa.org) -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only fetch the first N round pages")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None)
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the section 1.4 shrink guard")
    args = p.parse_args()

    status, idx = get(INDEX_URL)
    if status != 200:
        raise RuntimeError(f"index HTTP {status}")
    links = round_links(idx)
    log(f"Index: {len(links)} round pages")
    in_scope = [(u, t) for u, t in links if IN_SCOPE_RE.search(t or "")]
    log(f"In scope by index label (Georgia CTSA / ACTSI): {len(in_scope)}")
    # Index labels are sometimes truncated/blank (e.g. 'Round #142'); fetch every page and
    # decide scope on the page's own heading.
    if args.limit:
        links = links[: args.limit]
    rows, rounds, non200, skipped = [], [], 0, []
    for i, (url, label) in enumerate(links, 1):
        cache = args.cache_dir / url.rsplit("/", 1)[-1] if args.cache_dir else None
        if cache and cache.exists():
            status, page = 200, cache.read_text()
        else:
            status, page = get(url)
            time.sleep(REQUEST_DELAY)
        if status != 200:
            non200 += 1
            skipped.append(url)
            if non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError("too many consecutive non-200 pages; refusing to truncate")
            continue
        non200 = 0
        if cache:
            args.cache_dir.mkdir(parents=True, exist_ok=True)
            cache.write_text(page)
        info, awards = parse_round(url, page)
        rounds.append(info)
        if IN_SCOPE_RE.search(info.get("round_heading") or ""):
            log(f"  round {info['round_number']} [{info['round_program']}] {info['round_date']}: {len(awards)} awards")
        for n, a in enumerate(awards, 1):
            a.update({k: info.get(k) for k in ("round_number", "round_date", "round_program", "round_heading", "round_url")})
            a["award_seq"] = str(n)
            rows.append(a)
    log(f"{len(rounds)} round pages parsed, {len(skipped)} skipped, {len(rows)} in-scope awards")
    for u in skipped:
        log(f"  skipped: {u}")
    empty = [r["round_number"] for r in rounds if IN_SCOPE_RE.search(r.get("round_heading") or "")
             and not any(x["round_number"] == r["round_number"] for x in rows)]
    log(f"In-scope rounds with no award list on the page: {empty}")

    df = pd.DataFrame(rows)
    df["funder_award_id"] = "GACTSA-R" + df["round_number"] + "-" + df["award_seq"].str.zfill(2)
    df["currency"] = df["amount"].map(lambda x: "USD" if isinstance(x, str) and x else None)
    dupes = df["funder_award_id"].duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    for c in ["title", "amount", "pi_family_name", "pi_institution", "round_date"]:
        log(f"  {c:16s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "georgia_ctsa_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_georgia_ctsa_projects.parquet"
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
