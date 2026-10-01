#!/usr/bin/env python3
"""
National Institute of Justice (NIJ) to S3 Data Pipeline
=======================================================

NIJ publishes every award it has made since FY2003 at
https://nij.ojp.gov/funding/awards/list ("List of Funded Projects", Drupal view,
25 rows per page). The list has ONE ROW PER AWARD ACTION: an award's original
funding and each supplement are separate rows sharing the award number (the
supplement's title ends in " - S" and carries its own fiscal year, amount and
continuation solicitation). "Number of Awards: 7,493" counts actions, which fold
into ~6,600 distinct award numbers. Each row carries: Fiscal Year, Title,
Original Solicitation (funding opportunity), Recipient, State, Award Number,
Amount, Status. The award detail pages add only the Funding Category, county and
congressional district (no abstract, no named PI), and the Funding Category is
also a list filter, so the whole corpus comes from list pages (method 5, static
HTML; no bulk export exists on nij.ojp.gov or ojp.gov):

  1. one full pass ordered by title (a stable sort; the default FY sort has
     thousands of ties, so pages would drift),
  2. one pass per Funding Category filter value to tag each action with its
     category (Competitive, Competitive Discretionary, Formula, Continuation,
     Non-Competitive, Noncompetitive Discretionary); these passes also pick up
     any action the full pass missed.

Actions are deduplicated on (award number, fiscal year, amount, title), then
folded into one award per award number: title / solicitation / category /
recipient from the earliest (original) action, start year = its fiscal year,
amount = sum over actions, `n_actions` and `last_fiscal_year` kept.

robots.txt (nij.ojp.gov) allows /funding/awards/list for all agents.

Scope filter (oxjob #1451, Kyle 2026-10-01): keep every research, evaluation,
fellowship and technology-development award; exclude ONLY clearly operational
formula and backlog-reduction programmes, i.e. money that pays crime labs to
process casework rather than to study anything:
  - Funding Category "Formula" (DNA Capacity Enhancement and Backlog Reduction,
    Coverdell Forensic Science Improvement formula, etc.)
  - the competitive half of the Paul Coverdell Forensic Science Improvement
    Grants (COVERDELL_RE): the same forensic-lab capacity programme as its
    formula half, paying for equipment, overtime, accreditation and backlog
    elimination (many award titles are literally "... Backlog Reduction")
  - any award whose solicitation or title is a DNA / casework backlog-reduction
    programme (BACKLOG_RE below), whatever its category, e.g. the competitive
    "No Suspect Casework DNA Backlog Reduction Program" and continuation
    "2007 Convicted Offender Backlog" awards
  - awards whose status is "Declined" or "No Award" (never awarded)
A solicitation that names research / evaluation / development / fellowship is
never excluded by the Coverdell or backlog patterns. Everything else is kept,
including competitive forensic programmes with an operational flavour (Solving
Cold Cases with DNA, Postconviction DNA Testing, Using DNA Technology to
Identify the Missing, forensic training delivery), which are flagged in the
report rather than dropped. The script writes excluded rows to the
parquet too, with `excluded=1` and `exclusion_reason`, so the notebook filters
them and the audit trail stays in S3.

funder_award_id: the NIJ award number (e.g. 2019-DU-BX-0001,
15PNIJ-24-GG-01571-RESS), which is what grantees cite.

Output: s3://openalex-ingest/awards/nij/nij_projects.parquet
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

BASE = "https://nij.ojp.gov"
LIST_URL = f"{BASE}/funding/awards/list"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/nij/nij_projects.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 openalex-walden/1.0"}
REQUEST_DELAY = 0.5
CATEGORIES = ["Competitive", "Competitive Discretionary", "Formula", "Continuation",
              "Non-Competitive", "Noncompetitive Discretionary"]
MAX_CONSECUTIVE_EMPTY = 3
MAX_CONSECUTIVE_NON200 = 5

# DNA / casework backlog-reduction programmes (operational lab capacity, not research)
BACKLOG_RE = re.compile(
    r"backlog\s+reduction|dna\s+backlog|offender\s+backlog|casework\s+backlog|"
    r"capacity\s+enhancement\s+(?:and|for)\s+backlog|backlog\s+elimination|"
    r"\bbacklog\b.*\b(?:dna|casework|offender|arrestee)\b|\b(?:dna|casework|offender|arrestee)\b.*\bbacklog\b",
    re.I)
# Paul Coverdell Forensic Science Improvement Grants (National Forensic Sciences
# Improvement Act): the forensic-lab capacity programme. Its formula half is already
# category "Formula"; the competitive half pays for the same things (equipment,
# overtime, outsourcing, accreditation, eliminating case backlogs), so it is
# excluded as operational capacity / backlog reduction too.
COVERDELL_RE = re.compile(r"coverdell|forensic\s+sciences?\s+improvement", re.I)
# a research / evaluation programme that mentions backlogs stays in
RESEARCH_RE = re.compile(r"research|evaluation|fellowship|development|study|assessment of", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(session: requests.Session, params: dict) -> requests.Response | None:
    for attempt in range(4):
        try:
            return session.get(LIST_URL, params=params, timeout=90)
        except requests.RequestException as e:
            log(f"  request error ({attempt + 1}/4): {e}")
            time.sleep(3 * (attempt + 1))
    return None


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("​", "").replace("﻿", "")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def parse_rows(page: str) -> list[dict]:
    body = re.search(r"<tbody>(.*?)</tbody>", page, re.S)
    if not body:
        return []
    rows = []
    for tr in re.findall(r"<tr>(.*?)</tr>", body.group(1), re.S):
        cells = dict(re.findall(r'<td headers="([^"]+)"[^>]*>(.*?)</td>', tr, re.S))
        title_html = cells.get("view-title-table-column", "")
        href = re.search(r'href="([^"]+)"', title_html)
        sol_html = cells.get("view-field-funding-opportunity-table-column", "")
        sol_href = re.search(r'href="([^"]+)"', sol_html)
        rows.append({
            "award_number": text(cells.get("view-field-award-number-table-column")),
            "fiscal_year": text(cells.get("view-field-fiscal-year-table-column")),
            "title": text(title_html),
            "solicitation": text(sol_html),
            "solicitation_url": BASE + sol_href.group(1) if sol_href else None,
            "recipient": text(cells.get("view-field-awardee-table-column")),
            "state": text(cells.get("view-field-awardee-address-administrative-area-table-column")),
            "amount_text": text(cells.get("view-field-award-amount-table-column")),
            "status": text(cells.get("view-field-award-status-table-column")),
            "landing_page_url": BASE + href.group(1) if href else None,
        })
    return rows


def reported_total(page: str) -> int | None:
    m = re.search(r"Number of Awards:\s*([\d,]+)", re.sub(r"<[^>]+>", " ", page))
    return int(m.group(1).replace(",", "")) if m else None


def last_page(page: str) -> int:
    pages = [int(p) for p in re.findall(r"[?&]page=(\d+)", html.unescape(page))]
    return max(pages) if pages else 0


def fetch_page(session: requests.Session, params: dict, cache_dir: Path | None) -> tuple[int | None, str]:
    path = None
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        key = "_".join(f"{k}-{v}" for k, v in sorted(params.items()))
        path = cache_dir / (re.sub(r"[^A-Za-z0-9_.-]", "_", key) + ".html")
        if path.exists() and path.stat().st_size > 0:
            return 200, path.read_text()
    time.sleep(REQUEST_DELAY)
    r = get(session, params)
    if r is None:
        return None, ""
    if r.status_code == 200 and path:
        path.write_text(r.text)
    return r.status_code, r.text


def crawl(session: requests.Session, category: str | None, limit_pages: int | None,
          cache_dir: Path | None) -> tuple[list[dict], int | None]:
    params = {"order": "title", "sort": "asc"}
    if category:
        params["field_funding_type_value"] = category
    status, first = fetch_page(session, {**params, "page": 0}, cache_dir)
    if status != 200:
        raise RuntimeError(f"first page failed for category={category!r}: HTTP {status}")
    total = reported_total(first)
    n_pages = last_page(first) + 1
    if limit_pages:
        n_pages = min(n_pages, limit_pages)
    log(f"  [{category or 'ALL'}] reported {total} award actions, {n_pages} pages")
    rows = parse_rows(first)
    consecutive_empty = consecutive_non200 = 0
    page = 1
    while page < n_pages:
        status, body = fetch_page(session, {**params, "page": page}, cache_dir)
        if status != 200:
            consecutive_non200 += 1
            log(f"  page {page}: HTTP {status} ({consecutive_non200}/{MAX_CONSECUTIVE_NON200}); continuing")
            if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError(f"{MAX_CONSECUTIVE_NON200} consecutive failures at page {page}")
            page += 1
            continue
        consecutive_non200 = 0
        got = parse_rows(body)
        if not got:
            consecutive_empty += 1
            log(f"  page {page}: empty ({consecutive_empty}/{MAX_CONSECUTIVE_EMPTY})")
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                break
            page += 1
            continue
        consecutive_empty = 0
        rows += got
        if page % 25 == 0:
            log(f"  [{category or 'ALL'}] page {page}/{n_pages}: {len(rows)} rows")
        page += 1
    for r in rows:
        r["funding_category"] = category
    return rows, total


def parse_amount(s: str | None) -> float | None:
    if not s:
        return None
    digits = re.sub(r"[^\d.]", "", s)
    return float(digits) if digits else None


def exclusion(row) -> str | None:
    # status "Declined" / "No Award": the money was never awarded
    if row["status"] in {"Declined", "No Award"}:
        return "not_awarded"
    if row["funding_category"] == "Formula":
        return "formula"
    hay = " | ".join(x for x in [row["solicitation"], row["title"]] if x)
    if COVERDELL_RE.search(hay) and not RESEARCH_RE.search(row["solicitation"] or ""):
        return "coverdell_forensic_capacity"
    if BACKLOG_RE.search(hay) and not RESEARCH_RE.search(row["solicitation"] or ""):
        return "backlog_reduction"
    return None


SUPPLEMENT_SUFFIX = re.compile(r"\s+-\s+S\s*$")


def fold(actions: pd.DataFrame) -> pd.DataFrame:
    """One award per award number. The original action (earliest fiscal year,
    non-" - S" title first) supplies title/solicitation/category/recipient."""
    a = actions.copy()
    a["fy"] = pd.to_numeric(a["fiscal_year"], errors="coerce")
    a["is_supp"] = a["title"].fillna("").str.contains(SUPPLEMENT_SUFFIX)
    a["amt"] = a["amount_text"].map(parse_amount)
    a = a.sort_values(["award_number", "is_supp", "fy"])
    out = []
    for num, g in a.groupby("award_number", sort=False):
        b = g.iloc[0]
        amts = g["amt"].dropna()
        out.append({
            "award_number": num,
            "fiscal_year": str(int(b["fy"])) if pd.notna(b["fy"]) else None,
            "last_fiscal_year": str(int(g["fy"].max())) if g["fy"].notna().any() else None,
            "title": SUPPLEMENT_SUFFIX.sub("", b["title"]) if b["title"] else None,
            "solicitation": b["solicitation"],
            "solicitation_url": b["solicitation_url"],
            "funding_category": b["funding_category"],
            "action_categories": "; ".join(sorted({c for c in g["funding_category"] if isinstance(c, str)})) or None,
            "recipient": b["recipient"],
            "state": b["state"],
            "status": g.sort_values("fy").iloc[-1]["status"],
            "amount_original": float(b["amt"]) if pd.notna(b["amt"]) else None,
            "amount": float(amts.sum()) if len(amts) else None,
            "currency": "USD" if len(amts) else None,
            "n_actions": str(len(g)),
            "landing_page_url": b["landing_page_url"],
        })
    return pd.DataFrame(out)


def main() -> None:
    p = argparse.ArgumentParser(description="NIJ funded-projects list -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="max list pages per pass (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache list pages here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    s = requests.Session()
    s.headers.update(HEADERS)
    log("Full pass (order=title)")
    rows, total = crawl(s, None, args.limit, args.cache_dir)
    for c in CATEGORIES:
        got, _ = crawl(s, c, args.limit, args.cache_dir)
        rows += got
    acts = pd.DataFrame(rows)
    acts = acts[acts["award_number"].notna()]
    key = ["award_number", "fiscal_year", "amount_text", "title"]
    # an action's category comes from the category pass that listed it
    acts["funding_category"] = acts.groupby(key, dropna=False)["funding_category"].transform("first")
    n_rows = len(acts)
    acts = acts.drop_duplicates(subset=key).reset_index(drop=True)
    log(f"Actions: {n_rows} rows over all passes, {len(acts)} distinct (site reports {total}); "
        f"uncategorised {acts['funding_category'].isna().sum()}")
    if not args.limit and total and len(acts) < total * 0.98:
        raise SystemExit(f"only {len(acts)} of {total} reported award actions collected; refusing to continue")

    df = fold(acts)
    df["exclusion_reason"] = df.apply(exclusion, axis=1)
    df["excluded"] = df["exclusion_reason"].notna().map(lambda b: "1" if b else "0")
    dupes = df["award_number"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'award_number'].tolist()[:20]}")

    kept = df[df["excluded"] == "0"]
    log(f"Awards: {len(df)}; excluded {len(df) - len(kept)} "
        f"({df['exclusion_reason'].value_counts().to_dict()}); kept {len(kept)}")
    log(f"  funding_category (all): {df['funding_category'].value_counts(dropna=False).to_dict()}")
    for c in ["title", "solicitation", "recipient", "amount", "fiscal_year", "funding_category"]:
        log(f"  {c:18s} {kept[c].notna().mean():6.1%}")
    log(f"  kept amount USD {kept['amount'].sum():,.0f}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "nij_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_nij_projects.parquet"
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
