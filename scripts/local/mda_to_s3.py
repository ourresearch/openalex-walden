#!/usr/bin/env python3
"""
Muscular Dystrophy Association (MDA) to S3 Data Pipeline
========================================================

MDA publishes every research grant it funds in its "Grants at a Glance"
Drupal view at https://www.mda.org/science/grants-at-a-glance (?page=0..N,
~25 grants per page, newest first). Each row carries: title, grantee,
funded start/end dates, grant type, disease(s), and a hidden overlay with the
lay abstract, a Crossref grant DOI (10.55762/MDA.<award>.pc.gr.<n>, grants
from 2016 onward), award total (USD), institution and country. Method 5
(static HTML) on the runbook ladder; no export / API exists.

Pre-2016 rows (Drupal node ids < ~5000) are legacy news-style entries:
the title is a placeholder like "Grant - Summer 2015 - CMT - Daniel Summers,
Ph.D.", the grantee carries a disease prefix, and dates / amount /
institution are blank. The script parses the round ("Summer 2015") and the
PI out of those strings and leaves the rest NULL.

funder_award_id (runbook section 2.1.1): MDA registers grant DOIs with
Crossref; the deposited award number is the <award> segment of the DOI
(e.g. 1446047), and those rows already exist in openalex_awards_raw as
crossref_work stubs (priority 1). Rows with a DOI ship that bare number so
they upgrade the stubs. Rows without a DOI ship "MDA-NID-<drupal node id>"
(the page's own stable record id; MDA prints no grant number for them).

Output: s3://openalex-ingest/awards/mda/mda_projects.parquet
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

BASE = "https://www.mda.org/science/grants-at-a-glance"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/mda/mda_projects.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"}
REQUEST_DELAY = 1.0
RETRIES = 3
MAX_CONSECUTIVE_EMPTY = 3
MAX_CONSECUTIVE_NON200 = 5

MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
SEASON_MONTH = {"winter": 1, "spring": 4, "summer": 7, "fall": 10, "autumn": 10}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> tuple[int, str]:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            log(f"GET {url} -> {r.status_code} ({len(r.content)} bytes)")
            r.encoding = "utf-8"
            return r.status_code, r.text
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 * (attempt + 1))
    log(f"GET {url} failed: {last}")
    return 0, ""


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


SUFFIXES = {"phd", "md", "dphil", "dsc", "scd", "jr", "sr", "ii", "iii", "iv",
            "mph", "ms", "msc", "mbbs", "mbchb", "dvm", "pharmd", "facp", "frcp", "frcpc",
            "dpt", "mba", "bsc", "mres", "mphil", "otr"}
# ("ma", "ms", "do", "rn", "pt", "bs" deliberately absent: real surnames like "Ke Ma", "Do")
HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|mr|mrs|ms)\.?\s+)+", re.I)


def split_name(name: str | None) -> tuple[str | None, str | None]:
    """Canonical runbook section 2.4.1 helper (wolf_to_s3.py): strip trailing
    degree/suffix tokens, last token = family, rest = given. Extended to
    dotted degrees ("Ph.D.", "M.D.") and comma-joined degrees ("Smith, MD")."""
    if not name:
        return None, None
    name = HONORIFIC_RE.sub("", name.strip())
    name = name.split(",")[0] if re.search(r",\s*(ph\.?d|m\.?d|d\.?phil|m\.?s|mph|dvm|pharmd)\b", name, re.I) else name
    tokens = name.split()
    while tokens and tokens[-1].lower().replace(".", "").strip(",") in SUFFIXES:
        tokens.pop()
    tokens = [t.strip(",") for t in tokens if t.strip(",")]
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


# legacy grantee "CMT - Daniel Summers, Ph.D." / "ALS – Udai Pandey" / "FSHD: Scott Harper"
PREFIX_SEP_RE = re.compile(r"\s+-\s+|\s*[–—]\s*|:\s+")
LEGACY_TITLE_RE = re.compile(r"^Grant\s*-\s*(Winter|Spring|Summer|Fall|Autumn)?\s*(\d{4})\s*-\s*(.*)$", re.I)


def field(row: str, cls: str) -> str | None:
    m = re.search(rf'views-field-{cls}">.*?<div class="field-content">(.*?)</div>', row, re.S)
    return text(m.group(1)) if m else None


def overlay_field(ov: str, label: str) -> str | None:
    m = re.search(rf"<strong>{label}:</strong>(.*?)</p>", ov, re.S)
    return text(m.group(1)) if m else None


def parse_row(row: str) -> dict | None:
    t = re.search(r'views-field-title">.*?<a href="\?nid=(\d+)">(.*?)</a>', row, re.S)
    if not t:
        return None
    nid, raw_title = t.group(1), text(t.group(2))
    dates = re.findall(r'content="(\d{4}-\d{2}-\d{2})T', row)
    ov_m = re.search(r'js-overlay-content"[^>]*>(.*)$', row, re.S)
    ov = ov_m.group(1) if ov_m else ""
    # abstract: everything between the image/h2 and the first "Grantee:" line
    body = re.split(r'<p class="mb0"><strong>Grantee:', ov)[0]
    body = re.sub(r"<h2>.*?</h2>", "", body, flags=re.S)
    doi_m = re.search(r'doi\.org/(10\.55762/[^"<\s]+)', body, re.I)
    body_wo_doi = re.sub(r"<p>\s*<a [^>]*doi\.org[^>]*>.*?</a>\s*</p>", "", body, flags=re.S | re.I)
    desc = text(body_wo_doi)
    grantee = field(row, "field-grantee-name") or overlay_field(ov, "Grantee")
    amount_txt = overlay_field(ov, "Award total")
    digits = re.sub(r"[^\d.]", "", amount_txt or "")
    amount = float(digits) if digits else None
    title, legacy_round, start = raw_title, None, dates[0] if dates else None
    lm = LEGACY_TITLE_RE.match(raw_title or "")
    if lm:
        # "Grant - Summer 2015 - CMT - Daniel Summers, Ph.D." is a placeholder, not a title
        title = None
        legacy_round = " ".join(x for x in (lm.group(1), lm.group(2)) if x)
        if not start:
            began = re.search(r"began\s+([A-Z][a-z]{2})[a-z]*\.?\s+(\d{1,2}),\s+(\d{4})", desc or "")
            if began and began.group(1).lower() in MONTHS:
                start = f"{began.group(3)}-{MONTHS[began.group(1).lower()]:02d}-{int(began.group(2)):02d}"
            else:
                mo = SEASON_MONTH.get((lm.group(1) or "").lower(), 1)
                start = f"{lm.group(2)}-{mo:02d}-01"
    # legacy grantee carries a disease prefix: "CMT - Daniel Summers, Ph.D."
    sep = list(PREFIX_SEP_RE.finditer(grantee or ""))
    person = grantee[sep[-1].end():].strip() if sep else grantee
    disease_prefix = grantee[: sep[-1].start()].strip() if sep else None
    given, family = split_name(person)
    grant_type = field(row, "field-grant-type") or overlay_field(ov, "Grant type")
    amount_source = "award_total" if amount is not None else None
    if lm:
        # Legacy records state the award in a fixed sentence of MDA's own record:
        # "... was awarded an MDA research grant totaling $300,000 over three years ..."
        gm = re.search(r"awarded an? MDA ([a-z ]+?grant) totaling \$([\d,]+(?:\.\d+)?)", desc or "", re.I)
        if gm:
            if amount is None:
                amount = float(gm.group(2).replace(",", ""))
                amount_source = "description"
            if not grant_type:
                grant_type = gm.group(1).strip().title()
    display_title = title
    if title is None:
        # composed title for legacy placeholder rows (runbook 2.3.1 style)
        parts = [f"MDA {grant_type or 'Grant'}"]
        if legacy_round:
            parts[0] += f", {legacy_round}"
        label = ": ".join(x for x in (disease_prefix, person and re.sub(r",.*$", "", person)) if x)
        display_title = f"{parts[0]} ({label})" if label else parts[0]
    doi = doi_m.group(1).rstrip(".").lower() if doi_m else None
    award_no = None
    if doi:
        am = re.match(r"10\.55762/mda\.(\d+)\.", doi)
        award_no = am.group(1) if am else None
    return {
        "nid": nid,
        "raw_title": raw_title,
        "title": title,
        "display_title": display_title,
        "title_is_composed": "true" if title is None else "false",
        "disease_prefix": disease_prefix,
        "legacy_round": legacy_round,
        "grantee_raw": grantee,
        "lead_name": person,
        "lead_given_name": given,
        "lead_family_name": family,
        "start_date": start,
        "end_date": dates[1] if len(dates) > 1 else None,
        "grant_type": grant_type,
        "diseases": field(row, "field-disease-tree"),
        "description": desc,
        "grant_doi": doi,
        "crossref_award_number": award_no,
        "amount_text": amount_txt,
        "amount": amount,
        "amount_source": amount_source,
        "currency": "USD" if amount is not None else None,
        "institution": overlay_field(ov, "Institution"),
        "country": overlay_field(ov, "Country"),
        "landing_page_url": f"{BASE}?nid={nid}",
    }


def norm_title(t: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (t or "").lower()).strip()


def crossref_awards() -> list[dict]:
    """MDA's own Crossref grant deposits (prefix 10.55762): award number, DOI, title."""
    out, cursor = [], "*"
    while True:
        r = requests.get(
            "https://api.crossref.org/prefixes/10.55762/works",
            params={"filter": "type:grant", "rows": 1000, "cursor": cursor, "mailto": "team@ourresearch.org"},
            headers=HEADERS, timeout=120,
        )
        r.raise_for_status()
        msg = r.json()["message"]
        for it in msg["items"]:
            titles = [t.get("title") for p in it.get("project") or [] for t in p.get("project-title") or []]
            out.append({"award": (it.get("award") or "").strip(), "doi": it["DOI"].lower(),
                        "title": titles[0] if titles else None})
        if not msg["items"]:
            return out
        cursor = msg["next-cursor"]


def main() -> None:
    p = argparse.ArgumentParser(description="MDA Grants at a Glance -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="stop after N grants (smoke test)")
    p.add_argument("--max-pages", type=int, default=200)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None)
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the section 1.4 shrink guard")
    args = p.parse_args()

    rows: list[dict] = []
    page, last_page = 0, None
    consecutive_empty = consecutive_non200 = 0
    t0 = time.time()
    while page <= (last_page if last_page is not None else args.max_pages):
        cache = args.cache_dir / f"page{page}.html" if args.cache_dir else None
        if cache and cache.exists():
            status, page_html = 200, cache.read_text()
        else:
            status, page_html = get(f"{BASE}?page={page}")
            time.sleep(REQUEST_DELAY)
        if status != 200:
            consecutive_non200 += 1
            log(f"page {page}: HTTP {status} ({consecutive_non200}/{MAX_CONSECUTIVE_NON200}); continuing")
            if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError("too many consecutive non-200 pages; refusing to truncate")
            page += 1
            continue
        consecutive_non200 = 0
        if cache:
            args.cache_dir.mkdir(parents=True, exist_ok=True)
            cache.write_text(page_html)
        if last_page is None:
            pages = [int(x) for x in re.findall(r"grants-at-a-glance\?page=(\d+)", page_html)]
            last_page = max(pages) if pages else args.max_pages
            log(f"pager: last page = {last_page}")
        parsed = [r for r in (parse_row(c) for c in page_html.split('<div class="views-row ')[1:]) if r]
        if not parsed:
            consecutive_empty += 1
            log(f"page {page}: 0 grants ({consecutive_empty}/{MAX_CONSECUTIVE_EMPTY})")
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                break
            page += 1
            continue
        consecutive_empty = 0
        rows += parsed
        log(f"[{time.time() - t0:5.0f}s] page {page}/{last_page}: +{len(parsed)} -> {len(rows)} grants")
        if args.limit and len(rows) >= args.limit:
            rows = rows[: args.limit]
            break
        page += 1

    df = pd.DataFrame(rows)
    before = len(df)
    df = df.drop_duplicates(subset=["nid"], keep="first")
    log(f"Parsed {before} rows, {before - len(df)} duplicate node ids dropped")
    # Crossref deposits: the site omits the DOI on some grants that MDA did
    # register; recover those by exact (normalised) title when unambiguous.
    xref = crossref_awards()
    log(f"Crossref: {len(xref)} MDA grant deposits")
    by_doi = {x["doi"]: x for x in xref}
    by_title: dict[str, list[dict]] = {}
    for x in xref:
        by_title.setdefault(norm_title(x["title"]), []).append(x)
    claimed = set(df["grant_doi"].dropna())
    via_site = via_title = 0
    awards, dois = [], []
    for doi, title in zip(df["grant_doi"], df["title"]):
        if isinstance(doi, str) and doi:
            x = by_doi.get(doi)
            awards.append(x["award"] if x and x["award"] else re.match(r"10\.55762/mda\.(\d+)\.", doi).group(1)
                          if re.match(r"10\.55762/mda\.(\d+)\.", doi) else None)
            dois.append(doi)
            via_site += 1
            continue
        cands = [x for x in by_title.get(norm_title(title), []) if x["doi"] not in claimed] if title else []
        if len(cands) == 1 and cands[0]["award"]:
            awards.append(cands[0]["award"])
            dois.append(cands[0]["doi"])
            claimed.add(cands[0]["doi"])
            via_title += 1
        else:
            awards.append(None)
            dois.append(None)
    df["crossref_award_number"] = awards
    df["grant_doi"] = dois
    log(f"  award number via site DOI: {via_site}, via Crossref title match: {via_title}, "
        f"Crossref deposits not on site: {len(set(by_doi) - claimed)}")
    df["funder_award_id"] = [
        a if isinstance(a, str) and a else f"MDA-NID-{n}"
        for a, n in zip(df["crossref_award_number"], df["nid"])
    ]
    # The view lists a few grants twice under two node ids (same DOI, title
    # and grantee); keep the first (newest node). Same-DOI rows that differ
    # in title or grantee still fail below.
    key = df["funder_award_id"].str.lower()
    same = df.assign(_k=key).duplicated(subset=["_k", "title", "lead_name"], keep="first")
    for n, a in zip(df.loc[same, "nid"], df.loc[same, "funder_award_id"]):
        log(f"  duplicate listing nid {n} of award {a}; dropped")
    df = df[~same].reset_index(drop=True)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    for c in ["title", "display_title", "start_date", "end_date", "amount", "grant_doi", "lead_family_name",
              "institution", "description", "grant_type"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  total amount USD {df['amount'].sum():,.0f}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "mda_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_mda_projects.parquet"
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
