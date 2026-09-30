#!/usr/bin/env python3
"""
PRIN (Progetti di Rilevante Interesse Nazionale) to S3 Data Pipeline
====================================================================

Italy's national PRIN grant programme, run by the research ministry
(MURST 1999-2000, MIUR 2001-2019 apart from the 2006-2008 MUR interlude,
MUR since 2020). The ministry's PRIN portal (CINECA-hosted) at
https://prin.mur.gov.it/ has a PUBLIC project search ("Cerca un progetto",
no login) covering the calls 1999-2020:

    list:   /Ricerca?Filtro.Anno=<year>&Filtro.Ateneo=%&page=<n>   (10 rows/page)
    detail: /Progetti/GetDettaglio?codice=<protocol code>           (HTML fragment)

The detail fragment carries: Codice (protocol code, e.g. 2017FJCPEX),
Ente (lead institution), Coordinatore/PI, Descrizione (title),
Finanziamento assegnato (EUR; older calls already converted from lire),
Durata in mesi, Area/Settore (ERC panel or CUN area), Abstract, and the
per-unit breakdown (Sede N: Ente / Responsabile / Finanziamento).
Only public fields are used; the researcher login area is not touched.

Not covered: PRIN 2022 / PRIN 2022 PNRR / PRIN 2024 are not in the portal
search (their results are PDF decree annexes on mur.gov.it).

Method 5 (static HTML) on the runbook ladder; no bulk export exists.

`funder_award_id` = the protocol code, which is what researchers cite
("PRIN 2017 prot. 2017FJCPEX", "2010BX2SNA_002" = code + unit suffix).

Output: s3://openalex-ingest/awards/prin/prin_projects.parquet
"""

import argparse
import html
import json
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
# (= sys.stdout.reconfigure(encoding="utf-8", line_buffering=True) + utf-8 file I/O.)
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

BASE = "https://prin.mur.gov.it"
YEARS = ["2020", "2017", "2015", "2012", "2010-2011", "2009", "2008", "2007", "2006",
         "2005", "2004", "2003", "2002", "2001", "2000", "1999"]
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/prin/prin_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
RETRIES = 4
MAX_CONSECUTIVE_EMPTY = 3
MAX_CONSECUTIVE_NON200 = 5
WORKERS = 4

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, params: dict | None = None) -> requests.Response | None:
    last = None
    for attempt in range(RETRIES):
        try:
            r = SESSION.get(url, params=params, timeout=90)
            if r.status_code == 200:
                r.encoding = "utf-8"
                return r
            last = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last = str(e)
        time.sleep(2 * (attempt + 1))
    log(f"  GET {url} {params or ''} failed: {last}")
    return None


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("​", "").replace("﻿", "")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def list_year(year: str, cache_dir: Path | None, limit: int | None = None) -> list[dict]:
    """Paginate the public search for one call year. Terminator: the portal's
    reported total ("Risultati 1-10 di N") when plausible, otherwise
    MAX_CONSECUTIVE_EMPTY pages in a row with no new codes (runbook Step 1 #6:
    one empty/duplicate page is not by itself end-of-corpus)."""
    rows, seen = [], set()
    page, empty, non200, total = 1, 0, 0, None
    while True:
        cache = cache_dir / f"list_{year}_{page}.html" if cache_dir else None
        if cache and cache.exists():
            body = cache.read_text()
        else:
            r = get(f"{BASE}/Ricerca", {"Filtro.Anno": year, "Filtro.Ateneo": "%", "page": page})
            if r is None:
                non200 += 1
                print(f"  {year} page {page}: failed ({non200}/{MAX_CONSECUTIVE_NON200}); continuing")
                if non200 >= MAX_CONSECUTIVE_NON200:
                    raise RuntimeError(f"{year}: {non200} failed list pages in a row at page {page}")
                page += 1
                continue
            non200 = 0
            body = r.text
            if cache:
                cache.write_text(body)
        if total is None:
            m = re.search(r"Risultati\s+\d+-\d+\s+di\s+(\d+)", body)
            total = int(m.group(1)) if m else None
        new = 0
        for tr in re.findall(r"<tr>(.*?)</tr>", body, re.S):
            code = re.search(r'data-id="([^"]+)"', tr)
            if not code:
                continue
            tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
            c = code.group(1).strip()
            if c in seen:
                continue
            seen.add(c)
            new += 1
            rows.append({"code": c, "list_ente": text(tds[1]) if len(tds) > 1 else None,
                         "list_pi": text(tds[2]) if len(tds) > 2 else None,
                         "list_title": text(tds[3]) if len(tds) > 3 else None,
                         "list_year": text(tds[4]) if len(tds) > 4 else None})
        if new == 0:
            empty += 1
            if empty >= MAX_CONSECUTIVE_EMPTY:
                break
        else:
            empty = 0
        if total and total > 10 and len(rows) >= total:
            break
        if limit and len(rows) >= limit:
            break
        if page % 20 == 0:
            log(f"  {year}: page {page}, {len(rows)} codes (portal total {total})")
        page += 1
    log(f"  {year}: {len(rows)} codes over {page} pages (portal total {total})")
    return rows


SUFFIXES = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}


def split_surname_first(name: str | None) -> tuple[str | None, str | None]:
    """PRIN writes names as 'SURNAME Given' with the surname upper-case
    ("PAOLUCCI Francesco", "DE ROSA Maria Luisa", "GARFI' Vittorio"). Leading
    all-caps tokens are the family name, the rest the given name. Trailing
    degree/suffix tokens are dropped with the canonical runbook §2.4.1 set.
    (wolf_to_s3.split_name assumes 'Given Family' order, so it can't be
    reused verbatim for this source.)"""
    if not name:
        return None, None
    tokens = name.replace(",", " ").split()
    while tokens and tokens[-1].lower().strip(",.") in SUFFIXES:
        tokens.pop()
    if not tokens:
        return None, None
    fam = []
    for t in tokens[:-1]:  # always leave at least one token for the given name
        letters = re.sub(r"[^A-Za-zÀ-ÿ]", "", t)
        if letters and letters == letters.upper():
            fam.append(t)
        else:
            break
    if not fam:
        if len(tokens) == 1:
            return None, tokens[0].title()
        fam = tokens[:1]
    given = tokens[len(fam):]
    family = " ".join(fam).title()
    return (" ".join(given) or None), family


def money(s: str | None) -> float | None:
    if not s:
        return None
    d = re.sub(r"[^\d.]", "", s.replace(",", ""))
    try:
        return float(d) if d else None
    except ValueError:
        return None


def li_value(body: str, label: str) -> str | None:
    """<li><strong>Label</strong>: value</li> or <li><strong>Label:</strong> value</li>"""
    m = re.search(r"<li>\s*<strong[^>]*>\s*" + re.escape(label) + r"\s*:?\s*</strong>\s*:?(.*?)</li>", body, re.S)
    return text(m.group(1)) if m else None


def parse_detail(code: str, body: str) -> dict:
    year = re.search(r"ANNO\s+(\d{4}(?:-\d{4})?)", body)
    abstract = re.search(r">\s*Abstract\s*</button>.*?<div class=\"collapse-body\">(.*?)</div>", body, re.S)
    units = []
    for n, blk in re.findall(r">\s*Sede\s+(\d+)\s*</button>.*?<div class=\"collapse-body\">(.*?)</div>", body, re.S):
        ente = re.search(r"Ente:\s*(.*?)<br", blk, re.S)
        resp = re.search(r"Responsabile:\s*(.*?)<br", blk, re.S)
        fin = re.search(r"Finanziamento:\s*(.*?)<br", blk, re.S)
        g, f = split_surname_first(text(resp.group(1)) if resp else None)
        units.append({"unit": int(n), "institution": text(ente.group(1)) if ente else None,
                      "name": text(resp.group(1)) if resp else None, "given_name": g, "family_name": f,
                      "amount": money(text(fin.group(1)) if fin else None)})
    pi = li_value(body, "Coordinatore /PI")
    g, f = split_surname_first(pi)
    amount_text = li_value(body, "Finanziamento assegnato")
    amount = money(amount_text)
    return {
        "code": code,
        "detail_code": li_value(body, "Codice"),
        "call_year": year.group(1) if year else None,
        "institution": li_value(body, "Ente"),
        "pi_name": pi,
        "lead_given_name": g,
        "lead_family_name": f,
        "title": li_value(body, "Descrizione"),
        "abstract": text(abstract.group(1)) if abstract else None,
        "amount_text": amount_text,
        "amount": amount,
        "currency": "EUR" if amount is not None else None,
        "duration_months": re.sub(r"[^\d]", "", li_value(body, "Durata in mesi") or "") or None,
        "area": li_value(body, "Area/Settore"),
        "units": json.dumps(units, ensure_ascii=False),
        "n_units": len(units),
        "landing_page_url": f"{BASE}/Progetti/GetDettaglio?codice={code}",
    }


def fetch_detail(code: str, cache_dir: Path | None) -> tuple[str, str | None]:
    cache = cache_dir / f"d_{re.sub(r'[^A-Za-z0-9_-]', '_', code)}.html" if cache_dir else None
    if cache and cache.exists():
        return code, cache.read_text()
    r = get(f"{BASE}/Progetti/GetDettaglio", {"codice": code})
    if r is None or "Codice" not in r.text:
        return code, None
    if cache:
        cache.write_text(r.text)
    return code, r.text


def main() -> None:
    p = argparse.ArgumentParser(description="MUR PRIN public project search -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="smoke test: first N codes of each year")
    p.add_argument("--years", default=None, help="comma list of call years (default: all)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()
    if args.cache_dir:
        args.cache_dir.mkdir(parents=True, exist_ok=True)

    years = args.years.split(",") if args.years else YEARS
    listed = []
    for y in years:
        rows = list_year(y, args.cache_dir, args.limit)
        listed += rows[: args.limit] if args.limit else rows
    lst = pd.DataFrame(listed).drop_duplicates("code")
    log(f"Listed {len(lst)} distinct protocol codes")

    recs, failed, t0 = [], [], time.time()
    with ThreadPoolExecutor(WORKERS) as ex:
        for i, (code, body) in enumerate(ex.map(lambda c: fetch_detail(c, args.cache_dir), lst["code"]), 1):
            if body is None:
                failed.append(code)
            else:
                recs.append(parse_detail(code, body))
            if i % 250 == 0:
                el = time.time() - t0
                log(f"  details {i}/{len(lst)} ({len(failed)} failed) ETA {el / i * (len(lst) - i) / 60:.1f} min")
    if len(failed) > 0.01 * len(lst):
        raise SystemExit(f"{len(failed)} detail fetches failed (>1%); rerun (cache resumes): {failed[:20]}")
    for c in failed:
        log(f"  detail failed: {c}")

    df = pd.DataFrame(recs).merge(lst, on="code", how="left")
    # detail fields are authoritative; fall back to the list row
    df["title"] = df["title"].fillna(df["list_title"])
    df["pi_name"] = df["pi_name"].fillna(df["list_pi"])
    df["institution"] = df["institution"].fillna(df["list_ente"])
    df["call_year"] = df["call_year"].fillna(df["list_year"])
    bad = df["detail_code"].notna() & (df["detail_code"] != df["code"])
    if bad.any():
        raise SystemExit(f"detail code mismatch: {df.loc[bad, ['code', 'detail_code']].head().to_dict('records')}")
    dupes = df["code"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'code'].tolist()[:20]}")
    df["funder_award_id"] = df["code"]
    # the portal's abstract is usually just the title again; keep it only when it adds text
    df["description"] = [a if a and a != t else None for a, t in zip(df["abstract"], df["title"])]

    log(f"Parsed {len(df)} projects; by call year:")
    for y, n in df["call_year"].value_counts().sort_index().items():
        log(f"  {y}: {n}")
    for c in ["title", "description", "amount", "pi_name", "lead_family_name", "institution",
              "duration_months", "area"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  total amount EUR {df['amount'].sum():,.0f}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "prin_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_prin_projects.parquet"
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(df)}")
        if len(df) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(df)}); "
                             "rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
