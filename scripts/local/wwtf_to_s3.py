#!/usr/bin/env python3
"""
Vienna Science and Technology Fund (WWTF) to S3 Data Pipeline
=============================================================

WWTF publishes every funded project in its project database at
https://www.wwtf.at/funding/project-database/ with a bulk export
("open as CSV document", ./csv/): one tab-separated row per project with
project number, title, call, PI, home institution, up to three project
partners, start/end date, amount awarded (EUR), status, disciplines and
keywords. Ladder item 0 (funder's own bulk export).

The export has no abstract, co-PI split or landing URL, so the script also:
  1. reads the 650 listing keys on the database page and resolves them to
     detail-page URLs through the page's own listing endpoint
     (/modules/ajax_gateway.php?act=presult, the same call the page makes);
  2. fetches each detail page (/funding/programmes/<prog>/<number>/) for the
     abstract, PI, co-PIs (name + institution), status, GrantID (Crossref
     grant DOI 10.47379/...) and funding volume.

funder_award_id = the WWTF project number (LS17-012, ICT19-045, VRG14-005...),
exactly the string WWTF deposits as the Crossref grant "award" and the form
citing works write in acknowledgements.

Scope: all WWTF programmes, including the Universitaets-Infrastrukturprogramm
(UIP, research equipment for Vienna universities, no PI; flagged as
funding_type infrastructure), summer schools, chairs, Vienna Research Groups
and the ME/CFS fellowships.

Output: s3://openalex-ingest/awards/wwtf/wwtf_projects.parquet
"""

import argparse
import csv
import html
import io
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O. See runbook section 1.2. (grep marker: sys.stdout.reconfigure)
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

BASE = "https://www.wwtf.at"
DB_URL = f"{BASE}/funding/project-database/?lang=EN"
CSV_URL = f"{BASE}/funding/project-database/csv/?lang=EN"
AJAX_URL = f"{BASE}/modules/ajax_gateway.php?act=presult"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/wwtf/wwtf_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5
RETRIES = 4


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch(url: str, method: str = "GET", data=None) -> requests.Response:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.request(method, url, data=data, headers=HEADERS, timeout=60)
            if r.status_code == 200:
                return r
            last_err = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last_err = e
        log(f"  {method} {url} -> {last_err}; retry {attempt + 1}/{RETRIES}")
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"{method} {url} failed: {last_err}")


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<br\s*/?>|</p>", "\n", fragment)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = "\n".join(re.sub(r"[ \t]+", " ", ln).strip() for ln in t.split("\n"))
    t = re.sub(r"\n{2,}", "\n", t).strip()
    return t or None


HONORIFIC_RE = re.compile(
    r"^(?:(?:univ\.?-?\s*prof|ao\.?\s*univ\.?-?\s*prof|o\.?\s*univ\.?-?\s*prof|prof|professor|priv\.?-?\s*doz"
    r"|dr|di|dipl\.?-?\s*ing|mag|mmag|dr\.?\s*med|dr\.?\s*rer\.?\s*nat|ing|mr|mrs|ms)\.?(?:\s+|$))+",
    re.I,
)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook section 2.4.1 helper (wolf_to_s3.py) with a leading
    academic-title strip ("Univ.-Prof. Dr.") and "Family, Given" support."""
    if not name:
        return None, None
    name = HONORIFIC_RE.sub("", name.strip()).strip(" ,")
    if name.count(",") == 1:
        fam, giv = (p.strip() for p in re.split(r",", name, maxsplit=1))
        if fam and giv and len(fam.split()) <= 3 and len(giv.split()) <= 3:
            return giv, fam
    tokens = name.strip().split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr", "msc", "bsc", "ma", "mba"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def split_people(s: str | None) -> list[str]:
    """'A & B', 'A, B & C', 'A und B', 'A and B' -> list of names.
    A single 'Family, Given' (no conjunction) stays one name."""
    if not s or not s.strip():
        return []
    s = s.strip()
    if re.search(r"\s(?:&|und|and)\s", s):
        parts = re.split(r"\s+(?:&|und|and)\s+|,\s+", s)
        return [p.strip() for p in parts if p.strip()]
    return [s]


def parse_amount(s: str | None) -> float | None:
    """CSV uses German decimal comma (434810,16); a lone ',ddd' with no other
    separator is a thousands separator (93,796)."""
    if s is None or not str(s).strip():
        return None
    s = str(s).strip().replace("€", "").replace(" ", "")
    if re.fullmatch(r"\d{1,3}(,\d{3})+", s):
        return float(s.replace(",", ""))
    if re.fullmatch(r"\d{1,3}(\.\d{3})+(,\d+)?", s):
        return float(s.replace(".", "").replace(",", "."))
    if re.fullmatch(r"\d+(,\d{1,2})?", s):
        return float(s.replace(",", "."))
    if re.fullmatch(r"\d+(\.\d{1,2})?", s):
        return float(s)
    return None


def parse_detail_amount(s: str | None) -> float | None:
    """Detail page uses English format: '€ 999,357' / '€ 434,810.16'."""
    if not s:
        return None
    s = s.replace("€", "").replace(" ", "").strip()
    if re.fullmatch(r"\d{1,3}(,\d{3})*(\.\d+)?", s):
        return float(s.replace(",", ""))
    return None


def crossref_awards() -> dict[str, str]:
    """project number (upper) -> grant DOI from WWTF's Crossref grant deposits
    (funder DOI 10.13039/501100001821, prefix 10.47379)."""
    out, cursor = {}, "*"
    while True:
        r = None
        for attempt in range(RETRIES):
            try:
                r = requests.get(
                    "https://api.crossref.org/works",
                    params={"filter": "type:grant,award.funder:10.13039/501100001821", "rows": 1000,
                            "cursor": cursor, "mailto": "team@ourresearch.org"},
                    headers=HEADERS, timeout=120,
                )
                if r.status_code == 200:
                    break
            except Exception as e:  # noqa: BLE001
                log(f"  Crossref: {e}; retry {attempt + 1}/{RETRIES}")
            time.sleep(3 * (attempt + 1))
        if r is None or r.status_code != 200:
            raise RuntimeError("Crossref grant query failed")
        msg = r.json()["message"]
        for item in msg["items"]:
            award = (item.get("award") or "").strip().upper()
            if award:
                out[award] = item["DOI"].lower()
        if not msg["items"]:
            return out
        cursor = msg["next-cursor"]


def listing_urls() -> dict[str, str]:
    """project number -> detail URL, via the database page's listing keys."""
    page = fetch(DB_URL).text
    keys = re.findall(r'data-pkey="(\d+_[A-Z]{2}_[A-Z])"', page)
    log(f"Database page: {len(keys)} listing keys")
    out = {}
    for i in range(0, len(keys), 25):
        batch = keys[i: i + 25]
        r = fetch(AJAX_URL, method="POST", data=[("obj[]", k) for k in batch])
        for k, frag in r.json().items():
            m = re.search(r'href="([^"]+)">\s*([A-Za-z0-9_\-]+)\s', frag)
            if m:
                out[m.group(2).strip().upper()] = BASE + m.group(1).replace("\\/", "/")
            else:
                log(f"  listing key {k}: no link in {frag[:120]!r}")
        time.sleep(REQUEST_DELAY)
    return out


def parse_detail(page: str) -> dict:
    fields = {}
    for lab, val in re.findall(
        r"<div class='col-md-3 col-sm-4 col-12'><em>([^<]+?):</em></div>\s*"
        r"<div class='col-md-9 col-sm-8 col-12'>(.*?)</div>\s*</div>", page, re.S):
        fields[lab.strip()] = val
    co = []
    for chunk in re.split(r"<br\s*/?>", fields.get("Co-Principal Investigator(s)", "") or ""):
        t = text(chunk)
        if not t:
            continue
        m = re.match(r"^(.*?)\s*\((.*)\)\s*$", t, re.S)
        co.append({"name": (m.group(1) if m else t).strip(), "institution": m.group(2).strip() if m else None})
    art = page[page.find("<article class='row'>"):]
    body = re.search(r"</header>\s*<div class='clearfix'></div>\s*<div class='col-12 mt-3'><hr /></div>\s*"
                     r"<div class='col-12'>(.*?)</div>\s*<div class='mt-3'>", art, re.S)
    grant_id = text(fields.get("GrantID"))
    return {
        "detail_pi": text(fields.get("Principal Investigator")),
        "detail_institution": text(fields.get("Institution")),
        "detail_title": text(fields.get("Project title")),
        "detail_status": text(fields.get("Status")),
        "detail_grant_id": grant_id.lower() if grant_id and grant_id.startswith("10.") else None,
        "detail_amount_text": text(fields.get("Funding volume")),
        "co_investigators": co,
        "description": text(body.group(1)) if body else None,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="WWTF project database -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the section 1.4 shrink guard")
    args = p.parse_args()

    raw = fetch(CSV_URL).content
    try:
        csv_text = raw.decode("utf-8")
    except UnicodeDecodeError:
        csv_text = raw.decode("cp1252")
    # Data rows carry one more field than the header: an always-empty,
    # unlabeled column between "% of 4" and "keywords" (keywords is the LAST
    # field). Parse with csv.reader and name that column explicitly.
    rows = [r for r in csv.reader(io.StringIO(csv_text), delimiter="\t", quoting=csv.QUOTE_NONE) if any(x.strip() for x in r)]
    header, body = rows[0], rows[1:]
    names = header[:-1] + ["unlabeled_blank", header[-1]]
    bad = [r[0] for r in body if len(r) != len(names)]
    if bad:
        raise SystemExit(f"CSV rows with unexpected field count ({len(bad)}): {bad[:10]}")
    df = pd.DataFrame(body, columns=names, dtype=str).replace({"": None})
    df = df.drop(columns=["unlabeled_blank"])
    df.columns = [c.strip().lower().replace(" ", "_").replace("%", "pct") for c in df.columns]
    df = df.rename(columns={"project_number": "project_number_raw", "date_beginn": "csv_start_date",
                            "date_end": "csv_end_date", "grants_awarded": "amount_text"})
    df["project_number"] = df["project_number_raw"].str.strip()
    df = df[df["project_number"].notna() & (df["project_number"] != "")]
    log(f"CSV export: {len(df)} projects, columns {list(df.columns)}")
    if len(df) < 500:
        raise SystemExit(f"CSV export returned only {len(df)} rows; expected ~650")

    urls = listing_urls()
    log(f"Listing: {len(urls)} detail URLs")
    if args.limit:
        df = df.head(args.limit)

    details = []
    for i, num in enumerate(df["project_number"], 1):
        url = urls.get(num.upper())
        rec = {"landing_page_url": url}
        if url:
            cache = args.cache_dir / f"{re.sub(r'[^A-Za-z0-9_-]', '_', num)}.html" if args.cache_dir else None
            if cache and cache.exists():
                page = cache.read_text()
            else:
                page = fetch(url + "?lang=EN").text
                if cache:
                    args.cache_dir.mkdir(parents=True, exist_ok=True)
                    cache.write_text(page)
                time.sleep(REQUEST_DELAY)
            rec.update(parse_detail(page))
        details.append(rec)
        if i % 50 == 0:
            log(f"  {i}/{len(df)} detail pages")
    det = pd.DataFrame(details, index=df.index)
    df = pd.concat([df, det], axis=1)

    # Lead PI: detail page anchor text (cleanest), else the CSV string.
    leads, lead_given, lead_family, extra = [], [], [], []
    for d_pi, c_pi in zip(df.get("detail_pi"), df["principal_investigator"]):
        names = split_people(d_pi if isinstance(d_pi, str) and d_pi else (c_pi if isinstance(c_pi, str) else None))
        lead = names[0] if names else None
        g, f = split_name(lead) if lead else (None, None)
        leads.append(lead)
        lead_given.append(g)
        lead_family.append(f)
        extra.append(names[1:])
    df["lead_name"], df["lead_given_name"], df["lead_family_name"] = leads, lead_given, lead_family
    df["lead_institution"] = df["detail_institution"].where(df["detail_institution"].notna(), df["home_institution_pi"])

    people = []
    for lead, g, f, inst, ex, co in zip(df["lead_name"], df["lead_given_name"], df["lead_family_name"],
                                        df["lead_institution"], extra, df["co_investigators"]):
        ppl = []
        if lead:
            ppl.append({"role": "PI", "name": lead, "given_name": g, "family_name": f, "institution": inst})
        for n in ex:
            gg, ff = split_name(n)
            ppl.append({"role": "PI", "name": n, "given_name": gg, "family_name": ff, "institution": inst})
        for c in (co if isinstance(co, list) else []):
            gg, ff = split_name(c["name"])
            ppl.append({"role": "Co-PI", "name": c["name"], "given_name": gg, "family_name": ff,
                        "institution": c["institution"]})
        people.append(ppl)
    df["people"] = [json.dumps(p, ensure_ascii=False) for p in people]
    df = df.drop(columns=["co_investigators"])

    # The CSV export is authoritative; the detail page's rounded "Funding volume"
    # only fills gaps. (UIP09-ak_bild: CSV "93,796" = EUR 93,796, i.e. 2 x the
    # academy's usual 46,898 UIP share; the detail page renders it as "EUR 94".)
    df["amount"] = df["amount_text"].map(parse_amount)
    det_amt = df["detail_amount_text"].map(parse_detail_amount)
    mism = (det_amt.notna() & df["amount"].notna() & ((det_amt - df["amount"]).abs() > 1))
    for n, a, b in zip(df.loc[mism, "project_number"], df.loc[mism, "amount"], det_amt[mism]):
        log(f"  amount mismatch {n}: csv {a} vs detail {b}; keeping csv")
    df["amount"] = df["amount"].where(df["amount"].notna(), det_amt)

    # Grant DOI: only WWTF's own Crossref deposits (award == project number).
    # Newer projects print a GrantID (10.47379/LS25090) on the detail page before
    # it is registered (doi.org 404), so that value is kept as detail_grant_id only.
    xref = crossref_awards()
    log(f"Crossref: {len(xref)} WWTF grant deposits")
    df["grant_doi"] = [xref.get(n.upper()) for n in df["project_number"]]
    log(f"  Crossref deposits not in CSV: {sorted(set(xref) - set(df['project_number'].str.upper()))[:20]}")
    df["currency"] = df["amount"].map(lambda a: "EUR" if pd.notna(a) else None)
    df["title"] = df["project_title"].where(df["project_title"].notna(), df.get("detail_title"))
    df["call"] = df["call"].str.strip()
    df["funder_award_id"] = df["project_number"]

    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    missing_url = df["landing_page_url"].isna().sum()
    log(f"Projects without a listing URL: {missing_url}")
    for c in ["title", "description", "csv_start_date", "csv_end_date", "amount", "grant_doi",
              "lead_family_name", "lead_institution", "landing_page_url"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  total amount EUR {df['amount'].sum():,.0f}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "wwtf_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook section 1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_wwtf_projects.parquet"
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(df)}")
        if len(df) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound", "403"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
