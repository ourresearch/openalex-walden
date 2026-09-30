#!/usr/bin/env python3
"""
Canada First Research Excellence Fund (CFREF) to S3 Data Pipeline
=================================================================

CFREF (administered by the Tri-agency Institutional Programs Secretariat)
publishes the results of each of its three competitions on
https://www.cfref-apogee.gc.ca/results-resultats/:

    competition_1-eng.aspx   Inaugural Competition 1 (5 awards, announced July 2015)
    competition_2-eng.aspx   Inaugural Competition 2 (13 awards, announced Sept 2016)
    index-eng.aspx           2022 Competition        (11 awards, announced April 2023)

Each award block gives the lead institution, "Award amount: $N", the
initiative title, partner institutions (2022 only), a summary, and a link to
a full abstract page. CFREF awards go to institutions, not to a named PI, so
lead_investigator carries only the affiliation (institution). Method 5
(static HTML); ladder item 0 checked 2026-09-30: no export on the site, no
CFREF dataset on open.canada.ca.

Announcement dates come from the fund's own press-release index
(news_room-salle_de_presse/press_releases-communiques/index-eng.aspx):
Competition 1 has one release per institution (2015-07-28..31); Competition 2
one release ("$900 million", 2016-09-06); 2022 one release ("$1.4 billion",
2023-04-28). These are shipped as start_date.

funder_award_id (runbook §2.1.1): works citing CFREF (F4320326644) write the
grant number, "CFREF-2015-00013" / "CFREF-2022-00042" (the dominant form in
crossref_work_funders / crossref_work.grants for this funder). The CFREF site
does not print it, so AWARD_NUMBERS below maps (competition, lead institution)
-> number. Source: the NSERC Awards open data (open.canada.ca), which carries
every CFREF award under application ids cfref-2014-0000N (Competition 1),
cfref-2015-000NN (Competition 2) and cfref-2022-000NN (2022 Competition);
exactly 5 + 13 + 11 = 29 ids, one per (competition, institution), matching the
29 awards on the CFREF site one-to-one (checked 2026-09-30 against
openalex.awards.openalex_awards_raw provenance nserc_open_data).

Output: s3://openalex-ingest/awards/cfref/cfref_projects.parquet
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
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). No-op on
# Linux/Databricks. See runbook §1.2.
import sys as _sys_utf8  # _sys_utf8 is sys: this block is sys.stdout.reconfigure(...) + file-I/O utf-8 defaults
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

BASE = "https://www.cfref-apogee.gc.ca"
COMPETITIONS = [
    # (competition label, results page, award-number year)
    ("Inaugural Competition 1", "/results-resultats/competition_1-eng.aspx", "2014"),
    ("Inaugural Competition 2", "/results-resultats/competition_2-eng.aspx", "2015"),
    ("2022 Competition", "/results-resultats/index-eng.aspx", "2022"),
]
PRESS_INDEX = "/news_room-salle_de_presse/press_releases-communiques/index-eng.aspx"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/cfref/cfref_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5
RETRIES = 3

# (award-number year, institution key) -> sequence number. See module docstring.
AWARD_NUMBERS = {
    ("2014", "laval"): 1, ("2014", "saskatchewan"): 2, ("2014", "sherbrooke"): 3,
    ("2014", "toronto"): 4, ("2014", "british columbia"): 5,
    ("2015", "alberta"): 1, ("2015", "calgary"): 2, ("2015", "dalhousie"): 3,
    ("2015", "guelph"): 4, ("2015", "laurentian"): 5, ("2015", "mcgill"): 6,
    ("2015", "universite de montreal"): 7, ("2015", "polytechnique"): 8,
    ("2015", "queen's"): 9, ("2015", "saskatchewan"): 10, ("2015", "waterloo"): 11,
    ("2015", "western"): 12, ("2015", "york"): 13,
    ("2022", "dalhousie"): 2, ("2022", "ottawa"): 7, ("2022", "york"): 10,
    ("2022", "calgary"): 15, ("2022", "toronto metropolitan"): 22, ("2022", "concordia"): 41,
    ("2022", "university of toronto"): 42, ("2022", "mcgill"): 45, ("2022", "victoria"): 48,
    ("2022", "universite de montreal"): 51, ("2022", "memorial"): 62,
}

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], 1)}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(path: str) -> str:
    url = path if path.startswith("http") else BASE + path
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            r.raise_for_status()
            r.encoding = "utf-8"
            time.sleep(REQUEST_DELAY)
            return r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<br\s*/?>|</p>|</li>", "\n", fragment)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"[ \t\r\f\v]+", " ", t)
    t = re.sub(r"\s*\n\s*", "\n", t).strip()
    return t or None


def fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", s.replace("’", "'")).strip()


def award_number(year: str, institution: str) -> str:
    inst = fold(institution)
    hits = [(k, n) for (y, k), n in AWARD_NUMBERS.items() if y == year and k in inst]
    # "university of toronto" must not also claim "toronto metropolitan university"
    if len(hits) > 1:
        hits = [h for h in hits if len(h[0]) == max(len(x[0]) for x in hits)]
    if len(hits) != 1:
        raise SystemExit(f"no unique CFREF award number for ({year}, {institution!r}): {hits}")
    return f"CFREF-{year}-{hits[0][1]:05d}"


def parse_results(page: str) -> list[dict]:
    """One award per id-carrying h2/h3/h4 heading (the three competition pages
    use h4, h3 and h2 respectively)."""
    heads = list(re.finditer(r'<h([234])[^>]*\bid="[^"]+"[^>]*>(.*?)</h\1>', page, re.S))
    out = []
    for i, m in enumerate(heads):
        end = heads[i + 1].start() if i + 1 < len(heads) else page.find("Date modified", m.end())
        block = page[m.end(): end if end > 0 else None]
        t = text(block) or ""
        amt = re.search(r"Award amount:\s*\$\s*([\d,]+)", t, re.I)
        if not amt:
            continue  # a heading that is not an award (e.g. section title)
        title = re.search(r"Title:\s*(.+)", t)
        partners = re.search(r"Partner institutions:\s*(.+)", t)
        paras = [text(p) for p in re.findall(r"<p(?:\s[^>]*)?>(.*?)</p>", block, re.S)]
        summary = next((p for p in paras if p and len(p) > 120 and not re.match(r"(Award|Title|Partner)", p)), None)
        abstract = re.search(r'href="([^"]*abstracts-resumes[^"]*)"', block)
        out.append({
            "institution": text(m.group(2)),
            "amount_text": amt.group(0),
            "amount": float(amt.group(1).replace(",", "")),
            "title": title.group(1).strip() if title else None,
            "partner_institutions": partners.group(1).strip() if partners else None,
            "summary": summary,
            "abstract_url": BASE + abstract.group(1) if abstract else None,
        })
    return out


def parse_abstract(page: str) -> str | None:
    main = re.search(r'<main[^>]*>(.*?)</main>', page, re.S)
    body = main.group(1) if main else page
    body = body.split("Date modified")[0]
    paras = [text(p) for p in re.findall(r"<p(?:\s[^>]*)?>(.*?)</p>", body, re.S)]
    paras = [p for p in paras if p and len(p) > 40]
    return "\n\n".join(paras) or None


def press_dates() -> list[tuple[str, str, str]]:
    """(href, title, YYYY-MM-DD) from the fund's press-release index."""
    page = get(PRESS_INDEX)
    out = []
    for href, title, date in re.findall(
            r'<a href="([^"]+)"[^>]*>(.*?)</a>\s*<br\s*/?>\s*<span class="font-small bold">([^<]+)</span>', page, re.S):
        d = re.match(r"([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})", date.strip())
        if d and d.group(1).lower() in MONTHS:
            out.append((href, text(title) or "", f"{d.group(3)}-{MONTHS[d.group(1).lower()]:02d}-{int(d.group(2)):02d}"))
    return out


def announcement_date(year: str, institution: str, releases) -> str | None:
    if year == "2014":  # one release per institution, file named after it
        key = re.sub(r"^the ", "", fold(institution)).replace(" ", "_")
        hits = [d for h, _, d in releases
                if "/2015/" in h and fold(h.rsplit("/", 1)[-1]).removesuffix("-eng.aspx") == key]
    elif year == "2015":
        hits = [d for _, t, d in releases if "$900 million" in t]
    else:
        hits = [d for _, t, d in releases if "$1.4 billion" in t]
    return hits[0] if len(hits) == 1 else None


def main() -> None:
    p = argparse.ArgumentParser(description="CFREF competition results -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only keep the first N awards (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    releases = press_dates()
    log(f"Press releases: {len(releases)} dated entries")
    rows = []
    for label, path, year in COMPETITIONS:
        awards = parse_results(get(path))
        log(f"{label}: {len(awards)} awards on {path}")
        if not awards:
            raise SystemExit(f"no awards parsed from {path}; page layout changed?")
        for a in awards:
            a["competition"] = label
            a["competition_year"] = year
            a["funder_award_id"] = award_number(year, a["institution"])
            a["announcement_date"] = announcement_date(year, a["institution"], releases)
            a["results_page_url"] = BASE + path
            rows.append(a)
    if args.limit:
        rows = rows[: args.limit]
    for a in rows:
        a["abstract"] = parse_abstract(get(a["abstract_url"])) if a["abstract_url"] else None

    df = pd.DataFrame(rows)
    df["currency"] = "CAD"
    df["partner_institutions_json"] = df["partner_institutions"].map(
        lambda s: json.dumps([x.strip() for x in s.split(";") if x.strip()], ensure_ascii=False) if s else None)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    if not args.limit and len(df) != len(AWARD_NUMBERS):
        raise SystemExit(f"parsed {len(df)} awards but the crosswalk has {len(AWARD_NUMBERS)}")
    for c in ["title", "amount", "summary", "abstract", "announcement_date", "partner_institutions"]:
        log(f"  {c:22s} {df[c].notna().mean():6.1%}")
    log(f"  total amount CAD {df['amount'].sum():,.0f}")

    df["amount"] = df["amount"].map(lambda v: f"{v:.0f}")
    df = df.astype("string")  # runbook §1.2 item 5
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "cfref_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_cfref_projects.parquet"
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
