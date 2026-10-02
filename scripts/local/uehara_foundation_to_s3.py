#!/usr/bin/env python3
"""
Uehara Memorial Foundation (上原記念生命科学財団) grants to S3
==============================================================

The Uehara Memorial Foundation (OpenAlex F4320310760, Tokyo) runs a research
report search system (研究報告書検索システム, https://www.ueharazaidan.or.jp/report_search/)
holding the final report of every research grant, research-encouragement grant,
special research-promotion grant and overseas fellowship it has funded since
1985. An empty search (POST fl=search) returns all of them, 25 per page; each
hit carries the theme, report year (報告年度), researcher + affiliation, grant
kind (助成金の種類), field (領域／部門), and a link to the report PDF
(report_search/pdf/<id>.pdf). Method 5 (static HTML) on the runbook ladder;
there is no bulk export. (The tracker's josei_juryousha.html URL is dead; the
new site's grantee lists at /grant/grantor.html are per-year PDFs.)

The PDF id is the key to the grant:
  * reports up to report year 2017: the id IS the foundation's grant number
    {grant year}{kind digit}{4-digit serial} (1 研究奨励金, 2 研究助成金,
    3/4 海外留学助成金, 9 研究推進特別奨励金) -- the form researchers cite
    (e.g. 201320273). Grant year = the id's first 4 digits.
  * reports from report year 2018: the id is a report serial
    {report year}0{serial} and the grant number is not published. These ship
    with a synthetic funder_award_id UEHARA-RPT-{pdf id}, and the grant year is
    looked up by researcher name in the foundation's own per-year grantee lists
    (grantor.html PDFs, FY2015+) of the same kind, 1-4 years before the report
    (typically 2). No unambiguous match -> grant year NULL.

Amounts are not published per grant (fixed per kind and year in the call
documents), so none are shipped.

Output: s3://openalex-ingest/awards/uehara_foundation/uehara_foundation_projects.parquet
"""

import argparse
import hashlib
import html
import io
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Same block as twcf_to_s3.py (runbook §1.2 item 7); the §4.0 grep looks for
# sys.stdout.reconfigure, which this shim calls as _sys_utf8.stdout.reconfigure.
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

SITE = "https://www.ueharazaidan.or.jp"
SEARCH_URL = f"{SITE}/report_search/"
GRANTOR_URL = f"{SITE}/grant/grantor.html"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/uehara_foundation/uehara_foundation_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5
RETRIES = 4
LAST_GRANT_NUMBER_REPORT_YEAR = 2017

# report kind -> (funding_type, grantee-list PDF basename stems of the same kind)
KINDS = {
    "研究助成": ("research", ("joseikin",)),
    "研究奨励": ("research", ("shoreikin",)),
    "研究推進特別奨励": ("research", ("tokubetushorei", "suishin")),
    "海外助成": ("fellowship", ("research", "posdoc", "postdoc", "kaigai", "wakate-kaigai")),
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch(s: requests.Session, method: str, url: str, **kw) -> requests.Response:
    last = None
    for attempt in range(RETRIES):
        try:
            r = s.request(method, url, timeout=120, **kw)
            r.raise_for_status()
            return r
        except Exception as e:  # noqa: BLE001
            last = e
            log(f"  {method} {url} failed ({e}); retry {attempt + 1}/{RETRIES}")
            time.sleep(4 * (attempt + 1))
    raise RuntimeError(f"{method} {url} failed: {last}")


def norm(s: str | None) -> str:
    """NFKC (the grantee PDFs use CJK compatibility radicals, e.g. U+2F24 for 大) and no spaces."""
    return re.sub(r"\s", "", unicodedata.normalize("NFKC", s or ""))


def search_page(s: requests.Session, page: int) -> str:
    data = {"fl": "search", "page": str(page), "theme": "", "author": "", "keyword": "",
            "year1": "", "year2": "", "allword": ""}
    r = fetch(s, "POST", SEARCH_URL, data=data)
    r.encoding = "utf-8"
    return r.text


def parse_page(page_html: str) -> list[dict]:
    rows = []
    for b in page_html.split('class="report_search_result_line">')[1:]:
        pdf = re.search(r"href='pdf/(\d+)\.pdf'", b)
        title = re.search(r'report_search_result_title">(.*?)</div>', b, re.S)
        author = re.search(r'report_search_result_author">(.*?)</div>', b, re.S)
        spans = re.findall(r"<span>([^<]*)</span>", b)
        t = html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", title.group(1)))).strip() if title else ""
        m = re.search(r"\s*[（(](\d{4})[）)]\s*$", t)
        rows.append({
            "pdf_id": pdf.group(1) if pdf else None,
            "title": (t[: m.start()] if m else t).strip() or None,
            "report_year": m.group(1) if m else None,
            "author_raw": html.unescape(re.sub(r"\s+", " ", author.group(1))).strip() if author else None,
            "kind": spans[0].strip() if spans else None,
            "field": spans[1].strip() if len(spans) > 1 and spans[1].strip() else None,
        })
    return rows


def split_author(raw: str | None) -> tuple[str | None, str | None, str | None, str | None]:
    """'安部　力 （岐阜大学 大学院医学系研究科）' -> (name, given, family, affiliation).
    Names are FAMILY<full-width space>GIVEN; unsplittable -> NULL rather than a guess."""
    if not raw:
        return None, None, None, None
    m = re.match(r"^(.*?)\s*[（(](.*)[）)]\s*$", raw)
    name, affil = (m.group(1), m.group(2)) if m else (raw, None)
    affil = re.sub(r"\s+", " ", affil).strip() if affil else None
    tokens = [t for t in re.split(r"[\s　]+", name.strip()) if t]
    if len(tokens) >= 2 and all(re.fullmatch(r"[A-Za-z.\-']+", t) for t in tokens):
        suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
        while len(tokens) > 1 and tokens[-1].lower().strip(",.") in suffixes:
            tokens.pop()
        given, family = " ".join(tokens[:-1]) or None, tokens[-1]
    elif len(tokens) >= 2:
        given, family = " ".join(tokens[1:]), tokens[0]
    else:
        given, family = None, None
    return " ".join(tokens) or None, given, family, affil or None


def grantee_lists(s: requests.Session, cache_dir: Path) -> dict[tuple[int, str], str]:
    """(grant year, pdf stem) -> NFKC-normalised text of the foundation's grantee-list PDF."""
    import pypdf
    page = fetch(s, "GET", GRANTOR_URL).text
    urls = sorted(set(re.findall(r'href="([^"]+/(\d{4})_?([a-z\-]+)\.pdf)"', page)))
    out = {}
    for url, year, stem in urls:
        if not any(stem in stems for _, stems in KINDS.values()):
            continue
        path = cache_dir / f"list_{year}_{stem}.pdf"
        if not path.exists():
            path.write_bytes(fetch(s, "GET", url).content)
            time.sleep(REQUEST_DELAY)
        text = "".join(p.extract_text() or "" for p in pypdf.PdfReader(io.BytesIO(path.read_bytes())).pages)
        out[(int(year), stem)] = norm(text)
    log(f"Grantee lists: {len(out)} PDFs, years {min(y for y, _ in out)}-{max(y for y, _ in out)}")
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Uehara Memorial Foundation report search -> grants parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the first N result pages (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache result pages / PDFs here")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()
    cache = args.cache_dir or (args.output_dir / "uehara_cache")
    cache.mkdir(parents=True, exist_ok=True)

    s = requests.Session()
    s.headers.update(HEADERS)
    first = search_page(s, 1)
    m = re.search(r'report_search_result_count">([\d,]+)<', first) or re.search(r"([\d,]+)\s*件ヒット", first)
    hits = int(m.group(1).replace(",", "")) if m else None
    per_page = len(parse_page(first)) or 25
    pages = -(-hits // per_page) if hits else None
    if not pages:
        raise RuntimeError("could not read the hit count from the report search")
    log(f"Report search: {hits} hits, {pages} pages of {per_page}")
    if args.limit:
        pages = min(pages, args.limit)

    rows, consecutive_empty = [], 0
    for pg in range(1, pages + 1):
        path = cache / f"page_{pg}.html"
        if path.exists():
            page_html = path.read_text()
        else:
            page_html = first if pg == 1 else search_page(s, pg)
            time.sleep(REQUEST_DELAY)
        got = parse_page(page_html)
        if not got:
            consecutive_empty += 1
            log(f"  page {pg}: no rows ({consecutive_empty}/3); continuing")
            if consecutive_empty >= 3:
                raise RuntimeError(f"3 consecutive empty result pages at page {pg} of {pages}")
            continue
        consecutive_empty = 0
        path.write_text(page_html)
        rows += got
        if pg % 25 == 0:
            log(f"  page {pg}/{pages}: {len(rows)} reports")
    df = pd.DataFrame(rows)
    log(f"Parsed {len(df)} reports (site says {hits})")
    if not args.limit and len(df) < hits:
        raise RuntimeError(f"parsed {len(df)} < {hits} hits; refusing to ship a truncated corpus")

    lists = grantee_lists(s, cache)
    df["report_year_i"] = pd.to_numeric(df["report_year"], errors="coerce")
    pdf_year = pd.to_numeric(df["pdf_id"].str[:4], errors="coerce")
    df["is_grant_number"] = df["pdf_id"].notna() & (df["report_year_i"] <= LAST_GRANT_NUMBER_REPORT_YEAR) & (pdf_year < df["report_year_i"])
    parts = df["author_raw"].map(split_author)
    df["researcher_name"] = parts.map(lambda x: x[0])
    df["lead_given_name"] = parts.map(lambda x: x[1])
    df["lead_family_name"] = parts.map(lambda x: x[2])
    df["affiliation"] = parts.map(lambda x: x[3])

    grant_year, source = [], []
    for r in df.itertuples():
        if r.is_grant_number:
            grant_year.append(int(r.pdf_id[:4]))
            source.append("grant_number")
            continue
        stems = KINDS.get(r.kind, (None, ()))[1]
        key = norm(r.researcher_name)
        cands = []
        if key and pd.notna(r.report_year_i):
            ry = int(r.report_year_i)
            cands = [ry - lag for lag in (1, 2, 3, 4) if any(key in lists.get((ry - lag, st), "") for st in stems)]
        if pd.notna(r.report_year_i) and int(r.report_year_i) - 2 in cands:
            grant_year.append(int(r.report_year_i) - 2)
            source.append("grantee_list")
        elif len(cands) == 1:
            grant_year.append(cands[0])
            source.append("grantee_list")
        else:
            grant_year.append(None)
            source.append(None)
    df["grant_year"] = pd.array(grant_year, dtype="Int64")
    df["grant_year_source"] = source

    def award_id(r) -> str:
        if r.is_grant_number:
            return r.pdf_id
        if r.pdf_id:
            return f"UEHARA-RPT-{r.pdf_id}"
        basis = norm(f"{r.researcher_name}|{r.title}")
        return f"UEHARA-{r.report_year or 'NA'}-{hashlib.sha1(basis.encode('utf-8')).hexdigest()[:10]}"
    df["funder_award_id"] = [award_id(r) for r in df.itertuples()]
    df["funding_type"] = df["kind"].map(lambda k: KINDS.get(k, ("research",))[0])
    df["landing_page_url"] = df["pdf_id"].map(lambda x: f"{SEARCH_URL}pdf/{x}.pdf" if x else SEARCH_URL)
    df = df.drop(columns=["report_year_i"])

    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        log(f"  {int(dupes.sum())} rows share a funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()[:10]}")
        df = df.drop_duplicates(subset=["funder_award_id"], keep="first")
    for c in ["title", "report_year", "grant_year", "researcher_name", "lead_family_name", "affiliation", "kind"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log("  " + df.groupby(["kind", "is_grant_number"]).size().to_string().replace("\n", "\n  "))
    log(f"  grant year source: {df['grant_year_source'].value_counts(dropna=False).to_dict()}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "uehara_foundation_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_uehara_foundation_projects.parquet"
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
