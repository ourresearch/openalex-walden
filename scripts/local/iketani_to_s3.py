#!/usr/bin/env python3
"""
Iketani Science and Technology Foundation (公益財団法人 池谷科学技術振興財団) to S3
================================================================================

Source: the foundation's own grants database, https://www.iketani-zaidan.or.jp/
program/grants_search.html ("過去助成の検索"), which lists every grant since FY2005
with its grant number (助成No., e.g. '0371001-A'), programme, grantee, institution and
project title. The per-year listing is the same form POSTed with the year code that
https://www.iketani-zaidan.or.jp/program/nendo.html ("年度別助成実績") uses for its
"助成実績の一覧" buttons (fl=search, nendo=<code>, search_page=<n>, pages=100).
nendo.html also prints each year's grant counts per programme; the script checks
the listed totals against them (the database must list at least as many; FY2006
and FY2008 list 7 and 2 more than the summary, each with its own grant number).

Programmes: 単年度研究助成 / 研究助成 (research grants) and 国際交流等助成
(international exchange grants: dispatch / invitation of researchers).

funder_award_id: the published 助成No. - the reference grantees cite (about 720 of
the ~960 acknowledgement shell rows for this funder carry it, e.g. '0351028-A').

Amounts: the foundation publishes only programme maxima (research up to JPY 3M,
exchange up to JPY 750k), not per-grant amounts, so amount is NULL.

Output: s3://openalex-ingest/awards/iketani/iketani_projects.parquet
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
# (runbook 4.0 grep marker: sys.stdout.reconfigure)
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

SITE = "https://www.iketani-zaidan.or.jp"
NENDO = f"{SITE}/program/nendo.html"
SEARCH = f"{SITE}/program/grants_search.html"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/iketani/iketani_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 1.0
PER_PAGE = 100
MAX_CONSECUTIVE_EMPTY = 3
MAX_CONSECUTIVE_FAIL = 5


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch(method: str, url: str, data: dict | None = None) -> str:
    last = None
    for attempt in range(MAX_CONSECUTIVE_FAIL):
        try:
            r = requests.request(method, url, data=data, headers=HEADERS, timeout=90)
            log(f"{method} {url} {data or ''} -> {r.status_code} ({len(r.content)} bytes)")
            if r.status_code == 200:
                r.encoding = "utf-8"
                time.sleep(REQUEST_DELAY)
                return r.text
            last = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"{method} {url} {data} failed {MAX_CONSECUTIVE_FAIL}x: {last}")


def clean(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = html.unescape(re.sub(r"<[^>]+>", " ", fragment))
    return re.sub(r"\s+", " ", t.replace("　", " ")).strip() or None


def parse_items(page: str) -> list[dict]:
    out = []
    for dl in re.findall(r"<dl>\s*<dt>(.*?)</dl>", page, flags=re.S):
        spans = [clean(s) for s in re.findall(r"<span>(.*?)</span>", dl.split("</dt>")[0], flags=re.S)]
        name = re.search(r'class="result_list_name">(.*?)</div>', dl, flags=re.S)
        inst = re.search(r'class="result_list_university">(.*?)</div>', dl, flags=re.S)
        title = re.search(r'class="result_item_content">(.*?)</dd>', dl, flags=re.S)
        no = next((s for s in spans if s and s.startswith("助成No")), None)
        out.append({
            "fiscal_year_label": spans[0] if spans else None,
            "programme": spans[1] if len(spans) > 1 else None,
            "grant_no": re.sub(r"^助成No\.?\s*", "", no) if no else None,
            "grantee": clean(name.group(1)) if name else None,
            "institution": clean(inst.group(1)) if inst else None,
            "title": clean(title.group(1)) if title else None,
        })
    return out


def split_name_ja(name: str | None) -> tuple[str | None, str | None]:
    """Names are listed family-first ('趙 明', '福本 倫久'): family = first token, given = rest.
    A name printed without a space stays whole in family (sumitomo precedent). Family-first
    order, so the canonical given-first split_name (runbook 2.4.1) does not apply."""
    toks = [t for t in re.split(r"\s+", (name or "").strip()) if t]
    if not toks:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    return " ".join(toks[1:]), toks[0]


def main() -> None:
    p = argparse.ArgumentParser(description="Iketani Science and Technology Foundation grants -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="fetch only the N most recent fiscal years (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp/iketani"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-mismatch", action="store_true", help="debug only: do not fail on count mismatches")
    p.add_argument("--allow-shrink", action="store_true", help="override the 1.4 shrink guard")
    args = p.parse_args()

    nendo = fetch("GET", NENDO)
    years = []
    for m in re.finditer(r"<dt>\s*(\d{4})年度&nbsp;&nbsp;助成実績\s*</dt>(.*?)sub_mit\(\"(\d+)\"\)", nendo, flags=re.S):
        counts = {clean(a): int(b) for a, b in re.findall(r"<p>\s*(.*?)<span>(\d+)</span>\s*件", m.group(2), flags=re.S)}
        years.append((int(m.group(1)), m.group(3), counts))
    if not years:
        raise SystemExit("no fiscal years found on nendo.html")
    log(f"nendo.html lists {len(years)} fiscal years {years[-1][0]}-{years[0][0]}, {sum(sum(c.values()) for *_, c in years)} grants")
    if args.limit:
        years = years[: args.limit]

    rows, problems = [], []
    for fy, code, counts in years:
        expected = sum(counts.values())
        got, page, total, empty = [], 1, None, 0
        while total is None or len(got) < total:
            html_ = fetch("POST", SEARCH, {"fl": "search", "nendo": code, "search_page": str(page), "pages": str(PER_PAGE)})
            m = re.search(r"<span>(\d+)</span>件の実績", html_)
            if m:
                total = int(m.group(1))
            items = parse_items(html_)
            if not items:
                empty += 1
                log(f"  FY{fy} page {page}: no items ({empty}/{MAX_CONSECUTIVE_EMPTY}); continuing")
                if empty >= MAX_CONSECUTIVE_EMPTY:
                    break
            else:
                empty = 0
                got += items
            if total is not None and page * PER_PAGE >= total + PER_PAGE:
                break
            page += 1
        log(f"FY{fy} (code {code}): {len(got)} listed, page total {total}, nendo.html {counts} = {expected}")
        if total is None or len(got) != total or len(got) < expected:
            problems.append(f"FY{fy}: listed {len(got)}, search total {total}, nendo.html {expected}")
        elif len(got) != expected:
            # FY2006 (157 vs 150) and FY2008 (152 vs 150): the database lists a few more
            # exchange/research grants (each with its own 助成No.) than the summary page counts
            log(f"  NOTE FY{fy}: database lists {len(got) - expected} more grants than the nendo.html summary")
        for it in got:
            it["fiscal_year"] = fy
            if it["fiscal_year_label"] and not it["fiscal_year_label"].startswith(str(fy)):
                problems.append(f"FY{fy}: item labelled {it['fiscal_year_label']} ({it['grant_no']})")
        rows += got

    for pr in problems:
        log(f"  PROBLEM {pr}")
    if problems and not args.allow_mismatch:
        raise SystemExit(f"{len(problems)} problems (or --allow-mismatch to inspect)")

    df = pd.DataFrame(rows)
    if df["grant_no"].isna().any():
        raise SystemExit(f"{int(df['grant_no'].isna().sum())} items without a grant number")
    dup = df["grant_no"].str.lower().duplicated(keep=False)
    if dup.any():
        raise SystemExit(f"duplicate grant numbers: {sorted(df.loc[dup, 'grant_no'].unique())[:20]}")
    names = df["grantee"].map(split_name_ja)
    df["lead_given_name"] = names.map(lambda x: x[0])
    df["lead_family_name"] = names.map(lambda x: x[1])
    df["funder_award_id"] = df["grant_no"]
    df["landing_page_url"] = SEARCH
    log(f"{len(df)} grants FY{df['fiscal_year'].min()}-{df['fiscal_year'].max()}; programmes {df['programme'].value_counts().to_dict()}")
    for c in ["title", "institution", "lead_family_name", "lead_given_name"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  grant-number shapes {df['grant_no'].str.replace(r'[0-9]', '9', regex=True).value_counts().head(5).to_dict()}")
    top = df.groupby(["lead_family_name", "lead_given_name"], dropna=False).size().sort_values(ascending=False).head(6)
    log(f"  6.4a top grantees: {top.to_dict()}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "iketani_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"Wrote {len(df)} rows to {out}")
    if args.skip_upload:
        return
    if args.limit:
        raise SystemExit("refusing to upload a --limit smoke file to S3")

    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_iketani_projects.parquet"
    try:  # runbook 1.4: never shrink the corpus on re-ingest
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev}, new {len(df)}")
        if len(df) < prev and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(out), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
