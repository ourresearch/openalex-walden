#!/usr/bin/env python3
"""
Naito Foundation (内藤記念科学振興財団) grant recipients to S3
===============================================================

The Naito Foundation (OpenAlex F4320309785, Tokyo) publishes its past grant
recipients ("これまでの受領者") at
https://www.naito-f.or.jp/jp/joseikn/jo_index.php?data=past . The page's
year x programme selector POSTs to db_post.php (year=YYYY, grant_id=CODE) and
gets back an HTML fragment with one <div class="subsidy_box"> per grant:
research theme, researcher name + position, institution + department, amount
("300万円" = JPY 3,000,000). The database covers FY2006..current. Method 5
(static HTML) on the runbook ladder; no bulk export exists.

Programmes (grant_id): KEN 科学奨励金・研究助成 (research grants), STP 若手
ステップアップ研究助成, JYO 女性研究者研究助成金, IKU 次世代育成支援研究助成金,
RYU 海外研究留学助成金 (overseas research fellowships), TOK 特定研究助成金,
WAK 若手研究者海外派遣助成金 (young-researcher overseas travel grants).
All are research money; nothing is excluded. funding_type: research, except
RYU = fellowship and WAK (travel to present at international meetings) = conference.

The foundation publishes no grant number, so funder_award_id is synthetic:
NAITO-{year}-{programme}-{sha1(name|theme)[:8]}.

Output: s3://openalex-ingest/awards/naito_foundation/naito_foundation_projects.parquet
"""

import argparse
import hashlib
import html
import re
import time
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

BASE = "https://www.naito-f.or.jp/jp/joseikn"
LIST_URL = f"{BASE}/jo_index.php?data=past"
POST_URL = f"{BASE}/db_post.php"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/naito_foundation/naito_foundation_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.3
RETRIES = 4

PROGRAMMES = {
    "KEN": ("内藤記念科学奨励金・研究助成", "research"),
    "STP": ("内藤記念科学奨励金・若手ステップアップ研究助成", "research"),
    "JYO": ("内藤記念女性研究者研究助成金", "research"),
    "IKU": ("内藤記念次世代育成支援研究助成金", "research"),
    "RYU": ("内藤記念海外研究留学助成金", "fellowship"),
    "TOK": ("内藤記念特定研究助成金", "research"),
    "WAK": ("内藤記念若手研究者海外派遣助成金", "conference"),
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch(s: requests.Session, method: str, url: str, **kw) -> str:
    last = None
    for attempt in range(RETRIES):
        try:
            r = s.request(method, url, timeout=60, **kw)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last = e
            log(f"  {method} {url} {kw.get('data')} failed ({e}); retry {attempt + 1}/{RETRIES}")
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"{method} {url} failed: {last}")


def years_and_programmes(s: requests.Session) -> tuple[list[int], list[str]]:
    page = fetch(s, "GET", LIST_URL)
    years = sorted({int(y) for y in re.findall(r"<option value='(\d{4})'>", page)})
    progs = re.findall(r"<option value='([A-Z]{3})'>", page)
    if not years or not progs:
        raise RuntimeError("could not read the year / programme selectors")
    for p in progs:
        if p not in PROGRAMMES:
            log(f"  WARNING: unknown programme code {p} on the site; ingesting it with funding_type research")
    return years, progs


def clean(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = html.unescape(re.sub(r"<[^>]+>", " ", fragment))
    t = re.sub(r"[\s　]+", " ", t).strip()
    return t or None


def lines(fragment: str | None) -> list[str]:
    if not fragment:
        return []
    return [x for x in (clean(p) for p in re.split(r"<br\s*/?>", fragment)) if x]


def split_name_ja(name: str) -> tuple[str | None, str | None]:
    """Names here are FAMILY<full-width space>GIVEN. Latin-script names go through the
    canonical given-first split (runbook §2.4.1 suffix set). Unsplittable -> (None, None),
    i.e. NULL rather than a guess."""
    tokens = [t for t in re.split(r"[\s　]+", (name or "").strip()) if t]
    if not tokens:
        return None, None
    if all(re.fullmatch(r"[A-Za-z.\-']+", t) for t in tokens):
        suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
        while tokens and tokens[-1].lower().strip(",.") in suffixes:
            tokens.pop()
        if len(tokens) < 2:
            return None, (tokens[0] if tokens else None)
        return " ".join(tokens[:-1]), tokens[-1]
    if len(tokens) < 2:
        return None, None
    return " ".join(tokens[1:]), tokens[0]


def man_yen(s: str | None) -> float | None:
    """'300万円' -> 3,000,000; '1,000万円' -> 10,000,000; '50万円' -> 500,000."""
    if not s:
        return None
    m = re.search(r"([\d,.]+)\s*万円", s)
    if m:
        return float(m.group(1).replace(",", "")) * 10000
    m = re.search(r"([\d,]+)\s*円", s)
    return float(m.group(1).replace(",", "")) if m else None


def parse_fragment(frag: str, year: int, prog: str) -> list[dict]:
    rows = []
    for b in frag.split('<div class="subsidy_box">')[1:]:
        def cell(k):
            m = re.search(rf'class="{k}">(.*?)</div>', b, re.S)
            return m.group(1) if m else None
        who, inst, amt = lines(cell("subsidy03")), lines(cell("subsidy04")), lines(cell("subsidy05"))
        name = who[0] if who else None
        given, family = split_name_ja(name)
        title = clean(cell("subsidy01"))
        rows.append({
            "year": str(year),
            "programme_code": prog,
            "programme": PROGRAMMES.get(prog, (prog, "research"))[0],
            "funding_type": PROGRAMMES.get(prog, (prog, "research"))[1],
            "title": title,
            "researcher_name": name,
            "lead_given_name": given,
            "lead_family_name": family,
            "position": " ".join(who[1:]) or None,
            "institution": inst[0] if inst else None,
            "department": " ".join(inst[1:]) or None,
            "amount_text": " ".join(amt) or None,
            "amount_jpy": man_yen(amt[0]) if amt else None,
        })
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description="Naito Foundation grant recipients -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the N most recent years (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache HTML fragments here")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    s = requests.Session()
    s.headers.update(HEADERS)
    years, progs = years_and_programmes(s)
    if args.limit:
        years = years[-args.limit:]
    log(f"Years {years[0]}-{years[-1]}, programmes {progs}")

    rows, empty = [], 0
    for y in years:
        for prog in progs:
            cache = args.cache_dir / f"{y}_{prog}.html" if args.cache_dir else None
            if cache and cache.exists():
                frag = cache.read_text()
            else:
                frag = fetch(s, "POST", POST_URL, data={"year": str(y), "grant_id": prog})
                if cache:
                    cache.parent.mkdir(parents=True, exist_ok=True)
                    cache.write_text(frag)
                time.sleep(REQUEST_DELAY)
            got = parse_fragment(frag, y, prog)
            if not got:
                empty += 1
                if "該当データなし" not in frag:
                    raise RuntimeError(f"{y} {prog}: no grants parsed and no 'no data' marker; page format changed?")
            rows += got
        log(f"  {y}: {sum(1 for r in rows if r['year'] == str(y))} grants")
    log(f"{len(rows)} grants; {empty} empty year x programme combinations (programme not run that year)")

    df = pd.DataFrame(rows)
    df = df[df["title"].notna() | df["researcher_name"].notna()].copy()
    basis = (df["researcher_name"].fillna("") + "|" + df["title"].fillna("")).str.replace(r"\s+", "", regex=True)
    df["funder_award_id"] = ("NAITO-" + df["year"] + "-" + df["programme_code"] + "-"
                             + basis.map(lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest()[:8]))
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    df["landing_page_url"] = LIST_URL
    for c in ["title", "researcher_name", "lead_family_name", "institution", "amount_jpy"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  total JPY {df['amount_jpy'].sum():,.0f}")
    log(df.groupby("programme_code").size().to_string())

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "naito_foundation_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_naito_foundation_projects.parquet"
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
