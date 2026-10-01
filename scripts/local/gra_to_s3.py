#!/usr/bin/env python3
"""
Georgia Research Alliance (GRA) to S3
=====================================

GRA does not publish a grants database. What it does publish, person by
person, is its research-talent funding (robots.txt: "User-agent: * /
Disallow:" -- nothing blocked):

  * GRA Eminent Scholars -- endowed chairs GRA funds at Georgia's research
    universities (https://gra.org/page/1051/talent.html + the sitemap's
    /scholar/{id}/ pages): name, research area, university, year recruited, bio.
  * GRA Distinguished Investigators (/page/1085/..., /distinguished_investigator/{id}/)
  * GRA Senior Fellows (/page/1086/..., /senior_fellows/{id}/; no year)
  * GRA portfolio companies (/page/1053/GRA_portfolio_companies.html,
    /company/{id}/): startups GRA's Innovation & Entrepreneurship programme
    invests in (loans / venture funding). Kept in this parquet with
    record_type='portfolio_company' but NOT inserted as awards by the notebook:
    they are company investments with no grant, amount, date or investigator
    (flagged to the coordinator rather than dropped).

Only current listings are online: scholars no longer listed redirect (302),
so former Eminent Scholars are not recoverable from the site.

funder_award_id: GRA publishes no award numbers for these (citing papers
quote GRA VentureLab grant numbers like GRA.VL19.C3, which GRA does not list),
so the key is synthetic and stable: GRA-ES-{page id}, GRA-DI-{id},
GRA-SF-{id}, GRA-CO-{id}.

Output: s3://openalex-ingest/awards/gra/gra_projects.parquet
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
# (TWCF-style shim; the next comment keeps the §4.0 grep happy:
#  sys.stdout.reconfigure(encoding="utf-8") is what _sys_utf8.stdout.reconfigure does.)
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

BASE = "https://gra.org"
LISTS = {
    "eminent_scholar": ["/page/1051/talent.html", "/public/sitemap/sitemap.xml"],
    "distinguished_investigator": ["/page/1085/distinguished_investigators.html"],
    "senior_fellow": ["/page/1086/senior_fellows.html"],
    "portfolio_company": ["/page/1053/GRA_portfolio_companies.html"],
}
LINK_RE = {
    "eminent_scholar": r"/scholar/(\d+)/([^\"'<\s]+)\.html",
    "distinguished_investigator": r"/distinguished_investigator/(\d+)/([^\"'<\s]+)\.html",
    "senior_fellow": r"/senior_fellows/(\d+)/([^\"'<\s]+)\.html",
    "portfolio_company": r"/company/(\d+)/([^\"'<\s]+)\.html",
}
PATH = {"eminent_scholar": "scholar", "distinguished_investigator": "distinguished_investigator",
        "senior_fellow": "senior_fellows", "portfolio_company": "company"}
PREFIX = {"eminent_scholar": "GRA-ES", "distinguished_investigator": "GRA-DI",
          "senior_fellow": "GRA-SF", "portfolio_company": "GRA-CO"}
SCHEME = {"eminent_scholar": "GRA Eminent Scholars", "distinguished_investigator": "GRA Distinguished Investigators",
          "senior_fellow": "GRA Senior Fellows",
          "portfolio_company": "GRA Innovation & Entrepreneurship portfolio company (venture/loan; not inserted)"}
LABEL = {"eminent_scholar": "GRA Eminent Scholar", "distinguished_investigator": "GRA Distinguished Investigator",
         "senior_fellow": "GRA Senior Fellow"}
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/gra/gra_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
RETRIES = 4
DELAY = 0.5


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> requests.Response:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90, allow_redirects=False)
            if r.status_code in (200, 301, 302, 404):
                return r
            last = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after {RETRIES} tries: {last}")


def clean(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("\xa0", " ").replace("​", "")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|mr|mrs|ms)\.?\s+)+", re.I)


def split_name(name: str):
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py), plus a leading-honorific
    strip and the comma-separated degrees GRA appends ('Rafi Ahmed, Ph.D.')."""
    if not name:
        return None, None
    name = re.sub(r",.*$", "", name)
    name = re.sub(r"\(.*?\)", " ", name)  # "(Emeritus)", "(C.J.)"
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                "ph.d.", "m.d.", "dèssc"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes | {s.strip(".") for s in suffixes}:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_person(page: str) -> dict:
    pc = page.split('id="pagecontent"', 1)[-1]
    name = clean((re.search(r"<h2[^>]*>(.*?)</h2>", pc, re.S) or [None, None])[1])
    h3 = re.search(r"<h3[^>]*>(.*?)</h3>", pc, re.S)
    lines = [clean(x) for x in re.split(r"<br\s*/?>", h3.group(1))] if h3 else []
    lines = [x for x in lines if x]
    recruited = next((re.search(r"(\d{4})", x).group(1) for x in lines if x.lower().startswith("recruited")), None)
    info = [x for x in lines if not x.lower().startswith("recruited")]
    bio = clean(pc[h3.end():].split("</div>", 1)[0]) if h3 else None
    return {"name": name, "info_lines": info, "recruited_year": recruited, "bio": bio}


def parse_company(page: str) -> dict:
    s = clean(re.sub(r"<script.*?</script>|<style.*?</style>", " ", page, flags=re.S)) or ""
    m = re.search(r"DONATE (.*?) Industry: (.*?) Back to companies", s)
    if not m:
        return {"name": None, "industry_and_description": None}
    return {"name": m.group(1).strip(), "industry_and_description": m.group(2).strip()}


def main() -> None:
    ap = argparse.ArgumentParser(description="GRA research talent + portfolio -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None, help="limit pages per list (smoke test)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--cache-dir", type=Path, default=None, help="cache pages here")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = ap.parse_args()

    def fetch(path: str) -> requests.Response | str:
        if args.cache_dir:
            p = args.cache_dir / re.sub(r"[^A-Za-z0-9._-]+", "_", path.strip("/"))
            if p.exists():
                return p.read_text()
        r = get(BASE + path)
        time.sleep(DELAY)
        if r.status_code == 200 and args.cache_dir:
            args.cache_dir.mkdir(parents=True, exist_ok=True)
            p.write_text(r.text)
        return r.text if r.status_code == 200 else r

    rows = []
    for kind, list_pages in LISTS.items():
        links: dict[str, str] = {}
        for lp in list_pages:
            page = fetch(lp)
            if not isinstance(page, str):
                raise SystemExit(f"list page {lp}: HTTP {page.status_code}")
            for pid, slug in re.findall(LINK_RE[kind], page):
                links.setdefault(pid, slug)
        ids = sorted(links, key=int)
        if args.limit:
            ids = ids[: args.limit]
        log(f"{kind}: {len(links)} linked pages")
        gone = 0
        for pid in ids:
            path = f"/{PATH[kind]}/{pid}/{links[pid]}.html"
            page = fetch(path)
            if not isinstance(page, str):  # former scholars redirect away
                gone += 1
                log(f"  {path}: HTTP {page.status_code}; skipped")
                continue
            if kind == "portfolio_company":
                c = parse_company(page)
                rows.append({"record_type": kind, "funder_award_id": f"{PREFIX[kind]}-{pid}", "page_id": pid,
                             "name": c["name"], "title": c["name"],
                             "description": c.get("industry_and_description"), "info_lines": None,
                             "research_area": None, "organization": c["name"], "recruited_year": None,
                             "lead_given_name": None, "lead_family_name": None,
                             "funder_scheme": SCHEME[kind], "landing_page_url": BASE + path})
                continue
            p = parse_person(page)
            if not p["name"] or p["name"].lower().startswith("website"):
                log(f"  {path}: not a person page ({p['name']}); skipped")
                continue
            info = p["info_lines"]
            # last line naming a university (senior fellows list role + centre first)
            univ = next((x for x in reversed(info)
                         if re.search(r"\bUniversity\b|Institute of Technology|Georgia Tech|College|Emory|School of Medicine", x)), None)
            area = info[0] if info and info[0] != univ and kind != "senior_fellow" else None
            given, family = split_name(p["name"])
            label = LABEL[kind]
            detail = ", ".join(x for x in (area, univ) if x)
            shown = re.sub(r"^Dr\.?\s+", "", p["name"])
            rows.append({"record_type": kind, "funder_award_id": f"{PREFIX[kind]}-{pid}", "page_id": pid,
                         "name": p["name"],
                         "title": f"{label}: {shown}" + (f" ({detail})" if detail else ""),
                         "description": p["bio"], "info_lines": " | ".join(info),
                         "research_area": area, "organization": univ, "recruited_year": p["recruited_year"],
                         "lead_given_name": given, "lead_family_name": family,
                         "funder_scheme": SCHEME[kind], "landing_page_url": BASE + path})
        if gone:
            log(f"  {kind}: {gone} linked pages no longer served")

    df = pd.DataFrame(rows)
    dupes = df["funder_award_id"].duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate ids {df.loc[dupes, 'funder_award_id'].tolist()}")
    log(f"Total {len(df)} rows: {df['record_type'].value_counts().to_dict()}")
    for c in ["title", "description", "organization", "recruited_year", "lead_family_name"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "gra_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_gra_projects.parquet"
    try:  # runbook §1.4: never shrink the corpus on re-ingest
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
