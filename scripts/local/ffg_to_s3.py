#!/usr/bin/env python3
"""
FFG (Oesterreichische Forschungsfoerderungsgesellschaft) to S3 Data Pipeline
===========================================================================

FFG publishes the projects it funds (with the recipients' consent; focus on
projects from 2015 on) in the FFG Projektdatenbank at https://projekte.ffg.at/.
The database has its own **Excel export** (ladder item 0): the search result
list lets a user tick projects and "als Excel exportieren", which POSTs the
selected project IDs (`id[]`) to https://projekte.ffg.at/projekt/excel and
returns an .xls with one row per (project x organisation):

    Projekt-ID, Kurztitel, Langtitel, Abstract, Programm, Ausschreibung,
    Projektstart, Projektende, Projektstatus, Keywords, Rolle im Projekt
    (Konsortialfuehrer / Projektpartner / ...), Organisationsname,
    Organisationsart, Staat, Bundesland, Stadt, Adresse (Office)

So the script (1) pages through the full, unfiltered search result list
(100 per page, sorted by title for stable paging) to collect every
Projekt-ID, then (2) requests the export in batches of 100 IDs, and
(3) collapses it to one row per project (lead organisation = the
"Konsortialfuehrer"/coordinator row; all organisations kept as a list).

The export has **no funding amounts, no person names and no FFG
project number** -- "Projekt-ID" is the database's GUID, which is also the
project page URL (https://projekte.ffg.at/projekt/<Projekt-ID>).
data.gv.at / data.europa.eu carry no FFG project dataset (checked 2026-09-30).

Output: s3://openalex-ingest/awards/ffg/ffg_projects.parquet
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
# file I/O (Path.write_text / open() without explicit encoding=). See runbook §1.2.
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

BASE = "https://projekte.ffg.at"
SLUG = "ffg"
S3_BUCKET = "openalex-ingest"
S3_KEY = f"awards/{SLUG}/{SLUG}_projects.parquet"
PAGE = 100
BATCH = 100

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org; awards ingest)"}
REQUEST_DELAY = 0.5
RETRIES = 4
MAX_CONSECUTIVE_EMPTY = 3

_session = requests.Session()
_session.headers.update(HEADERS)

EXPORT_COLS = {
    "Projekt-ID": "project_id", "Kurztitel": "acronym", "Langtitel": "title", "Abstract": "abstract",
    "Programm": "programme", "Ausschreibung": "call", "Projektstart": "start_raw", "Projektende": "end_raw",
    "Projektstatus": "status", "Keywords": "keywords", "Rolle im Projekt": "role",
    "Organisationsname": "org_name", "Organisationsart": "org_type", "Staat": "org_country",
    "Bundesland": "org_state", "Stadt": "org_city",
}
UUID_RE = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def request(method: str, url: str, **kw) -> requests.Response:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = _session.request(method, url, timeout=180, **kw)
            if r.status_code == 200:
                time.sleep(REQUEST_DELAY)
                return r
            last_err = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last_err = e
        log(f"  retry {attempt + 1}/{RETRIES} {method} {url}: {last_err}")
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{method} {url} failed: {last_err}")


def list_ids(cache: Path | None) -> list[str]:
    ids: list[str] = []
    total, start, consecutive_empty = None, 0, 0
    while total is None or start < total:
        f = cache / f"list_{start:05d}.html" if cache else None
        if f is not None and f.exists():
            page = f.read_text()
        else:
            r = request("GET", f"{BASE}/projekt", params={
                "advanced_search": 1, "go": 1, "q": "", "status": "", "start": start, "sort": "title-asc"})
            r.encoding = "utf-8"
            page = r.text
            if f is not None:
                f.write_text(page)
        if total is None:
            m = re.search(r"lieferte\s+([\d.]+)\s+Ergebnis", page)
            if not m:
                raise RuntimeError("result count not found on the first list page")
            total = int(m.group(1).replace(".", ""))
            log(f"Search reports {total} projects")
        got = re.findall(rf'class="project-selector" type="checkbox" value="({UUID_RE})"', page)
        log(f"  list start={start}: {len(got)} ids")
        if not got:
            consecutive_empty += 1
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                raise RuntimeError(f"{MAX_CONSECUTIVE_EMPTY} empty list pages in a row at start={start}")
        else:
            consecutive_empty = 0
        ids += got
        start += PAGE
    uniq = list(dict.fromkeys(ids))
    log(f"Collected {len(uniq)} unique project ids ({len(ids)} incl. repeats) of {total} reported")
    if len(uniq) < 0.98 * total:
        raise RuntimeError(f"only {len(uniq)} of {total} ids collected -- paging broke")
    return uniq


def export_batch(ids: list[str], f: Path | None) -> pd.DataFrame:
    import xlrd
    if f is not None and f.exists():
        data = f.read_bytes()
    else:
        payload = [("id[]", i) for i in ids] + [("projects_selected", str(len(ids))), ("projects_total", str(len(ids)))]
        r = request("POST", f"{BASE}/projekt/excel", data=payload)
        data = r.content
        if not data.startswith(b"\xd0\xcf\x11\xe0"):
            raise RuntimeError(f"export did not return an .xls ({r.headers.get('content-type')})")
        if f is not None:
            f.write_bytes(data)
    # The generated .xls has a slightly malformed compound-document header;
    # xlrd reads it fine with ignore_workbook_corruption.
    bk = xlrd.open_workbook(file_contents=data, ignore_workbook_corruption=True)
    sh = bk.sheet_by_index(0)
    hdr_row = next(r for r in range(min(sh.nrows, 20)) if sh.cell_value(r, 0) == "Projekt-ID")
    header = [str(v).strip() for v in sh.row_values(hdr_row)]
    rows = [dict(zip(header, (str(v).strip() for v in sh.row_values(r)))) for r in range(hdr_row + 1, sh.nrows)]
    return pd.DataFrame(rows).rename(columns=EXPORT_COLS)


def parse_date(s: str | None) -> str | None:
    m = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", (s or "").strip())
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else None


def clean(s):
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    s = html.unescape(str(s)).replace("​", "").replace("﻿", "").strip()
    return s or None


def main() -> None:
    p = argparse.ArgumentParser(description="FFG Projektdatenbank (Excel export) -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="export only the first N project ids (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache list pages / export files here")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()
    if args.cache_dir:
        args.cache_dir.mkdir(parents=True, exist_ok=True)

    ids = list_ids(args.cache_dir)
    if args.limit:
        ids = ids[: args.limit]

    parts = []
    for b in range(0, len(ids), BATCH):
        chunk = ids[b: b + BATCH]
        f = args.cache_dir / f"export_{b:05d}_{len(chunk)}.xls" if args.cache_dir else None
        df = export_batch(chunk, f)
        missing = set(chunk) - set(df["project_id"])
        log(f"  export {b + len(chunk)}/{len(ids)}: {len(df)} org rows, {df['project_id'].nunique()} projects"
            + (f", {len(missing)} ids absent from export" if missing else ""))
        parts.append(df)
    long = pd.concat(parts, ignore_index=True)
    long = long.drop_duplicates()
    for c in long.columns:
        long[c] = long[c].map(clean)

    # One row per project. Project-level fields are repeated on every org row
    # (Abstract only on the first); take the first non-null per project.
    proj_cols = ["acronym", "title", "abstract", "programme", "call", "start_raw", "end_raw", "status", "keywords"]
    projects = long.groupby("project_id", sort=False)[proj_cols].first().reset_index()

    lead_roles = {"konsortialführer", "einzelantragsteller", "koordinator", "projektkoordinator", "förderungsnehmer", "fördernehmer"}
    orgs = (long.assign(is_lead=long["role"].fillna("").str.lower().isin(lead_roles))
                .sort_values(["project_id", "is_lead"], ascending=[True, False], kind="stable"))
    org_lists = orgs.groupby("project_id", sort=False).apply(
        lambda g: [{"role": r.role, "name": r.org_name, "type": r.org_type, "country": r.org_country,
                    "state": r.org_state, "city": r.org_city} for r in g.itertuples()],
        include_groups=False).rename("organisations")
    lead = orgs.drop_duplicates("project_id").set_index("project_id")
    projects = projects.join(org_lists, on="project_id")
    projects["lead_role"] = projects["project_id"].map(lead["role"])
    projects["lead_org"] = projects["project_id"].map(lead["org_name"])
    projects["lead_org_country"] = projects["project_id"].map(lead["org_country"])
    projects["start_date"] = projects["start_raw"].map(parse_date)
    projects["end_date"] = projects["end_raw"].map(parse_date)
    projects["landing_page_url"] = BASE + "/projekt/" + projects["project_id"]

    dupes = projects["project_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate project_id: {projects.loc[dupes, 'project_id'].tolist()[:20]}")
    log(f"{len(projects)} projects from {len(long)} (project x organisation) rows; "
        f"{len(set(ids) - set(projects['project_id']))} listed ids absent from the export")
    log("  roles: " + ", ".join(f"{k}:{v}" for k, v in long["role"].value_counts().items()))
    for c in ["acronym", "title", "abstract", "programme", "call", "start_date", "end_date", "lead_org"]:
        log(f"  {c:12s} {projects[c].notna().mean():6.1%}")
    log("  lead role: " + ", ".join(f"{k}:{v}" for k, v in projects["lead_role"].value_counts(dropna=False).items()))

    org_col = projects.pop("organisations")
    projects = projects.astype("string")
    projects["organisations"] = org_col

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / f"{SLUG}_projects.parquet"
    projects.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(projects)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / f"_previous_{SLUG}_projects.parquet"
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(projects)}")
        if len(projects) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(projects)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
