#!/usr/bin/env python3
"""
AMED (Japan Agency for Medical Research and Development) AMEDfind -> S3 Pipeline
================================================================================

Downloads AMED-funded R&D project records from AMED's own first-party project
database, **AMEDfind** (https://amedfind.amed.go.jp/amed/).

Source authority
----------------
AMEDfind is AMED's official R&D project database (~11,055 projects). It is the
first-party source and is preferred over the JST 'GRANTS' integrated portal
(grants.jst.go.jp), which only federates a subset (~10,373) and paginates via
JS only. KAKEN does NOT contain AMED projects (KAKEN is MEXT/JSPS KAKENHI only),
so the existing KAKEN ingest does not cover AMED.

How the data is fetched
-----------------------
AMEDfind is a Nuxt/Vue SPA backed by a Spring Boot search API. The full project
list (with all the fields we need) is returned by:

    POST https://amedfind.amed.go.jp/amed/web/api/get/list

The endpoint is NOT documented and rejects requests that lack the front-end's
own request header **`x-request-origin: amedfind_web`** with an empty HTTP 400
(this header, not a TLS/browser fingerprint, is the gate — verified by capturing
the app's own request and replaying it with plain HTTP). With the header, plain
`requests` works; no browser/Playwright is needed.

Pagination is `searchKind:"paging"` + `searchConditions.start` (1-based) /
`limit` (up to 500). The `data` array carries one entry per project; each
`themeBasic[0]` has: entrusterThemeNo (stable per-project id, used in the
detail-page route), themeNo (latest fiscal-year management number), themeName
(title), jigyo (事業 = funding program), institution, researcherName + job
(the 研究代表者 / PI), allStartDate / allEndDate (full ISO dates), totalBudget
(課題への総配分額 in 千円 / thousands of yen), and themeOverview (description).

Amounts: present for ~97-99% of mature projects; the newest (current fiscal
year) projects often have totalBudget 0 because the allocation is not yet
reported. Overall coverage is well above the §6.7 50% bar, so §6.7 is not
waived. totalBudget is in 千円 -> multiply by 1000 for whole JPY.

OpenAlex funder
---------------
F4320311405 - Japan Agency for Medical Research and Development (JP)

Output
------
s3://openalex-ingest/awards/amed/amed_projects.parquet

Usage
-----
    python scripts/local/amed_to_s3.py --limit 50 --skip-upload   # smoke test
    python scripts/local/amed_to_s3.py                            # full run + upload
    python scripts/local/amed_to_s3.py --allow-shrink
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests


# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Critical here: every title / PI / description / program is Japanese.
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


BASE_URL = "https://amedfind.amed.go.jp"
LIST_API = f"{BASE_URL}/amed/web/api/get/list"
DETAIL_URL = f"{BASE_URL}/amed/theme-detail/{{tid}}"
FUNDER_ID = 4320311405
PROVENANCE = "amed_amedfind"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/amed/amed_projects.parquet"

# `x-request-origin: amedfind_web` is the required gate header (see module docstring).
HEADERS = {
    "User-Agent": "openalex-walden-amed/1.0 (+https://openalex.org)",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-request-origin": "amedfind_web",
    "Referer": f"{BASE_URL}/amed/theme-list",
}

DEFAULT_OUTPUT_DIR = Path("/tmp")
REQUEST_TIMEOUT = (10, 90)
MAX_RETRIES = 5
PAGE_SIZE = 500
MAX_CONSECUTIVE_EMPTY = 3   # empty page != EOF (runbook §1); totalCount terminates

# Default search-conditions DTO the front-end posts (extracted from the bundle);
# empty text fields => browse-all. start/limit are overridden per page.
SEARCH_CONDITIONS_DEFAULT = {
    "amedFormat": True,
    "synonymExists": True,
    "searchTargetThemeName": True,
    "searchTargetThemeNo": True,
    "searchTargetJigyoName": True,
    "searchTargetProjectName": False,
    "searchTargetThemeOverview": True,
    "searchTargetAchievementOverview": False,
    "searchTargetAchievementInstitute": False,
    "searchTargetAchievementOral": False,
    "searchTargetAchievementScienceTechnology": False,
    "sort": "-themeDate",
    "start": 1,
    "limit": PAGE_SIZE,
    "outputTarget": "the",
}


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    value = re.sub(r"[ \t　]+", " ", str(value)).strip()
    return value or None


def iso_date(value: Any) -> str | None:
    """'2025-10-31T00:00:00Z' -> '2025-10-31'. Returns None on anything else."""
    if not value or not isinstance(value, str):
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", value)
    return m.group(1) if m else None


def year_of(date_str: str | None) -> str | None:
    if not date_str:
        return None
    m = re.match(r"(\d{4})", date_str)
    if not m:
        return None
    y = int(m.group(1))
    return str(y) if 1800 <= y <= 2100 else None


def parse_amount_jpy(total_budget: Any) -> int | None:
    """totalBudget is in 千円 (thousands of yen). -1 / 0 / None => no amount."""
    try:
        v = int(total_budget)
    except (TypeError, ValueError):
        return None
    return v * 1000 if v > 0 else None


def build_record(tb: dict[str, Any], theme_id: str | None) -> dict[str, Any] | None:
    """Build one award row from a get/list `themeBasic[0]` entry."""
    title = clean_text(tb.get("themeName"))
    # entrusterThemeNo is the stable per-project key and the detail-route id.
    award_id = clean_text(tb.get("entrusterThemeNo")) or clean_text(tb.get("themeNo"))
    if not title or not award_id:
        return None
    start_date = iso_date(tb.get("allStartDate"))
    end_date = iso_date(tb.get("allEndDate"))
    amount = parse_amount_jpy(tb.get("totalBudget"))
    # Japanese PI names are not space-delimited; we do NOT guess a given/family
    # boundary (runbook: NULL rather than guess). The verbatim name is shipped in
    # family_name by the notebook; given_name stays NULL.
    return {
        "funder_award_id": award_id,
        "theme_no": clean_text(tb.get("themeNo")),
        "theme_id": clean_text(theme_id),
        "display_name": title,
        "description": clean_text(tb.get("themeOverview")),
        "funder_scheme": clean_text(tb.get("jigyo")),
        "integrated_project": clean_text(tb.get("integratedProject3")) or clean_text(tb.get("integratedProject")),
        "amount": str(amount) if amount is not None else None,
        "amount_thousand_yen": clean_text(tb.get("totalBudget")),
        "currency": "JPY" if amount is not None else None,
        "lead_researcher_name": clean_text(tb.get("researcherName")),
        "lead_affiliation_name": clean_text(tb.get("institution")),
        "lead_job": clean_text(tb.get("job")),
        "researcher_id": clean_text(tb.get("researcherId")),
        "erad_researcher_id": clean_text(tb.get("eradResearcherId")),
        "start_date": start_date,
        "end_date": end_date,
        "start_year": year_of(start_date),
        "end_year": year_of(end_date),
        "landing_page_url": DETAIL_URL.format(tid=award_id),
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }


def post_list(session: requests.Session, start: int, limit: int) -> dict[str, Any] | None:
    cond = dict(SEARCH_CONDITIONS_DEFAULT)
    cond["start"] = start
    cond["limit"] = limit
    body = json.dumps({"searchKind": "paging", "searchConditions": cond})
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.post(LIST_API, data=body, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            log(f"  start={start}: HTTP {r.status_code} (attempt {attempt}/{MAX_RETRIES})")
        except requests.RequestException as exc:
            last_exc = exc
            log(f"  start={start}: {exc} (attempt {attempt}/{MAX_RETRIES})")
        time.sleep(min(2 ** attempt, 30))
    log(f"  GIVING UP on start={start}: {last_exc}")
    return None


def crawl(limit: int | None, page_sleep: float) -> list[dict[str, Any]]:
    session = requests.Session()
    session.headers.update(HEADERS)
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    total: int | None = None
    start = 1
    consecutive_empty = 0
    while True:
        payload = post_list(session, start, PAGE_SIZE)
        if payload is None:
            raise RuntimeError(f"get/list failed at start={start}; aborting (do not upload partial).")
        if total is None:
            total = int(payload.get("totalCount") or 0)
            log(f"  totalCount reported: {total:,}")
        rows = payload.get("data") or []
        added = 0
        for entry in rows:
            tb_list = entry.get("themeBasic") or []
            if not tb_list:
                continue
            rec = build_record(tb_list[0], entry.get("themeId"))
            if rec and rec["funder_award_id"] not in seen:
                seen.add(rec["funder_award_id"])
                records.append(rec)
                added += 1
            if limit and len(records) >= limit:
                break
        log(f"  start={start}: {len(rows)} rows, +{added} new, total {len(records):,}"
            + (f"/{total:,}" if total else ""))
        if not rows:
            consecutive_empty += 1
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                log("  3 consecutive empty pages; assuming end of corpus")
                break
        else:
            consecutive_empty = 0
        if limit and len(records) >= limit:
            break
        if total and start + PAGE_SIZE > total:
            break
        start += PAGE_SIZE
        if page_sleep:
            time.sleep(page_sleep)
    return records


# --------------------------------------------------------------------------
# Dataframe / validation / upload
# --------------------------------------------------------------------------

STRING_COLUMNS = [
    "funder_award_id", "theme_no", "theme_id", "display_name", "description",
    "funder_scheme", "integrated_project", "amount", "amount_thousand_yen",
    "currency", "lead_researcher_name", "lead_affiliation_name", "lead_job",
    "researcher_id", "erad_researcher_id", "start_date", "end_date",
    "start_year", "end_year", "landing_page_url", "downloaded_at",
]


def build_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    for col in STRING_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[STRING_COLUMNS]
    dup = df["funder_award_id"].duplicated(keep=False)
    if dup.any():
        ex = df.loc[dup, ["funder_award_id", "display_name"]].head(20)
        raise RuntimeError(f"Duplicate funder_award_id values:\n{ex}")
    # Force string dtype (runbook §1.2 item 5: avoid pyarrow int-inference on
    # null-heavy columns like amount).
    return df.astype("string")


def validate(df: pd.DataFrame) -> None:
    n = len(df)
    log("=" * 72)
    log(f"Local validation: {n:,} rows")

    def pct(col: str) -> None:
        c = int(df[col].notna().sum()) if col in df else 0
        log(f"  {col:<22}{c:>7,}/{n:<7,} ({100.0 * c / n:5.1f}%)" if n else f"  {col}: 0")

    for col in ["display_name", "description", "funder_scheme",
                "lead_researcher_name", "lead_affiliation_name", "amount",
                "currency", "start_date", "end_date", "start_year"]:
        pct(col)
    log(f"  unique funder_award_id {df['funder_award_id'].nunique():,}/{n:,}")
    yrs = pd.to_numeric(df["start_year"], errors="coerce").dropna()
    if len(yrs):
        log(f"  start_year range {int(yrs.min())}-{int(yrs.max())}")
    amts = pd.to_numeric(df["amount"], errors="coerce").dropna()
    if len(amts):
        log(f"  amount JPY {amts.min():,.0f}-{amts.max():,.0f}; total JPY {amts.sum():,.0f}")


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> None:
    import boto3
    from botocore.exceptions import ClientError
    client = boto3.client("s3")
    log(f"Runbook §1.4 shrink-check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            log("  No existing S3 parquet; first ingest.")
            return
        raise
    prev = output_dir / "_previous_amed_projects.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev))
        prev_count = len(pd.read_parquet(prev))
    finally:
        prev.unlink(missing_ok=True)
    log(f"  previous rows {prev_count:,}, new rows {new_count:,}")
    if new_count < prev_count and not allow_shrink:
        raise RuntimeError(
            f"Runbook §1.4 violation: refusing to shrink AMED corpus "
            f"{prev_count:,} -> {new_count:,}. Use --allow-shrink to override."
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="AMEDfind -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit", type=int, default=None, help="Smoke-test: stop after N records")
    parser.add_argument("--page-sleep", type=float, default=0.3)
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    log("=" * 72)
    log("AMED (AMEDfind) ingest starting")
    log(f"source={LIST_API}  funder_id={FUNDER_ID}  provenance={PROVENANCE}")

    records = crawl(args.limit, args.page_sleep)
    log(f"Collected {len(records):,} project records")
    if not records:
        log("No records; aborting.")
        sys.exit(1)

    df = build_dataframe(records)
    validate(df)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "amed_projects.parquet"
    df.to_parquet(out, index=False, engine="pyarrow")
    log(f"Wrote {out} ({out.stat().st_size:,} bytes)")

    if args.skip_upload:
        log("--skip-upload set; not uploading to S3.")
        return
    if args.limit:
        log("Partial run (--limit); refusing to upload a partial corpus.")
        return
    check_no_shrink(len(df), args.allow_shrink, args.output_dir)
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3
    boto3.client("s3").upload_file(str(out), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    main()
