#!/usr/bin/env python3
"""
Health Research Council of New Zealand (HRC) Research Repository -> S3 Pipeline
===============================================================================

Downloads funded-study records from the HRC's own public Research Repository at
https://www.hrc.govt.nz/resources/research-repository .

Source authority
----------------
The Research Repository is HRC's own published list of HRC-funded studies. The
listing paginates server-side (`?page=0..N`) and each study renders as a static
server-rendered page at `/resources/research-repository/{slug}` carrying a clean
label/value block:

    Year:            2016
    Duration:        39 months
    Approved budget: $1,121,058.08      <- NZD amount (present)
    Researchers:     Professor Allan Herbison   <- lead investigator
    Host:            University of Otago         <- institution
    Health issue:    Reproduction/fertility/sexual health
    Proposal type:   Project
    Lay summary      <free text>

MBIE's national "Who got funded" dataset explicitly EXCLUDES HRC, so this
first-party repository is the authoritative source.

OpenAlex funder
---------------
F4320334749 - Health Research Council of New Zealand (ROR 00zbf3d93,
DOI 10.13039/501100001505)

Output
------
s3://openalex-ingest/awards/hrc_nz/hrc_nz_projects.parquet

Usage
-----
    python scripts/local/hrc_nz_to_s3.py --limit 10 --skip-upload
    python scripts/local/hrc_nz_to_s3.py --skip-upload
    python scripts/local/hrc_nz_to_s3.py
    python scripts/local/hrc_nz_to_s3.py --allow-shrink
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests


# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
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


BASE_URL = "https://www.hrc.govt.nz"
LISTING_URL = f"{BASE_URL}/resources/research-repository"
RECORD_PREFIX = "/resources/research-repository/"
FUNDER_ID = 4320334749
PROVENANCE = "hrc_research_repository"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/hrc_nz/hrc_nz_projects.parquet"

HEADERS = {
    "User-Agent": "openalex-walden-hrc-nz/1.0 (+https://openalex.org)",
    "Accept": "text/html,application/xhtml+xml",
}

DEFAULT_OUTPUT_DIR = Path("/tmp")
REQUEST_TIMEOUT = (10, 90)
MAX_RETRIES = 5
MAX_PAGES = 200  # safety cap; real listing is ~65 pages

# Label -> output column. Values are the line immediately following the label
# in the server-rendered record page.
FIELD_LABELS = {
    "Year:": "year_raw",
    "Duration:": "duration_raw",
    "Approved budget:": "approved_budget_raw",
    "Researchers:": "researchers_raw",
    "Host:": "host_organization",
    "Health issue:": "health_issue",
    "Proposal type:": "proposal_type",
}

# Slugs that are navigation/section pages, not study records.
NON_RECORD_SLUGS = {"", "search"}


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def fetch(url: str, params: dict | None = None) -> str | None:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except requests.RequestException as exc:  # noqa: PERF203
            last_exc = exc
            sleep = min(2 ** attempt, 30)
            log(f"  retry {attempt}/{MAX_RETRIES} for {url} after {exc} (sleep {sleep}s)")
            time.sleep(sleep)
    log(f"  GIVING UP on {url}: {last_exc}")
    return None


def page_to_lines(htmltext: str) -> list[str]:
    """Strip scripts/styles/tags -> ordered list of non-empty visible lines."""
    import html as _html

    t = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", htmltext, flags=re.S)
    t = re.sub(r"<[^>]+>", "\n", t)
    t = _html.unescape(t)
    return [ln.strip() for ln in t.splitlines() if ln.strip()]


def extract_slugs(listing_html: str) -> list[str]:
    slugs = []
    for m in re.finditer(r'href="[^"]*' + re.escape(RECORD_PREFIX) + r'([a-z0-9][a-z0-9\-]{6,})"', listing_html):
        slug = m.group(1)
        if slug not in NON_RECORD_SLUGS:
            slugs.append(slug)
    # preserve order, dedupe
    seen = set()
    out = []
    for s in slugs:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def enumerate_slugs(limit: int | None) -> list[str]:
    all_slugs: list[str] = []
    seen: set[str] = set()
    for page in range(0, MAX_PAGES):
        htmltext = fetch(LISTING_URL, params={"page": page})
        if not htmltext:
            break
        slugs = extract_slugs(htmltext)
        new = [s for s in slugs if s not in seen]
        if not new:
            # no new records on this page -> end of listing
            log(f"  page={page}: no new slugs; stopping enumeration")
            break
        for s in new:
            seen.add(s)
            all_slugs.append(s)
        log(f"  page={page}: +{len(new)} slugs (total {len(all_slugs)})")
        if limit and len(all_slugs) >= limit:
            return all_slugs[:limit]
        time.sleep(0.2)
    return all_slugs


def parse_record(slug: str, htmltext: str) -> dict[str, Any] | None:
    lines = page_to_lines(htmltext)
    # Title: the record page repeats the study title twice right before the
    # "Back to the HRC-funded research repository" line; take the line before it.
    title = None
    for i, ln in enumerate(lines):
        if ln.startswith("Back to the HRC-funded research repository"):
            if i >= 1:
                title = lines[i - 1]
            break
    record: dict[str, Any] = {
        "funder_award_id": f"hrc-{slug}",
        "slug": slug,
        "display_name": title,
        "landing_page_url": f"{BASE_URL}{RECORD_PREFIX}{slug}",
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
    # label -> next line
    for i, ln in enumerate(lines):
        if ln in FIELD_LABELS and i + 1 < len(lines):
            col = FIELD_LABELS[ln]
            if col not in record or not record.get(col):
                record[col] = lines[i + 1]
    # Lay summary: the paragraph(s) after the "Lay summary" heading, up to the
    # next structural marker.
    summary = None
    for i, ln in enumerate(lines):
        if ln.lower() == "lay summary":
            buf = []
            for nxt in lines[i + 1:]:
                if nxt in ("Sidebar anchor", "Back to top", "More menu anchor") or nxt.startswith("Main menu"):
                    break
                buf.append(nxt)
            summary = " ".join(buf).strip() or None
            break
    record["description"] = summary
    return record


_MONEY_RE = re.compile(r"[-+]?\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)")


def parse_amount(raw: Any) -> float | None:
    if not raw or not isinstance(raw, str):
        return None
    m = _MONEY_RE.search(raw)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def parse_year(raw: Any) -> int | None:
    if not raw or not isinstance(raw, str):
        return None
    m = re.search(r"(19|20)\d{2}", raw)
    return int(m.group(0)) if m else None


def parse_duration_months(raw: Any) -> int | None:
    if not raw or not isinstance(raw, str):
        return None
    m = re.search(r"(\d+)\s*month", raw, re.I)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*year", raw, re.I)
    if m:
        return int(m.group(1)) * 12
    return None


def build_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    # Ensure all expected columns exist even if some pages omitted a label.
    for col in [
        "funder_award_id", "slug", "display_name", "description",
        "researchers_raw", "host_organization", "approved_budget_raw",
        "year_raw", "duration_raw", "health_issue", "proposal_type",
        "landing_page_url", "downloaded_at",
    ]:
        if col not in df.columns:
            df[col] = None
    df["amount"] = df["approved_budget_raw"].map(parse_amount)
    df["currency"] = df["amount"].map(lambda a: "NZD" if a is not None else None)
    df["start_year"] = df["year_raw"].map(parse_year)
    df["duration_months"] = df["duration_raw"].map(parse_duration_months)
    # Force string dtype on text columns to avoid pandas int-inference on
    # null-heavy columns (runbook Step 1.2 item 5).
    for col in [
        "funder_award_id", "slug", "display_name", "description",
        "researchers_raw", "host_organization", "approved_budget_raw",
        "year_raw", "duration_raw", "health_issue", "proposal_type",
        "landing_page_url", "downloaded_at", "currency",
    ]:
        df[col] = df[col].astype("string")
    return df


def validate(df: pd.DataFrame) -> None:
    n = len(df)
    log(f"Local validation: {n} rows")

    def pct(col: str) -> None:
        c = int(df[col].notna().sum())
        log(f"  {col:<24}{c:>7}/{n}  ({100.0 * c / n:5.1f}%)" if n else f"  {col}: 0")

    for col in ["display_name", "description", "researchers_raw",
                "host_organization", "amount", "currency", "start_year",
                "proposal_type"]:
        pct(col)
    uniq = df["funder_award_id"].nunique()
    log(f"  unique funder_award_id {uniq}/{n}")
    if n:
        yrs = df["start_year"].dropna()
        if len(yrs):
            log(f"  year range {int(yrs.min())}-{int(yrs.max())}")
        amts = df["amount"].dropna()
        if len(amts):
            log(f"  amount range NZD {amts.min():,.0f}-{amts.max():,.0f}; total NZD {amts.sum():,.0f}")


def upload_to_s3(local_path: Path, allow_shrink: bool) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for upload; use --skip-upload locally.") from exc

    client = boto3.client("s3")
    new_rows = len(pd.read_parquet(local_path))
    log(f"Runbook section 1.4 shrink-check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev_path = local_path.with_suffix(".prev.parquet")
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_rows = len(pd.read_parquet(prev_path))
        log(f"  previous S3 rows={prev_rows:,}, new rows={new_rows:,}")
        if new_rows < prev_rows and not allow_shrink:
            raise RuntimeError(
                f"Refusing to shrink: new {new_rows:,} < previous {prev_rows:,}. "
                f"Use --allow-shrink to override."
            )
    except client.exceptions.ClientError:
        log("  No existing S3 parquet found; treating this as first ingest.")

    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    client.upload_file(str(local_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="HRC NZ Research Repository -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit", type=int, default=None, help="Smoke-test: stop after N records")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--record-sleep", type=float, default=0.15)
    args = parser.parse_args()

    log("=" * 72)
    log("HRC NZ Research Repository ingest starting")
    log(f"source={LISTING_URL}")
    log(f"funder_id={FUNDER_ID}")
    log(f"provenance={PROVENANCE}")

    slugs = enumerate_slugs(args.limit)
    log(f"Enumerated {len(slugs)} study slugs")
    if not slugs:
        log("No slugs found; aborting.")
        sys.exit(1)

    records = []
    for idx, slug in enumerate(slugs, 1):
        htmltext = fetch(f"{BASE_URL}{RECORD_PREFIX}{slug}")
        if not htmltext:
            log(f"  [{idx}/{len(slugs)}] {slug}: fetch failed, skipping")
            continue
        rec = parse_record(slug, htmltext)
        if rec and rec.get("display_name"):
            records.append(rec)
        if idx % 100 == 0:
            log(f"  parsed {idx}/{len(slugs)}")
        time.sleep(args.record_sleep)

    df = build_dataframe(records)
    validate(df)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "hrc_nz_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"Wrote {out} ({out.stat().st_size:,} bytes)")

    if args.skip_upload:
        log("--skip-upload set; not uploading to S3.")
        return
    upload_to_s3(out, args.allow_shrink)


if __name__ == "__main__":
    main()
