#!/usr/bin/env python3
"""
Velux Stiftung Projects → S3 Pipeline (GRANT PATTERN, hybrid REST + HTML)
==========================================================================

Downloads funded projects from Velux Stiftung's own WordPress at
veluxstiftung.ch. Velux Stiftung is the Switzerland-based Velux
foundation (distinct from the Danish Velux Fonden), funding Daylight
Research, Forestry, Healthy Ageing, and Ophthalmology.

Discovery (hybrid method-2 REST + method-5 static HTML):
  - `/wp/v2/projects` REST endpoint gives the canonical project list
    (31 projects total) with title, slug, content, projects_type.
  - Per-project detail page (~110KB) carries structured labeled
    fields not exposed via REST: "Funding amount: CHF N", "YYYY -
    YYYY" project period.

Awarding body in OpenAlex:
  Velux Stiftung (F4320309607, CH, DOI 10.13039/100007214).

Currency hardcoded CHF.

Output
------
  s3://openalex-ingest/awards/velux_stiftung/velux_stiftung_projects.parquet

Usage
-----
    python velux_stiftung_to_s3.py                                  # full run (~30s for 31 pages)
    python velux_stiftung_to_s3.py --skip-upload                    # local dev
    python velux_stiftung_to_s3.py --limit 5                        # smoke
    python velux_stiftung_to_s3.py --skip-download --skip-upload    # reuse cache
    python velux_stiftung_to_s3.py --allow-shrink                   # override §1.4

Requirements
------------
    pip install pandas pyarrow requests beautifulsoup4 boto3
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- Windows UTF-8 compatibility shim (fleet 2026-05-22) -----------------
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

# =============================================================================
# Configuration
# =============================================================================

PROJECTS_URL = "https://veluxstiftung.ch/wp-json/wp/v2/projects"
TYPES_URL = "https://veluxstiftung.ch/wp-json/wp/v2/projects_type"

FUNDER_ID = 4320309607
FUNDER_DISPLAY_NAME = "Velux Stiftung"

PROVENANCE = "velux_stiftung_projects"
CURRENCY = "CHF"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/velux_stiftung/velux_stiftung_projects.parquet"

USER_AGENT = "openalex-walden-velux-stiftung-ingest/1.0 (+https://openalex.org)"

REQUEST_DELAY_S = 0.3
PER_PAGE = 100
DEFAULT_CACHE = Path(".cache/velux_stiftung.json")

# Page-level field patterns
PERIOD_RE = re.compile(r"\b(19\d{2}|20\d{2})\s*[-–—]\s*(19\d{2}|20\d{2})\b")
AMOUNT_RE = re.compile(
    r"Funding amount\s*[:|]?\s*CHF\s*([\d',.]+)",
    re.IGNORECASE,
)


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# =============================================================================
# HTTP + cache
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, accept: str = "text/html", timeout: int = 30) -> str:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT})
    elapsed = time.monotonic() - _last_request_t
    if elapsed < REQUEST_DELAY_S:
        time.sleep(REQUEST_DELAY_S - elapsed)
    resp = _session.get(url, headers={"Accept": accept}, timeout=timeout)
    _last_request_t = time.monotonic()
    resp.raise_for_status()
    return resp.text


def load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_cache(path: Path, cache: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False))


def fetch_projects(use_cache: bool, cache: dict) -> list[dict]:
    if use_cache and "projects" in cache:
        log(f"  cache hit: {len(cache['projects'])} projects")
        return cache["projects"]
    out = []
    page = 1
    while True:
        url = f"{PROJECTS_URL}?per_page={PER_PAGE}&page={page}"
        log(f"  GET projects page {page}")
        try:
            text = _http_get(url, "application/json")
            batch = json.loads(text)
        except requests.HTTPError as e:
            if e.response.status_code == 400 and page > 1:
                break
            raise
        if not batch:
            break
        out.extend(batch)
        log(f"    +{len(batch)} (running total {len(out)})")
        if len(batch) < PER_PAGE:
            break
        page += 1
    cache["projects"] = out
    return out


def fetch_types(use_cache: bool, cache: dict) -> dict[int, str]:
    if use_cache and "types" in cache:
        return {int(k): v for k, v in cache["types"].items()}
    text = _http_get(f"{TYPES_URL}?per_page=100", "application/json")
    items = json.loads(text)
    types = {item["id"]: item.get("name", "") for item in items}
    cache["types"] = types
    return types


def fetch_page_html(url: str, use_cache: bool, cache: dict) -> str:
    if "pages" not in cache:
        cache["pages"] = {}
    if use_cache and url in cache["pages"]:
        return cache["pages"][url]
    html = _http_get(url, "text/html")
    cache["pages"][url] = html
    return html


# =============================================================================
# Parse + content cleanup
# =============================================================================

HTML_TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def strip_html(html: str) -> str:
    if not html:
        return ""
    import html as _html
    text = HTML_TAG_RE.sub(" ", html)
    text = _html.unescape(text)
    text = WS_RE.sub(" ", text).strip()
    return text


def parse_amount(s: str) -> Optional[float]:
    """Parse "485,000" or "1'500'000" (Swiss) into a float."""
    if not s:
        return None
    raw = s.replace("'", "").replace(",", "").strip()
    # If raw still has period, treat as decimal if ≤ 2 trailing digits
    if "." in raw:
        parts = raw.split(".")
        if len(parts[-1]) > 2:
            raw = raw.replace(".", "")
    try:
        return float(raw)
    except ValueError:
        return None


def parse_page_html(html: str) -> tuple[Optional[float], Optional[int], Optional[int]]:
    """Extract (amount_chf, start_year, end_year) from project detail page."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    # Amount: "Funding amount: CHF 485,000"
    amount = None
    m = AMOUNT_RE.search(text)
    if m:
        amount = parse_amount(m.group(1))

    # Period: "2019 - 2023"
    start_year, end_year = None, None
    m = PERIOD_RE.search(text)
    if m:
        try:
            start_year, end_year = int(m.group(1)), int(m.group(2))
        except ValueError:
            pass

    return amount, start_year, end_year


def parse_project(p: dict, types: dict[int, str], cache: dict, use_cache: bool) -> dict:
    title = strip_html(p.get("title", {}).get("rendered", ""))
    content = strip_html(p.get("content", {}).get("rendered", ""))
    description = content[:5000] if content else None
    link = p.get("link")

    type_ids = p.get("projects_type") or []
    type_names = [types[t] for t in type_ids if t in types]

    amount, start_year, end_year = None, None, None
    if link:
        try:
            html = fetch_page_html(link, use_cache, cache)
            amount, start_year, end_year = parse_page_html(html)
        except Exception as exc:  # noqa: BLE001
            log(f"  page-fetch fail {link}: {type(exc).__name__}")

    return {
        "project_id":    str(p.get("id")),
        "slug":          p.get("slug"),
        "title":         title,
        "link":          link,
        "description":   description,
        "type_names":    " / ".join(type_names) if type_names else None,
        "amount":        amount,
        "start_year":    start_year,
        "end_year":      end_year,
        "page_date":     p.get("date"),
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError("No project rows parsed")
    n = len(rows)
    for f in ("title", "slug", "description", "amount", "start_year", "end_year", "type_names"):
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<14} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")

    slugs = [r["slug"] for r in rows if r.get("slug")]
    if len(slugs) != len(set(slugs)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(slugs).items() if v > 1][:5]
        raise RuntimeError(f"slug collisions: {dups}")
    log(f"  slug uniqueness: {len(slugs)}/{n} distinct ✓")

    amts = [r["amount"] for r in rows if r.get("amount") is not None]
    if amts:
        log(f"  amount stats: n={len(amts)} min=CHF {min(amts):,.0f} max=CHF {max(amts):,.0f} total=CHF {sum(amts):,.0f}")


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["funder_award_id"] = "velux-stiftung-" + df["slug"]
    df["currency"] = pd.Series([CURRENCY if a is not None else None for a in df["amount"]], dtype="object")
    df = df.astype("string")
    return df


# =============================================================================
# Shrink-check + upload (standard)
# =============================================================================

def check_no_shrink(new_count: int, allow_shrink: bool) -> bool:
    if allow_shrink:
        log("  --allow-shrink set; skipping §1.4 shrink-check")
        return True
    try:
        import boto3, io
        s3 = boto3.client("s3")
        prev_bytes = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)["Body"].read()
        prev_df = pd.read_parquet(io.BytesIO(prev_bytes))
        prev_count = len(prev_df)
        log(f"  §1.4 shrink-check: previous S3 parquet had {prev_count:,} rows")
        if new_count < prev_count:
            log(f"  §1.4 FAIL: new ({new_count:,}) < previous ({prev_count:,}). Aborting.")
            return False
        log(f"  §1.4 OK: new {new_count:,} >= previous {prev_count:,}")
        return True
    except Exception as e:
        log(f"  §1.4 shrink-check skipped: {type(e).__name__}: {str(e)[:100]}. (normal on first run)")
        return True


def upload_to_s3(local_file: Path) -> None:
    try:
        import boto3
    except ImportError:
        raise RuntimeError("boto3 required for S3 upload; pass --skip-upload for local only")
    log(f"Uploading {local_file} -> s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(local_file), S3_BUCKET, S3_KEY)
    log("  upload OK")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Velux Stiftung projects → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "velux_stiftung_projects.parquet"

    log(f"=== Velux Stiftung projects ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache={args.cache}")

    cache = load_cache(args.cache)

    types = fetch_types(args.skip_download, cache)
    log(f"  loaded {len(types)} project types: {list(types.values())[:5]}")
    projects = fetch_projects(args.skip_download, cache)
    save_cache(args.cache, cache)
    log(f"Fetched {len(projects)} projects from REST")

    if args.limit is not None:
        projects = projects[:args.limit]
        log(f"--limit {args.limit}: processing {len(projects)} projects")

    log("Parsing rows (fetching per-project detail HTML for amount/year)...")
    rows = []
    for i, p in enumerate(projects, 1):
        row = parse_project(p, types, cache, args.skip_download)
        if row.get("title"):
            rows.append(row)
        if i % 10 == 0:
            log(f"  [{i}/{len(projects)}] parsed (total so far: {len(rows)})")
            save_cache(args.cache, cache)
    save_cache(args.cache, cache)
    log(f"  {len(rows)} parsed rows")

    validate_rows(rows)

    log("Building DataFrame...")
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.1f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Velux Stiftung ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== Velux Stiftung ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
