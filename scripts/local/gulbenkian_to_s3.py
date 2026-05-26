#!/usr/bin/env python3
"""
Calouste Gulbenkian Foundation Projects → S3 Pipeline (GRANT PATTERN, method-2 WordPress REST API)
==================================================================================================

Downloads funded projects from Fundação Calouste Gulbenkian's own
WordPress REST API at gulbenkian.pt. The foundation is Portugal's
largest private cultural and scientific philanthropic organization,
funding projects in arts, sciences, education, and social cohesion.

Discovery (method-2 REST API): clean public WordPress endpoint at
`/wp-json/wp/v2/project` with ~401 funded project records. Each
project has rich ACF metadata: date_start, date_end, local
(location), partners, duration, beneficiaries, budget (when
published), plus taxonomies for intervention domain, county,
methodology, phase, beneficiary age, skills.

Awarding body in OpenAlex:
  Fundação Calouste Gulbenkian (F4320323335, PT, DOI 10.13039/501100005635).

The GB-domiciled branch (F4320320052) is the London-based UK Branch
of the foundation; this ingest targets the PT main organization.

Amount handling:
  amount/currency are populated from ACF `budget` where present
  (EUR hardcoded — Gulbenkian is Portugal-based and lists budget
  in euros). Many older projects leave `budget` empty; NULL is
  correct in that case. NOT a global §6.7 waiver — when the
  foundation publishes a budget, we capture it.

Output
------
  s3://openalex-ingest/awards/gulbenkian/gulbenkian_projects.parquet

Usage
-----
    python gulbenkian_to_s3.py                                  # full run
    python gulbenkian_to_s3.py --skip-upload                    # local dev
    python gulbenkian_to_s3.py --limit 10                       # smoke
    python gulbenkian_to_s3.py --skip-download --skip-upload    # reuse cache
    python gulbenkian_to_s3.py --allow-shrink                   # override §1.4

Requirements
------------
    pip install pandas pyarrow requests boto3
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

# Bare domain (no www) to avoid 301 redirects on every call
BASE_URL = "https://gulbenkian.pt/wp-json/wp/v2"
PROJECTS_URL = f"{BASE_URL}/project"

FUNDER_ID = 4320323335
FUNDER_DISPLAY_NAME = "Fundação Calouste Gulbenkian"

PROVENANCE = "gulbenkian_projects"
CURRENCY = "EUR"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/gulbenkian/gulbenkian_projects.parquet"

USER_AGENT = "openalex-walden-gulbenkian-ingest/1.0 (+https://openalex.org)"

REQUEST_DELAY_S = 0.3
PER_PAGE = 100  # WP REST max
DEFAULT_CACHE = Path(".cache/gulbenkian_projects.json")

# Strip "€" and Portuguese thousand separators (".") and decimal commas
AMOUNT_NUMBER_RE = re.compile(r"([\d.,]+)")


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# =============================================================================
# HTTP + cache
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get_json(url: str, timeout: int = 30):
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    elapsed = time.monotonic() - _last_request_t
    if elapsed < REQUEST_DELAY_S:
        time.sleep(REQUEST_DELAY_S - elapsed)
    resp = _session.get(url, timeout=timeout)
    _last_request_t = time.monotonic()
    resp.raise_for_status()
    return resp.json()


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
            batch = _http_get_json(url)
        except requests.HTTPError as e:
            if e.response.status_code == 400 and page > 1:
                log(f"    end of pages at page {page}")
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


def parse_amount(s: Optional[str]) -> Optional[float]:
    """Parse "€ 12.500,00" / "12500" / "12.500" forms into a float.
    Portuguese convention uses "." as thousand separator and "," as decimal."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip().replace("€", "").replace("EUR", "").strip()
    if not s:
        return None
    m = AMOUNT_NUMBER_RE.search(s)
    if not m:
        return None
    raw = m.group(1)
    # If raw has both "." and ",", "." is thousand-sep and "," is decimal: remove "." and replace "," with "."
    if "." in raw and "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif "," in raw:
        # Comma may be decimal (Portuguese) — assume so
        raw = raw.replace(",", ".")
    else:
        # Only "." — treat as decimal if ≤ 3 digits trailing, else thousand-sep
        parts = raw.split(".")
        if len(parts) > 1 and len(parts[-1]) <= 2:
            # decimal
            pass
        else:
            raw = raw.replace(".", "")
    try:
        val = float(raw)
    except ValueError:
        return None
    if val < 1:
        return None
    return val


YEAR_RE = re.compile(r"\b(19\d{2}|20[0-3]\d)\b")


def parse_year(s) -> Optional[int]:
    if s is None or s == "":
        return None
    s = str(s).strip()
    if s.isdigit() and 1900 <= int(s) <= 2030:
        return int(s)
    m = YEAR_RE.search(s)
    if m:
        return int(m.group(1))
    return None


def parse_project(p: dict) -> dict:
    title = strip_html(p.get("title", {}).get("rendered", ""))
    excerpt = strip_html(p.get("excerpt", {}).get("rendered", ""))
    content = strip_html(p.get("content", {}).get("rendered", ""))
    description = excerpt or (content[:5000] if content else None)
    page_date = p.get("date", "")
    page_year = int(page_date[:4]) if page_date[:4].isdigit() else None

    acf = p.get("acf") or {}
    if not isinstance(acf, dict):
        acf = {}

    start_year = parse_year(acf.get("date_start")) or page_year
    end_year = parse_year(acf.get("date_end"))

    budget_raw = acf.get("budget") if isinstance(acf.get("budget"), str) else None
    amount = parse_amount(budget_raw)

    return {
        "project_id":    str(p.get("id")),
        "slug":          p.get("slug"),
        "title":         title,
        "link":          p.get("link"),
        "date":          page_date,
        "page_year":     page_year,
        "start_year":    start_year,
        "end_year":      end_year,
        "date_start":    acf.get("date_start"),
        "date_end":      acf.get("date_end"),
        "local":         acf.get("local"),
        "partners":      acf.get("partners"),
        "duration":      acf.get("duration"),
        "beneficiaries": acf.get("beneficiaries"),
        "budget_raw":    budget_raw,
        "amount":        amount,
        "description":   description,
        "content_full":  content[:8000] if content else None,
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError("No project rows parsed")
    n = len(rows)
    for f in ("title", "slug", "start_year", "description", "amount", "local", "partners"):
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<18} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")

    slugs = [r["slug"] for r in rows if r.get("slug")]
    if len(slugs) != len(set(slugs)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(slugs).items() if v > 1][:5]
        log(f"  slug NOT unique ({len(slugs) - len(set(slugs))} duplicates, e.g. {dups}); "
            f"build_dataframe will fall back to slug+project_id for funder_award_id.")
    else:
        log(f"  slug uniqueness: {len(slugs)}/{n} distinct ✓")

    # project_id IS canonical; verify
    ids = [r["project_id"] for r in rows if r.get("project_id")]
    if len(ids) != len(set(ids)):
        raise RuntimeError("project_id NOT unique — investigate; should be the WP post ID")
    log(f"  project_id uniqueness: {len(ids)}/{n} distinct ✓")

    amts = [r["amount"] for r in rows if r.get("amount") is not None]
    if amts:
        log(f"  amount stats: n={len(amts)} min=€{min(amts):,.0f} max=€{max(amts):,.0f} total=€{sum(amts):,.0f}")


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    # funder_award_id = slug + project_id (project_id always present, disambiguates slug repeats)
    df["funder_award_id"] = "gulbenkian-" + df["slug"].astype(str) + "-" + df["project_id"].astype(str)
    df["currency"] = pd.Series([CURRENCY if a is not None else None for a in df["amount"]], dtype="object")
    df = df.astype("string")
    return df


# =============================================================================
# Shrink-check + upload
# =============================================================================

def check_no_shrink(new_count: int, allow_shrink: bool) -> bool:
    if allow_shrink:
        log("  --allow-shrink set; skipping §1.4 shrink-check")
        return True
    try:
        import boto3
        import io
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
    parser = argparse.ArgumentParser(description="Fetch Gulbenkian projects → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "gulbenkian_projects.parquet"

    log(f"=== Gulbenkian projects ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache={args.cache}")

    cache = load_cache(args.cache)

    projects = fetch_projects(args.skip_download, cache)
    save_cache(args.cache, cache)
    log(f"Fetched {len(projects)} projects")

    if args.limit is not None:
        projects = projects[:args.limit]
        log(f"--limit {args.limit}: processing {len(projects)} projects")

    log("Parsing rows...")
    rows = [parse_project(p) for p in projects]
    rows = [r for r in rows if r.get("title")]
    log(f"  {len(rows)} parsed rows")

    validate_rows(rows)

    log("Building DataFrame...")
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.1f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Gulbenkian ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== Gulbenkian ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
