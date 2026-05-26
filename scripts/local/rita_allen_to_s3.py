#!/usr/bin/env python3
"""
Rita Allen Foundation Scholars → S3 Pipeline (FELLOWSHIP PATTERN, method-5 static HTML)
========================================================================================

Downloads Rita Allen Foundation Scholars (and sibling programs like
Award in Pain Recipients, Civic Science Fellows etc. when listed in
the same per-year archive) from the foundation's own website at
ritaallen.org. The foundation publishes a per-year archive page at
`/scholar-year/{YYYY}/` and the per-year page renders each scholar's
record server-side with name, institution-at-award, slug, and a full
bio paragraph all inline. Discovery method-5 (static HTML), no auth,
no JS, no pagination.

Year-page archive enumerated via the WordPress sitemap at
`/wp-sitemap-taxonomies-scholar-year-1.xml` — 43 year terms span
1976-2016+. Each year page lists ~5-10 scholars in
`<nav class="scholars-list"><li>...</li></nav>` with this shape:

  <li>
    <a data-classburger-trigger="{slug}" href="#">
      <h4 class="title">{Scholar Name} +</h4>
      <span class="subtitle">{Institution at award}</span>
    </a>
    <div class="modal" data-classburger="{slug}">
      <div class="content rte-content">
        <p>{Full bio paragraph(s) — may begin with "(Award in X Recipient)" for sibling programs}</p>
      </div>
    </div>
  </li>

The avatar image URL on each card also typically encodes the award
year (`/app/uploads/{YYYY}/...`).

The bio sometimes begins with `(Award in Pain Recipient)` or
`(Civic Science Fellow)` — when present, the script extracts that as
the `program` field; otherwise the program defaults to the
foundation's main "Rita Allen Foundation Scholars" program.

Awarding body in OpenAlex:
  Rita Allen Foundation (F4320306590, US, no ROR, DOI 10.13039/100001447,
  Crossref 100001447).

Amount handling:
  Rita Allen publishes program-level amount ranges in narrative form
  on /programs pages (Scholars: $110,000/yr × 5 years = $550,000;
  Pain: $50,000/yr × 3 years; Civic Science: varies) but does NOT
  publish a verified per-scholar amount in any structured form on the
  scholar archive itself. Per the runbook's prize-pattern source-
  authority rule (which the fellowship pattern inherits), we leave
  amount/currency NULL with a §6.7 waiver rather than backfill from
  program-page narrative. The fellowship/scholar precedent here is
  HHMI (priority 44, NULL by design), CIFAR (priority 79, NULL by
  design), and Damon Runyon (priority 73, NULL by design).

Output
------
  s3://openalex-ingest/awards/rita_allen/rita_allen_scholars.parquet

Usage
-----
    python rita_allen_to_s3.py                                  # full run
    python rita_allen_to_s3.py --skip-upload                    # local dev
    python rita_allen_to_s3.py --limit 3                        # smoke (3 year pages)
    python rita_allen_to_s3.py --skip-download --skip-upload    # reuse cache
    python rita_allen_to_s3.py --allow-shrink                   # override §1.4

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

SITEMAP_URL = "https://ritaallen.org/wp-sitemap-taxonomies-scholar-year-1.xml"

FUNDER_ID = 4320306590
FUNDER_DISPLAY_NAME = "Rita Allen Foundation"

PROVENANCE = "rita_allen_scholars"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/rita_allen/rita_allen_scholars.parquet"

USER_AGENT = "openalex-walden-rita-allen-ingest/1.0 (+https://openalex.org)"

REQUEST_DELAY_S = 0.5  # polite throttle between page fetches
DEFAULT_CACHE = Path(".cache/rita_allen_pages.json")

# Match a parenthesized program qualifier early in the bio. Rita Allen's
# bio template puts the scholar name in an <a> tag, then the program
# qualifier in parens immediately after — `<a>Steve Davidson</a> (Award
# in Pain Recipient) earned a B.S....`. After `get_text()` strips the
# anchor whitespace this becomes `Steve Davidson(Award in Pain Recipient)
# earned...` with no separator, so the regex needs to find the
# parenthesized qualifier anywhere in the bio prefix (first 200 chars),
# not just at position 0.
#
# Only catch known program-tag patterns to avoid false positives on
# parenthesized text inside the prose (e.g., "(B.S. 2002)", "(MIT)").
PROGRAM_PREFIX_RE = re.compile(
    r"\(((?:Award in|Civic Science|Pain Scholars?|Scholars? in|Postdoctoral Fellow)[^)]{0,80})\)",
    re.IGNORECASE,
)


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def slugify(s: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")
    return s or "unknown"


# =============================================================================
# Name split (canonical helper per runbook §2.4.1)
# =============================================================================

_SUFFIX_TOKENS = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}


def split_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Split 'Chad A. Mirkin' -> ('Chad A.', 'Mirkin'). Strips degree/suffix tokens."""
    if not name:
        return None, None
    tokens = name.split()
    while tokens and tokens[-1].lower().strip(",.") in _SUFFIX_TOKENS:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


# =============================================================================
# HTTP + checkpoint cache
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 30) -> str:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,application/xml"})
    elapsed = time.monotonic() - _last_request_t
    if elapsed < REQUEST_DELAY_S:
        time.sleep(REQUEST_DELAY_S - elapsed)
    resp = _session.get(url, timeout=timeout)
    _last_request_t = time.monotonic()
    resp.raise_for_status()
    return resp.text


def load_checkpoint(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_checkpoint(path: Path, cache: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False))


def get_page(url: str, cache: dict[str, str], use_cache: bool) -> str:
    if use_cache and url in cache:
        return cache[url]
    html = _http_get(url)
    cache[url] = html
    return html


# =============================================================================
# Discovery + parse
# =============================================================================

def discover_year_pages(use_cache: bool, cache: dict[str, str]) -> list[tuple[int, str]]:
    """Return [(year, url), ...] enumerated from the WordPress scholar-year sitemap."""
    log(f"Fetching sitemap: {SITEMAP_URL}")
    xml = get_page(SITEMAP_URL, cache, use_cache)
    locs = re.findall(r"<loc>([^<]+)</loc>", xml)
    out = []
    for loc in locs:
        m = re.search(r"/scholar-year/(\d{4})/", loc)
        if m:
            out.append((int(m.group(1)), loc))
    out.sort(key=lambda x: x[0])
    log(f"  found {len(out)} year-archive URLs ({out[0][0]}-{out[-1][0]})")
    return out


def parse_year_page(html: str, year: int) -> list[dict]:
    """Extract one row per scholar from a /scholar-year/YYYY/ page."""
    soup = BeautifulSoup(html, "html.parser")
    nav = soup.find("nav", class_="scholars-list")
    if not nav:
        return []

    rows = []
    for li in nav.find_all("li", recursive=True):
        # Collapsed toggle: name (h4.title) + institution (span.subtitle)
        h4 = li.find("h4", class_="title")
        if not h4:
            continue
        name = h4.get_text(strip=True).rstrip(" +").strip()
        if not name:
            continue

        subtitle = li.find("span", class_="subtitle")
        institution = subtitle.get_text(strip=True) if subtitle else None

        # Slug from the data-classburger-trigger attribute on the <a>
        a = li.find("a", attrs={"data-classburger-trigger": True})
        slug = (a.get("data-classburger-trigger") if a else None) or slugify(name)

        # Bio from the modal's .content.rte-content
        modal = li.find("div", class_="modal")
        bio = None
        if modal:
            content = modal.find("div", class_="content")
            if content:
                # Join paragraphs into one string, separated by blank lines
                paragraphs = [p.get_text(strip=True) for p in content.find_all("p") if p.get_text(strip=True)]
                if paragraphs:
                    bio = "\n\n".join(paragraphs)

        # Detect program qualifier in bio prefix (first 200 chars).
        # Examples: "(Award in Pain Recipient)", "(Civic Science Fellow)".
        program = None
        if bio:
            m = PROGRAM_PREFIX_RE.search(bio[:200])
            if m:
                program = m.group(1).strip()

        # Avatar URL — sometimes hints at the year via /uploads/YYYY/MM/
        avatar_url = None
        figure = li.find("figure", class_="avatar")
        if figure and figure.get("style"):
            m = re.search(r"url\(['\"]?(https?://[^)'\"]+)['\"]?\)", figure["style"])
            if m:
                avatar_url = m.group(1)

        # Lab/homepage URL inside the modal's first external link
        lab_url = None
        if modal:
            for a_el in modal.find_all("a", href=True):
                href = a_el["href"]
                if href.startswith("http") and "ritaallen.org" not in href:
                    lab_url = href
                    break

        given_name, family_name = split_name(name)

        rows.append({
            "scholar_name":      name,
            "given_name":        given_name,
            "family_name":       family_name,
            "slug":              slug,
            "award_year":        year,
            "institution":       institution,
            "program":           program,  # None when bio has no parenthesized prefix
            "bio":               bio,
            "lab_url":           lab_url,
            "avatar_url":        avatar_url,
            "scholar_year_url":  f"https://ritaallen.org/scholar-year/{year}/",
        })

    return rows


# =============================================================================
# Validation + DataFrame
# =============================================================================

def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError("No scholar rows extracted")

    # Coverage
    n = len(rows)
    for f in ["scholar_name", "award_year", "institution", "bio"]:
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<18} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")

    # funder_award_id uniqueness — slug + year must be unique
    ids = [f"rita-allen-{r['award_year']}-{r['slug']}" for r in rows]
    if len(ids) != len(set(ids)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(ids).items() if v > 1][:5]
        raise RuntimeError(
            f"funder_award_id collisions: {len(ids) - len(set(ids))} duplicates. "
            f"Example: {dups}. Slug-uniqueness within a year is the prize-pattern "
            f"invariant per runbook prize table row 6 — investigate before parquet write."
        )
    log(f"  funder_award_id uniqueness: {len(ids)}/{n} distinct ✓")


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["funder_award_id"] = "rita-allen-" + df["award_year"].astype(str) + "-" + df["slug"]
    # Runbook §1.2.5: force string dtype before to_parquet.
    df = df.astype("string")
    return df


# =============================================================================
# Shrink-check (runbook §1.4)
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
    parser = argparse.ArgumentParser(description="Fetch Rita Allen Scholars → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"),
                        help="Output directory; parquet written to {output-dir}/rita_allen_scholars.parquet")
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE,
                        help="Checkpoint cache for fetched year pages")
    parser.add_argument("--skip-download", action="store_true",
                        help="Use cached year-page HTML (must have run once)")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Write parquet locally but skip S3 upload")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook §1.4 shrink-check. Use only after confirming intent.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit to first N year pages (smoke test)")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "rita_allen_scholars.parquet"

    log(f"=== Rita Allen Scholars ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache={args.cache}")

    cache = load_checkpoint(args.cache)
    log(f"  loaded {len(cache)} cached pages")

    year_pages = discover_year_pages(args.skip_download, cache)
    save_checkpoint(args.cache, cache)

    if args.limit is not None:
        year_pages = year_pages[:args.limit]
        log(f"--limit {args.limit}: processing {len(year_pages)} year pages")

    all_rows = []
    for i, (year, url) in enumerate(year_pages, 1):
        html = get_page(url, cache, args.skip_download)
        rows = parse_year_page(html, year)
        log(f"  [{i:2}/{len(year_pages)}] year={year}: {len(rows)} scholars  (total so far: {len(all_rows) + len(rows)})")
        all_rows.extend(rows)
        # Persist cache periodically
        if i % 5 == 0:
            save_checkpoint(args.cache, cache)

    save_checkpoint(args.cache, cache)

    log(f"Parsed {len(all_rows)} scholar rows across {len(year_pages)} year pages")
    validate_rows(all_rows)

    log("Building DataFrame...")
    df = build_dataframe(all_rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")

    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.1f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== Rita Allen Scholars ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed. See above; re-run with --allow-shrink if intentional.")

    upload_to_s3(output_path)
    log("=== Rita Allen Scholars ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
