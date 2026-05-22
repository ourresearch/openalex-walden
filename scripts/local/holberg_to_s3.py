#!/usr/bin/env python3
"""
Holberg Prize + Nils Klim Prize to S3 Data Pipeline
====================================================

Downloads laureate records for both the Holberg Prize and the sister
Nils Klim Prize from the holbergprisen.no WordPress REST API, flattens
them to the awards-pipeline parquet schema, and uploads to S3.

Both prizes are administered by Universitetet i Bergen (UiB) on behalf
of the Norwegian government; we map them to the single OpenAlex funder
F4320323078 and distinguish them via `funder_scheme`.

Source authority: holbergprisen.no is the awarding body's own site, in
keeping with the prize-pattern source-authority rule (no Wikipedia /
Wikidata fallback).

Data source
-----------
WordPress REST API at https://holbergprisen.no/wp-json/wp/v2/
  - /bc_prisvinner       : laureate posts (custom post type)
  - /bc_prize            : prize-type taxonomy (Holberg / Nils Klim / Schools)
  - /arstall             : year taxonomy
  - /land                : country taxonomy

No auth required. Public, anonymous reads. WordPress's default 10
records/page is bumped to per_page=100 — the corpus is 46 records total
so one request fetches everything.

Output
------
s3://openalex-ingest/awards/holberg/holberg_laureates.parquet

Usage
-----
    python holberg_to_s3.py                # download + upload (default)
    python holberg_to_s3.py --skip-upload  # local dev / smoke test
    python holberg_to_s3.py --skip-download # reuse cached records.json

Requirements
------------
    pip install pandas pyarrow requests

    AWS CLI configured for write access to s3://openalex-ingest/awards/holberg/
"""

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). This
# crashes scrapers writing laureate names with non-ASCII chars (Polish ł,
# Turkish ğ, Greek μ, combining accents, zero-width spaces). Production
# runs on Linux/Databricks where UTF-8 is the default, but this fixes
# local validation on Windows without requiring contractors to set
# PYTHONUTF8=1 in their environment. See runbook §1.2.
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

WP_BASE = "https://holbergprisen.no/wp-json/wp/v2"

# Per-prize hardcoded amounts (public, documented on holbergprisen.no).
# Slug → (display name, amount NOK, currency)
PRIZE_META = {
    "holbergprisen":     ("Holberg Prize",       6_000_000, "NOK"),
    "nils-klim-prisen":  ("Nils Klim Prize",       500_000, "NOK"),
    # bc_prize/62 'Holbergprisen i skolen' is a teaching program, not a
    # research prize — explicitly excluded below.
}
EXCLUDED_PRIZE_SLUGS = {"holbergprisen-i-skolen"}

# Awarding body — Universitetet i Bergen.
# Verified in OpenAlex public API: F4320323078, country=NO.
FUNDER_ID = 4320323078
FUNDER_DISPLAY_NAME = "Universitetet i Bergen"

PROVENANCE = "holberg_wp_rest"

# S3 destination — matches awards pipeline convention.
S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/holberg/holberg_laureates.parquet"

# Polite rate — WP REST endpoints handle this easily but we throttle anyway.
MIN_REQUEST_INTERVAL_S = 0.25
USER_AGENT = "openalex-walden-holberg-ingest/1.0 (+https://openalex.org)"


# =============================================================================
# HTTP helper
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, params: Optional[dict] = None, timeout: int = 60) -> requests.Response:
    """GET with throttle, single shared Session, verbose logging."""
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    qs = ""
    if params:
        qs = "?" + "&".join(f"{k}={v}" for k, v in params.items())
    print(f"  GET {url}{qs}")
    resp = _session.get(url, params=params, timeout=timeout)
    _last_request_t = time.monotonic()
    print(f"    -> {resp.status_code} {resp.reason}  len={len(resp.content)}")
    resp.raise_for_status()
    return resp


def _fetch_taxonomy(slug: str) -> dict[int, dict]:
    """Fetch all terms of a WP taxonomy as {id: term_dict}."""
    print(f"\n  Caching taxonomy: {slug}")
    out: dict[int, dict] = {}
    page = 1
    while True:
        resp = _http_get(f"{WP_BASE}/{slug}", params={"per_page": 100, "page": page})
        batch = resp.json()
        if not batch:
            break
        for t in batch:
            out[t["id"]] = t
        # WP exposes total pages in headers; break if we got fewer than asked.
        if len(batch) < 100:
            break
        page += 1
    print(f"    cached {len(out)} {slug} terms")
    return out


# =============================================================================
# Smoke-test gate (runbook §1)
# =============================================================================

def smoke_test() -> None:
    """Quick probe to confirm the REST API is reachable and shape matches."""
    print("\n" + "=" * 60)
    print("Smoke test: confirm WP REST API is reachable")
    print("=" * 60)
    resp = _http_get(f"{WP_BASE}/bc_prisvinner", params={"per_page": 1, "page": 1})
    total = resp.headers.get("X-WP-Total")
    if not total:
        print("[ERROR] no X-WP-Total header — API shape changed?")
        sys.exit(3)
    print(f"  [OK] reachable; X-WP-Total={total}")
    # Sanity: expect ~46 laureates as of 2026. Warn (not fail) if very different.
    try:
        n = int(total)
        if n < 30 or n > 200:
            print(f"  [WARN] laureate count {n} is outside expected 30-200 — "
                  f"check whether bc_prisvinner now includes other post types.")
    except ValueError:
        pass


# =============================================================================
# Download
# =============================================================================

def download_laureates(output_dir: Path) -> Path:
    """Fetch all bc_prisvinner posts; resolve taxonomy FKs by ID."""
    print("\n" + "=" * 60)
    print("Step 1: Download all laureates")
    print("=" * 60)

    # 1) cache the small taxonomies up front so we can resolve FKs without
    # firing one request per laureate.
    prize_terms   = _fetch_taxonomy("bc_prize")
    arstall_terms = _fetch_taxonomy("arstall")
    land_terms    = _fetch_taxonomy("land")

    # 2) fetch all laureates in one page (corpus is ~46).
    laureates: list[dict] = []
    page = 1
    while True:
        resp = _http_get(
            f"{WP_BASE}/bc_prisvinner",
            params={"per_page": 100, "page": page, "_embed": "false"},
        )
        batch = resp.json()
        if not batch:
            break
        laureates.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    print(f"\n  Fetched {len(laureates)} laureate posts")

    # 3) enrich each with resolved taxonomy names + raw HTML for parsing.
    out_records: list[dict] = []
    for p in laureates:
        prize_ids = p.get("bc_prize") or []
        prize_name = None
        prize_slug = None
        for pid in prize_ids:
            term = prize_terms.get(pid)
            if not term:
                continue
            if term["slug"] in EXCLUDED_PRIZE_SLUGS:
                prize_slug = None
                break
            prize_name = term["name"]
            prize_slug = term["slug"]
        if not prize_slug:
            # Either no prize tag or it's the excluded schools program.
            continue

        # Year — single value per laureate.
        arstall_ids = p.get("arstall") or []
        year_name = None
        for aid in arstall_ids:
            term = arstall_terms.get(aid)
            if term:
                year_name = term["name"]
                break

        # Country — laureates often have nationality + working country.
        # We keep both; the notebook uses the first as primary, second as
        # affiliation country.
        country_names = []
        for lid in p.get("land") or []:
            term = land_terms.get(lid)
            if term:
                country_names.append(term["name"])

        out_records.append({
            "wp_id":          p["id"],
            "slug":           p["slug"],
            "title":          unescape(p["title"]["rendered"]),
            "link":           p["link"],
            "date_posted":    p.get("date"),
            "prize_slug":     prize_slug,
            "prize_name":     prize_name,
            "year":           year_name,
            "country_names":  "|".join(country_names) if country_names else None,
            "content_html":   p["content"]["rendered"],
        })

    # Persist raw records to disk so --skip-download can be useful next run.
    raw_path = output_dir / "holberg_laureates_raw.json"
    raw_path.write_text(json.dumps(out_records, ensure_ascii=False, indent=2))
    print(f"\n  Cached {len(out_records)} records to {raw_path}")
    print(f"  By prize: " +
          ", ".join(f"{k}={sum(1 for r in out_records if r['prize_slug']==k)}"
                    for k in PRIZE_META))
    return raw_path


# =============================================================================
# Parse: extract citation + split laureate name
# =============================================================================

# Strip HTML tags but preserve text content.
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE  = re.compile(r"\s+")


def _strip_html(html: str) -> str:
    text = _TAG_RE.sub(" ", html)
    text = unescape(text)
    return _WS_RE.sub(" ", text).strip()


# Citation pattern: "{prize_name} {year} tildeles {nationality-adj} {name} for {citation}."
# The "for ..." clause is the citation we want.
_CITATION_RE = re.compile(
    r"(?:Holbergprisen|Nils Klim-prisen)\s+\d{4}\s+tildeles\s+[^.]*?\s+for\s+(.+?)\.",
    flags=re.IGNORECASE | re.DOTALL,
)


def extract_citation(content_html: str) -> Optional[str]:
    """First-sentence citation from the cited content HTML, or NULL."""
    text = _strip_html(content_html)
    m = _CITATION_RE.search(text)
    if not m:
        return None
    citation = m.group(1).strip()
    # Strip leading "hennes/hans/dens" etc and any trailing whitespace.
    return citation if citation else None


# Conservative name-split: family = last token, given = everything before.
# Strips trailing degree suffixes that occasionally appear in titles.
_DEGREE_SUFFIXES = {"PhD", "MD", "DPhil", "Jr.", "Sr.", "II", "III", "IV"}


def split_name(full: str) -> tuple[str, str]:
    """Return (given, family) from "First Middle Last [Suffix]" style."""
    tokens = full.split()
    while tokens and tokens[-1].rstrip(".") in {s.rstrip(".") for s in _DEGREE_SUFFIXES}:
        tokens.pop()
    if not tokens:
        return ("", "")
    if len(tokens) == 1:
        return ("", tokens[0])
    return (" ".join(tokens[:-1]), tokens[-1])


# =============================================================================
# Build DataFrame
# =============================================================================

def build_dataframe(raw_path: Path) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Build flat DataFrame")
    print("=" * 60)

    records = json.loads(raw_path.read_text())
    rows: list[dict] = []
    seen_award_ids: set[str] = set()

    for r in records:
        prize_slug = r["prize_slug"]
        prize_display, amount, currency = PRIZE_META[prize_slug]
        year = r["year"]
        given, family = split_name(r["title"])
        citation = extract_citation(r["content_html"])
        # Slug-collision detection MUST raise per runbook prize table row 6:
        # duplicate funder_award_id silently merges rows downstream.
        funder_award_id = f"{prize_slug}-{year}-{r['slug']}"
        if funder_award_id in seen_award_ids:
            raise RuntimeError(
                f"Duplicate funder_award_id detected: {funder_award_id!r}. "
                "Prize-pattern requires unique IDs per (prize × laureate). "
                "Inspect raw records before re-running."
            )
        seen_award_ids.add(funder_award_id)

        # Countries: first is nationality (primary), second (if present) is
        # working country. Notebook uses both.
        country_names = (r.get("country_names") or "").split("|") if r.get("country_names") else []
        nationality = country_names[0] if country_names else None
        affiliation_country = country_names[1] if len(country_names) > 1 else None

        rows.append({
            "funder_award_id":          funder_award_id,
            "prize_slug":               prize_slug,
            "prize_name":               prize_display,
            "year":                     year,
            "laureate_full_name":       r["title"],
            "laureate_given_name":      given,
            "laureate_family_name":     family,
            "nationality":              nationality,
            "affiliation_country_raw":  affiliation_country,
            "description":              citation,
            "amount":                   amount,
            "currency":                 currency,
            "landing_page_url":         r["link"],
            "wp_post_id":               r["wp_id"],
            "declined":                 False,  # no declined Holberg laureates on record
        })

    df = pd.DataFrame.from_records(rows)
    print(f"  Built DataFrame: {len(df)} rows × {len(df.columns)} cols")
    print(f"  By prize:")
    print(df.groupby("prize_name").size().to_string())
    print(f"  By year (first 5 and last 5):")
    yrs = df["year"].value_counts().sort_index()
    print(yrs.head().to_string())
    print(f"  ...")
    print(yrs.tail().to_string())

    # Runbook §1.2.5: cast every column to string before parquet so pyarrow
    # doesn't int-infer null-heavy text columns and break downstream COALESCE.
    df = df.astype("string")
    return df


# =============================================================================
# Write parquet + upload
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "holberg_laureates.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df)} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def check_no_shrink(new_count: int, output_dir: Path, allow_shrink: bool) -> bool:
    """
    Runbook §1.4 — never shrink the corpus on re-ingest. Read the existing
    S3 parquet's row count; if the new dataframe has fewer rows, abort.
    Returns True if it's safe to proceed; False if the user must override
    with --allow-shrink.
    """
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  Re-ingest safety check vs {s3_uri}")

    # Probe existence without downloading.
    head = subprocess.run(
        ["aws", "s3api", "head-object",
         "--bucket", S3_BUCKET, "--key", S3_KEY],
        capture_output=True, text=True,
    )
    if head.returncode != 0:
        # Most common reason on first ingest is "404 Not Found"
        if "Not Found" in head.stderr or "NoSuchKey" in head.stderr:
            print("  [OK] no existing parquet at S3 path — first ingest, no shrink check.")
            return True
        print(f"  [WARN] head-object failed: {head.stderr.strip()[:200]}")
        print(f"         proceeding without comparison (treat as first ingest)")
        return True

    # File exists — pull row count.
    prev_path = output_dir / "_prev_holberg_laureates.parquet"
    pull = subprocess.run(
        ["aws", "s3", "cp", s3_uri, str(prev_path)],
        capture_output=True, text=True,
    )
    if pull.returncode != 0:
        print(f"[ERROR] couldn't fetch existing parquet for shrink check: "
              f"{pull.stderr.strip()[:200]}")
        return False
    try:
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as e:
        print(f"[ERROR] couldn't read existing parquet ({e}); aborting upload "
              f"to avoid clobbering unknown data. Re-run with --allow-shrink "
              f"if you've verified the previous file is corrupt or empty.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)

    print(f"  previous count: {prev_count:,}   new count: {new_count:,}")
    if new_count < prev_count:
        if allow_shrink:
            print(f"  [OVERRIDE] new < previous but --allow-shrink set; proceeding.")
            return True
        print(f"\n[ERROR] §1.4 violation: refusing to shrink corpus "
              f"({prev_count:,} → {new_count:,}). Cause is almost always a "
              f"source-side partial outage, schema change, or pagination "
              f"bug, NOT a genuine retraction of past prizes. Investigate "
              f"first. To override after confirming the shrink is "
              f"intentional, re-run with --allow-shrink.")
        return False
    print(f"  [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame,
                 output_dir: Path, allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 4: Upload to S3 (with §1.4 shrink check)")
    print("=" * 60)

    if not check_no_shrink(len(df), output_dir, allow_shrink):
        return False

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  Uploading {parquet_path} -> {s3_uri}")
    try:
        subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
        print(f"  [OK] uploaded to {s3_uri}")
        return True
    except FileNotFoundError:
        print("[ERROR] aws CLI not found. brew install awscli, then re-run.")
        return False
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] aws s3 cp failed (exit {e.returncode}).")
        return False


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/holberg"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse holberg_laureates_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3 (local dev)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override the §1.4 shrink-check. Only use when "
                             "you've manually confirmed a smaller corpus is "
                             "intentional.")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Holberg Prize + Nils Klim Prize → S3 Pipeline")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "holberg_laureates_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        print(f"\n[SKIP] reusing {raw_path}")
    else:
        raw_path = download_laureates(args.output_dir)

    df = build_dataframe(raw_path)
    parquet_path = write_parquet(df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload; manual upload command:")
        print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")
    else:
        ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
        if not ok:
            print(f"\n[WARN] upload failed or aborted. Parquet at {parquet_path};")
            print(f"  manually after investigating:")
            print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(7)

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print(f"Next: notebooks/awards/CreateHolbergAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
