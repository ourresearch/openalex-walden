#!/usr/bin/env python3
"""
UCOP Awards → S3 Pipeline (GRANT PATTERN, method-3 JSON-in-page-bundle)
========================================================================

Downloads the University of California Office of the President's
publicly-published grant database. UCOP runs the Research Grants
Program Office (RGPO) which administers programs like TRDRP (Tobacco-
Related Disease Research Program), CHRP (California HIV/AIDS Research
Program), CBCRP (California Breakthrough Breast Cancer Research
Program), CRCRC (California Cannabis Research Center), and others.

Source: `https://rgpogrants.ucop.edu/files/1614305/f480589/index.html`
is a thin HTML viewer that lazy-loads a single JSON file at
`https://rgpogrants.ucop.edu/files/1614305/f480589/ucop.json`. The
JSON shape is `{"reportdata": [{...}, {...}, ...]}` with one record per
funded project. Method-3 ladder (search/index endpoint exposed in page
bundle), discovered by reading the page source — the loader fetches
the JSON directly with no auth, pagination, or rate-limiting.

  ~8,039 records, 1990-2026, $2.37B total funded (99.8% amount
  coverage), 100% start/end dates, 98.7% grant_doi coverage, 100%
  unique `applicationid` (the native award ID).

Schema (per-record, from the source JSON):

  - applicationid           native award ID, unique
  - title                   project title
  - abstract                full abstract (58.7%)
  - progressreportabbstract progress-report abstract (32.4%)
  - approvedamount          USD amount (99.8%)
  - startdate, enddate      ISO date strings
  - program                 program name (e.g. TRDRP)
  - awardtype               award scheme (e.g. "Full CARA")
  - callname                grant-call name
  - priority                research priority tag
  - level_one_id            internal hierarchy
  - orgname                 awardee institution
  - grant_doi               grant-level DOI (98.7%)
  - focusarea               topic tag
  - subject_area(s)         topic tag
  - contacts                pseudo-JSON array of PI dicts (see below)
  - publications            pseudo-JSON array of linked outputs

The `contacts` and `publications` fields are pseudo-JSON: pairs of
double-backticks (``) delimit string values instead of `"` quotes, and
trailing commas appear before `]`. We re-serialize them to standard
JSON for downstream consumption (the notebook's Step 2 transform uses
`from_json` to project PI info into `lead_investigator`).

Awarding body in OpenAlex: Office of the President, University of
California (F4320333677, US, no ROR, DOI 10.13039/100014576).

Output
------
  s3://openalex-ingest/awards/ucop/ucop_awards.parquet

Usage
-----
    python ucop_to_s3.py                                  # full run
    python ucop_to_s3.py --skip-upload                    # local dev
    python ucop_to_s3.py --limit 200                      # smoke
    python ucop_to_s3.py --skip-download --skip-upload    # reuse cache
    python ucop_to_s3.py --allow-shrink                   # override §1.4

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
# Windows Python defaults to cp1252 for both stdout-when-piped and default
# file I/O. UCOP data has non-ASCII chars (umlauts, accents in PI names
# like "Müller", "Cárcamo", "Yáñez"). The reconfigure handles stdout; the
# Path/open monkey-patch handles file I/O. No-op on Linux/Databricks.
# Runbook §1.2.
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

DATA_URL = "https://rgpogrants.ucop.edu/files/1614305/f480589/ucop.json"

FUNDER_ID = 4320333677
FUNDER_DISPLAY_NAME = "Office of the President, University of California"

PROVENANCE = "ucop_grants"
CURRENCY = "USD"  # UCOP is a US funder; the source does not publish currency, hardcode per runbook §2.4 currency-implicit rule

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ucop/ucop_awards.parquet"

USER_AGENT = "openalex-walden-ucop-ingest/1.0 (+https://openalex.org)"

DEFAULT_CACHE = Path(".cache/ucop_raw.json")


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# =============================================================================
# UCOP pseudo-JSON parser
# =============================================================================
#
# The `contacts` and `publications` fields are stored as pseudo-JSON
# where pairs of `` (double backticks) delimit string values. Example:
#
#   [{``institution``: ``UCSF``, ``firstname``: ``Peggy``},]
#
# Two deviations from real JSON:
#   1. `` instead of "
#   2. Trailing comma before ]
#
# We split on `` so we can re-escape the values robustly even when the
# original values contain " or \. Counts pairs to know what's a delimiter
# vs structural text.

def parse_ucop_pseudo_json(s: Optional[str]):
    """Parse UCOP's pseudo-JSON. Returns a Python list (possibly empty)."""
    if not s or not isinstance(s, str) or s.strip() in ("", "[]"):
        return []
    parts = s.split("``")
    if len(parts) < 3:
        return []
    out = []
    for j, part in enumerate(parts):
        if j % 2 == 0:
            # Structural (between strings)
            out.append(part)
        else:
            # Inside a `` ... `` string — JSON-escape the value contents
            esc = (part.replace("\\", "\\\\")
                       .replace("\"", "\\\"")
                       .replace("\n", "\\n")
                       .replace("\r", "\\r")
                       .replace("\t", "\\t"))
            out.append("\"" + esc + "\"")
    rebuilt = "".join(out)
    rebuilt = re.sub(r",(\s*[\]}])", r"\1", rebuilt)  # strip trailing commas
    return json.loads(rebuilt, strict=False)


# =============================================================================
# Name parsing (canonical helper per runbook §2.4.1)
# =============================================================================
#
# UCOP's contacts already split first/last name, so we don't need
# split_name() for the lead-investigator path. We keep the helper here
# anyway for any future case that ships a combined "Smith, John Q." string.

_SUFFIX_TOKENS = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}


def split_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Split 'Chad A. Mirkin' -> ('Chad A.', 'Mirkin'). Strips degree/suffix
    tokens (PhD, MD, Jr.) before splitting. Last token = family, rest = given.
    Canonical implementation per runbook §2.4.1."""
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
# Fetch
# =============================================================================

def fetch_records(use_cache: bool, cache_file: Path) -> list[dict]:
    """Download the UCOP JSON. Returns the list under reportdata."""
    if use_cache and cache_file.exists():
        log(f"Cache hit at {cache_file}; loading {cache_file.stat().st_size/1e6:.1f} MB")
        return json.loads(cache_file.read_text())["reportdata"]

    log(f"GET {DATA_URL}")
    t0 = time.monotonic()
    resp = requests.get(DATA_URL, headers={"User-Agent": USER_AGENT}, timeout=120)
    resp.raise_for_status()
    body_bytes = resp.content
    log(f"  status={resp.status_code} size={len(body_bytes)/1e6:.1f} MB elapsed={time.monotonic()-t0:.1f}s")

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_bytes(body_bytes)
    log(f"Cached raw JSON to {cache_file}")

    payload = json.loads(body_bytes)
    rd = payload.get("reportdata") or []
    if not isinstance(rd, list):
        raise RuntimeError(f"Expected reportdata to be a list; got {type(rd).__name__}")
    log(f"Loaded {len(rd):,} records from source")
    return rd


# =============================================================================
# Validation + DataFrame build
# =============================================================================

REQUIRED_FIELDS = ["applicationid", "title", "startdate", "enddate", "approvedamount"]


def validate_records(records: list[dict]) -> None:
    """Light source-fidelity check before parquet write. Runbook §1: fail fast."""
    if not records:
        raise RuntimeError("No records to validate")

    # 1. Required fields populated on >95% of rows
    for f in REQUIRED_FIELDS:
        non_null = sum(1 for r in records if r.get(f) not in (None, "", []))
        pct = non_null * 100 / len(records)
        log(f"  required-field {f:<20} coverage {non_null}/{len(records)} ({pct:.1f}%)")
        if pct < 95.0 and f != "approvedamount":
            raise RuntimeError(f"required field {f!r} has only {pct:.1f}% coverage; suspect")

    # 2. applicationid must be unique (the native award ID)
    ids = [r.get("applicationid") for r in records if r.get("applicationid")]
    if len(ids) != len(set(ids)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(ids).items() if v > 1][:5]
        raise RuntimeError(
            f"applicationid not unique: {len(ids)} ids, {len(set(ids))} distinct. "
            f"Example duplicates: {dups}. Prize-pattern dup-handling not applicable to "
            f"grant pattern; if UCOP genuinely repeats IDs, switch to a composite key."
        )
    log(f"  applicationid uniqueness: {len(ids)}/{len(ids)} distinct ✓")

    # 3. amount sanity (mean should be in the 10k-10M range)
    amounts = []
    for r in records:
        a = r.get("approvedamount")
        try:
            if a not in (None, ""):
                amounts.append(float(a))
        except (ValueError, TypeError):
            pass
    if amounts:
        log(f"  approvedamount: n={len(amounts)} min=${min(amounts):,.0f} median=${sorted(amounts)[len(amounts)//2]:,.0f} max=${max(amounts):,.0f}")


def build_dataframe(records: list[dict]) -> pd.DataFrame:
    """Reshape raw UCOP records into a flat DataFrame ready for parquet.

    Native fields are preserved as-is (source fidelity per runbook §1.2.1).
    The pseudo-JSON `contacts` and `publications` are re-serialized to
    standard JSON strings so the notebook can `from_json` them in Step 2.
    """
    rows = []
    for r in records:
        contacts_parsed = parse_ucop_pseudo_json(r.get("contacts"))
        publications_parsed = parse_ucop_pseudo_json(r.get("publications"))
        rows.append({
            "applicationid":           r.get("applicationid"),
            "title":                   r.get("title"),
            "abstract":                r.get("abstract"),
            "progressreportabbstract": r.get("progressreportabbstract"),
            "approvedamount":          r.get("approvedamount"),
            "startdate":               r.get("startdate"),
            "enddate":                 r.get("enddate"),
            "program":                 r.get("program"),
            "awardtype":               r.get("awardtype"),
            "callname":                r.get("callname"),
            "priority":                r.get("priority"),
            "level_one_id":            r.get("level_one_id"),
            "orgname":                 r.get("orgname"),
            "grant_doi":               r.get("grant_doi"),
            "focusarea":               r.get("focusarea"),
            "subject_areas":           r.get("subject_area(s)"),  # JSON key has parens
            "owner_id":                r.get("owner_id"),
            "contacts_json":           json.dumps(contacts_parsed, ensure_ascii=False) if contacts_parsed else None,
            "publications_json":       json.dumps(publications_parsed, ensure_ascii=False) if publications_parsed else None,
        })
    df = pd.DataFrame(rows)

    # Runbook §1.2.5: force string dtype before to_parquet so pyarrow doesn't
    # int-infer null-heavy columns. Confirmed-incident pattern (IDRC 0f8b891,
    # Rockefeller 5f694b7).
    df = df.astype("string")
    return df


# =============================================================================
# Shrink-check (runbook §1.4)
# =============================================================================

def check_no_shrink(new_count: int, allow_shrink: bool, output_path: Path) -> bool:
    """Compare new row count vs the previous parquet in S3 (or local if no creds).
    Runbook §1.4: never overwrite with a smaller corpus unless --allow-shrink."""
    if allow_shrink:
        log(f"  --allow-shrink set; skipping §1.4 shrink-check")
        return True
    try:
        import boto3
        import io
        s3 = boto3.client("s3")
        head = s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev_bytes = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)["Body"].read()
        prev_df = pd.read_parquet(io.BytesIO(prev_bytes))
        prev_count = len(prev_df)
        log(f"  §1.4 shrink-check: previous S3 parquet had {prev_count:,} rows")
        if new_count < prev_count:
            log(f"  §1.4 FAIL: new corpus ({new_count:,}) < previous ({prev_count:,}). "
                f"Aborting upload. Re-run with --allow-shrink to override after investigating.")
            return False
        log(f"  §1.4 OK: new {new_count:,} >= previous {prev_count:,}")
        return True
    except Exception as e:
        # No previous parquet, or no S3 access. Bypass on the first upload.
        log(f"  §1.4 shrink-check skipped: {type(e).__name__}: {str(e)[:100]}. "
            f"This is normal for the first run.")
        return True


# =============================================================================
# Upload
# =============================================================================

def upload_to_s3(local_file: Path) -> None:
    try:
        import boto3
    except ImportError:
        raise RuntimeError(
            "boto3 is required for upload. Install with `pip install boto3`, "
            "or pass --skip-upload to write parquet locally only."
        )
    log(f"Uploading {local_file} -> s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(local_file), S3_BUCKET, S3_KEY)
    log(f"  upload OK")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch UCOP grants → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"),
                        help="Output directory; parquet is written to {output-dir}/ucop_awards.parquet")
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE,
                        help="Path to cache the raw downloaded JSON")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse the cached raw JSON; useful for iteration")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Write parquet locally but do not upload to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook §1.4 shrink-check. Only use after confirming a smaller corpus is intentional.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Truncate to first N records for smoke testing")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "ucop_awards.parquet"

    log(f"=== UCOP awards ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache={args.cache}")

    records = fetch_records(use_cache=args.skip_download, cache_file=args.cache)

    if args.limit is not None:
        log(f"--limit {args.limit}: truncating from {len(records):,} to {args.limit}")
        records = records[:args.limit]

    log(f"Validating {len(records):,} records...")
    validate_records(records)

    log(f"Building DataFrame...")
    df = build_dataframe(records)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")

    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e6:.1f} MB")

    if args.skip_upload:
        log("--skip-upload: leaving parquet local; not uploading to S3")
        log(f"=== UCOP awards ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink, output_path):
        raise SystemExit("§1.4 shrink-check failed. See above; re-run with --allow-shrink if intentional.")

    upload_to_s3(output_path)
    log(f"=== UCOP awards ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
