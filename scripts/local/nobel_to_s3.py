#!/usr/bin/env python3
"""
Nobel Prize to S3 Data Pipeline (PRIZE PATTERN TEMPLATE)
=========================================================

Ingests Nobel Prize laureates for the four science categories
(Physics, Chemistry, Economic Sciences, Physiology or Medicine) from
the official Nobel API at https://api.nobelprize.org/2.1/.

Background: the Nobel Foundation itself is NOT a funder entity in
OpenAlex. But the actual *awarding bodies* are, and they map to
specific Nobel categories:

  - Royal Swedish Academy of Sciences (F4320320936)
      → Physics, Chemistry, Economic Sciences
  - Karolinska Institutet (F4320322315)
      → Physiology or Medicine

Literature and Peace are out of scope for this ingest because their
awarding bodies (Swedish Academy, Norwegian Nobel Committee) aren't
indexed as funders in OpenAlex, and the awards don't represent
research funding in the OpenAlex sense.

Schema notes (PRIZE PATTERN — different from grant pattern):
  - amount = `prizeAmount` (the original SEK amount in the year of award)
    Keep both the original SEK and the inflation-adjusted SEK as separate
    columns; the notebook chooses which to expose as the canonical amount.
  - currency = "SEK"
  - lead_investigator = the laureate (split-portion when shared)
  - funder = the awarding body (mapped per category above)
  - One row per (prize × laureate) — i.e. shared prizes produce N rows.

Output: s3://openalex-ingest/awards/nobel/nobel_prizes.parquet
"""

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

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

API_BASE = "https://api.nobelprize.org/2.1"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/nobel/nobel_prizes.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
LIMIT = 100  # Nobel API page size
REQUEST_DELAY = 0.3
RETRIES = 3

# Category -> (OpenAlex funder_id, awarding-body display_name)
# Only the four science-relevant categories are ingested.
CATEGORY_TO_FUNDER = {
    "phy": (4320320936, "Royal Swedish Academy of Sciences"),
    "che": (4320320936, "Royal Swedish Academy of Sciences"),
    "eco": (4320320936, "Royal Swedish Academy of Sciences"),
    "med": (4320322315, "Karolinska Institutet"),
    # Excluded: "lit" (Swedish Academy), "pea" (Norwegian Nobel Committee)
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_with_retries(url: str, params: dict | None = None) -> dict:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed: {url} ({last_err})")


def fetch_all_prizes() -> list[dict]:
    out: list[dict] = []
    offset = 0
    while True:
        d = fetch_with_retries(f"{API_BASE}/nobelPrizes",
                               params={"limit": LIMIT, "offset": offset})
        prizes = d.get("nobelPrizes", [])
        out.extend(prizes)
        total = d.get("meta", {}).get("count", 0)
        log(f"  fetched {len(prizes)} prizes (running {len(out)}/{total})")
        if len(prizes) < LIMIT or len(out) >= total:
            break
        offset += LIMIT
        time.sleep(REQUEST_DELAY)
    return out


def en(text_obj) -> str | None:
    """Pull the English value from Nobel API's multi-language text objects."""
    if isinstance(text_obj, dict):
        return text_obj.get("en")
    return text_obj


def normalise_prize_laureate(prize: dict, laureate_in_prize: dict) -> dict:
    """Emit one row per (prize, laureate). Shared prizes produce N rows."""
    cat_code = (prize.get("category") or {}).get("se", "").lower()[:3] \
        or (prize.get("categoryFullName") or {}).get("en", "")[:3].lower()
    # Better: derive from API's category structure
    cat_obj = prize.get("category") or {}
    cat_full = en(prize.get("categoryFullName")) or en(cat_obj) or ""
    # Map full English name back to 3-letter code
    cat_code = {
        "Chemistry": "che",
        "Physics": "phy",
        "Physiology or Medicine": "med",
        "Literature": "lit",
        "Peace": "pea",
        "Economic Sciences": "eco",
    }.get(en(cat_obj) or "", "")

    return {
        # Source-fidelity fields preserved
        "award_year": prize.get("awardYear"),
        "category_code": cat_code,
        "category_full_en": cat_full,
        "date_awarded": prize.get("dateAwarded"),
        "prize_amount_sek": prize.get("prizeAmount"),  # nominal SEK
        "prize_amount_adjusted_sek": prize.get("prizeAmountAdjusted"),  # 2023 SEK
        # Laureate fields
        "laureate_id": laureate_in_prize.get("id"),
        "laureate_known_name": en(laureate_in_prize.get("knownName")),
        "laureate_full_name": en(laureate_in_prize.get("fullName")),
        "portion": laureate_in_prize.get("portion"),  # e.g. "1/2", "1/4"
        "sort_order": laureate_in_prize.get("sortOrder"),
        "motivation_en": en(laureate_in_prize.get("motivation")),
        # Provenance
        "source": "nobelprize.org/2.1",
        "downloaded_at": datetime.utcnow().isoformat(),
    }


def fetch_laureate_detail(laureate_id: str) -> dict | None:
    """Hit /laureates/{id} for biographical / affiliation data."""
    try:
        d = fetch_with_retries(f"{API_BASE}/laureates/{laureate_id}")
        # API may return either {laureates:[...]} or the laureate object directly
        if "laureates" in d and d["laureates"]:
            return d["laureates"][0]
        return d
    except Exception as e:
        log(f"    failed to fetch laureate {laureate_id}: {e}")
        return None


def main() -> None:
    p = argparse.ArgumentParser(description="Nobel Prize -> parquet -> S3")
    p.add_argument("--limit-prizes", type=int, default=None,
                   help="For smoke-test: stop after N prizes")
    p.add_argument("--skip-laureate-details", action="store_true",
                   help="Skip /laureates/{id} calls (faster smoke-test, no birth/affiliation)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    args = p.parse_args()

    log("=" * 60)
    log("Nobel Prize -> S3 pipeline starting")

    log("Phase 1: fetch all Nobel Prizes from API")
    prizes = fetch_all_prizes()
    log(f"  total prizes returned: {len(prizes)}")

    # Filter to in-scope categories
    in_scope = []
    for p_ in prizes:
        cat_en = en(p_.get("category")) or ""
        if cat_en in {"Physics", "Chemistry", "Physiology or Medicine", "Economic Sciences"}:
            in_scope.append(p_)
    log(f"  in-scope (Physics/Chemistry/Medicine/Economics): {len(in_scope)}")

    if args.limit_prizes:
        in_scope = in_scope[: args.limit_prizes]
        log(f"  smoke-test mode: limited to first {len(in_scope)} prizes")

    log("Phase 2: emit one row per (prize × laureate); fetch biographical detail")
    rows: list[dict] = []
    laureate_cache: dict[str, dict] = {}
    for i, p_ in enumerate(in_scope, 1):
        for laureate_in_prize in p_.get("laureates", []) or []:
            row = normalise_prize_laureate(p_, laureate_in_prize)
            lid = laureate_in_prize.get("id")
            if not args.skip_laureate_details and lid:
                if lid not in laureate_cache:
                    detail = fetch_laureate_detail(lid)
                    laureate_cache[lid] = detail or {}
                    time.sleep(REQUEST_DELAY)
                detail = laureate_cache[lid]
                row["laureate_given_name"] = en(detail.get("givenName"))
                row["laureate_family_name"] = en(detail.get("familyName"))
                row["laureate_gender"] = detail.get("gender")
                # Affiliation at time of award (if present)
                # Some laureate detail objects include `nobelPrizes[].affiliations`
                row["affiliation_name"] = None
                row["affiliation_country"] = None
                for np in detail.get("nobelPrizes", []) or []:
                    if np.get("awardYear") == p_.get("awardYear"):
                        affs = np.get("affiliations") or []
                        if affs:
                            row["affiliation_name"] = en(affs[0].get("name"))
                            row["affiliation_country"] = en(affs[0].get("country"))
                        break
            else:
                row["laureate_given_name"] = en(laureate_in_prize.get("givenName"))
                row["laureate_family_name"] = en(laureate_in_prize.get("familyName"))
                row["laureate_gender"] = None
                row["affiliation_name"] = None
                row["affiliation_country"] = None
            rows.append(row)
        if i == 1 or i % 50 == 0 or i == len(in_scope):
            log(f"  [{i}/{len(in_scope)}] cumulative rows: {len(rows):,}")

    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")
    log(f"Coverage — laureate_full_name: {df.laureate_full_name.notna().sum()}, "
        f"prize_amount_sek: {df.prize_amount_sek.notna().sum()}, "
        f"motivation: {df.motivation_en.notna().sum()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "nobel_prizes.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {parquet_path}")

    if args.skip_upload:
        log("--skip-upload set; done.")
        return

    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3
    s3 = boto3.client("s3")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    main()
