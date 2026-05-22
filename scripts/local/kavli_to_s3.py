#!/usr/bin/env python3
"""
Kavli Prize to S3 (PRIZE PATTERN)
==================================

Kavli Foundation publishes the full laureate corpus in the
`__NEXT_DATA__` JSON embedded in
https://kavliprize.org/laureates — no per-page iteration needed.

Each prize entry has year, field (astrophysics / nanoscience /
neuroscience), citation, and laureates (with name, institution,
locations, bio, slug). The Kavli Prize is biennial since 2008 across
3 fields × 2-4 laureates per category, so ~80 award-laureate rows
total.

Output: s3://openalex-ingest/awards/kavli/kavli_laureates.parquet
Awarding body in OpenAlex: Kavli Foundation (F4320306399)
"""

import argparse
import json
import re
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

URL = "https://kavliprize.org/laureates"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/kavli/kavli_laureates.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def block_to_text(blocks) -> str:
    """Sanity render Sanity-CMS blockContent arrays to plain text."""
    if not isinstance(blocks, list):
        return ""
    parts = []
    for b in blocks:
        if isinstance(b, dict) and b.get("_type") == "block":
            for c in b.get("children") or []:
                if isinstance(c, dict) and c.get("text"):
                    parts.append(c["text"])
            parts.append("\n")
    return re.sub(r"\s+", " ", "".join(parts)).strip()


def split_name(name: str) -> tuple[str | None, str | None]:
    """Split 'Robert S. Langer' -> ('Robert S.', 'Langer').

    Strips trailing degree/suffix tokens (PhD, MD, Jr., Sr., II, III) before
    splitting. Last whitespace-separated token = family name; rest = given.
    """
    if not name:
        return None, None
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def expand_prize(prize: dict) -> list[dict]:
    year = prize.get("year")
    field = prize.get("field") or ""
    citation = (prize.get("citation") or "").strip().strip("“”\"")
    intro_text = block_to_text(prize.get("intro"))
    laureates = prize.get("laureates") or []
    out = []
    for la in laureates:
        if not isinstance(la, dict):
            continue
        name = (la.get("name") or "").strip()
        slug_obj = la.get("slug") or {}
        slug = slug_obj.get("current") if isinstance(slug_obj, dict) else None
        institution = la.get("institution")
        locations = la.get("locations") or []
        countries = []
        for loc in locations:
            if isinstance(loc, dict) and loc.get("location"):
                countries.append(loc["location"])
        bio = block_to_text(la.get("bio"))
        given_name, family_name = split_name(name)
        out.append({
            "kavli_laureate_id": la.get("_id"),
            "slug": slug,
            "year": year,
            "field": field,
            "citation": citation,
            "name": name,
            "given_name": given_name,
            "family_name": family_name,
            "institution": institution,
            "countries": countries,
            "bio": bio[:2000] if bio else None,
            "intro_text": intro_text[:1500] if intro_text else None,
            "downloaded_at": datetime.utcnow().isoformat(),
        })
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Kavli Prize -> parquet -> S3")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    args = p.parse_args()

    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    log("=" * 60)
    log("Kavli Prize -> S3 starting")

    r = requests.get(URL, headers=HEADERS, timeout=30, verify=False)
    r.raise_for_status()
    m = re.search(
        r'__NEXT_DATA__"\s*type="application/json"[^>]*>([^<]+)</script>', r.text
    )
    if not m:
        raise RuntimeError("Could not find __NEXT_DATA__ on Kavli laureates page")
    payload = json.loads(m.group(1))
    prizes = payload["props"]["pageProps"]["queryResult"]["prizes"]
    log(f"Found {len(prizes)} prize entries in __NEXT_DATA__")

    rows: list[dict] = []
    for prize in prizes:
        rows.extend(expand_prize(prize))
    log(f"Total laureate rows: {len(rows)}")

    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")
    if not df.empty:
        log(f"Coverage: name={df.name.notna().sum()}, year={df.year.notna().sum()}, "
            f"field={df.field.astype(bool).sum()}, institution={df.institution.notna().sum()}")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "kavli_laureates.parquet"
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
