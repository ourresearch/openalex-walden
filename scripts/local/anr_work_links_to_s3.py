#!/usr/bin/env python3
"""
ANR (Agence Nationale de la Recherche) grant->work LINKAGES to S3
=================================================================

Companion to anr_to_s3.py. That script ingests ANR's *grants* (the award
entities, from data.gouv.fr). THIS script ingests ANR's *funder-reported
grant->publication linkages* -- a bulk file ANR sends us directly, mapping
each of their grants to the DOIs of the publications it funded.

This is the same trusted "funder-reported work linkage" model we built for
NWO (CreateNWOWorkAwards.ipynb): the funder asserts the grant->work edge from
researchers' own end-of-project reporting, and we trust it even when none of
our own pipelines (Crossref/text-mining/DataCite) independently derive it.

Source: emailed by ANR (Open Science Office) via their partage.anr.fr file-share.
    File: anr_publications_to_openalex_YYYY-MM-DD_HH-MM-SS.csv
    Delimiter: ';'  | Encoding: UTF-8 with BOM
    Columns:
        doi                       - publication DOI (one row per DOI)
        anr_grants                - '|'-delimited ANR grant ids (projet.code_decision,
                                    e.g. ANR-12-CHRI-0003) -- matches anr_awards.funder_award_id
        internal_source           - 'Oui'/'Non' (came from ANR's internal final-project
                                    reports vs open/closed DBs: HAL, OpenAlex, WOS)
        ID Open Alex short        - ANR's own DOI->OpenAlex work resolution (e.g. W2284217858).
                                    Kept for reference only; the Databricks notebook re-resolves
                                    from the DOI (canonical, handles merges -- ~1.4% of ANR's
                                    W-ids are stale vs the DOI).
        main_domain/field/subfield (Open Alex) - ANR's topic tags (reference only)

Output: s3://openalex-ingest/awards/anr/anr_work_links.parquet
    Normalized schema:
        doi                  STRING        (lowercased, bare DOI -- no https://doi.org/ prefix)
        anr_grants           ARRAY<STRING> (split on '|', trimmed, de-duplicated)
        internal_source      BOOLEAN       ('Oui' -> true)
        openalex_work_short  STRING        (ANR's asserted W-id, reference/cross-check only)
        main_domain          STRING
        main_field           STRING
        main_subfield        STRING

Requirements:
    pip install pandas pyarrow
    AWS CLI configured with write access to s3://openalex-ingest/awards/anr/

Usage:
    python anr_work_links_to_s3.py --input "/path/to/anr_publications_to_openalex_*.csv"
    python anr_work_links_to_s3.py --input ... --skip-upload   # local parquet only

Author: OpenAlex Team
"""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). Production
# runs on Linux/Databricks where UTF-8 is the default; this fixes local
# validation on Windows without requiring PYTHONUTF8=1. See runbook §1.2.
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

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/anr/anr_work_links.parquet"

# Source column names (exactly as they appear in ANR's CSV header)
COL_DOI = "doi"
COL_GRANTS = "anr_grants"
COL_INTERNAL = "internal_source"
COL_WID = "ID Open Alex short"
COL_DOMAIN = "main_domain (Open Alex)"
COL_FIELD = "main_field (Open Alex)"
COL_SUBFIELD = "main_subfield (Open Alex)"

GRANT_DELIM = "|"


# =============================================================================
# Transform
# =============================================================================

def normalize_doi(doi: str) -> Optional[str]:
    """Lowercase, strip, and reduce to the bare DOI (no scheme / doi.org prefix)."""
    if doi is None:
        return None
    d = str(doi).strip().lower()
    if not d:
        return None
    for prefix in ("https://doi.org/", "http://doi.org/", "doi.org/", "doi:"):
        if d.startswith(prefix):
            d = d[len(prefix):]
    return d or None


def split_grants(raw: str) -> list:
    """Split the '|'-delimited grant ids, trim, drop blanks, de-duplicate (order-preserving)."""
    if raw is None:
        return []
    seen, out = set(), []
    for part in str(raw).split(GRANT_DELIM):
        g = part.strip()
        if g and g not in seen:
            seen.add(g)
            out.append(g)
    return out


def process(input_path: Path) -> pd.DataFrame:
    print(f"\n{'='*60}\nStep 1: Reading ANR linkage CSV\n{'='*60}")
    print(f"  [READ] {input_path}")
    # utf-8-sig strips the BOM; keep everything as string to avoid pandas coercing W-ids etc.
    df = pd.read_csv(input_path, sep=";", dtype=str, encoding="utf-8-sig", keep_default_na=False)
    print(f"  [INFO] {len(df):,} rows, columns: {list(df.columns)}")

    required = {COL_DOI, COL_GRANTS, COL_INTERNAL, COL_WID}
    missing = required - set(df.columns)
    if missing:
        sys.exit(f"  [ERROR] Missing expected columns: {missing}")

    out = pd.DataFrame()
    out["doi"] = df[COL_DOI].map(normalize_doi)
    out["anr_grants"] = df[COL_GRANTS].map(split_grants)
    out["internal_source"] = df[COL_INTERNAL].str.strip().str.lower().eq("oui")
    out["openalex_work_short"] = df[COL_WID].str.strip().replace("", None)
    out["main_domain"] = df.get(COL_DOMAIN, "").replace("", None)
    out["main_field"] = df.get(COL_FIELD, "").replace("", None)
    out["main_subfield"] = df.get(COL_SUBFIELD, "").replace("", None)

    # Drop rows with no DOI (the join key on the work side); report what we drop.
    n_before = len(out)
    out = out[out["doi"].notna()].reset_index(drop=True)
    if n_before != len(out):
        print(f"  [INFO] Dropped {n_before - len(out):,} rows with no DOI")

    # Diagnostics
    n_no_grant = (out["anr_grants"].map(len) == 0).sum()
    n_multi = (out["anr_grants"].map(len) > 1).sum()
    print(f"  [INFO] rows with >=1 grant: {(out['anr_grants'].map(len) > 0).sum():,}  "
          f"| multi-grant: {n_multi:,}  | no grant (dropped downstream): {n_no_grant:,}")
    print(f"  [INFO] internal_source=true (final reports): {out['internal_source'].sum():,}")
    return out


def write_parquet(df: pd.DataFrame, output_path: Path) -> Path:
    print(f"\n{'='*60}\nStep 2: Writing parquet\n{'='*60}")
    schema = pa.schema([
        ("doi", pa.string()),
        ("anr_grants", pa.list_(pa.string())),
        ("internal_source", pa.bool_()),
        ("openalex_work_short", pa.string()),
        ("main_domain", pa.string()),
        ("main_field", pa.string()),
        ("main_subfield", pa.string()),
    ])
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, output_path)
    print(f"  [WROTE] {output_path}  ({output_path.stat().st_size/1e6:.1f} MB, {len(df):,} rows)")
    return output_path


# =============================================================================
# S3 upload (mirrors anr_to_s3.py)
# =============================================================================

def find_aws_cli() -> Optional[str]:
    import shutil
    aws_path = shutil.which("aws")
    if aws_path:
        return aws_path
    for path in [
        Path.home() / "Library/Python/3.11/bin/aws",
        Path.home() / "Library/Python/3.12/bin/aws",
        Path("/usr/local/bin/aws"),
        Path("/opt/homebrew/bin/aws"),
    ]:
        if path.exists():
            return str(path)
    return None


def upload_to_s3(local_path: Path) -> bool:
    print(f"\n{'='*60}\nStep 3: Uploading to S3\n{'='*60}")
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}")
    aws_cmd = find_aws_cli()
    if not aws_cmd:
        print("  [ERROR] AWS CLI not found. Install with: pip install awscli")
        return False
    print(f"  [INFO] Using AWS CLI: {aws_cmd}")
    try:
        subprocess.run([aws_cmd, "s3", "cp", str(local_path), s3_uri],
                       capture_output=True, text=True, check=True)
        print("  [SUCCESS] Upload complete!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] Upload failed: {e.stderr}")
        return False


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Upload ANR grant->work linkages to S3")
    parser.add_argument("--input", required=True,
                        help="Path to ANR linkage CSV (anr_publications_to_openalex_*.csv)")
    parser.add_argument("--output-dir", default=".",
                        help="Directory for the local parquet (default: current dir)")
    parser.add_argument("--skip-upload", action="store_true", help="Skip the S3 upload step")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        sys.exit(f"[ERROR] Input file not found: {input_path}")

    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")
    df = process(input_path)
    parquet_path = write_parquet(df, Path(args.output_dir) / "anr_work_links.parquet")

    if not args.skip_upload:
        if not upload_to_s3(parquet_path):
            print("\n[WARNING] S3 upload failed. You can upload manually:")
            print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)

    print("\n[DONE]")


if __name__ == "__main__":
    main()
