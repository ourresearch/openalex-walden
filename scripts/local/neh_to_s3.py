#!/usr/bin/env python3
"""
NEH (National Endowment for the Humanities) to S3 Data Pipeline (bulk CSV)
===========================================================================

Downloads NEH's complete grant history (1965-present) from the NEH
Open Data Portal and writes a single parquet for the OpenAlex awards
pipeline.

Source authority
----------------
NEH publishes every grant they've awarded as bulk CSV on their own
Azure Blob storage, partitioned by decade. This is NEH's own
publishing channel — NOT USAspending or any other aggregator.

Index page: https://securegrants.neh.gov/open/data/
Bulk CSVs (one per decade):
  https://nehopendatastorage.blob.core.windows.net/nehopendata/NEH_Grants{1960s..2020s}.csv

All 7 CSVs share an identical 33-column schema (confirmed
2026-05-20). Per-decade row counts roughly: 1960s 500, 1970s 11000,
1980s 12500, 1990s 6000, 2000s 7500, 2010s 10500, 2020s 9000 → total
~57000 historical grants spanning 1965-present.

Method-4 (bulk file download) on the runbook ladder — direct,
documented (NEH provides a GrantsDictionary.pdf), and stable.

Schema highlights mapped to the 23-col awards schema:
  - AppNumber           → funder_award_id (e.g. "FB-10007-68")
  - Institution +
    InstCity/State/Country → lead_investigator.affiliation
  - YearAwarded         → start_year (also derive start_date from BeginGrant)
  - ProjectTitle        → display_name
  - ProjectDesc + ToSupport → description (concatenated when both present)
  - Program             → funder_scheme
  - AwardOutright + AwardMatching + SupplementAmount → amount (USD, sum)
  - BeginGrant / EndGrant → start_date / end_date
  - Participants ("Name [Role]; Name [Role]; ...") → lead_investigator (first row)
  - PrimaryDiscipline / Disciplines → metadata, kept in raw frame but not in awards schema directly

Output
------
s3://openalex-ingest/awards/neh/neh_grants.parquet

Usage
-----
    python neh_to_s3.py                  # download all 7 decades + upload
    python neh_to_s3.py --skip-upload    # local dev / smoke test
    python neh_to_s3.py --skip-download  # reuse local CSVs
    python neh_to_s3.py --decades 2020s,2010s  # subset for smoke
    python neh_to_s3.py --allow-shrink   # override §1.4 shrink-check

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

import argparse
import re
import subprocess
import sys
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

# NEH's own Azure Blob storage. Per-decade CSV files.
BLOB_BASE = "https://nehopendatastorage.blob.core.windows.net/nehopendata"
DECADES = ["1960s", "1970s", "1980s", "1990s", "2000s", "2010s", "2020s"]

# Awarding body — National Endowment for the Humanities.
# Verified F4320306100, country US.
FUNDER_ID = 4320306100
FUNDER_DISPLAY_NAME = "National Endowment for the Humanities"

PROVENANCE = "neh_open_data"
CURRENCY = "USD"  # NEH is US federal — single-currency funder.

# S3 destination.
S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/neh/neh_grants.parquet"

USER_AGENT = "openalex-walden-neh-ingest/1.0 (+https://openalex.org)"

# Date format used by every datetime column in the NEH CSVs.
NEH_DATE_FMT = "%m/%d/%Y %I:%M:%S %p"


# =============================================================================
# Download
# =============================================================================

def download_decade(decade: str, output_dir: Path) -> Path:
    out = output_dir / f"NEH_Grants{decade}.csv"
    url = f"{BLOB_BASE}/NEH_Grants{decade}.csv"
    print(f"  [{decade}] downloading {url}")
    headers = {"User-Agent": USER_AGENT}
    with requests.get(url, headers=headers, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:
                    f.write(chunk)
    sz_mb = out.stat().st_size / (1024 * 1024)
    print(f"  [{decade}] wrote {sz_mb:.1f} MB to {out}")
    return out


def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: head-fetch the 2020s decade and verify schema")
    print("=" * 60)
    url = f"{BLOB_BASE}/NEH_Grants2020s.csv"
    r = requests.get(url, headers={"User-Agent": USER_AGENT,
                                    "Range": "bytes=0-4095"}, timeout=30)
    r.raise_for_status()
    header = r.text.split("\n", 1)[0]
    cols = [c.strip() for c in header.split(",")]
    if "AppNumber" not in cols or "Institution" not in cols:
        print(f"[ERROR] header missing AppNumber/Institution — schema changed?\n  got: {cols[:10]}")
        sys.exit(3)
    if len(cols) < 30:
        print(f"[ERROR] only {len(cols)} columns — schema shrunk?")
        sys.exit(3)
    print(f"  [OK] header has {len(cols)} columns including AppNumber + Institution")


# =============================================================================
# Parse + assemble
# =============================================================================

def parse_neh_date(raw: Optional[str]) -> Optional[str]:
    """NEH date format: '8/1/2018 12:00:00 AM' → 'YYYY-MM-DD'."""
    if not raw or pd.isna(raw):
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, NEH_DATE_FMT).date().isoformat()
    except ValueError:
        return None


# Participants format: 'Name [Role]; Name [Role]; ...'.
# Project Director is the canonical PI role per NEH.
_ROLE_BRACKET_RE = re.compile(r'\s*\[([^\]]+)\]\s*$')


def parse_participants(raw: Optional[str]) -> tuple[Optional[str], Optional[str], list[tuple[str,str]]]:
    """
    Returns (lead_full_name, lead_role, all_participants).
    all_participants is a list of (name, role) tuples in the order
    they appear in the source string. Lead is the first Project
    Director, or the first participant if none has that role.
    """
    if not raw or pd.isna(raw):
        return (None, None, [])
    s = str(raw).strip()
    if not s:
        return (None, None, [])
    parts = [p.strip() for p in s.split(";") if p.strip()]
    out: list[tuple[str,str]] = []
    for p in parts:
        m = _ROLE_BRACKET_RE.search(p)
        if m:
            role = m.group(1).strip()
            name = p[:m.start()].strip()
        else:
            name = p
            role = ""
        if name:
            out.append((name, role))
    # Prefer Project Director as lead.
    lead = next((t for t in out if "Project Director" in t[1]), None)
    if not lead and out:
        lead = out[0]
    if lead:
        return (lead[0], lead[1] or None, out)
    return (None, None, out)


# Conservative name-split, same idiom as Holberg/CIFAR/Nuffield.
_DEGREE_SUFFIXES = {"PhD", "Ph.D.", "MD", "DPhil", "Jr.", "Sr.", "II", "III", "IV"}
_PREFIX_TITLES = {"Dr", "Dr.", "Prof", "Prof.", "Professor", "Mr", "Mr.",
                  "Mrs", "Mrs.", "Ms", "Ms.", "Sir", "Dame", "Lord", "Lady"}


def split_name(full: str) -> tuple[str, str]:
    tokens = full.split()
    while tokens and tokens[0].rstrip(".") in {t.rstrip(".") for t in _PREFIX_TITLES}:
        tokens.pop(0)
    while tokens and tokens[-1].rstrip(".") in {s.rstrip(".") for s in _DEGREE_SUFFIXES}:
        tokens.pop()
    if not tokens:
        return ("", "")
    if len(tokens) == 1:
        return ("", tokens[0])
    return (" ".join(tokens[:-1]), tokens[-1])


def build_dataframe(csv_paths: list[Path]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Read + concat all decades")
    print("=" * 60)
    frames = []
    for p in csv_paths:
        df = pd.read_csv(p, low_memory=False, dtype=str, keep_default_na=False)
        print(f"  {p.name}: {len(df)} rows × {len(df.columns)} cols")
        frames.append(df)
    raw = pd.concat(frames, ignore_index=True)
    print(f"\n  combined raw: {len(raw)} rows × {len(raw.columns)} cols")

    # Slug-collision check on AppNumber (the funder_award_id source).
    dup_mask = raw.duplicated(subset=["AppNumber"], keep=False) & (raw["AppNumber"] != "")
    if dup_mask.any():
        n_dups = dup_mask.sum()
        sample = raw.loc[dup_mask, "AppNumber"].head(5).tolist()
        raise RuntimeError(
            f"Found {n_dups} duplicate AppNumbers in source CSVs — sample: {sample}. "
            f"NEH's AppNumber is supposed to be unique. Investigate raw "
            f"payload before shipping (do not silently keep one row per id)."
        )

    # Build the canonical flat output.
    rows = []
    for r in raw.to_dict(orient="records"):
        # Amount = sum of awarded outright + matching + supplement (USD).
        def num(s):
            try:
                return float(s) if s not in ("", None) else 0.0
            except (TypeError, ValueError):
                return 0.0
        amount = num(r.get("AwardOutright")) + num(r.get("AwardMatching")) + num(r.get("SupplementAmount"))
        if amount <= 0:
            amount_value: Optional[float] = None
        else:
            amount_value = amount

        # Combine ProjectDesc + ToSupport for the awards.description field.
        # ProjectDesc carries rich HTML — strip tags AND decode HTML entities
        # (precedent: Wolf had &nbsp;/&quot; leak into the parquet, fixed in
        # walden f1281fa with html.unescape; we apply the same hygiene here).
        desc_parts = []
        if r.get("ToSupport"):
            desc_parts.append(unescape(r["ToSupport"]).strip())
        if r.get("ProjectDesc") and r["ProjectDesc"].strip() not in {"", "Description not available"}:
            clean = re.sub(r"<[^>]+>", " ", r["ProjectDesc"])
            clean = unescape(re.sub(r"\s+", " ", clean)).strip()
            if clean and clean.lower() != "description not available":
                desc_parts.append(clean)
        description = " — ".join(desc_parts) if desc_parts else None

        lead_name, lead_role, all_parts = parse_participants(r.get("Participants"))
        lead_given, lead_family = split_name(lead_name) if lead_name else ("", "")

        # Defensive html.unescape on every user-visible text field — NEH's
        # CSVs are mostly clean but `&amp;`/`&nbsp;` show up sporadically in
        # ProjectTitle and Institution (e.g. "Art &amp; Architecture Museum").
        def u(v): return unescape(v) if isinstance(v, str) and v else (v or None)
        rows.append({
            "funder_award_id":     r.get("AppNumber") or None,
            "applicant_type":      r.get("ApplicantType") or None,
            "institution":         u(r.get("Institution")),
            "organization_type":   r.get("OrganizationType") or None,
            "inst_city":           u(r.get("InstCity")),
            "inst_state":          r.get("InstState") or None,
            "inst_country":        r.get("InstCountry") or None,
            "inst_postal_code":    r.get("InstPostalCode") or None,
            "latitude":            r.get("Latitude") or None,
            "longitude":           r.get("Longitude") or None,
            "year_awarded":        r.get("YearAwarded") or None,
            "project_title":       u(r.get("ProjectTitle")),
            "program":             u(r.get("Program")),
            "division":            u(r.get("Division")),
            "primary_discipline":  u(r.get("PrimaryDiscipline")),
            "disciplines":         u(r.get("Disciplines")),
            "description":         description,
            "begin_grant":         parse_neh_date(r.get("BeginGrant")),
            "end_grant":           parse_neh_date(r.get("EndGrant")),
            "council_date":        parse_neh_date(r.get("CouncilDate")),
            "amount_usd":          amount_value,
            "currency":            CURRENCY if amount_value is not None else None,
            "award_outright":      r.get("AwardOutright") or None,
            "award_matching":      r.get("AwardMatching") or None,
            "supplement_amount":   r.get("SupplementAmount") or None,
            "original_amount":     r.get("OriginalAmount") or None,
            "lead_full_name":      lead_name,
            "lead_given_name":     lead_given,
            "lead_family_name":    lead_family,
            "lead_role":           lead_role,
            "all_participants":    "; ".join(f"{n} [{role}]" if role else n
                                             for n, role in all_parts) or None,
            "participant_count":   str(len(all_parts)),
            "landing_page_url":    f"https://securegrants.neh.gov/publicquery/main.aspx?gn={r.get('AppNumber','')}",
            "declined":            False,  # NEH doesn't publish declined-grant data
        })
    df = pd.DataFrame.from_records(rows)
    print(f"\n  flat output: {len(df)} rows × {len(df.columns)} cols")
    n_amt = df["amount_usd"].notna().sum()
    n_lead = df["lead_full_name"].astype(bool).sum()
    n_inst = df["institution"].astype(bool).sum()
    print(f"  coverage: amount={n_amt} ({n_amt*100/len(df):.0f}%)  "
          f"lead={n_lead} ({n_lead*100/len(df):.0f}%)  "
          f"institution={n_inst} ({n_inst*100/len(df):.0f}%)")
    print(f"  programs (top 8):")
    print(df.groupby("program").size().sort_values(ascending=False).head(8).to_string())

    # Runbook §1.2.5 — string before parquet to prevent pyarrow int-inference.
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "neh_grants.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_mb = parquet_path.stat().st_size / (1024 * 1024)
    print(f"  [OK] wrote {len(df)} rows ({sz_mb:.1f} MB) to {parquet_path}")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook §1.4 — never shrink the corpus on re-ingest."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for the §1.4 shrink-check; rerun with --skip-upload to bypass"
        ) from exc
    client = boto3.client("s3")
    print(f"  §1.4 re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("    no existing parquet — first ingest, no shrink check.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest")
        return True
    prev_path = output_dir / "_prev_neh_grants.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as e:
        print(f"    [ERROR] couldn't read existing parquet ({e}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)
    print(f"    previous count: {prev_count}   new count: {new_count}")
    if new_count < prev_count:
        if allow_shrink:
            print(f"    [OVERRIDE] new < previous but --allow-shrink set; proceeding.")
            return True
        print(
            f"\n[ERROR] §1.4 violation: refusing to shrink corpus "
            f"({prev_count} -> {new_count}). Investigate first."
        )
        return False
    print(f"    [OK] new corpus not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path,
                 allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 4: Upload to S3 (with §1.4 shrink check)")
    print("=" * 60)
    if not check_no_shrink(len(df), allow_shrink, output_dir):
        return False
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  Uploading {parquet_path} -> {s3_uri}")
    try:
        subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
        print(f"  [OK] uploaded to {s3_uri}")
        return True
    except FileNotFoundError:
        print("[ERROR] aws CLI not found.")
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/neh"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse downloaded CSVs in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--decades", type=str, default=None,
                        help="Comma-separated decades to ingest (default: all 1960s-2020s)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    selected_decades = (
        [d.strip() for d in args.decades.split(",")] if args.decades else DECADES
    )
    unknown = [d for d in selected_decades if d not in DECADES]
    if unknown:
        print(f"[ERROR] unknown decades {unknown}; allowed: {DECADES}")
        sys.exit(2)

    print("=" * 60)
    print("NEH (National Endowment for the Humanities) → S3 Pipeline")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Decades:    {selected_decades}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    csv_paths = []
    for dec in selected_decades:
        path = args.output_dir / f"NEH_Grants{dec}.csv"
        if args.skip_download:
            if not path.exists():
                print(f"[ERROR] --skip-download given but {path} missing")
                sys.exit(6)
            print(f"  [SKIP] reusing existing {path}")
        else:
            path = download_decade(dec, args.output_dir)
        csv_paths.append(path)

    df = build_dataframe(csv_paths)
    parquet_path = write_parquet(df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload; manual upload command:")
        print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")
    else:
        ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
        if not ok:
            sys.exit(7)

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print(f"Next: notebooks/awards/CreateNEHAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
