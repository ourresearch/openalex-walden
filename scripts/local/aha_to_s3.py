#!/usr/bin/env python3
"""
American Heart Association (AHA) to S3 Data Pipeline (direct funder export)
==========================================================================

AHA publishes no public grants API. Instead — as part of the OpenAlex/AHA
collaboration (they are moving compliance monitoring + funding intelligence
off Dimensions onto open data) — AHA sends us a **periodic Excel export of
their own award metadata straight from their grants-management system**
(ProposalCentral / "Report Builder", RB). This script ingests that export.

Source workbook (AHA-supplied, e.g.
`20260701 Awardee List 2015 to 2026.xlsx`) has two tabs:

  * **"RB pull ..."**  — AHA's OWN authoritative Report Builder pull. Rich,
    clean, structured: award id, PI name + degrees + academic rank,
    institution + city + state, proposal title, funding mechanism, program,
    EXACT start/end dates, duration, total amount, and two abstracts
    (a lay "General" abstract and a scientific "Technical" abstract).
    Includes the newest cycle's award starts. **This is the source we ingest.**

  * **"Dimensions pull ..."** — a Digital Science / Dimensions export of the
    same grants, provided only for AHA's internal cross-check. It carries an
    explicit copyright + redistribution restriction ("external use prohibited
    without prior written approval") and, on inspection, is an all-but-exact
    subset of the RB pull (≈10,201 of its 10,203 grant numbers are already in
    RB). We therefore **do NOT ingest the Dimensions tab** — it would mean
    redistributing Digital Science's copyrighted data into a public open
    dataset, for a net gain of ~2 records. It is used at most as a private QA
    cross-check (see --qa-crosscheck).

Why this is a NEW pattern in the awards pipeline
------------------------------------------------
Most `*_to_s3.py` scripts scrape a public site or API. AHA is the first
**partner-supplied private Excel export** direct-funder ingest. The awarding
body hands us their canonical grants table; we normalise it to the award
schema. Re-runs simply point --input at the newest export AHA sends.

Multi-PI collaborative awards
-----------------------------
AHA's collaborative mechanisms (Collaborative Sciences Award, Strategically
Focused Research Networks, ...) list ONE award id across MULTIPLE rows — one
row per co-PI at a different institution, each carrying that PI's portion of
the total. Within such a group the title and start/end dates are identical;
PI, institution and amount differ. We **aggregate each award id into a single
award row**: total amount = sum of the per-PI portions, and every PI is kept
in an `investigators[]` array (the first-listed row is treated as the lead /
primary applicant). If we emitted one row per PI they would share one award
`id` and all but one would be silently dropped on the CreateAwards dedup.

Schema choices / known limitations
----------------------------------
  * `funder_award_id` = AHA's own award number (e.g. `15BGIA22410018`,
    `26HTRN1673063`) — source-authoritative grant id.
  * `currency = 'USD'` hardcoded (US funder); 100% of awards publish an amount.
    ~29 rows publish a $0 amount (declined/withdrawn placeholders) → amount NULL.
  * `description` = the scientific ("Technical") abstract, falling back to the
    lay ("General") abstract; NULL if neither present.
  * EXACT `start_date` / `end_date` are published (unlike most foundation
    ingests) → both populated; `start_year`/`end_year` derived from them.
  * `funder_scheme` = the "Funding Mechanism" string; `funding_type` derived
    from it (Predoctoral/Undergraduate/Supplement/SURE → training;
    Fellowship/Career Development/Scientist Development → fellowship;
    else research).
  * PI names come as separate First/Last columns (degrees are a SEPARATE
    column, so names need no honorific/degree stripping). No ORCIDs published
    → `orcid` NULL. PI degrees and academic rank have no home in the award
    schema and are dropped.
  * `affiliation.country` = 'US' when the institution state is a US
    state/territory, else NULL (a few international awards).

Output
------
s3://openalex-ingest/awards/aha/aha_projects.parquet

Usage
-----
    python aha_to_s3.py --input "/path/to/AHA export.xlsx"   # full run
    python aha_to_s3.py --input ... --skip-upload            # local smoke test
    python aha_to_s3.py --input ... --qa-crosscheck          # print RB vs Dim QA
    python aha_to_s3.py --input ... --allow-shrink           # override §1.4

Requirements
------------
    pip install pandas pyarrow openpyxl boto3
    AWS CLI configured for s3://openalex-ingest/awards/aha/
"""

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22, runbook §1.2 #7) ---
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

# Awarding body — American Heart Association. Verified F4320306230, country US,
# ROR https://ror.org/013kjyp64, DOI 10.13039/100000968. Path A (F4320*
# Crossref-registered funder, present in openalex.common.funder).
FUNDER_ID = 4320306230
FUNDER_DISPLAY_NAME = "American Heart Association"

PROVENANCE = "aha_report_builder"
CURRENCY = "USD"  # Hardcoded — AHA is a US funder

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/aha/aha_projects.parquet"

LANDING_PAGE_URL = "https://professional.heart.org/en/research-programs/aha-funded-research"

# The AHA workbook's RB tab has a two-row banner above the header row, so the
# real column names live on the 3rd row (0-based header index 2). The tab name
# carries the year range and can drift between exports, so we match by prefix.
RB_SHEET_PREFIX  = "RB pull"
DIM_SHEET_PREFIX = "Dimensions pull"
RB_HEADER_ROW  = 2
DIM_HEADER_ROW = 1

# US states + DC + territories (postal codes) — used to set affiliation.country.
US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC","PR","GU","VI","AS","MP",
}


# =============================================================================
# Read + normalise the workbook
# =============================================================================

def _find_sheet(xl: pd.ExcelFile, prefix: str) -> str:
    for name in xl.sheet_names:
        if str(name).strip().lower().startswith(prefix.lower()):
            return name
    raise SystemExit(f"[ERROR] no sheet starting with {prefix!r} in {xl.sheet_names}")


def read_rb(path: Path) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 1: Read AHA Report Builder tab")
    print("=" * 60)
    xl = pd.ExcelFile(path)
    sheet = _find_sheet(xl, RB_SHEET_PREFIX)
    df = pd.read_excel(path, sheet_name=sheet, header=RB_HEADER_ROW, dtype=object)
    df.columns = [str(c).strip() for c in df.columns]
    required = {"Award ID From GM", "Proposal Title", "Start Date", "End Date",
                "Funding Mechanism", "Award Total Amount"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"[ERROR] RB tab {sheet!r} missing columns: {missing}")
    # drop rows with no award id
    df = df[df["Award ID From GM"].notna()].copy()
    df["_award_id"] = df["Award ID From GM"].astype(str).str.strip()
    df = df[df["_award_id"] != ""]
    print(f"  sheet: {sheet!r}")
    print(f"  rows (PI-level): {len(df)}")
    print(f"  distinct award ids: {df['_award_id'].nunique()}")
    return df


# =============================================================================
# Field helpers
# =============================================================================

def _clean_str(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    # collapse internal whitespace/newlines
    return re.sub(r"\s+", " ", s)


def _clean_multiline(v) -> Optional[str]:
    """Like _clean_str but preserve paragraph structure of abstracts."""
    if v is None:
        return None
    s = str(v).replace("\r\n", "\n").strip()
    if not s or s.lower() == "nan":
        return None
    return s


def _to_iso_date(v) -> Optional[str]:
    if v is None:
        return None
    ts = pd.to_datetime(v, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d")


def _year_of(iso: Optional[str]) -> Optional[str]:
    return iso[:4] if iso else None


def _clean_amount(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(str(v).replace(",", "").replace("$", "").strip())
    except ValueError:
        return None
    if f <= 0:
        return None
    return f


def country_for_state(state: Optional[str], inst: Optional[str],
                      us_institutions: Optional[set] = None) -> Optional[str]:
    """AHA funds overwhelmingly US institutions. Set country='US' when the row
    carries a US state; otherwise recover it deterministically if the SAME
    institution name appears with a US state elsewhere in the export (AHA
    leaves state blank on many collaborative co-PI rows — Emory, Harvard,
    Stanford, ... — but fills it for the same institution on other awards).
    Genuinely international awardees (e.g. International Visiting Professorship
    hosts like University College London) never get a US state anywhere, so
    they correctly stay NULL rather than being wrongly stamped US."""
    s = (state or "").strip().upper()
    if s in US_STATES:
        return "US"
    if us_institutions and inst and inst.strip() in us_institutions:
        return "US"
    return None


def funding_type_for(mechanism: Optional[str]) -> str:
    m = (mechanism or "").lower()
    if any(k in m for k in ("predoctoral", "undergraduate", "student",
                            "supplement", "research experience", "bootcamp",
                            "sure", "summer")):
        return "training"
    if "fellowship" in m or "career development" in m or "scientist development" in m:
        return "fellowship"
    return "research"


def _investigator(row: dict, us_institutions: set) -> dict:
    """Build one investigator record in the target nested shape (JSON-ready)."""
    inst = _clean_str(row.get("Institution Name"))
    state = _clean_str(row.get("Institution State"))
    return {
        "given_name":  _clean_str(row.get("PI First Name")),
        "family_name": _clean_str(row.get("PI Last Name")),
        "orcid": None,                       # AHA publishes no ORCIDs
        "role_start": None,
        "affiliation": {
            "name": inst,
            "country": country_for_state(state, inst, us_institutions),
            "ids": None,
        },
    }


# =============================================================================
# Aggregate PI-level rows into one award row each
# =============================================================================

def build_dataframe(rb: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Aggregate to one row per award (multi-PI collapse)")
    print("=" * 60)

    # Institutions that carry a US state anywhere in the export — used to
    # recover country for co-PI rows where AHA left the state blank.
    st = rb["Institution State"].astype(str).str.strip().str.upper()
    us_institutions = set(
        rb.loc[st.isin(US_STATES), "Institution Name"]
        .dropna().astype(str).str.strip().unique()
    )

    out_rows: list[dict] = []
    n_multi = 0
    for award_id, grp in rb.groupby("_award_id", sort=False):
        recs = grp.to_dict("records")
        first = recs[0]  # first-listed row = lead / primary applicant

        # amount = SUM of the per-PI portions (collaborative awards split it)
        total = 0.0
        any_amt = False
        for r in recs:
            a = _clean_amount(r.get("Award Total Amount"))
            if a is not None:
                total += a
                any_amt = True
        amount = total if (any_amt and total > 0) else None

        # abstract: scientific (Technical) preferred, lay (General) fallback
        description = (_clean_multiline(first.get("Abstract Award Technical"))
                       or _clean_multiline(first.get("Abstract Award General")))

        start_iso = _to_iso_date(first.get("Start Date"))
        end_iso = _to_iso_date(first.get("End Date"))

        investigators = [_investigator(r, us_institutions) for r in recs]
        # keep only investigators that have at least a family name
        investigators = [i for i in investigators if i["family_name"]]
        if len(recs) > 1:
            n_multi += 1

        out_rows.append({
            "funder_award_id":   award_id,
            "title":             _clean_str(first.get("Proposal Title")),
            "description":       description,
            "amount":            f"{amount:.2f}" if amount is not None else None,
            "currency":          CURRENCY if amount is not None else None,
            "funder_scheme":     _clean_str(first.get("Funding Mechanism")),
            "program_name":      _clean_str(first.get("Program Name")),
            "funding_type":      funding_type_for(_clean_str(first.get("Funding Mechanism"))),
            "cycle":             _clean_str(first.get("Cycle")),
            "start_date":        start_iso,
            "end_date":          end_iso,
            "start_year":        _year_of(start_iso),
            "end_year":          _year_of(end_iso),
            "n_investigators":   str(len(investigators)),
            "investigators_json": json.dumps(investigators, ensure_ascii=False),
            "landing_page_url":  LANDING_PAGE_URL,
        })

    df = pd.DataFrame.from_records(out_rows)
    n = len(df)
    print(f"  awards: {n}   (multi-PI collaborative awards collapsed: {n_multi})")
    if n:
        def pct(col):
            c = df[col].notna().sum()
            return f"{c} ({c * 100 // n}%)"
        print(f"    title:        {pct('title')}")
        print(f"    description:  {pct('description')}")
        print(f"    amount:       {pct('amount')}")
        print(f"    start_date:   {pct('start_date')}")
        print(f"    end_date:     {pct('end_date')}")
        print(f"    scheme:       {pct('funder_scheme')}")
        amt = pd.to_numeric(df["amount"], errors="coerce")
        print(f"    amount USD:   min {amt.min():,.0f}  median {amt.median():,.0f}  max {amt.max():,.0f}")
        print(f"    total funded: ${amt.sum() / 1e6:,.1f}M")
        print("\n  By start year:")
        print(df.groupby("start_year").size().sort_index().to_string())
        print("\n  funding_type split:")
        print(df.groupby("funding_type").size().to_string())
    # Runbook §1.2.5 — cast all columns to string before parquet.
    df = df.astype("string")
    return df


# =============================================================================
# QA cross-check vs the Dimensions tab (never ingested; audit only)
# =============================================================================

def qa_crosscheck(path: Path, rb: pd.DataFrame) -> None:
    print("\n" + "=" * 60)
    print("QA: RB vs Dimensions coverage cross-check (Dimensions NOT ingested)")
    print("=" * 60)
    xl = pd.ExcelFile(path)
    try:
        dsheet = _find_sheet(xl, DIM_SHEET_PREFIX)
    except SystemExit:
        print("  no Dimensions tab found — skipping QA")
        return
    dim = pd.read_excel(path, sheet_name=dsheet, header=DIM_HEADER_ROW, dtype=object)
    dim.columns = [str(c).strip() for c in dim.columns]
    rb_ids = {x.upper() for x in rb["_award_id"]}
    dim_ids: set[str] = set()
    for v in dim.get("Grant Number(s)", pd.Series(dtype=object)).dropna():
        for part in re.split(r"[;,]", str(v)):
            p = part.strip().upper()
            if p and p != "NAN":
                dim_ids.add(p)
    print(f"  RB distinct award ids:  {len(rb_ids)}")
    print(f"  Dim distinct grant ids: {len(dim_ids)}")
    print(f"  overlap:                {len(rb_ids & dim_ids)}")
    print(f"  RB-only:                {len(rb_ids - dim_ids)}")
    dim_only = sorted(dim_ids - rb_ids)
    print(f"  Dimensions-only:        {len(dim_only)}  {dim_only[:20]}")
    print("  -> RB is an all-but-exact superset; Dimensions adds ~nothing.")


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "aha_projects.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df)} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 required for §1.4 shrink-check; rerun with --skip-upload to bypass"
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
    prev_path = output_dir / "_prev_aha_projects.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as e:
        print(f"    [ERROR] couldn't read existing parquet ({e}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)
    print(f"    previous: {prev_count}   new: {new_count}")
    if new_count < prev_count:
        if allow_shrink:
            print("    [OVERRIDE] --allow-shrink set; proceeding.")
            return True
        print(f"\n[ERROR] §1.4 violation: refusing to shrink ({prev_count} -> {new_count}).")
        return False
    print("    [OK] not smaller; safe to overwrite.")
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
    parser.add_argument("--input", type=Path, required=True,
                        help="Path to the AHA award-metadata .xlsx export")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/aha"))
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3 (local smoke test)")
    parser.add_argument("--qa-crosscheck", action="store_true",
                        help="Print RB-vs-Dimensions coverage QA and exit-friendly")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    if not args.input.exists():
        raise SystemExit(f"[ERROR] input not found: {args.input}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("American Heart Association → S3 Pipeline (partner Excel export)")
    print("=" * 60)
    print(f"  Input:      {args.input}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    rb = read_rb(args.input)

    if args.qa_crosscheck:
        qa_crosscheck(args.input, rb)

    df = build_dataframe(rb)
    if df.empty:
        print("[ERROR] no awards built — aborting before write")
        sys.exit(6)
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
    print("Next: notebooks/awards/CreateAHAAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
