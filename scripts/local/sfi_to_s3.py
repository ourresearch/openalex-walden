#!/usr/bin/env python3
"""
Science Foundation Ireland (SFI) awards.

Source: SFI's official open-data Grant Commitments file (Method 1, bulk CSV),
published on data.gov.ie / sfi.ie:
  https://www.sfi.ie/about-us/governance/open-data/Open-Data-2024-07-31.csv

This is a premium per-grant feed: real SFI proposal ids + named lead applicant
WITH ORCID + research body WITH ROR + title + real start/end dates + the total
commitment amount (EUR). funder_award_id = the Proposal ID. ORCID and ROR are
carried through for exact person/institution linkage downstream.

Awarding body funder: F4320320847 (Science Foundation Ireland, IE; now operating
as Taighde Eireann - Research Ireland, but the OpenAlex funder + the file's
`Funder Name`/Crossref id remain SFI). provenance = 'sfi_open_data'.
"""
import sys

# --- Fleet UTF-8 compatibility shim ---
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass
if sys.platform == "win32":
    import builtins
    from pathlib import Path as _P
    _open = builtins.open
    def _utf8_open(f, mode="r", *a, **k):
        if "b" not in mode and k.get("encoding") is None:
            k["encoding"] = "utf-8"
        return _open(f, mode, *a, **k)
    builtins.open = _utf8_open
    _wt, _rt = _P.write_text, _P.read_text
    _P.write_text = lambda self, data, encoding="utf-8", **k: _wt(self, data, encoding=encoding, **k)
    _P.read_text = lambda self, encoding="utf-8", **k: _rt(self, encoding=encoding, **k)

import argparse
import io
import re
import ssl
import urllib.request
from pathlib import Path
from typing import Optional

import pandas as pd

CSV_URL = "https://www.sfi.ie/about-us/governance/open-data/Open-Data-2024-07-31.csv"
FUNDER_ID = 4320320847          # Science Foundation Ireland, in openalex.common.funder
PROVENANCE = "sfi_open_data"
FUNDER_NAME_FILTER = "science foundation ireland"
COUNTRY = "IE"
CURRENCY = "EUR"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/sfi/sfi_projects.parquet"
USER_AGENT = "Mozilla/5.0 (compatible; openalex-awards-ingest/1.0)"

_FELLOWSHIP_RE = re.compile(r"fellow|career|starting investigator", re.IGNORECASE)
# Canonical name splitter (wolf_to_s3.py idiom): strip prefix titles + trailing
# degree/generational suffixes, last remaining token = family, rest = given.
_PREFIX = {"dr", "prof", "professor", "mr", "ms", "mrs", "miss"}
_SUFFIX = {"phd", "md", "dphil", "dsc", "scd", "mph", "msc", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}


def split_name(full: str) -> tuple[Optional[str], Optional[str]]:
    toks = [t for t in str(full).split() if t]
    while toks and toks[0].lower().strip(".") in _PREFIX:
        toks.pop(0)
    while toks and toks[-1].lower().strip(".,") in _SUFFIX:
        toks.pop()
    if not toks:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


# Some SFI "Lead Applicant" values are an institutional ROLE, not a person, on
# block/infrastructure awards (e.g. "VP Research (UCD)", "VP Research UCC"). Those
# must NOT be split into given/family (§6.4a / Carlsberg institution-as-person).
# An ALL-CAPS institution abbreviation in trailing parens is institutional; a
# mixed-case parenthesised token (a maiden name like "(Saldova)") is a real person.
def is_institutional_lead(name: str) -> bool:
    n = str(name)
    if re.search(r"\bv\.?\s*p\.?\s+research\b|\bvice[- ]president\b|\bresearch office\b", n, re.I):
        return True
    if re.search(r"\(\s*[A-Z]{2,6}\s*\)\s*$", n):   # "(UCD)", "(NUIG)" — institution, not a surname
        return True
    return False


def log(m): print(m, flush=True)


def _norm(s):
    if s is None:
        return None
    s = str(s).strip()
    return s or None


def download(limit: Optional[int]) -> pd.DataFrame:
    log("=" * 60)
    log("Step 1: download SFI open-data grant-commitments CSV")
    log("=" * 60)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE   # sfi.ie cert chain is incomplete from some hosts
    raw = urllib.request.urlopen(
        urllib.request.Request(CSV_URL, headers={"User-Agent": USER_AGENT}), timeout=120, context=ctx
    ).read()
    log(f"  downloaded {len(raw):,} bytes")
    df = pd.read_csv(io.BytesIO(raw), dtype=str, encoding="latin-1", on_bad_lines="skip")
    df.columns = [c.strip() for c in df.columns]
    log(f"  {len(df):,} rows, {len(df.columns)} cols")
    # keep only SFI-funded rows (file is single-funder, but guard anyway)
    if "Funder Name" in df.columns:
        before = len(df)
        df = df[df["Funder Name"].fillna("").str.strip().str.lower() == FUNDER_NAME_FILTER]
        log(f"  filtered to SFI funder rows: {len(df):,} (dropped {before - len(df):,})")
    if limit:
        df = df.head(limit)
        log(f"  [limit {limit}]")
    return df


def _amount(v) -> Optional[float]:
    try:
        a = float(str(v).replace(",", "").strip())
    except (ValueError, TypeError):
        return None
    # §6.7: never ship 0 or negative (de-commitment/clawback rows); NULL them, never impute.
    return a if a > 0 else None


def _date(v) -> Optional[str]:
    s = _norm(v)
    if not s:
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    return m.group(1) if m else None


def build_dataframe(src: pd.DataFrame) -> pd.DataFrame:
    log("\n" + "=" * 60)
    log("Step 2: build award rows")
    log("=" * 60)
    rows = []
    seen = set()
    for _, r in src.iterrows():
        pid = _norm(r.get("Proposal ID"))
        if not pid or pid in seen:
            continue
        seen.add(pid)
        scheme = " - ".join(x for x in (_norm(r.get("Programme Name")), _norm(r.get("Sub-Programme"))) if x)
        lead_raw = r.get("Lead Applicant") or ""
        if is_institutional_lead(lead_raw):
            given = family = orcid = None   # institutional/role placeholder, not a person
        else:
            given, family = split_name(lead_raw)
            orcid = _norm(r.get("ORCID"))
            orcid = orcid if (orcid and "orcid.org" in orcid) else None
        ror = _norm(r.get("Research Body ROR ID"))
        rows.append({
            "funder_award_id":  pid,
            "title":            _norm(r.get("Proposal Title")),
            "funder_scheme":    scheme or None,
            "funding_type":     "fellowship" if (scheme and _FELLOWSHIP_RE.search(scheme)) else "grant",
            "lead_given_name":  given,
            "lead_family_name": family,
            "lead_orcid":       orcid if (orcid and "orcid.org" in orcid) else None,
            "institution_name": _norm(r.get("Research Body")),
            "institution_ror":  ror if (ror and "ror.org" in ror) else None,
            "country":          COUNTRY,
            "amount":           _amount(r.get("Current Total Commitment")),
            "currency":         CURRENCY,
            "start_date":       _date(r.get("Start Date")),
            "end_date":         _date(r.get("End Date")),
        })
    df = pd.DataFrame.from_records(rows)
    if df.empty:
        raise RuntimeError("No SFI rows built — aborting.")
    dup = df["funder_award_id"].duplicated()
    if dup.any():
        raise RuntimeError(f"Duplicate Proposal IDs: {df['funder_award_id'][dup].tolist()[:5]}")
    n = len(df)
    cov = lambda c: f"{df[c].notna().sum()*100//n}%"
    log(f"  rows={n}")
    for c in ("title", "lead_family_name", "lead_orcid", "institution_name",
              "institution_ror", "amount", "start_date"):
        log(f"    {c:18} {cov(c)}")
    a = pd.to_numeric(df["amount"], errors="coerce").dropna()
    log(f"  amount(EUR>0): n={len(a)} avg={a.mean():,.0f} min={a.min():,.0f} max={a.max():,.0f}")
    yrs = pd.to_numeric(df["start_date"].str[:4], errors="coerce").dropna()
    log(f"  start_year {int(yrs.min())}-{int(yrs.max())} | funding_type {df['funding_type'].value_counts().to_dict()}")
    return df.astype("string")


def check_no_shrink(df, allow_shrink):
    try:
        import boto3
        boto3.client("s3").head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev = pd.read_parquet(f"s3://{S3_BUCKET}/{S3_KEY}")
        if len(df) < len(prev) and not allow_shrink:
            raise SystemExit(f"§1.4 shrink-check FAILED: new {len(df):,} < existing {len(prev):,}")
        log(f"  §1.4 shrink-check OK ({len(df):,} >= {len(prev):,})")
    except SystemExit:
        raise
    except Exception as e:
        log(f"  §1.4 shrink-check: no prior/n.a. ({type(e).__name__})")


def main():
    ap = argparse.ArgumentParser(description="SFI open-data awards -> parquet -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/sfi"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    df = build_dataframe(download(args.limit))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "sfi_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"\n  wrote {len(df):,} rows -> {out} ({out.stat().st_size//1024} KB)")
    if args.skip_upload:
        log("  --skip-upload: not uploading")
        log(f"  manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
        return
    check_no_shrink(df, args.allow_shrink)
    import boto3
    boto3.client("s3").upload_file(str(out), S3_BUCKET, S3_KEY)
    log(f"  uploaded -> s3://{S3_BUCKET}/{S3_KEY}")


if __name__ == "__main__":
    main()
