#!/usr/bin/env python3
"""
Doris Duke Foundation to S3 Data Pipeline
==========================================

Downloads two related datasets from dorisduke.org:

1. Organizational grants — embedded as JSON inside the React Server
   Components stream on dorisduke.org/grantees. Each card carries
   Date, Organization Name, Program, Amount ($), Description, Duration,
   Grantee URL, and a synthetic ID. ~129 unique grants 2021-2024.

2. Doris Duke Artist Awards (DDAA) recipients — exposed via the site's
   Sanity CMS public GROQ API at w64oxp25.apicdn.sanity.io. Each artist
   document carries name, slug, subtitle (discipline-flavored), and
   references an `artistEdition` (year) and `granteeDiscipline`. ~153
   artists across 13 editions (2012-2026).

**Source pattern is a first on the project**: Sanity CMS GROQ API.
Discovery path: page is Next.js with RSC streaming, RSC payload had
no clean JSON dump, BUT cdn.sanity.io image refs exposed the
projectId (`w64oxp25`) and dataset (`production`). The GROQ endpoint
at `${projectId}.apicdn.sanity.io/v2024-01-01/data/query/${dataset}?query=`
is unauthenticated and serves all published docs.

Schema choices
--------------
Doris Duke funds organizations (Source 1) AND individuals (Source 2).
We model them as two `funder_scheme` values within the same provenance:

  - `Doris Duke Foundation Grant`  → org-level, amount populated, funding_type=grant
  - `Doris Duke Artist Awards`     → individual, amount NULL §6.7-waived,
                                     funding_type=fellowship

Country = 'US' (Doris Duke Charitable Foundation funds US-based grantees
and US-based artists; the public site doesn't expose per-grant country).

Amount and currency
-------------------
Organizational grants: per-grant USD parsed from the source's `Amount`
field (note: the RSC payload encodes it as `$$X,YYY` — the double
dollar is a Sanity CMS portable-text quirk; we strip both).

DDAA artists: amount NULL with §6.7-waiver. The DDF Artist Awards program
amount has changed over its 14-year history and the live site does NOT
publish a verbatim per-year/per-recipient figure in machine-extractable
form. Boss can backfill via SQL CASE on edition year if the published
historical schedule is acceptable (rough public knowledge: ~$275K
through 2023, $325K from 2024+).

Output
------
s3://openalex-ingest/awards/doris_duke/doris_duke.parquet

Usage
-----
    python doris_duke_to_s3.py                    # full run
    python doris_duke_to_s3.py --skip-upload      # local dev
    python doris_duke_to_s3.py --skip-download    # reuse cached JSON
    python doris_duke_to_s3.py --limit 25         # smoke test
    python doris_duke_to_s3.py --allow-shrink     # override §1.4

Requirements
------------
    pip install pandas pyarrow requests boto3
"""

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim ---
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

SITE_BASE = "https://www.dorisduke.org"
GRANTS_URL = f"{SITE_BASE}/grantees"

SANITY_PROJECT = "w64oxp25"
SANITY_DATASET = "production"
SANITY_API     = f"https://{SANITY_PROJECT}.apicdn.sanity.io/v2024-01-01/data/query/{SANITY_DATASET}"

# Awarding body — Doris Duke Charitable Foundation.
# Verified F4320306134, country US, DOI 10.13039/100001025.
FUNDER_ID = 4320306134
FUNDER_DISPLAY_NAME = "Doris Duke Charitable Foundation"

PROVENANCE = "doris_duke"

# S3 destination.
S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/doris_duke/doris_duke.parquet"

USER_AGENT = "openalex-walden-doris-duke-ingest/1.0 (+https://openalex.org)"

MIN_REQUEST_INTERVAL_S = 0.4


# =============================================================================
# HTTP helper
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 60) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT})
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.get(url, timeout=timeout, allow_redirects=True)
    _last_request_t = time.monotonic()
    return resp


def groq_query(query: str) -> list:
    """Hit the Sanity GROQ API and return the `result` array (or empty list)."""
    url = f"{SANITY_API}?query={urllib.parse.quote(query)}"
    r = _http_get(url)
    r.raise_for_status()
    data = r.json()
    return data.get("result") or []


# =============================================================================
# Source 1: organizational grants from RSC stream on /grantees
# =============================================================================

_RSC_PUSH_RE = re.compile(r'self\.__next_f\.push\(\[1,\s*"((?:[^"\\]|\\.)*)"')


def extract_org_grants(html: str) -> list[dict]:
    """Extract `{"Publish": true, ...}` records from the Next.js RSC payload."""
    pushes = _RSC_PUSH_RE.findall(html)
    if not pushes:
        return []
    joined = "\n".join(pushes)
    decoded = joined.encode().decode("unicode_escape", errors="replace")
    records: list[dict] = []
    i = 0
    while True:
        idx = decoded.find('{"Publish":', i)
        if idx < 0:
            break
        try:
            rec, end = json.JSONDecoder().raw_decode(decoded[idx:])
        except Exception:
            i = idx + 1
            continue
        records.append(rec)
        i = idx + end
    # Dedup by ID (the RSC payload reuses the same record set across pushes)
    by_id: dict[str, dict] = {}
    for r in records:
        rid = r.get("ID")
        if rid and rid not in by_id:
            by_id[rid] = r
    return list(by_id.values())


_AMOUNT_RE = re.compile(r"\$+([0-9][\d,]*)")


def _parse_amount(s: Optional[str]) -> Optional[float]:
    """Source stores amounts as '$$X,YYY' (Sanity portable-text quirk). Strip $$/$, commas."""
    if not s:
        return None
    m = _AMOUNT_RE.search(s)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


# =============================================================================
# Source 2: DDAA artists via Sanity GROQ
# =============================================================================

def fetch_artists() -> list[dict]:
    """Pull all DDAA artist documents with edition year + discipline resolved."""
    query = (
        '*[_type=="artist"]{'
        '_id,name,slug,subtitle,'
        '"edition_title":edition->title,'
        '"discipline":discipline->title'
        '}'
    )
    return groq_query(query)


def fetch_individual_grantees() -> list[dict]:
    """The Sanity schema also has a `grantee` doctype (7 individuals as of 2026-05-27).
    These are non-Artist-Award fellowship-style grantees."""
    query = '*[_type=="grantee"]{_id,name,slug,subtitle,disciplines}'
    return groq_query(query)


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: /grantees RSC + Sanity GROQ")
    print("=" * 60)
    r = _http_get(GRANTS_URL)
    r.raise_for_status()
    grants = extract_org_grants(r.text)
    print(f"  org-grants from RSC stream: {len(grants)} unique")
    if grants:
        s = grants[0]
        print(f"  sample org grant: {s.get('Organization Name')} | {s.get('Date')} | {s.get('Amount')} | {s.get('Program')}")
    artists = fetch_artists()
    print(f"  DDAA artists via Sanity: {len(artists)}")
    if artists:
        a = artists[0]
        print(f"  sample artist: {a.get('name')} | edition={a.get('edition_title')} | discipline={a.get('discipline')}")
    inds = fetch_individual_grantees()
    print(f"  individual `grantee` docs via Sanity: {len(inds)}")


# =============================================================================
# Download
# =============================================================================

def download_all(output_dir: Path, limit: Optional[int]) -> Path:
    print("\n" + "=" * 60)
    print("Step 1: Fetch org grants + DDAA artists + individual grantees")
    print("=" * 60)
    r = _http_get(GRANTS_URL)
    r.raise_for_status()
    org_grants = extract_org_grants(r.text)
    print(f"  org-grants: {len(org_grants)}")

    artists = fetch_artists()
    print(f"  DDAA artists: {len(artists)}")

    individuals = fetch_individual_grantees()
    print(f"  individual grantees: {len(individuals)}")

    if limit is not None:
        org_grants = org_grants[:limit]
        artists = artists[:limit]
        individuals = individuals[:limit]
        print(f"  [LIMIT] truncated each source to first {limit}")

    payload = {
        "org_grants":   org_grants,
        "artists":      artists,
        "individuals":  individuals,
    }
    raw_path = output_dir / "doris_duke_raw.json"
    raw_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"\n  cached to {raw_path}")
    return raw_path


# =============================================================================
# Build DataFrame
# =============================================================================

# Conservative name-split for individual recipients.
_DEGREE_SUFFIXES = {"PhD", "Ph.D.", "MD", "M.D.", "DPhil", "Jr.", "Jr", "Sr.", "Sr",
                    "II", "III", "IV", "MPH", "MSc"}
_PREFIX_TITLES = {"Dr", "Dr.", "Prof", "Prof.", "Professor"}


def split_name(full: str) -> tuple[str, str]:
    tokens = full.split()
    while tokens and tokens[0].rstrip(".") in {t.rstrip(".") for t in _PREFIX_TITLES}:
        tokens.pop(0)
    while tokens and tokens[-1].rstrip(".,") in {s.rstrip(".,") for s in _DEGREE_SUFFIXES}:
        tokens.pop()
    if not tokens:
        return ("", "")
    if len(tokens) == 1:
        return ("", tokens[0])
    return (" ".join(tokens[:-1]), tokens[-1])


def _slugify(s: str) -> str:
    s = unescape(s).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def build_dataframe(raw_path: Path) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Build flat DataFrame")
    print("=" * 60)
    payload = json.loads(raw_path.read_text())
    rows: list[dict] = []
    seen_ids: set[str] = set()

    # ---- Source 1: organizational grants ----
    for r in payload.get("org_grants", []):
        rid = r.get("ID")
        if not rid:
            continue
        funder_award_id = f"doris-duke-grant-{_slugify(rid)}"
        if funder_award_id in seen_ids:
            continue
        seen_ids.add(funder_award_id)

        org = (r.get("Organization Name") or "").strip() or None
        date = r.get("Date") or None
        year = None
        if date and len(date) >= 4 and date[:4].isdigit():
            year = int(date[:4])
        amount = _parse_amount(r.get("Amount"))
        rows.append({
            "funder_award_id":  funder_award_id,
            "source_kind":      "organizational_grant",
            "scheme":           "Doris Duke Foundation Grant",
            "funding_type":     "grant",
            "year":             year,
            "approved_on":      date,
            "duration":         r.get("Duration") or None,
            "program":          r.get("Program") or None,
            "recipient_name":   None,
            "given_name":       None,
            "family_name":      None,
            "discipline":       None,
            "grantee_org":      org,
            "grantee_url":      r.get("Grantee Site") or None,
            "amount":           amount,
            "currency":         "USD" if amount is not None else None,
            "description":      r.get("Description") or None,
            "landing_page_url": r.get("Current URL") or GRANTS_URL,
        })

    # ---- Source 2: DDAA artists ----
    for a in payload.get("artists", []):
        aid = a.get("_id")
        if not aid:
            continue
        slug = (a.get("slug") or {}).get("current") if isinstance(a.get("slug"), dict) else None
        slug = slug or _slugify(a.get("name") or "")
        funder_award_id = f"doris-duke-artist-{_slugify(aid)}"
        if funder_award_id in seen_ids:
            continue
        seen_ids.add(funder_award_id)

        name = (a.get("name") or "").strip() or None
        given, family = split_name(name or "")
        edition = a.get("edition_title")
        year = int(edition) if edition and str(edition).isdigit() else None
        discipline = a.get("discipline") or a.get("subtitle") or None
        rows.append({
            "funder_award_id":  funder_award_id,
            "source_kind":      "ddaa_artist",
            "scheme":           "Doris Duke Artist Awards",
            "funding_type":     "fellowship",
            "year":             year,
            "approved_on":      None,
            "duration":         None,
            "program":          "Arts",
            "recipient_name":   name,
            "given_name":       given or None,
            "family_name":      family or None,
            "discipline":       discipline,
            "grantee_org":      None,
            "grantee_url":      None,
            "amount":           None,         # §6.7-waived
            "currency":         None,
            "description":      None,
            "landing_page_url": f"{SITE_BASE}/grantees/{slug}" if slug else None,
        })

    # ---- Source 3: individual `grantee` docs (7 non-artist fellowships) ----
    for g in payload.get("individuals", []):
        gid = g.get("_id")
        if not gid:
            continue
        slug = (g.get("slug") or {}).get("current") if isinstance(g.get("slug"), dict) else None
        slug = slug or _slugify(g.get("name") or "")
        funder_award_id = f"doris-duke-grantee-{_slugify(gid)}"
        if funder_award_id in seen_ids:
            continue
        seen_ids.add(funder_award_id)
        name = (g.get("name") or "").strip() or None
        given, family = split_name(name or "")
        disc = None
        ds = g.get("disciplines")
        if isinstance(ds, list) and ds:
            # Resolution would need a follow-up ->title fetch; ship the raw _ref list as a hint.
            first = ds[0]
            if isinstance(first, dict) and "title" in first:
                disc = first["title"]
        rows.append({
            "funder_award_id":  funder_award_id,
            "source_kind":      "grantee",
            "scheme":           "Doris Duke Grantee",
            "funding_type":     "fellowship",
            "year":             None,
            "approved_on":      None,
            "duration":         None,
            "program":          None,
            "recipient_name":   name,
            "given_name":       given or None,
            "family_name":      family or None,
            "discipline":       disc or g.get("subtitle") or None,
            "grantee_org":      None,
            "grantee_url":      None,
            "amount":           None,
            "currency":         None,
            "description":      None,
            "landing_page_url": f"{SITE_BASE}/grantees/{slug}" if slug else None,
        })

    df = pd.DataFrame.from_records(rows)
    n_amt = df["amount"].notna().sum()
    n_yr  = df["year"].notna().sum()
    n_org = df["grantee_org"].astype(bool).sum()
    n_name = df["recipient_name"].astype(bool).sum()
    print(f"  rows: {len(df)}")
    print(f"  by source_kind:")
    print(df.groupby("source_kind").size().to_string())
    print(f"\n  coverage:")
    print(f"    amount: {n_amt} ({n_amt*100/len(df):.0f}%)")
    print(f"    year:   {n_yr} ({n_yr*100/len(df):.0f}%)")
    print(f"    grantee_org (orgs only):     {n_org}")
    print(f"    recipient_name (artists/grantees only): {n_name}")
    if n_amt:
        amts = df["amount"].dropna()
        print(f"  amount stats (orgs only): min=${amts.min():,.0f} median=${amts.median():,.0f} max=${amts.max():,.0f}  total=${amts.sum():,.0f}")
    if n_yr:
        yrs = df["year"].dropna().astype(int)
        print(f"  year range: {yrs.min()} – {yrs.max()}")
    print(f"\n  By scheme:")
    print(df.groupby("scheme").size().to_string())
    # Runbook §1.2.5 — astype("string") immediately before parquet write
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "doris_duke.parquet"
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
    prev_path = output_dir / "_prev_doris_duke.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/doris_duke"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse doris_duke_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap each source to first N rows (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Doris Duke Charitable Foundation → S3 Pipeline")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "doris_duke_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        print(f"\n[SKIP] reusing {raw_path}")
    else:
        raw_path = download_all(args.output_dir, args.limit)

    df = build_dataframe(raw_path)
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
    print(f"Next: notebooks/awards/CreateDorisDukeAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
