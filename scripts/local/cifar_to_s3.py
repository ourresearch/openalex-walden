#!/usr/bin/env python3
"""
CIFAR (Canadian Institute for Advanced Research) to S3 Data Pipeline
=====================================================================

Downloads CIFAR researcher appointments from the cifar.ca WordPress REST
API and flattens them into one row per (researcher × program) for the
OpenAlex awards pipeline.

CIFAR is a unique funder: rather than awarding traditional grants, it
appoints researchers to thematic Programs (e.g., "Quantum Materials",
"Pan-Canadian AI Strategy") with specific roles (Fellow, Associate
Fellow, Advisor, Canada CIFAR AI Chair, etc.). Each appointment is
treated here as one row in the awards table, distinguished via
`funder_scheme` (the program name). One researcher may hold multiple
appointments across different programs — those become separate rows.

Source authority
----------------
cifar.ca is CIFAR's own site, in keeping with the prize-pattern
source-authority rule (no Wikipedia/Wikidata). Wordpress REST API,
public, no auth.

Endpoints used (all GET, no auth):
  - /wp-json/wp/v2/bio                    — 793 researcher profile posts
  - /wp-json/wp/v2/appointment_program    — 31 thematic programs (taxonomy)
  - /wp-json/wp/v2/appointment_role       — 62 roles (taxonomy)
  - /wp-json/wp/v2/institution            — 465 institution terms (taxonomy)
  - /wp-json/wp/v2/country                — 36 country terms (taxonomy)

Output
------
s3://openalex-ingest/awards/cifar/cifar_appointments.parquet

Amount and currency
-------------------
CIFAR does NOT publicly disclose per-fellow funding amounts (similar to
HHMI which has the same waiver at priority 44). `amount` and `currency`
ship as NULL by design; the Step 6.7 amount-coverage check is waived in
the notebook header.

Usage
-----
    python cifar_to_s3.py                # download + upload (default)
    python cifar_to_s3.py --skip-upload  # local dev / smoke test
    python cifar_to_s3.py --skip-download # reuse cached records.json
    python cifar_to_s3.py --allow-shrink  # override §1.4 shrink-check

Requirements
------------
    pip install pandas pyarrow requests boto3

    AWS CLI configured for write access to s3://openalex-ingest/awards/cifar/
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

# =============================================================================
# Configuration
# =============================================================================

WP_BASE = "https://cifar.ca/wp-json/wp/v2"

# Awarding body — Canadian Institute for Advanced Research.
# Verified in OpenAlex public API: F4320309949, country CA.
FUNDER_ID = 4320309949
FUNDER_DISPLAY_NAME = "Canadian Institute for Advanced Research"

PROVENANCE = "cifar_wp_rest"

# S3 destination — matches the awards pipeline convention.
S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/cifar/cifar_appointments.parquet"

# Polite — WP REST endpoints handle bursts but we throttle anyway.
MIN_REQUEST_INTERVAL_S = 0.25
USER_AGENT = "openalex-walden-cifar-ingest/1.0 (+https://openalex.org)"

# Roles that map to funding_type='research' vs other categories. The role
# vocabulary at CIFAR is messy — 62 roles, many overlap. We classify based
# on substring heuristics applied to the resolved role name (notebook does
# the CASE in SQL using the same vocabulary).
RESEARCH_ROLE_KEYWORDS = (
    "fellow", "chair", "scholar", "researcher", "investigator",
    "co-director", "associate director", "program director", "program co-director",
)
ADVISORY_ROLE_KEYWORDS = (
    "advisor", "advisory", "committee",
)


# =============================================================================
# HTTP helper (rate-limited, single shared Session)
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, params: Optional[dict] = None, timeout: int = 60) -> requests.Response:
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
        if len(batch) < 100:
            break
        page += 1
    print(f"    cached {len(out)} {slug} terms")
    return out


# =============================================================================
# Smoke-test gate (runbook §1)
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: confirm WP REST API is reachable")
    print("=" * 60)
    resp = _http_get(f"{WP_BASE}/bio", params={"per_page": 1, "page": 1})
    total = resp.headers.get("X-WP-Total")
    if not total:
        print("[ERROR] no X-WP-Total header — API shape changed?")
        sys.exit(3)
    print(f"  [OK] reachable; X-WP-Total={total} bios")
    try:
        n = int(total)
        if n < 200 or n > 2000:
            print(f"  [WARN] bio count {n} is outside expected 200-2000 — "
                  f"corpus may have shifted.")
    except ValueError:
        pass


# =============================================================================
# Download
# =============================================================================

def download_bios(output_dir: Path) -> Path:
    """
    Fetch all bios + taxonomies; emit one record per (bio × program)
    so that researchers in multiple programs become multiple rows. This
    matches the prize-pattern's (prize × laureate) idiom.
    """
    print("\n" + "=" * 60)
    print("Step 1: Download all CIFAR researcher appointments")
    print("=" * 60)

    program_terms = _fetch_taxonomy("appointment_program")
    role_terms = _fetch_taxonomy("appointment_role")
    institution_terms = _fetch_taxonomy("institution")
    country_terms = _fetch_taxonomy("country")

    # Fetch all bios in batches of 100.
    bios: list[dict] = []
    page = 1
    while True:
        resp = _http_get(f"{WP_BASE}/bio", params={"per_page": 100, "page": page,
                                                   "_embed": "false"})
        batch = resp.json()
        if not batch:
            break
        bios.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    print(f"\n  Fetched {len(bios)} bio posts")

    # Flatten to (bio × program) rows; resolve taxonomy FKs.
    rows: list[dict] = []
    skipped_no_program = 0
    for b in bios:
        programs = b.get("appointment_program") or []
        if not programs:
            skipped_no_program += 1
            continue
        # Resolve all taxonomy refs once per bio (reused across program rows).
        role_ids = b.get("appointment_role") or []
        role_names = [role_terms[r]["name"] for r in role_ids if r in role_terms]
        primary_role = role_names[0] if role_names else None
        institution_ids = b.get("institution") or []
        institution_names = [
            institution_terms[i]["name"] for i in institution_ids if i in institution_terms
        ]
        primary_institution = institution_names[0] if institution_names else None
        country_ids = b.get("country") or []
        country_names = [
            country_terms[c]["name"] for c in country_ids if c in country_terms
        ]
        primary_country = country_names[0] if country_names else None

        bio_title = unescape(b["title"]["rendered"])
        for prog_id in programs:
            term = program_terms.get(prog_id)
            if not term:
                continue
            rows.append({
                "bio_id": b["id"],
                "bio_slug": b["slug"],
                "bio_title": bio_title,
                "bio_link": b["link"],
                "bio_date": b.get("date"),
                "program_id": prog_id,
                "program_name": unescape(term["name"]),
                "program_slug": term["slug"],
                "role_id": role_ids[0] if role_ids else None,
                "role_name": primary_role,
                "all_roles": "|".join(role_names) if role_names else None,
                "institution_id": institution_ids[0] if institution_ids else None,
                "institution_name": primary_institution,
                "all_institutions": "|".join(institution_names) if institution_names else None,
                "country_id": country_ids[0] if country_ids else None,
                "country_name": primary_country,
            })

    print(f"\n  Built {len(rows)} (bio × program) rows from {len(bios)} bios")
    if skipped_no_program:
        print(f"  Skipped {skipped_no_program} bios with no appointment_program "
              f"(non-fellow profiles, e.g. staff)")

    raw_path = output_dir / "cifar_appointments_raw.json"
    raw_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2))
    print(f"  Cached to {raw_path}")
    return raw_path


# =============================================================================
# Build DataFrame
# =============================================================================

# Conservative name-split, same idiom as Holberg.
_DEGREE_SUFFIXES = {"PhD", "MD", "DPhil", "Jr.", "Sr.", "II", "III", "IV"}


def split_name(full: str) -> tuple[str, str]:
    tokens = full.split()
    while tokens and tokens[-1].rstrip(".") in {s.rstrip(".") for s in _DEGREE_SUFFIXES}:
        tokens.pop()
    if not tokens:
        return ("", "")
    if len(tokens) == 1:
        return ("", tokens[0])
    return (" ".join(tokens[:-1]), tokens[-1])


def classify_funding_type(role_name: Optional[str]) -> str:
    if not role_name:
        return "other"
    lower = role_name.lower()
    if any(k in lower for k in ADVISORY_ROLE_KEYWORDS):
        return "other"
    if any(k in lower for k in RESEARCH_ROLE_KEYWORDS):
        return "research"
    return "other"


def build_dataframe(raw_path: Path) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Build flat DataFrame")
    print("=" * 60)

    records = json.loads(raw_path.read_text())
    seen_award_ids: set[str] = set()
    flat: list[dict] = []
    for r in records:
        given, family = split_name(r["bio_title"])
        funder_award_id = f"cifar-{r['program_slug']}-{r['bio_slug']}"
        if funder_award_id in seen_award_ids:
            raise RuntimeError(
                f"Duplicate funder_award_id {funder_award_id!r}. CIFAR's bios "
                f"shouldn't appear in the same program twice. Investigate the "
                f"raw payload before re-running."
            )
        seen_award_ids.add(funder_award_id)
        flat.append({
            "funder_award_id":      funder_award_id,
            "bio_id":               r["bio_id"],
            "bio_slug":             r["bio_slug"],
            "researcher_full_name": r["bio_title"],
            "researcher_given":     given,
            "researcher_family":    family,
            "program_id":           r["program_id"],
            "program_name":         r["program_name"],
            "program_slug":         r["program_slug"],
            "role_id":              r["role_id"],
            "role_name":            r["role_name"],
            "all_roles":            r["all_roles"],
            "institution_name":     r["institution_name"],
            "all_institutions":     r["all_institutions"],
            "country_name":         r["country_name"],
            "funding_type_hint":    classify_funding_type(r["role_name"]),
            "landing_page_url":     r["bio_link"],
            "first_seen_date":      r["bio_date"],
            "declined":             False,  # schema parity, no declined fellows on record
        })

    df = pd.DataFrame.from_records(flat)
    print(f"  Built DataFrame: {len(df)} rows × {len(df.columns)} cols")
    print(f"  By program (top 10):")
    print(df.groupby("program_name").size().sort_values(ascending=False).head(10).to_string())
    print(f"\n  By funding_type_hint:")
    print(df.groupby("funding_type_hint").size().to_string())

    # Runbook §1.2.5 — all columns to string before parquet.
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "cifar_appointments.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df)} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """
    Runbook §1.4 — never shrink the corpus on re-ingest.
    """
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
            print("    no existing parquet at S3 path — first ingest, no shrink check.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest")
        return True
    prev_path = output_dir / "_prev_cifar_appointments.parquet"
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
            f"({prev_count} -> {new_count}). Investigate first; re-run with "
            f"--allow-shrink only if intentional."
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/cifar"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse cifar_appointments_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3 (local dev)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override the §1.4 shrink-check. Only use when "
                             "you've confirmed a smaller corpus is intentional.")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("CIFAR (Canadian Institute for Advanced Research) → S3 Pipeline")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "cifar_appointments_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        print(f"\n[SKIP] reusing {raw_path}")
    else:
        raw_path = download_bios(args.output_dir)

    df = build_dataframe(raw_path)
    parquet_path = write_parquet(df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload; manual upload command:")
        print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")
    else:
        ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
        if not ok:
            print(f"\n[WARN] upload failed/aborted. Parquet at {parquet_path}.")
            sys.exit(7)

    print("\n" + "=" * 60)
    print("Pipeline complete.")
    print(f"Next: notebooks/awards/CreateCIFARAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
