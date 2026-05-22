#!/usr/bin/env python3
"""
Lemelson-MIT Awards → S3 Pipeline (PRIZE PATTERN, Drupal JSON:API multi-program)
=================================================================================

Downloads laureates from ALL of the Lemelson-MIT Program's award
programs administered at MIT under the Lemelson Foundation:

  1. **Lemelson-MIT Prize** ($500K/yr 1995-2019, discontinued; 26 winners)
  2. **Lemelson-MIT Student Prize** ($15K graduate / $10K undergrad,
     1995-2021; 4 categories: Cure it / Eat it / Move it / Use it / Drive it)
  3. **Lemelson-MIT Award for Global Innovation**
  4. **Lemelson-MIT Award for Sustainability** ($100K, since 2024)
  5. **Lemelson-MIT Lifetime Achievement Award**
  6. **Invention Apprentice** (a teen invention program; included for
     completeness even though it's not really a research prize)

Source: `lemelson.mit.edu` — Drupal site with a fully-public **JSON:API**
endpoint at `/jsonapi/node/award_winner`. Method #2 on the runbook ladder
(REST/JSON:API), discovered via the Drupal-standard pattern of probing
`/jsonapi` and finding `node--award_winner` in the resource registry.

Each winner record carries:
  - title                         → recipient's full name
  - field_award_year              → award year
  - field_invention_name          → the invention/contribution
  - field_school                  → home institution
  - field_location                → {country_code, locality, administrative_area}
  - field_award (relationship)    → taxonomy_term--award (program name)
  - field_prize_category (rel.)   → taxonomy_term--prize_category (Cure/Eat/Move/Use/Drive)
  - field_invention_type (rel.)   → taxonomy_term--invention_types
  - body                          → bio paragraphs
  - path.alias                    → /award-winners/{slug}

**This is the first multi-program funder ingest where each program has
a distinct amount tier AND distinct funder_scheme** — analogous to
Welch (#133) but with 6 programs instead of 2, and with several
programs lacking a documented amount (where we apply §6.7 waiver
per-program).

Awarding body in OpenAlex: The Lemelson Foundation (F4320314845, US, DOI 10.13039/100014181).
The MIT program is the administrative arm; the funder is the Lemelson
Foundation that endows the prizes.

Output
------
s3://openalex-ingest/awards/lemelson_mit/lemelson_mit_awards.parquet

Usage
-----
    python lemelson_mit_to_s3.py                # full run (118 rows, ~3 paginated calls)
    python lemelson_mit_to_s3.py --skip-upload  # local dev
    python lemelson_mit_to_s3.py --skip-download
    python lemelson_mit_to_s3.py --allow-shrink

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
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

JSONAPI_BASE = "https://lemelson.mit.edu/jsonapi"
WINNERS_ENDPOINT = f"{JSONAPI_BASE}/node/award_winner?page%5Blimit%5D=100"
AWARD_TAXONOMY_URL = f"{JSONAPI_BASE}/taxonomy_term/award?page%5Blimit%5D=50"
CATEGORY_TAXONOMY_URL = f"{JSONAPI_BASE}/taxonomy_term/prize_category?page%5Blimit%5D=50"

# Awarding body — The Lemelson Foundation (the MIT program is administrative).
# Verified F4320314845, country US, DOI 10.13039/100014181.
FUNDER_ID = 4320314845
FUNDER_DISPLAY_NAME = "Lemelson Foundation"

PROVENANCE = "lemelson_mit"
CURRENCY = "USD"

# Per-program amounts (USD). Verified 2026-05-22 from foundation pages
# (lemelson.mit.edu/500k-prize-impact-invention, /studentprize). Programs
# without a published per-laureate amount get NULL and a §6.7 waiver.
AWARD_AMOUNTS = {
    "Lemelson-MIT Prize":                       500_000.0,
    "Lemelson-MIT Student Prize":                15_000.0,   # graduate; undergrad team is $10K
    "Lemelson-MIT Award for Sustainability":    100_000.0,
    # Without published per-laureate amounts → NULL (§6.7 waiver)
    "Lemelson-MIT Award for Global Innovation":   None,
    "Lemelson-MIT Lifetime Achievement Award":    None,
    "Invention Apprentice":                       None,
}

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/lemelson_mit/lemelson_mit_awards.parquet"

USER_AGENT = "openalex-walden-lemelson-mit-ingest/1.0 (+https://openalex.org)"

MIN_REQUEST_INTERVAL_S = 0.4


# =============================================================================
# HTTP helper (rate-limited)
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 30) -> requests.Response:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/vnd.api+json",
        })
    elapsed = time.monotonic() - _last_request_t
    if elapsed < MIN_REQUEST_INTERVAL_S:
        time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
    resp = _session.get(url, timeout=timeout, allow_redirects=True)
    _last_request_t = time.monotonic()
    return resp


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: JSON:API endpoint reachable + node--award_winner exists")
    print("=" * 60)
    resp = _http_get(JSONAPI_BASE)
    resp.raise_for_status()
    j = resp.json()
    links = j.get("links", {})
    if "node--award_winner" not in links:
        print(f"[ERROR] node--award_winner not in /jsonapi root — registry may have changed")
        sys.exit(3)
    print("  /jsonapi confirms node--award_winner endpoint")


# =============================================================================
# Taxonomy fetch — map award term IDs to names
# =============================================================================

def fetch_taxonomy(url: str, label: str) -> dict[str, str]:
    """Return {uuid: human_name} for a taxonomy term endpoint."""
    out: dict[str, str] = {}
    while url:
        resp = _http_get(url)
        resp.raise_for_status()
        j = resp.json()
        for t in j.get("data", []):
            tid = t.get("id")
            name = t.get("attributes", {}).get("name")
            if tid and name:
                out[tid] = name
        url = j.get("links", {}).get("next", {}).get("href")
    print(f"  {label}: {len(out)} terms — {sorted(out.values())}")
    return out


# =============================================================================
# Pagination — fetch all award_winner nodes
# =============================================================================

def fetch_all_winners() -> list[dict]:
    print("\n" + "=" * 60)
    print("Step 1: Fetch all Lemelson-MIT award winners via Drupal JSON:API")
    print("=" * 60)
    all_records: list[dict] = []
    url: Optional[str] = WINNERS_ENDPOINT
    page = 0
    while url:
        page += 1
        resp = _http_get(url)
        resp.raise_for_status()
        j = resp.json()
        data = j.get("data", [])
        all_records.extend(data)
        print(f"  page {page}: +{len(data)} (total {len(all_records)})")
        url = j.get("links", {}).get("next", {}).get("href")
    return all_records


# =============================================================================
# Name splitter (runbook §2.4.1)
# =============================================================================

_DEGREE_SUFFIXES = {"PhD", "Ph.D.", "Ph.D", "MD", "M.D.", "DPhil", "ScD",
                    "Jr.", "Jr", "Sr.", "Sr", "II", "III", "IV"}
_HONORIFIC_PREFIXES_RE = re.compile(
    r'^(?:Dr\.?|Prof\.?|Professor|Mr\.?|Ms\.?|Mrs\.?|Sir|Dame)\s+',
    re.I,
)
_JOINT_RE = re.compile(r'\s+(?:and|&)\s+', re.I)


def split_name(full_name: str) -> tuple[Optional[str], Optional[str], bool]:
    if not full_name:
        return None, None, False
    is_joint = bool(_JOINT_RE.search(full_name))
    name = _JOINT_RE.split(full_name, maxsplit=1)[0]
    name = _HONORIFIC_PREFIXES_RE.sub("", name).strip()
    parts = [p.strip() for p in name.replace(";", ",").split(",")]
    name = parts[0].strip()
    toks = name.split()
    while toks and toks[-1].rstrip(".") in {s.rstrip(".") for s in _DEGREE_SUFFIXES}:
        toks.pop()
    if not toks:
        return None, None, is_joint
    if len(toks) == 1:
        return None, toks[0], is_joint
    return " ".join(toks[:-1]), toks[-1], is_joint


def _strip_html(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    text = re.sub(r'<[^>]+>', ' ', s)
    return unescape(re.sub(r'\s+', ' ', text)).strip() or None


# =============================================================================
# Build DataFrame
# =============================================================================

def build_dataframe(all_records: list[dict],
                    award_terms: dict[str, str],
                    category_terms: dict[str, str]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print(f"Step 2: Build flat DataFrame from {len(all_records)} raw records")
    print("=" * 60)

    seen_award_ids: set[str] = set()
    final: list[dict] = []
    unknown_program = 0

    for r in all_records:
        attrs = r.get("attributes", {})
        rels = r.get("relationships", {})

        year = attrs.get("field_award_year")
        if year is None:
            continue
        year = int(year)

        title = attrs.get("title") or ""
        path_alias = (attrs.get("path") or {}).get("alias") or ""
        slug = path_alias.rsplit("/", 1)[-1] if path_alias else re.sub(
            r'[^a-z0-9]+', '-', title.lower()).strip('-')

        # Resolve the award program from the field_award relationship
        award_rel = (rels.get("field_award") or {}).get("data") or {}
        award_term_id = award_rel.get("id") if isinstance(award_rel, dict) else None
        program = award_terms.get(award_term_id, "Unknown Lemelson Program")
        if program == "Unknown Lemelson Program":
            unknown_program += 1

        # Prize category (Cure/Eat/Move/Use/Drive — student prize subcategory)
        cat_rel = (rels.get("field_prize_category") or {}).get("data") or {}
        cat_id = cat_rel.get("id") if isinstance(cat_rel, dict) else None
        category = category_terms.get(cat_id)

        amount = AWARD_AMOUNTS.get(program)
        currency = CURRENCY if amount is not None else None

        given, family, is_joint = split_name(title)

        # description from body
        body = attrs.get("body") or {}
        body_text = _strip_html(body.get("value")) if isinstance(body, dict) else None
        invention = attrs.get("field_invention_name")
        # Compose description: invention name (as headline) + body
        desc_parts = []
        if invention:
            desc_parts.append(invention)
        if body_text:
            # Cap body to first ~600 chars
            if len(body_text) > 600:
                body_text = body_text[:600].rsplit(" ", 1)[0] + "…"
            desc_parts.append(body_text)
        description = ". ".join(desc_parts) if desc_parts else None

        # affiliation
        affiliation = attrs.get("field_school")

        # Country
        location = attrs.get("field_location") or {}
        country = location.get("country_code") if isinstance(location, dict) else None

        # Unique per (program, year, slug)
        program_slug = re.sub(r'[^a-z0-9]+', '-', program.lower()).strip('-')
        funder_award_id = f"lemelson-mit-{program_slug}-{year}-{slug}"
        if funder_award_id in seen_award_ids:
            # Lemelson sometimes has multiple winners per (program, year)
            # with different slugs — should be unique. Add a counter just
            # in case.
            i = 2
            base = funder_award_id
            while f"{base}-v{i}" in seen_award_ids:
                i += 1
            funder_award_id = f"{base}-v{i}"
        seen_award_ids.add(funder_award_id)

        display_name = f"{program} {year} — {title}"

        final.append({
            "funder_award_id":   funder_award_id,
            "year":              year,
            "slug":              slug,
            "name":              title,
            "given_name":        given,
            "family_name":       family,
            "is_joint":          is_joint,
            "program":           program,
            "category":          category,         # Student-Prize subcategory; NULL for other programs
            "invention_name":    invention,
            "affiliation":       affiliation,
            "country":           country,
            "display_name":      display_name,
            "description":       description,
            "amount":            amount,
            "currency":          currency,
            "start_date":        f"{year}-01-01",
            "end_date":          f"{year}-12-31",
            "landing_page_url":  f"https://lemelson.mit.edu{path_alias}" if path_alias else None,
            "declined":          False,
        })

    if unknown_program:
        print(f"  [WARN] {unknown_program} rows had no resolvable program")

    df = pd.DataFrame.from_records(final)
    print(f"  rows: {len(df)}")
    print(f"  year range: {df['year'].min()}-{df['year'].max()}")
    print(f"  programs:")
    print(df.groupby("program").size().to_string())
    print(f"  amount coverage (NULL = §6.7 waiver for programs without")
    print(f"  documented per-laureate amount):")
    print(f"    has_amount: {df['amount'].notna().sum()}/{len(df)} "
          f"({df['amount'].notna().sum()*100/len(df):.0f}%)")
    print(f"  affiliation coverage: {df['affiliation'].notna().sum()}/{len(df)} "
          f"({df['affiliation'].notna().sum()*100/len(df):.0f}%)")
    print(f"  country coverage: {df['country'].notna().sum()}/{len(df)} "
          f"({df['country'].notna().sum()*100/len(df):.0f}%)")
    print(f"  description coverage: {df['description'].notna().sum()}/{len(df)} "
          f"({df['description'].notna().sum()*100/len(df):.0f}%)")

    # Runbook §1.2.5 — string before parquet
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "lemelson_mit_awards.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df)} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook §1.4."""
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
    prev_path = output_dir / "_prev_lemelson.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/lemelson"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse lemelson_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Lemelson-MIT → S3 Pipeline (Drupal JSON:API, multi-program PRIZE)")
    print("=" * 60)
    print(f"  Awarding body: {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Output dir:    {args.output_dir.absolute()}")
    print(f"  S3 dest:       s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:       {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "lemelson_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        cached = json.loads(raw_path.read_text())
        all_records = cached["records"]
        award_terms = cached["award_terms"]
        category_terms = cached["category_terms"]
        print(f"\n[SKIP] reusing {raw_path} with {len(all_records)} cached records")
    else:
        # Fetch the two taxonomies first so we can label programs as we build
        print("\n" + "=" * 60)
        print("Step 0: Fetch award + prize_category taxonomies")
        print("=" * 60)
        award_terms = fetch_taxonomy(AWARD_TAXONOMY_URL, "award")
        category_terms = fetch_taxonomy(CATEGORY_TAXONOMY_URL, "prize_category")
        all_records = fetch_all_winners()
        raw_path = args.output_dir / "lemelson_raw.json"
        raw_path.write_text(json.dumps({
            "records": all_records,
            "award_terms": award_terms,
            "category_terms": category_terms,
        }, ensure_ascii=False, indent=2))
        print(f"\n  Cached {len(all_records)} raw records + 2 taxonomies to {raw_path}")

    df = build_dataframe(all_records, award_terms, category_terms)
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
    print(f"Next: notebooks/awards/CreateLemelsonMITAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
