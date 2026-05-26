#!/usr/bin/env python3
"""
Human Frontier Science Program awards -> S3 Data Pipeline
=========================================================

Downloads award records from HFSP's official public awardee listing and writes
a parquet file for the OpenAlex awards pipeline.

Source authority
----------------
The source is first-party:

    https://www.hfsp.org/awardees/awards

This Drupal page is static HTML with numbered pagination. Each award card
contains award year, HFSP program/type, project/fellowship title, listed
awardees, institutions, cities, countries, and usually an abstract. The page
does not publish per-award monetary amounts, so `amount` and `currency` remain
NULL by source authority.

OpenAlex funder mapping note
----------------------------
OpenAlex has two HFSP funder rows with the same ROR:
  - F4320320338, DOI 10.13039/501100000854
  - F4320307846, DOI 10.13039/100004412

Crossref's `501100000854` row includes alternate names "International Human
Frontier Science Program Organization" and "HFSPO", so this ingest uses
F4320320338 and documents the duplicate in the tracker and notebook.

Output
------
s3://openalex-ingest/awards/hfsp/hfsp_awards.parquet

Usage
-----
    python hfsp_to_s3.py --skip-upload
    python hfsp_to_s3.py --limit 20 --skip-upload
    python hfsp_to_s3.py --skip-download --skip-upload
    python hfsp_to_s3.py --allow-shrink

Requirements
------------
    pip install beautifulsoup4 pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
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


BASE_URL = "https://www.hfsp.org"
AWARDS_URL = f"{BASE_URL}/awardees/awards"

FUNDER_ID = 4320320338
FUNDER_DISPLAY_NAME = "Human Frontier Science Program"
PROVENANCE = "hfsp_awards_listing"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/hfsp/hfsp_awards.parquet"

USER_AGENT = "openalex-walden-hfsp-ingest/1.0 (+https://openalex.org)"
MIN_REQUEST_INTERVAL_S = 0.25


_session: Optional[requests.Session] = None
_last_request_t = 0.0


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
    return _session


def polite_get(url: str, timeout: int = 90, max_attempts: int = 4) -> requests.Response:
    global _last_request_t
    session = get_session()
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        elapsed = time.monotonic() - _last_request_t
        if elapsed < MIN_REQUEST_INTERVAL_S:
            time.sleep(MIN_REQUEST_INTERVAL_S - elapsed)
        try:
            resp = session.get(url, timeout=timeout)
            _last_request_t = time.monotonic()
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                sleep_s = min(2 ** attempt, 20)
                print(f"  [WARN] HTTP {resp.status_code}; retrying in {sleep_s}s")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            sleep_s = min(2 ** attempt, 20)
            print(f"  [WARN] request failed ({exc}); retrying in {sleep_s}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"GET failed after {max_attempts} attempts: {url}") from last_exc


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def slugify(text: str, max_len: int = 90) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return (text[:max_len].strip("-") or "row")


def split_hfsp_name(name_with_nationality: str) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str], str]:
    text = clean_text(name_with_nationality) or ""
    nat_match = re.search(r"\(([^)]+)\)\s*$", text)
    paren_value = clean_text(nat_match.group(1)) if nat_match else None
    role = "host_supervisor" if paren_value and "supervisor" in paren_value.lower() else "awardee"
    nationality = None if role == "host_supervisor" else paren_value
    name = clean_text(re.sub(r"\s*\([^)]+\)\s*$", "", text)) or ""
    tokens = name.split()
    if not tokens:
        return None, None, None, nationality, role

    # HFSP renders names as FAMILY Given, with multi-token uppercase family
    # names such as "DE LA IGLESIA Horacio". If all tokens are uppercase,
    # keep the first as family and the rest as given initials/name.
    family_tokens: list[str] = []
    given_tokens: list[str] = []
    for i, token in enumerate(tokens):
        stripped = token.strip(".,")
        if i == 0 or (stripped.upper() == stripped and len(stripped) > 1 and not given_tokens):
            family_tokens.append(token)
        else:
            given_tokens = tokens[i:]
            break
    if not given_tokens and len(tokens) > 1:
        family_tokens = [tokens[0]]
        given_tokens = tokens[1:]

    family = clean_text(" ".join(family_tokens).title())
    given = clean_text(" ".join(given_tokens))
    return name, given, family, nationality, role


def parse_affiliation(text: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    raw_lines = [clean_text(line.strip(" -")) for line in text.splitlines()]
    lines = [line for line in raw_lines if line]
    if not lines:
        return None, None, None
    institution = lines[0]
    city = None
    country = None
    if len(lines) > 1:
        place = lines[1]
        parts = [clean_text(part) for part in re.split(r"\s+-\s+", place) if clean_text(part)]
        if len(parts) >= 2:
            city = parts[0]
            country = parts[-1]
        else:
            country = place
    return institution, city, country


def program_to_funding_type(program: Optional[str]) -> str:
    program_l = (program or "").lower()
    if "fellowship" in program_l:
        return "fellowship"
    if "unlabeled hfsp fellowship" in program_l:
        return "fellowship"
    return "grant"


def discover_max_page() -> int:
    resp = polite_get(AWARDS_URL)
    soup = BeautifulSoup(resp.text, "html.parser")
    pages = [0]
    for anchor in soup.find_all("a", href=True):
        match = re.search(r"[?&]page=(\d+)", anchor["href"])
        if match:
            pages.append(int(match.group(1)))
    max_page = max(pages)
    print(f"  Pagination advertises page 0 through {max_page}")
    return max_page


def parse_award_card(card, page_number: int, card_index: int) -> dict[str, Any]:
    small = card.find("small")
    small_text = clean_text(small.get_text(" ", strip=True)) if small else ""
    year_match = re.search(r"\b(19|20)\d{2}\b", small_text or "")
    year = year_match.group(0) if year_match else None
    program_div = small.find("div") if small else None
    program = clean_text(program_div.get_text(" ", strip=True)) if program_div else None
    title_tag = card.find("h3")
    title = clean_text(title_tag.get_text(" ", strip=True)) if title_tag else None

    people: list[dict[str, Optional[str]]] = []
    for item in card.select(".list-group-item"):
        h5 = item.find("h5")
        p = item.find("p")
        name_raw = clean_text(h5.get_text(" ", strip=True)) if h5 else None
        if not name_raw:
            continue
        person_name, given, family, nationality, role = split_hfsp_name(name_raw)
        institution, city, country = parse_affiliation(p.get_text("\n", strip=True) if p else "")
        people.append({
            "person_name": person_name,
            "given_name": given,
            "family_name": family,
            "nationality": nationality,
            "role": role,
            "institution": institution,
            "city": city,
            "country": country,
        })

    abstract_node = card.select_one(".panel-body")
    abstract = clean_text(abstract_node.get_text(" ", strip=True)) if abstract_node else None
    awardees = [person for person in people if person.get("role") != "host_supervisor"]
    host_supervisors = [person for person in people if person.get("role") == "host_supervisor"]
    if not program and host_supervisors:
        program = "Unlabeled HFSP fellowship row"
    page_url = AWARDS_URL if page_number == 0 else f"{AWARDS_URL}?page={page_number}"
    source_hash = hashlib.sha1(
        f"{year}|{program}|{title}|{page_number}|{card_index}".encode("utf-8")
    ).hexdigest()[:12]
    native = f"hfsp-{year or 'unknown'}-{slugify(program or 'program', 40)}-{slugify(title or 'award', 70)}-{source_hash}"

    lead = awardees[0] if awardees else {}
    co = awardees[1] if len(awardees) > 1 else {}
    countries = sorted({x.get("country") for x in awardees if x.get("country")})
    institutions = sorted({x.get("institution") for x in awardees if x.get("institution")})

    return {
        "funder_award_id": native,
        "display_name": title,
        "description": abstract,
        "source_year": year,
        "start_date": f"{year}-01-01" if year else None,
        "end_date": f"{year}-12-31" if year else None,
        "program": program,
        "funding_type": program_to_funding_type(program),
        "amount": None,
        "currency": None,
        "lead_person_name": lead.get("person_name"),
        "lead_given_name": lead.get("given_name"),
        "lead_family_name": lead.get("family_name"),
        "lead_nationality": lead.get("nationality"),
        "lead_institution": lead.get("institution"),
        "lead_city": lead.get("city"),
        "lead_country": lead.get("country"),
        "co_person_name": co.get("person_name"),
        "co_given_name": co.get("given_name"),
        "co_family_name": co.get("family_name"),
        "co_nationality": co.get("nationality"),
        "co_institution": co.get("institution"),
        "co_city": co.get("city"),
        "co_country": co.get("country"),
        "awardee_count": str(len(awardees)),
        "person_count": str(len(people)),
        "host_supervisor_count": str(len(host_supervisors)),
        "host_supervisors_raw": "; ".join(x["person_name"] for x in host_supervisors if x.get("person_name")) or None,
        "investigator_count": str(len(awardees)),
        "investigators_json": json.dumps(awardees, ensure_ascii=False, sort_keys=True),
        "people_json": json.dumps(people, ensure_ascii=False, sort_keys=True),
        "institution_names": "; ".join(institutions) if institutions else None,
        "award_countries": "; ".join(countries) if countries else None,
        "landing_page_url": page_url,
        "source_page_number": str(page_number),
        "source_card_index": str(card_index),
        "source_hash": source_hash,
    }


def fetch_award_rows(limit: Optional[int] = None) -> list[dict[str, Any]]:
    print("\n" + "=" * 60)
    print("Step 1: Download HFSP award pages")
    print("=" * 60)
    max_page = discover_max_page()
    rows: list[dict[str, Any]] = []

    for page in range(max_page + 1):
        url = AWARDS_URL if page == 0 else f"{AWARDS_URL}?page={page}"
        resp = polite_get(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("div.call-to-action.awards")
        if not cards:
            raise RuntimeError(f"Unexpected empty page before advertised end: {url}")
        for idx, card in enumerate(cards):
            rows.append(parse_award_card(card, page, idx))
            if limit and len(rows) >= limit:
                print(f"  [LIMIT] keeping first {len(rows):,} rows")
                return rows
        if page % 25 == 0 or page == max_page:
            print(f"  Page {page:,}/{max_page:,}: {len(cards):,} cards (running {len(rows):,})")

    empty_check = polite_get(f"{AWARDS_URL}?page={max_page + 1}")
    empty_cards = BeautifulSoup(empty_check.text, "html.parser").select("div.call-to-action.awards")
    if empty_cards:
        raise RuntimeError(
            f"Page {max_page + 1} still has {len(empty_cards)} cards; pagination discovery was incomplete."
        )
    print(f"  [OK] page {max_page + 1} is empty, so crawl reached the end.")
    return rows


def normalize_rows(rows: list[dict[str, Any]], *, full_run: bool) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Normalize rows")
    print("=" * 60)
    retrieved_at = datetime.now(timezone.utc).isoformat()
    for row in rows:
        row["retrieved_at"] = retrieved_at
    df = pd.DataFrame(rows)
    validate_dataframe(df, full_run=full_run)
    df = df.astype("string")
    return df


def validate_dataframe(df: pd.DataFrame, *, full_run: bool) -> None:
    total = len(df)
    if total == 0:
        raise RuntimeError("No HFSP award rows after normalization")
    duplicate_ids = total - df["funder_award_id"].nunique()
    if duplicate_ids:
        dupes = df[df["funder_award_id"].duplicated(keep=False)]["funder_award_id"].head(10).tolist()
        raise RuntimeError(f"Duplicate funder_award_id values found: {dupes}")
    if full_run and total < 4900:
        raise RuntimeError(f"Full HFSP run returned only {total:,} rows; expected at least 4,900.")

    required_cols = ["display_name", "source_year", "program", "lead_person_name", "lead_institution", "landing_page_url"]
    for col in required_cols:
        coverage = df[col].notna().mean()
        print(f"  {col:20s}: {df[col].notna().sum():,}/{total:,} ({coverage * 100:.1f}%)")
        if coverage < 0.95:
            raise RuntimeError(f"Unexpectedly low coverage for {col}: {coverage * 100:.1f}%")
    print(f"  {'description':20s}: {df['description'].notna().sum():,}/{total:,} ({df['description'].notna().mean() * 100:.1f}%)")
    print(f"  {'co_person_name':20s}: {df['co_person_name'].notna().sum():,}/{total:,} ({df['co_person_name'].notna().mean() * 100:.1f}%)")
    print(f"  {'amount':20s}: {df['amount'].notna().sum():,}/{total:,} ({df['amount'].notna().mean() * 100:.1f}%)")
    print(f"  Year range: {df['source_year'].min()} - {df['source_year'].max()}")
    print(f"  Rows by funding_type: {df['funding_type'].value_counts(dropna=False).to_dict()}")
    print(f"  Top programs: {df['program'].value_counts(dropna=False).head(12).to_dict()}")


def write_outputs(rows: list[dict[str, Any]], df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write raw JSON and parquet")
    print("=" * 60)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "hfsp_awards_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"  [OK] wrote raw rows to {raw_path}")

    parquet_path = output_dir / "hfsp_awards.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    sz_kb = parquet_path.stat().st_size / 1024
    print(f"  [OK] wrote {len(df):,} rows ({sz_kb:.1f} KB) to {parquet_path}")
    return parquet_path


def load_cached_rows(output_dir: Path) -> list[dict[str, Any]]:
    raw_path = output_dir / "hfsp_awards_raw.json"
    if not raw_path.exists():
        raise FileNotFoundError(f"--skip-download requested but cache is missing: {raw_path}")
    with raw_path.open("r", encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list):
        raise RuntimeError(f"Cached JSON should be a list of records: {raw_path}")
    print(f"  [OK] loaded {len(rows):,} cached rows from {raw_path}")
    return rows


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook section 1.4: refuse to overwrite S3 with a smaller corpus."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for the runbook section 1.4 shrink-check; "
            "rerun with --skip-upload for local validation."
        ) from exc

    client = boto3.client("s3")
    print(f"  Re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            print("    no existing parquet: first ingest, no shrink check needed.")
            return True
        print(f"    [WARN] head_object failed ({code}); treating as first ingest.")
        return True

    prev_path = output_dir / "_prev_hfsp_awards.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as exc:
        print(f"    [ERROR] could not read existing parquet ({exc}); aborting upload.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)

    print(f"    previous count: {prev_count:,}   new count: {new_count:,}")
    if new_count < prev_count:
        if allow_shrink:
            print("    [OVERRIDE] new corpus is smaller but --allow-shrink was set.")
            return True
        print(f"\n[ERROR] Refusing to shrink HFSP corpus ({prev_count:,} -> {new_count:,}).")
        return False
    print("    [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path,
                 allow_shrink: bool) -> bool:
    print("\n" + "=" * 60)
    print("Step 4: Upload to S3")
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
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] aws s3 cp failed (exit {exc.returncode}).")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download HFSP awards and write OpenAlex staging parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/hfsp"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit rows for smoke testing")
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse hfsp_awards_raw.json from output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Do not upload parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override runbook section 1.4 shrink-check")
    args = parser.parse_args()

    print("=" * 60)
    print("HFSP awards -> S3 pipeline")
    print("=" * 60)
    print(f"  Funder:     {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Source:     {AWARDS_URL}")
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        print("\nStep 1: Reuse cached raw JSON")
        rows = load_cached_rows(args.output_dir)
        if args.limit:
            rows = rows[:args.limit]
            print(f"  [LIMIT] keeping first {len(rows):,} cached rows")
    else:
        rows = fetch_award_rows(limit=args.limit)

    df = normalize_rows(rows, full_run=args.limit is None)
    parquet_path = write_outputs(rows, df, args.output_dir)

    if args.skip_upload:
        print("\n[SKIP] --skip-upload set; not uploading to S3.")
        print(f"Local parquet ready: {parquet_path}")
        return

    ok = upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
