#!/usr/bin/env python3
"""
Brain & Behavior Research Foundation (NARSAD) to S3 Data Pipeline
==================================================================

Downloads the full BBRF grantee directory from bbrfoundation.org's
public Drupal-rendered grantee search at /grantees. Pages paginate
via `?page=N` (0-indexed, 0..~279 = ~280 pages at 20 grantees/page).

Each grantee node renders the following Drupal field-name fields:
  - field-name-title             → scientist name (in <h2>)
  - field-name-field-institution → host institution
  - field-name-field-country     → country (taxonomy term)
  - field-name-field-grantee-illness → illness focus (taxonomy)
  - field-name-field-grant (field-collection) → one or more grant blocks:
      - field-name-field-grant-type → "Young Investigator" / "Independent Investigator" / "Distinguished Investigator" / etc.
      - field-name-field-grant-year → award year

Multi-grant grantees (a scientist who held YI then IIG then DI) ship as
multiple rows — one per (name, grant_type, grant_year) tuple. The
funder_award_id is constructed from the same triple so per-award
identity is stable across re-ingests.

Source authority
----------------
bbrfoundation.org is the awarding body's own site (Drupal 10, public,
no auth, public sitemap). No third-party aggregator.

Amount and currency
-------------------
BBRF publishes program-level amounts on individual program pages
(/grants-prizes/{program}). Examples:
  - Young Investigator: USD 35,000 per year (2-year, $70K total)
  - Independent Investigator: USD 100,000 per year (2-year, $200K total)
  - Distinguished Investigator: USD 100,000 (1-year)
But the GRANTEE LISTING does not surface per-grantee amounts — only
grant_type and grant_year. We ship `amount` / `currency` as NULL
(§6.7-waived, same as HHMI #44 / Damon Runyon #73 / CIFAR #79). A
future ingest could populate amounts via SQL CASE on `grant_type` if
the boss decides the program-level uniform amounts are an acceptable
proxy.

Output
------
s3://openalex-ingest/awards/bbrf/bbrf_grantees.parquet

Usage
-----
    python bbrf_to_s3.py                    # full run (~280 page fetches)
    python bbrf_to_s3.py --skip-upload      # local dev
    python bbrf_to_s3.py --skip-download    # reuse cached JSON
    python bbrf_to_s3.py --limit 3          # smoke test (first N pages)
    python bbrf_to_s3.py --allow-shrink     # override §1.4

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

GRANTEES_URL = "https://bbrfoundation.org/grantees"

# Awarding body — Brain & Behavior Research Foundation (NARSAD).
# Verified F4320306147, country US, DOI 10.13039/100000874.
FUNDER_ID = 4320306147
FUNDER_DISPLAY_NAME = "Brain and Behavior Research Foundation"

PROVENANCE = "bbrf_narsad"

# S3 destination.
S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/bbrf/bbrf_grantees.parquet"

USER_AGENT = "openalex-walden-bbrf-ingest/1.0 (+https://openalex.org)"

# Polite throttle — ~280 page fetches at 0.5s/page = ~140s.
MIN_REQUEST_INTERVAL_S = 0.5

# Cap page count to a generous upper bound; loop exits on empty page anyway.
MAX_PAGES = 500


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


def _strip_tags(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s)
    return unescape(re.sub(r"\s+", " ", s)).strip()


# =============================================================================
# Drupal field extraction
# =============================================================================

_VIEWS_ROW_RE = re.compile(
    r'<div class="views-row [^"]*">(.*?)(?=<div class="views-row |<div class="pager|</div>\s*</div>\s*</div>\s*</div>\s*</section>)',
    re.DOTALL,
)

# Inside a views-row, a Drupal field looks like:
#   <div class="field field-name-FIELD_NAME [...]">
#     <div class="field-items"><div class="field-item even">VALUE</div></div>
#   </div>
def _drupal_field_value(block: str, field_name: str) -> Optional[str]:
    pat = re.compile(
        rf'<div class="field field-name-{re.escape(field_name)}[^"]*">'
        rf'\s*<div class="field-items">'
        rf'\s*<div class="field-item even">(.+?)</div>',
        re.DOTALL,
    )
    m = pat.search(block)
    return _strip_tags(m.group(1)) if m else None


def _parse_grants(block: str) -> list[dict]:
    """Extract list of {grant_type, grant_year} from the field-collection-views."""
    out: list[dict] = []
    # Pair grant-type and grant-year by ORDER inside the views-row block —
    # Drupal field-collections render the two fields in deterministic order
    # per grant, so the first grant-type pairs with the first grant-year,
    # the second with the second, etc.
    type_vals = [
        _strip_tags(m.group(1))
        for m in re.finditer(
            r'<div class="field field-name-field-grant-type[^"]*">'
            r'\s*<div class="field-items">'
            r'\s*<div class="field-item even">(.+?)</div>',
            block,
            re.DOTALL,
        )
    ]
    year_vals = [
        _strip_tags(m.group(1))
        for m in re.finditer(
            r'<div class="field field-name-field-grant-year[^"]*">'
            r'\s*<div class="field-items">'
            r'\s*<div class="field-item even">(.+?)</div>',
            block,
            re.DOTALL,
        )
    ]
    # Pair by index; if counts mismatch (shouldn't normally), use None for missing side.
    n = max(len(type_vals), len(year_vals))
    for i in range(n):
        gt = type_vals[i] if i < len(type_vals) else None
        gy = year_vals[i] if i < len(year_vals) else None
        if gt or gy:
            out.append({"grant_type": gt, "grant_year": gy})
    return out


def parse_grantee_block(block: str) -> Optional[dict]:
    """Extract one grantee's structured fields from a views-row block."""
    name = _drupal_field_value(block, "title")
    if not name:
        return None
    return {
        "name":        name,
        "institution": _drupal_field_value(block, "field-institution"),
        "country":     _drupal_field_value(block, "field-country"),
        "illness":     _drupal_field_value(block, "field-grantee-illness"),
        "grants":      _parse_grants(block),
    }


def parse_page(html: str) -> list[dict]:
    """Return list of grantee dicts from one /grantees?page=N response."""
    out: list[dict] = []
    for m in _VIEWS_ROW_RE.finditer(html):
        rec = parse_grantee_block(m.group(1))
        if rec:
            out.append(rec)
    return out


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: /grantees page 0 + page 1 + last page")
    print("=" * 60)
    r = _http_get(f"{GRANTEES_URL}?page=0")
    r.raise_for_status()
    grantees = parse_page(r.text)
    print(f"  page 0: {len(grantees)} grantees")
    if not grantees:
        print("[ERROR] page 0 returned 0 grantees — selector changed?")
        sys.exit(3)
    # Show a sample
    s = grantees[0]
    n_grants = len(s.get("grants") or [])
    print(f"  sample: {s['name']} | {s['institution']} | {s['country']} | grants={n_grants}")
    # Find last page from pager markup
    last_m = re.search(r"\?page=(\d+)[^\"]*\"[^>]*class=\"pager-last", r.text)
    if last_m:
        print(f"  pager-last hint: page={last_m.group(1)} (=> {int(last_m.group(1))+1} total pages)")


# =============================================================================
# Download — iterate page=0..N until empty
# =============================================================================

def download_grantees(output_dir: Path, limit: Optional[int]) -> Path:
    print("\n" + "=" * 60)
    print("Step 1: Walk /grantees?page=N")
    print("=" * 60)
    all_grantees: list[dict] = []
    page_n = 0
    consecutive_empty = 0
    consecutive_non200 = 0
    # Empty pages and transient non-200s are NOT terminators (v2 §1: a single
    # blip silently truncated RWJF). MAX_PAGES is the authoritative ceiling;
    # we raise rather than short-circuit on persistent failure.
    MAX_CONSECUTIVE_EMPTY = 3
    MAX_CONSECUTIVE_NON200 = 5
    while page_n < MAX_PAGES:
        if limit is not None and page_n >= limit:
            print(f"  [LIMIT] stopping after {limit} pages")
            break
        try:
            resp = _http_get(f"{GRANTEES_URL}?page={page_n}")
            if resp.status_code != 200:
                consecutive_non200 += 1
                print(f"  page {page_n}: HTTP {resp.status_code} "
                      f"(consecutive_non200={consecutive_non200}/{MAX_CONSECUTIVE_NON200}); continuing")
                if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
                    raise RuntimeError(
                        f"download_grantees: {consecutive_non200} consecutive non-200s "
                        f"ending at page {page_n}; refusing to short-circuit corpus walk."
                    )
                page_n += 1
                continue
            consecutive_non200 = 0
            page_grantees = parse_page(resp.text)
        except RuntimeError:
            raise
        except Exception as e:
            print(f"  page {page_n}: error {e}; treating as non-200 flake; continuing")
            consecutive_non200 += 1
            if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError(
                    f"download_grantees: {consecutive_non200} consecutive errors "
                    f"ending at page {page_n}."
                ) from e
            page_n += 1
            continue
        if not page_grantees:
            consecutive_empty += 1
            print(f"  page {page_n}: 0 grantees "
                  f"(consecutive_empty={consecutive_empty}/{MAX_CONSECUTIVE_EMPTY})")
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                print(f"  stopping after {MAX_CONSECUTIVE_EMPTY} consecutive empty pages (likely end of corpus)")
                break
        else:
            consecutive_empty = 0
            all_grantees.extend(page_grantees)
            if page_n % 25 == 0 or page_n < 3:
                n_grants = sum(len(g.get("grants") or []) for g in all_grantees)
                print(f"  page {page_n}: +{len(page_grantees)} grantees (total grantees={len(all_grantees)}, total grants={n_grants})")
        page_n += 1
    raw_path = output_dir / "bbrf_raw.json"
    raw_path.write_text(json.dumps(all_grantees, ensure_ascii=False, indent=2))
    print(f"\n  cached {len(all_grantees)} grantees to {raw_path}")
    return raw_path


# =============================================================================
# Build DataFrame
# =============================================================================

# Name normalization: BBRF formats most names as "First M. Last, Ph.D."
# Strip postnominals before split.
_DEGREE_SUFFIXES_RE = re.compile(
    r",\s*(?:Ph\.?\s*D\.?|M\.?\s*D\.?|D\.?\s*Phil\.?|M\.?\s*D\.?\s*-?\s*Ph\.?\s*D\.?|"
    r"Sc\.?\s*D\.?|Dr\.?\s*P\.?\s*H\.?|D\.?\s*V\.?\s*M\.?|D\.?\s*O\.?|R\.?\s*N\.?|"
    r"M\.?\s*P\.?\s*H\.?|M\.?\s*Sc\.?|J\.?\s*D\.?|Jr\.?|Sr\.?|II|III|IV)"
    r"(?:[,\s.]|$)",
    re.IGNORECASE,
)
_PREFIX_TITLES = {"Dr", "Dr.", "Prof", "Prof.", "Professor"}


def _clean_name(raw: str) -> str:
    """Strip degree postnominals like ", Ph.D." from BBRF-format names."""
    cleaned = raw
    # Iteratively strip suffixes (in case there are two: "Foo, M.D., Ph.D.")
    while True:
        new = _DEGREE_SUFFIXES_RE.sub("", cleaned)
        if new == cleaned:
            break
        cleaned = new
    return cleaned.strip(" ,.")


def split_name(full: str) -> tuple[str, str]:
    tokens = full.split()
    while tokens and tokens[0].rstrip(".") in {t.rstrip(".") for t in _PREFIX_TITLES}:
        tokens.pop(0)
    if not tokens:
        return ("", "")
    if len(tokens) == 1:
        return ("", tokens[0])
    return (" ".join(tokens[:-1]), tokens[-1])


# ISO-3166 alpha-2 mapping for the few country values BBRF uses.
_COUNTRY_TO_ISO = {
    "united states":  "US",
    "united kingdom": "GB",
    "canada":         "CA",
    "australia":      "AU",
    "germany":        "DE",
    "france":         "FR",
    "italy":          "IT",
    "spain":          "ES",
    "netherlands":    "NL",
    "belgium":        "BE",
    "switzerland":    "CH",
    "austria":        "AT",
    "sweden":         "SE",
    "norway":         "NO",
    "denmark":        "DK",
    "finland":        "FI",
    "ireland":        "IE",
    "japan":          "JP",
    "china":          "CN",
    "south korea":    "KR",
    "korea":          "KR",
    "israel":         "IL",
    "india":          "IN",
    "brazil":         "BR",
    "mexico":         "MX",
    "argentina":      "AR",
    "chile":          "CL",
    "new zealand":    "NZ",
    "singapore":      "SG",
    "hong kong":      "HK",
    "taiwan":         "TW",
    "south africa":   "ZA",
    "portugal":       "PT",
    "poland":         "PL",
    "czech republic": "CZ",
    "hungary":        "HU",
    "greece":         "GR",
    "turkey":         "TR",
    "russia":         "RU",
}


def _country_iso(country_name: Optional[str]) -> Optional[str]:
    if not country_name:
        return None
    norm = country_name.strip().lower()
    if not norm:
        return None
    # Exact match first.
    if norm in _COUNTRY_TO_ISO:
        return _COUNTRY_TO_ISO[norm]
    # BBRF writes the UK constituent countries inline (e.g. "United Kingdom - England",
    # "United Kingdom-Scotland"). Map all of those to GB.
    if "united kingdom" in norm:
        return "GB"
    return None


def _slugify(s: str) -> str:
    s = unescape(s).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def build_dataframe(raw_path: Path) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Build flat DataFrame (one row per grant)")
    print("=" * 60)
    grantees = json.loads(raw_path.read_text())
    seen_ids: set[str] = set()
    rows: list[dict] = []
    for g in grantees:
        raw_name = g.get("name") or ""
        cleaned_name = _clean_name(raw_name)
        if not cleaned_name:
            continue
        given, family = split_name(cleaned_name)
        slug = _slugify(cleaned_name)
        country_iso = _country_iso(g.get("country"))
        grants = g.get("grants") or [{"grant_type": None, "grant_year": None}]
        for gr in grants:
            grant_type = gr.get("grant_type")
            grant_year = gr.get("grant_year")
            funder_award_id = f"bbrf-{slug}-{_slugify(grant_type or 'unknown')}-{grant_year or 'unknown'}"
            if funder_award_id in seen_ids:
                continue
            seen_ids.add(funder_award_id)
            rows.append({
                "funder_award_id":   funder_award_id,
                "slug":              slug,
                "name":              cleaned_name,
                "name_raw":          raw_name,
                "given_name":        given or None,
                "family_name":       family or None,
                "institution":       g.get("institution"),
                "country_raw":       g.get("country"),
                "country":           country_iso,
                "illness":           g.get("illness"),
                "grant_type":        grant_type,
                "grant_year":        grant_year,
                "landing_page_url":  GRANTEES_URL,
            })
    df = pd.DataFrame.from_records(rows)
    n_inst = df["institution"].astype(bool).sum()
    n_country = df["country"].astype(bool).sum()
    n_gt = df["grant_type"].astype(bool).sum()
    n_gy = df["grant_year"].astype(bool).sum()
    print(f"  grants (rows): {len(df)}  unique grantees: {df['slug'].nunique()}")
    print(f"  coverage: institution={n_inst} ({n_inst*100/len(df):.0f}%) "
          f"country_iso={n_country} ({n_country*100/len(df):.0f}%) "
          f"grant_type={n_gt} ({n_gt*100/len(df):.0f}%) "
          f"grant_year={n_gy} ({n_gy*100/len(df):.0f}%)")
    if "grant_year" in df.columns and df["grant_year"].notna().any():
        years_num = pd.to_numeric(df["grant_year"], errors="coerce").dropna()
        if not years_num.empty:
            print(f"  year range: {int(years_num.min())} – {int(years_num.max())}")
    print(f"\n  By grant_type (top 10):")
    print(df.groupby("grant_type").size().sort_values(ascending=False).head(10).to_string())
    print(f"\n  By country_iso (top 8):")
    print(df.groupby("country").size().sort_values(ascending=False).head(8).to_string())
    # Runbook §1.2.5 — astype("string") immediately before parquet write.
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "bbrf_grantees.parquet"
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
    prev_path = output_dir / "_prev_bbrf_grantees.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/bbrf"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse bbrf_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only scrape first N pages (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Brain & Behavior Research Foundation (NARSAD) → S3 Pipeline")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "bbrf_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        print(f"\n[SKIP] reusing {raw_path}")
    else:
        raw_path = download_grantees(args.output_dir, args.limit)

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
    print(f"Next: notebooks/awards/CreateBBRFAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
