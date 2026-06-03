#!/usr/bin/env python3
"""
Sweden-America Foundation to S3 Data Pipeline
==============================================

Downloads fellowship-recipient profiles from sweamfo.se's WordPress site.
Each fellow has a public "travel report" page under
`/reserapporter-fran-stipendiater/{slug}/`, enumerated via the public
WP sitemap at `/wp-sitemap-posts-page-1.xml`.

Source authority
----------------
sweamfo.se is the foundation's own site. The fellow pages render
server-side; no auth required. As of 2026-05-27 the sitemap exposes
46 fellow profile URLs (corpus is small — the Foundation makes
~30 awards/yr but only published modern recipients have travel
reports on the site; historical recipients like Bertil Ohlin
(1922-23 fellow, 1977 Nobel laureate) are documented separately).

Each fellow page header uses the pattern:
    <h1>Stipendiat YYYY-YYYY: Given Family</h1>
This is the cleanest extraction anchor — name + fellowship year-range
are right there. Body text gives host institution ("vid Harvard
University" or English equivalents) and free-form description in
Swedish.

Schema choices vs the project's standard funder schema
------------------------------------------------------
One row per fellow. Sweden-America Foundation sends Swedish students
to U.S. host institutions, so `lead_investigator.affiliation.country`
is hardcoded `US` (host country, per program design) and the country
of the awarding body is SE.

`amount` / `currency` ship NULL with §6.7 waiver. The Foundation does
publish a general range on its eligibility page but per-fellow amounts
are not disclosed on profile pages. Same waiver as HHMI #44 /
Damon Runyon #73 / CIFAR #79.

Provenance: `sweden_america_foundation` (verified count=0 on production).

Output
------
s3://openalex-ingest/awards/sweden_america_fdn/sweden_america_fdn_fellows.parquet

Usage
-----
    python sweden_america_fdn_to_s3.py                    # full run (~30 sec)
    python sweden_america_fdn_to_s3.py --skip-upload      # local dev
    python sweden_america_fdn_to_s3.py --skip-download    # reuse cached JSON
    python sweden_america_fdn_to_s3.py --limit 10         # smoke test
    python sweden_america_fdn_to_s3.py --allow-shrink     # override §1.4

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

# =============================================================================
# Configuration
# =============================================================================

SITE_BASE = "https://sweamfo.se"
SITEMAP_URL = f"{SITE_BASE}/wp-sitemap-posts-page-1.xml"

# Awarding body — Sweden-America Foundation (Sverige-Amerika Stiftelsen).
# Verified F4320320938, country SE.
FUNDER_ID = 4320320938
FUNDER_DISPLAY_NAME = "Sweden-America Foundation"

PROVENANCE = "sweden_america_foundation"

# S3 destination.
S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/sweden_america_fdn/sweden_america_fdn_fellows.parquet"

USER_AGENT = "openalex-walden-sweden-america-ingest/1.0 (+https://openalex.org)"

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


def _strip_tags(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s)
    return unescape(re.sub(r"\s+", " ", s)).strip()


# =============================================================================
# Sitemap enumeration
# =============================================================================

_LOC_RE = re.compile(r"<loc>([^<]+)</loc>")
# Fellow profile URLs: /reserapporter-fran-stipendiater/{slug}/  (slug present)
_FELLOW_URL_RE = re.compile(
    rf"^{re.escape(SITE_BASE)}/reserapporter-fran-stipendiater/[a-z0-9-]+/?$"
)


def enumerate_fellow_urls() -> list[str]:
    r = _http_get(SITEMAP_URL)
    r.raise_for_status()
    out: list[str] = []
    seen: set[str] = set()
    for u in _LOC_RE.findall(r.text):
        u = u.strip()
        if _FELLOW_URL_RE.match(u) and u not in seen:
            seen.add(u)
            out.append(u)
    return out


# =============================================================================
# Per-page parser
# =============================================================================

# Header: "Stipendiat 2023-2024: Linda Pipkorn"
_HEADER_RE = re.compile(
    r"<h1[^>]*>\s*Stipendiat\s+(?P<y1>\d{4})\s*[-–]\s*(?P<y2>\d{4})\s*:?\s*(?P<name>[^<]+?)\s*</h1>",
    re.IGNORECASE,
)
# Fallback: same form in the <title> tag.
_TITLE_RE = re.compile(
    r"<title[^>]*>\s*Stipendiat\s+(?P<y1>\d{4})\s*[-–]\s*(?P<y2>\d{4})\s*:?\s*(?P<name>[^|<&]+?)\s*(?:&#8211;|–|-|\|)",
    re.IGNORECASE,
)
# Article body bounds.
_ARTICLE_RE = re.compile(r"<article[^>]*>(.+?)</article>", re.DOTALL)
_MAIN_RE = re.compile(r"<main[^>]*>(.+?)</main>", re.DOTALL)

# "vid <Institution>" — Swedish for "at <Institution>". Greedy across the
# institution-name suffix so that "vid The University of Pennsylvania" captures
# the full name, not just "The University". The trailing alternation reaches
# through optional "of <Place>" and "at <Place>" tails.
_INST_TAIL = (
    r"University(?:\s+of\s+[A-ZÅÄÖÉ][\w &,\-']*(?:\s+[A-ZÅÄÖÉ][\w &,\-']*){0,3})?"
    r"|Institute(?:\s+of\s+[A-ZÅÄÖÉ][\w &,\-']*(?:\s+[A-ZÅÄÖÉ][\w &,\-']*){0,3})?"
    r"|Institutet|Tech|College(?:\s+of\s+[A-ZÅÄÖÉ][\w &,\-']*)?"
    r"|School(?:\s+of\s+[A-ZÅÄÖÉ][\w &,\-']*)?"
    r"|Lab(?:oratory)?|Foundation|Academy|Society|Hospital|Center|Centre"
)
_VID_INST_RE = re.compile(
    rf"\b[Vv]id\s+((?:[Tt]he\s+)?[A-ZÅÄÖÉ][A-Za-zÅÄÖåäöÉéèêëæœ&,. \-']{{1,80}}?"
    rf"(?:{_INST_TAIL}))",
)
# English fallback "at <Institution>"
_AT_INST_RE = re.compile(
    rf"\bat\s+((?:the\s+)?[A-Z][A-Za-z, \-']{{1,80}}?(?:{_INST_TAIL}))",
)


_SWEDISH_NOISE_PREFIX = re.compile(
    r"^(?:both?\s+|båda?\s+|universitetet\s+|universitet\s+|the\s+)+",
    re.IGNORECASE,
)


def _normalize_institution(s: str) -> Optional[str]:
    s = _SWEDISH_NOISE_PREFIX.sub("", s).strip()
    s = s.strip(",;.").strip()
    # Final guard: institutions should start with a capital letter.
    if not s or not s[0].isupper():
        return None
    return s


def parse_fellow_page(url: str, html: str) -> Optional[dict]:
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    # Header
    m = _HEADER_RE.search(html) or _TITLE_RE.search(html)
    if not m:
        return None
    y1 = int(m.group("y1"))
    y2 = int(m.group("y2"))
    name = unescape(m.group("name").strip())

    # Article body
    body_m = _ARTICLE_RE.search(html) or _MAIN_RE.search(html)
    body_text = ""
    if body_m:
        body_text = _strip_tags(body_m.group(1))

    # Host institution
    institution: Optional[str] = None
    im = _VID_INST_RE.search(body_text)
    if im:
        institution = _normalize_institution(im.group(1))
    if not institution:
        im2 = _AT_INST_RE.search(body_text)
        if im2:
            institution = _normalize_institution(im2.group(1))

    # Description: first sentence-ish chunk after the h1 (~300-500 chars)
    description: Optional[str] = None
    if body_text:
        # Drop the leading "Stipendiat YYYY-YYYY: Name" prefix if present
        cleaned = re.sub(
            r"^Stipendiat\s+\d{4}\s*[-–]\s*\d{4}\s*:?\s*[^\.]{0,80}\.?\s*",
            "",
            body_text,
        )
        # Take a reasonable chunk; cap at 500 chars
        description = cleaned[:500].strip() or None

    return {
        "url": url,
        "slug": slug,
        "name": name,
        "start_year": y1,
        "end_year": y2,
        "institution": institution,
        "description": description,
    }


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: sitemap + sample 3 fellow pages")
    print("=" * 60)
    urls = enumerate_fellow_urls()
    print(f"  sitemap fellow URLs: {len(urls)}")
    if len(urls) < 10:
        print(f"[ERROR] only {len(urls)} URLs — selector changed?")
        sys.exit(3)
    for u in urls[:3]:
        r = _http_get(u)
        r.raise_for_status()
        rec = parse_fellow_page(u, r.text)
        if rec is None:
            print(f"  [WARN] failed parse: {u}")
            continue
        print(f"  {rec['start_year']}-{rec['end_year']}  {rec['name']:30s}  "
              f"@ {rec['institution']!r}")


# =============================================================================
# Download
# =============================================================================

def download_fellows(output_dir: Path, limit: Optional[int]) -> Path:
    print("\n" + "=" * 60)
    print("Step 1: Walk sitemap + scrape fellow profile pages")
    print("=" * 60)
    urls = enumerate_fellow_urls()
    if limit:
        urls = urls[:limit]
        print(f"  [LIMIT] only scraping first {limit}")
    print(f"  fetching {len(urls)} fellow profile pages")
    out: list[dict] = []
    for i, url in enumerate(urls, 1):
        try:
            r = _http_get(url)
            if r.status_code != 200:
                print(f"  [{i}/{len(urls)}] {url} -> HTTP {r.status_code}; skipping")
                continue
            rec = parse_fellow_page(url, r.text)
            if rec is None:
                print(f"  [{i}/{len(urls)}] {url} -> couldn't parse h1; skipping")
                continue
            out.append(rec)
        except Exception as e:
            print(f"  [{i}/{len(urls)}] {url} -> error {e}")
            continue
        if i % 10 == 0 or i == len(urls):
            n_inst = sum(1 for r in out if r.get("institution"))
            print(f"  [{i}/{len(urls)}] parsed: {len(out)} fellows, institution coverage: {n_inst}")
    raw_path = output_dir / "sweden_america_fdn_raw.json"
    raw_path.write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"\n  cached {len(out)} records to {raw_path}")
    return raw_path


# =============================================================================
# Build DataFrame
# =============================================================================

_DEGREE_SUFFIXES = {"PhD","Ph.D.","MD","M.D.","DPhil","Jr.","Jr","Sr.","Sr","II","III","IV","MPH","MSc"}
_PREFIX_TITLES = {"Dr","Dr.","Prof","Prof.","Professor"}


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


def build_dataframe(raw_path: Path) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print("Step 2: Build flat DataFrame")
    print("=" * 60)
    records = json.loads(raw_path.read_text())
    seen_ids: set[str] = set()
    rows: list[dict] = []
    for r in records:
        slug = r.get("slug")
        if not slug:
            continue
        funder_award_id = f"sweden-america-{slug}"
        if funder_award_id in seen_ids:
            continue
        seen_ids.add(funder_award_id)
        name = r.get("name") or ""
        given, family = split_name(name)
        rows.append({
            "funder_award_id":   funder_award_id,
            "slug":              slug,
            "name":              name,
            "given_name":        given or None,
            "family_name":       family or None,
            "start_year":        r.get("start_year"),
            "end_year":          r.get("end_year"),
            "institution":       r.get("institution"),
            "description":       r.get("description"),
            "landing_page_url":  r["url"],
        })
    df = pd.DataFrame.from_records(rows)
    n_nm = df["name"].astype(bool).sum()
    n_inst = df["institution"].astype(bool).sum()
    n_desc = df["description"].astype(bool).sum()
    n_year = df["start_year"].notna().sum()
    print(f"  rows: {len(df)}")
    print(f"  coverage: name={n_nm} ({n_nm*100/len(df):.0f}%) "
          f"institution={n_inst} ({n_inst*100/len(df):.0f}%) "
          f"description={n_desc} ({n_desc*100/len(df):.0f}%) "
          f"start_year={n_year} ({n_year*100/len(df):.0f}%)")
    if n_year:
        yrs = df["start_year"].dropna().astype(int)
        print(f"  year range: {yrs.min()} - {yrs.max()}")
    print(f"\n  Top 10 host institutions:")
    print(df["institution"].value_counts().head(10).to_string())
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "sweden_america_fdn_fellows.parquet"
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
    prev_path = output_dir / "_prev_sweden_america_fdn_fellows.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/sweamfo"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse sweden_america_fdn_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only scrape first N fellow pages (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Sweden-America Foundation → S3 Pipeline")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "sweden_america_fdn_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        print(f"\n[SKIP] reusing {raw_path}")
    else:
        raw_path = download_fellows(args.output_dir, args.limit)

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
    print(f"Next: notebooks/awards/CreateSwedenAmericaFdnAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
