#!/usr/bin/env python3
"""
Stockholm Water Prize → S3 Data Pipeline (PRIZE PATTERN, big-page-inline scrape)
================================================================================

Downloads the Stockholm Water Prize laureate roster from the Stockholm
Water Foundation's official site at
`stockholmwaterfoundation.org/stockholm-water-prize/laureates`. The
Stockholm Water Prize is awarded annually since 1991 in collaboration
with the Royal Swedish Academy of Sciences, with H.M. King Carl XVI
Gustaf of Sweden as official patron and presenter. Laureates include
David Schindler (1991), Jorg Imberger (1996), Werner Stumm + James
Morgan (joint, 1999), Kader Asmal (2000), Sandra Postel (2021),
Taikan Oki (2024), and Günter Blöschl (2025).

Source authority
----------------
`stockholmwaterfoundation.org` is the foundation's own site. (Note:
the predecessor URL `siwi.org/prizes/stockholmwaterprize/` redirects
here — the prize moved from the Stockholm International Water
Institute to the dedicated Stockholm Water Foundation site as of
2024/2025. The OpenAlex funder F4320320937 is still attributed to
SIWI; we use it because that's the current registry entry and the
Foundation is operationally the same body.)

The `/laureates` page renders **all 36 laureate cards inline** in a
single ~1.3MB server-side-rendered HTML response — no FacetWP, no
admin-ajax, no pagination. Method #5 on the runbook ladder (static-
HTML scrape), specifically the "big-page-inline" variant where the
entire corpus fits in one request. This is the first ingest on the
project that tests that pattern at scale.

Each laureate card has the shape:

  <div class="blurb_div">
    <a href=".../laureates/{year}-{name-slug}-{country-slug}/">
      <figure><img alt="{Name}"></figure>
      <div class="blurb_div-content">
        <span class="pre-title">{YEAR}</span>
        <h3>{Name}</h3>
        <p>{citation/description}</p>
      </div>
    </a>
  </div>

The 2026 cohort is announced but the detail page may not exist yet,
so we tolerate a missing landing-page URL on the latest card while
still capturing name + year + citation from the listing.

Amount handling — §6.7 NULL waiver
-----------------------------------
The Stockholm Water Foundation's own site does NOT publish the
Stockholm Water Prize monetary value on the public landing page, the
laureates index, or individual laureate detail pages (verified by
text-search 2026-05-21). This matches the Wolf Prize (#47) and
Fields Medal (#50) precedent — we apply the runbook §6.7 amount
waiver and ship `amount = NULL, currency = NULL`. Documented in the
notebook header so reviewers don't flag it as missing data.

(Widely-reported figures place the prize at USD 150,000 historically,
but those figures come from third-party sources, not the awarding
body's own materials. Per runbook source-authority discipline, we
don't ship aggregator-sourced amounts when the awarding body itself
declines to publish them.)

Output
------
s3://openalex-ingest/awards/stockholm_water_prize/stockholm_water_prize_laureates.parquet

Usage
-----
    python stockholm_water_prize_to_s3.py                # full run (<10 sec)
    python stockholm_water_prize_to_s3.py --skip-upload  # local dev
    python stockholm_water_prize_to_s3.py --skip-download
    python stockholm_water_prize_to_s3.py --allow-shrink

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

LAUREATES_URL = "https://stockholmwaterfoundation.org/stockholm-water-prize/laureates"

# Awarding body — Stockholm International Water Institute (SIWI) in OpenAlex.
# The prize is currently administered by the Stockholm Water Foundation
# (an organizationally affiliated successor); OpenAlex still indexes the
# SIWI funder, so we use that funder_id.
FUNDER_ID = 4320320937
FUNDER_DISPLAY_NAME = "Stockholm International Water Institute"

PROVENANCE = "stockholm_water_prize"
# §6.7 NULL waiver — foundation does not publish per-laureate amount.
CURRENCY = None

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/stockholm_water_prize/stockholm_water_prize_laureates.parquet"

USER_AGENT = "openalex-walden-stockholm-water-prize-ingest/1.0 (+https://openalex.org)"

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
            "Accept": "text/html, application/xhtml+xml, */*; q=0.01",
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
    print("Smoke test: laureate listing reachable + parseable")
    print("=" * 60)
    resp = _http_get(LAUREATES_URL)
    resp.raise_for_status()
    cards = parse_listing(resp.text)
    print(f"  parsed {len(cards)} laureate cards")
    if len(cards) < 30:
        print(f"[ERROR] expected ~36 cards, got {len(cards)} — markup may have changed")
        sys.exit(3)
    # Spot-check the earliest
    schindler = [c for c in cards if c["year"] == 1991]
    if not schindler:
        print("[WARN] 1991 Schindler not in parse output — verify selector")
    else:
        c = schindler[0]
        print(f"  spot-check (1991 Schindler): name={c['name']!r}  country={c.get('country')!r}")


# =============================================================================
# Card parsing
# =============================================================================

# Each laureate card consists of:
#   <a href=".../laureates/{year-slug-country}/" ...>
#     <figure>...picture/img with long srcset...</figure>  (often >5KB)
#     <div class="blurb_div-content">
#       <span class="pre-title">YYYY</span>
#       <h3>Name</h3>
#       <p>Citation text</p>
#     </div>
#   </a>
#
# Anchor on `<span class="pre-title">YYYY</span>` + immediately-following
# <h3> + <p>, then look BACKWARD from the span start to find the nearest
# preceding `<a href="...laureates/...">` anchor (URL discovery is a
# separate pass — the inline figure+srcset markup between the anchor
# and the span exceeds any reasonable single-regex `.{0,N}?` bound).
_CARD_RE = re.compile(
    r'<span class="pre-title">(?P<year>\d{4})</span>\s*'
    r'<h3[^>]*>(?P<name>[^<]+)</h3>\s*'
    r'<p[^>]*>(?P<citation>[^<]+)</p>',
    re.DOTALL,
)
_PRECEDING_URL_RE = re.compile(
    r'<a[^>]*href="(https?://stockholmwaterfoundation\.org/'
    r'stockholm-water-prize/laureates/[^"#]+)"[^>]*>'
    r'(?![\s\S]*<a[^>]*href="https?://stockholmwaterfoundation\.org/'
    r'stockholm-water-prize/laureates/)',
    re.DOTALL,
)


def _strip_html(s: str) -> str:
    text = re.sub(r'<[^>]+>', ' ', s)
    return unescape(re.sub(r'\s+', ' ', text)).strip()


def _find_preceding_url(html: str, span_start: int) -> Optional[str]:
    """Find the nearest `<a href="...laureates/...">` opening tag that
    precedes `span_start`. Returns URL with trailing slash, or None."""
    # Scan only the slice ending at span_start; pick the LAST anchor in it.
    slice_ = html[:span_start]
    # Iterate matches; keep the latest one
    last_url = None
    for m in re.finditer(
        r'<a[^>]*href="(https?://stockholmwaterfoundation\.org/'
        r'stockholm-water-prize/laureates/[^"#]+?)/?"[^>]*>',
        slice_,
    ):
        last_url = m.group(1)
    if not last_url:
        return None
    return last_url.rstrip("/") + "/"


def parse_listing(html: str) -> list[dict]:
    """Parse the laureate listing page into a list of laureate dicts.
    Each card yields (year, name, citation, url, country_from_slug)."""
    out: list[dict] = []
    seen_year_slug: set[tuple[int, str]] = set()
    for m in _CARD_RE.finditer(html):
        year = int(m.group("year"))
        name = _strip_html(m.group("name"))
        citation = _strip_html(m.group("citation"))
        # Most laureate names carry the country as a comma-suffix:
        #   "Professor Kaveh Madani, Iran"
        #   "Werner Stumm, Switzerland and James J. Morgan, USA"
        # Extract from the FIRST recipient's tail (after the first comma,
        # before " and " if present).
        country_from_name = None
        # Try the first recipient's tail first; fall back to the entire name's
        # tail if the first recipient has no comma (joint awards like
        # "Dr. Rita Colwell and Johns Hopkins, USA" put the country after the
        # co-recipient).
        first_recipient = re.split(r'\s+and\s+', name, maxsplit=1, flags=re.I)[0]
        tail_source = first_recipient if "," in first_recipient else name
        if "," in tail_source:
            tail = tail_source.rsplit(",", 1)[1].strip()
            if tail and re.fullmatch(r'[A-Za-z][A-Za-z \-]{1,40}', tail):
                country_from_name = tail
            # Strip the country suffix from name so split_name() doesn't treat
            # it as the family name. Keep the original full string in
            # raw_name for the display.
        name_stripped = first_recipient.rsplit(",", 1)[0].strip() if "," in first_recipient and country_from_name else first_recipient
        # If joint, append the co-laureate after " and "
        joint_tail = re.split(r'\s+and\s+', name, maxsplit=1, flags=re.I)
        if len(joint_tail) > 1:
            name_stripped = f"{name_stripped} and {joint_tail[1].strip()}"
        # Look backward for the wrapping anchor's URL
        url = _find_preceding_url(html, m.start())
        # Extract slug + country tail from URL (e.g.
        # /1991-professor-david-w-schindler-canada/ → slug=david-w-schindler,
        # country=canada). Joint-winner URLs (1999 Stumm+Morgan) have the
        # form -switzerland-and-james-j-morgan-usa — pick the LAST country.
        slug = None
        country = None
        if url:
            tail_m = re.search(r'/laureates/(\d{4})-(.+?)/?$', url)
            if tail_m:
                tail = tail_m.group(2)
                # heuristic: last hyphen-separated country code
                # Try: <name>-<country>; for joint awards: <name>-<country>-and-<name2>-<country2>
                # Pull off "-and-..." segments first
                base = tail.split("-and-")[-1] if "-and-" in tail else tail
                # Then split base on '-' and pop tokens that look like country names
                # Known country/region suffixes (lowercase). Conservative list — only
                # match well-known tokens we've seen in this prize's slugs.
                COUNTRY_TOKENS = {
                    "usa", "uk", "canada", "australia", "switzerland", "japan",
                    "israel", "germany", "sweden", "denmark", "netherlands",
                    "france", "italy", "spain", "norway", "finland", "india",
                    "china", "korea", "brazil", "south", "africa", "great",
                    "britain", "new", "zealand", "egypt", "kenya", "iran",
                    "iraq", "lebanon", "bangladesh", "pakistan", "thailand",
                    "vietnam", "indonesia", "philippines", "mexico", "chile",
                    "argentina", "peru", "colombia", "venezuela", "uruguay",
                    "paraguay", "bolivia", "ecuador",
                }
                # Walk tokens from the right collecting consecutive country tokens
                toks = base.split("-")
                country_toks: list[str] = []
                while toks and toks[-1].lower() in COUNTRY_TOKENS:
                    country_toks.insert(0, toks.pop())
                if country_toks:
                    # Preserve common-acronym casing: "USA" not "Usa", "UK" not "Uk"
                    country = " ".join(
                        t.upper() if t.lower() in {"usa", "uk"} else t.capitalize()
                        for t in country_toks
                    )
                slug = "-".join(toks) if toks else None
        # Prefer the country extracted from the NAME field (more reliable —
        # always present on modern entries) over the URL slug (older only).
        if country_from_name:
            country = country_from_name
        # Dedupe on (year, name)
        key = (year, name.lower())
        if key in seen_year_slug:
            continue
        seen_year_slug.add(key)
        out.append({
            "year":             year,
            "name":             name,            # original (with ", Country" suffix)
            "name_stripped":    name_stripped,   # country-suffix removed for splitter
            "citation":         citation,
            "landing_page_url": url,
            "slug_from_url":    slug,
            "country":          country,
        })
    return out


# =============================================================================
# Name splitter (runbook §2.4.1)
# =============================================================================

_DEGREE_SUFFIXES = {"PhD", "Ph.D.", "Ph.D", "MD", "M.D.", "DPhil", "ScD",
                    "Jr.", "Jr", "Sr.", "Sr", "II", "III", "IV"}
_HONORIFIC_PREFIXES_RE = re.compile(
    r'^(?:Dr\.?|Prof\.?|Professor|Mr\.?|Ms\.?|Mrs\.?|Sir|Dame)\s+',
    re.I,
)
# Joint-laureate splitter — "Werner Stumm and James J. Morgan" → split on ' and '.
_JOINT_RE = re.compile(r'\s+and\s+', re.I)


def split_name(full_name: str) -> tuple[Optional[str], Optional[str], bool]:
    """Return (given_name, family_name, is_joint). For joint laureates,
    use the FIRST recipient's name (consistent with runbook §2.4.1
    single-PI convention; co-laureate captured in description)."""
    if not full_name:
        return None, None, False
    is_joint = bool(_JOINT_RE.search(full_name))
    if is_joint:
        # take the first recipient only
        name = _JOINT_RE.split(full_name)[0].strip()
    else:
        name = full_name.strip()
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


# =============================================================================
# Build DataFrame
# =============================================================================

_ORG_TOKENS_RE = re.compile(
    r'\b(Department|Institute|Foundation|University|Laboratory|Center|Centre|'
    r'Agency|Society|Association|Council|Commission|Ministry|WaterAid|'
    r'Water Aid)\b',
    re.I,
)


def build_dataframe(all_rows: list[dict]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print(f"Step 2: Build flat DataFrame from {len(all_rows)} raw cards")
    print("=" * 60)

    seen_award_ids: set[str] = set()
    final: list[dict] = []
    for r in all_rows:
        name = r["name"]
        # Detect organizational recipients (Department, Institute, WaterAid, etc.)
        is_org = bool(_ORG_TOKENS_RE.search(name)) and not name.lower().startswith(("dr", "prof", "sir"))
        if is_org:
            given, family, is_joint = None, None, False
            recipient_kind = "organization"
        else:
            # Use the country-stripped name for splitting so the country
            # suffix doesn't get captured as the family_name.
            given, family, is_joint = split_name(r.get("name_stripped") or name)
            recipient_kind = "individual"
        # funder_award_id uses slug-from-URL when available (more stable than name),
        # else fall back to a name-based slug.
        seed = r.get("slug_from_url") or re.sub(
            r'[^a-z0-9]+', '-', name.lower()).strip('-')[:60]
        funder_award_id = f"stockholm-water-prize-{r['year']}-{seed}"
        if funder_award_id in seen_award_ids:
            raise RuntimeError(
                f"funder_award_id collision: {funder_award_id} — "
                "Stockholm Water Prize (year, slug) should be unique per runbook prize pattern"
            )
        seen_award_ids.add(funder_award_id)

        display_name = f"Stockholm Water Prize {r['year']} — {name}"
        description = r.get("citation")

        final.append({
            "funder_award_id":   funder_award_id,
            "year":              r["year"],
            "name":              name,
            "given_name":        given,
            "family_name":       family,
            "country":           r.get("country"),
            "recipient_kind":    recipient_kind,
            "is_joint":          is_joint,
            "citation":          r.get("citation"),
            "display_name":      display_name,
            "description":       description,
            "amount":            None,  # §6.7 waiver — see header
            "currency":          CURRENCY,
            "start_date":        f"{r['year']}-01-01",
            "end_date":          f"{r['year']}-12-31",
            "landing_page_url":  r.get("landing_page_url"),
            "slug_from_url":     r.get("slug_from_url"),
            "declined":          False,
        })

    df = pd.DataFrame.from_records(final)
    print(f"  rows: {len(df)}")
    print(f"  year range: {df['year'].min()}-{df['year'].max()}")
    print(f"  recipient kinds: {df['recipient_kind'].value_counts().to_dict()}")
    print(f"  joint laureates: {df['is_joint'].sum()}")
    print(f"  country coverage: {df['country'].notna().sum()}/{len(df)} "
          f"({df['country'].notna().sum()*100/len(df):.0f}%)")
    print(f"  landing-page URL coverage: {df['landing_page_url'].notna().sum()}/{len(df)} "
          f"(missing on the most recent cohort if detail page not yet published)")
    print(f"  citation coverage: {df['citation'].notna().sum()}/{len(df)}")

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
    parquet_path = output_dir / "stockholm_water_prize_laureates.parquet"
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
    prev_path = output_dir / "_prev_stockholm_water_prize.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/stockholm"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse stockholm_water_prize_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Stockholm Water Prize → S3 Pipeline (PRIZE PATTERN, big-page-inline)")
    print("=" * 60)
    print(f"  Awarding body: {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Output dir:    {args.output_dir.absolute()}")
    print(f"  S3 dest:       s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:       {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "stockholm_water_prize_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        all_rows = json.loads(raw_path.read_text())
        print(f"\n[SKIP] reusing {raw_path} with {len(all_rows)} cached rows")
    else:
        print("\n" + "=" * 60)
        print("Step 1: Download laureate listing page (single big page)")
        print("=" * 60)
        resp = _http_get(LAUREATES_URL)
        resp.raise_for_status()
        all_rows = parse_listing(resp.text)
        raw_path = args.output_dir / "stockholm_water_prize_raw.json"
        raw_path.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2))
        print(f"  Cached {len(all_rows)} raw rows to {raw_path}")

    df = build_dataframe(all_rows)
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
    print(f"Next: notebooks/awards/CreateStockholmWaterPrizeAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
