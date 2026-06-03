#!/usr/bin/env python3
"""
Klingenstein-Simons Fellowship in Neuroscience to S3 Data Pipeline
==================================================================

Downloads the full historical roster of Klingenstein Fellowship Awards in
Neuroscience (rebranded to Klingenstein-Simons Fellowship after the Simons
Foundation joined as co-funder) from the foundation's own site at
klingenstein.org. The administering body is the Esther A. & Joseph
Klingenstein Fund.

Source authority
----------------
klingenstein.org is the awarding body's own site (Cloudflare-protected as
of mid-2026 — confirmed bypassed by Playwright + stealth on 2026-05-27).
Two layers of pages:

1. Current cohorts (2023, 2024, 2025) inline on
   `/esther-a-joseph-klingenstein-fund/neuroscience/fellowship-programs/`.
2. Per-year archives 1981-2022 at
   `/grantees/grantee/eajk-neuroscience-fellows/{YEAR}`.

Years with no fellows announced (1982, 1984, 1986) are silently absent
from the archive index. The current-cohort page also lists the available
archive years as text-content `<a>` links, which the scraper uses as the
authoritative year list rather than hardcoding.

Method 6 — first contractor ingest on the project to use Playwright +
playwright-stealth to bypass Cloudflare. Helper module:
`scripts/local/_playwright_helper.py`.

Schema choices
--------------
- One row per (fellow, year) tuple. `funder_award_id` = `eajk-{year}-{name-slug}`.
- Funder = Esther A. and Joseph Klingenstein Fund (F4320306403, US, DOI 10.13039/100001391).
- `funder_scheme` reflects program name as it appeared in the announcement year:
    pre-collaboration  -> "Klingenstein Fellowship Awards in Neuroscience"
    post-collaboration -> "Klingenstein-Simons Fellowship Awards in Neuroscience"
  Transition year is hardcoded at 2010 per the page text difference (older
  pages say "The Klingenstein Fellowship..." with no Simons mention;
  newer pages say "Through its collaboration with the Simons Foundation,
  the Klingenstein-Simons Fellowship...").
- `funding_type` = `fellowship` for every row.
- `amount` / `currency` ship NULL with §6.7 amount-coverage **WAIVED** —
  the foundation's program-level amount has changed over 45 years and is
  not published per cohort on the archive pages. Same waiver as HHMI
  (#44) / Damon Runyon (#73) / CIFAR (#79).
- `lead_investigator.affiliation.country` hardcoded to 'US' — the program
  funds US institutions only per the eligibility text on the program page.
- Research project titles (the line below each fellow's name on
  post-~2005 pages) carry through as `description`. Older cohorts often
  list only Name + Institution, so description is sparse.

Provenance: `klingenstein_simons` (verified count=0 on production).

Output
------
s3://openalex-ingest/awards/klingenstein_simons/klingenstein_simons_fellows.parquet

Usage
-----
    python klingenstein_simons_to_s3.py                    # full run
    python klingenstein_simons_to_s3.py --skip-upload      # local dev
    python klingenstein_simons_to_s3.py --skip-download    # reuse cached JSON
    python klingenstein_simons_to_s3.py --limit 5          # smoke test
    python klingenstein_simons_to_s3.py --allow-shrink     # override §1.4

Requirements
------------
    pip install pandas pyarrow requests boto3 playwright playwright-stealth
    playwright install chromium
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

# Shared Playwright helper (Method 6). Same dir as this script.
sys.path.insert(0, str(Path(__file__).parent))
from _playwright_helper import PlaywrightSession  # noqa: E402

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

SITE_BASE = "https://klingenstein.org"
CURRENT_FELLOWS_URL = f"{SITE_BASE}/esther-a-joseph-klingenstein-fund/neuroscience/fellowship-programs/"
ARCHIVE_URL_TEMPLATE = f"{SITE_BASE}/grantees/grantee/eajk-neuroscience-fellows/{{year}}"

# Funder — Esther A. and Joseph Klingenstein Fund.
# Verified F4320306403, country US, DOI 10.13039/100001391.
FUNDER_ID = 4320306403
FUNDER_DISPLAY_NAME = "Esther A. and Joseph Klingenstein Fund"

PROVENANCE = "klingenstein_simons"

# Scheme name transition year — Simons Foundation joined the program
# around 2010-2011. Before that the program was just "Klingenstein
# Fellowship Awards in Neuroscience". The exact transition year is
# documented in the program page's narrative; we adopt 2010 as the
# rollover point to align with the rebranded name first appearing on
# announcement pages.
SIMONS_COLLAB_YEAR = 2010

SCHEME_PRE_SIMONS = "Klingenstein Fellowship Awards in Neuroscience"
SCHEME_POST_SIMONS = "Klingenstein-Simons Fellowship Awards in Neuroscience"

# S3 destination.
S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/klingenstein_simons/klingenstein_simons_fellows.parquet"

# Polite throttle for Cloudflare-protected origin.
MIN_REQUEST_INTERVAL_S = 0.6


# =============================================================================
# Per-page fellow parser
# =============================================================================

# A typical fellow block looks like (rendered text):
#
#   Alice X. Lastname, Ph.D.
#   Title Of Project Goes Here
#   INSTITUTION
#   Some University
#   LAB WEBSITE
#
# Or, for older cohorts (no project title):
#
#   Alice X. Lastname, Ph.D.
#   INSTITUTION
#   Some University
#
# Or, for deceased fellows (life dates appear under the name):
#
#   Paul R. Adams, Ph.D.
#   1937-2011
#   INSTITUTION
#   Stony Brook University
#
# We parse the {YEAR} FELLOWS section's inner text into block-records.

_YEAR_FELLOWS_HEADER_RE = re.compile(r"(?P<year>(?:19|20)\d{2})\s+FELLOWS\s*\n")
_LIFE_DATES_RE = re.compile(r"^\d{4}\s*[\-–—]\s*\d{4}$")
_TRAILING_DEGREE_RE = re.compile(
    r"^(.+?),\s*(Ph\.?D\.?|M\.?D\.?|Sc\.?D\.?|D\.?O\.?|Dr\.?\s*med\.?|DPhil)(?:\s*[/,]\s*(?:Ph\.?D\.?|M\.?D\.?|Sc\.?D\.?))?\.?$"
)


def parse_fellows_from_text(text: str, year: Optional[int] = None) -> list[dict]:
    """Walk the year-page rendered text and yield one dict per fellow.

    Returns dicts with keys: year, name, given_name, family_name,
    research_title, institution.
    """
    # Slice to the {YEAR} FELLOWS .. PAST FELLOWS ARCHIVES region.
    start_idx = None
    if year is not None:
        m = re.search(rf"{year}\s+FELLOWS\s*\n", text)
        if m:
            start_idx = m.end()
    if start_idx is None:
        # Fallback: first "{YEAR} FELLOWS" header in the page
        m = _YEAR_FELLOWS_HEADER_RE.search(text)
        if not m:
            return []
        year = int(m.group("year"))
        start_idx = m.end()
    # End at PAST FELLOWS ARCHIVES (or footer "Klingenstein Philanthropies").
    # Also stop at the NEXT "{YEAR} FELLOWS" header — when the current page
    # has multiple inline cohorts (e.g. 2025, 2024, 2023 all rendered), each
    # year-region ends at the next year-header above it. Without this we'd
    # double-count fellows across years.
    end_idx = len(text)
    for terminator in ("PAST FELLOWS ARCHIVES", "Our Fellows:", "Klingenstein Philanthropies\n80"):
        ti = text.find(terminator, start_idx)
        if ti > 0 and ti < end_idx:
            end_idx = ti
    next_year_header = _YEAR_FELLOWS_HEADER_RE.search(text, start_idx)
    if next_year_header and next_year_header.start() < end_idx:
        end_idx = next_year_header.start()
    region = text[start_idx:end_idx]

    # Split into lines, drop blanks.
    lines = [ln.strip() for ln in region.split("\n") if ln.strip()]

    # State-machine parse. A fellow block starts on a Name line and ends
    # at the next Name line (or end of region). We identify Name lines
    # heuristically: contains "Ph.D." / "M.D." / "Sc.D." / "D.O." / etc.,
    # OR (fallback) a Capitalized 2-6-token line that's not a known marker.
    KNOWN_MARKERS = {
        "INSTITUTION", "LAB WEBSITE", "OVERVIEW & LEADERSHIP", "NEUROSCIENCE", "Fellows",
        "Applying", "Scientific Advisory Committee", "EARLY CHILDHOOD", "Grantees",
        "Advisory Committee", "EARLY LITERACY", "ENVIRONMENT", "KLINGENSTEIN CENTER",
    }

    def is_degree_line(s: str) -> bool:
        # Conservative: must contain Ph.D./M.D./Sc.D./D.O. tokens
        return bool(re.search(r"\bPh\.?\s*D\.?|\bM\.?\s*D\.?|\bSc\.?\s*D\.?|\bD\.?\s*O\.?|\bDPhil\b|\bDr\.?\s*med\.?", s))

    def looks_like_name_no_degree(s: str) -> bool:
        # Fallback: capitalized 2-6 tokens, no all-caps marker words
        if s in KNOWN_MARKERS:
            return False
        if s.upper() == s:  # ALL CAPS lines are headings, not names
            return False
        tokens = s.split()
        if not (2 <= len(tokens) <= 6):
            return False
        # Each token should be capitalized or be an initial/connector
        for t in tokens:
            t_stripped = t.strip(".,")
            if not t_stripped:
                return False
            if not (t_stripped[0].isupper() or t_stripped in {"de", "van", "von", "del", "la"}):
                return False
        return True

    fellows: list[dict] = []
    cur: Optional[dict] = None
    expecting_institution = False
    for ln in lines:
        if ln in KNOWN_MARKERS:
            if ln == "INSTITUTION":
                expecting_institution = True
            elif ln == "LAB WEBSITE":
                expecting_institution = False
            continue
        if expecting_institution and cur is not None and not cur.get("institution"):
            cur["institution"] = ln
            expecting_institution = False
            continue
        if cur is not None and _LIFE_DATES_RE.match(ln):
            cur["life_dates"] = ln
            continue
        if is_degree_line(ln) or (cur is not None and cur.get("institution") and looks_like_name_no_degree(ln)):
            # Start a new fellow block (if cur exists, finalize it)
            if cur is not None and cur.get("name"):
                fellows.append(cur)
            # Strip trailing degree(s) from the name
            m = _TRAILING_DEGREE_RE.match(ln)
            name = m.group(1).strip() if m else ln
            # Some entries also have a trailing ", M.D., Ph.D." form not caught by the strict regex
            name = re.sub(r"[,]\s*(?:Ph\.?D\.?|M\.?D\.?|Sc\.?D\.?|D\.?O\.?|DPhil)\.?", "", name).strip().rstrip(",.")
            cur = {"year": year, "name": name, "research_title": None, "institution": None}
            expecting_institution = False
            continue
        # Within-block lines: if we've started a fellow, the next non-marker
        # non-degree line is the research_title.
        if cur is not None and cur.get("research_title") is None and not expecting_institution:
            cur["research_title"] = ln
    # Flush trailing fellow
    if cur is not None and cur.get("name"):
        fellows.append(cur)
    # Drop entries that fell through the heuristic without an institution
    fellows = [f for f in fellows if f.get("name")]
    return fellows


# =============================================================================
# Year enumeration
# =============================================================================

def collect_year_urls(session: PlaywrightSession) -> list[tuple[int, str]]:
    """Walk the current-fellows page to find the year-archive link list.

    Returns sorted [(year, url), ...] pairs for every year that has a
    distinct page. Years currently embedded on the main page (typically
    the latest 3 cohorts) are returned with `url` pointing at the main
    page itself so the caller knows to slice them out of one fetch.
    """
    session.fetch(CURRENT_FELLOWS_URL)
    # Year-text <a> nodes
    rows = session.page.evaluate(
        """() => {
          const anchors = Array.from(document.querySelectorAll('a'));
          return anchors
            .filter(a => /^(19|20)\\d{2}$/.test(a.textContent.trim()))
            .map(a => ({year: parseInt(a.textContent.trim(), 10), href: a.href}));
        }"""
    )
    seen: set[int] = set()
    out: list[tuple[int, str]] = []
    for r in rows:
        y = int(r["year"])
        if y in seen:
            continue
        seen.add(y)
        out.append((y, r["href"]))
    # Also find {YEAR} FELLOWS headers already embedded on the current page —
    # these are the "current cohorts" rendered inline.
    current_page_text = session.page.locator("main, article, body").first.inner_text(timeout=5000)
    inline_years = sorted(set(int(m.group(1)) for m in re.finditer(r"((?:19|20)\d{2})\s+FELLOWS", current_page_text)))
    for y in inline_years:
        if y not in seen:
            seen.add(y)
            out.append((y, CURRENT_FELLOWS_URL))
    out.sort()
    return out


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: load current-fellows page + parse 2024 cohort")
    print("=" * 60)
    with PlaywrightSession(min_interval_s=MIN_REQUEST_INTERVAL_S) as session:
        years = collect_year_urls(session)
        print(f"  enumerated {len(years)} year-pages "
              f"(range {min(y for y, _ in years)}..{max(y for y, _ in years)})")
        # Parse 2024 from the current-cohorts page
        cur_text = session.page.locator("main, article, body").first.inner_text(timeout=5000)
        sample = parse_fellows_from_text(cur_text, year=2024)
        print(f"  2024 cohort parsed: {len(sample)} fellows")
        if sample:
            s = sample[0]
            print(f"  first: {s['name']} / {s.get('institution')} / "
                  f"{(s.get('research_title') or '')[:60]}")


# =============================================================================
# Download
# =============================================================================

def download_fellows(output_dir: Path, limit: Optional[int]) -> Path:
    print("\n" + "=" * 60)
    print("Step 1: Walk year-archive pages")
    print("=" * 60)
    all_rows: list[dict] = []
    with PlaywrightSession(min_interval_s=MIN_REQUEST_INTERVAL_S) as session:
        year_urls = collect_year_urls(session)
        if limit is not None:
            year_urls = year_urls[-limit:]
            print(f"  [LIMIT] scraping last {limit} years: {[y for y, _ in year_urls]}")

        # Cache the current-fellows page text once; multiple inline years share it.
        cur_page_text: Optional[str] = None

        for i, (year, url) in enumerate(year_urls, 1):
            try:
                if url == CURRENT_FELLOWS_URL:
                    if cur_page_text is None:
                        session.fetch(CURRENT_FELLOWS_URL)
                        cur_page_text = session.page.locator("main, article, body").first.inner_text(timeout=8000)
                    page_text = cur_page_text
                else:
                    session.fetch(url)
                    page_text = session.page.locator("main, article, body").first.inner_text(timeout=8000)
                fellows = parse_fellows_from_text(page_text, year=year)
            except Exception as e:
                print(f"  [{i}/{len(year_urls)}] {year} ({url}) -> ERROR {e!s}")
                continue
            for f in fellows:
                all_rows.append({**f, "landing_page_url": url})
            print(f"  [{i}/{len(year_urls)}] year {year}: +{len(fellows)} fellows (total {len(all_rows)})")
    raw_path = output_dir / "klingenstein_simons_raw.json"
    raw_path.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2))
    print(f"\n  cached {len(all_rows)} fellows to {raw_path}")
    return raw_path


# =============================================================================
# Build DataFrame
# =============================================================================

_DEGREE_SUFFIXES = {
    "PhD", "Ph.D.", "Ph.D", "MD", "M.D.", "M.D", "Sc.D.", "Sc.D", "D.O.", "D.O",
    "DPhil", "Jr.", "Jr", "Sr.", "Sr", "II", "III", "IV", "MPH", "MSc",
}
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
    records = json.loads(raw_path.read_text())
    seen_ids: set[str] = set()
    rows: list[dict] = []
    for r in records:
        name = (r.get("name") or "").strip()
        year = r.get("year")
        if not name or not year:
            continue
        slug = _slugify(name)
        funder_award_id = f"eajk-{year}-{slug}"
        if funder_award_id in seen_ids:
            print(f"  [WARN] duplicate funder_award_id {funder_award_id!r}; skipping later record")
            continue
        seen_ids.add(funder_award_id)
        given, family = split_name(name)
        scheme = SCHEME_POST_SIMONS if year >= SIMONS_COLLAB_YEAR else SCHEME_PRE_SIMONS
        rows.append({
            "funder_award_id":  funder_award_id,
            "slug":              slug,
            "year":              int(year),
            "name":              name,
            "given_name":        given or None,
            "family_name":       family or None,
            "institution":       r.get("institution"),
            "research_title":    r.get("research_title"),
            "scheme_label":      scheme,
            "life_dates":        r.get("life_dates"),
            "landing_page_url":  r.get("landing_page_url"),
        })

    df = pd.DataFrame.from_records(rows)
    if df.empty:
        raise RuntimeError("no fellows parsed — investigate scraper")
    n_nm = df["name"].astype(bool).sum()
    n_inst = df["institution"].astype(bool).sum()
    n_title = df["research_title"].astype(bool).sum()
    print(f"  rows: {len(df)}")
    print(f"  coverage: name={n_nm} ({n_nm*100/len(df):.0f}%) "
          f"institution={n_inst} ({n_inst*100/len(df):.0f}%) "
          f"research_title={n_title} ({n_title*100/len(df):.0f}%)")
    print(f"  year range: {df['year'].min()} – {df['year'].max()}  "
          f"({df['year'].nunique()} distinct years)")
    print(f"\n  By scheme:")
    print(df.groupby("scheme_label").size().to_string())
    print(f"\n  Top 10 institutions:")
    print(df["institution"].value_counts().head(10).to_string())
    # Runbook §1.2.5 — astype("string") before parquet write
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "klingenstein_simons_fellows.parquet"
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
    prev_path = output_dir / "_prev_klingenstein_simons_fellows.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/klingenstein_simons"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse klingenstein_simons_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only fetch last N year-archive pages (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Klingenstein-Simons Fellowship in Neuroscience → S3 Pipeline")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "klingenstein_simons_raw.json"
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
    print("Next: notebooks/awards/CreateKlingensteinSimonsAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
