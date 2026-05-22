#!/usr/bin/env python3
"""
Templeton Prize → S3 Data Pipeline (PRIZE PATTERN, static-HTML scrape)
=======================================================================

Downloads the Templeton Prize laureate roster from the official site at
`templetonprize.org/templeton-prize-winners-2/`. The Templeton Prize is
awarded annually since 1973 by the **John Templeton Foundation** to
"a living person who has made an exceptional contribution to affirming
life's spiritual dimension." Laureates have included Mother Teresa, the
Dalai Lama, Desmond Tutu, Francis Collins, Marcelo Gleiser, and (as of
2026) Simon Conway Morris.

Source authority
----------------
`templetonprize.org` is the awarding body's own site (WordPress, no auth).
The `/templeton-prize-winners-2/` page renders all 56 laureate cards
inline (no pagination, no FacetWP / admin-ajax — server-side rendered
HTML). Method #5 on the runbook ladder (static-HTML scrape).

Each laureate card has the shape:

  <div class="mix laureate-item decade-1980-1999" data-title="aleksandr solzhenitsyn">
    <div class="laureate-img ..." data-bg-image="url('.../1983-Solzhenitsyn.jpg)">
      <a href="https://www.templetonprize.org/laureate/aleksandr-solzhenitsyn/" ...>
    </div>
    <div class="laureate-content">
      <h2>Aleksandr Solzhenitsyn (1983)</h2>
      <p>Novelist and Historian</p>
    </div>
  </div>

We extract: slug (from URL), name + year (from `<h2>`), short occupation
(from `<p>`), decade class, and the image-filename year prefix (which
matches the parenthesized year — a useful internal consistency check).

Why this is a same-funder, second-priority ingest
-------------------------------------------------
The John Templeton Foundation (F4320306193, US, DOI 10.13039/100000925)
is already in `CreateAwards.ipynb` at **priority 39** for ~5,956
Templeton GRANTS (its main funding-instrument output). This ingest adds
a SECOND priority slot for the same funder, dedicated specifically to
the Templeton PRIZE — a flagship $1.4M-equivalent honor that is
operationally separate from the grant program. The two coexist cleanly
because:

  - The Step 3 DELETE clause uses `(provenance, priority)` as the key,
    so `provenance='templeton_prize'` won't touch any rows owned by
    the existing Templeton grant ingest.
  - Per-row `id = abs(xxhash64(funder_id ':' funder_award_id)) % 9e9`,
    so `templeton-prize-1983-aleksandr-solzhenitsyn` hashes to a
    distinct ID from any grant award_id under the same funder.
  - The dedup ordering in `openalex_awards` is `ROW_NUMBER() OVER
    (PARTITION BY id ORDER BY priority ASC)` — distinct IDs mean no
    cross-priority dedup happens; both rows coexist as separate awards
    per laureate / per grantee.

This precedent should be documented in the runbook: a single
`funder_id` can legitimately appear at multiple priorities when the
funder runs operationally separate award programs (a flagship prize +
a research-grant program, in this case).

Amount rule
-----------
The foundation's `templeton-prize-history/quick-facts/` page states
verbatim: **"The Templeton Prize is a monetary award in the amount of
£1,100,000 sterling."** We ship GBP 1,100,000 uniformly across all
years and document the assumption. The amount has varied historically
(it was set in 2001 to exceed the Nobel Prize and is reviewed
periodically), but the foundation publishes only the current rule.
§6.7 amount-coverage NOT waived — 100% expected.

Output
------
s3://openalex-ingest/awards/templeton_prize/templeton_prize_laureates.parquet

Usage
-----
    python templeton_prize_to_s3.py                # full run (~56 laureates, <10 sec)
    python templeton_prize_to_s3.py --skip-upload  # local dev
    python templeton_prize_to_s3.py --skip-download
    python templeton_prize_to_s3.py --allow-shrink

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

WINNERS_URL = "https://www.templetonprize.org/templeton-prize-winners-2/"
QUICK_FACTS_URL = "https://www.templetonprize.org/templeton-prize-history/quick-facts/"

# Awarding body — John Templeton Foundation.
# Already at priority 39 for Templeton grants; this ingest adds a SECOND
# priority specifically for the Templeton Prize. See "Why this is a
# same-funder, second-priority ingest" in the module docstring above.
FUNDER_ID = 4320306193
FUNDER_DISPLAY_NAME = "John Templeton Foundation"

PROVENANCE = "templeton_prize"
CURRENCY = "GBP"  # per Quick Facts page
TEMPLETON_PRIZE_AMOUNT_GBP = 1_100_000.0

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/templeton_prize/templeton_prize_laureates.parquet"

USER_AGENT = "openalex-walden-templeton-prize-ingest/1.0 (+https://openalex.org)"

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
    print("Smoke test: winners listing reachable + parseable")
    print("=" * 60)
    resp = _http_get(WINNERS_URL)
    resp.raise_for_status()
    cards = parse_listing(resp.text)
    print(f"  parsed {len(cards)} laureate cards")
    if len(cards) < 40:
        print(f"[ERROR] expected ~56 cards, got {len(cards)} — markup may have changed")
        sys.exit(3)
    # Spot-check a known laureate
    found = [c for c in cards if c["slug"] == "aleksandr-solzhenitsyn"]
    if not found:
        print("[WARN] Solzhenitsyn (1983) not in parse output — verify card-class selector")
    else:
        c = found[0]
        print(f"  spot-check (1983 Solzhenitsyn): name={c['name']!r} year={c['year']} "
              f"occupation={c['occupation']!r}")


# =============================================================================
# Card parsing
# =============================================================================

# Match each laureate-item card and capture inner HTML.
_CARD_BLOCK_RE = re.compile(
    r'<div class="mix laureate-item([^"]*)"\s+data-title="([^"]*)"\s*>(.+?)'
    r'(?=<div class="mix laureate-item|<!--\s*end laureate-list\s*-->|</section)',
    re.DOTALL,
)
# Within a card:
#   - URL  : <a href=".../laureate/{slug}/" ...>
#   - name + year : <h2>Name (YYYY)</h2>
#   - occupation : <p>...</p>
#   - image : data-bg-image="url('.../YYYY-Lastname...jpg)"
_LAUREATE_URL_RE = re.compile(
    r'href="(https://www\.templetonprize\.org/laureate/[^"]+/)"',
)
_H2_NAME_YEAR_RE = re.compile(
    r'<h2[^>]*>\s*(.+?)\s*\(\s*(\d{4})(?:\s*[–-]\s*(\d{4}))?\s*\)\s*</h2>',
    re.DOTALL,
)
_OCCUPATION_RE = re.compile(
    r'<div class="laureate-content">\s*<h2[^>]*>.+?</h2>\s*<p[^>]*>(.+?)</p>',
    re.DOTALL,
)
_IMG_RE = re.compile(
    r"data-bg-image=\"url\(&#039;([^&]+)",
)


def parse_listing(html: str) -> list[dict]:
    """Parse the winners listing page into a list of laureate dicts."""
    out = []
    seen_slugs: set[str] = set()
    for m in _CARD_BLOCK_RE.finditer(html):
        decade_classes = m.group(1).strip()
        data_title     = m.group(2).strip()
        body           = m.group(3)
        # URL → slug
        url_m = _LAUREATE_URL_RE.search(body)
        if not url_m:
            continue
        url = url_m.group(1)
        slug_m = re.search(r'/laureate/([^/?#]+)/', url)
        if not slug_m:
            continue
        slug = slug_m.group(1)
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        # Name + year
        ny_m = _H2_NAME_YEAR_RE.search(body)
        if not ny_m:
            continue
        name_raw = _strip_html(ny_m.group(1))
        year     = int(ny_m.group(2))
        # joint-prize parens like "(2008–2009)" — unusual, but capture both
        year_end = int(ny_m.group(3)) if ny_m.group(3) else None
        # Occupation
        occ = None
        occ_m = _OCCUPATION_RE.search(body)
        if occ_m:
            occ = _strip_html(occ_m.group(1))
        # Image URL → confirms year prefix
        img = None
        img_m = _IMG_RE.search(body)
        if img_m:
            img = unescape(img_m.group(1))
        out.append({
            "slug":          slug,
            "name":          name_raw,
            "year":          year,
            "year_end":      year_end,
            "occupation":    occ,
            "landing_page_url": url,
            "image_url":     img,
            "decade_class":  decade_classes,
            "data_title":    data_title,
        })
    return out


def _strip_html(s: str) -> str:
    text = re.sub(r'<[^>]+>', ' ', s)
    return unescape(re.sub(r'\s+', ' ', text)).strip()


# =============================================================================
# Name splitter (runbook §2.4.1)
# =============================================================================

_DEGREE_SUFFIXES = {"PhD", "Ph.D.", "Ph.D", "MD", "M.D.", "DPhil", "ScD",
                    "Jr.", "Jr", "Sr.", "Sr", "II", "III", "IV", "OBE", "CBE",
                    "Sir", "Dame", "DBE"}
_HONORIFIC_PREFIXES_RE = re.compile(
    r'^(?:Dr\.?|Prof\.?|Professor|Mr\.?|Ms\.?|Mrs\.?|Sir|Dame|Brother|Father|Mother|'
    r"Reverend|Rev\.?|Rabbi|Lord|His Holiness|Her Holiness|The Dalai Lama|"
    r'His All Holiness Ecumenical Patriarch|Archbishop|Bishop)\s+',
    re.I,
)


def split_name(full_name: str) -> tuple[Optional[str], Optional[str]]:
    """Return (given_name, family_name) following runbook §2.4.1.
    Strips religious / academic honorifics common among Templeton laureates."""
    if not full_name:
        return None, None
    name = _HONORIFIC_PREFIXES_RE.sub("", full_name).strip()
    if not name:
        return None, None
    parts = [p.strip() for p in name.replace(";", ",").split(",")]
    name = parts[0].strip()
    toks = name.split()
    while toks and toks[-1].rstrip(".") in {s.rstrip(".") for s in _DEGREE_SUFFIXES}:
        toks.pop()
    if not toks:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


# =============================================================================
# Build DataFrame
# =============================================================================

def build_dataframe(all_rows: list[dict]) -> pd.DataFrame:
    print("\n" + "=" * 60)
    print(f"Step 2: Build flat DataFrame from {len(all_rows)} raw cards")
    print("=" * 60)

    seen_award_ids: set[str] = set()
    final: list[dict] = []
    for r in all_rows:
        given, family = split_name(r["name"])
        funder_award_id = f"templeton-prize-{r['year']}-{r['slug']}"
        if funder_award_id in seen_award_ids:
            raise RuntimeError(
                f"funder_award_id collision: {funder_award_id} — "
                "Templeton Prize (year, slug) should be unique per runbook prize pattern"
            )
        seen_award_ids.add(funder_award_id)

        display_name = f"Templeton Prize {r['year']} — {r['name']}"
        # Description: occupation + bio-style intro
        description = r.get("occupation")
        # End-year handling: most laureates have a single year. Joint-year
        # prizes (rare) span two years.
        end_year = r.get("year_end") or r["year"]

        final.append({
            "funder_award_id":   funder_award_id,
            "slug":              r["slug"],
            "name":              r["name"],
            "given_name":        given,
            "family_name":       family,
            "year":              r["year"],
            "year_end":          end_year,
            "occupation":        r.get("occupation"),
            "display_name":      display_name,
            "description":       description,
            "amount":            TEMPLETON_PRIZE_AMOUNT_GBP,
            "currency":          CURRENCY,
            "start_date":        f"{r['year']}-01-01",
            "end_date":          f"{end_year}-12-31",
            "landing_page_url":  r["landing_page_url"],
            "image_url":         r.get("image_url"),
            "decade_class":      r.get("decade_class"),
            "declined":          False,
        })

    df = pd.DataFrame.from_records(final)
    print(f"  rows: {len(df)}")
    print(f"  year range: {df['year'].min()}-{df['year'].max()}")
    print(f"  occupation coverage: {df['occupation'].notna().sum()}/{len(df)} "
          f"({df['occupation'].notna().sum() * 100 / len(df):.0f}%)")
    print(f"  given_name coverage: {df['given_name'].notna().sum()}/{len(df)} "
          f"({df['given_name'].notna().sum() * 100 / len(df):.0f}%)")
    # Year-distribution check
    print(f"  laureates per decade:")
    print(df.groupby((df['year'].astype(int) // 10) * 10).size().to_string())

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
    parquet_path = output_dir / "templeton_prize_laureates.parquet"
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
    prev_path = output_dir / "_prev_templeton_prize.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/templeton"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse templeton_prize_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Templeton Prize → S3 Pipeline (PRIZE PATTERN, static-HTML scrape)")
    print("=" * 60)
    print(f"  Awarding body: {FUNDER_DISPLAY_NAME} (F{FUNDER_ID})")
    print(f"  Output dir:    {args.output_dir.absolute()}")
    print(f"  S3 dest:       s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:       {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "templeton_prize_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        all_rows = json.loads(raw_path.read_text())
        print(f"\n[SKIP] reusing {raw_path} with {len(all_rows)} cached rows")
    else:
        print("\n" + "=" * 60)
        print("Step 1: Download winners listing page")
        print("=" * 60)
        resp = _http_get(WINNERS_URL)
        resp.raise_for_status()
        all_rows = parse_listing(resp.text)
        raw_path = args.output_dir / "templeton_prize_raw.json"
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
    print(f"Next: notebooks/awards/CreateTempletonPrizeAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
