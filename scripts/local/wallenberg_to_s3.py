#!/usr/bin/env python3
"""
Knut och Alice Wallenbergs Stiftelse (KAW) Research Projects → S3 Pipeline (GRANT PATTERN)
==========================================================================================

Downloads project pages from the foundation's own Drupal-published
research-project archive at kaw.wallenberg.org. Knut and Alice
Wallenberg Foundation (KAW) is Sweden's largest private research
funder, awarding ~SEK 2-3 billion per year to basic research in
medicine, technology, and natural sciences. Programs include
Wallenberg Scholars, Wallenberg Academy Fellows, Wallenberg Clinical
Scholars, prolongation grants, Project grants, etc.

Discovery (method-5 static HTML, Drupal Simple XML Sitemap):

  /sitemap.xml?page=1     2,000 URLs total (mostly press + research)
  /sitemap.xml?page=2     757 URLs

After filtering for the `/en/research/{slug}` and `/forskning/{slug}`
paths (English + Swedish versions of the same project pages), the
corpus is **655 unique research project pages**. We use the English
versions for canonical title + description; the Swedish version is
the same project under a different slug.

Each project page is server-rendered with:

  - `<h1>` project title
  - First paragraph: grant program label (e.g., "Wallenberg Academy
    Fellow, prolongation grant 2020") — encodes program + year
  - "Institution:{Name}" labeled paragraph
  - "Research field:{Description}" labeled paragraph
  - JSON-LD `Article` block with name, description, datePublished
  - Rich narrative paragraphs (PI name + work) — used as description

Awarding body in OpenAlex:
  Knut och Alice Wallenbergs Stiftelse (F4320322327, SE,
  DOI 10.13039/501100004063).

Amount handling:
  amount/currency are NULL with §6.7 waiver. KAW publishes program-
  level fixed amounts in narrative form on /en/grant-guide and
  per-class press releases (e.g., Wallenberg Academy Fellow = SEK
  7.5M over 5 years; Wallenberg Scholar = SEK 18M over 5 years) but
  does NOT publish per-project amount on the project page in any
  structured form. Per the runbook source-authority rule we don't
  backfill from press-release narrative. Grant/fellowship-pattern
  precedent for NULL amount: HHMI #44, CIFAR #79, Damon Runyon #73,
  Packard #95, Rita Allen #107, Schmidt Sciences #108, NOMIS #109,
  Wenner-Gren #110.

Output
------
  s3://openalex-ingest/awards/wallenberg/wallenberg_projects.parquet

Usage
-----
    python wallenberg_to_s3.py                                  # full run
    python wallenberg_to_s3.py --skip-upload                    # local dev
    python wallenberg_to_s3.py --limit 5                        # smoke
    python wallenberg_to_s3.py --skip-download --skip-upload    # reuse cache
    python wallenberg_to_s3.py --allow-shrink                   # override §1.4

Requirements
------------
    pip install pandas pyarrow requests beautifulsoup4 boto3
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- Windows UTF-8 compatibility shim (fleet 2026-05-22) -----------------
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

SITEMAP_URLS = [
    "https://kaw.wallenberg.org/sitemap.xml?page=1",
    "https://kaw.wallenberg.org/sitemap.xml?page=2",
]

FUNDER_ID = 4320322327
FUNDER_DISPLAY_NAME = "Knut och Alice Wallenbergs Stiftelse"

PROVENANCE = "kaw_wallenberg_projects"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/wallenberg/wallenberg_projects.parquet"

USER_AGENT = "openalex-walden-wallenberg-ingest/1.0 (+https://openalex.org)"

REQUEST_DELAY_S = 0.3
DEFAULT_CACHE = Path(".cache/wallenberg_pages.json")

ENGLISH_PATH_RE = re.compile(r"^https://kaw\.wallenberg\.org/en/research/[^/]+/?$")


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def slugify(s: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")
    return s or "unknown"


_SUFFIX_TOKENS = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}


def split_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not name:
        return None, None
    tokens = name.split()
    while tokens and tokens[-1].lower().strip(",.") in _SUFFIX_TOKENS:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


# =============================================================================
# HTTP + cache
# =============================================================================

_session: Optional[requests.Session] = None
_last_request_t = 0.0


def _http_get(url: str, timeout: int = 30) -> str:
    global _session, _last_request_t
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT})
    elapsed = time.monotonic() - _last_request_t
    if elapsed < REQUEST_DELAY_S:
        time.sleep(REQUEST_DELAY_S - elapsed)
    resp = _session.get(url, timeout=timeout)
    _last_request_t = time.monotonic()
    resp.raise_for_status()
    return resp.text


def load_cache(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_cache(path: Path, cache: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False))


def get_page(url: str, cache: dict[str, str], use_cache: bool) -> str:
    if use_cache and url in cache:
        return cache[url]
    html = _http_get(url)
    cache[url] = html
    return html


# =============================================================================
# Discovery + parse
# =============================================================================

def discover_project_urls(use_cache: bool, cache: dict[str, str]) -> list[str]:
    urls = []
    for sm_url in SITEMAP_URLS:
        xml = get_page(sm_url, cache, use_cache)
        for m in re.finditer(r"<loc>([^<]+)</loc>", xml):
            u = m.group(1)
            if ENGLISH_PATH_RE.match(u):
                urls.append(u)
    # dedup while preserving order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    log(f"  enumerated {len(out)} English research-project URLs")
    return out


PROGRAM_YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")

# Labeled-paragraph patterns. The page uses bare colons with no spaces after
# (`get_text(strip=True)` collapses internal whitespace), so the regex is
# `^Label\s*:\s*(.+)$` and accepts the no-space form `Label:value`.
LABEL_PATTERNS = {
    "institution":           re.compile(r"^Institution\s*:\s*(.+)$", re.IGNORECASE),
    "research_field":        re.compile(r"^Research field\s*:\s*(.+)$", re.IGNORECASE),
    "principal_investigator": re.compile(r"^Principal investigators?\s*:\s*(.+)$", re.IGNORECASE),
    "co_investigators":      re.compile(r"^Co-investigators?\s*:\s*(.+)$", re.IGNORECASE),
    "grant_text":            re.compile(r"^Grant\s+(?:in\s+)?(SEK|EUR|USD|kr)?\s*:\s*(.+)$", re.IGNORECASE),
    "project_period":        re.compile(r"^(?:Project )?Period\s*:\s*(.+)$", re.IGNORECASE),
}

# Currency display map
SEK_TOKENS = ("SEK", "kr", "kronor")
USD_TOKENS = ("USD", "US$", "$")
EUR_TOKENS = ("EUR", "€")

# "Grant in SEK:57 million over five years" → 57_000_000 SEK
# "Grant in SEK:240 million" → 240_000_000 SEK
# "Grant in SEK:7.5 million over 5 years" → 7_500_000 SEK
AMOUNT_NUMBER_RE = re.compile(r"([0-9]+(?:[.,][0-9]+)?)\s*(million|billion|m\b|mkr|miljon|mdkr)?", re.IGNORECASE)


def parse_amount_and_currency(text: Optional[str], currency_hint: Optional[str]) -> tuple[Optional[float], Optional[str]]:
    """Parse "57 million over five years" or "SEK 7.5M" etc."""
    if not text:
        return None, None
    # Currency detection
    upper = text.upper()
    if any(t in upper for t in (s.upper() for s in SEK_TOKENS)) or (currency_hint and currency_hint.upper() in ("SEK", "KR")):
        currency = "SEK"
    elif any(t in upper for t in (s.upper() for s in USD_TOKENS)) or (currency_hint and currency_hint.upper() == "USD"):
        currency = "USD"
    elif any(t in upper for t in (s.upper() for s in EUR_TOKENS)) or (currency_hint and currency_hint.upper() == "EUR"):
        currency = "EUR"
    else:
        # KAW grants are virtually all SEK; default to SEK when no token matches
        # but currency_hint suggested an amount was paid in SEK by context.
        currency = "SEK" if currency_hint else None

    m = AMOUNT_NUMBER_RE.search(text)
    if not m:
        return None, currency
    raw_num = m.group(1).replace(",", ".")
    try:
        val = float(raw_num)
    except ValueError:
        return None, currency

    unit = (m.group(2) or "").lower()
    if "billion" in unit or "mdkr" in unit:
        val *= 1_000_000_000
    elif "million" in unit or "miljon" in unit or "mkr" in unit or unit == "m":
        val *= 1_000_000

    if val < 1000:
        # implausibly small for a Wallenberg grant; suppress
        return None, currency
    return val, currency


def parse_project_page(html: str, url: str) -> Optional[dict]:
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else None
    if not title:
        return None

    slug = url.rstrip("/").rsplit("/", 1)[-1]

    # JSON-LD for description + publication date
    description = None
    published = None
    for m in re.finditer(r'<script type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S):
        try:
            d = json.loads(m.group(1))
        except json.JSONDecodeError:
            continue
        g = d.get("@graph", [d])
        for item in g:
            if isinstance(item, dict) and item.get("@type") == "Article":
                description = item.get("description") or description
                published = item.get("datePublished") or published
                break

    # Walk the main paragraphs. The first substantive paragraph is the grant-
    # program label. Subsequent paragraphs may be labeled (Institution:, PI:,
    # Co-investigators:, Grant in SEK:, Period:, Research field:). Remaining
    # ones are body prose.
    program_label = None
    fields: dict[str, str] = {}
    body_paragraphs: list[str] = []

    main = soup.find("main") or soup
    for p in main.find_all("p"):
        t = p.get_text(strip=True)
        if not t:
            continue
        # Footer/contact block — bail before consuming it as body
        if t.startswith("Knut and Alice Wallenberg Foundation") and "P.O. Box" in t:
            break
        if program_label is None:
            program_label = t
            continue

        matched = False
        for key, pat in LABEL_PATTERNS.items():
            m = pat.match(t)
            if m:
                if key == "grant_text":
                    # Capture both the currency-hint group and the trailing value
                    fields["grant_currency_hint"] = m.group(1) or ""
                    fields["grant_text"] = m.group(2).strip()
                else:
                    fields[key] = m.group(1).strip()
                matched = True
                break
        if matched:
            continue

        body_paragraphs.append(t)

    body_text = " ".join(body_paragraphs)[:5000] if body_paragraphs else None
    if not description and fields.get("research_field"):
        description = fields["research_field"]

    # PI split
    pi_raw = fields.get("principal_investigator")
    pi_name = None
    pi_role = None
    if pi_raw:
        # Strip trailing ", role" if present (e.g., "Tomas Olsson, professor of neurology")
        if "," in pi_raw:
            pi_name, _, role = pi_raw.partition(",")
            pi_name = pi_name.strip()
            pi_role = role.strip()
        else:
            pi_name = pi_raw.strip()
    pi_given, pi_family = split_name(pi_name)

    # Co-investigators: KAW concatenates names without separators after
    # get_text(strip=True) (e.g., "Jan HillertIngrid LundbergMarie Wahren Herlenius").
    # We keep the raw string for downstream extraction rather than trying to
    # un-concatenate it heuristically.
    co_investigators_raw = fields.get("co_investigators")

    # Amount + currency
    grant_text = fields.get("grant_text")
    grant_ccy_hint = fields.get("grant_currency_hint")
    amount, currency = parse_amount_and_currency(grant_text, grant_ccy_hint)

    # Year: prefer year in program_label, else from published date
    year = None
    if program_label:
        years = PROGRAM_YEAR_RE.findall(program_label)
        if years:
            year = max(int(y) for y in years if 1990 <= int(y) <= 2030)
    if not year and published:
        try:
            year = int(published[:4])
        except (ValueError, TypeError):
            pass

    return {
        "slug":                  slug,
        "title":                 title,
        "program_label":         program_label,
        "institution":           fields.get("institution"),
        "research_field":        fields.get("research_field"),
        "principal_investigator_raw": pi_raw,
        "pi_given_name":         pi_given,
        "pi_family_name":        pi_family,
        "pi_role":               pi_role,
        "co_investigators_raw":  co_investigators_raw,
        "grant_text":            grant_text,
        "amount":                amount,
        "currency":              currency,
        "project_period":        fields.get("project_period"),
        "description":           description,
        "body_text":             body_text,
        "datePublished":         published,
        "award_year":            year,
        "landing_page_url":      url,
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError("No project rows parsed")
    n = len(rows)
    for f in ("title", "slug", "program_label", "institution", "research_field", "award_year", "description"):
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<18} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")

    slugs = [r["slug"] for r in rows if r.get("slug")]
    if len(slugs) != len(set(slugs)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(slugs).items() if v > 1][:5]
        raise RuntimeError(f"slug collisions: {dups}")
    log(f"  slug uniqueness: {len(slugs)}/{n} distinct ✓")


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["funder_award_id"] = "kaw-" + df["slug"]
    df = df.astype("string")
    return df


# =============================================================================
# Shrink-check + upload
# =============================================================================

def check_no_shrink(new_count: int, allow_shrink: bool) -> bool:
    if allow_shrink:
        log("  --allow-shrink set; skipping §1.4 shrink-check")
        return True
    try:
        import boto3
        import io
        s3 = boto3.client("s3")
        prev_bytes = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)["Body"].read()
        prev_df = pd.read_parquet(io.BytesIO(prev_bytes))
        prev_count = len(prev_df)
        log(f"  §1.4 shrink-check: previous S3 parquet had {prev_count:,} rows")
        if new_count < prev_count:
            log(f"  §1.4 FAIL: new ({new_count:,}) < previous ({prev_count:,}). Aborting.")
            return False
        log(f"  §1.4 OK: new {new_count:,} >= previous {prev_count:,}")
        return True
    except Exception as e:
        log(f"  §1.4 shrink-check skipped: {type(e).__name__}: {str(e)[:100]}. (normal on first run)")
        return True


def upload_to_s3(local_file: Path) -> None:
    try:
        import boto3
    except ImportError:
        raise RuntimeError("boto3 required for S3 upload; pass --skip-upload for local only")
    log(f"Uploading {local_file} -> s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(local_file), S3_BUCKET, S3_KEY)
    log("  upload OK")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch KAW Wallenberg research projects → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "wallenberg_projects.parquet"

    log(f"=== KAW Wallenberg projects ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache={args.cache}")

    cache = load_cache(args.cache)

    urls = discover_project_urls(args.skip_download, cache)
    save_cache(args.cache, cache)

    if args.limit is not None:
        urls = urls[:args.limit]
        log(f"--limit {args.limit}: processing {len(urls)} URLs")

    rows = []
    for i, url in enumerate(urls, 1):
        try:
            html = get_page(url, cache, args.skip_download)
        except Exception as e:
            log(f"  [{i}/{len(urls)}] FETCH FAIL {url}: {type(e).__name__}: {e}")
            continue
        row = parse_project_page(html, url)
        if row is None:
            log(f"  [{i}/{len(urls)}] PARSE FAIL {url}")
            continue
        rows.append(row)
        if i % 50 == 0:
            log(f"  [{i}/{len(urls)}] parsed (total so far: {len(rows)})")
            save_cache(args.cache, cache)
    save_cache(args.cache, cache)

    log(f"Parsed {len(rows)} project rows")
    validate_rows(rows)

    log("Building DataFrame...")
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.1f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== KAW Wallenberg ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== KAW Wallenberg ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
