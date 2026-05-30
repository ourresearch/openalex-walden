#!/usr/bin/env python3
"""
William T. Grant Foundation Grants → S3 Pipeline (GRANT PATTERN, method-5 static HTML)
=====================================================================================

Downloads grants from the William T. Grant Foundation's own
WordPress-published grants archive at wtgrantfoundation.org. The
foundation funds research on reducing inequality and improving the use
of research evidence in youth-serving systems, plus capacity-building
grants to NYC youth-service organizations.

Discovery (method-5 static HTML): two Yoast sitemap files enumerate the
full corpus.

  /grants-sitemap.xml     1,000 grant URLs
  /grants-sitemap2.xml      573 grant URLs
  TOTAL                   1,573 grant URLs

Each grant detail page carries an "About This Grant" sidebar — a list of
`<div class="about-post-item"><span class="about-post-role">Label</span>
…value…</div>` rows exposing the structured fields we need:

  Principal Investigator    person + institution (research/scholar grants)
  Grantee Organization      org only            (youth-service grants)
  Co-Principal Investigator person + institution (~10% of grants)
  Grant Period              e.g., January 2004 – December 2004
  Programs                  e.g., Research Grants  (-> funder_scheme)
  Focus Areas               e.g., Reducing Inequality
  Topics                    e.g., Criminal Justice
  Grant Amount              e.g., $109,766.10      (~87% of grants)

A grant is led by EITHER a Principal Investigator (named person at an
institution) OR a Grantee Organization (org-level youth-service grant);
together these cover 100% of grants. The page OpenGraph title is the
grant title; OpenGraph description is the abstract.

Native award ID is the URL slug (e.g.
`/grants/policing-and-protesting-juvenile-justice-inequality/` ->
`policing-and-protesting-juvenile-justice-inequality`), which is stable,
unique, and source-authoritative.

Awarding body in OpenAlex:
  William T. Grant Foundation (F4320306360, US, no ROR,
  DOI 10.13039/100001143).

Amount: published as a dollar figure on ~87% of grant pages; populated
where present, NULL otherwise (NOT a blanket §6.7 waiver). Currency USD
hardcoded (single-country US foundation), set only where amount present.

Output
------
  s3://openalex-ingest/awards/wt_grant/wt_grant_grants.parquet

Usage
-----
    python wt_grant_to_s3.py                                  # full run (~10 min @ 0.3s throttle)
    python wt_grant_to_s3.py --skip-upload                    # local dev
    python wt_grant_to_s3.py --limit 10                       # smoke
    python wt_grant_to_s3.py --skip-download --skip-upload    # reuse cache
    python wt_grant_to_s3.py --allow-shrink                   # override §1.4

Requirements
------------
    pip install pandas pyarrow requests beautifulsoup4 boto3
"""

import argparse
import html as ihtml
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
    "https://wtgrantfoundation.org/grants-sitemap.xml",
    "https://wtgrantfoundation.org/grants-sitemap2.xml",
]

FUNDER_ID = 4320306360
FUNDER_DISPLAY_NAME = "William T. Grant Foundation"

PROVENANCE = "wt_grant_foundation"
CURRENCY = "USD"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/wt_grant/wt_grant_grants.parquet"

USER_AGENT = "openalex-walden-wtgrant-ingest/1.0 (+https://openalex.org)"

REQUEST_DELAY_S = 0.3
DEFAULT_CACHE = Path(".cache/wt_grant_pages.json")

GRANT_URL_RE = re.compile(r"^https://wtgrantfoundation\.org/grants/[^/]+/?$")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


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


def load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_cache(path: Path, cache: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False))


def get_page(url: str, cache: dict, use_cache: bool) -> str:
    if use_cache and url in cache:
        return cache[url]
    html = _http_get(url)
    cache[url] = html
    return html


# =============================================================================
# Discovery + parse
# =============================================================================

def discover_grant_urls(use_cache: bool, cache: dict) -> list:
    urls = []
    for sm_url in SITEMAP_URLS:
        xml = get_page(sm_url, cache, use_cache)
        for m in re.finditer(r"<loc>([^<]+)</loc>", xml):
            u = ihtml.unescape(m.group(1)).strip()
            if GRANT_URL_RE.match(u):
                urls.append(u)
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    log(f"  enumerated {len(out)} grant URLs across {len(SITEMAP_URLS)} sitemaps")
    return out


AMOUNT_NUMBER_RE = re.compile(r"\$\s*([0-9,]+(?:\.[0-9]+)?)")
# "January 2004 – December 2004" (en-dash/em-dash/hyphen). Day part optional.
PERIOD_RE = re.compile(
    r"([A-Za-z]+)\s+(\d{4})\s*[–—\-]\s*([A-Za-z]+)\s+(\d{4})"
)


def parse_amount(s: str) -> Optional[float]:
    if not s:
        return None
    m = AMOUNT_NUMBER_RE.search(s)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def parse_period(s: str):
    """Parse "January 2004 – December 2004" -> (start_year, end_year)."""
    if not s:
        return None, None
    m = PERIOD_RE.search(s)
    if m:
        return int(m.group(2)), int(m.group(4))
    # Single year fallback (e.g. "2004")
    y = re.search(r"\b(19\d{2}|20\d{2})\b", s)
    if y:
        return int(y.group(1)), None
    return None, None


def split_name(display: str):
    """'Vincent Guilamo-Ramos' -> ('Vincent', 'Guilamo-Ramos')."""
    if not display:
        return None, None
    parts = display.strip().split()
    if len(parts) == 1:
        return None, parts[0]
    return " ".join(parts[:-1]), parts[-1]


def _role_items(about_block):
    """Yield (role, content_div) for each about-post-item."""
    out = []
    for item in about_block.find_all("div", class_="about-post-item"):
        role_span = item.find("span", class_="about-post-role")
        if not role_span:
            continue
        role = role_span.get_text(strip=True)
        out.append((role, item))
    return out


def _person_and_org(item):
    """From a PI/Co-PI item, return (person_name, person_slug, org_name)."""
    person_name = person_slug = org_name = None
    for a in item.find_all("a"):
        href = a.get("href", "")
        text = a.get_text(strip=True)
        if "/people/" in href and person_name is None:
            person_name = text
            person_slug = href.rstrip("/").rsplit("/", 1)[-1]
        elif "/institution/" in href and org_name is None:
            org_name = text
    # Fallback: split the raw "Name, Institution" text if links absent
    if person_name is None or org_name is None:
        role_span = item.find("span", class_="about-post-role")
        raw = item.get_text(" ", strip=True)
        if role_span:
            raw = raw.replace(role_span.get_text(strip=True), "", 1).strip()
        if "," in raw:
            a, b = raw.split(",", 1)
            person_name = person_name or a.strip() or None
            org_name = org_name or b.strip() or None
        elif raw:
            person_name = person_name or raw or None
    return person_name, person_slug, org_name


def _org_only(item):
    a = item.find("a")
    if a:
        return a.get_text(strip=True)
    role_span = item.find("span", class_="about-post-role")
    raw = item.get_text(" ", strip=True)
    if role_span:
        raw = raw.replace(role_span.get_text(strip=True), "", 1).strip()
    return raw or None


def _link_text_value(item):
    """For Programs/Focus Areas/Topics: join link texts (or raw text)."""
    links = [a.get_text(strip=True) for a in item.find_all("a")]
    if links:
        return "; ".join([t for t in links if t])
    role_span = item.find("span", class_="about-post-role")
    raw = item.get_text(" ", strip=True)
    if role_span:
        raw = raw.replace(role_span.get_text(strip=True), "", 1).strip()
    return raw or None


def parse_grant_page(html: str, url: str) -> Optional[dict]:
    soup = BeautifulSoup(html, "html.parser")
    slug = url.rstrip("/").rsplit("/", 1)[-1]

    # Title from og:title (strip site suffix); fall back to <h1>.
    title = None
    og_title = soup.find("meta", property="og:title")
    if og_title:
        t = ihtml.unescape(og_title.get("content", "") or "")
        t = re.sub(r"\s*[-–—]\s*William T\. Grant Foundation\s*$", "", t).strip()
        title = t or None
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else None
    if not title:
        return None

    description = None
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        description = ihtml.unescape(og_desc.get("content", "") or "").strip() or None

    # Locate the "About This Grant" sidebar.
    about_block = None
    for h3 in soup.find_all(["h3", "h2", "h4"]):
        if "About This Grant" in h3.get_text(strip=True):
            about_block = h3.parent
            break
    if about_block is None:
        about_block = soup

    pi_name = pi_slug = pi_org = None
    copi_name = copi_org = None
    grantee_org_only = None
    program = focus_areas = topics = None
    amount_raw = period_raw = None

    for role, item in _role_items(about_block):
        rl = role.lower()
        if rl == "principal investigator" and pi_name is None and pi_org is None:
            pi_name, pi_slug, pi_org = _person_and_org(item)
        elif rl == "co-principal investigator" and copi_name is None:
            copi_name, _cs, copi_org = _person_and_org(item)
        elif rl == "grantee organization" and grantee_org_only is None:
            grantee_org_only = _org_only(item)
        elif rl == "programs" and program is None:
            program = _link_text_value(item)
        elif rl == "focus areas" and focus_areas is None:
            focus_areas = _link_text_value(item)
        elif rl == "topics" and topics is None:
            topics = _link_text_value(item)
        elif rl == "grant period" and period_raw is None:
            role_span = item.find("span", class_="about-post-role")
            raw = item.get_text(" ", strip=True)
            if role_span:
                raw = raw.replace(role_span.get_text(strip=True), "", 1).strip()
            period_raw = raw or None
        elif rl == "grant amount" and amount_raw is None:
            role_span = item.find("span", class_="about-post-role")
            raw = item.get_text(" ", strip=True)
            if role_span:
                raw = raw.replace(role_span.get_text(strip=True), "", 1).strip()
            amount_raw = raw or None

    # Lead shaping: PI person (+ their org) OR org-level grantee.
    given_name, family_name = (split_name(pi_name) if pi_name else (None, None))
    grantee_org = pi_org or grantee_org_only

    copi_given, copi_family = (split_name(copi_name) if copi_name else (None, None))

    amount = parse_amount(amount_raw or "")
    start_year, end_year = parse_period(period_raw or "")

    return {
        "slug":             slug,
        "funder_award_id":  slug,
        "title":            title,
        "description":      description,
        "program":          program,
        "focus_areas":      focus_areas,
        "topics":           topics,
        "amount":           amount,
        "amount_raw":       amount_raw,
        "currency":         CURRENCY if amount is not None else None,
        "period_raw":       period_raw,
        "start_year":       start_year,
        "end_year":         end_year,
        "given_name":       given_name,
        "family_name":      family_name,
        "grantee_org":      grantee_org,
        "pi_people_slug":   pi_slug,
        "copi_given_name":  copi_given,
        "copi_family_name": copi_family,
        "copi_org":         copi_org,
        "landing_page_url": url,
    }


# =============================================================================
# Validate + DataFrame
# =============================================================================

def validate_rows(rows: list) -> None:
    if not rows:
        raise RuntimeError("No grant rows parsed")
    n = len(rows)
    for f in ("title", "slug", "program", "amount", "start_year", "end_year",
              "grantee_org", "given_name", "description"):
        non_null = sum(1 for r in rows if r.get(f) not in (None, "", []))
        log(f"  {f:<16} coverage {non_null}/{n} ({non_null*100/n:.1f}%)")

    # Every grant must have a lead: a PI person OR an org.
    no_lead = [r["slug"] for r in rows
               if not r.get("grantee_org") and not r.get("given_name")]
    if no_lead:
        log(f"  WARN: {len(no_lead)} rows with no PI and no org (e.g. {no_lead[:3]})")

    slugs = [r["slug"] for r in rows if r.get("slug")]
    if len(slugs) != len(set(slugs)):
        from collections import Counter
        dups = [(k, v) for k, v in Counter(slugs).items() if v > 1][:5]
        raise RuntimeError(f"slug collisions: {dups}")
    log(f"  slug uniqueness: {len(slugs)}/{n} distinct ok")

    years = [r["start_year"] for r in rows if r.get("start_year")]
    if years:
        log(f"  start_year range: {min(years)}–{max(years)}")

    amts = [r["amount"] for r in rows if r.get("amount") is not None]
    if amts:
        srt = sorted(amts)
        log(f"  amount stats: n={len(amts)} min=${min(amts):,.0f} "
            f"median=${srt[len(srt)//2]:,.0f} max=${max(amts):,.0f} "
            f"total=${sum(amts):,.0f}")

    progs = {}
    for r in rows:
        p = r.get("program") or "(none)"
        progs[p] = progs.get(p, 0) + 1
    log("  programs: " + ", ".join(f"{k}={v}" for k, v in sorted(progs.items(), key=lambda x: -x[1])))


def build_dataframe(rows: list) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    # §1.2.5: force string dtype to stop pandas int-inferring null-heavy cols.
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
    parser = argparse.ArgumentParser(description="Fetch William T. Grant Foundation grants → parquet → S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "wt_grant_grants.parquet"

    log("=== William T. Grant Foundation grants ingest start ===")
    log(f"  funder_id={FUNDER_ID} ({FUNDER_DISPLAY_NAME})")
    log(f"  provenance={PROVENANCE}")
    log(f"  output={output_path}")
    log(f"  cache={args.cache}")

    cache = load_cache(args.cache)

    urls = discover_grant_urls(args.skip_download, cache)
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
        row = parse_grant_page(html, url)
        if row is None:
            log(f"  [{i}/{len(urls)}] PARSE FAIL {url}")
            continue
        rows.append(row)
        if i % 100 == 0:
            log(f"  [{i}/{len(urls)}] parsed (total so far: {len(rows)})")
        if i % 50 == 0:
            save_cache(args.cache, cache)
    save_cache(args.cache, cache)

    log(f"Parsed {len(rows)} grant rows")
    validate_rows(rows)

    log("Building DataFrame...")
    df = build_dataframe(rows)
    log(f"  rows={len(df):,} cols={len(df.columns)} -> {output_path}")
    df.to_parquet(output_path, index=False)
    log(f"  wrote {output_path.stat().st_size/1e3:.1f} KB")

    if args.skip_upload:
        log("--skip-upload: not uploading to S3")
        log("=== WT Grant ingest done (local-only) ===")
        return

    if not check_no_shrink(len(df), args.allow_shrink):
        raise SystemExit("§1.4 shrink-check failed.")

    upload_to_s3(output_path)
    log("=== WT Grant ingest done (uploaded) ===")


if __name__ == "__main__":
    main()
