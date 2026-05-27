#!/usr/bin/env python3
"""
Camille and Henry Dreyfus Foundation to S3 Data Pipeline
=========================================================

Downloads Dreyfus Foundation awardees from www.dreyfus.org's per-year award
announcement pages, enumerated from the foundation's own WordPress
post-sitemap. Each page lists that year's recipients of a single program.

Programs covered
----------------
  - Camille Dreyfus Teacher-Scholar Awards (2016-2026, USD 100,000/awardee)
  - Henry Dreyfus Teacher-Scholar Awards (2016-2025, USD 75,000/awardee)
  - Supplemental Grants for Teacher-Scholars (2025, amount NULL §6.7)
  - Machine Learning in the Chemical Sciences and Engineering (2022, amount NULL §6.7)

Source authority
----------------
www.dreyfus.org is the awarding body's own site (WordPress, public, no
auth). WP REST is restricted by Kadence Security (401), so we parse the
rendered HTML on year-program announcement pages instead. Pages are
enumerated from /post-sitemap.xml rather than guessed.

Amount and currency
-------------------
Camille and Henry Teacher-Scholar Awards: per-recipient amounts are
published on each program's landing page (Camille USD 100,000, Henry
USD 75,000). Supplemental Grants and the one-off 2022 Machine Learning
Awards do not publish per-recipient amounts on their announcement pages;
amount/currency ship as NULL for those schemes (§6.7 waiver, same pattern
as Packard and Damon Runyon).

Coverage note
-------------
Per-year heritage HTML varies. The unified parser captures 100% of
recipients on 2018+ Camille pages and all sampled Henry pages, but the
2016 Camille page lists 13 recipients and the parser extracts 12 (92%)
because one awardee's photo is wrapped in <strong>...<img>...</strong>
which collides with the name-detection heuristic. Documented; no
silent loss elsewhere.

Output
------
s3://openalex-ingest/awards/dreyfus/dreyfus_awardees.parquet

Usage
-----
    python dreyfus_to_s3.py                    # full run
    python dreyfus_to_s3.py --skip-upload      # local dev
    python dreyfus_to_s3.py --skip-download    # reuse cached JSON
    python dreyfus_to_s3.py --limit 10         # smoke test
    python dreyfus_to_s3.py --allow-shrink     # override §1.4

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

SITE_BASE = "https://www.dreyfus.org"
POST_SITEMAP_URL = f"{SITE_BASE}/post-sitemap.xml"

# Awarding body — Camille and Henry Dreyfus Foundation.
# Verified F4320306315, country US (DOI 10.13039/100001082).
FUNDER_ID = 4320306315
FUNDER_DISPLAY_NAME = "Camille and Henry Dreyfus Foundation"

PROVENANCE = "dreyfus_foundation"

# S3 destination.
S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/dreyfus/dreyfus_awardees.parquet"

USER_AGENT = "openalex-walden-dreyfus-ingest/1.0 (+https://openalex.org)"

# Polite throttle — small corpus (~50 year-program pages) so 2 req/sec is plenty.
MIN_REQUEST_INTERVAL_S = 0.5

# Program registry. Each entry binds a sitemap URL pattern to a funder_scheme
# slug, display name, official per-recipient amount, and funding_type. Amount
# is sourced from the program's own landing page on dreyfus.org.
PROGRAMS = [
    {
        "scheme": "camille_dreyfus_teacher_scholar",
        "scheme_label": "Camille Dreyfus Teacher-Scholar Awards",
        "url_regex": re.compile(
            r"^https://www\.dreyfus\.org/(20[0-9]{2})-camille-dreyfus-teacher-scholar-awards/?$"
        ),
        "funding_type": "research",
        "amount": 100000.0,
        "currency": "USD",
    },
    {
        "scheme": "henry_dreyfus_teacher_scholar",
        "scheme_label": "Henry Dreyfus Teacher-Scholar Awards",
        "url_regex": re.compile(
            r"^https://www\.dreyfus\.org/(20[0-9]{2})-henry-dreyfus-teacher-scholar-awards/?$"
        ),
        "funding_type": "research",
        "amount": 75000.0,
        "currency": "USD",
    },
    {
        "scheme": "supplemental_grants_teacher_scholar",
        "scheme_label": "Supplemental Grants for Teacher-Scholars",
        "url_regex": re.compile(
            r"^https://www\.dreyfus\.org/(20[0-9]{2})-supplemental-grants/?$"
        ),
        "funding_type": "research",
        "amount": None,
        "currency": None,
    },
    {
        "scheme": "machine_learning_chemical_sciences",
        "scheme_label": "Machine Learning in the Chemical Sciences and Engineering Awards",
        "url_regex": re.compile(
            r"^https://www\.dreyfus\.org/(20[0-9]{2})-machine-learning-in-the-chemical-sciences-and-engineering-awards/?$"
        ),
        "funding_type": "research",
        "amount": None,
        "currency": None,
    },
]


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
    resp = _session.get(url, timeout=timeout)
    _last_request_t = time.monotonic()
    return resp


# =============================================================================
# Sitemap enumeration
# =============================================================================

_LOC_RE = re.compile(r"<loc>([^<]+)</loc>")


def enumerate_program_pages() -> list[dict]:
    """Walk post-sitemap.xml, return list of {url, year, scheme, ...} dicts."""
    r = _http_get(POST_SITEMAP_URL)
    r.raise_for_status()
    urls = _LOC_RE.findall(r.text)
    out: list[dict] = []
    for u in urls:
        u = u.strip()
        for prog in PROGRAMS:
            m = prog["url_regex"].match(u)
            if m:
                out.append({
                    "url": u,
                    "year": int(m.group(1)),
                    "scheme": prog["scheme"],
                    "scheme_label": prog["scheme_label"],
                    "funding_type": prog["funding_type"],
                    "amount": prog["amount"],
                    "currency": prog["currency"],
                })
                break
    out.sort(key=lambda d: (d["scheme"], d["year"]))
    return out


# =============================================================================
# Per-page awardee parser
# =============================================================================

def _extract_main(html: str) -> Optional[str]:
    """Slice the article body out of the rendered page."""
    start = html.find('entry-content"')
    if start < 0:
        return None
    start = html.find(">", start) + 1
    end_candidates = [
        html.find(m, start)
        for m in ('<div id="sidebar"', "<footer", "<!-- #left-area -->", "</article>")
    ]
    end_candidates = [e for e in end_candidates if e > 0]
    return html[start:min(end_candidates)] if end_candidates else html[start:]


def _strip_tags(s: str) -> str:
    s = re.sub(r"<[^>]+>", "", s)
    return unescape(re.sub(r"\s+", " ", s)).strip()


_STRONG_INNER_RE = re.compile(
    r"<(?:strong|b)>((?:(?!</?(?:strong|b)>).)*?)</(?:strong|b)>", re.DOTALL
)
_HREF_INNER_RE = re.compile(
    r'<a [^>]*href="(?P<url>[^"]*)"[^>]*>(?P<inner>.*?)</a>', re.DOTALL
)
_EM_INNER_RE = re.compile(r"<(?:em|i)>(.*?)</(?:em|i)>", re.DOTALL)


def _parse_block(p: str) -> Optional[dict]:
    """Try to extract {name, institution, research_title, profile_url} from one <p>."""
    # Collect non-img-only strongs
    strongs: list[tuple[str, int, int]] = []
    for m in _STRONG_INNER_RE.finditer(p):
        content = m.group(1)
        if "<img" in content and not _strip_tags(content):
            continue
        text = _strip_tags(content).rstrip(",").strip()
        if text:
            strongs.append((text, m.start(), m.end()))

    # Identify the awardee name. Names are short, mostly proper nouns, no
    # colons, no slashes, no ampersands. Titles are longer, often have
    # special chars. The first short strong is the name in every layout
    # we've observed (2016-2026).
    name: Optional[str] = None
    name_end_pos = -1
    for text, st, en in strongs:
        if len(text) > 80:
            continue
        if len(text.split()) > 6:
            continue
        if any(c in text for c in [":", "?", "/", "&", "#"]):
            continue
        name = text
        name_end_pos = en
        break

    # Fallback: first <a href> with short text content (some older layouts
    # don't <strong>-wrap the name).
    profile_url: Optional[str] = None
    if not name:
        for m in _HREF_INNER_RE.finditer(p):
            text = _strip_tags(m.group("inner")).rstrip(",").strip()
            if text and len(text) <= 80 and len(text.split()) <= 6:
                name = text
                name_end_pos = m.end()
                profile_url = m.group("url")
                break
    else:
        # Look for a profile URL whose <a> wraps or is wrapped by the name's <strong>
        for m in _HREF_INNER_RE.finditer(p):
            inner_text = _strip_tags(m.group("inner")).rstrip(",").strip()
            if inner_text == name or name in inner_text:
                profile_url = m.group("url")
                break

    if not name:
        return None

    # Sanity-check name shape: must be a person, not a heading. Accept
    # multi-word names OR single-word names with internal capitals.
    if " " not in name and not any(c.isupper() for c in name[1:]):
        return None

    # Title: first <em>/<i> content (heritage 2019 pages use <b>/<i>).
    em_m = _EM_INNER_RE.search(p)
    title = _strip_tags(em_m.group(1)) if em_m else ""
    em_start = em_m.start() if em_m else len(p)

    # Fallback: if <em> was empty/short, look for the next strong after the
    # name (2022 ML pages wrap titles in <strong><em>...</em></strong>).
    if not title or len(title) < 10:
        for text, st, en in strongs:
            if st < name_end_pos:
                continue
            if len(text) > 15:
                title = text
                break

    # Institution: slice between name-end and title-start, strip tags,
    # take the leading text chunk (this handles all known layouts:
    # `</strong>Institution<br />`, `</a><br />Institution<br />`, etc.).
    inst_segment = p[name_end_pos:em_start]
    inst_text = _strip_tags(inst_segment)
    inst_text = re.sub(r"^[\s,;.]+", "", inst_text)
    institution = inst_text.strip() or None
    if institution and len(institution) > 200:
        institution = institution[:200].rstrip()
    # Reject institutions that are obviously narrative fragments (rare,
    # but the 2022 Camille intro paragraph would otherwise leak through).
    if institution and len(institution.split()) > 30:
        institution = None

    return {
        "name": name,
        "institution": institution,
        "research_title": title or None,
        "profile_url": profile_url,
    }


def parse_page(html: str) -> list[dict]:
    """Return all awardee dicts on one year-program page."""
    main = _extract_main(html)
    if not main:
        return []
    out: list[dict] = []
    seen: set[str] = set()
    for p in re.findall(r"<p[^>]*>(.*?)</p>", main, re.DOTALL):
        rec = _parse_block(p)
        if rec and rec["name"] not in seen:
            out.append(rec)
            seen.add(rec["name"])
    return out


# =============================================================================
# Smoke test
# =============================================================================

def smoke_test() -> None:
    print("\n" + "=" * 60)
    print("Smoke test: post-sitemap.xml + 2024 Camille parses")
    print("=" * 60)
    pages = enumerate_program_pages()
    print(f"  {len(pages)} year-program pages enumerated from post-sitemap")
    if not pages:
        print("[ERROR] post-sitemap returned 0 matching pages — selector changed?")
        sys.exit(3)
    by_scheme: dict[str, int] = {}
    for pg in pages:
        by_scheme[pg["scheme"]] = by_scheme.get(pg["scheme"], 0) + 1
    for k, v in by_scheme.items():
        print(f"    {k}: {v} year-pages")
    # Sample a recent Camille page
    sample = next(
        (p for p in pages if p["scheme"] == "camille_dreyfus_teacher_scholar"),
        pages[0],
    )
    r = _http_get(sample["url"])
    r.raise_for_status()
    awardees = parse_page(r.text)
    print(f"  sample {sample['url']}: parsed {len(awardees)} awardees")
    if awardees:
        a = awardees[0]
        print(f"    first: {a['name']} / {a['institution']} / "
              f"{(a['research_title'] or '')[:70]}")


# =============================================================================
# Download
# =============================================================================

def download_awardees(output_dir: Path, limit: Optional[int]) -> Path:
    print("\n" + "=" * 60)
    print("Step 1: Enumerate post-sitemap + scrape year-program pages")
    print("=" * 60)
    pages = enumerate_program_pages()
    if limit:
        pages = pages[:limit]
        print(f"  [LIMIT] only scraping first {limit} year-program pages")
    print(f"  fetching {len(pages)} year-program pages")
    all_rows: list[dict] = []
    for i, page in enumerate(pages, 1):
        try:
            resp = _http_get(page["url"])
            if resp.status_code != 200:
                print(f"  [{i}/{len(pages)}] {page['url']} -> HTTP {resp.status_code}; skipping")
                continue
            awardees = parse_page(resp.text)
        except Exception as e:
            print(f"  [{i}/{len(pages)}] {page['url']} -> error {e}")
            continue
        for a in awardees:
            all_rows.append({
                **a,
                "year":         page["year"],
                "scheme":       page["scheme"],
                "scheme_label": page["scheme_label"],
                "funding_type": page["funding_type"],
                "amount":       page["amount"],
                "currency":     page["currency"],
                "landing_page_url": page["url"],
            })
        print(f"  [{i}/{len(pages)}] {page['scheme']} {page['year']}: +{len(awardees)} rows (total {len(all_rows)})")
    raw_path = output_dir / "dreyfus_raw.json"
    raw_path.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2))
    print(f"\n  cached {len(all_rows)} records to {raw_path}")
    return raw_path


# =============================================================================
# Build DataFrame
# =============================================================================

# Name-split idiom shared with Holberg/CIFAR/Damon Runyon/Nuffield.
_DEGREE_SUFFIXES = {"PhD", "Ph.D.", "MD", "M.D.", "DPhil", "Jr.", "Jr", "Sr.", "Sr",
                    "II", "III", "IV", "MPH", "MSc"}
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
        name = r.get("name") or ""
        year = r.get("year")
        scheme = r.get("scheme") or ""
        slug = _slugify(name)
        funder_award_id = f"dreyfus-{scheme}-{year}-{slug}"
        # Same-name collisions across years are expected (e.g. recipient
        # gets Camille then Supplemental); the year/scheme prefix dedups.
        # Same-name within one year would be unusual but we still bail
        # rather than collapse two records silently.
        if funder_award_id in seen_ids:
            print(f"  [WARN] duplicate funder_award_id {funder_award_id!r}; skipping later record")
            continue
        seen_ids.add(funder_award_id)
        given, family = split_name(name)
        rows.append({
            "funder_award_id":   funder_award_id,
            "slug":              slug,
            "scheme":            scheme,
            "scheme_label":      r.get("scheme_label"),
            "funding_type":      r.get("funding_type"),
            "year":              year,
            "name":              name,
            "given_name":        given or None,
            "family_name":       family or None,
            "institution":       r.get("institution"),
            "research_title":    r.get("research_title"),
            "profile_url":       r.get("profile_url"),
            "amount":            r.get("amount"),
            "currency":          r.get("currency"),
            "landing_page_url":  r.get("landing_page_url"),
        })
    df = pd.DataFrame.from_records(rows)
    n_nm = df["name"].astype(bool).sum()
    n_inst = df["institution"].astype(bool).sum()
    n_title = df["research_title"].astype(bool).sum()
    n_amt = df["amount"].notna().sum()
    n_url = df["profile_url"].astype(bool).sum()
    print(f"  rows: {len(df)}")
    print(f"  coverage: name={n_nm} ({n_nm*100/len(df):.0f}%) "
          f"institution={n_inst} ({n_inst*100/len(df):.0f}%) "
          f"title={n_title} ({n_title*100/len(df):.0f}%) "
          f"amount={n_amt} ({n_amt*100/len(df):.0f}%) "
          f"profile_url={n_url} ({n_url*100/len(df):.0f}%)")
    print(f"  year range: {df['year'].min()} – {df['year'].max()}")
    print(f"\n  By scheme:")
    print(df.groupby("scheme").size().to_string())
    print(f"\n  Top 10 institutions:")
    print(df["institution"].value_counts().head(10).to_string())
    # Runbook §1.2.5 — astype("string") immediately before parquet write
    # (prevents pyarrow int-inference on all-null columns; precedent walden 5f694b7).
    df = df.astype("string")
    return df


# =============================================================================
# Parquet + S3 (with §1.4 shrink-check)
# =============================================================================

def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    print("\n" + "=" * 60)
    print("Step 3: Write parquet")
    print("=" * 60)
    parquet_path = output_dir / "dreyfus_awardees.parquet"
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
    prev_path = output_dir / "_prev_dreyfus_awardees.parquet"
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
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/dreyfus"))
    parser.add_argument("--skip-download", action="store_true",
                        help="Reuse dreyfus_raw.json in output-dir")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Don't push parquet to S3")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only scrape first N year-program pages (smoke-test)")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override §1.4 shrink-check")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("Camille and Henry Dreyfus Foundation → S3 Pipeline")
    print("=" * 60)
    print(f"  Output dir: {args.output_dir.absolute()}")
    print(f"  S3 dest:    s3://{S3_BUCKET}/{S3_KEY}")
    print(f"  Started:    {datetime.now(timezone.utc).isoformat()}")

    smoke_test()

    if args.skip_download:
        raw_path = args.output_dir / "dreyfus_raw.json"
        if not raw_path.exists():
            print(f"[ERROR] --skip-download given but {raw_path} missing")
            sys.exit(6)
        print(f"\n[SKIP] reusing {raw_path}")
    else:
        raw_path = download_awardees(args.output_dir, args.limit)

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
    print(f"Next: notebooks/awards/CreateDreyfusAwards.ipynb in Databricks")
    print("=" * 60)


if __name__ == "__main__":
    main()
