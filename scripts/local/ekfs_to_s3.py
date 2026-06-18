#!/usr/bin/env python3
"""
EKFS (Else Kroener-Fresenius-Stiftung) currently-funded scientific projects -> parquet -> S3.

Funder : Else Kroener-Fresenius-Stiftung
funder_id : F4320321672  (numeric 4320321672)
ROR : 03zcxha54   country: DE   currency: EUR
Provenance slug : ekfs

Source (English UI, paginated list -> per-project detail pages):
  https://ekfs.de/en/scientific-funding/currently-funded-projects?page=0 .. page=53
  ~644 projects (53 full pages of 12 + a short last page).

Per-project fields scraped:
  title         <h1> of the detail page (== <title> prefix)
  institution   teaser line "Institution: ..."
  pi_full       teaser line "Applicant: ..."  (PI; may list joint applicants)
  scheme        structured field "Funding line: ..."
  description   first substantive <body> field (abstract)
  amount        NOT published by EKFS -> null (§6.7 amount-waiver)
  start/end     EKFS does NOT publish project term/duration on these pages -> null
  funder_award_id  "ekfs-<slug>"  (synthetic; no native grant number)
  landing_page_url full https URL of the detail page

Output: /tmp/ekfs_grants.parquet  (13 columns, ALL string/nullable dtype).
Completeness guard: harvested vs advertised (~644); hard-fail if short.
On guard-pass and not --skip-upload:
  aws s3 cp <out>/ekfs_grants.parquet s3://openalex-ingest/awards/ekfs/ekfs_grants.parquet
"""

import argparse
import os
import re
import sys
import time
import random

import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE = "https://ekfs.de/en/scientific-funding/currently-funded-projects"
DETAIL_RE = re.compile(r"^/en/scientific-funding/currently-funded-projects/([^/?#]+)$")
S3_DEST = "s3://openalex-ingest/awards/ekfs/ekfs_grants.parquet"
OUT_NAME = "ekfs_grants.parquet"

# Date format actually emitted in start_date_raw / end_date_raw when present.
# (The downstream notebook does TRY_TO_DATE(.., 'dd.MM.yyyy').)
DATE_FORMAT_EMITTED = "dd.MM.yyyy"  # i.e. literal "DD.MM.YYYY" strings; currently always null

COLUMNS = [
    "funder_award_id", "title", "pi_full", "pi_given", "pi_family",
    "institution", "amount", "currency", "scheme",
    "start_date_raw", "end_date_raw", "description", "landing_page_url",
]

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# Academic titles / honorifics to strip from the front of a PI name.
# Order matters (longest / compound first). Applied iteratively.
TITLE_TOKENS = [
    r"Univ\.?[-\s]?Prof\.?", r"Prof\.?", r"Priv\.?[-\s]?Doz\.?", r"PD",
    r"Dr\.?\s*med\.?\s*dent\.?", r"Dr\.?\s*med\.?\s*vet\.?", r"Dr\.?\s*med\.?",
    r"Dr\.?\s*rer\.?\s*nat\.?", r"Dr\.?\s*phil\.?", r"Dr\.?\s*-?\s*Ing\.?",
    r"Dr\.?", r"Mag\.?", r"Dipl\.?[-\s]?[A-Za-zäöü]+\.?",
]
# Trailing post-nominal degrees to drop (so they don't end up in pi_family).
TRAILING_DEGREES = re.compile(
    r"[,;]?\s*\b("
    r"M\.?\s?Sc\.?|MSc|B\.?\s?Sc\.?|BSc|Ph\.?\s?D\.?|PhD|M\.?D\.?|MD|"
    r"M\.?\s?A\.?|MA|MPH|FRCP|FRCS|DPhil|habil\.?"
    r")\b[./]?",
    re.I,
)
# Multi-applicant separators: "&", " und ", ";", " and ", "/" (only as people-sep, rare), ","
MULTI_SPLIT = re.compile(r"\s*(?:&|;|\bund\b|\band\b)\s*", re.I)


def strip_titles(name: str) -> str:
    """Remove leading honorifics/degrees and trailing post-nominals from a single name."""
    n = name.strip()
    changed = True
    while changed:
        changed = False
        for tok in TITLE_TOKENS:
            m = re.match(r"^\s*" + tok + r"\s+", n)
            if m:
                n = n[m.end():]
                changed = True
                break
    # strip trailing degree post-nominals (possibly several)
    prev = None
    while prev != n:
        prev = n
        n = TRAILING_DEGREES.sub("", n).strip().strip(",;").strip()
    return n.strip()


def split_pi(pi_full: str):
    """
    Return (pi_given, pi_family) for the LEAD applicant only.
    Multi-PI strings keep the full roster in pi_full; the given/family split
    uses the first listed person.
    """
    if not pi_full:
        return None, None
    # take the first applicant when several are listed
    lead = MULTI_SPLIT.split(pi_full.strip())[0].strip()
    # a trailing comma can also separate co-applicants (e.g. "A Popp, B Guntinas")
    # but commas also appear inside post-nominals; strip degrees first, then,
    # if a residual comma remains, treat the part before it as the lead name.
    clean = strip_titles(lead)
    if "," in clean:
        clean = clean.split(",", 1)[0].strip()
    clean = strip_titles(clean)  # second pass in case a title followed a comma
    if not clean:
        return None, None
    parts = clean.split()
    if len(parts) == 1:
        return None, parts[0]
    given = parts[0]
    family = parts[-1]
    return given, family


def get_with_retry(sess, url, *, params=None, tries=4, base_delay=1.0):
    last = None
    for attempt in range(tries):
        try:
            r = sess.get(url, params=params, timeout=40)
            if r.status_code == 200:
                return r
            last = f"HTTP {r.status_code}"
            # backoff on 429/5xx
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(base_delay * (2 ** attempt) + random.uniform(0, 0.5))
                continue
            return r  # other status: return as-is, caller decides
        except requests.RequestException as e:
            last = str(e)
            time.sleep(base_delay * (2 ** attempt) + random.uniform(0, 0.5))
    raise RuntimeError(f"GET failed after {tries} tries: {url} ({last})")


def discover_last_page(sess) -> int:
    """Read page 0, follow the 'Last page' pager link to learn the final page index."""
    r = get_with_retry(sess, BASE, params={"page": 0})
    soup = BeautifulSoup(r.text, "html.parser")
    last = 0
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]page=(\d+)", a["href"])
        if m:
            last = max(last, int(m.group(1)))
    return last


def list_slugs(sess, page: int):
    r = get_with_retry(sess, BASE, params={"page": page})
    soup = BeautifulSoup(r.text, "html.parser")
    slugs = []
    for a in soup.find_all("a", href=True):
        m = DETAIL_RE.match(a["href"])
        if m:
            slugs.append(m.group(1))
    # preserve order, dedupe within page
    return list(dict.fromkeys(slugs))


def parse_detail(html: str, slug: str):
    soup = BeautifulSoup(html, "html.parser")

    # ---- title: prefer the content <h1> that matches the <title> prefix ----
    title = None
    if soup.title:
        title = soup.title.get_text(strip=True).split("|")[0].strip()
    # fall back / verify against an <h1> that isn't a nav landmark
    if not title:
        for h in soup.find_all("h1"):
            t = h.get_text(" ", strip=True)
            if t and t.lower() not in ("header navigation",):
                title = t
                break

    # ---- institution + applicant from teasertext ----
    institution = None
    pi_full = None
    for d in soup.find_all("div", class_=re.compile(r"field--name-field-base-teasertext")):
        for ln in d.get_text("\n", strip=True).split("\n"):
            ln = ln.strip()
            low = ln.lower()
            if low.startswith("institution"):
                institution = ln.split(":", 1)[1].strip() if ":" in ln else None
            elif low.startswith("applicant"):
                pi_full = ln.split(":", 1)[1].strip() if ":" in ln else None
        break

    # ---- scheme: structured "Funding line:" field ----
    scheme = None
    for div in soup.find_all("div", class_="field"):
        lab = div.find(class_="field__label", recursive=False)
        item = div.find(class_="field__item", recursive=False)
        if lab and item and "funding line" in lab.get_text(" ", strip=True).lower():
            scheme = item.get_text(" ", strip=True) or None
            break

    # ---- description: first substantive body field (the abstract) ----
    description = None
    for d in soup.find_all("div", class_=re.compile(r"field--name-body")):
        t = d.get_text(" ", strip=True)
        if t and len(t) > 80 and "Newsletter" not in t and "Rathausplatz" not in t \
                and "Phone +49" not in t:
            description = t
            break

    pi_given, pi_family = split_pi(pi_full) if pi_full else (None, None)

    return {
        "funder_award_id": f"ekfs-{slug}",
        "title": title,
        "pi_full": pi_full,
        "pi_given": pi_given,
        "pi_family": pi_family,
        "institution": institution,
        "amount": None,        # §6.7 amount-waiver: EKFS does not publish amounts
        "currency": None,
        "scheme": scheme,
        "start_date_raw": None,  # EKFS does not publish project term on these pages
        "end_date_raw": None,
        "description": description,
        "landing_page_url": f"{BASE}/{slug}",
    }


def scrape(sess, limit=None, polite=0.5):
    last_page = discover_last_page(sess)
    print(f"[pager] last page index = {last_page} (pages 0..{last_page} => {last_page + 1} pages)")

    # gather all slugs in listing order
    all_slugs = []
    per_page = {}
    for pg in range(last_page + 1):
        slugs = list_slugs(sess, pg)
        per_page[pg] = len(slugs)
        all_slugs.extend(slugs)
        if pg in (0, last_page) or pg % 10 == 0:
            print(f"[list] page {pg:2d}: {len(slugs)} projects (running total {len(all_slugs)})")
        time.sleep(polite + random.uniform(0, 0.3))

    # advertised total = sum of project links across all listing pages
    advertised = len(all_slugs)
    uniq_slugs = list(dict.fromkeys(all_slugs))
    dupes_in_listing = advertised - len(uniq_slugs)
    print(f"[list] advertised project links across listing = {advertised} "
          f"(unique slugs = {len(uniq_slugs)}, dup links in listing = {dupes_in_listing})")

    if limit:
        uniq_slugs = uniq_slugs[:limit]
        print(f"[limit] --limit {limit} -> scraping {len(uniq_slugs)} detail pages")

    rows = []
    for i, slug in enumerate(uniq_slugs, 1):
        r = get_with_retry(sess, f"{BASE}/{slug}")
        if r.status_code != 200:
            print(f"  !! {slug}: HTTP {r.status_code} (skipped)")
            continue
        row = parse_detail(r.text, slug)
        rows.append(row)
        if i % 50 == 0 or i == len(uniq_slugs):
            print(f"[detail] {i}/{len(uniq_slugs)} scraped")
        time.sleep(polite + random.uniform(0, 0.25))

    return rows, advertised, len(uniq_slugs)


def main():
    ap = argparse.ArgumentParser(
        description="Scrape EKFS currently-funded scientific projects to a parquet and upload to S3.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--limit", type=int, default=None,
                    help="Only scrape the first N detail pages (smoke test; bypasses completeness guard).")
    ap.add_argument("--skip-upload", action="store_true",
                    help="Build the parquet but do not upload to S3.")
    ap.add_argument("--skip-download", action="store_true",
                    help="Skip scraping; re-use an existing parquet in --output-dir (e.g. to re-upload).")
    ap.add_argument("--output-dir", default="/tmp",
                    help="Directory to write/read the parquet.")
    ap.add_argument("--allow-shrink", action="store_true",
                    help="(§1.4) Permit uploading even if the new row count is smaller than the "
                         "current S3 object's row count.")
    ap.add_argument("--polite-delay", type=float, default=0.5,
                    help="Base seconds to sleep between HTTP requests (jitter added).")
    ap.add_argument("--expected-min", type=int, default=600,
                    help="Completeness floor: hard-fail if fewer unique projects are harvested.")
    args = ap.parse_args()

    # honor SKIP_UPLOAD env as well as the flag
    skip_upload = args.skip_upload or os.environ.get("SKIP_UPLOAD", "").strip().lower() in ("1", "true", "yes")

    out_path = os.path.join(args.output_dir, OUT_NAME)
    os.makedirs(args.output_dir, exist_ok=True)

    if args.skip_download:
        if not os.path.exists(out_path):
            print(f"FATAL: --skip-download but {out_path} does not exist", file=sys.stderr)
            sys.exit(2)
        df = pd.read_parquet(out_path)
        print(f"[skip-download] loaded {len(df)} rows from {out_path}")
        advertised = harvested = len(df)
    else:
        sess = requests.Session()
        sess.headers.update({"User-Agent": UA, "Accept-Language": "en"})
        rows, advertised, harvested = scrape(sess, limit=args.limit, polite=args.polite_delay)

        # ---- dedup by funder_award_id ----
        df = pd.DataFrame(rows, columns=COLUMNS)
        before = len(df)
        df = df.drop_duplicates(subset=["funder_award_id"], keep="first").reset_index(drop=True)
        print(f"[dedup] {before} -> {len(df)} rows after dedup on funder_award_id")

        # force every column to nullable string dtype
        for c in COLUMNS:
            df[c] = df[c].astype("string")

        # ---- completeness guard (skip when --limit smoke test) ----
        if args.limit is None:
            print(f"[guard] harvested unique projects = {len(df)}; advertised (listing) = {advertised}; "
                  f"floor = {args.expected_min}")
            if len(df) < args.expected_min:
                print(f"FATAL completeness guard: harvested {len(df)} < floor {args.expected_min} "
                      f"(advertised {advertised}). Aborting; nothing written/uploaded.",
                      file=sys.stderr)
                sys.exit(3)
            # also fail if we lost more than a couple of projects vs. the listing
            if len(df) < advertised - 2:
                print(f"FATAL completeness guard: harvested {len(df)} is short of advertised "
                      f"{advertised} by >2. Aborting.", file=sys.stderr)
                sys.exit(3)
        else:
            print("[guard] --limit set: completeness guard SKIPPED (smoke test).")

        df.to_parquet(out_path, index=False)
        print(f"[write] wrote {len(df)} rows -> {out_path}")

    # ---- field fill-rate report ----
    print("\n=== field fill rates ===")
    n = len(df)
    for c in COLUMNS:
        non_null = df[c].notna().sum()
        pct = (100.0 * non_null / n) if n else 0.0
        print(f"  {c:18s} {non_null:5d}/{n}  {pct:6.2f}%")

    # ---- §1.4 shrink-check vs existing S3 object ----
    if not skip_upload:
        try:
            import subprocess, tempfile
            with tempfile.TemporaryDirectory() as td:
                prev = os.path.join(td, "prev.parquet")
                rc = subprocess.run(["aws", "s3", "cp", S3_DEST, prev],
                                    capture_output=True, text=True)
                if rc.returncode == 0 and os.path.exists(prev):
                    prev_n = len(pd.read_parquet(prev))
                    print(f"\n[shrink-check] current S3 rows = {prev_n}; new rows = {n}")
                    if n < prev_n and not args.allow_shrink:
                        print(f"FATAL shrink-check: new {n} < existing {prev_n} on S3. "
                              f"Pass --allow-shrink to override. Not uploading.", file=sys.stderr)
                        sys.exit(4)
                else:
                    print("\n[shrink-check] no existing S3 object (first upload) - skipping shrink check.")
        except Exception as e:
            print(f"[shrink-check] warning: could not check existing object ({e}); proceeding.")

        print(f"\n[upload] aws s3 cp {out_path} {S3_DEST}")
        import subprocess
        rc = subprocess.run(["aws", "s3", "cp", out_path, S3_DEST])
        if rc.returncode != 0:
            print("FATAL: s3 upload failed", file=sys.stderr)
            sys.exit(5)
        print("[upload] done.")
    else:
        print("\n[upload] SKIP_UPLOAD/--skip-upload set -> not uploading.")

    print(f"\n[meta] date format emitted for start/end_date_raw = {DATE_FORMAT_EMITTED} "
          f"(currently always null - EKFS publishes no project term).")
    print("[done]")


if __name__ == "__main__":
    main()
