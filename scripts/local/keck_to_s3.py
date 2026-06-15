#!/usr/bin/env python3
"""
W. M. Keck Foundation to S3 Data Pipeline
=========================================

Scrapes the W. M. Keck Foundation's grant-abstract PDF archive and uploads a
parquet to S3 for Databricks ingestion. Keck is a US research funder on the
IAMHRF expanded list (OpenAlex funder F4320306159).

Data source: there is NO bulk grants CSV/Excel anywhere on wmkeck.org (checked
    the research-current-grantees page and site root for csv/xlsx links — none).
    The only structured source is the per-cycle "Abstracts" PDF archive, which
    the WordPress media REST API enumerates:
        https://www.wmkeck.org/wp-json/wp/v2/media?search=Abstracts&per_page=100
    -> ~47 PDF media records. Filenames encode program + funding cycle:
        Abstracts_D25_MR  ==  December 2025, Medical Research
        Abstracts_J24_SE  ==  June 2024, Science & Engineering
        SoCal_Abstracts_J20 == June 2020, Southern California
    Revised re-uploads carry vN suffixes (Abstracts_D22_MRv3 / _MRv4); dedupe by
    (program, cycle) keeping the media record with the newest `date`. That
    leaves 45 latest PDFs (one per program+cycle).

    `pdftotext -layout` yields clean, blank-line-delimited records anchored on a
    standalone "$<amount>" line. The three non-blank lines directly above the
    amount are, in order:
        <Institution>
        <City, ST>
        <Investigator names, comma-separated>   (Medical Research / Sci & Eng)
        -- OR --
        <Category e.g. "Civic & Community">      (Southern California; org-level)
    For Medical Research / Science & Engineering the line immediately AFTER the
    amount (one line, then a blank, then the long abstract paragraph) is the
    project Title. Southern California records jump straight to the abstract and
    carry no title and no PI (the third line above the amount is a funding
    category, not a person) — recipe confirms "SoCal may be org-level".

    No native grant id; synthesize keck-<year>-<md5(inst|pi|title)[:10]>. USD.

Gotchas handled here:
    - Cloudflare 403s the default requests UA on the HTML pages and rate-limits
      (429) rapid PDF fetches -> Chrome UA + slow throttled crawl with backoff.
    - A running page footer ("W. M. Keck Foundation December 2025 Medical
      Research Abstracts 2") splits abstracts/records across pages -> stripped.
    - Institutions can contain commas ("University of California, Berkeley") so
      the "City, ST" two-letter-state line is used as the structural anchor, not
      a naive comma split.

Output: s3://openalex-ingest/awards/keck/keck_grants.parquet
"""

import argparse
import hashlib
import re
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
MEDIA_API = "https://www.wmkeck.org/wp-json/wp/v2/media?search=Abstracts&per_page=100"
LANDING = "https://www.wmkeck.org/research-current-grantees/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/keck/keck_grants.parquet"

# funding cycle token: a D/J + 2-digit year between underscores or string edges.
# NB: \b does NOT work here because '_' is a regex word char (no boundary).
CYCLE_RE = re.compile(r"(?:^|_)([DJ])(\d{2})(?:_|$)")
# program token, tolerating vN / _2 / _emptyheader / Socal casing variants
PROG_RE = re.compile(
    r"(?:^|_)(MR|SE|SoCal|Socal)(?:v\d+)?(?:_\d+|_emptyheader)?(?:_|$)", re.I)
PROG_NAME = {"MR": "Medical Research",
             "SE": "Science & Engineering",
             "SoCal": "Southern California"}

AMOUNT_RE = re.compile(r"^\$\s*([\d,]+)\s*$")
# A "City, location" line: "Boston, MA" / "El Segundo, California" /
# "Washington, D.C." — a short line ending in a comma-separated 2-letter state,
# a period-spaced abbreviation, or a full US state/territory name.
US_STATES = (
    "Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|"
    "Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|"
    "Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|"
    "Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|New Mexico|"
    "New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|"
    "Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|"
    "Virginia|Washington|West Virginia|Wisconsin|Wyoming|D\\.C\\.")
CITY_RE = re.compile(
    r"^.{1,45},\s*(?:[A-Z]{2}\.?|D\.C\.|(?:%s))\.?$" % US_STATES)
# running page footer that interleaves records across page breaks
FOOTER_RE = re.compile(r"^W\.\s*M\.\s*Keck Foundation\b.*Abstracts\s*\d*\s*$")
# a "June 2021"-style cycle-date stamp that appears below the amount in older
# PDFs and must never be mistaken for a title
MONTHYEAR_RE = re.compile(
    r"^(January|February|March|April|May|June|July|August|September|October|"
    r"November|December)\s+20\d{2}$", re.I)
# SoCal funding categories (org-level metadata, NOT investigators). In newer
# PDFs the category is the line just below City, ST; in older PDFs it sits
# between the org name and City, ST. Either way it is stripped, never a PI.
# Matched by pattern so spelling drift ("Civic and Community Service(s)",
# "Precollegiate Education", "Health Care" — incl. the source typo "Heath Care")
# is caught without an exact list.
CATEGORY_RE = re.compile(
    r"^(Civic (and|&) Community|Arts (and|&) Culture|Heal?th( Care)?|"
    r"Education|Precollegiate Education|Early Childhood|Community|"
    r"Human Services|Environment|Science (and|&) Engineering|"
    r"Medical Research)\b.*$", re.I)


def parse_meta(stem):
    """Filename stem -> (program_code, program_name, cycle, year_awarded)."""
    cm = CYCLE_RE.search(stem)
    cycle = (cm.group(1) + cm.group(2)) if cm else None
    year = ("20" + cm.group(2)) if cm else None
    pm = PROG_RE.search(stem)
    code = {"mr": "MR", "se": "SE", "socal": "SoCal"}.get(
        pm.group(1).lower()) if pm else None
    return code, PROG_NAME.get(code), cycle, year


def parse_pi(raw):
    """'Nicholas Polizzi, Edward Boyden' -> first PI ('Nicholas','Polizzi').

    Keck lists one or more investigators on a single line, separated by commas
    and/or ampersands ('Andrej Luptak, Jennifer Prescher & Oswald Stewart'). We
    attribute the record to the FIRST named investigator (lead) for pi_given /
    pi_family, mirroring the one-PI-per-row convention of sibling scrapers.
    """
    if not raw:
        return None, None
    first = re.split(r"\s*[,&]\s*", raw, maxsplit=1)[0].strip()
    first = re.sub(r"\s+", " ", first)
    if not first:
        return None, None
    parts = first.split()
    if len(parts) < 2:
        return None, first
    return " ".join(parts[:-1]), parts[-1]


def _is_category(line):
    """True for an org-level SoCal funding category (not an investigator)."""
    return bool(CATEGORY_RE.match(line.strip()))


def parse_pdf(path, code, year):
    """Extract one record per standalone $amount line."""
    txt = subprocess.run(["pdftotext", "-layout", str(path), "-"],
                         capture_output=True, text=True).stdout
    lines = [l.rstrip() for l in txt.split("\n")]
    # drop running footers so cross-page abstracts/records line up cleanly
    lines = [l for l in lines if not FOOTER_RE.match(l.strip())]

    amt_idx = [i for i, l in enumerate(lines) if AMOUNT_RE.match(l.strip())]
    rows = []
    for i in amt_idx:
        amount = AMOUNT_RE.match(lines[i].strip()).group(1).replace(",", "")

        # collect the CONTIGUOUS non-blank block immediately above the amount,
        # stopping at the blank line that separates it from the prior abstract.
        # That block is exactly the record header in every layout:
        #   newer:  Institution / City,ST / (Investigators | Category) / $amt
        #   older:  Institution / Category / City,ST / $amt
        # (stopping at the blank avoids dragging in the previous record's
        # trailing abstract words, which a fixed-count walk would do.)
        above, j = [], i - 1
        while j >= 0 and lines[j].strip():
            above.append(lines[j].strip())
            j -= 1
        above = above[::-1]
        if len(above) < 2:
            continue

        # pull out the org-level category (if any) so it never pollutes the
        # institution or gets read as an investigator
        program_category = next((s for s in above if _is_category(s)), None)
        above = [s for s in above if not _is_category(s)]

        # Anchor on the City line. Institution is the SINGLE line directly above
        # the city (Keck org names are one line); taking only that line — rather
        # than everything above — protects against a previous record's trailing
        # abstract prose when pdftotext drops the separating blank. The PI line
        # (if any) sits directly below the city.
        city = institution = pi_line = None
        for k, s in enumerate(above):
            if CITY_RE.match(s):
                city = s
                institution = above[k - 1].strip() if k >= 1 else None
                if k + 1 < len(above):
                    pi_line = above[k + 1].strip()
                break
        if not city:
            # no recognizable city (one bare-"Los Angeles" SoCal record): the
            # org name is the first block line; do NOT treat the leftover as a
            # PI (it is a place/category, never an investigator).
            institution = above[0] if above else None
            pi_line = None
        if (not institution or "total" in institution.lower()
                or _is_category(institution)):
            continue

        # PI is investigator-only: present on Medical Research / Science &
        # Engineering records, absent on Southern California (org-level grants).
        given = family = None
        if pi_line and code != "SoCal":
            given, family = parse_pi(pi_line)

        # title: the single line after the amount, set off by a blank line, then
        # the abstract paragraph. SoCal jumps straight into a multi-line abstract
        # (no title) -> only treat a following line as a title when the line two
        # below it (post-blank) starts a new paragraph.
        title = None
        k = i + 1
        while k < len(lines) and not lines[k].strip():
            k += 1
        if k < len(lines):
            cand = lines[k].strip()
            nxt = lines[k + 1].strip() if k + 1 < len(lines) else ""
            # a title is a standalone short line followed by a blank line; an
            # abstract's first line is followed immediately by more prose. Older
            # PDFs stamp a "June 2021" cycle line here (and carry no title), so
            # reject month-year stamps and amounts.
            if (cand and not nxt and not AMOUNT_RE.match(cand)
                    and not MONTHYEAR_RE.match(cand)):
                title = re.sub(r"\s+", " ", cand)

        key = f"{year}|{institution}|{pi_line or ''}|{title or ''}".lower()
        aid = "keck-%s-%s" % (year, hashlib.md5(key.encode()).hexdigest()[:10])
        rows.append({
            "funder_award_id": aid,
            "title": title,
            "pi_given": given,
            "pi_family": family,
            "institution": institution,
            "city": city,
            "amount": amount,
            "program": PROG_NAME.get(code),
            "year_awarded": year,
            "landing_page_url": LANDING,
        })
    return rows


def latest_pdfs(session):
    """WP media REST -> [(url, stem, code, year)] deduped to newest per cycle."""
    data = session.get(MEDIA_API, timeout=60).json()
    by_key = {}
    for m in data:
        url = m.get("source_url", "")
        if not url.lower().endswith(".pdf"):
            continue
        stem = url.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        code, _, cycle, year = parse_meta(stem)
        if not code or not cycle:
            print(f"  [skip] unparseable media name: {stem}")
            continue
        date = m.get("date", "")
        prev = by_key.get((code, cycle))
        if prev is None or date > prev[0]:
            by_key[(code, cycle)] = (date, url, stem, code, year)
    return [(u, st, c, y) for (_, u, st, c, y) in sorted(by_key.values())]


def upload_to_s3(local_path: Path, bucket: str, key: str) -> bool:
    s3_uri = f"s3://{bucket}/{key}"
    print(f"\nUploading to {s3_uri}...")
    try:
        subprocess.run(["aws", "s3", "cp", str(local_path), s3_uri],
                       capture_output=True, text=True, check=True)
        print(f"Upload complete: {s3_uri}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Upload failed: {e.stderr}")
        return False


def main():
    ap = argparse.ArgumentParser(description="W. M. Keck Foundation grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/keck_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--delay", type=float, default=4.0,
                    help="seconds between PDF fetches (Cloudflare-friendly)")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("W. M. Keck Foundation grant abstracts -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"User-Agent": UA})

    pdfs = latest_pdfs(s)
    print(f"WP media: {len(pdfs)} latest PDFs after revision dedupe "
          f"(expected ~45)\n")
    if len(pdfs) < 40:
        print(f"[WARN] only {len(pdfs)} PDFs enumerated — media API changed?")

    rows, seen = [], set()
    failed = []
    for n, (url, stem, code, year) in enumerate(pdfs, 1):
        local = args.output_dir / (stem + ".pdf")
        try:
            for attempt in range(4):
                r = s.get(url, timeout=90)
                if r.status_code == 200:
                    break
                wait = args.delay * (2 ** attempt)
                print(f"  [{stem}] HTTP {r.status_code}; backoff {wait:.0f}s")
                time.sleep(wait)
            else:
                failed.append((stem, f"HTTP {r.status_code}"))
                continue
            local.write_bytes(r.content)
            recs = parse_pdf(local, code, year)
        except Exception as e:
            failed.append((stem, str(e)[:80]))
            print(f"  [{stem}] ERROR: {e}")
            continue
        kept = 0
        for rec in recs:
            if rec["funder_award_id"] not in seen:
                seen.add(rec["funder_award_id"])
                rows.append(rec)
                kept += 1
        print(f"  [{n:>2}/{len(pdfs)}] {stem:<28} {PROG_NAME.get(code):<22} "
              f"{kept:>3} records")
        if n < len(pdfs):
            time.sleep(args.delay)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "city"):
        if c in df:
            nn = df[c].notna().sum()
            print(f"  {c:<12}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    if "program" in df and len(df):
        print("  by program:")
        for prog, cnt in df["program"].value_counts().items():
            print(f"      {prog:<22} {cnt}")

    if failed:
        print(f"\n[WARN] {len(failed)} PDF(s) failed:")
        for stem, why in failed:
            print(f"      {stem}: {why}")

    # HARD GATE — never write/upload a partial crawl. Failed fetches (e.g. 429s)
    # or a short crawl would shrink the S3 parquet and, via the notebook's
    # provenance-scoped DELETE+INSERT, the published raw table on re-ingest.
    # Re-run instead (raise --delay if these are rate-limits).
    if failed or len(df) < 300:
        print(f"\n[ERROR] refusing to write a partial dataset: "
              f"{len(failed)} failed fetch(es), {len(df)} rows (expected ~318). "
              f"Re-run; raise --delay if 429s.")
        sys.exit(1)

    out = args.output_dir / "keck_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
