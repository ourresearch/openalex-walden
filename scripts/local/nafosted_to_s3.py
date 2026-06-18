#!/usr/bin/env python3
"""
NAFOSTED (Vietnam) — National Foundation for Science & Technology Development.
Awards scraper for the OpenAlex Walden awards pipeline.

OpenAlex funder_id : F4320309617 (numeric 4320309617)
ROR                : 04rw64z44
Country            : VN
Provenance slug    : nafosted

Source (public, no-login, server-rendered HTML, ~7,083 results):
  https://dichvucong.nafosted.gov.vn/tra-cuu-de-tai.php?l=1&search=1&page=N
  Pagination: step `page` by 30 (30 rows/page). page=0..7080 -> 237 pages.
  page=7080 returns the final 3 rows (7080+3 = 7083); page=7110 returns 0 rows
  (clean end, no truncation cap observed 2026-06-18).

The portal migrates to dichvucong.mst.gov.vn in Oct-2025 — harvest now.

Per-row table columns (`table#table > tbody > tr`, 9 <td>):
  0 STT  1 Ma de tai (grant code)  2 Ten de tai (title)  3 Chuong trinh (program)
  4 Nganh/Linh vuc (field)  5 Chu nhiem de tai (PI)  6 Thoi gian thuc hien
  (duration months + "DD/MM/YYYY-DD/MM/YYYY" date range)  7 To chuc chu tri
  (institution)  8 Trang thai (status)

Output (13 cols, all string/nullable, null where absent):
  funder_award_id, title, pi_full, pi_given, pi_family, institution, amount,
  currency, scheme, start_date_raw, end_date_raw, description, landing_page_url

Notes vs the brief:
  - amount/currency : null (genuinely not published; §6.7 amount-waiver).
  - start/end dates : the lookup DOES expose a "DD/MM/YYYY-DD/MM/YYYY" range in
    the duration cell, so we capture real start_date_raw/end_date_raw from it
    (preferred over null / over fabricating from the program year).
  - landing_page_url: no per-row detail links exist, so this is the search URL.
"""

import argparse
import io
import os
import re
import sys
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ----------------------------------------------------------------------------- config
BASE = "https://dichvucong.nafosted.gov.vn/tra-cuu-de-tai.php"
SEARCH_URL = f"{BASE}?l=1&search=1"          # canonical landing/search URL
PAGE_STEP = 30                               # rows per page (page param increment)
ADVERTISED_FALLBACK = 7083                   # used only if the count can't be parsed
PROVENANCE = "nafosted"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")

S3_DEST = "s3://openalex-ingest/awards/nafosted/nafosted_grants.parquet"

COLUMNS = [
    "funder_award_id", "title", "pi_full", "pi_given", "pi_family",
    "institution", "amount", "currency", "scheme",
    "start_date_raw", "end_date_raw", "description", "landing_page_url",
]

# Vietnamese academic-title tokens to strip from the front of a PI name.
# Matched case-insensitively, with or without trailing dots, possibly chained
# (e.g. "GS.TS.", "PGS.TS", "TS", "ThS.", "KS"). Order longest-first not needed
# because we anchor and loop. NCS = grad-student researcher; BS = bac si (MD);
# included defensively though rare in this dataset.
_TITLE_TOKENS = [
    "gsts", "pgsts", "gstskh", "tskh",
    "gs", "pgs", "ts", "ths", "ncs", "ks", "bs", "cn",
]
# Regex: one-or-more leading title tokens, where each token is letters optionally
# separated/terminated by '.' and whitespace. We normalise by removing '.' for
# the membership test.
_LEADING_TITLE_RE = re.compile(
    r"^(?:\s*(?:gs|pgs|ts|kh|ths|ncs|ks|bs|cn)\s*\.?\s*)+",
    re.IGNORECASE,
)

_DATE_RANGE_RE = re.compile(
    r"(\d{1,2}/\d{1,2}/\d{4})\s*[-–]\s*(\d{1,2}/\d{1,2}/\d{4})"
)
_ADVERTISED_RE = re.compile(r"<b>\s*\((\d+)\)\s*</b>")


# ----------------------------------------------------------------------------- helpers
def _clean(s):
    """Collapse whitespace; return None for empty."""
    if s is None:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def strip_pi_titles(raw):
    """
    Strip leading Vietnamese academic-title tokens (GS./PGS./TS./ThS./KS. ...)
    from a PI string and return the cleaned personal name.
    """
    if not raw:
        return None
    name = raw.strip()
    # Iteratively peel leading title tokens (handles "GS.TS.", "PGS. TS", etc.)
    prev = None
    while name and name != prev:
        prev = name
        stripped = _LEADING_TITLE_RE.sub("", name).strip()
        # Guard: don't strip into oblivion (e.g. a name that *is* "Ts ..." only).
        if stripped and stripped != name:
            name = stripped
    return _clean(name) or _clean(raw)


def split_vietnamese_name(full):
    """
    Split a Vietnamese personal name into (given, family).

    Vietnamese names are written Family-Given order, so the FIRST token is the
    family name and the remaining tokens are the given name (which in Vietnamese
    usage includes any middle name and is the part people are addressed by).

    Single-token names -> family only, given=None.
    """
    if not full:
        return (None, None)
    toks = full.split()
    if len(toks) == 1:
        return (None, toks[0])
    family = toks[0]
    given = " ".join(toks[1:])
    return (_clean(given), _clean(family))


def parse_dates(duration_cell_text):
    """Extract (start_date_raw, end_date_raw) as DD/MM/YYYY strings, or (None,None)."""
    if not duration_cell_text:
        return (None, None)
    m = _DATE_RANGE_RE.search(duration_cell_text)
    if not m:
        return (None, None)
    return (m.group(1), m.group(2))


def parse_advertised(html):
    m = _ADVERTISED_RE.search(html)
    if m:
        return int(m.group(1))
    # looser fallback: "Danh sách tìm kiếm: (7083) kết quả"
    m = re.search(r"kết quả", html)
    m2 = re.search(r"\((\d{2,6})\)", html)
    return int(m2.group(1)) if m2 else None


def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept-Language": "vi,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s


def fetch_page(session, page, retries=5, backoff=2.0, timeout=60):
    """Fetch one results page (page = 0,30,60,...). Returns HTML text."""
    url = f"{BASE}?l=1&search=1&page={page}"
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code == 200 and r.text:
                r.encoding = r.apparent_encoding or "utf-8"
                return r.text
            last_err = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last_err = str(e)
        sleep = backoff * attempt
        print(f"  ! page={page} attempt {attempt}/{retries} failed ({last_err}); "
              f"retry in {sleep:.1f}s", file=sys.stderr)
        time.sleep(sleep)
    raise RuntimeError(f"page={page} failed after {retries} attempts: {last_err}")


def parse_rows(html):
    """Parse the results table of one page into a list of dicts (raw cell text)."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="table")
    if not table:
        return []
    tbody = table.find("tbody")
    if not tbody:
        return []
    out = []
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 9:
            continue
        # Use get_text with a separator so the <br> in the duration cell becomes a space.
        def cell(i):
            return tds[i].get_text(" ", strip=True)
        out.append({
            "stt": cell(0),
            "code": cell(1),
            "title": cell(2),
            "program": cell(3),
            "field": cell(4),
            "pi": cell(5),
            "duration": cell(6),
            "institution": cell(7),
            "status": cell(8),
        })
    return out


def build_record(raw, page, idx, synth_counter):
    """Map one raw row dict to the 13-column output contract."""
    code = _clean(raw["code"])
    if not code:
        synth_counter[0] += 1
        code = f"NAFOSTED-SYNTH-p{page}-i{idx}"

    title = _clean(raw["title"])
    program = _clean(raw["program"])
    field = _clean(raw["field"])
    status = _clean(raw["status"])
    institution = _clean(raw["institution"])

    pi_full = _clean(raw["pi"])
    pi_name = strip_pi_titles(pi_full) if pi_full else None
    pi_given, pi_family = split_vietnamese_name(pi_name)

    start_raw, end_raw = parse_dates(raw["duration"])

    # Derive a year for the description (from end-of-grant-code patterns or dates).
    year = None
    if code:
        ym = re.search(r"(20\d{2})", code)
        if ym:
            year = ym.group(1)
    if not year and start_raw:
        ym = re.search(r"/(20\d{2})$", start_raw)
        if ym:
            year = ym.group(1)

    # description = pack status + program + year (+ field for context if present)
    desc_bits = []
    if status:
        desc_bits.append(f"Status: {status}")
    if program:
        desc_bits.append(f"Program: {program}")
    if field:
        desc_bits.append(f"Field: {field}")
    if year:
        desc_bits.append(f"Year: {year}")
    description = " | ".join(desc_bits) or None

    return {
        "funder_award_id": code,
        "title": title,
        "pi_full": pi_full,
        "pi_given": pi_given,
        "pi_family": pi_family,
        "institution": institution,
        "amount": None,                 # not published (§6.7 waiver)
        "currency": None,               # not published
        "scheme": program,              # program = scheme
        "start_date_raw": start_raw,    # real DD/MM/YYYY from the duration cell
        "end_date_raw": end_raw,
        "description": description,
        "landing_page_url": SEARCH_URL,
    }


# ----------------------------------------------------------------------------- harvest
def harvest(session, limit=None, delay=0.7, max_empty_pages=2):
    """
    Walk pages 0,30,60,... until two consecutive empty pages (clean end) or until
    `limit` rows collected. Returns (records, advertised, pages_fetched, synth_count).
    """
    records = []
    synth_counter = [0]
    advertised = None
    page = 0
    empty_streak = 0
    pages_fetched = 0

    while True:
        html = fetch_page(session, page)
        pages_fetched += 1
        if advertised is None:
            advertised = parse_advertised(html)
            if advertised:
                print(f"  advertised total: {advertised}")

        rows = parse_rows(html)
        if not rows:
            empty_streak += 1
            print(f"  page param={page}: 0 rows (empty streak {empty_streak})")
            if empty_streak >= max_empty_pages:
                print(f"  reached clean end at page param={page}")
                break
            page += PAGE_STEP
            time.sleep(delay)
            continue
        empty_streak = 0

        for i, raw in enumerate(rows):
            records.append(build_record(raw, page, i, synth_counter))

        if (page // PAGE_STEP) % 10 == 0 or len(rows) < PAGE_STEP:
            print(f"  page param={page} ({page // PAGE_STEP + 1}): +{len(rows)} rows "
                  f"(running total {len(records)})")

        if limit is not None and len(records) >= limit:
            print(f"  reached --limit={limit}")
            records = records[:limit]
            break

        # Safety stop well past the advertised end (237 pages -> page 7080).
        if advertised and page > advertised + 5 * PAGE_STEP:
            print(f"  safety stop: page param={page} far past advertised {advertised}")
            break

        page += PAGE_STEP
        time.sleep(delay)

    return records, advertised, pages_fetched, synth_counter[0]


# ----------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(
        description="Harvest NAFOSTED (Vietnam) awards into a parquet and "
                    "(optionally) upload to S3.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--limit", type=int, default=None,
                    help="Stop after N rows (smoke test). Disables the completeness "
                         "guard so partial runs never upload.")
    ap.add_argument("--skip-upload", action="store_true",
                    help="Build the parquet but do not upload to S3.")
    ap.add_argument("--skip-download", action="store_true",
                    help="Skip harvesting; re-use an existing parquet in --output-dir "
                         "(for re-upload / shrink-check only).")
    ap.add_argument("--output-dir", default="/tmp",
                    help="Directory for nafosted_grants.parquet.")
    ap.add_argument("--allow-shrink", action="store_true",
                    help="Permit upload even if the new row count is materially "
                         "smaller than the previous parquet (§1.4 shrink-check).")
    ap.add_argument("--delay", type=float, default=0.7,
                    help="Polite delay (seconds) between page fetches.")
    ap.add_argument("--shrink-tol", type=float, default=0.05,
                    help="Allowed fractional shrink vs previous parquet before "
                         "the shrink-check trips (0.05 = 5%%).")
    args = ap.parse_args()

    # SKIP_UPLOAD env var also honored (in addition to --skip-upload).
    env_skip = os.environ.get("SKIP_UPLOAD", "").strip().lower() in ("1", "true", "yes")
    skip_upload = args.skip_upload or env_skip

    out_path = os.path.join(args.output_dir, "nafosted_grants.parquet")

    # ---- previous row count (for shrink-check) -----------------------------------
    prev_rows = None
    if os.path.exists(out_path):
        try:
            prev_rows = len(pd.read_parquet(out_path))
        except Exception as e:
            print(f"  (could not read existing parquet for shrink-check: {e})")

    # ---- harvest ------------------------------------------------------------------
    if args.skip_download:
        if not os.path.exists(out_path):
            print(f"ERROR: --skip-download but {out_path} does not exist", file=sys.stderr)
            sys.exit(2)
        df = pd.read_parquet(out_path)
        advertised = ADVERTISED_FALLBACK
        synth = int((df["funder_award_id"].astype(str)
                     .str.startswith("NAFOSTED-SYNTH-")).sum())
        print(f"Re-using existing parquet: {out_path} ({len(df)} rows)")
    else:
        session = make_session()
        print("Harvesting NAFOSTED awards ...")
        t0 = time.time()
        records, advertised, pages, synth = harvest(
            session, limit=args.limit, delay=args.delay)
        print(f"Harvest done: {len(records)} raw records from {pages} pages "
              f"in {time.time() - t0:.0f}s (advertised={advertised}, synthetic ids={synth})")

        df = pd.DataFrame.from_records(records, columns=COLUMNS)

        # ---- dedup by funder_award_id (keep first) --------------------------------
        before = len(df)
        df = df.drop_duplicates(subset=["funder_award_id"], keep="first").reset_index(drop=True)
        dropped = before - len(df)
        if dropped:
            print(f"  dedup: dropped {dropped} duplicate funder_award_id rows "
                  f"({before} -> {len(df)})")

    # ---- enforce all-string dtype -------------------------------------------------
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = None
    df = df[COLUMNS]
    # Convert to pandas string dtype, mapping NaN/None -> <NA> (writes as null).
    df = df.astype("string")
    # Normalise empty strings to null
    df = df.replace({"": pd.NA, "None": pd.NA})

    # ---- fill-rate report ---------------------------------------------------------
    n = len(df)
    print("\n=== Field fill rates (%d rows) ===" % n)
    def fr(col):
        cnt = int(df[col].notna().sum())
        pct = (100.0 * cnt / n) if n else 0.0
        print(f"  {col:16s}: {cnt:6d}/{n} ({pct:5.1f}%)")
        return cnt
    for c in ["funder_award_id", "title", "pi_full", "pi_given", "pi_family",
              "institution", "scheme", "start_date_raw", "end_date_raw", "description"]:
        fr(c)
    native = int((~df["funder_award_id"].astype("string")
                  .str.startswith("NAFOSTED-SYNTH-", na=False)).sum())
    print(f"  native grant-code ids : {native}/{n}  | synthetic ids: {n - native}")

    # ---- completeness guard -------------------------------------------------------
    adv = advertised or ADVERTISED_FALLBACK
    guard_ok = True
    guard_skipped = False
    if args.limit is not None or args.skip_download:
        guard_skipped = True
        print(f"\n[guard] skipped (limit/skip-download run).")
    else:
        # Allow a small slack for advertised>harvested due to true duplicate codes,
        # but hard-fail if materially short (>2% below advertised).
        ratio = (n / adv) if adv else 1.0
        print(f"\n[guard] advertised={adv}  harvested(after dedup)={n}  "
              f"ratio={ratio:.4f}")
        if ratio < 0.98:
            guard_ok = False
            print(f"[guard] FAIL: harvested {n} is materially short of advertised {adv} "
                  f"(<98%). No upload.")
        else:
            print(f"[guard] PASS.")

    # ---- write parquet (always, unless skip-download reused it) -------------------
    if not args.skip_download:
        df.to_parquet(out_path, index=False)
        print(f"\nWrote {out_path} ({n} rows, all dtype string)")
    else:
        print(f"\nUsing existing {out_path} ({n} rows)")

    # ---- shrink-check (§1.4) ------------------------------------------------------
    shrink_ok = True
    if prev_rows is not None and not args.skip_download:
        if n < prev_rows * (1 - args.shrink_tol):
            shrink_ok = args.allow_shrink
            msg = (f"[shrink] new rows {n} < previous {prev_rows} "
                   f"(tol {args.shrink_tol:.0%}).")
            if args.allow_shrink:
                print(f"{msg} Allowed by --allow-shrink.")
            else:
                print(f"{msg} BLOCKING upload — pass --allow-shrink to override.")
        else:
            print(f"[shrink] ok (new {n} vs previous {prev_rows}).")

    # ---- upload -------------------------------------------------------------------
    if skip_upload:
        print("\nSKIP_UPLOAD set — not uploading to S3.")
        return 0
    if guard_skipped:
        print("\nGuard skipped (partial run) — not uploading to S3.")
        return 0
    if not guard_ok:
        print("\nCompleteness guard failed — NOT uploading.")
        return 1
    if not shrink_ok:
        print("\nShrink-check failed — NOT uploading.")
        return 1

    import subprocess
    print(f"\nUploading -> {S3_DEST}")
    rc = subprocess.call(["aws", "s3", "cp", out_path, S3_DEST])
    if rc != 0:
        print(f"aws s3 cp exited {rc}", file=sys.stderr)
        return rc
    print("Upload complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
