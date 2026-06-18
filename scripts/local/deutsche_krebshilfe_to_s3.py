#!/usr/bin/env python3
"""
Deutsche Krebshilfe (German Cancer Aid) -> OpenAlex Walden awards parquet.

Source: https://www.krebshilfe.de/forschen/projekte/forschungsprojekte/
  - Paginated list (TYPO3 plugin):  ?tx_dkhprojects_list[currentPage]=N  (no cHash required)
  - 10 projects/page; the pager CLAMPS to the last page when N > last (content repeats),
    which is how we detect the end. ~9 pages / ~90 projects as of build.
  - Per-project detail pages: /forschungsprojekte/<numeric-id>-<slug>/

Funder:  Deutsche Krebshilfe  | funder_id 4320323556 | ROR 01wxdd722 | DE | EUR
Provenance slug: deutsche_krebshilfe

Output: /tmp/deutsche_krebshilfe_grants.parquet  (13 cols, ALL string/nullable)
  funder_award_id, title, pi_full, pi_given, pi_family, institution, amount,
  currency, scheme, start_date_raw, end_date_raw, description, landing_page_url

funder_award_id = NATIVE numeric slug id (e.g. 70113412), not synthetic.
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

import pandas as pd

# ----------------------------------------------------------------------------- config
BASE = "https://www.krebshilfe.de"
LIST_URL = BASE + "/forschen/projekte/forschungsprojekte/"
LIST_PAGE = LIST_URL + "?tx_dkhprojects_list%5BcurrentPage%5D={page}"
DETAIL_URL = BASE + "/forschen/projekte/forschungsprojekte/{slug}/"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")

OUTPUT_COLS = [
    "funder_award_id", "title", "pi_full", "pi_given", "pi_family",
    "institution", "amount", "currency", "scheme",
    "start_date_raw", "end_date_raw", "description", "landing_page_url",
]
CURRENCY = "EUR"
S3_DEST = "s3://openalex-ingest/awards/deutsche_krebshilfe/deutsche_krebshilfe_grants.parquet"
SLUG_RE = re.compile(r"forschungsprojekte/(\d+-[a-z0-9\-]+)/")
PER_PAGE = 10
MAX_PAGE_PROBE = 40          # safety ceiling for page walk
SHRINK_TOLERANCE = 0.10      # §1.4: refuse if new harvest < (1-tol) * existing rows

# Title prefixes to strip when splitting PI names.
TITLE_TOKENS = {
    "prof", "prof.", "professor", "professorin", "dr", "dr.", "phd", "ph.d.",
    "pd", "priv.-doz.", "priv.", "doz.", "md", "dipl.", "dipl",
    "med.", "rer.", "nat.", "habil.", "univ.-prof.", "univ.", "mba", "msc",
    "m.sc.", "b.sc.", "apl.", "hon.-prof.",
}


# ----------------------------------------------------------------------------- http
def fetch(url, retries=4, backoff=2.0, delay=0.6):
    """GET with browser UA, retry/backoff, polite delay."""
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=45) as r:
                html = r.read().decode("utf-8", "replace")
            time.sleep(delay)
            return html
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            last = e
            wait = backoff * (attempt + 1)
            sys.stderr.write(f"  [retry {attempt+1}/{retries}] {url} -> {e}; sleep {wait:.1f}s\n")
            time.sleep(wait)
    raise RuntimeError(f"failed to fetch {url}: {last}")


# ----------------------------------------------------------------------------- parsing
def unescape_text(html_fragment):
    import html as ihtml
    txt = re.sub(r"<[^>]+>", " ", html_fragment)
    txt = ihtml.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()


def list_slugs(html):
    """Ordered-unique detail slugs (e.g. '70113412-studie-...') on a list page."""
    seen, out = set(), []
    for s in SLUG_RE.findall(html):
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def harvest_slugs():
    """
    Walk pages until the pager CLAMPS (a page returns the same slug set as the
    previous page) or until pages stop returning projects. Returns (slugs, n_pages).
    """
    import html as ihtml  # noqa
    all_slugs, seen = [], set()
    prev_fp = None
    pages = 0
    for page in range(1, MAX_PAGE_PROBE + 1):
        html = fetch(LIST_PAGE.format(page=page))
        slugs = list_slugs(html)
        if not slugs:
            sys.stderr.write(f"  page {page}: 0 projects -> stop\n")
            break
        fp = tuple(slugs)
        if prev_fp is not None and fp == prev_fp:
            sys.stderr.write(f"  page {page}: clamp (== page {page-1}) -> last real page = {page-1}\n")
            break
        pages = page
        prev_fp = fp
        added = 0
        for s in slugs:
            if s not in seen:
                seen.add(s)
                all_slugs.append(s)
                added += 1
        sys.stderr.write(f"  page {page}: {len(slugs)} slugs ({added} new), first={slugs[0]}\n")
    return all_slugs, pages


FIELD_RE = re.compile(
    r'list-definition-term">\s*'
    r'<(?:p|h2|h3)[^>]*class="headline-h3[^"]*"[^>]*>(?P<lab>.*?)</(?:p|h2|h3)>'
    r'.*?<div class="mod-headline-copytext__list-description-element">(?P<val>.*?)</div>',
    re.S,
)


def parse_detail(html):
    """Extract the label->value definition list (robust to <p>/<h2> labels and
    copytext-vs-list-bullet values)."""
    import html as ihtml
    fields = {}
    for m in FIELD_RE.finditer(html):
        lab = unescape_text(m.group("lab"))
        valhtml = m.group("val")
        lis = re.findall(r"<li[^>]*>(.*?)</li>", valhtml, re.S)
        if lis:
            val = "; ".join(unescape_text(x) for x in lis)
        else:
            val = unescape_text(valhtml)
        if lab and lab not in fields:
            fields[lab] = val
    # h1 fallback for title
    h1 = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.S)
    fields["__h1__"] = unescape_text(h1.group(1)) if h1 else None
    return fields


# ----------------------------------------------------------------------------- normalisers
def parse_amount(raw):
    """'2.206.277 €' -> '2206277' (integer euros, string). None if absent."""
    if not raw:
        return None
    # German thousands sep '.', possible decimal ',' -- take the leading number group
    m = re.search(r"([\d\. \s]+)(?:,\d+)?\s*€?", raw)
    if not m:
        return None
    digits = re.sub(r"[^\d]", "", m.group(1))
    return digits or None


def parse_years(raw):
    """'2019 - 2027' / '2019\\xa0- 2027' -> ('2019-01-01','2027-01-01'). Year precision."""
    if not raw:
        return None, None
    yrs = re.findall(r"(19|20)\d{2}", raw)  # capture groups not ideal -> use full
    yrs = re.findall(r"((?:19|20)\d{2})", raw)
    start = f"{yrs[0]}-01-01" if len(yrs) >= 1 else None
    end = f"{yrs[1]}-01-01" if len(yrs) >= 2 else None
    return start, end


def split_pi(raw):
    """
    'Professor Dr. Ralf Hofheinz' -> full, given='Ralf', family='Hofheinz'.
    Multi-PI ('A & B', 'A und B', 'A, B') -> keep full string; split the FIRST name only.
    """
    if not raw:
        return None, None, None
    full = raw.strip()
    # first PI for the given/family split. Multi-PI joiners: ';' (list-bullet),
    # '&', 'und', 'and', '/'. Comma is NOT split on (German names are
    # "Title Given Family", not "Family, Given").
    first = re.split(r"\s*;\s*|\s+(?:und|and)\s+|\s*&\s*|\s*/\s*", full)[0].strip()
    tokens = first.split()
    name_tokens = [t for t in tokens if t.lower().strip(".,") not in
                   {x.strip(".") for x in TITLE_TOKENS}]
    # drop any token that is purely a title abbreviation we missed (ends with '.')
    name_tokens = [t for t in name_tokens if not (len(t) <= 4 and t.endswith("."))]
    given = family = None
    if len(name_tokens) >= 2:
        family = name_tokens[-1]
        given = " ".join(name_tokens[:-1])
    elif len(name_tokens) == 1:
        family = name_tokens[0]
    return full or None, given, family


def native_id(slug):
    """'70113412-studie-...' -> '70113412'."""
    m = re.match(r"(\d+)-", slug)
    return m.group(1) if m else slug


def build_record(slug, fields):
    title = fields.get("Projekt-Titel") or fields.get("__h1__")
    pi_full, pi_given, pi_family = split_pi(fields.get("Projektleitung"))
    institution = fields.get("Standort") or fields.get("Beteiligte Standorte")
    amount = parse_amount(fields.get("Fördersumme"))
    start, end = parse_years(fields.get("Förderzeitraum"))
    description = fields.get("Worum geht es?")
    return {
        "_slug": slug,                     # internal: true unique key (collision fallback)
        "funder_award_id": native_id(slug),
        "title": title or None,
        "pi_full": pi_full,
        "pi_given": pi_given,
        "pi_family": pi_family,
        "institution": institution or None,
        "amount": amount,
        "currency": CURRENCY if amount else None,
        "scheme": None,  # no dedicated funding-line / programme field on detail pages
        "start_date_raw": start,
        "end_date_raw": end,
        "description": description or None,
        "landing_page_url": DETAIL_URL.format(slug=slug),
    }


# ----------------------------------------------------------------------------- shrink check (§1.4)
def existing_s3_rowcount():
    """Best-effort row count of the currently-published parquet on S3. None if absent/unknown."""
    try:
        import subprocess, tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tf:
            tmp = tf.name
        r = subprocess.run(["aws", "s3", "cp", S3_DEST, tmp],
                           capture_output=True, text=True)
        if r.returncode != 0:
            return None
        df = pd.read_parquet(tmp)
        os.unlink(tmp)
        return len(df)
    except Exception as e:
        sys.stderr.write(f"  [shrink-check] could not read existing S3 parquet: {e}\n")
        return None


# ----------------------------------------------------------------------------- main
def run(args):
    cache = os.path.join(args.output_dir, "deutsche_krebshilfe_raw.json")

    # 1) gather slugs + records
    if args.skip_download and os.path.exists(cache):
        sys.stderr.write(f"--skip-download: loading cached records from {cache}\n")
        blob = json.load(open(cache))
        slugs = blob["slugs"]
        n_pages = blob["n_pages"]
        records = blob["records"]
    else:
        sys.stderr.write("Harvesting list pages...\n")
        slugs, n_pages = harvest_slugs()
        if args.limit:
            slugs = slugs[: args.limit]
            sys.stderr.write(f"--limit {args.limit}: capping to {len(slugs)} projects\n")
        sys.stderr.write(f"Fetching {len(slugs)} detail pages...\n")
        records = []
        for i, slug in enumerate(slugs, 1):
            html = fetch(DETAIL_URL.format(slug=slug))
            rec = build_record(slug, parse_detail(html))
            records.append(rec)
            if i % 10 == 0 or i == len(slugs):
                sys.stderr.write(f"  {i}/{len(slugs)} detail pages\n")
        json.dump({"slugs": slugs, "n_pages": n_pages, "records": records},
                  open(cache, "w"), ensure_ascii=False)

    advertised = len(set(slugs))  # authoritative denominator = UNIQUE pager-confirmed projects (a project listed across a page boundary is still one project)

    # 2) dataframe + dedup by funder_award_id
    df = pd.DataFrame(records, columns=["_slug"] + OUTPUT_COLS)
    before = len(df)
    # True unique key is the slug (a page may be reached once). Dedup on it.
    df = df.drop_duplicates(subset=["_slug"], keep="first").reset_index(drop=True)
    dropped = before - len(df)

    # Native-id collision handling: the funder occasionally reuses one numeric id
    # for two genuinely different projects (different PI/title/amount/city).
    # Keep BOTH grants; for the colliding rows only, fall back funder_award_id to
    # the full slug so the key stays unique. Non-colliding rows keep the native id.
    id_counts = df["funder_award_id"].value_counts()
    collisions = set(id_counts[id_counts > 1].index)
    if collisions:
        n_coll_rows = int(df["funder_award_id"].isin(collisions).sum())
        sys.stderr.write(
            f"  native-id COLLISIONS: {len(collisions)} id(s) shared by "
            f"{n_coll_rows} projects -> using slug as funder_award_id for those rows: "
            f"{sorted(collisions)}\n")
        mask = df["funder_award_id"].isin(collisions)
        df.loc[mask, "funder_award_id"] = df.loc[mask, "_slug"]

    df = df.drop(columns=["_slug"])
    # force all-string dtype (nullable)
    for c in OUTPUT_COLS:
        df[c] = df[c].astype("string")

    harvested = len(df)
    assert df["funder_award_id"].is_unique, "funder_award_id not unique after collision fix"

    # 3) completeness guard
    sys.stderr.write("\n========== COMPLETENESS ==========\n")
    sys.stderr.write(f"  list pages (real)      : {n_pages}\n")
    sys.stderr.write(f"  advertised (slug set)  : {advertised}\n")
    sys.stderr.write(f"  detail records fetched : {before}\n")
    sys.stderr.write(f"  duplicate ids dropped  : {dropped}\n")
    sys.stderr.write(f"  harvested (unique)     : {harvested}\n")

    # When --limit is used we intentionally subset, so skip the hard-fail guard.
    if not args.limit:
        # materially short = lost >1 record vs advertised denominator (allow exactly equal)
        if harvested < advertised:
            shortfall = advertised - harvested
            frac = shortfall / advertised if advertised else 1.0
            msg = (f"COMPLETENESS GUARD FAIL: harvested {harvested} < advertised "
                   f"{advertised} (short {shortfall}, {frac:.1%}). NOT uploading.")
            sys.stderr.write("  !! " + msg + "\n")
            raise SystemExit(msg)

    # 4) fill-rate report
    sys.stderr.write("\n========== FIELD FILL RATES ==========\n")
    n = len(df) or 1
    for c in OUTPUT_COLS:
        filled = df[c].notna().sum()
        sys.stderr.write(f"  {c:<18}: {filled:>4}/{len(df)}  ({100*filled/n:5.1f}%)\n")

    # 5) write parquet
    out_path = os.path.join(args.output_dir, "deutsche_krebshilfe_grants.parquet")
    df.to_parquet(out_path, index=False)
    sys.stderr.write(f"\nWrote {out_path}  ({len(df)} rows, all string dtype)\n")

    # 6) shrink check (§1.4) + upload
    if args.skip_upload or os.environ.get("SKIP_UPLOAD"):
        sys.stderr.write("SKIP_UPLOAD set -> not uploading.\n")
        return

    prev = existing_s3_rowcount()
    if prev is not None and not args.allow_shrink:
        floor = int(prev * (1 - SHRINK_TOLERANCE))
        sys.stderr.write(f"  [shrink-check] existing S3 rows={prev}, new={len(df)}, "
                         f"floor={floor} (tol {SHRINK_TOLERANCE:.0%})\n")
        if len(df) < floor:
            msg = (f"SHRINK GUARD FAIL: new {len(df)} < {floor} "
                   f"(={1-SHRINK_TOLERANCE:.0%} of existing {prev}). "
                   f"Pass --allow-shrink to override.")
            raise SystemExit(msg)

    import subprocess
    sys.stderr.write(f"Uploading -> {S3_DEST}\n")
    r = subprocess.run(["aws", "s3", "cp", out_path, S3_DEST],
                       capture_output=True, text=True)
    sys.stderr.write(r.stdout + r.stderr)
    if r.returncode != 0:
        raise SystemExit(f"aws s3 cp failed (rc={r.returncode})")
    sys.stderr.write("Upload OK.\n")


def main():
    ap = argparse.ArgumentParser(
        description="Scrape Deutsche Krebshilfe research projects -> awards parquet "
                    "and (optionally) upload to S3.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--limit", type=int, default=None,
                    help="cap number of projects (for quick test runs; disables completeness guard)")
    ap.add_argument("--skip-upload", action="store_true",
                    help="build parquet but do not upload to S3 (also honored via env SKIP_UPLOAD)")
    ap.add_argument("--skip-download", action="store_true",
                    help="reuse cached raw records (deutsche_krebshilfe_raw.json) instead of scraping")
    ap.add_argument("--output-dir", default="/tmp",
                    help="directory for parquet + json cache")
    ap.add_argument("--allow-shrink", action="store_true",
                    help="bypass the §1.4 shrink guard (allow uploading a much smaller dataset)")
    args = ap.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)
    run(args)


if __name__ == "__main__":
    main()
