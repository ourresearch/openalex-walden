#!/usr/bin/env python3
"""
Fondation Brain Canada / Brain Canada Foundation -> awards harvester.

OpenAlex funder_id : F4320311238  (ROR 01bcmwk98, DOI 10.13039/100009408)
Provenance slug    : brain_canada   priority 387   currency CAD
Source             : https://braincanada.ca/funded-grants  (ApostropheCMS)

Mechanism
---------
The grant inventory lives at /funded-grants as server-rendered cards, BUT the
?page=N pagination is UNSTABLE: adjacent pages overlap and items shift as you
page through, so a listing walk silently drops ~70 grants (a full crawl yields
~759 distinct of the true ~833). The XML sitemap
(robots.txt -> /sitemaps/index.xml -> /sitemaps/en-CA.xml) lists every
published grant detail page exactly ONCE and is the authoritative inventory
(833 distinct grant slugs). Cards carry only a truncated description + the
detail link; the full record (PI, institution, amount, Brain Canada
contribution, competition, start date, partner, disease/area) lives on each
grant DETAIL page. So we:

  1. Read the sitemap for the complete set of detail-page slugs (the stable
     native id), supplemented by a listing walk to catch anything live but
     not yet in the sitemap.
  2. Fetch each detail page and extract the 13-column record.
  3. Completeness guard: hard-fail (no upload) if harvested count is
     materially below the sitemap distinct-grant count.
  4. Write parquet (overwrite) and, unless SKIP_UPLOAD, upload to S3.

Column contract (13, all string/nullable, null where absent):
  funder_award_id, title, pi_full, pi_given, pi_family, institution,
  amount, currency, scheme, start_date_raw, end_date_raw, description,
  landing_page_url

Conventions (matched to the pre-existing /tmp parquet, with two fixes):
  * funder_award_id = URL slug.
  * amount          = "Total Grant Amount" (bare number, symbols stripped).
                      These are frequently CO-FUNDED; the Brain Canada
                      contribution is recorded in `description`, not `amount`.
  * description     = <full abstract> <partner names...> | Brain Canada
                      Contribution: $X | Disease/Disorder Area: Y |
                      Area of Research: Z | Co-applicants: Name (Inst); ...
                      (pipe-joined metadata suffix; co-applicants = every
                      team member after the lead PI).
  * scheme          = "<Grant type> — <Competition>" when both present, else
                      whichever exists (em dash U+2014 separator).
  * start_date_raw  = YYYY-01-01.  DATE RULE (fix vs old parquet): if the
                      Competition carries a YYYY-YYYY cycle, use the FIRST
                      year (a "...2025-2026" program starts 2025). Else use
                      the site "Start Date" meta year. Only accept a 4-digit
                      19xx/20xx year; otherwise null (the site has at least
                      one garbage "0222" value -> null, not 0222).
  * pi_*            = lead PI (first team member); family = last token with
                      surname particles (de/De/van/Al/la/...) absorbed, given
                      = the remainder; academic titles (Dr./Prof.) stripped.

Reproduces the pre-existing /tmp parquet byte-for-byte on all 13 columns
across validation samples, except (a) the intended start-date fix above and
(b) it captures a partner/abstract line in a handful of records where the
old harvest left description empty (strictly more complete, never less).
"""

import os
import re
import sys
import time
import random

import requests
import pandas as pd
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
BASE        = "https://braincanada.ca"
LISTING_URL = BASE + "/funded-grants"
CURRENCY    = "CAD"
OUT_PATH    = "/tmp/brain_canada_grants.parquet"
S3_DEST     = "s3://openalex-ingest/awards/brain_canada/brain_canada_grants.parquet"

# Tolerance for the completeness guard: how far below the advertised total
# the harvest may fall (a few detail-page 404s/timeouts are acceptable, a
# truncated crawl is not).
GUARD_MIN_FRACTION = 0.97

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-CA,en;q=0.9",
}

COLUMNS = [
    "funder_award_id", "title", "pi_full", "pi_given", "pi_family",
    "institution", "amount", "currency", "scheme", "start_date_raw",
    "end_date_raw", "description", "landing_page_url", "country",
]

# Affiliation country (Codex review): Brain Canada is a Canadian funder but funds
# some international collaborators, so we assert "Canada" ONLY when the institution
# is confidently Canadian and leave country null otherwise — never wrong-assert.
# Explicit matcher (no blanket English "University" — that would catch e.g. Helsinki).
_CA_INST = re.compile(
    r"(université|universite|McGill|McMaster|MacMaster|Concordia|Dalhousie|Queen.?s\s+Univ|Ryerson|"
    r"Carleton|Waterloo|Guelph|Sherbrooke|Laval|Simon Fraser|York Univ|Western Univ|Wilfrid Laurier|"
    r"NSCAD|OCAD|Memorial Univ|Brock|Trent|Lakehead|Regina|Windsor|Lethbridge|Victoria|"
    r"Toronto|Calgary|Alberta|British Col|Montr[ée]al|Ottawa|Manitoba|Saskatchewan|Saskatoon|"
    r"Vancouver|Winnipeg|Edmonton|Halifax|Hamilton|Kingston|Nova Scotia|Newfoundland|New Brunswick|"
    r"\bQu[ée]bec\b|\bOntario\b|\bCanada\b|Child ?& ?Family|"
    r"CAMH|Addiction and Mental Health|University Health Network|\bUHN\b|Hospital for Sick|SickKids|"
    r"Sick Kids|\bIWK\b|Holland Bloorview|Jewish General|Lady Davis|\bIRCM\b|\bINRS\b|Armand Frappier|"
    r"Institut national de la recherche|BCCHR|BC Children|Princess Margaret|Krembil|Lunenfeld|"
    r"London Health Science|Sunnybrook|Douglas (Mental|Hospital|Institute|Research)|Bruy[èe]re|Robarts|"
    r"Ottawa Hospital|St\.? Michael|Mount Sinai|Baycrest|Rotman|\bCHUM?\b|Sainte?-Justine|Sinai Health|"
    r"Providence Health|Vancouver Coastal|Interior Health|Schlegel|KalGene|Centre de recherche|"
    r"Health Sciences? Centre|Centre hospitalier)", re.I)

session = requests.Session()
session.headers.update(HEADERS)


# --------------------------------------------------------------------------- #
# HTTP helper with polite delay + retry/backoff
# --------------------------------------------------------------------------- #
def get(url, tries=4):
    last = None
    for attempt in range(1, tries + 1):
        try:
            r = session.get(url, timeout=45)
            if r.status_code == 200:
                # polite jittered delay between successful requests
                time.sleep(random.uniform(0.5, 1.0))
                return r.text
            if r.status_code == 404:
                return None  # genuinely missing detail page
            last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = str(e)
        sleep = min(2 ** attempt, 16) + random.uniform(0, 0.5)
        print(f"    retry {attempt}/{tries} for {url} ({last}); sleeping {sleep:.1f}s",
              flush=True)
        time.sleep(sleep)
    print(f"    GIVING UP on {url}: {last}", flush=True)
    return None


# --------------------------------------------------------------------------- #
# URL discovery
# --------------------------------------------------------------------------- #
# Authoritative source of the complete grant inventory. The /funded-grants
# listing uses UNSTABLE pagination (adjacent pages overlap / items shift as you
# page), so a listing walk silently drops ~70 grants. The XML sitemap lists
# every published grant detail page exactly once -> use it as the spine.
ROBOTS_URL  = BASE + "/robots.txt"
SITEMAP_IDX = BASE + "/sitemaps/index.xml"

SLUG_HREF_RE = re.compile(r'/funded-grants/([a-z0-9][a-z0-9\-]+)/?$')
SITEMAP_GRANT_RE = re.compile(
    r'<loc>\s*(https://braincanada\.ca/funded-grants/[^<\s]+?)\s*</loc>', re.I
)


def _slug_of(url):
    seg = url.rstrip("/").rsplit("/", 1)[-1]
    return seg if seg and seg != "funded-grants" else None


def sitemap_urls():
    """Read robots.txt -> sitemap index -> child sitemaps; return the set of
    distinct funded-grants detail slugs. Returns [] if the sitemap is
    unavailable (caller then relies on the listing fallback)."""
    # find the sitemap index (prefer robots.txt pointer, fall back to default)
    idx_url = SITEMAP_IDX
    robots = get(ROBOTS_URL, tries=2)
    if robots:
        m = re.search(r'(?im)^\s*sitemap:\s*(\S+)', robots)
        if m:
            idx_url = m.group(1).strip()

    idx = get(idx_url, tries=3)
    if not idx:
        print("  sitemap index unavailable", flush=True)
        return []

    children = re.findall(r'<loc>\s*([^<\s]+\.xml)\s*</loc>', idx, re.I)
    if not children:
        # idx may itself be a urlset, not an index
        children = [idx_url]

    slugs = set()
    for child in children:
        xml = get(child, tries=3) if child != idx_url else idx
        if not xml:
            continue
        for url in SITEMAP_GRANT_RE.findall(xml):
            s = _slug_of(url)
            if s:
                slugs.add(s)
        print(f"  sitemap {child.rsplit('/',1)[-1]}: "
              f"{len(slugs)} distinct grant slugs so far", flush=True)
    return sorted(slugs)


def discover_last_page(first_html):
    """Read the advertised last page number from the pagination block."""
    nums = [int(n) for n in re.findall(r'[?&]page=(\d+)', first_html)]
    soup = BeautifulSoup(first_html, "html.parser")
    last_el = soup.select_one(".pagination__pages__item.is-last")
    if last_el:
        m = re.search(r"\d+", last_el.get_text())
        if m:
            nums.append(int(m.group()))
    return max(nums) if nums else 1


def slugs_on_page(html):
    """Return ordered, de-duplicated grant slugs linked from a listing page."""
    soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    for a in soup.select("a.project-directory__grant-card-link[href]"):
        m = SLUG_HREF_RE.search(a["href"])
        if m and m.group(1) not in seen:
            seen.add(m.group(1))
            out.append(m.group(1))
    return out


def walk_listing():
    """Supplementary slug source: walk ?page=N. Pagination is unstable so this
    is lossy on its own; used only to catch any live grant not yet in the
    sitemap. Returns (slugs, last_page)."""
    first = get(LISTING_URL)
    if first is None:
        return [], 0
    last_page = discover_last_page(first)
    slugs, seen = [], set()
    for page in range(1, last_page + 1):
        html = first if page == 1 else get(f"{LISTING_URL}?page={page}")
        if html is None:
            continue
        for s in slugs_on_page(html):
            if s not in seen:
                seen.add(s); slugs.append(s)
        if page % 20 == 0 or page == last_page:
            print(f"  listing page {page}/{last_page}: "
                  f"{len(slugs)} unique so far", flush=True)
    return slugs, last_page


def collect_all_slugs():
    """
    Build the complete slug set. Spine = sitemap (authoritative, each grant
    once). Supplement with the listing walk to capture anything published but
    not yet in the sitemap. The completeness target = the sitemap distinct
    count (the listing's advertised total over-counts because of pagination
    overlap, so it is NOT used as the guard baseline).
    """
    print("Discovering grant URLs from sitemap...", flush=True)
    sm = sitemap_urls()
    sitemap_total = len(sm)
    print(f"Sitemap: {sitemap_total} distinct grant slugs", flush=True)

    print("Walking listing pages (supplementary)...", flush=True)
    listing, last_page = walk_listing()
    print(f"Listing: {len(listing)} unique slugs (last page {last_page})",
          flush=True)

    seen, ordered = set(), []
    for s in list(sm) + listing:          # sitemap first, then listing extras
        if s not in seen:
            seen.add(s); ordered.append(s)

    extra = [s for s in listing if s not in set(sm)]
    if extra:
        print(f"  +{len(extra)} live slugs from listing not in sitemap "
              f"(e.g. {extra[0]})", flush=True)

    if sitemap_total == 0:
        # sitemap failed entirely -> fall back to listing as the baseline
        print("WARN: sitemap empty; using listing count as completeness "
              "baseline (lossy).", flush=True)
        baseline = len(listing)
    else:
        baseline = sitemap_total

    print(f"Total unique slugs to fetch: {len(ordered)} "
          f"(completeness baseline = {baseline})", flush=True)
    return ordered, baseline


# --------------------------------------------------------------------------- #
# Detail-page parsing
# --------------------------------------------------------------------------- #
TITLE_RE   = re.compile(r"^(dr|prof|professor|mr|ms|mrs)\.?\s+", re.I)
YEAR_RE    = re.compile(r"\b(19|20)\d{2}\b")
RANGE_RE   = re.compile(r"((?:19|20)\d{2})\s*[-–—]\s*((?:19|20)\d{2})")


def clean(txt):
    if txt is None:
        return None
    t = re.sub(r"\s+", " ", txt).strip()
    return t or None


def strip_title(name):
    if not name:
        return name
    prev = None
    while prev != name:
        prev = name
        name = TITLE_RE.sub("", name).strip()
    return name


# Surname particles that attach to the family name when they immediately
# precede the final token (e.g. "Yves De Koninck" -> family "De Koninck",
# "Sara de la Salle" -> family "de la Salle"). Matched case-insensitively.
NAME_PARTICLES = {"de", "del", "della", "la", "le", "van", "von", "der",
                  "den", "al", "el", "bin", "ibn", "da", "di", "do", "dos",
                  "das", "du", "ter", "ten"}


def split_name(full):
    """
    Split into (full, given, family) matching the pre-existing parquet:
      family = LAST token, given = everything before, EXCEPT preceding
      surname particles (de/De/van/Al/la/...) are pulled into the family.
    Academic titles are stripped first.
    """
    if not full:
        return None, None, None
    full = strip_title(full)
    parts = full.split()
    if not parts:
        return None, None, None
    if len(parts) == 1:
        return full, parts[0], None

    # family starts as the last token; absorb any particle tokens just before it
    fam_start = len(parts) - 1
    while fam_start > 1 and parts[fam_start - 1].lower().strip(".") in NAME_PARTICLES:
        fam_start -= 1
    given = " ".join(parts[:fam_start])
    family = " ".join(parts[fam_start:])
    return full, given or None, family or None


def money(txt):
    """
    '$200,000' -> '200000'; '$1,359,868.42' -> '1359868' (drop cents);
    null if no digits. The decimal portion is discarded so cents never get
    concatenated onto the dollar value (e.g. avoid 1359868.42 -> '135986842').
    """
    if not txt:
        return None
    # keep only the integer part before a decimal point
    integer_part = txt.split(".", 1)[0]
    digits = re.sub(r"[^\d]", "", integer_part)
    return digits or None


def meta_pairs(soup):
    out = {}
    for item in soup.select(".grant-detail__meta-item"):
        lab = item.select_one(".grant-detail__meta-label")
        val = item.select_one(".grant-detail__meta-value")
        if lab and val:
            out[clean(lab.get_text())] = clean(val.get_text())
    return out


def derive_start_date(competition, start_meta):
    """
    DATE RULE: prefer the first year of a YYYY-YYYY cycle in the competition
    name; else the site 'Start Date' meta. Accept only a real 19xx/20xx year.
    Return 'YYYY-01-01' or None.
    """
    if competition:
        m = RANGE_RE.search(competition)
        if m:
            return f"{m.group(1)}-01-01"
    if start_meta:
        m = YEAR_RE.search(start_meta)
        if m:
            return f"{m.group(0)}-01-01"
    # fall back to a single year embedded in the competition (no range)
    if competition:
        m = YEAR_RE.search(competition)
        if m:
            return f"{m.group(0)}-01-01"
    return None


def researcher_items(soup):
    """
    Ordered list of (name, institution) for every researcher in the
    "Team Members" block. Each is a .grant-detail__researcher-meta-item
    carrying a -meta-label (name) and optionally a -meta-institution-name.
    Display order is preserved; the FIRST item is the lead PI.
    """
    out = []
    for it in soup.select(".grant-detail__researcher-meta-item"):
        lab = it.select_one(".grant-detail__researcher-meta-label")
        ins = it.select_one(".grant-detail__researcher-meta-institution-name")
        name = clean(lab.get_text(" ")) if lab else None
        inst = clean(ins.get_text(" ")) if ins else None
        if name:
            out.append((name, inst))
    return out


def primary_researcher(soup):
    """
    Lead PI name + their institution = the FIRST researcher meta-item.
    (Lead items may carry a --primary modifier, but ordering is authoritative;
    take the first.) Falls back to legacy name/institution selectors.
    """
    items = researcher_items(soup)
    if items:
        return items[0]

    n_el = (soup.select_one(".grant-detail__researcher-meta-label--primary")
            or soup.select_one(".grant-detail__researcher-name"))
    i_el = soup.select_one(
        ".grant-detail__researcher-meta-institution-name--primary"
    )
    name = clean(n_el.get_text(" ")) if n_el else None
    inst = clean(i_el.get_text(" ")) if i_el else None
    return name, inst


def build_description(soup, meta):
    """
    <abstract paragraph(s)> <partner line(s)> | Brain Canada Contribution: $X
    | Disease/Disorder Area: Y | Area of Research: Z
    Mirrors the convention in the pre-existing parquet.
    """
    main = soup.select_one(".grant-detail__main")
    body_parts = []
    if main:
        for p in main.select(".apos-area .text-widget p"):
            # get_text(separator=' ') turns inline <br> into a space so that
            # e.g. "Challenge<br/>CAR-T" renders as "Challenge CAR-T".
            t = clean(p.get_text(separator=" "))
            if t:
                body_parts.append(t)
    body = " ".join(body_parts).strip()

    suffix = []
    contrib = meta.get("Brain Canada Contribution")
    if contrib:
        suffix.append(f"Brain Canada Contribution: {contrib}")
    disease = meta.get("Disease / Disorder Area") or meta.get("Disease/Disorder Area")
    if disease:
        suffix.append(f"Disease/Disorder Area: {disease}")
    area = meta.get("Area of Research")
    if area:
        suffix.append(f"Area of Research: {area}")

    # Co-applicants = every team member after the lead PI, formatted
    # "Name (Institution)" or just "Name" when no institution; joined with "; ".
    coapps = []
    for name, inst in researcher_items(soup)[1:]:
        coapps.append(f"{name} ({inst})" if inst else name)
    if coapps:
        suffix.append("Co-applicants: " + "; ".join(coapps))

    pieces = []
    if body:
        pieces.append(body)
    if suffix:
        pieces.append(" | ".join(suffix))
    desc = " | ".join(pieces).strip()
    return desc or None


def parse_detail(slug, html):
    soup = BeautifulSoup(html, "html.parser")

    title_el = (soup.select_one("h1.page-banner__title")
                or soup.select_one(".page-banner__content")
                or soup.select_one("h1"))
    title = clean(title_el.get_text()) if title_el else None

    meta = meta_pairs(soup)
    pi_name, institution = primary_researcher(soup)
    pi_full, pi_given, pi_family = split_name(pi_name)

    amount = money(meta.get("Total Grant Amount") or meta.get("Grant Amount"))

    # scheme = "<Grant type> — <Competition>" when both present, else whichever
    # exists (matches the pre-existing parquet; em dash U+2014 separator).
    grant_type  = meta.get("Grant type")
    competition = meta.get("Competition")
    if grant_type and competition:
        scheme = f"{grant_type} — {competition}"
    else:
        scheme = competition or grant_type

    # date rule keys off the COMPETITION cycle (the program-year range),
    # not the combined scheme string.
    start_date_raw = derive_start_date(competition, meta.get("Start Date"))
    description = build_description(soup, meta)

    return {
        "funder_award_id": slug,
        "title": title,
        "pi_full": pi_full,
        "pi_given": pi_given,
        "pi_family": pi_family,
        "institution": institution,
        "amount": amount,
        "currency": CURRENCY,
        "scheme": scheme,
        "start_date_raw": start_date_raw,
        "end_date_raw": None,
        "description": description,
        "landing_page_url": f"{LISTING_URL}/{slug}",
    }


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    if any(a in ("-h", "--help") for a in sys.argv[1:]):
        print(__doc__)
        print("Usage: brain_canada_to_s3.py   "
              "(no args; set SKIP_UPLOAD=1 to skip the S3 upload)")
        return
    slugs, completeness_baseline = collect_all_slugs()

    # de-dup slugs defensively (also the natural-key dedup)
    seen, ordered = set(), []
    for s in slugs:
        if s not in seen:
            seen.add(s); ordered.append(s)
    slugs = ordered
    print(f"Fetching {len(slugs)} detail pages...", flush=True)

    rows, failed = [], 0
    for i, slug in enumerate(slugs, 1):
        html = get(f"{LISTING_URL}/{slug}")
        if html is None:
            failed += 1
            continue
        try:
            rows.append(parse_detail(slug, html))
        except Exception as e:  # never let one bad page kill the run
            failed += 1
            print(f"    parse error on {slug}: {e}", flush=True)
        if i % 50 == 0 or i == len(slugs):
            print(f"  detail {i}/{len(slugs)} "
                  f"({len(rows)} parsed, {failed} failed)", flush=True)

    df = pd.DataFrame(rows, columns=COLUMNS)
    df = df.drop_duplicates(subset=["funder_award_id"], keep="first")
    for c in COLUMNS:
        df[c] = df[c].astype("string")
    df = df.reset_index(drop=True)

    # (Codex review) the primary-researcher selector occasionally returns the PI's
    # surname instead of an institution — null those rather than ship a bad affiliation.
    bad_inst = (df["institution"].notna() & df["pi_family"].notna()
                & (df["institution"].str.strip().str.lower()
                   == df["pi_family"].str.strip().str.lower()))
    if bad_inst.any():
        print(f"  nulling {int(bad_inst.sum())} institution==pi_family rows", flush=True)
        df.loc[bad_inst, "institution"] = pd.NA
    # derive per-row affiliation country (Canada-when-confident, else null)
    df["country"] = df["institution"].apply(
        lambda s: "Canada" if (isinstance(s, str) and _CA_INST.search(s)) else pd.NA).astype("string")

    # ---- report fill rates -------------------------------------------------
    n = len(df)
    print("\n=== Harvest summary ===", flush=True)
    print(f"rows: {n}", flush=True)
    for c in ["title", "pi_full", "institution", "amount", "start_date_raw"]:
        filled = df[c].notna().sum()
        print(f"  {c:16}: {filled}/{n} non-null ({100*filled/max(n,1):.1f}%)",
              flush=True)

    df.to_parquet(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH}", flush=True)

    # ---- completeness guard ------------------------------------------------
    # Baseline = sitemap distinct-grant count (authoritative). A few detail
    # 404s/timeouts are tolerated; a truncated crawl is not.
    threshold = int(completeness_baseline * GUARD_MIN_FRACTION)
    print(f"\nCompleteness guard: harvested {n} vs sitemap-advertised "
          f"{completeness_baseline} (min required {threshold}, "
          f"{GUARD_MIN_FRACTION:.0%})", flush=True)
    if n < threshold:
        print("COMPLETENESS GUARD FAILED -> NOT uploading to S3.", flush=True)
        sys.exit(1)
    print("Completeness guard PASSED.", flush=True)

    # ---- upload ------------------------------------------------------------
    if os.environ.get("SKIP_UPLOAD"):
        print("SKIP_UPLOAD set -> skipping S3 upload.", flush=True)
        return
    import subprocess
    print(f"Uploading to {S3_DEST} ...", flush=True)
    rc = subprocess.call(["aws", "s3", "cp", OUT_PATH, S3_DEST])
    if rc != 0:
        print(f"aws s3 cp failed (rc={rc})", flush=True)
        sys.exit(1)
    print("Upload complete.", flush=True)


if __name__ == "__main__":
    main()
