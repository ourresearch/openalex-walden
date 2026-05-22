#!/usr/bin/env python3
"""
Fields Medal to S3 (PRIZE PATTERN)
===================================

Scrapes Fields Medal laureate data from IMU's own site (the awarding body).

Source: https://www.mathunion.org/imu-awards/fields-medal
Output: s3://openalex-ingest/awards/fields_medal/fields_medal_laureates.parquet
Awarding body in OpenAlex: International Mathematical Union (F4320320877)

The IMU index page groups laureates under `<h3>{year}</h3>` inside
`<div class="list__group">`. Each laureate is an `<li class="blue-link">`
whose `<a>` links either to a personal homepage or back to the per-year
detail page (older cohorts). Declined laureates are marked with a
trailing asterisk on the name and a `<li class="free-text">` footnote.

For each year cohort we additionally fetch the year-detail page (e.g.
fields-medals-2014) to recover the ICM ceremony city and the per-laureate
citation paragraph. The citation format varies across years — many older
cohorts have no per-laureate citation, in which case we leave it NULL.

IMU does not publish affiliation when awarded in any structured form, so
`affiliation_when_awarded` and `affiliation_current_or_last` are written
as NULL. We do not backfill them from Wikipedia because the provenance
would no longer be a single funder source.

About 64 medalists, 1936-2022 (quadrennial; 1940/1944/1948 skipped due to
WWII). Each row has: year, ICM ceremony location, name, declined flag,
citation (when available), personal homepage URL.

Fields Medal is non-monetary in OpenAlex terms (the CA$15k stipend is
nominal): `amount`/`currency` are left NULL by design, and Step 6.7's
amount-coverage check is waived in the notebook with an explicit note.
"""

import argparse
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

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

INDEX_URL = "https://www.mathunion.org/imu-awards/fields-medal"
BASE_URL = "https://www.mathunion.org"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/fields_medal/fields_medal_laureates.parquet"

HEADERS = {
    "User-Agent": "openalex-walden/1.0 (openalex@ourresearch.org) python-requests"
}

MAX_CITATION_CHARS = 2000

INTER_REQUEST_DELAY_S = 0.6

CEREMONY_LOC_RE = re.compile(
    r"International Congress of Mathematicians\s+\d{4}[^.]*?\bin\s+([A-Z][A-Za-zÀ-ſ'\-\s,]{2,80}?)(?:[.,;]|\s+on\b|\s+the\b|$)",
    flags=re.S,
)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Split 'Maryam Mirzakhani' -> ('Maryam', 'Mirzakhani').

    Strips trailing degree/suffix tokens (PhD, Jr., II, etc.) before
    splitting. Last whitespace-separated token = family name; rest = given.
    Matches the proven pattern from kavli_to_s3.py (after the
    middle-initial fix in walden 7ff24a4).
    """
    if not name:
        return None, None
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def slugify_family(year: int | None, family_name: str | None) -> str | None:
    """Build a stable funder_award_id slug like '2014-mirzakhani'.

    Collisions are detected at the dataframe level (raises in main()).
    """
    if year is None or not family_name:
        return None
    fam = re.sub(r"[^a-z0-9]+", "-", family_name.lower()).strip("-")
    if not fam:
        return None
    return f"{year}-{fam}"


def clean_text(s: str | None, max_chars: int | None = None) -> str | None:
    if s is None:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\s+([,;.:])", r"\1", s)
    if not s:
        return None
    if max_chars and len(s) > max_chars:
        s = s[:max_chars].rstrip() + "…"
    return s


def parse_index(html: str) -> list[dict]:
    """Walk div.list__group blocks on the IMU index page.

    Each cohort group has an `<h3>{year}</h3>` and one `<li class="blue-link">`
    per laureate. Committee groups (which appear interleaved) have no
    blue-link items — those are skipped.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []
    for group in soup.select("div.list__group"):
        h3 = group.find("h3")
        if h3 is None:
            continue
        h3_text = h3.get_text(strip=True)
        m = re.match(r"^(19|20)\d{2}$", h3_text)
        if not m:
            continue
        year = int(h3_text)

        laureates = group.select("li.blue-link a")
        if not laureates:
            # Selection-committee group — laureates are listed as free-text
            # there; skip so we don't treat committee members as medalists.
            continue

        # Year detail link is the `<ul><li><a href="...fields-medals-{year}">`
        # after the laureate list. Use it as a fallback source_url.
        detail_url = None
        for a in group.find_all("a"):
            href = a.get("href", "") or ""
            if f"fields-medals-{year}" in href:
                detail_url = urljoin(BASE_URL, href)
                break

        for a in laureates:
            raw_name = clean_text(a.get_text(" "))
            if not raw_name:
                continue
            declined = raw_name.endswith("*")
            name = raw_name.rstrip("*").strip()
            href = (a.get("href", "") or "").strip()
            if not href:
                personal_url = None
            else:
                resolved = href if href.startswith("http") else urljoin(BASE_URL, href)
                # When IMU has no personal page for a laureate, the link
                # points back at the year detail page — treat as no
                # personal URL.
                if "/imu-awards/fields-medal/fields-medals-" in resolved:
                    personal_url = None
                else:
                    personal_url = resolved
            given_name, family_name = split_name(name)
            rows.append({
                "year": year,
                "medalist_name": name,
                "given_name": given_name,
                "family_name": family_name,
                "personal_url": personal_url,
                "declined": declined,
                "slug": slugify_family(year, family_name),
                "year_detail_url": detail_url,
            })
    return rows


def fetch_year_detail(year_url: str, session: requests.Session) -> str | None:
    if not year_url:
        return None
    try:
        r = session.get(year_url, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except requests.RequestException as e:
        log(f"WARN: failed to fetch {year_url}: {e}")
        return None
    return r.text


def parse_ceremony_location(html: str | None) -> str | None:
    """Pull the host city from the lede paragraph of a year detail page.

    Example: "At the Opening Ceremony of the International Congress of
    Mathematicians 2014 on August 13, 2014, the Fields Medals (started
    in 1936)..." — we'd return None there because the city isn't named
    in that prefix. For 2018: "...Congress of Mathematicians 2018 in
    Rio de Janeiro on August 1..." → 'Rio de Janeiro'. Older pages may
    not include the city in this format; in those cases we return None.
    """
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    for p in soup.find_all("p"):
        text = clean_text(p.get_text(" "))
        if not text:
            continue
        m = CEREMONY_LOC_RE.search(text)
        if m:
            loc = clean_text(m.group(1))
            if loc and len(loc) < 80:
                return loc
    return None


def parse_year_citations(html: str | None, cohort: list[tuple[str | None, str | None]]) -> dict[str, str]:
    """Return {family_name: citation} for as many cohort members as we can match.

    Two patterns occur on IMU year detail pages:

    - **Name-prefixed** (e.g. 2014): "Artur Avila is awarded a Fields Medal
      for his profound contributions to dynamical systems theory ..." — the
      laureate's name appears at the start of the paragraph alongside an
      "awarded" / "Medal" verb.

    - **"For ..." style** (e.g. 2018, 2022): standalone paragraphs starting
      with "For ..." and listed in the same order as the laureates on the
      page (alphabetical by family name). When the count of such paragraphs
      matches the unfilled cohort size, we pair them in order.

    Older cohorts (1936-2010) generally have no per-laureate citation text
    on IMU's site; those rows get NULL. We do not synthesize citations
    from prose that merely mentions the laureate's name — that produced
    survey/laudation excerpts (e.g. Drinfeld 1990) rather than citations.
    """
    if not html or not cohort:
        return {}
    soup = BeautifulSoup(html, "html.parser")
    skip_markers = ("copyright", "reproduced from", "©", "with friendly permission")
    paragraphs: list[str] = []
    for p in soup.find_all("p"):
        text = clean_text(p.get_text(" "))
        if not text or len(text) < 20:
            continue
        if any(m in text.lower() for m in skip_markers):
            continue
        paragraphs.append(text)

    citations: dict[str, str] = {}

    # Pass 1 — name-prefixed citations (2014 modern style)
    for fam, given in cohort:
        if not fam:
            continue
        fam_lc = fam.lower()
        given_lc = (given or "").lower()
        for text in paragraphs:
            head = text[:80].lower()
            head_matches_name = fam_lc in head or (given_lc and given_lc in head)
            if not head_matches_name:
                continue
            text_lc = text.lower()
            if "medal" not in text_lc and "awarded" not in text_lc:
                continue
            if fam not in citations or len(text) > len(citations[fam]):
                citations[fam] = text

    # Pass 2 — "For ..." citations paired by cohort order (2018/2022 style)
    for_paragraphs = [
        t for t in paragraphs
        if t.startswith("For ") and len(t) < 1500
    ]
    unfilled = [fam for fam, _ in cohort if fam and fam not in citations]
    if for_paragraphs and len(for_paragraphs) == len(unfilled):
        for fam, text in zip(unfilled, for_paragraphs):
            citations[fam] = text

    for fam, text in list(citations.items()):
        if len(text) > MAX_CITATION_CHARS:
            citations[fam] = text[:MAX_CITATION_CHARS].rstrip() + "…"
    return citations


def main() -> None:
    p = argparse.ArgumentParser(description="Fields Medal -> parquet -> S3")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"),
                   help="Where to write the parquet locally")
    p.add_argument("--skip-upload", action="store_true",
                   help="Write parquet locally only; skip the S3 upload")
    p.add_argument("--limit", type=int, default=None,
                   help="Truncate to first N rows (smoke testing)")
    p.add_argument("--skip-detail-fetch", action="store_true",
                   help="Skip the per-year detail-page fetches (faster smoke test; "
                        "ceremony_location and citation will be NULL)")
    args = p.parse_args()

    log("=" * 60)
    log("Fields Medal -> S3 starting")
    log(f"Index: {INDEX_URL}")

    session = requests.Session()
    r = session.get(INDEX_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    log(f"Fetched {len(r.text):,} bytes from IMU index")

    rows = parse_index(r.text)
    log(f"Parsed {len(rows)} laureate rows from IMU index")

    if args.limit:
        rows = rows[: args.limit]
        log(f"Limited to first {len(rows)} rows for smoke test")

    # Fetch each year detail page once, share among that year's laureates.
    year_html_cache: dict[str, str | None] = {}
    if not args.skip_detail_fetch:
        unique_urls = sorted({r_["year_detail_url"] for r_ in rows if r_["year_detail_url"]})
        log(f"Fetching {len(unique_urls)} year-detail pages from mathunion.org...")
        for i, url in enumerate(unique_urls):
            if i > 0:
                time.sleep(INTER_REQUEST_DELAY_S)
            year_html_cache[url] = fetch_year_detail(url, session)
        ok = sum(1 for h in year_html_cache.values() if h)
        log(f"Year detail pages fetched: {ok}/{len(unique_urls)} succeeded")

    # Build {year_detail_url: cohort} so we can pair "For ..." citations
    # against the laureate roster in page order.
    cohort_by_url: dict[str, list[tuple[str | None, str | None]]] = {}
    for r_ in rows:
        url = r_["year_detail_url"]
        if not url:
            continue
        cohort_by_url.setdefault(url, []).append((r_["family_name"], r_["given_name"]))

    citations_by_url: dict[str, dict[str, str]] = {}
    location_by_url: dict[str, str | None] = {}
    for url, cohort in cohort_by_url.items():
        html = year_html_cache.get(url)
        citations_by_url[url] = parse_year_citations(html, cohort)
        location_by_url[url] = parse_ceremony_location(html)

    for r_ in rows:
        url = r_["year_detail_url"]
        r_["ceremony_location"] = location_by_url.get(url)
        cite_map = citations_by_url.get(url, {})
        r_["citation"] = cite_map.get(r_["family_name"]) if r_["family_name"] else None
        # IMU does not publish affiliation in structured form — leave NULL
        # rather than backfill from Wikipedia (would taint provenance).
        r_["affiliation_when_awarded"] = None
        r_["affiliation_current_or_last"] = None
        r_["source_url"] = url or INDEX_URL
        r_["downloaded_at"] = datetime.now(timezone.utc).isoformat()
        del r_["year_detail_url"]

    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")

    # Force string typing on text columns to dodge the pandas/pyarrow
    # int-inference bug on null-heavy columns (Rockefeller incident,
    # walden 5f694b7; also bit IDRC per 0f8b891).
    str_cols = [
        "ceremony_location", "medalist_name", "given_name", "family_name",
        "personal_url", "affiliation_when_awarded", "affiliation_current_or_last",
        "citation", "slug", "source_url", "downloaded_at",
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    if "year" in df.columns:
        df["year"] = df["year"].astype("Int64")

    if not df.empty:
        log(
            f"Coverage: name={df.medalist_name.notna().sum()}, "
            f"year={df.year.notna().sum()}, "
            f"citation={df.citation.notna().sum()}, "
            f"ceremony_loc={df.ceremony_location.notna().sum()}, "
            f"personal_url={df.personal_url.notna().sum()}, "
            f"declined={int(df.declined.sum())}"
        )
        log(f"Year range: {df.year.min()} - {df.year.max()}")
        # Slug-collision: must FAIL — two laureates with the same slug
        # would produce duplicate funder_award_id and silently merge in
        # the awards table. No 1936-2022 cohort has a collision; this
        # guards future ceremonies.
        if df.slug.notna().any():
            dup_mask = df.slug.duplicated(keep=False) & df.slug.notna()
            if dup_mask.any():
                sample = df.loc[dup_mask, ["year", "medalist_name", "slug"]].to_string()
                raise RuntimeError(
                    f"Slug collision: {dup_mask.sum()} rows share a slug with another. "
                    "Two laureates have the same year+family-name and would "
                    "produce duplicate funder_award_id. Update slugify_family() "
                    f"to disambiguate (e.g., add given-name initial) before re-running.\n{sample}"
                )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "fields_medal_laureates.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {parquet_path} ({parquet_path.stat().st_size:,} bytes)")

    if args.skip_upload:
        log("--skip-upload set; done.")
        return

    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3
    s3 = boto3.client("s3")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    main()
