#!/usr/bin/env python3
"""
Abel Prize to S3 (PRIZE PATTERN)
=================================

Scrapes Abel Prize laureate data from the awarding body's own site
(abelprize.no), per the runbook's prize-pattern source-authority rule:
prize sources must come from the awarding body, not Wikipedia.

Source:
- Index: https://abelprize.no/winners
- Per-laureate detail: https://abelprize.no/abel-prize-laureates/{year}
Output: s3://openalex-ingest/awards/abel_prize/abel_prize_laureates.parquet
Awarding body in OpenAlex: Norwegian Academy of Science and Letters
                           ("Det Norske Videnskaps-Akademi") — F8651541334

The /winners index page renders each laureate as a card: an `<a>` linking
to `/abel-prize-laureates/{year}` wrapping an `<h2>` whose text is either
"{year}: {Name}" (most cohorts) or just "{Name}" (the current cohort, year
inferred from the href). Shared years (2004 Atiyah+Singer, 2008
Thompson+Tits, 2015 Nash+Nirenberg, 2020 Furstenberg+Margulis, 2021
Lovász+Wigderson) appear as two consecutive cards both pointing at the
same year href.

For each laureate we also fetch the per-year detail page to recover the
citation paragraph and the institution(s) line. The detail page is
narrative HTML with no structured schema (no microdata / JSON-LD), so we
grab the h2 (name), the first non-empty paragraph after it (citation if
it starts with "for", otherwise blank), and the line below the h2
(institution).

About 26 awards, 2003-2026, with 5 shared years -> 29 laureate rows.

Monetary prize:
- 2003-2018: NOK 6,000,000
- 2019-onwards: NOK 7,500,000
The notebook applies the amount + portion split in SQL; the parquet
stays source-faithful.
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

INDEX_URL = "https://abelprize.no/winners"
BASE_URL = "https://abelprize.no"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/abel_prize/abel_prize_laureates.parquet"

HEADERS = {
    "User-Agent": "openalex-walden/1.0 (openalex@ourresearch.org) python-requests"
}

DETAIL_THROTTLE_SECS = 0.5  # be polite to abelprize.no's WAF
MAX_CITATION_CHARS = 2000


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def split_name(name: str) -> tuple[str | None, str | None]:
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
    if year is None or not family_name:
        return None
    fam = re.sub(r"[^a-z0-9]+", "-", family_name.lower()).strip("-")
    if not fam:
        return None
    return f"{year}-{fam}"


def clean_text(s: str | None, max_chars: int | None = None) -> str | None:
    if not s:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\s+([,;.:])", r"\1", s)
    if not s:
        return None
    if max_chars and len(s) > max_chars:
        s = s[:max_chars].rstrip() + "…"
    return s


def parse_year_and_name(h2_text: str, fallback_year: int | None) -> tuple[int | None, str | None]:
    """Parse an h2 like '2025: Masaki Kashiwara' or 'Gerd Faltings'."""
    if not h2_text:
        return fallback_year, None
    m = re.match(r"^\s*((?:19|20)\d{2})\s*:\s*(.+?)\s*$", h2_text)
    if m:
        return int(m.group(1)), m.group(2).strip()
    return fallback_year, h2_text.strip() or None


def parse_index_cards(html: str) -> list[dict]:
    """Parse the /winners listing into a list of laureate card stubs.

    Each laureate is rendered as an h2 (name, optionally prefixed by
    "YYYY:") plus a sibling <a href="/abel-prize-laureates/YYYY"> for the
    photo/read-more links. The h2 is NOT a child of the <a>; we walk h2
    -> nearest sibling/adjacent laureate link to recover the year.

    Shared years (2021 Lovász+Wigderson etc.) produce two consecutive h2
    cards both pointing at the same year href; the (year, name) dedupe
    keeps them distinct.
    """
    soup = BeautifulSoup(html, "html.parser")
    cards: list[dict] = []
    seen = set()  # (year, name_lower) dedupe — hero section echoes the current laureate

    laureate_href_re = re.compile(r"/abel-prize-laureates/((?:19|20)\d{2})")

    for h2 in soup.find_all("h2"):
        h2_text = h2.get_text(separator=" ", strip=True)
        if not h2_text:
            continue
        # Find the nearest laureate-detail <a> (preferring preceding, falling back to following)
        a = h2.find_previous("a", href=lambda x: x and laureate_href_re.search(x))
        if a is None:
            a = h2.find_next("a", href=lambda x: x and laureate_href_re.search(x))
        if a is None:
            # Not a laureate card — some other h2 elsewhere on the page
            continue
        m = laureate_href_re.search(a["href"])
        if not m:
            continue
        year_from_href = int(m.group(1))
        year, name = parse_year_and_name(h2_text, fallback_year=year_from_href)
        if not name:
            continue
        key = (year, name.lower())
        if key in seen:
            continue
        seen.add(key)

        detail_url = urljoin(BASE_URL, a["href"].split("#")[0])
        cards.append({
            "year": year,
            "laureate_name": name,
            "detail_url": detail_url,
        })

    return cards


def _name_tokens(s: str) -> set[str]:
    """Lowercase alpha tokens of length >=3, for fuzzy h2 ↔ laureate matching."""
    return {t for t in re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ]+", (s or "").lower()) if len(t) >= 3}


def fetch_detail(detail_url: str, laureate_name: str | None = None) -> dict:
    """Fetch a /abel-prize-laureates/{year} page and extract citation + institution.

    Shared-year pages (2004, 2008, etc.) hold TWO h2 blocks — one per
    laureate — each with their own institution. We pick the h2 whose
    text shares the most tokens with `laureate_name`; if no name is
    passed, we fall back to the first h2.
    """
    r = requests.get(detail_url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    h2s = soup.find_all("h2")
    if not h2s:
        return {"citation": None, "institution": None, "page_name": None}

    # Pick the right h2 for the laureate by token overlap
    if laureate_name:
        want = _name_tokens(laureate_name)
        best_h2 = max(
            h2s,
            key=lambda h: len(_name_tokens(h.get_text(separator=" ", strip=True)) & want),
        )
    else:
        best_h2 = h2s[0]
    page_name = clean_text(best_h2.get_text(separator=" ", strip=True))

    # Layout (consistent across years): the laureate header block has
    #   <h2>YYYY: Name</h2>
    #   <div>{institution(s)}</div>
    #   <div>{citation wrapped in quotes}</div>
    #   <div>Biography Citation Press room</div>   <- nav footer, ignore
    # We pick the 1st and 2nd non-empty block-level descendants after h2.
    # Block elements here include <div>, <p>, <blockquote>; we ignore the
    # nav footer by stopping after 2 captures.
    blocks: list[str] = []
    for el in best_h2.find_all_next(["p", "div", "blockquote", "h2", "h3", "h4"], limit=40):
        if el.name in ("h2", "h3", "h4"):
            break
        # Skip elements that contain nested block descendants — we want leaf-ish text only
        if el.find(["div", "p"], recursive=False):
            continue
        text = clean_text(el.get_text(separator=" ", strip=True))
        if not text:
            continue
        # Skip the "Biography Citation Press room" nav footer (and similar)
        if re.match(r"^\s*Biography\b", text, re.IGNORECASE) and "Press" in text:
            break
        blocks.append(text)
        if len(blocks) >= 2:
            break

    institution = blocks[0] if blocks else None
    raw_citation = blocks[1] if len(blocks) > 1 else None
    citation: str | None = None
    if raw_citation:
        # Strip wrapping quote characters of any style (straight, curly, French)
        c = raw_citation.strip().strip('"“”«»‘’«»“”')
        c = c.strip()
        # Drop leading "for " or "For " — preserves the meaning while normalizing
        # If it doesn't even look like a citation, leave it None (don't mistake
        # a press-room blurb for a citation).
        if c.lower().startswith("for "):
            citation = c[:MAX_CITATION_CHARS]
        else:
            # Citation didn't match the expected pattern; leave NULL and warn.
            # Per source-authority rule, NULL is correct when the funder doesn't
            # publish the field in the expected form.
            citation = None

    return {
        "citation": citation,
        "institution": institution,
        "page_name": page_name,
    }


def assign_portions(records: list[dict]) -> None:
    """Set portion='1' for solo years and '1/N' for shared years. Mutates in place."""
    counts: dict[int, int] = {}
    for r in records:
        if r.get("year") is None:
            continue
        counts[r["year"]] = counts.get(r["year"], 0) + 1
    for r in records:
        y = r.get("year")
        n = counts.get(y, 1)
        r["portion"] = "1" if n <= 1 else f"1/{n}"


def main() -> None:
    p = argparse.ArgumentParser(description="Abel Prize -> parquet -> S3 (abelprize.no source)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true",
                   help="Write parquet locally only; skip the S3 upload")
    p.add_argument("--limit", type=int, default=None,
                   help="Truncate to first N laureates (smoke testing)")
    p.add_argument("--skip-detail", action="store_true",
                   help="Skip per-laureate detail fetches (smoke testing index parse only)")
    args = p.parse_args()

    log("=" * 60)
    log("Abel Prize -> S3 starting")
    log(f"Index: {INDEX_URL}")

    r = requests.get(INDEX_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    log(f"Fetched index ({len(r.text):,} bytes)")

    cards = parse_index_cards(r.text)
    log(f"Parsed {len(cards)} laureate cards from /winners")

    if args.limit:
        cards = cards[: args.limit]
        log(f"Limited to first {len(cards)} cards for smoke test")

    # Enrich each card with detail-page data
    records: list[dict] = []
    for i, card in enumerate(cards, start=1):
        log(f"[{i}/{len(cards)}] {card['year']} {card['laureate_name']} <- {card['detail_url']}")
        detail = {"citation": None, "institution": None, "page_name": None}
        if not args.skip_detail:
            try:
                detail = fetch_detail(card["detail_url"], laureate_name=card["laureate_name"])
            except requests.HTTPError as e:
                log(f"  WARNING: detail fetch failed ({e}) — leaving citation/institution NULL")
            time.sleep(DETAIL_THROTTLE_SECS)

        given_name, family_name = split_name(card["laureate_name"])
        records.append({
            "year": card["year"],
            "laureate_name": card["laureate_name"],
            "given_name": given_name,
            "family_name": family_name,
            "institution": detail.get("institution"),
            "citation": detail.get("citation"),
            "detail_page_name": detail.get("page_name"),
            "slug": slugify_family(card["year"], family_name),
            "source_url": card["detail_url"],
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        })

    assign_portions(records)

    df = pd.DataFrame(records)
    log(f"DataFrame shape: {df.shape}")

    # String coercion to dodge pyarrow int-inference bug on null-heavy columns
    str_cols = [
        "laureate_name", "given_name", "family_name", "institution",
        "citation", "detail_page_name", "slug", "portion",
        "source_url", "downloaded_at",
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")
    if "year" in df.columns:
        df["year"] = df["year"].astype("Int64")

    # Collision detection: per runbook, MUST raise (silently merging rows
    # in the awards table is the bug we're guarding against).
    if df.empty:
        raise RuntimeError("Parsed 0 laureate rows — abelprize.no /winners structure may have changed.")
    if df.slug.notna().any():
        dup_mask = df.slug.duplicated(keep=False) & df.slug.notna()
        if dup_mask.any():
            log("FATAL: duplicate slugs detected:")
            log(str(df.loc[dup_mask, ["year", "laureate_name", "slug"]]))
            raise RuntimeError(
                f"{int(dup_mask.sum())} rows have duplicate funder_award_id slugs — "
                "fix the slug rule (add tiebreaker) before shipping."
            )

    if not df.empty:
        log(
            f"Coverage: name={df.laureate_name.notna().sum()}, "
            f"year={df.year.notna().sum()}, "
            f"institution={df.institution.notna().sum()}, "
            f"citation={df.citation.notna().sum()}, "
            f"portion={df.portion.notna().sum()}"
        )
        log(f"Year range: {df.year.min()} - {df.year.max()}")
        log(f"Portion distribution: {df.portion.value_counts().to_dict()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "abel_prize_laureates.parquet"
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
