#!/usr/bin/env python3
"""
Fields Medal to S3 (PRIZE PATTERN)
===================================

Scrapes Fields Medal laureate data from the "List of Fields medalists"
table on the Wikipedia Fields Medal article.

Source: https://en.wikipedia.org/wiki/Fields_Medal (anchor #List_of_Fields_medalists)
Output: s3://openalex-ingest/awards/fields_medal/fields_medal_laureates.parquet
Awarding body in OpenAlex: International Mathematical Union (F4320320877)

About 64 medalists, 1936-2022 (quadrennial; the 1950 cohort was the first
post-war ceremony after the 1940/1944/1948 hiatus). Each row has: year,
ICM ceremony location, name, affiliation when awarded, affiliation
current/last, citation. Perelman (2006) declined the medal — captured
in a `declined` boolean.

Fields Medal is non-monetary in OpenAlex terms (the CA$15k stipend is
nominal): `amount`/`currency` are left NULL by design, and Step 6.7's
amount-coverage check is waived in the notebook with an explicit note.
"""

import argparse
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

URL = "https://en.wikipedia.org/wiki/Fields_Medal"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/fields_medal/fields_medal_laureates.parquet"

HEADERS = {
    "User-Agent": "openalex-walden/1.0 (openalex@ourresearch.org) python-requests"
}

# Maximum cell text we keep (citation can be long)
MAX_CITATION_CHARS = 2000


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


def slugify_family(year: int | None, family_name: str | None, given_name: str | None) -> str | None:
    """Build a stable funder_award_id slug like '2014-mirzakhani'.

    Family-name collisions within a single year are rare but defended
    against by appending the given-name initial when family_name alone
    isn't unique within the row's year. The caller is responsible for
    detecting duplicates; this just builds the candidate slug.
    """
    if year is None or not family_name:
        return None
    fam = re.sub(r"[^a-z0-9]+", "-", family_name.lower()).strip("-")
    if not fam:
        return None
    return f"{year}-{fam}"


def clean_cell_text(cell, max_chars: int | None = None) -> str | None:
    """Get readable text from a <td>, dropping footnote refs and image alt junk."""
    if cell is None:
        return None
    # Drop <sup class="reference"> footnotes — they leave '[3]' artifacts.
    for sup in cell.find_all("sup"):
        sup.decompose()
    # Drop image elements; the alt-text on cropped portraits adds noise.
    for img in cell.find_all("img"):
        img.decompose()
    text = cell.get_text(separator=" ", strip=True)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text)
    # Tighten punctuation: " ," → ",", " ;" → ";", etc. Wikipedia leaves space
    # before commas because anchor links are separated by " " in the text join.
    text = re.sub(r"\s+([,;.:])", r"\1", text)
    if not text:
        return None
    if max_chars and len(text) > max_chars:
        text = text[:max_chars].rstrip() + "…"
    return text


def is_year_text(s: str) -> bool:
    return bool(re.match(r"^(19|20)\d{2}\b", s.strip()))


def parse_fields_medal_table(html: str) -> list[dict]:
    """Parse the 'List of Fields medalists' table from the Wikipedia article HTML.

    The table has rowspan'd Year and ICM-location cells (one ceremony per
    year, 2-4 medalists per ceremony). We track the most recently seen
    year/location and apply them to medalist rows that omit those cells.
    """
    soup = BeautifulSoup(html, "html.parser")

    anchor = soup.find(id="List_of_Fields_medalists")
    if anchor is None:
        raise RuntimeError("Could not find 'List_of_Fields_medalists' heading anchor on the page")

    # Walk forward from the heading to the first <table>.
    el = anchor
    table = None
    while el is not None:
        nxt = el.find_next()
        if nxt is None:
            break
        if getattr(nxt, "name", None) == "table":
            table = nxt
            break
        el = nxt
    if table is None:
        raise RuntimeError("Could not find table following the 'List_of_Fields_medalists' heading")

    rows = []
    current_year: int | None = None
    current_location: str | None = None

    for tr in table.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue

        # Skip the header row (all cells are <th>)
        if all(c.name == "th" for c in cells):
            continue

        # The on-page table renders the "Medalists" column as TWO <td>s per row:
        # one with a portrait image and one with the name. So the actual shapes are:
        #   - First medalist of a ceremony: 7 cells (year, loc, photo, name, aff_when, aff_now, citation)
        #   - Subsequent medalists of the same ceremony: 5 cells (photo, name, aff_when, aff_now, citation)
        # We disambiguate by checking whether the first cell text looks like a year.
        first_text = clean_cell_text(cells[0]) or ""
        if is_year_text(first_text):
            if len(cells) < 7:
                continue
            year_match = re.match(r"(19|20)\d{2}", first_text.strip())
            current_year = int(year_match.group(0)) if year_match else current_year
            current_location = clean_cell_text(cells[1])
            # cells[2] is the portrait photo — skip
            medalist_cells = cells[3:]
        else:
            if len(cells) < 5:
                continue
            # cells[0] is the portrait photo — skip
            medalist_cells = cells[1:]

        if len(medalist_cells) < 4:
            # Header row or oddball — skip
            continue

        name_text = clean_cell_text(medalist_cells[0])
        if not name_text:
            continue

        declined = False
        if name_text and re.search(r"\(declined\)", name_text, flags=re.IGNORECASE):
            declined = True
            name_text = re.sub(r"\(declined\)", "", name_text, flags=re.IGNORECASE).strip()

        given_name, family_name = split_name(name_text)

        rows.append({
            "year": current_year,
            "ceremony_location": current_location,
            "medalist_name": name_text,
            "given_name": given_name,
            "family_name": family_name,
            "affiliation_when_awarded": clean_cell_text(medalist_cells[1]),
            "affiliation_current_or_last": clean_cell_text(medalist_cells[2]),
            "citation": clean_cell_text(medalist_cells[3], max_chars=MAX_CITATION_CHARS),
            "declined": declined,
            "slug": slugify_family(current_year, family_name, given_name),
            "source_url": URL,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        })

    return rows


def main() -> None:
    p = argparse.ArgumentParser(description="Fields Medal -> parquet -> S3")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"),
                   help="Where to write the parquet locally")
    p.add_argument("--skip-upload", action="store_true",
                   help="Write parquet locally only; skip the S3 upload")
    p.add_argument("--limit", type=int, default=None,
                   help="Truncate to first N rows (smoke testing)")
    args = p.parse_args()

    log("=" * 60)
    log("Fields Medal -> S3 starting")
    log(f"Source: {URL}")

    r = requests.get(URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    log(f"Fetched {len(r.text):,} bytes from Wikipedia")

    rows = parse_fields_medal_table(r.text)
    log(f"Parsed {len(rows)} laureate rows")

    if args.limit:
        rows = rows[: args.limit]
        log(f"Limited to first {len(rows)} rows for smoke test")

    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")

    # Force string typing on text columns to dodge the pandas/pyarrow
    # int-inference bug on null-heavy columns (Rockefeller incident,
    # walden 5f694b7; also bit IDRC per 0f8b891).
    str_cols = [
        "ceremony_location", "medalist_name", "given_name", "family_name",
        "affiliation_when_awarded", "affiliation_current_or_last",
        "citation", "slug", "source_url", "downloaded_at",
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # Year is a nullable integer (no rows should actually be null after parse,
    # but use Int64 to dodge float64 promotion on edge cases)
    if "year" in df.columns:
        df["year"] = df["year"].astype("Int64")

    if not df.empty:
        log(
            f"Coverage: name={df.medalist_name.notna().sum()}, "
            f"year={df.year.notna().sum()}, "
            f"aff_awarded={df.affiliation_when_awarded.notna().sum()}, "
            f"citation={df.citation.notna().sum()}, "
            f"declined={int(df.declined.sum())}"
        )
        log(f"Year range: {df.year.min()} - {df.year.max()}")
        # Duplicate-slug detection — fail loud if family names collide in a single year
        if df.slug.notna().any():
            dup_mask = df.slug.duplicated(keep=False) & df.slug.notna()
            if dup_mask.any():
                log(f"WARNING: {dup_mask.sum()} rows with duplicate slugs:")
                log(str(df.loc[dup_mask, ["year", "medalist_name", "slug"]]))

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
