#!/usr/bin/env python3
"""
Blavatnik Awards for Young Scientists to S3 (PRIZE PATTERN)
============================================================

Scrapes laureates and finalists of the Blavatnik Awards for Young
Scientists from the awarding body's own static-HTML profile pages on
`blavatnikawards.org`. Each profile is served at
`/honorees/profile/{slug}/` with stable structured markup (h1 = name,
h2 = `{year} {region} Award Winner — {role}`, strong-labeled key/value
pairs for Current Position, Institution, Discipline, Recognized for).

Source authority: the awarding body publishes only on
blavatnikawards.org. The Blavatnik Foundation's own site
(blavatnikfoundation.org) does not have an honorees database; the
Foundation contracts the New York Academy of Sciences to administer the
Awards and host this site. No Wikipedia/Wikidata backfill.

Output: s3://openalex-ingest/awards/blavatnik/blavatnik_awards.parquet
Awarding body in OpenAlex: Blavatnik Family Foundation (F4320312914,
DOI 10.13039/100011643).

Schema: one row per (year × region × laureate-or-finalist). Per the
Blavatnik About page, each year three Finalists are selected per
category, of whom one becomes the Laureate winning US$250,000 in
unrestricted funds. The Finalists' awards are not publicly priced on
the awarding body's site, so amount is set to 250000 for Laureates /
Winners and NULL for Finalists. Currency is USD. This is documented in
the per-funder notebook header.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://blavatnikawards.org"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/blavatnik/blavatnik_awards.parquet"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    )
}
REQUEST_DELAY = 0.5
RETRIES = 3
CHECKPOINT_FILE = "blavatnik_checkpoint.json"

# Region-specific listing pages plus the umbrella /honorees/ page (which
# also includes Israel laureates' profile links).
LISTING_PATHS = [
    "/honorees/",
    "/honorees/national-finalists/",
    "/honorees/uk-honorees/",
    "/honorees/israel-laureates/",
]

# Per the Blavatnik About page, Laureates win US$250,000. Other Finalists
# are not publicly priced.
LAUREATE_AMOUNT_USD = 250_000

# Status taxonomy as it appears in h2 markers on profile pages. "Winner"
# and "Laureate" both denote the $250,000 prize recipient; the runner-up
# Finalists carry the "Finalist" label.
LAUREATE_STATUS_TOKENS = ("Laureate", "Winner")
FINALIST_STATUS_TOKENS = ("Finalist",)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_html(url: str) -> str:
    last_err: Optional[Exception] = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            log(f"  retry {attempt+1}/{RETRIES} after error: {e}")
            time.sleep(1 + attempt)
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


def slugify(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text).strip("-")
    return text


def split_name(full: str) -> tuple[Optional[str], Optional[str]]:
    """Split 'First Middle Last' into (given, family). Strips trailing
    degree/suffix tokens. Treats the last remaining token as family."""
    if not full:
        return (None, None)
    tokens = full.strip().split()
    suffixes = {"PhD", "Ph.D.", "MD", "M.D.", "DPhil", "Jr.", "Jr", "Sr.", "Sr",
                "II", "III", "IV"}
    while tokens and tokens[-1].rstrip(",.").lstrip() in suffixes:
        tokens.pop()
    if not tokens:
        return (None, None)
    if len(tokens) == 1:
        return (None, tokens[0])
    return (" ".join(tokens[:-1]), tokens[-1])


def discover_profile_urls() -> list[str]:
    """Walk the 4 listing pages, collect unique /honorees/profile/{slug}/
    URLs across all regions. Returns absolute URLs sorted for determinism."""
    seen: set[str] = set()
    for path in LISTING_PATHS:
        url = urljoin(BASE_URL, path)
        log(f"  listing: GET {url}")
        html = fetch_html(url)
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=re.compile(r"^/honorees/profile/[a-z0-9-]+/$")):
            href = a.get("href")
            if href:
                seen.add(urljoin(BASE_URL, href))
        time.sleep(REQUEST_DELAY)
    log(f"  discovered {len(seen)} unique profile URLs across {len(LISTING_PATHS)} listings")
    return sorted(seen)


def text_after_strong(soup: BeautifulSoup, label: str) -> Optional[str]:
    """Return the text content immediately following a <strong>label</strong>
    marker, up to the next <strong>, <br>, or </p>. Used for Blavatnik's
    structured key/value pairs (Current Position, Institution, etc.)."""
    target = soup.find("strong", string=re.compile(rf"^\s*{re.escape(label)}\s*:?\s*$", re.IGNORECASE))
    if not target:
        return None
    buf: list[str] = []
    for sib in target.next_siblings:
        if getattr(sib, "name", None) == "strong":
            break
        if getattr(sib, "name", None) == "br":
            continue
        if isinstance(sib, str):
            buf.append(sib)
        else:
            buf.append(sib.get_text(" ", strip=True))
        if "\n\n" in "".join(buf):
            break
    text = " ".join(buf).strip(" :")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def parse_profile(url: str, html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1")
    name = h1.get_text(" ", strip=True) if h1 else None

    h2 = soup.find("h2")
    h2_text = h2.get_text(" ", strip=True) if h2 else ""
    # Examples:
    #   "2025 National Award Winner — Faculty"
    #   "2026 Israel Award Winner — Faculty"
    #   "2024 UK Award Finalist — Postdoctoral"
    year_match = re.search(r"\b(20\d{2})\b", h2_text)
    award_year = int(year_match.group(1)) if year_match else None
    # Blavatnik regions/tiers: National (US Laureate/Finalist), Regional
    # (US below-National), UK / "United Kingdom", Israel. "Regional" is the
    # US-only tier below National finalists — kept as its own region label
    # for downstream disambiguation rather than collapsing into National.
    # The UK h2 uses "United Kingdom" (older years) or "UK" (some shorter
    # forms); both are normalized to "UK" for stable downstream values.
    region_match = re.search(
        r"\b(National|Regional|United\s+Kingdom|UK|Israel)\b\s+Award",
        h2_text, re.IGNORECASE)
    region = region_match.group(1) if region_match else None
    if region and region.lower().replace(" ", "") == "unitedkingdom":
        region = "UK"
    status: Optional[str] = None
    if any(tok in h2_text for tok in LAUREATE_STATUS_TOKENS):
        status = "Laureate"
    elif any(tok in h2_text for tok in FINALIST_STATUS_TOKENS):
        status = "Finalist"
    role_match = re.search(r"[—\-–]\s*(Faculty|Postdoctoral|Postdoc|Post-Doc)\s*$",
                           h2_text, re.IGNORECASE)
    role = role_match.group(1) if role_match else None
    if role:
        lc = role.lower().replace("-", "")
        if lc.startswith("postdoc"):
            role = "Postdoctoral"

    current_position = text_after_strong(soup, "Current Position")
    institution = text_after_strong(soup, "Institution")
    discipline = text_after_strong(soup, "Discipline")
    citation = text_after_strong(soup, "Recognized for")
    research_summary = text_after_strong(soup, "Research Summary")

    given_name, family_name = split_name(name)

    slug = url.rstrip("/").split("/")[-1]

    amount_usd: Optional[int]
    if status == "Laureate":
        amount_usd = LAUREATE_AMOUNT_USD
    else:
        amount_usd = None

    return {
        "profile_slug": slug,
        "profile_url": url,
        "laureate_name": name,
        "laureate_given_name": given_name,
        "laureate_family_name": family_name,
        "award_year": award_year,
        "region": region,
        "status": status,
        "role": role,
        "h2_raw": h2_text or None,
        "current_position": current_position,
        "institution": institution,
        "discipline": discipline,
        "citation": citation,
        "research_summary": research_summary,
        "amount_usd": amount_usd,
        "currency": "USD" if amount_usd is not None else None,
        # No Blavatnik honoree has declined the award per the awarding
        # body's site; kept for schema parity with Fields Medal / Abel
        # Prize so the description CASE in the notebook is idempotent
        # across prize sources.
        "declined": False,
        "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def build_funder_award_id(row: dict[str, Any]) -> str:
    parts = [
        "blavatnik",
        str(row.get("award_year") or "unknown"),
        slugify(row.get("region") or "unknown"),
        slugify(row.get("role") or "unknown"),
        row["profile_slug"],
    ]
    return "-".join(parts)


def assign_award_ids(rows: list[dict[str, Any]]) -> None:
    seen: set[str] = set()
    for row in rows:
        aid = build_funder_award_id(row)
        if aid in seen:
            # Duplicate funder_award_id collisions silently merge rows
            # downstream in openalex_awards_raw; the runbook prize-table
            # rule requires raising here. See plans/awards/how-to-add-a-funder.md
            # "Two ingest patterns" line 25.
            raise RuntimeError(f"Duplicate funder_award_id would be emitted: {aid}")
        seen.add(aid)
        row["funder_award_id"] = aid


def load_checkpoint(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        log(f"  warning: checkpoint at {path} unreadable, starting fresh")
        return {}


def save_checkpoint(path: Path, by_url: dict[str, dict[str, Any]]) -> None:
    path.write_text(json.dumps(by_url, ensure_ascii=False), encoding="utf-8")


def collect_records(output_dir: Path, *, limit: Optional[int] = None,
                    resume: bool = False) -> list[dict[str, Any]]:
    checkpoint_path = output_dir / CHECKPOINT_FILE
    by_url: dict[str, dict[str, Any]] = load_checkpoint(checkpoint_path) if resume else {}
    if by_url:
        log(f"  resumed checkpoint with {len(by_url)} cached profiles")

    profile_urls = discover_profile_urls()
    if limit:
        profile_urls = profile_urls[:limit]
        log(f"  --limit {limit} active: scraping {len(profile_urls)} of total")

    for i, url in enumerate(profile_urls, 1):
        if url in by_url:
            continue
        log(f"  [{i:>3}/{len(profile_urls)}] profile {url}")
        try:
            html = fetch_html(url)
            row = parse_profile(url, html)
        except Exception as e:
            log(f"    ERROR: {e} — skipping")
            continue
        by_url[url] = row
        if i % 25 == 0:
            save_checkpoint(checkpoint_path, by_url)
        time.sleep(REQUEST_DELAY)

    save_checkpoint(checkpoint_path, by_url)
    rows = [by_url[u] for u in profile_urls if u in by_url]
    return rows


def write_parquet(rows: list[dict[str, Any]], output_dir: Path) -> Path:
    df = pd.DataFrame(rows)
    log(f"  DataFrame: {df.shape[0]} rows × {df.shape[1]} cols")
    # Defensive coverage report before string-coercion so we can spot
    # rows missing year/region/status before the cast hides numerics.
    if not df.empty:
        for col in ("award_year", "region", "status", "citation", "institution"):
            if col in df.columns:
                pct = df[col].notna().mean() * 100
                log(f"    pct_{col}: {pct:.1f}%")
    # Runbook §1.2.5: force string dtype before to_parquet so pyarrow
    # doesn't infer null-heavy columns as int.
    df = df.astype("string")
    parquet_path = output_dir / "blavatnik_awards.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"  wrote {parquet_path} ({parquet_path.stat().st_size:,} bytes)")
    return parquet_path


def upload_to_s3(parquet_path: Path) -> None:
    import boto3  # lazy import — only needed when actually uploading
    client = boto3.client("s3")
    log(f"  uploading to s3://{S3_BUCKET}/{S3_KEY}")
    client.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("  upload complete")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/blavatnik"),
                        help="Local working directory for HTML cache, checkpoint, and parquet")
    parser.add_argument("--resume", action="store_true",
                        help="Reuse cached profile fetches from a prior run's checkpoint")
    parser.add_argument("--limit", type=int, default=None,
                        help="Smoke-test with the first N profile URLs only")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Write parquet locally but skip the S3 upload step")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    log("Step 1: discovering profile URLs across regional listings")
    log("Step 2: scraping per-honoree profile pages")
    rows = collect_records(args.output_dir, limit=args.limit, resume=args.resume)
    log(f"  collected {len(rows)} profile rows")

    if not rows:
        raise RuntimeError("Parsed 0 honoree rows — blavatnikawards.org structure may have changed.")

    log("Step 3: assigning synthetic funder_award_id and dedupe-checking")
    assign_award_ids(rows)
    log(f"  {len(rows)} unique funder_award_id values; no collisions")

    log("Step 4: writing parquet")
    parquet_path = write_parquet(rows, args.output_dir)

    if args.skip_upload:
        log("--skip-upload: not uploading to S3.")
        return
    upload_to_s3(parquet_path)


if __name__ == "__main__":
    main()
