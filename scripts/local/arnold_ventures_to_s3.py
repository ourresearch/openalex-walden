#!/usr/bin/env python3
"""
Arnold Ventures to S3 Data Pipeline
====================================

Arnold Ventures (formerly Laura and John Arnold Foundation) publishes its full
grants directory through an Algolia search index. The site exposes the App ID
and a search-only API key in its public bundle.

Algolia config (extracted 2026-05-04 from arnoldventures.org/grants):
  - Application ID: PYJ9B8SLTV
  - Search API key:  d24384ea1c21933773c3f88fa6f605ea
  - Index:           grants  (~2,632 records)

Schema per record:
  title, url (note: ddev.site staging URL — convert to production), grantAmount
  (numeric USD), grantTerm ("YYYY - YYYY"), grantDescription, granteeUrl,
  fundingSource, years (array of ints), topics (array), body, outcomeDescription,
  objectID.

Output: s3://openalex-ingest/awards/arnold_ventures/arnold_ventures_projects.parquet
"""

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

ALGOLIA_APP = "PYJ9B8SLTV"
ALGOLIA_KEY = "d24384ea1c21933773c3f88fa6f605ea"
ALGOLIA_INDEX = "grants"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/arnold_ventures/arnold_ventures_projects.parquet"

ALGOLIA_HOST = f"https://{ALGOLIA_APP}-dsn.algolia.net"
HEADERS = {
    "X-Algolia-Application-Id": ALGOLIA_APP,
    "X-Algolia-API-Key": ALGOLIA_KEY,
    "Content-Type": "application/json",
}
HITS_PER_PAGE = 1000  # Algolia max for browse
REQUEST_DELAY = 0.3
RETRIES = 3


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _post_query(body: dict) -> dict:
    url = f"{ALGOLIA_HOST}/1/indexes/{ALGOLIA_INDEX}/query"
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.post(url, headers=HEADERS, json=body, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Algolia query failed: {last_err}")


def _walk_pages(filters: list, label: str) -> list[dict]:
    """Walk page-pagination for one filter slice. Caps at paginationLimitedTo (1000)."""
    out: list[dict] = []
    page = 0
    while True:
        body = {"query": "", "hitsPerPage": HITS_PER_PAGE, "page": page}
        if filters:
            body["facetFilters"] = [f for f in filters if isinstance(f, list)]
            num = [f for f in filters if isinstance(f, str)]
            if num:
                body["numericFilters"] = num
        d = _post_query(body)
        hits = d.get("hits", [])
        out.extend(hits)
        nb_pages = d.get("nbPages", 1)
        nb_hits = d.get("nbHits")
        if nb_hits and nb_hits > 1000:
            # paginationLimitedTo=1000; we'd silently miss records — caller should split further
            log(f"  ⚠ {label}: nbHits={nb_hits} exceeds pagination cap; some records will be missed")
        page += 1
        if page >= nb_pages or not hits:
            break
        time.sleep(REQUEST_DELAY)
    return out


# Arnold's index has paginationLimitedTo=1000. The full corpus is ~2,632 grants
# split across 4 fundingSource values; LJAF alone is 2,224 (over the cap). We
# slice by (fundingSource × amount-range) so every slice stays under 1,000,
# then dedupe by objectID. A "no fundingSource" sweep catches any record that
# might lack the field. Coverage is verified at the end against `nbHits`.
AMOUNT_BUCKETS = [
    ("=0",          ["grantAmount = 0"]),
    ("0-100k",      ["grantAmount > 0",        "grantAmount < 100000"]),
    ("100k-500k",   ["grantAmount >= 100000",  "grantAmount < 500000"]),
    ("500k-1M",     ["grantAmount >= 500000",  "grantAmount < 1000000"]),
    ("1M-5M",       ["grantAmount >= 1000000", "grantAmount < 5000000"]),
    ("5M+",         ["grantAmount >= 5000000"]),
]


def algolia_search_all() -> list[dict]:
    """Slice the index by fundingSource × amount range; dedupe by objectID."""
    # Discover fundingSource values dynamically
    facets = _post_query({
        "query": "", "hitsPerPage": 0, "facets": ["fundingSource"], "maxValuesPerFacet": 100,
    }).get("facets", {}).get("fundingSource", {})
    sources = sorted(facets.keys())
    expected_total = _post_query({"query": "", "hitsPerPage": 0}).get("nbHits", 0)
    log(f"  fundingSource values: {sources}; expected total: {expected_total:,}")

    by_id: dict[str, dict] = {}
    for src in sources:
        for label, num_filters in AMOUNT_BUCKETS:
            slice_label = f"{src}/{label}"
            filters = [["fundingSource:" + src]] + num_filters
            hits = _walk_pages(filters, slice_label)
            new = sum(1 for h in hits if h.get("objectID") not in by_id)
            for h in hits:
                if h.get("objectID"):
                    by_id[h["objectID"]] = h
            log(f"  slice {slice_label:<20} hits={len(hits):>4} new={new:>4} total_unique={len(by_id):,}")

    log(f"  unique objectIDs collected: {len(by_id):,} / expected {expected_total:,}")
    if len(by_id) < expected_total:
        log(f"  ⚠ MISSING {expected_total - len(by_id)} records — slicing missed some. "
            f"Consider narrower amount buckets or different facet.")
    return list(by_id.values())


def normalise_record(h: dict) -> dict:
    # Production URLs use arnoldventures.org, but the index has ddev staging URLs.
    # Convert if present so landing-page links resolve.
    url = h.get("url") or ""
    if "ddev.site" in url:
        url = url.replace("arnoldventures.ddev.site", "www.arnoldventures.org")
    grantee_url = h.get("granteeUrl") or ""
    if "ddev.site" in grantee_url:
        grantee_url = grantee_url.replace("arnoldventures.ddev.site", "www.arnoldventures.org")
    years_raw = h.get("years")
    if years_raw is None:
        years: list[int] = []
    elif isinstance(years_raw, list):
        years = [int(y) for y in years_raw if y is not None]
    else:
        years = [int(years_raw)]

    return {
        "objectID": h.get("objectID"),
        "title": h.get("title"),
        "url": url,
        # Source-fidelity money fields
        "grant_amount": h.get("grantAmount"),  # already numeric USD
        "grant_term_raw": h.get("grantTerm"),  # "YYYY - YYYY" string
        "grant_description": h.get("grantDescription"),
        "grant_body": h.get("body"),
        "outcome_description": h.get("outcomeDescription"),
        "grantee_name": (h.get("granteeUrl") or "").rsplit("/", 1)[-1].replace("-", " ").title(),
        "grantee_url": grantee_url,
        "funding_source": h.get("fundingSource"),  # "LJAF" / "AVI" / etc.
        "years_json": json.dumps(years),
        "start_year": min(years) if years else None,
        "end_year": max(years) if years else None,
        "topics_json": json.dumps(h.get("topics") or []),
        "downloaded_at": datetime.utcnow().isoformat(),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Arnold Ventures Algolia -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None,
                   help="For smoke-test: stop after N records")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    args = p.parse_args()

    log("=" * 60)
    log("Arnold Ventures -> S3 pipeline starting")

    hits = algolia_search_all()
    log(f"Total hits from Algolia: {len(hits):,}")
    if args.limit:
        hits = hits[: args.limit]
        log(f"Smoke-test mode: limited to first {len(hits)} records")

    rows = [normalise_record(h) for h in hits]
    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")
    log(f"Coverage — title: {df.title.notna().sum()}, "
        f"grant_amount: {df.grant_amount.notna().sum()}, "
        f"start_year: {df.start_year.notna().sum()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "arnold_ventures_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {parquet_path}")

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
