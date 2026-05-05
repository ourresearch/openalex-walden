#!/usr/bin/env python3
"""
Kavli Prize to S3 (PRIZE PATTERN)
==================================

Kavli Foundation publishes the full laureate corpus in the
`__NEXT_DATA__` JSON embedded in
https://kavliprize.org/laureates — no per-page iteration needed.

Each prize entry has year, field (astrophysics / nanoscience /
neuroscience), citation, and laureates (with name, institution,
locations, bio, slug). The Kavli Prize is biennial since 2008 across
3 fields × 2-4 laureates per category, so ~80 award-laureate rows
total.

Output: s3://openalex-ingest/awards/kavli/kavli_laureates.parquet
Awarding body in OpenAlex: Kavli Foundation (F4320306399)
"""

import argparse
import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

URL = "https://kavliprize.org/laureates"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/kavli/kavli_laureates.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def block_to_text(blocks) -> str:
    """Sanity render Sanity-CMS blockContent arrays to plain text."""
    if not isinstance(blocks, list):
        return ""
    parts = []
    for b in blocks:
        if isinstance(b, dict) and b.get("_type") == "block":
            for c in b.get("children") or []:
                if isinstance(c, dict) and c.get("text"):
                    parts.append(c["text"])
            parts.append("\n")
    return re.sub(r"\s+", " ", "".join(parts)).strip()


def expand_prize(prize: dict) -> list[dict]:
    year = prize.get("year")
    field = prize.get("field") or ""
    citation = (prize.get("citation") or "").strip().strip("“”\"")
    intro_text = block_to_text(prize.get("intro"))
    laureates = prize.get("laureates") or []
    out = []
    for la in laureates:
        if not isinstance(la, dict):
            continue
        name = (la.get("name") or "").strip()
        slug_obj = la.get("slug") or {}
        slug = slug_obj.get("current") if isinstance(slug_obj, dict) else None
        institution = la.get("institution")
        locations = la.get("locations") or []
        countries = []
        for loc in locations:
            if isinstance(loc, dict) and loc.get("location"):
                countries.append(loc["location"])
        bio = block_to_text(la.get("bio"))
        # Split "First Last" → first token = given, rest = family
        given_name = name.split(" ", 1)[0] if name else None
        family_name = name.split(" ", 1)[1] if name and " " in name else None
        out.append({
            "kavli_laureate_id": la.get("_id"),
            "slug": slug,
            "year": year,
            "field": field,
            "citation": citation,
            "name": name,
            "given_name": given_name,
            "family_name": family_name,
            "institution": institution,
            "countries": countries,
            "bio": bio[:2000] if bio else None,
            "intro_text": intro_text[:1500] if intro_text else None,
            "downloaded_at": datetime.utcnow().isoformat(),
        })
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Kavli Prize -> parquet -> S3")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    args = p.parse_args()

    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    log("=" * 60)
    log("Kavli Prize -> S3 starting")

    r = requests.get(URL, headers=HEADERS, timeout=30, verify=False)
    r.raise_for_status()
    m = re.search(
        r'__NEXT_DATA__"\s*type="application/json"[^>]*>([^<]+)</script>', r.text
    )
    if not m:
        raise RuntimeError("Could not find __NEXT_DATA__ on Kavli laureates page")
    payload = json.loads(m.group(1))
    prizes = payload["props"]["pageProps"]["queryResult"]["prizes"]
    log(f"Found {len(prizes)} prize entries in __NEXT_DATA__")

    rows: list[dict] = []
    for prize in prizes:
        rows.extend(expand_prize(prize))
    log(f"Total laureate rows: {len(rows)}")

    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")
    if not df.empty:
        log(f"Coverage: name={df.name.notna().sum()}, year={df.year.notna().sum()}, "
            f"field={df.field.astype(bool).sum()}, institution={df.institution.notna().sum()}")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "kavli_laureates.parquet"
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
