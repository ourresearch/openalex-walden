#!/usr/bin/env python3
"""
CURE Epilepsy (Citizens United for Research in Epilepsy) to S3 Data Pipeline
===========================================================================

Scrapes CURE Epilepsy's grants-awarded records and uploads a parquet to S3 for
Databricks ingestion.

Data source (hybrid, both first-party WordPress):
    1. REST enumeration: /wp-json/wp/v2/grants_awarded?per_page=100&page=1..3
       (X-WP-Total 292) -> id, title, excerpt (description), link.
    2. The no-JS archive cards at /our-research/grants-awarded/?pg=1..33 carry
       the fields REST omits: scheme (span.grantawards__name) and
       "PI / Institution" (span.grantawards__name.archive-bold), joined to the
       REST records by the card's detail-page href.
    Default UA fine. Amounts and award dates are NOT published (section 6.7
    waiver; WP publish dates are not award dates and are not used).

Output: s3://openalex-ingest/awards/cure_epilepsy/cure_epilepsy_grants.parquet

Usage:
    python cure_epilepsy_to_s3.py
    python cure_epilepsy_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

REST = "https://www.cureepilepsy.org/wp-json/wp/v2/grants_awarded"
ARCHIVE = "https://www.cureepilepsy.org/our-research/grants-awarded/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/cure_epilepsy/cure_epilepsy_grants.parquet"

_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss)\.?\s+", re.I)


def parse_pi(raw):
    if not raw:
        return None, None
    first = re.split(r";| and |&", raw)[0].strip()
    first = _TITLE_RE.sub("", first).strip()
    first = re.sub(r",?\s*(PhD|MD|MBBS|DVM|PharmD|ScD|DO|MPH)\.?\s*$", "", first, flags=re.I).strip().rstrip(",")
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def fetch_rest(session):
    out = []
    for page in (1, 2, 3, 4):
        r = session.get(REST, params={"per_page": 100, "page": page}, timeout=60)
        if r.status_code == 400:  # past the last page
            break
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        out.extend(batch)
        if len(batch) < 100:
            break
        time.sleep(0.4)
    return out


def fetch_cards(session):
    """slug -> {scheme, pi_raw, institution} from the no-JS archive."""
    cards = {}
    empty = 0
    for pg in range(1, 60):
        r = session.get(ARCHIVE, params={"pg": pg}, timeout=60)
        if r.status_code != 200:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        items = soup.select(".alm-item")
        if not items:
            empty += 1
            if empty >= 2:
                break
            continue
        empty = 0
        for it in items:
            a = it.find("a", href=re.compile("/grant_type/"))
            if not a:
                continue
            slug = a["href"].rstrip("/").rsplit("/", 1)[-1]
            scheme = pi_inst = None
            for sp in it.select("span.grantawards__name"):
                txt = sp.get_text(" ", strip=True)
                if "archive-bold" in (sp.get("class") or []):
                    pi_inst = txt
                else:
                    scheme = txt
            pi_raw = inst = None
            if pi_inst and "/" in pi_inst:
                pi_raw, inst = [x.strip() for x in pi_inst.split("/", 1)]
            elif pi_inst:
                pi_raw = pi_inst.strip()
            cards[slug] = {"scheme": scheme, "pi_raw": pi_raw, "institution": inst}
        time.sleep(0.4)
    return cards


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
    ap = argparse.ArgumentParser(description="CURE Epilepsy grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/cure_epilepsy_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("CURE Epilepsy grants -> S3")
    print("=" * 60)

    s = requests.Session()
    posts = fetch_rest(s)
    print(f"REST records: {len(posts)}")
    if len(posts) < 250:
        print(f"[ERROR] only {len(posts)} — expected ~292")
        sys.exit(1)
    cards = fetch_cards(s)
    print(f"Archive cards: {len(cards)}")

    rows = []
    for p in posts:
        link = p.get("link", "")
        slug = link.rstrip("/").rsplit("/", 1)[-1]
        title = BeautifulSoup(p.get("title", {}).get("rendered", ""), "html.parser").get_text(" ", strip=True)
        desc = BeautifulSoup(p.get("excerpt", {}).get("rendered", ""), "html.parser").get_text(" ", strip=True) or None
        c = cards.get(slug, {})
        given, family = parse_pi(c.get("pi_raw"))
        rows.append({
            "funder_award_id": f"cure-{p.get('id') or slug}",
            "title": title or None,
            "description": desc,
            "pi_given": given,
            "pi_family": family,
            "institution": c.get("institution"),
            "programme": c.get("scheme"),
            "landing_page_url": link or None,
        })

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "programme", "description"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = args.output_dir / "cure_epilepsy_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
