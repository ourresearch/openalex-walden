#!/usr/bin/env python3
"""
Alzheimer Nederland to S3 Data Pipeline
=======================================

Scrapes Alzheimer Nederland's funded-research overview and uploads a parquet to
S3 for Databricks ingestion.

Data source: alzheimer-nederland.nl/onderzoek/ons-onderzoek/1993-2021. The page
    is a JS app that server-side-renders its content into a quoted string inside
    a `window.innerWidth >= 1024 ? "<desktop html>" : "<mobile html>"` ternary;
    the HTML is HTML-entity-encoded. We extract the desktop branch, unescape it,
    and parse the grant blocks. Default UA fine. Single self-contained page (no
    API, no pagination) — ~100 highlighted projects 1993-2021 grouped by Thema.

Per-grant block (Dutch labels): short title, `Aanvrager:` (PI), `Werkzaam bij:`
    (institution), `Originele titel:` (English title -> description), `Start:`
    (month-year), `Duur:` (months), `Gevraagde bijdrage aan Alzheimer Nederland:
    € N` (EUR, Dutch thousands '.'). Coverage is uneven (some blocks omit PI or
    institution); no native ids (synthesized slug). funder_scheme = the Thema
    section.

Output: s3://openalex-ingest/awards/alzheimer_nl/alzheimer_nl_grants.parquet

Usage:
    python alzheimer_nl_to_s3.py
    python alzheimer_nl_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import html as ihtml
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

URL = "https://www.alzheimer-nederland.nl/onderzoek/ons-onderzoek/1993-2021"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/alzheimer_nl/alzheimer_nl_grants.parquet"

LABELS = ("Aanvrager:", "Werkzaam bij:", "Originele titel:", "Start:", "Duur:",
          "Gevraagde bijdrage", "Thema:", "Co-aanvrager:", "Penvoerder:",
          "Fellowship bij:", "Hoofdonderzoeker:", "Instituut:", "Toekenning:",
          "Cofinanciering", "Co-financiering", "Looptijd:")
TITLE_NEXT = ("Aanvrager:", "Originele titel:", "Hoofdonderzoeker:")
MONTHS_NL = {m: i for i, m in enumerate(
    ["januari", "februari", "maart", "april", "mei", "juni", "juli", "augustus",
     "september", "oktober", "november", "december"], 1)}


def parse_pi(raw):
    if not raw:
        return None, None
    first = re.split(r";|,| en |&|/", raw)[0].strip()
    first = re.sub(r"\b(dr|prof|drs|ir|mr|ing|MD|PhD)\.?\b", "", first, flags=re.I).strip()
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    # Dutch tussenvoegsels stay with the family name
    tussen = {"van", "de", "den", "der", "ter", "te", "von", "le", "la"}
    fam_start = len(parts) - 1
    while fam_start > 1 and parts[fam_start - 1].lower() in tussen:
        fam_start -= 1
    return " ".join(parts[:fam_start]) or None, " ".join(parts[fam_start:])


def parse_amount(raw):
    if not raw:
        return None
    s = raw.replace("\xa0", " ")
    mm = re.search(r"€\s*([\d.]+)", s)
    if mm:
        return str(int(mm.group(1).replace(".", "")))
    return None


def month_year(raw):
    if not raw:
        return None, None
    m = re.search(r"([a-z]+)\s+(\d{4})", raw.lower())
    if not m or m.group(1) not in MONTHS_NL:
        ym = re.search(r"(\d{4})", raw)
        return (f"{ym.group(1)}-01-01", ym.group(1)) if ym else (None, None)
    return f"{m.group(2)}-{MONTHS_NL[m.group(1)]:02d}-01", m.group(2)


def extract_ssr(html):
    m = re.search(r"\?\s*\"(.*?)\"\s*\n\s*:", html, re.S)
    if not m:
        raise RuntimeError("desktop SSR fragment not found (layout change)")
    return ihtml.unescape(m.group(1).replace("\\n", "\n").replace('\\"', '"'))


def is_label(l):
    return any(l.startswith(x) for x in LABELS)


def parse_blocks(frag):
    soup = BeautifulSoup(frag, "html.parser")
    for x in soup(["script", "style"]):
        x.extract()
    lines = [l.strip() for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]

    rows, thema = [], None
    i = 0
    n = len(lines)
    while i < n:
        l = lines[i]
        if l.startswith("Thema:"):
            thema = l[len("Thema:"):].strip()
            i += 1
            continue
        # a grant title = a non-label line whose next line opens a grant block
        if (not is_label(l) and i + 1 < n
                and any(lines[i + 1].startswith(t) for t in TITLE_NEXT)):
            block = {"title": l, "thema": thema}
            j = i + 1
            while j < n and is_label(lines[j]):
                lj = lines[j]
                if lj.startswith("Aanvrager:") or lj.startswith("Hoofdonderzoeker:"):
                    block.setdefault("pi", lj.split(":", 1)[-1].strip())
                elif lj.startswith("Werkzaam bij:") or lj.startswith("Instituut:"):
                    block.setdefault("institution", lj.split(":", 1)[-1].strip())
                elif lj.startswith("Originele titel:"):
                    block["orig"] = lj.split(":", 1)[-1].strip()
                elif lj.startswith("Start:"):
                    block["start"] = lj.split(":", 1)[-1].strip()
                elif lj.startswith("Gevraagde bijdrage") or lj.startswith("Cofinanciering") \
                        or lj.startswith("Co-financiering"):
                    # Alzheimer Nederland's own contribution — authoritative for this funder
                    block["amount_raw"] = lj.split(":", 1)[-1].strip()
                elif lj.startswith("Toekenning:"):
                    # total project award (may be co-funded) — fallback only
                    block.setdefault("amount_total", lj.split(":", 1)[-1].strip())
                j += 1
            rows.append(block)
            i = j
            continue
        i += 1
    return rows


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
    ap = argparse.ArgumentParser(description="Alzheimer Nederland grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/alzheimer_nl_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Alzheimer Nederland funded research -> S3")
    print("=" * 60)

    s = requests.Session()
    r = s.get(URL, timeout=60)
    r.raise_for_status()
    blocks = parse_blocks(extract_ssr(r.text))
    print(f"Grant blocks: {len(blocks)}")
    if len(blocks) < 60:
        print(f"[ERROR] only {len(blocks)} blocks — expected ~100; layout change?")
        sys.exit(1)

    rows, seen = [], set()
    for b in blocks:
        given, family = parse_pi(b.get("pi"))
        start_date, start_year = month_year(b.get("start"))
        title = b.get("title")
        slug = re.sub(r"[^a-z0-9]+", "-", (title or "").lower()).strip("-")[:70]
        aid = f"alznl-{slug}" or "alznl-untitled"
        if aid in seen:
            aid += f"-{len(seen)}"
        seen.add(aid)
        rows.append({
            "funder_award_id": aid,
            "title": title,
            "description": b.get("orig"),
            "pi_given": given,
            "pi_family": family,
            "institution": b.get("institution"),
            "amount": parse_amount(b.get("amount_raw") or b.get("amount_total")),
            "programme": b.get("thema"),
            "start_date": start_date,
            "start_year": start_year,
        })

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "start_year"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)} ({round(100*df[c].notna().sum()/len(df))}%)")
    out = args.output_dir / "alzheimer_nl_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
