#!/usr/bin/env python3
"""
American Epilepsy Society (AES) to S3 Data Pipeline
===================================================

Scrapes the AES early-career award recipients (aesnet.org) and uploads a parquet
to S3 for Databricks ingestion.

Data source: server-rendered HTML at
    https://www.aesnet.org/research-funding/grantees/aes-award-recipients
The page is a set of per-year `<details>` accordions; each year's
`.accordion__content` lists award blocks in document order:
    <h4>Award/scheme name</h4>
    <p><strong>Recipient Name, degrees</strong></p>
    <p>Project title</p>
    <p>Institution</p>
    <p>Mentor: ...</p>  <p>Co-Mentor: ...</p>   (optional)
~200+ recipients across 2015-2025. Amounts are published per category, not per
recipient, so amount is NULL (section 6.7 waiver). funder_scheme = the award
name; year from the accordion header. provenance `aes`, priority 325.
F4320306597 (Path A).

Output: s3://openalex-ingest/awards/aes/aes_awards.parquet

Usage:
    python aes_to_s3.py
    python aes_to_s3.py --limit 15     # smoke test
    python aes_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

from bs4 import BeautifulSoup

URL = "https://www.aesnet.org/research-funding/grantees/aes-award-recipients"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/aes/aes_awards.parquet"
EXPECTED_MIN = 120
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

INST_KEY = re.compile(r"Universit|College|Institute|Hospital|School|Center|Centre|"
                      r"Clinic|Children|Health|Medic|Foundation", re.I)
DEGREE_RE = re.compile(r"\b(MD|PhD|DO|PharmD|MPH|MSc|MS|DVM|MBBS|DPhil|ScD|RN|BSN|"
                       r"MBChB|FRCP|MEng|BMBS|MBBCh)\b", re.I)
_TITLE_RE = re.compile(r"^(Dr\.?|Prof\.?|Professor)\s+", re.I)


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).replace("\xa0", " ").strip()
    return s or None


def split_name(name):
    """'Tanya McDonald' -> ('Tanya','McDonald'); strips leading Dr./Prof."""
    if not name:
        return None, None
    n = _TITLE_RE.sub("", name).strip()
    tokens = n.split()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_name_field(raw):
    """'Tanya McDonald, MD, PhD' -> (given, family). Drop degree tails."""
    raw = clean(raw)
    if not raw:
        return None, None
    name = raw.split(",")[0].strip()
    return split_name(name)


def is_meta(txt):
    return bool(re.match(r"^(Mentor|Co-?Mentor|Co-?PI|Mentors?)\s*:", txt, re.I))


# Strong/heading text that is a scheme label, not a person name.
SCHEME_KW = re.compile(r"Fellowship|Award|Grant|Mini-Grant|Consortium|Funded|"
                       r"Program|Scholarship|Recipients", re.I)
TRAIL_INST = re.compile(
    r"((?:[A-Z][\w&.'\-]+ ){0,5}"
    r"(?:University|Institute|Hospital|College|Center|Centre|School of Medicine|"
    r"Clinic|Health System)"
    r"(?: of [A-Z][\w ]+)?)\s*$")


def parse_concat_recipient(txt):
    """Old-format single line: 'Name, degrees Title... Institution'.

    Title and institution are concatenated with no delimiter and cannot be
    cleanly separated (a trailing-institution regex grabs title words), so we
    keep the full remainder as the title and leave institution NULL rather than
    ship garbage affiliations. The name (the award-linkage key) is reliable.
    """
    parts = txt.split(",", 1)
    name_part = parts[0].strip()
    rest = parts[1].strip() if len(parts) > 1 else ""
    # Guard against malformed run-on rows where the only comma is before a
    # trailing funder name and the whole project text precedes it: a real name
    # is <=4 tokens, so anything longer is name(first 2) + title(remainder).
    name_toks = name_part.split()
    if len(name_toks) > 4:
        rest = " ".join(name_toks[2:]) + ((" " + rest) if rest else "")
        name_part = " ".join(name_toks[:2])
    given, family = split_name(name_part)
    toks = rest.split()
    while toks and DEGREE_RE.match(toks[0].strip(".,")):
        toks.pop(0)
    title = " ".join(toks) or None
    return given, family, title, None


def parse_year_block(details):
    summary = details.find("summary")
    ymatch = re.search(r"\b(20[12]\d)\b", summary.get_text(" ", strip=True) if summary else "")
    year = int(ymatch.group(1)) if ymatch else None
    content = details.find(class_="accordion__content")
    if not content:
        return []
    has_h4 = content.find("h4") is not None
    rows = []
    scheme = None

    if has_h4:
        # NEW format: <h4> scheme, <p><strong> name, following <p> = title / institution
        cur = None
        for el in content.find_all(["h4", "p"]):
            if el.name == "h4":
                scheme = clean(el.get_text(" ", strip=True))
                continue
            strong = el.find("strong")
            txt = clean(el.get_text(" ", strip=True))
            if not txt:
                continue
            stext = clean(strong.get_text(" ", strip=True)) if strong else None
            if stext:
                if SCHEME_KW.search(stext) and "," not in stext:
                    scheme = stext  # stray scheme heading wrapped in <strong>
                    continue
                if cur:
                    rows.append(cur)
                given, family = parse_name_field(stext)
                cur = {"scheme": scheme, "year": year, "given": given, "family": family,
                       "title": None, "institution": None, "mentor": None}
                continue
            if cur is None:
                continue
            if is_meta(txt):
                cur["mentor"] = cur["mentor"] or txt
            elif cur["title"] is None and not INST_KEY.search(txt):
                cur["title"] = txt
            elif cur["institution"] is None:
                cur["institution"] = txt
            elif cur["title"] is None:
                cur["title"] = txt
        if cur:
            rows.append(cur)
    else:
        # OLD format: <p><strong> = scheme heading; non-strong <p> = concatenated recipient
        for el in content.find_all("p"):
            txt = clean(el.get_text(" ", strip=True))
            if not txt:
                continue
            strong = el.find("strong")
            if strong and clean(strong.get_text(" ", strip=True)):
                scheme = clean(strong.get_text(" ", strip=True))
                continue
            if is_meta(txt):
                continue
            # Recipients in the old format always carry "Name, degrees ..." — a
            # comma-less line here is a scheme heading that wasn't bolded; skip.
            if "," not in txt:
                continue
            given, family, title, institution = parse_concat_recipient(txt)
            if family:
                rows.append({"scheme": scheme, "year": year, "given": given,
                             "family": family, "title": title,
                             "institution": institution, "mentor": None})
    return rows


def slugify(*parts):
    s = " ".join(p for p in parts if p)
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")[:120]


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
    ap = argparse.ArgumentParser(description="American Epilepsy Society awards to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/aes_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("American Epilepsy Society awards -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers["User-Agent"] = UA
    r = s.get(URL, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    details = soup.select("details.accordion-tab")
    print(f"year accordions: {len(details)}")

    rows, seen = [], set()
    for d in details:
        for rec in parse_year_block(d):
            if not rec["family"]:
                continue
            aid = f"aes-{slugify(str(rec['year']), rec['given'], rec['family'], rec['scheme'])}"
            if aid in seen:
                continue
            seen.add(aid)
            rows.append({
                "funder_award_id": aid,
                "title": rec["title"],
                "pi_given": rec["given"],
                "pi_family": rec["family"],
                "institution": rec["institution"],
                "funder_scheme": rec["scheme"],
                "mentor": rec["mentor"],
                "start_year": rec["year"],
                "landing_page_url": URL,
            })
        if args.limit and len(rows) >= args.limit:
            rows = rows[: args.limit]
            break

    if not args.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} recipients — expected >={EXPECTED_MIN}; refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows)
    df["start_year"] = pd.to_numeric(df["start_year"], errors="coerce").astype("Int64")
    for c in df.columns:
        if c != "start_year":
            df[c] = df[c].astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "funder_scheme", "start_year"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    print(f"  dupes: {df.funder_award_id.duplicated().sum()}")
    print(f"  years: {df.start_year.min()}-{df.start_year.max()}")
    out = args.output_dir / "aes_awards.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if args.limit:
        print("\nSmoke run complete (no upload with --limit).")
        return
    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
