#!/usr/bin/env python3
"""
Chest Heart & Stroke Scotland to S3 Data Pipeline
=================================================

Scrapes Chest Heart & Stroke Scotland's funded research projects and uploads a
parquet to S3 for Databricks ingestion.

Data source: chss.org.uk/research-project/?project-type=<stroke|heart|chest>
    (3 typed listing pages, ~27/17/9 cards, 48 unique projects after de-dupe;
    a project can appear under several types — keep all, ';'-joined). Cards are
    `a.flex-small.flex-column` with an h3 title; detail pages have NO h1 (title
    comes from the listing h3) and carry labelled fields whose label/value may
    sit on split lines: `Grant holder:` ("Julian Camilleri-Brennan, University
    of Edinburgh", or multi-PI with a ';' before the institution), `Amount
    Awarded:` ("£31,560 over two years"), `Year:` ("2013 - Finished" — take the
    4-digit year from the labelled value ONLY; bare-regex matches footer 2026
    noise). Default "Mozilla/5.0" UA fine. Slug = url tail.
    (Cardio/respiratory/stroke funder; on the IAMHRF list.)

Output: s3://openalex-ingest/awards/chss/chss_grants.parquet
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

BASE = "https://www.chss.org.uk"
LIST_URL = BASE + "/research-project/?project-type={}"
PROJECT_TYPES = ("stroke", "heart", "chest")
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/chss/chss_grants.parquet"

INST_RE = re.compile(r"(Universit|College|Institute|Hospital|Infirmary|NHS|"
                     r"Trust|Centre|Center|School|Royal|Foundation|Clinic|"
                     r"Laborator|Department|Division)", re.I)
_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
ET_AL_RE = re.compile(r"\bet\.?\s*al\.?", re.I)
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
AMOUNT_RE = re.compile(r"£\s*([\d,]+(?:\.\d+)?)")
# detail-page labels -> record keys (label/value often split across text lines)
LABELS = {"grant holder": "holder", "amount awarded": "amount", "year": "year"}


def parse_pi(raw):
    """First named person of the grant-holder part -> (given, family)."""
    if not raw:
        return None, None
    first = re.split(r",|\band\b|&", raw)[0].strip()
    while _TITLE_RE.match(first):
        first = _TITLE_RE.sub("", first).strip()
    if not re.search(r"[A-Za-z]{2}", first):  # punctuation/empty -> no PI
        return None, None
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def split_grant_holder(raw):
    """'Dr A, Dr B and Dr C; Glasgow Royal Infirmary' or
    'Julian Camilleri-Brennan, University of Edinburgh' -> (pi_part, institution)."""
    if not raw:
        return None, None
    raw = raw.strip().lstrip(":").strip()
    m = ET_AL_RE.search(raw)
    if m:  # 'Dr. Terry Quinn et al. University of Glasgow' (comma optional)
        head, tail = raw[:m.start()].strip(" ,;"), raw[m.end():].strip(" ,;.")
        return head or None, tail or None
    if ";" in raw:
        head, _, tail = raw.rpartition(";")
        return head.strip() or None, tail.strip() or None
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if len(parts) > 1:
        idx = next((k for k, p in enumerate(parts[1:], 1) if INST_RE.search(p)), None)
        if idx is not None:
            return ", ".join(parts[:idx]) or None, ", ".join(parts[idx:]) or None
        return parts[0], None  # multi-PI list with no institution segment
    return raw or None, None


def get_listing(session):
    """3 typed pages -> de-duped [{url, title, types}]; keep every type a
    project appears under."""
    by_url, order = {}, []
    for ptype in PROJECT_TYPES:
        r = session.get(LIST_URL.format(ptype), timeout=40)
        if r.status_code != 200:
            print(f"  [WARN] {ptype} listing -> HTTP {r.status_code}")
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select('a.flex-small.flex-column[href*="/research-project/"]')
        print(f"  {ptype}: {len(cards)} cards")
        for c in cards:
            url = c.get("href", "").split("#")[0]
            if url.startswith("/"):
                url = BASE + url
            h3 = c.find("h3")
            title = h3.get_text(" ", strip=True) if h3 else None
            if not url or "/research-project/" not in url:
                continue
            if url not in by_url:
                by_url[url] = {"url": url, "title": title, "types": []}
                order.append(url)
            if ptype not in by_url[url]["types"]:
                by_url[url]["types"].append(ptype)
            if not by_url[url]["title"]:
                by_url[url]["title"] = title
        time.sleep(0.5)
    return [by_url[u] for u in order]


def grab_labelled(lines):
    """Pull labelled values; tolerate 'Label: value', 'Label'/': value' and
    'Label'/':'/'value' line splits. First match wins (metadata block is at top)."""
    out = {}
    for i, l in enumerate(lines):
        low = l.lower().rstrip(":").strip()
        for lab, key in LABELS.items():
            if key in out:
                continue
            if low == lab:
                j = i + 1
                while j < len(lines) and j <= i + 3:
                    nxt = lines[j].strip()
                    if nxt == ":":
                        j += 1
                        continue
                    out[key] = nxt.lstrip(":").strip()
                    break
            elif low.startswith(lab) and ":" in l:
                v = l.split(":", 1)[1].strip()
                if v:
                    out[key] = v
    return out


def parse_detail(html, listing_title, slug, types):
    soup = BeautifulSoup(html, "html.parser")
    for x in soup(["script", "style", "nav", "header", "footer"]):
        x.extract()
    lines = [l.strip() for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]
    f = grab_labelled(lines)

    pi_part, institution = split_grant_holder(f.get("holder"))
    given, family = parse_pi(pi_part)
    amount = None
    am = AMOUNT_RE.search(f.get("amount") or "")
    if am:
        amount = str(int(float(am.group(1).replace(",", ""))))
    ym = YEAR_RE.search(f.get("year") or "")  # labelled value ONLY (footer noise)
    return {
        "funder_award_id": f"chss-{slug}",
        "title": listing_title,
        "pi_given": given, "pi_family": family,
        "institution": institution,
        "amount": amount,
        "year_awarded": ym.group(0) if ym else None,
        "project_types": ";".join(types) or None,
        "landing_page_url": None,
    }


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
    ap = argparse.ArgumentParser(description="Chest Heart & Stroke Scotland grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/chss_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Chest Heart & Stroke Scotland funded projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    items = get_listing(s)
    print(f"Listing: {len(items)} unique projects")
    if len(items) < 40:
        print(f"[ERROR] only {len(items)} — page change? (expect ~48)")
        sys.exit(1)

    rows, seen = [], set()
    for it in items:
        slug = it["url"].rstrip("/").rsplit("/", 1)[-1]
        try:
            r = s.get(it["url"], timeout=40)
        except Exception:
            continue
        if r.status_code != 200:
            continue
        rec = parse_detail(r.text, it["title"], slug, it["types"])
        rec["landing_page_url"] = it["url"]
        if rec["title"] and rec["funder_award_id"] not in seen:
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        time.sleep(0.5)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "year_awarded"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)} ({round(100*df[c].notna().sum()/max(len(df),1))}%)")
    out = args.output_dir / "chss_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
