#!/usr/bin/env python3
"""
National Psoriasis Foundation to S3 Data Pipeline
=================================================

Scrapes the National Psoriasis Foundation's (NPF) funded research projects and
uploads a parquet to S3 for Databricks ingestion.

Data source: psoriasis.org. The "NPF-funded research projects" page
    (/npf-funded-research-projects/) is a single static page of accordions:
    8 `div[class*="ExpandableContentBlock"]` sections (Bridge Grants, Discovery
    Grants, Early Career Research Grants, More Than Skin Deep: Mental Health
    Grants, Psoriasis Prevention Initiative Grants, Psoriatic Arthritis
    Diagnostic Test Grants, Psoriatic Disease Research Fellowships, Translational
    Research Grants) holding 49 unique project links in `p a`. Each detail page
    carries clean labelled fields ("Principal Investigator:", "Institution:",
    "Funding Amount:", "Project Start Date:", "Project End Date:"). The project
    title lives in og:title (prefixed "NPF-Funded Research: "); the on-page <h1>
    is generic, so we fall back to the listing link text. UA "Mozilla/5.0" is
    sufficient. Pages are served as UTF-8 (meta charset) but the HTTP header
    defaults to ISO-8859-1, so parse `response.content` (not `.text`) and let
    requests percent-encode the few non-ASCII URL paths (e.g. .../γδt17-cells/).
    Slug (last URL path segment) = id. (Psoriatic-disease funder on the IAMHRF
    expanded list.)

Output: s3://openalex-ingest/awards/npf/npf_grants.parquet
"""

import argparse
import re
import subprocess
import sys
import time
from urllib.parse import urljoin
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

UA = "Mozilla/5.0"
BASE = "https://www.psoriasis.org"
LISTING = BASE + "/npf-funded-research-projects/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/npf/npf_grants.parquet"

# honorific prefix (Dr./Prof./...) to strip from a PI string
HON_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
# a trailing credential: a dotted abbreviation (M.D., Ph.D., B.Sc., M.Bio.H.Sc.)
# or a known dotless degree token — possibly several — at the end of the name.
# NPF lists PIs as "Given [Middle] Family, Degree[, Degree...]", so cutting at the
# first comma yields the name; this also strips the rare no-comma "Name M.D." form.
_DEG_TOKEN = (r"(?:[A-Za-z]{1,4}\.)+|MBBS|MSCE|MBChB|MRCP|FRCP|FAAP|FACS|PharmD|"
              r"DrPH|DPhil|MBA|BSN|MSN|ScD|DVM|EdD|PsyD|RN|RD|MSc|MPH|PhD|MD|DO|"
              r"MS|BSc|BS|BA|MA")
TRAIL_DEG_RE = re.compile(r"(?:[\s,]+(?:" + _DEG_TOKEN + r"))+[\s,]*$", re.I)
# accordion / boilerplate headings that are NOT project titles
BOILERPLATE = {"npf-funded research", "impact of npf-funded research",
               "we need your help"}


def parse_pi(raw):
    """'Eynav Klechevsky, Ph.D.' -> ('Eynav', 'Klechevsky'). Strip honorific +
    trailing degrees (like gerber_to_s3.py, extended to dotted M.D./B.Sc. forms)."""
    if not raw:
        return None, None
    name = HON_RE.sub("", raw.replace("\xa0", " ").strip())
    name = name.split(",")[0].strip()              # name precedes the first comma
    name = TRAIL_DEG_RE.sub("", name).strip()       # strip a no-comma "Name M.D."
    name = re.sub(r"\s+", " ", name).strip().rstrip(",").strip()
    if not re.search(r"[A-Za-z]{2}", name):
        return None, None
    parts = name.split()
    if len(parts) < 2:
        return None, name or None
    return " ".join(parts[:-1]), parts[-1]


def get_listing(session):
    """-> list of (scheme, title, url) for every project link, deduped by url."""
    r = session.get(LISTING, timeout=40)
    r.raise_for_status()
    soup = BeautifulSoup(r.content, "html.parser")  # .content -> bs4 sniffs UTF-8
    items, seen = [], set()
    for block in soup.select('div[class*="ExpandableContentBlock"]'):
        head = block.find(["h2", "h3", "h4"])
        scheme = head.get_text(" ", strip=True) if head else None
        for a in block.select("p a"):
            href = a.get("href")
            if not href:
                continue
            url = urljoin(BASE + "/", href)
            if url in seen:
                continue
            seen.add(url)
            items.append((scheme, a.get_text(" ", strip=True) or None, url))
    return items


def detail_title(soup):
    """Best project title from the detail page, or None to fall back to listing."""
    og = soup.find("meta", property="og:title")
    cand = (og.get("content") or "").strip() if og else ""
    cand = re.sub(r"^\s*NPF-Funded Research\s*:\s*", "", cand).strip()
    if cand and cand.lower() not in BOILERPLATE and cand.lower() != "page not found":
        return cand
    for h2 in soup.find_all("h2"):
        t = h2.get_text(" ", strip=True)
        if t and t.lower() not in BOILERPLATE:
            return t
    return None


def field(lines, label):
    """Value following 'Label:' on its line, or the next non-empty line if the
    label/value are split (tolerant of <strong>/<b>/<dt>-<dd> markup)."""
    pat = re.compile(re.escape(label) + r"\s*:\s*(.*)", re.I)
    for i, l in enumerate(lines):
        m = pat.search(l)
        if m:
            v = m.group(1).strip()
            if v:
                return v
            for j in range(i + 1, len(lines)):
                if lines[j].strip():
                    return lines[j].strip()
            return None
    return None


def parse_detail(html, scheme, listing_title, slug, url):
    soup = BeautifulSoup(html, "html.parser")
    title = detail_title(soup) or listing_title
    for x in soup(["script", "style", "nav", "header", "footer"]):
        x.extract()
    lines = [l.strip() for l in soup.get_text("\n", strip=True).split("\n")
             if l.strip()]

    pi_raw = field(lines, "Principal Investigator")
    institution = field(lines, "Institution")
    amount_raw = field(lines, "Funding Amount")
    start_raw = field(lines, "Project Start Date")
    end_raw = field(lines, "Project End Date")

    amount = None
    if amount_raw:
        digits = re.sub(r"[^\d]", "", amount_raw.split("$")[-1])
        amount = digits or None
    given, family = parse_pi(pi_raw)
    return {
        "funder_award_id": f"npf-{slug}",
        "title": title,
        "pi_given": given,
        "pi_family": family,
        "institution": institution,
        "amount": amount,
        "scheme": scheme,
        "start_date_raw": start_raw,
        "end_date_raw": end_raw,
        "landing_page_url": url,
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
    ap = argparse.ArgumentParser(description="National Psoriasis Foundation grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/npf_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("National Psoriasis Foundation funded projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    items = get_listing(s)
    print(f"Listing: {len(items)} project links")
    if len(items) < 40:
        print(f"[ERROR] only {len(items)} — page structure changed?")
        sys.exit(1)

    rows, seen, failed = [], set(), []
    for scheme, listing_title, url in items:
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        try:
            r = s.get(url, timeout=40)
            r.raise_for_status()
        except Exception as e:
            failed.append((url, str(e)[:80]))
            time.sleep(0.5)
            continue
        rec = parse_detail(r.content, scheme, listing_title, slug, url)
        if rec["title"] and rec["funder_award_id"] not in seen:
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        time.sleep(0.5)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount",
              "start_date_raw", "end_date_raw"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    print("\nPer-scheme counts:")
    for scheme, n in df["scheme"].value_counts(dropna=False).items():
        print(f"  {scheme}: {n}")
    if failed:
        print(f"\n[WARN] {len(failed)} failed pages:")
        for u, err in failed:
            print(f"  {u} -> {err}")
    if len(df) < 40:
        print(f"\n[WARN] only {len(df)} records — expected ~49; check parser")

    out = args.output_dir / "npf_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
