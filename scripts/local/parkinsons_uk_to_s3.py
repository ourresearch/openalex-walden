#!/usr/bin/env python3
"""
Parkinson's UK to S3 Data Pipeline
==================================

Scrapes Parkinson's UK's active research grants (published as a periodically
refreshed PDF catalogue) and uploads a parquet to S3 for Databricks ingestion.

Data source: parkinsons.org.uk/research/discover/our-research-projects links a
    "...Active Grants document - <Month Year> update.pdf" under
    /sites/default/files/. The link is re-discovered each run (the filename
    changes per update); default UA is fine, Chrome UA used as fallback.
    Requires the poppler `pdftotext` binary (-layout mode).

Scope: ACTIVE grants only (~56 projects, ~GBP 37.6M as of the May 2026
    edition) — the charity publishes no historical archive. Blocks carry
    project name (+native ref like G-2506), lead researcher, start/end
    month-year, host institution, cost GBP, and Type|Stage programme.

Output: s3://openalex-ingest/awards/parkinsons_uk/parkinsons_uk_grants.parquet

Usage:
    python parkinsons_uk_to_s3.py
    python parkinsons_uk_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

BASE = "https://www.parkinsons.org.uk"
LIST_PAGE = f"{BASE}/research/discover/our-research-projects"
CHROME_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/parkinsons_uk/parkinsons_uk_grants.parquet"

MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], 1)}

_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)


def parse_pi(pi_raw):
    """First PI -> (given, family). Strip titles; split on last whitespace token."""
    if not pi_raw:
        return None, None
    first = re.split(r";| and |&", pi_raw)[0].strip()
    first = _TITLE_RE.sub("", first).strip()
    first = re.sub(r"\b(PhD|MD|FMedSci|OBE|MBE|CBE|FRCP|FRS)\b\.?", "", first).strip().rstrip(",")
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def discover_pdf_url(session):
    for ua in (None, CHROME_UA):
        headers = {"User-Agent": ua} if ua else {}
        r = session.get(LIST_PAGE, headers=headers, timeout=60)
        if r.status_code != 200:
            continue
        m = re.search(r'href="(/sites/default/files/[^"]*Active[^"]*Grants[^"]*\.pdf)"',
                      r.text, re.I)
        if m:
            return BASE + m.group(1)
    return None


def month_year_to_date(s):
    m = re.match(r"(\w+)\s+(\d{4})", s or "")
    if not m or m.group(1) not in MONTHS:
        return None
    return f"{m.group(2)}-{MONTHS[m.group(1)]:02d}-01"


def parse_amount(raw):
    """'£277,706' / '£1.5 million' / '£2m' -> GBP integer string."""
    if not raw:
        return None
    s = raw.replace(",", "").replace("\xa0", " ")
    m = re.search(r"£\s*(\d+(?:\.\d+)?)\s*(million|m\b)?", s, re.I)
    if not m:
        return None
    val = float(m.group(1))
    if m.group(2):
        val *= 1_000_000
    return str(int(val))


LABELS = ("Lead researcher", "Start and end date", "Location", "Cost", "Type:")
REF_RE = re.compile(r"\(([A-Z]{1,3}-?\d{3,4}[A-Z]?)\)")


def field_after(lines, idx, label):
    """Value following `label` starting at line `idx`, joining wrapped lines."""
    first = lines[idx].strip()
    out = [first[len(label):].strip()] if first.startswith(label) else []
    for ln in lines[idx + 1:idx + 4]:
        stripped = ln.strip()
        if not stripped or any(stripped.startswith(l) for l in LABELS):
            break
        if re.fullmatch(r"\d+", stripped):  # page number
            continue
        out.append(stripped)
    return " ".join(x for x in out if x).strip() or None


def parse_blocks(text):
    """Anchor each grant on its 'Project name' line; the title is same-line
    (section-1 layout) or the non-blank line directly above (section-2 wrap),
    with the (REF) on the line below. Fields are read forward by label."""
    lines = text.splitlines()
    pn_idx = [i for i, ln in enumerate(lines) if re.search(r"\bProject name\b", ln)]
    rows = []
    for i in pn_idx:
        same = re.sub(r".*?Project name\s*", "", lines[i]).strip()
        pre = lines[i - 1].strip() if i > 0 else ""
        if re.fullmatch(r"\d+", pre) or any(pre.startswith(l) for l in LABELS):
            pre = ""  # page number / bled-in label, not a title
        # title window: pre-line (if any) + same-line + following lines up to a label
        parts = []
        if not same and pre:
            parts.append(pre)
        if same:
            parts.append(same)
        for ln in lines[i + 1:i + 4]:
            s = ln.strip()
            if not s or any(s.startswith(l) for l in LABELS):
                break
            if re.fullmatch(r"\d+", s):
                continue
            parts.append(s)
        title_raw = " ".join(p for p in parts if p).strip()

        ref = None
        m = REF_RE.search(title_raw)
        if m:
            ref = m.group(1)
            title_raw = (title_raw[:m.start()] + title_raw[m.end():]).strip()
        title = re.sub(r"\s+", " ", title_raw).strip(" -–—") or None

        # fields: scan the block forward to the next Project name
        nxt = next((j for j in pn_idx if j > i), len(lines))
        pi = dates = location = cost = None
        prog = None
        for j in range(i, min(nxt, len(lines))):
            s = lines[j].strip()
            if s.startswith("Lead researcher"):
                pi = field_after(lines, j, "Lead researcher")
            elif s.startswith("Start and end date"):
                dates = field_after(lines, j, "Start and end date")
            elif s.startswith("Location"):
                location = field_after(lines, j, "Location")
            elif s.startswith("Cost"):
                cost = field_after(lines, j, "Cost")
            elif s.startswith("Type:"):
                pm = re.search(r"Type:\s*([^|\n]+)\|\s*Stage:\s*([^\n]+)", s)
                if pm:
                    prog = f"{pm.group(1).strip()} | {pm.group(2).strip()}"

        if not (pi or cost):  # not a real grant block (header/contents echo)
            continue

        start_date = end_date = None
        if dates:
            dm = re.search(r"(\w+\s+\d{4})\s+to\s+(\w+\s+\d{4})", dates)
            if dm:
                start_date = month_year_to_date(dm.group(1))
                end_date = month_year_to_date(dm.group(2))

        slug = re.sub(r"[^a-z0-9]+", "-", (title or "untitled").lower()).strip("-")[:80]
        pi_given, pi_family = parse_pi(pi)
        rows.append({
            "funder_award_id": ref or slug,
            "title": title,
            "pi_name": pi,
            "pi_given": pi_given,
            "pi_family": pi_family,
            "institution": location,
            "amount": parse_amount(cost),
            "start_date": start_date,
            "end_date": end_date,
            "start_year": start_date[:4] if start_date else None,
            "programme": prog,
        })
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
    ap = argparse.ArgumentParser(description="Parkinson's UK active grants PDF to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/parkinsons_uk_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--pdf", type=Path, default=None, help="parse a local PDF instead")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Parkinson's UK active research grants -> S3")
    print("=" * 60)

    s = requests.Session()
    if args.pdf:
        pdf_path = args.pdf
        print(f"Using local PDF: {pdf_path}")
    else:
        url = discover_pdf_url(s)
        if not url:
            print("[ERROR] could not discover the Active Grants PDF link")
            sys.exit(1)
        print(f"PDF: {url}")
        r = s.get(url, headers={"User-Agent": CHROME_UA}, timeout=120)
        r.raise_for_status()
        pdf_path = args.output_dir / "active_grants.pdf"
        pdf_path.write_bytes(r.content)

    with tempfile.NamedTemporaryFile(suffix=".txt") as tf:
        subprocess.run(["pdftotext", "-layout", str(pdf_path), tf.name], check=True,
                       capture_output=True)
        text = Path(tf.name).read_text(encoding="utf-8", errors="replace")

    rows = parse_blocks(text)
    if len(rows) < 40:
        print(f"[ERROR] only {len(rows)} projects parsed — expected ~56; layout change?")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    dupes = df["funder_award_id"].duplicated().sum()
    if dupes:
        print(f"[WARN] {dupes} duplicate ids; suffixing")
        df["funder_award_id"] = df["funder_award_id"] + df.groupby(
            "funder_award_id").cumcount().map(lambda i: "" if i == 0 else f"-{i+1}")

    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("pi_name", "institution", "amount", "start_date", "programme"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)}")
    out = args.output_dir / "parkinsons_uk_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
