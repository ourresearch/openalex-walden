#!/usr/bin/env python3
"""
Brain Tumour Foundation of Canada to S3 Data Pipeline
=====================================================

Scrapes Brain Tumour Foundation of Canada's research recipients and uploads a
parquet to S3 for Databricks ingestion.

Data source: braintumour.ca. The research_recipients/ page is a static listing
    of 64 recipient cards. Each card is an <h3> (text contains "Recipient") wrapped
    in an <a> to a detail page; the heading carries PI + year + award type, e.g.
    "Dr. Liana Nobre - 2025 Research Grant Recipient". Detail pages label the
    project title INCONSISTENTLY ('Project Title: "..."' OR 'Project: "..."',
    quoted or not) and carry NO consistent institution label (free text near the
    top) and NO amounts. UA "Mozilla/5.0" required. Slug (from detail URL) = id.
    (Brain-tumour funder; on the IAMHRF list.)

Output: s3://openalex-ingest/awards/btfc/btfc_grants.parquet
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

BASE = "https://www.braintumour.ca"
LISTING = BASE + "/research_recipients/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/btfc/btfc_grants.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0"}

DASH = re.compile(r"\s[–—-]\s")  # en/em/hyphen dash with surrounding spaces
TITLE_PREFIX = re.compile(r"^(Dr|Prof|Professor|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")
RECIPIENT_RE = re.compile(r"\bRecipients?\b", re.I)

# Title appears as "Project Title:" or "Project:" — value inline or on next line.
TITLE_LABEL = re.compile(r"^Project(?:\s+Title)?\s*:\s*(.*)$", re.I)
QUOTES = "“”\"‘’'"

# Best-effort institution phrase: "University of X", or "<Words> University/Institute/..."
INST_PHRASE = re.compile(
    r"((?:University|Institute|College)\s+of\s+[A-Z][\w.’'-]+(?:\s+[A-Z][\w.’'-]+)*"
    r"|(?:[A-Z][\w.&’'-]+\s+){1,5}"
    r"(?:University|Institute|Hospital|Centre|Center|College))"
)
INST_KEYWORD = re.compile(r"University|Institute|Hospital|Centre|Center|College", re.I)
# Hard stops bound the post-heading window. "Generously funded ..." preamble is
# NOT a stop — the institution often sits a line or two below it — so it is
# skipped instead (see parse_detail).
INST_STOP = re.compile(
    r"^(Project|Description|What |How |Why |Total awarded|"
    r"Midpoint|Back to Top|Supervisor|Home|Research Recipients)", re.I)
INST_SKIP = re.compile(r"^Generously funded", re.I)
INST_SEG = re.compile(r"\s[–—-]\s|,\s")  # PI – Institution – City / comma-delimited


def norm_ws(s):
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip() if s else s


def strip_quotes(s):
    if not s:
        return None
    s = s.strip().strip(QUOTES).strip().rstrip(".").strip()
    return s or None


def parse_heading(heading):
    """Return (pi_given, pi_family, year, award_type) from a card heading."""
    parts = DASH.split(heading, 1)
    pi_raw = norm_ws(parts[0])
    right = parts[1].strip() if len(parts) > 1 else ""

    ym = YEAR_RE.search(heading)
    year = ym.group(0) if ym else None

    award_type = right
    if year:
        award_type = award_type.replace(year, "")
    award_type = RECIPIENT_RE.sub("", award_type)
    award_type = norm_ws(award_type).strip(" -–—") or None

    # Lead PI only for given/family (multi-PI headings use "and"/"&"/",")
    first = re.split(r"\s+and\s+|\s*&\s*|,", pi_raw)[0].strip()
    first = TITLE_PREFIX.sub("", first).strip()
    toks = first.split()
    if not toks:
        given = family = None
    elif len(toks) == 1:
        given, family = None, toks[0]
    else:
        given, family = " ".join(toks[:-1]), toks[-1]
    return given, family, year, award_type


def get_listing(session):
    r = session.get(LISTING, timeout=40)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    items, seen = [], set()
    for h3 in soup.find_all("h3"):
        heading = norm_ws(h3.get_text(" ", strip=True))
        if "recipient" not in heading.lower():
            continue
        a = h3.find("a") or h3.find_parent("a") or h3.find_next("a")
        href = a.get("href") if a else None
        if not href:
            continue
        url = href if href.startswith("http") else BASE + href
        if url in seen:
            continue
        seen.add(url)
        items.append({"heading": heading, "url": url})
    return items


def parse_detail(html, heading):
    soup = BeautifulSoup(html, "html.parser")
    for x in soup(["script", "style", "nav", "header", "footer"]):
        x.extract()
    main = soup.find("main") or soup
    lines = [l.strip() for l in main.get_text("\n", strip=True).split("\n") if l.strip()]

    # Title — first "Project Title:"/"Project:" label, value inline or next line.
    title = None
    for i, l in enumerate(lines):
        m = TITLE_LABEL.match(l)
        if m:
            title = strip_quotes(m.group(1)) or (
                strip_quotes(lines[i + 1]) if i + 1 < len(lines) else None)
            if title:
                break

    # Institution — best-effort from the lines just under the heading.
    hidx = next((i for i, l in enumerate(lines)
                 if "–" in l and RECIPIENT_RE.search(l)), 0)
    cands = []
    for l in lines[hidx + 1:hidx + 9]:
        if INST_STOP.match(l):
            break
        if INST_SKIP.match(l):  # funding-source preamble — skip, keep scanning
            continue
        cands.append(l)

    institution = None
    # 1) clean phrase ("University of X", "<Words> Institute/Hospital/...")
    for l in cands:
        m = INST_PHRASE.search(l)
        if m:
            institution = m.group(1).strip().rstrip(",").strip()
            break
    # 2) dash/comma segment carrying a keyword (e.g. "Hospital for Sick Children")
    if institution is None:
        for l in cands:
            for seg in INST_SEG.split(l):
                seg = seg.strip().rstrip(",").strip()
                if INST_KEYWORD.search(seg) and len(seg) < 70:
                    institution = seg
                    break
            if institution:
                break
    # 3) whole line carrying a keyword (last resort)
    if institution is None:
        for l in cands:
            if INST_KEYWORD.search(l):
                institution = l
                break
    return title, institution


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
    ap = argparse.ArgumentParser(description="Brain Tumour Foundation of Canada grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/btfc_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Brain Tumour Foundation of Canada research recipients -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update(HEADERS)

    items = get_listing(s)
    print(f"Listing: {len(items)} recipient cards")
    if len(items) < 50:
        print(f"[ERROR] only {len(items)} cards — page change?")
        sys.exit(1)

    rows, seen, fallback_titles = [], set(), 0
    for it in items:
        slug = it["url"].rstrip("/").rsplit("/", 1)[-1]
        award_id = f"btfc-{slug}"
        if award_id in seen:
            continue
        given, family, year, award_type = parse_heading(it["heading"])
        try:
            r = s.get(it["url"], timeout=40)
        except Exception as e:
            print(f"  [skip] {slug}: {e}")
            continue
        if r.status_code != 200:
            print(f"  [skip] {slug}: HTTP {r.status_code}")
            continue
        title, institution = parse_detail(r.text, it["heading"])

        real_title = title is not None
        if not real_title:
            fallback_titles += 1
            title = norm_ws(f"{award_type or ''} {year or ''}") or None

        seen.add(award_id)
        rows.append({
            "funder_award_id": award_id,
            "title": title,
            "pi_given": given,
            "pi_family": family,
            "institution": institution,
            "scheme": award_type,
            "year_awarded": year,
            "landing_page_url": it["url"],
        })
        time.sleep(0.5)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    if len(df) < 50:
        print(f"[ERROR] only {len(df)} rows after parse — aborting.")
        sys.exit(1)

    for c in ("pi_family", "year_awarded", "institution", "scheme"):
        n = df[c].notna().sum()
        print(f"  {c}: {n}/{len(df)} ({round(100 * n / max(len(df), 1))}%)")
    real = len(df) - fallback_titles
    print(f"  title (real): {real}/{len(df)} ({round(100 * real / max(len(df), 1))}%) "
          f"| fallback titles: {fallback_titles}")

    out = args.output_dir / "btfc_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
