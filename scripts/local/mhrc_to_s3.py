#!/usr/bin/env python3
"""
Mental Health Research Canada (MHRC) to S3 Data Pipeline
========================================================

Scrapes MHRC's funded research projects + impact profiles and uploads a parquet
to S3 for Databricks ingestion.

Data source: mhrc.ca (Squarespace). Three enumerable index pages:
    /funded-research-projects ("Ongoing Projects", 18 recipients),
    /impact/impact-profiles-grants (42 recipients), and
    /impact/impact-profiles-studentships-fellowships (59 recipients). Recipient
    entries live in Squarespace user-items-list JSON (`data-current-context`
    attribute): title = recipient name, description = <p><em>Province</em></p>
    <p>Program</p>, button.buttonLink = detail page. Both impact pages append an
    identical 7-item summer-volunteer/staff block (single-paragraph descriptions,
    no province) — excluded. Detail pages (throttled 0.4s) give institution
    best-effort: labelled "Academic Institution(s)" field on impact-profile
    pages, else bio prose ("...PhD student at Memorial University..."), else
    name-anchored "Dr. X, University of Y". The program text doubles as the
    award title and scheme; a 4-digit year is regexed from it when present.
    NO award amounts exist anywhere on the site. Default UA fine.
    (Mental-health funder; on the IAMHRF list.)

Output: s3://openalex-ingest/awards/mhrc/mhrc_grants.parquet
"""

import argparse
import hashlib
import html as htmllib
import json
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

BASE = "https://www.mhrc.ca"
INDEX_PAGES = [  # (slug for award id, url)
    ("funded", BASE + "/funded-research-projects"),
    ("grants", BASE + "/impact/impact-profiles-grants"),
    ("studentships", BASE + "/impact/impact-profiles-studentships-fellowships"),
]
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/mhrc/mhrc_grants.parquet"
THROTTLE = 0.4

_TITLE_RE = re.compile(r"^(Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
_DASH_SPLIT_RE = re.compile(r"\s+[–—-]\s+")  # en/em/hyphen with spaces

INST_WORD = (r"(?:University|Université|Institute|Institut|College|Collège|"
             r"Hospital|Polytechnic|CHU|School of [A-ZÀ-Þ]\w+)")
_CAP = r"[A-ZÀ-Þ][\w'’\-]*"
_CONN = r"(?:of|de|the|for|and|in|du|des|la|à|at)"
_PHRASE = rf"((?:{_CAP}|{_CONN})(?:\s+(?:{_CAP}|{_CONN}))*)"
_AT_RE = re.compile(rf"\b(?:at|from)\s+(?:the\s+)?{_PHRASE}")
_LABEL_RE = re.compile(r"Academic Institution\(s\)\s*\n([^\n]+)")
_TRAIL_CONN_RE = re.compile(rf"(\s+{_CONN})+$")


def parse_pi(raw):
    if not raw:
        return None, None
    first = re.split(r",| at | from ", raw)[0].strip().lstrip(":").strip()
    first = _TITLE_RE.sub("", first).strip()
    if not re.search(r"[A-Za-zÀ-ÿ]{2}", first):  # punctuation/empty -> no PI
        return None, None
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def desc_paragraphs(desc):
    """HTML-escaped Squarespace description -> list of non-empty paragraph texts."""
    desc = htmllib.unescape(desc or "")
    paras = re.findall(r"<p[^>]*>(.*?)</p>", desc, re.S)
    if not paras and desc.strip():  # tolerate non-<p> markup
        paras = [desc]
    out = []
    for p in paras:
        t = re.sub(r"<[^>]+>", " ", p)
        t = re.sub(r"\s+", " ", htmllib.unescape(t)).strip()
        if t:
            out.append(t)
    return out


def parse_index(html_text, slug, url):
    """One index page -> (recipient rows, n_skipped_non_recipient)."""
    soup = BeautifulSoup(html_text, "html.parser")
    rows, skipped = [], 0
    for node in soup.find_all(attrs={"data-current-context": True}):
        try:
            ctx = json.loads(node["data-current-context"])
        except (ValueError, KeyError):
            continue
        for it in ctx.get("userItems", []):
            name = (it.get("title") or "").strip()
            paras = desc_paragraphs(it.get("description"))
            if len(paras) == 1 and len(_DASH_SPLIT_RE.split(paras[0], 1)) == 2:
                # tolerant fallback: "Province – Program ..." in a single para
                paras = _DASH_SPLIT_RE.split(paras[0], 1)
            if not name or len(paras) < 2:
                skipped += 1  # volunteer/staff block or placeholder entry
                continue
            province, program = paras[0], " ".join(paras[1:]).strip()
            link = ((it.get("button") or {}).get("buttonLink") or "").strip()
            if link in ("", "/"):
                link = None
            elif link.startswith("/"):
                link = BASE + link
            rows.append({"name": name, "province": province, "program": program,
                         "detail_url": link, "index_slug": slug, "index_url": url})
    return rows, skipped


def extract_institution(page_text, family):
    """Best-effort institution from detail-page text. Returns (value, method)."""
    m = _LABEL_RE.search(page_text)
    if m:  # impact-profile pages carry a labelled field
        return m.group(1).strip(), "label"
    # comma-terminated lines are soft wraps ("...Dr. Jill Bally,\nUniversity of
    # Saskatchewan"); other line breaks are sentence boundaries so captures
    # can't run into the next heading ("...Saskatchewan\nWaitlists in...")
    flat = re.sub(r",\s*\n\s*", ", ", page_text)
    flat = re.sub(r"\s*\n\s*", ". ", flat)
    flat = re.sub(r"\s+", " ", flat)
    cands = []
    for sent in re.split(r"(?<=[.!?])\s+", flat):
        for am in _AT_RE.finditer(sent):
            phrase = am.group(1)
            if not re.search(INST_WORD, phrase):
                continue
            phrase = _TRAIL_CONN_RE.sub("", phrase).strip()
            pri = 0
            if family and family.lower() in sent.lower():
                pri += 2  # sentence names the recipient
            if re.search(r"\b(is|was)\s+(a|an|the)\b", sent):
                pri += 1  # biographical sentence
            cands.append((pri, phrase))
    if cands:
        cands.sort(key=lambda c: -c[0])  # stable: doc order within same priority
        return cands[0][1], "bio"
    if family:  # "Dr. Susan Petryk and Dr. Jill Bally, University of Saskatchewan"
        nohon = re.sub(r"\b(?:Dr|Prof|Mr|Mrs|Ms)\.\s+", "", flat)  # dots break [^.;]
        m = re.search(rf"{re.escape(family)}[^.;]*?,\s+(?:the\s+)?{_PHRASE}", nohon)
        if m and re.search(INST_WORD, m.group(1)):
            return _TRAIL_CONN_RE.sub("", m.group(1)).strip(), "comma"
    return None, None


def fetch_details(session, urls):
    """Fetch unique detail pages once each -> {url: visible_text}."""
    cache = {}
    for i, u in enumerate(sorted(urls)):
        try:
            r = session.get(u, timeout=40)
        except requests.RequestException:
            continue
        if r.status_code != 200:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        for x in soup(["script", "style", "nav", "header", "footer"]):
            x.extract()
        cache[u] = soup.get_text("\n", strip=True)
        if i + 1 < len(urls):
            time.sleep(THROTTLE)
    return cache


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
    ap = argparse.ArgumentParser(description="Mental Health Research Canada grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/mhrc_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--skip-details", action="store_true",
                    help="skip detail-page fetches (no institution column fill)")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Mental Health Research Canada funded projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"

    items = []
    for slug, url in INDEX_PAGES:
        try:
            r = s.get(url, timeout=40)
        except requests.RequestException as e:
            print(f"[ERROR] {url}: {e}")
            sys.exit(1)
        if r.status_code != 200:
            print(f"[ERROR] {url}: HTTP {r.status_code} — page moved?")
            sys.exit(1)
        rows, skipped = parse_index(r.text, slug, url)
        print(f"Index {slug}: {len(rows)} recipients (+{skipped} non-recipient entries skipped)")
        items.extend(rows)
        time.sleep(THROTTLE)
    print(f"Total raw entries: {len(items)}")

    # de-dupe across the 3 indexes by (name|program), first occurrence wins
    deduped, seen = [], set()
    for it in items:
        k = f"{it['name'].lower()}|{it['program'].lower()}"
        if k in seen:
            continue
        seen.add(k)
        deduped.append(it)
    print(f"De-duplicated: {len(deduped)} ({len(items) - len(deduped)} cross-index dupes dropped)")

    detail_cache, inst_methods = {}, {}
    if not args.skip_details:
        urls = {it["detail_url"] for it in deduped if it["detail_url"]}
        print(f"Fetching {len(urls)} unique detail pages (throttle {THROTTLE}s)...")
        detail_cache = fetch_details(s, urls)
        print(f"  fetched OK: {len(detail_cache)}")

    rows = []
    for it in deduped:
        given, family = parse_pi(it["name"])
        institution = None
        if it["detail_url"] and it["detail_url"] in detail_cache:
            institution, method = extract_institution(detail_cache[it["detail_url"]], family)
            if method:
                inst_methods[method] = inst_methods.get(method, 0) + 1
        ym = _YEAR_RE.search(it["program"])
        digest = hashlib.md5(f"{it['name'].lower()}|{it['program'].lower()}"
                             .encode("utf-8")).hexdigest()[:10]
        rows.append({
            "funder_award_id": f"mhrc-{it['index_slug']}-{digest}",
            "title": it["program"],
            "pi_given": given, "pi_family": family,
            "institution": institution,
            "province": it["province"],
            "scheme": it["program"],
            "year_awarded": ym.group(0) if ym else None,
            "landing_page_url": it["detail_url"] or it["index_url"],
        })

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    if len(df) < 110:
        print(f"[ERROR] only {len(df)} rows (expected ~117+) — page layout change?")
        sys.exit(1)
    for c in ("title", "pi_family", "province", "scheme", "year_awarded", "institution"):
        print(f"  {c}: {df[c].notna().sum()}/{len(df)} ({round(100*df[c].notna().sum()/max(len(df),1))}%)")
    if inst_methods:
        print(f"  institution methods: {inst_methods}")

    out = args.output_dir / "mhrc_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
