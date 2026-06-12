#!/usr/bin/env python3
"""
Cure Parkinson's to S3 Data Pipeline
====================================

Scrapes The Cure Parkinson's Trust funded research projects and uploads a
parquet to S3 for Databricks ingestion.

Data source: cureparkinsons.org.uk. Projects are exposed via the WordPress REST
    API: /wp-json/wp/v2/research-projects?per_page=100&page=1 (19 projects;
    id/slug/title.rendered/link/content.rendered). UA "Mozilla/5.0".
    Each REST `link` resolves to a narrative detail page (200) that carries a
    structured "Trial overview" sidebar — an <h4>Trial overview</h4> followed by
    a <ul class="wp-block-list"> of "Label: Value" items (Researcher,
    Institution, Status, Start Date / Dates, ...). That block is the primary
    source for PI + institution; it is present on 16/19 pages (researcher +
    institution labels on 15/19). For the handful lacking the block, PI and
    institution are recovered from the narrative prose ("Dr/Professor <Name>
    at/from <Org>"). No funding amounts are published anywhere on the site.
    Slug = id. (Parkinson's funder; on the IAMHRF list.)

Output: s3://openalex-ingest/awards/cure_parkinsons/cure_parkinsons_grants.parquet
"""

import argparse
import html
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

BASE = "https://cureparkinsons.org.uk"
REST_API = BASE + "/wp-json/wp/v2/research-projects?per_page=100&page={}"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/cure_parkinsons/cure_parkinsons_grants.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0"}

HEADING_RE = re.compile(r"^h[2-5]$")
HONORIFIC_RE = re.compile(r"^(?:Professors?|Prof|Drs?|Mr|Mrs|Ms|Miss|Sir|Dame)\.?\s+", re.I)
# splits a researcher field at the first co-PI / affiliation boundary
PI_SPLIT_RE = re.compile(r"\s*(?:&|/|,|\band\b| at | from )\s*", re.I)
# narrative-prose PI: honorific + 2-3 capitalised name tokens
PROSE_PI_RE = re.compile(
    r"\b(?:Dr|Prof|Professor)\.?\s+"
    r"([A-Z][A-Za-z'’-]+(?:\s+[A-Z][A-Za-z'’-]+){1,2})"
)
INST_RE = re.compile(
    r"\b((?:[A-Z][\w&'.’-]+\s+){0,4}"
    r"(?:University|College|Institute|Institut|Hospital|Center|Centre|School)"
    r"(?:\s+[\w&'.,’-]+){0,5})", re.I)
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")


def split_name(name):
    """'Lorraine Kalia' -> ('Lorraine', 'Kalia'); single token -> (None, token)."""
    if not name:
        return None, None
    parts = name.split()
    if len(parts) < 2:
        return None, parts[0]
    return " ".join(parts[:-1]), parts[-1]


def clean_researcher(raw):
    """Take the first named researcher, drop the honorific, return clean name."""
    if not raw:
        return None
    first = PI_SPLIT_RE.split(raw.strip())[0].strip()
    first = HONORIFIC_RE.sub("", first).strip().rstrip(".,")
    return first or None


def parse_overview(soup):
    """Return {label: value} from the 'Trial overview' wp-block-list, if any."""
    fields = {}
    for h in soup.find_all(HEADING_RE):
        if h.get_text(strip=True).lower().startswith("trial overview"):
            ul = h.find_next_sibling()
            if ul is not None and ul.name == "ul":
                for li in ul.find_all("li"):
                    t = li.get_text(" ", strip=True)
                    if ":" in t:
                        k, v = t.split(":", 1)
                        fields[k.strip().lower()] = v.strip()
            break
    return fields


def prose_institution(content_text, pi_name):
    """Best-effort institution from prose, preferring text right after the PI."""
    window = content_text
    if pi_name:
        idx = content_text.find(pi_name)
        if idx != -1:
            window = content_text[idx: idx + 220]
    m = INST_RE.search(window) or INST_RE.search(content_text)
    if not m:
        return None
    inst = m.group(1).strip().rstrip(".,;")
    # drop a leaked leading "<PI name> at/from " prefix (keep the org only)
    inst = re.split(r"\b(?:at|from)\s+", inst)[-1].strip()
    # cut a leaked co-PI clause ("...London and Professor Camille Carroll")
    inst = re.split(r"\s+and\s+(?:Drs?|Prof|Professors?)\b", inst)[0].strip()
    # cut the greedy tail at the first lowercase word that isn't an affiliation
    # connector ("...Sheffield conducted an 18-month" -> "...Sheffield")
    inst = re.split(r"\s+(?!(?:of|for|and|the|at|in|de|la|von|der)\b)[a-z]\w*",
                    inst)[0].strip()
    inst = re.sub(r"^the\s+", "", inst, flags=re.I)          # leading article
    inst = re.sub(r"\s+and$", "", inst, flags=re.I)          # dangling " and"
    return inst.strip().rstrip(".,;") or None


def first_year(*texts):
    for t in texts:
        if t:
            m = YEAR_RE.search(t)
            if m:
                return m.group(0)
    return None


def lead_description(content_html):
    """Lead sentence(s) of the body, before the in-page 'Contents' TOC."""
    text = BeautifulSoup(content_html, "html.parser").get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    lead = text.split("Contents", 1)[0].strip()
    if len(lead) < 40:
        lead = text  # no TOC marker / very short lead -> fall back to full body
    return lead[:400] or None


def get_projects(session):
    items = []
    for pg in range(1, 4):
        r = session.get(REST_API.format(pg), timeout=40)
        if r.status_code != 200:
            break
        batch = r.json()
        if not batch:
            break
        for it in batch:
            items.append({
                "slug": it.get("slug"),
                "title": html.unescape((it.get("title") or {}).get("rendered", "")).strip(),
                "link": it.get("link"),
                "content": (it.get("content") or {}).get("rendered", ""),
            })
        if len(batch) < 100:
            break
        time.sleep(0.5)
    return items


def parse_record(item, detail_html):
    soup = BeautifulSoup(detail_html, "html.parser")
    fields = parse_overview(soup)
    content_text = BeautifulSoup(item["content"], "html.parser").get_text(" ", strip=True)

    # --- PI: structured 'Researcher' first, prose honorific-name fallback ---
    pi_name = clean_researcher(fields.get("researcher"))
    if not pi_name:
        m = PROSE_PI_RE.search(content_text)
        if m:
            pi_name = m.group(1).strip()
    given, family = split_name(pi_name)

    # --- institution: structured first, prose fallback ---
    institution = fields.get("institution") or prose_institution(content_text, pi_name)
    if institution:
        institution = institution.strip().rstrip(".,;")

    year = first_year(fields.get("start date"), fields.get("dates"),
                      fields.get("date"), fields.get("end date"))

    return {
        "funder_award_id": f"cpt-{item['slug']}",
        "title": item["title"] or None,
        "pi_given": given,
        "pi_family": family,
        "institution": institution or None,
        "description": lead_description(item["content"]),
        "year": year,
        "landing_page_url": item["link"],
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
    ap = argparse.ArgumentParser(description="Cure Parkinson's grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/cpt_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Cure Parkinson's funded projects -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers.update(HEADERS)

    items = get_projects(s)
    print(f"REST listing: {len(items)} projects")
    if len(items) < 15:
        print(f"[ERROR] only {len(items)} projects — REST API change?")
        sys.exit(1)

    rows, seen = [], set()
    for it in items:
        if not it.get("slug") or not it.get("link"):
            continue
        try:
            r = s.get(it["link"], timeout=40)
        except Exception as e:
            print(f"  [warn] fetch failed for {it['slug']}: {e}")
            continue
        if r.status_code != 200:
            print(f"  [warn] {r.status_code} for {it['slug']}")
            continue
        rec = parse_record(it, r.text)
        if rec["title"] and rec["funder_award_id"] not in seen:
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        time.sleep(0.5)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    if len(df) < 15:
        print(f"[ERROR] only {len(df)} rows after parse — aborting (shrink guard).")
        sys.exit(1)

    for c in ("title", "pi_family", "institution", "description", "year"):
        n = df[c].notna().sum()
        print(f"  {c}: {n}/{len(df)} ({round(100 * n / max(len(df), 1))}%)")

    print("\nSample rows:")
    for _, row in df.head(3).iterrows():
        print(f"  - {row['funder_award_id']} | {row['title']}")
        print(f"      PI: {row['pi_given']} {row['pi_family']} | {row['institution']} | yr {row['year']}")

    out = args.output_dir / "cure_parkinsons_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    else:
        print(f"(skip-upload) S3 target: s3://{S3_BUCKET}/{S3_KEY}")

    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
