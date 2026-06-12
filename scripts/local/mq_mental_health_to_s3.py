#!/usr/bin/env python3
"""
MQ Mental Health Research to S3 Data Pipeline
=============================================

Scrapes MQ Mental Health Research's funded research projects and uploads a
parquet to S3 for Databricks ingestion.

Data source: mqmentalhealth.org. The site sits behind Cloudflare sgcaptcha, so
    plain requests get challenged; headless chromium (playwright) with the
    DEFAULT UA clears it (a custom UA string gets a hard 403). Flow:
      1. page.goto /research-projects/ -> 202 challenge auto-clears within a
         few seconds (poll title until it is no longer "Robot Challenge
         Screen")
      2. same browser context: GET /wp-json/wp/v2/research?per_page=100 ->
         200 JSON, X-WP-Total=65, items carry id/slug/title.rendered/link
      3. per item, GET the detail page /research/<slug>/ in the same context
         (~1.5s throttle). Detail pages carry a labelled meta block:
         "Principal investigator:" / "Institution:" / "Location:" /
         "Research award:" (label line, value on the next line) and
         "Funding Period: 2019-2022" (inline), followed by the project
         summary paragraphs.
    No amounts anywhere. PI values are short honorific'd names ("Dr. Taylor
    Keding", "Professor Rory O'Connor") -> stripped into pi_given/pi_family.
    Slug = id. (MQ is on the IAMHRF list; batch #8.)

Output: s3://openalex-ingest/awards/mq_mental_health/mq_mental_health_projects.parquet
"""

import argparse
import html as htmllib
import re
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

BASE = "https://www.mqmentalhealth.org"
WARMUP_URL = f"{BASE}/research-projects/"
REST_URL = f"{BASE}/wp-json/wp/v2/research?per_page=100"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/mq_mental_health/mq_mental_health_projects.parquet"

CHALLENGE_TITLES = ("robot challenge", "challenge", "forbidden", "attention required")
FOOTER_MARKERS = re.compile(r"^(donate|fundraise|subscribe|sign up|corporate giving|"
                            r"take part in a research project|latest news)", re.I)
LABEL_RE = re.compile(r"^(principal investigator|institution|location|country|"
                      r"research award|award scheme|funding period|funding year)s?\b",
                      re.I)
_TITLE_RE = re.compile(r"^((?:Associate\s+Professor|Assistant\s+Professor|"
                       r"Professor|Prof|Dr|Mr|Mrs|Ms|Miss|Sir|Dame|A/Prof)"
                       r"\.?\s+)+", re.I)


def parse_pi(raw):
    """First PI -> (given, family). Strip honorifics; family = last token."""
    if not raw:
        return None, None
    first = re.split(r";|,| and |&", raw)[0].strip()
    first = _TITLE_RE.sub("", first).strip()
    first = re.sub(r"\b(PhD|MD|FMedSci|OBE|MBE|CBE|FRCP|FRS)\b\.?",
                   "", first).strip().rstrip(",")
    if not re.search(r"[A-Za-z]{2}", first):  # punctuation/empty -> no PI
        return None, None
    parts = first.split()
    if len(parts) < 2:
        return (None, first or None)
    return " ".join(parts[:-1]), parts[-1]


def clean_title(rendered):
    """REST title.rendered -> plain text (strip tags, unescape entities)."""
    t = htmllib.unescape(re.sub(r"<[^>]+>", "", rendered or ""))
    return re.sub(r"\s+", " ", t).strip() or None


def warmup(page, ctx):
    """Clear the Cloudflare challenge, return the parsed REST item list."""
    page.goto(WARMUP_URL, wait_until="domcontentloaded", timeout=60000)
    deadline = time.time() + 120
    attempt = 0
    while time.time() < deadline:
        time.sleep(5)
        attempt += 1
        try:
            title = page.title()
        except Exception:
            continue  # challenge mid-navigation; just poll again
        low = title.lower()
        if not title or any(c in low for c in CHALLENGE_TITLES):
            print(f"  warmup poll {attempt}: still challenged ({title!r})")
            if attempt in (8, 16):  # re-kick the challenge
                page.goto(WARMUP_URL, wait_until="domcontentloaded", timeout=60000)
            continue
        r = ctx.request.get(REST_URL)
        total = r.headers.get("x-wp-total")
        print(f"  warmup cleared (poll {attempt}); REST status={r.status} "
              f"X-WP-Total={total}")
        if r.status == 200:
            return r.json()
    print("[ERROR] Cloudflare challenge never cleared")
    sys.exit(1)


def fetch_detail(ctx, page, url):
    """GET a detail page in the warmed-up context; one re-warmup retry."""
    for attempt in (1, 2):
        try:
            r = ctx.request.get(url, timeout=40000)
            if r.status == 200:
                return r.text()
            print(f"    status={r.status}, retrying" if attempt == 1 else
                  f"    status={r.status}, giving up")
        except Exception as e:
            print(f"    fetch error: {e}")
        if attempt == 1:  # re-warm via a real navigation, then retry once
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(6)
            except Exception:
                pass
    return None


def parse_detail(html):
    """Pull PI/institution/location/scheme/period/description off a detail page.

    The meta block renders as label lines ("Institution:") with the value on
    the following line, except "Funding Period: 2019-2022" which is inline.
    Tolerate inline values for all labels.
    """
    soup = BeautifulSoup(html, "html.parser")
    for x in soup(["script", "style", "noscript", "nav", "header", "footer", "form"]):
        x.extract()
    lines = [l.strip() for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]

    pi_raw = institution = location = scheme = period = None
    meta_end = None

    def label_value(line, idx):
        """Value after ':' on the same line, else the next line.

        An empty value makes the next line another label (e.g. an Institution-
        less page yields "Location:") — reject label-looking values.
        """
        v = line.split(":", 1)[1].strip() if ":" in line else ""
        if not v and idx + 1 < len(lines):
            v = lines[idx + 1].strip()
        if not v or LABEL_RE.match(v):
            return None
        return v

    for i, l in enumerate(lines):
        if len(l) > 60:  # labels are short; skip prose
            continue
        low = l.lower().rstrip(":").strip()
        if pi_raw is None and low.startswith("principal investigator"):
            pi_raw = label_value(l, i)
        elif institution is None and low.startswith("institution"):
            institution = label_value(l, i)
        elif location is None and low.startswith("location"):
            location = label_value(l, i)
        elif scheme is None and (low.startswith("research award")
                                 or low.startswith("award scheme")):
            scheme = label_value(l, i)
        elif period is None and (low.startswith("funding period")
                                 or low.startswith("funding year")):
            period = label_value(l, i)
            meta_end = i
        elif l.lower().startswith("[addtoany]"):
            meta_end = i if meta_end is None else max(meta_end, i)

    # Description: prose after the meta block, up to ~500 chars
    desc_parts, count = [], 0
    if meta_end is not None:
        for l in lines[meta_end + 1:]:
            if FOOTER_MARKERS.match(l):
                break
            if l.startswith("[") or len(l) < 40:  # shortcode/widget/heading noise
                continue
            desc_parts.append(l)
            count += len(l)
            if count >= 500:
                break
    if not desc_parts:  # fallback: first substantial paragraphs anywhere
        for p in soup.find_all("p"):
            t = p.get_text(" ", strip=True)
            if len(t) >= 80 and not FOOTER_MARKERS.match(t):
                desc_parts.append(t)
                count += len(t)
                if count >= 500:
                    break
    description = re.sub(r"\s+", " ", " ".join(desc_parts)).strip()[:500] or None

    return pi_raw, institution, location, scheme, period, description


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
    ap = argparse.ArgumentParser(description="MQ Mental Health projects to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/mq_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("MQ Mental Health Research projects -> S3")
    print("=" * 60)

    rows, seen, failed = [], set(), []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()  # default UA required; custom UA -> 403
        page = ctx.new_page()
        print("Warming up Cloudflare challenge...")
        items = warmup(page, ctx)
        print(f"REST list: {len(items)} projects")
        if len(items) < 50:
            print(f"[ERROR] only {len(items)} projects — REST/challenge change?")
            sys.exit(1)

        for n, it in enumerate(items, 1):
            slug = (it.get("slug") or "").strip()
            link = (it.get("link") or "").strip() or f"{BASE}/research/{slug}/"
            aid = f"mq-{slug}" if slug else None
            title = clean_title((it.get("title") or {}).get("rendered"))
            if not aid or not title or aid in seen:
                continue
            print(f"  [{n}/{len(items)}] {slug[:60]}")
            html = fetch_detail(ctx, page, link)
            if html is None:
                failed.append(slug)
                pi_raw = institution = location = scheme = period = description = None
            else:
                (pi_raw, institution, location, scheme, period,
                 description) = parse_detail(html)
            pi_given, pi_family = parse_pi(pi_raw)
            seen.add(aid)
            rows.append({
                "funder_award_id": aid,
                "title": title,
                "description": description,
                "pi_raw": pi_raw,
                "pi_given": pi_given,
                "pi_family": pi_family,
                "institution": institution,
                "location": location,
                "scheme": scheme,
                "funding_period_raw": period,
                "landing_page_url": link,
            })
            time.sleep(1.5)
        browser.close()

    if failed:
        print(f"\n[WARNING] {len(failed)} detail pages failed: {', '.join(failed)}")

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_raw", "pi_family", "institution", "location", "scheme",
              "funding_period_raw", "description"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    out = args.output_dir / "mq_mental_health_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
