#!/usr/bin/env python3
"""
Carl-Zeiss-Stiftung to S3 Data Pipeline
=======================================

Carl Zeiss Foundation publishes its project list as static HTML detail pages
under https://www.carl-zeiss-stiftung.de/en/project-overview/detail/{slug}.
There is no API, but the EN sitemap enumerates all detail URLs (~226 of them
as of 2026-05).

This script:
1. Pulls the czs_project sitemap to enumerate every detail URL.
2. Fetches each detail page and parses fields out of the rendered text:
   programme, type of funding, funded institution, target group,
   funding budget (parsed and raw), period of time (parsed and raw),
   project description (text after the H1).
3. Writes a parquet to S3.

Currency is hardcoded to EUR (Carl-Zeiss-Stiftung is a German foundation;
all "Funding budget" amounts on the site are in EUR).

Requirements:
    pip install pandas pyarrow requests beautifulsoup4
    AWS CLI configured with write access to s3://openalex-ingest/awards/carl_zeiss/
"""

import argparse
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

SITEMAP_URL = (
    "https://www.carl-zeiss-stiftung.de/en/?sitemap=czs_project"
    "&type=1533906435&cHash=e2392284ab2d7e849a5cc7667bb17124"
)
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/carl_zeiss/carl_zeiss_projects.parquet"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"
    ),
}

REQUEST_DELAY = 0.5  # seconds — be polite, ~226 pages = ~2 min total
REQUEST_TIMEOUT = 30


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def discover_detail_urls() -> list[str]:
    log(f"Fetching sitemap: {SITEMAP_URL}")
    resp = requests.get(SITEMAP_URL, timeout=REQUEST_TIMEOUT, headers=HEADERS)
    resp.raise_for_status()
    urls = re.findall(
        r"<loc>(https://www\.carl-zeiss-stiftung\.de/en/project-overview/detail/[^<]+)</loc>",
        resp.text,
    )
    urls = sorted(set(urls))
    log(f"Found {len(urls)} project detail URLs")
    return urls


# ---- field extraction --------------------------------------------------

# German amount style: "8.000.000 €" or "8.000.000,50 €". Also accept plain "8000000".
AMOUNT_RE = re.compile(
    r"(?P<num>\d{1,3}(?:[.\s]\d{3})*(?:,\d+)?|\d+(?:,\d+)?)\s*€"
)

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}

# "Period of time: January 2019 - December 2026" or with year-only / single date
PERIOD_RE = re.compile(
    r"(?:(?P<sm>[A-Za-z]+)\s+)?(?P<sy>\d{4})"
    r"\s*[-–—to]+\s*"
    r"(?:(?P<em>[A-Za-z]+)\s+)?(?P<ey>\d{4})"
)


def parse_eur_amount(text: str) -> float | None:
    """Parse '8.000.000 €' or '8.000.000,50 €' to a float."""
    if not text:
        return None
    m = AMOUNT_RE.search(text)
    if not m:
        return None
    raw = m.group("num").replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return None


def parse_period(text: str) -> tuple[str | None, str | None]:
    """Return (start_iso, end_iso) — best effort. Days default to 1."""
    if not text:
        return None, None
    m = PERIOD_RE.search(text)
    if not m:
        return None, None
    sm = (m.group("sm") or "").lower()
    em = (m.group("em") or "").lower()
    sy = int(m.group("sy"))
    ey = int(m.group("ey"))
    smo = MONTH_MAP.get(sm, 1)
    emo = MONTH_MAP.get(em, 12)
    return f"{sy:04d}-{smo:02d}-01", f"{ey:04d}-{emo:02d}-01"


# Field labels we look for on every page. These are colon-suffixed labels
# rendered inside the "Detailed information" block at the bottom.
FIELD_LABELS = [
    "Programme",
    "Type of funding",
    "Funded institution",
    "Target group",
    "Funding budget",
    "Period of time",
    "Funding line",
    "Funding region",
    "Funding topic",
    "Field of action",
]


def extract_labelled_fields(page_text: str) -> dict[str, str]:
    """Walk the rendered text and pull out 'Label: Value' pairs.

    The page repeats some labels (e.g. "Funded institution" appears once
    near the top with the real value, and again in the lower CTA block
    where it bleeds into navigation chrome). For each label we collect
    all matches, drop any that look like CTA / nav chrome, and keep the
    first surviving one.
    """
    label_pattern = "|".join(re.escape(lbl) for lbl in FIELD_LABELS)
    # accept both curly and straight apostrophe in the boundary
    boundary = (
        rf"(?:(?={label_pattern}\s*:)|"
        r"I[’']m looking for funding|"
        r"Target group currently funded|"
        r"Share page|$)"
    )
    pattern = re.compile(
        rf"(?P<lbl>{label_pattern})\s*:\s*(?P<val>.+?)\s*{boundary}",
        re.S,
    )

    chrome_phrases = (
        "i’m looking for funding", "i'm looking for funding",
        "target group currently funded",
        "share page",
    )

    candidates: dict[str, list[str]] = {}
    for m in pattern.finditer(page_text):
        v = re.sub(r"\s+", " ", m.group("val")).strip()
        candidates.setdefault(m.group("lbl"), []).append(v)

    out: dict[str, str] = {}
    for lbl, vals in candidates.items():
        for v in vals:
            if not v:
                continue
            vl = v.lower()
            if any(vl.startswith(p) for p in chrome_phrases):
                continue
            out[lbl] = v
            break
    return out


def parse_detail_page(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # Title
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else None

    main = soup.find("main") or soup

    # Detail pages render labels as <th>Label:</th><td>value</td> rows.
    # That's far more reliable than regexing rendered text. Fall back to
    # text scraping only if the table is absent.
    fields: dict[str, str] = {}
    for row in main.find_all("tr"):
        th = row.find("th")
        td = row.find("td")
        if not th or not td:
            continue
        label = th.get_text(" ", strip=True).rstrip(":").strip()
        value = td.get_text(" ", strip=True)
        if label and value and label not in fields:
            fields[label] = re.sub(r"\s+", " ", value).strip()

    if not fields:
        # Older/variant pages without table markup — fall back to text regex
        text = re.sub(r"\s+", " ", main.get_text(" ", strip=True))
        fields = extract_labelled_fields(text)

    # Description = the substantial paragraph(s) between the H1 and the next
    # labelled section. We grab the first <p> blocks under main.
    description = None
    paragraphs = [p.get_text(" ", strip=True) for p in main.find_all("p")]
    candidate_paras = [
        p for p in paragraphs
        if p and len(p) > 80 and "@" not in p and "+49" not in p
    ]
    if candidate_paras:
        description = " ".join(candidate_paras[:3])
        description = re.sub(r"\s+", " ", description)[:5000]

    funding_budget_raw = fields.get("Funding budget")
    period_raw = fields.get("Period of time")
    start_iso, end_iso = parse_period(period_raw or "")
    amount_eur = parse_eur_amount(funding_budget_raw or "")

    slug = url.rstrip("/").rsplit("/", 1)[-1]

    return {
        "slug": slug,
        "url": url,
        "title": title,
        "description": description,
        "programme": fields.get("Programme"),
        "type_of_funding": fields.get("Type of funding"),
        "funded_institution": fields.get("Funded institution"),
        "target_group": fields.get("Target group"),
        "funding_line": fields.get("Funding line"),
        "funding_region": fields.get("Funding region"),
        "funding_topic": fields.get("Funding topic"),
        "field_of_action": fields.get("Field of action"),
        "funding_budget_raw": funding_budget_raw,
        "period_raw": period_raw,
        "amount_eur": amount_eur,
        "start_date": start_iso,
        "end_date": end_iso,
        "currency": "EUR",  # hardcoded — German foundation, single currency
        "downloaded_at": datetime.utcnow().isoformat(),
    }


def fetch_with_retries(url: str, retries: int = 3) -> str:
    last_err = None
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed: {url}: {last_err}")


def main() -> None:
    p = argparse.ArgumentParser(description="Carl-Zeiss-Stiftung -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None,
                   help="For smoke-test: only fetch first N detail pages")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    args = p.parse_args()

    log("=" * 60)
    log("Carl-Zeiss-Stiftung -> S3 pipeline starting")

    urls = discover_detail_urls()
    if args.limit:
        urls = urls[: args.limit]
        log(f"Smoke-test mode: limited to first {len(urls)} URLs")

    rows: list[dict] = []
    for i, url in enumerate(urls, 1):
        try:
            html = fetch_with_retries(url)
            row = parse_detail_page(html, url)
            rows.append(row)
            if i == 1 or i % 25 == 0 or i == len(urls):
                log(f"[{i}/{len(urls)}] parsed: {row['title']!r:.80}")
        except Exception as e:
            log(f"[{i}/{len(urls)}] FAILED {url}: {e}")
        time.sleep(REQUEST_DELAY)

    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")
    log(f"Coverage — title: {df.title.notna().sum()}, amount_eur: {df.amount_eur.notna().sum()}, "
        f"start_date: {df.start_date.notna().sum()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "carl_zeiss_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {parquet_path}")

    if args.skip_upload:
        log("--skip-upload set; done.")
        return

    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3
    s3 = boto3.client("s3")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    main()
