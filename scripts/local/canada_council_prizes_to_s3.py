#!/usr/bin/env python3
"""
Canada Council Prizes to S3 (PRIZE PATTERN)
===========================================

Builds a Canada Council for the Arts prize-laureate parquet from official
Canada Council sources only:

* https://canadacouncil.ca/funding/prizes
* official Canada Council cumulative-winner PDFs linked from those prize pages
* official Governor General's Literary Awards data linked from the Canada
  Council page at https://ggbooks.ca/past-winners-and-finalists

The Canada Council prize pages are authoritative, but their cumulative PDFs are
heterogeneous. Some are simple "year / laureates" lists and can be parsed
reliably. Others are two-column architecture project tables, instrument-loan
tables, acquisitions, or scanned PDFs with poor text extraction. This script
only emits high-confidence rows and writes a skipped-source manifest explaining
which official sources need manual curation or a dedicated parser.

Output:
    s3://openalex-ingest/awards/canada_council_prizes/canada_council_prizes.parquet

Awarding body in OpenAlex:
    Canada Council for the Arts (F4320319951)

Amount note:
    Prize pages expose current public dollar amounts, but many cumulative lists
    span decades and do not publish historical per-laureate amounts or shared
    apportionment. The script preserves page-level source_amount_text for audit.
    The Databricks notebook maps amount to NULL and currency to CAD, with the
    prize-pattern amount waiver documented there.

Runtime requirements:
    python packages: pandas, pyarrow, requests, beautifulsoup4
    system command: pdftotext (Poppler)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import time
import unicodedata
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

INDEX_URL = "https://canadacouncil.ca/funding/prizes"
GGBOOKS_ARCHIVE_URL = "https://ggbooks.ca/past-winners-and-finalists"
GGBOOKS_FALLBACK_JSON_URL = (
    "https://ggbooks.ca/Areas/GGBooks/json/"
    "ggbooks-data-compressed-2025-09-18.json"
)

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/canada_council_prizes/canada_council_prizes.parquet"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 "
        "openalex-walden/1.0"
    )
}
REQUEST_DELAY = 0.2
RETRIES = 3

FUNDING_TYPE = "prize"
PROVENANCE = "canada_council_prizes"
DEFAULT_CURRENCY = "CAD"

# These slugs are official Canada Council prize pages, but the linked source is
# not safe for the generic year/laureate parser. They are skipped explicitly and
# written to skipped_sources.json for admin/manual follow-up.
SKIPPED_PRIZE_SOURCES: dict[str, str] = {
    "musical-instrument-bank": (
        "Instrument-loan ranges rather than ordinary annual prize laureates; "
        "needs a dedicated loan-period parser."
    ),
    "cbc-literary-prizes": "No official cumulative winner file linked from the Canada Council page.",
    "coburn-fellowships": (
        "Mixed fellowships, Hebrew courses, and faculty exchanges; row semantics "
        "are not consistently one prize x laureate."
    ),
    "governor-generals-medals-in-architecture": (
        "Two-column project/architecture-firm table; generic laureate parser "
        "would confuse projects and recipients."
    ),
    "healey-willan-prize": "Linked PDF is not text-extractable with pdftotext.",
    "jbc-watkins-award": "No official cumulative winner file linked from the Canada Council page.",
    "jean-a-chalmers-fund-for-the-crafts": (
        "Fund/project grants to organizations and individuals; not a clean "
        "prize-laureate list."
    ),
    "john-g-diefenbaker-award": (
        "Three-column university/laureate table; needs a dedicated parser to "
        "avoid mixing host institutions into names."
    ),
    "peter-dwyer-scholarships": (
        "Two-column school scholarship table with shared awards; needs a "
        "dedicated parser."
    ),
    "victor-martyn-lynch-staunton-awards": (
        "Linked PDF text extraction yields only a format marker, not the "
        "laureate rows."
    ),
    "vida-peene-awards": "No official cumulative winner file linked from the Canada Council page.",
    "york-wilson-endowment-award": (
        "Acquisition grants to galleries/museums, not ordinary laureate awards; "
        "needs a dedicated acquisition parser."
    ),
}

HEADER_RE = re.compile(
    r"(Year|Annee|Ann.e|Laureates|Laureats|Winners|Gagnant|Project/Projet|Instrument)",
    re.IGNORECASE,
)
BAD_ENTRY_RE = re.compile(
    r"("
    r"no award|aucun prix|bringing the arts|cumulative|new format|"
    r"shared/bourse|hebrew course|faculty exchange|program:|programme|offered|"
    r"Canada Council for the Arts|canadacouncil\.ca|1-800-263-5588|"
    r"N\.B\. Before|Before 2002|OUTSTANDING CONTRIBUTION|"
    r"CONTRIBUTION EXCEPTIONNELLE|SAIDYE BRONFMAN AWARD|PRIX SAIDYE"
    r")",
    re.IGNORECASE,
)
LOCATION_ONLY_NAMES = {
    "AB",
    "Alberta",
    "B.C.",
    "BC",
    "Britannique",
    "British Columbia",
    "Colombie-Britannique",
    "Columbia",
    "Manitoba",
    "Montreal",
    "Montréal",
    "NB",
    "N.B.",
    "New Brunswick",
    "NL",
    "Nova Scotia",
    "NS",
    "N.S.",
    "ON",
    "Ontario",
    "Ottawa",
    "PE",
    "QC",
    "Quebec",
    "Québec",
    "Saskatchewan",
    "Toronto",
    "Vancouver",
    "Musée des beaux-arts de l’Ontario",
}
ORG_WORD_RE = re.compile(
    r"\b("
    r"Gallery|Museum|Architects?|Architecture|Studio|Theatre|Orchestra|Centre|"
    r"Council|Foundation|University|Inc\.?|Association|Festival|Productions|"
    r"Company|Library|School|Institute|Corporation|Office|Collective|Society|"
    r"Network|Alliance|Firm|Biennale|Design|MODA|KPMB|MJMA"
    r")\b",
    re.IGNORECASE,
)

GG_CATEGORY_LABELS = {
    "fiction": "Fiction",
    "nonFiction": "Non-fiction",
    "proseAndPoetry": "Prose and Poetry",
    "juvenile": "Juvenile",
    "poetry": "Poetry",
    "drama": "Drama",
    "childrensLiteratureIllustration": "Children's Literature - Illustration",
    "youngPeoplesLiteratureText": "Young People's Literature - Text",
    "youngPeoplesLiteratureIllustratedBooks": (
        "Young People's Literature - Illustrated Books"
    ),
    "translationFrenchToEnglish": "Translation: French to English",
    "translationEnglishToFrench": "Translation: English to French",
}


@dataclass
class PrizePage:
    slug: str
    prize_name: str
    prize_url: str
    description: str | None
    source_amount_text: str | None
    pdf_urls: list[str]


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def slugify(value: Any) -> str:
    text = clean_text(value) or "unknown"
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text.lower()).strip("-")
    return text or "unknown"


def request_bytes(url: str) -> bytes:
    last_err: Exception | None = None
    for attempt in range(RETRIES):
        try:
            log(f"GET {url}")
            response = requests.get(url, headers=HEADERS, timeout=60)
            log(f"  status={response.status_code} bytes={len(response.content)}")
            response.raise_for_status()
            return response.content
        except Exception as exc:
            last_err = exc
            wait = 2**attempt
            log(f"  attempt {attempt + 1}/{RETRIES} failed: {exc}; retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


def request_text(url: str) -> str:
    return request_bytes(url).decode("utf-8", errors="replace")


def split_name(full_name: str | None) -> tuple[str | None, str | None]:
    """Canonical repo name split: suffix-strip, then last token = family."""
    if not full_name:
        return None, None
    if ORG_WORD_RE.search(full_name) or " and " in full_name.lower() or "&" in full_name:
        return None, None
    tokens = full_name.strip().split()
    suffixes = {
        "phd",
        "ph.d.",
        "md",
        "m.d.",
        "dphil",
        "frs",
        "jr",
        "jr.",
        "sr",
        "sr.",
        "ii",
        "iii",
        "iv",
    }
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if len(tokens) <= 1:
        return None, tokens[0] if tokens else None
    return " ".join(tokens[:-1]), tokens[-1]


def discover_prize_pages() -> list[PrizePage]:
    html = request_text(INDEX_URL)
    soup = BeautifulSoup(html, "html.parser")

    urls: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = urljoin(INDEX_URL, anchor["href"]).split("#")[0]
        if "/funding/prizes/" not in href:
            continue
        if href.rstrip("/") == INDEX_URL.rstrip("/") or href.endswith("/donations"):
            continue
        if href not in urls:
            urls.append(href)

    pages: list[PrizePage] = []
    for i, url in enumerate(urls, 1):
        log(f"Prize page [{i}/{len(urls)}]")
        page_html = request_text(url)
        page_soup = BeautifulSoup(page_html, "html.parser")
        h1 = page_soup.find("h1")
        title = clean_text(h1.get_text(" ", strip=True)) if h1 else None
        meta = page_soup.find("meta", attrs={"name": "description"})
        description = clean_text(meta.get("content")) if meta and meta.get("content") else None
        page_text = page_soup.get_text(" ", strip=True)
        amounts = sorted(set(re.findall(r"\$\s?\d[\d,]*(?:\.\d+)?", page_text)))
        pdf_urls = [
            urljoin(url, anchor["href"])
            for anchor in page_soup.find_all("a", href=True)
            if ".pdf" in anchor["href"].lower()
        ]
        pages.append(
            PrizePage(
                slug=url.rstrip("/").split("/")[-1],
                prize_name=title or url.rstrip("/").split("/")[-1].replace("-", " ").title(),
                prize_url=url,
                description=description,
                source_amount_text="; ".join(amounts) if amounts else None,
                pdf_urls=pdf_urls,
            )
        )
        time.sleep(REQUEST_DELAY)

    if not pages:
        raise RuntimeError("Discovered 0 prize pages from the Canada Council index.")
    return pages


def cache_path_for_url(cache_dir: Path, url: str, suffix: str) -> Path:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    return cache_dir / f"{digest}{suffix}"


def download_pdf_text(pdf_url: str, cache_dir: Path, *, no_cache: bool = False) -> str:
    pdf_path = cache_path_for_url(cache_dir, pdf_url, ".pdf")
    txt_path = cache_path_for_url(cache_dir, pdf_url, ".txt")

    if no_cache or not pdf_path.exists():
        pdf_path.write_bytes(request_bytes(pdf_url))

    if no_cache or not txt_path.exists():
        pdftotext = shutil.which("pdftotext")
        if not pdftotext:
            raise RuntimeError("pdftotext is required but was not found on PATH.")
        result = subprocess.run(
            [pdftotext, "-layout", str(pdf_path), str(txt_path)],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"pdftotext failed for {pdf_url}: {result.stderr.strip()}"
            )

    return txt_path.read_text(encoding="utf-8", errors="replace")


def looks_like_entry_start(line: str, *, allow_single_token: bool) -> bool:
    text = clean_text(line) or ""
    if not text or BAD_ENTRY_RE.search(text):
        return False
    if re.match(r"^(First|Second|Third|Special|Honou?rable)\s+Prize\s*:", text, re.I):
        return True

    first_chunk = re.split(r"[,|()]", text, 1)[0].strip()
    tokens = first_chunk.split()
    if not tokens:
        return False
    if re.match(r"^[a-z]\.[a-z]\.", tokens[0]):
        return True
    first_char = tokens[0][0]
    if not (first_char.isupper() or tokens[0].isupper()):
        return False
    if not allow_single_token:
        # A continuation line can start with an uppercase organization/title
        # fragment ("Science Council | ..."). When we are already inside an
        # entry, only split early on the strong signal used by these PDFs for a
        # new person row: "Name, role ...".
        return "," in text.split("|", 1)[0] and len(tokens) >= 2
    if len(tokens) >= 2:
        return True
    return allow_single_token and bool(re.search(r"[,|()]", text))


def extract_name_category_work(entry_text: str) -> tuple[str | None, str | None, str | None]:
    raw = clean_text(entry_text) or ""
    english = clean_text(raw.split("|", 1)[0]) or ""
    category = None
    work_title = None

    prefix = re.match(
        r"^(First Prize|Second Prize|Third Prize|Honou?rable Mention|Special Prize)"
        r"\s*[:-]\s*(.+)$",
        english,
        re.IGNORECASE,
    )
    if prefix:
        category = clean_text(prefix.group(1))
        english = clean_text(prefix.group(2)) or ""

    no_parens = clean_text(re.sub(r"\([^)]*\)", " ", english)) or ""
    work_match = re.search(r"\bfor(?:/pour)?\b\s+(.+)$", no_parens, re.IGNORECASE)
    if work_match:
        work_title = clean_text(work_match.group(1))
        no_parens = clean_text(no_parens[: work_match.start()]) or ""

    acquisition_match = re.search(
        r"\bfor the (?:acquisition|purchase)\b", no_parens, re.IGNORECASE
    )
    if acquisition_match:
        no_parens = clean_text(no_parens[: acquisition_match.start()]) or ""

    dash_match = re.match(r"^(.+?)\s+[\-–—]\s+([A-Za-z][A-Za-z /&-]{2,50})(?:\s|$)", no_parens)
    if dash_match:
        no_parens = clean_text(dash_match.group(1)) or ""
        category = category or clean_text(dash_match.group(2))

    if "," in no_parens:
        before_comma, after_comma = no_parens.split(",", 1)
        no_parens = clean_text(before_comma) or ""
        category = category or (clean_text(after_comma) or "")[:120]

    if ":" in no_parens:
        no_parens = clean_text(no_parens.split(":")[-1]) or ""

    no_parens = re.sub(r"^(Dr\.|Prof\.|Professor)\s+", "", no_parens).strip()
    if no_parens in LOCATION_ONLY_NAMES or no_parens.rstrip(")") in LOCATION_ONLY_NAMES:
        return None, category, work_title
    return no_parens or None, category, work_title


def parse_pdf_page(page: PrizePage, cache_dir: Path, *, no_cache: bool = False) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not page.pdf_urls:
        return [], {"skip_reason": "No PDF URLs linked from official prize page."}

    pdf_url = page.pdf_urls[0]
    text = download_pdf_text(pdf_url, cache_dir, no_cache=no_cache)
    if len(text.strip()) < 200:
        return [], {"skip_reason": f"pdftotext extracted only {len(text.strip())} chars."}

    rows: list[dict[str, Any]] = []
    current_year: int | None = None
    current_lines: list[str] = []

    def flush() -> None:
        nonlocal current_lines
        if current_year is None or not current_lines:
            current_lines = []
            return

        entry = clean_text(" ".join(current_lines)) or ""
        current_lines = []
        if not entry or BAD_ENTRY_RE.search(entry):
            return

        laureate_name, category, work_title = extract_name_category_work(entry)
        if not laureate_name:
            return
        if len(laureate_name) > 120 or len(laureate_name) < 2:
            return
        if BAD_ENTRY_RE.search(laureate_name):
            return
        if laureate_name in LOCATION_ONLY_NAMES or laureate_name.rstrip(")") in LOCATION_ONLY_NAMES:
            return

        given_name, family_name = split_name(laureate_name)
        is_org_like = bool(ORG_WORD_RE.search(laureate_name)) or given_name is None and family_name is None

        rows.append(
            {
                "source_type": "canada_council_pdf",
                "prize_slug": page.slug,
                "prize_name": page.prize_name,
                "prize_url": page.prize_url,
                "source_url": pdf_url,
                "source_description": page.description,
                "source_amount_text": page.source_amount_text,
                "year": current_year,
                "category": category,
                "language": None,
                "laureate_name": laureate_name,
                "laureate_given_name": given_name,
                "laureate_family_name": family_name,
                "is_organization_like": is_org_like,
                "work_title": work_title,
                "publisher": None,
                "institution": None,
                "raw_entry_text": entry,
                "parser_name": "pdf_year_laureate_v1",
                "parse_confidence": "high",
                "currency": DEFAULT_CURRENCY,
                "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        )

    for raw_line in text.splitlines():
        if not (clean_text(raw_line) or ""):
            flush()
            continue
        if HEADER_RE.search(raw_line):
            continue
        if BAD_ENTRY_RE.search(raw_line):
            flush()
            continue

        year_match = re.match(
            r"^\s*((?:19|20)\d{2})(?:\s*[-–]\s*(?:\d{2,4}))?\s+(.*)$",
            raw_line,
        )
        if year_match:
            flush()
            current_year = int(year_match.group(1))
            rest = clean_text(year_match.group(2)) or ""
            current_lines = [rest] if rest and looks_like_entry_start(rest, allow_single_token=True) else []
            continue

        if current_year is None:
            continue

        line = clean_text(raw_line) or ""
        if not current_lines:
            if looks_like_entry_start(line, allow_single_token=True):
                current_lines = [line]
        else:
            if looks_like_entry_start(line, allow_single_token=False):
                flush()
                current_lines = [line]
            else:
                current_lines.append(line)

    flush()
    return rows, {"pdf_url": pdf_url, "pdf_text_chars": len(text), "rows": len(rows)}


def discover_ggbooks_json_url() -> str:
    html = request_text(GGBOOKS_ARCHIVE_URL)
    soup = BeautifulSoup(html, "html.parser")
    script_urls = [
        urljoin(GGBOOKS_ARCHIVE_URL, tag["src"])
        for tag in soup.find_all("script", src=True)
        if "Archives" in tag["src"] or "GGBooks" in tag["src"]
    ]
    for tag in soup.find_all(attrs={"data-ux-module": True}):
        module = tag.get("data-ux-module") or ""
        if module.startswith("Components/Archives"):
            script_urls.append(urljoin(GGBOOKS_ARCHIVE_URL, f"/Areas/GGBooks/js/{module}.js"))

    for script_url in script_urls:
        try:
            js = request_text(script_url)
        except Exception as exc:
            log(f"  warning: failed to inspect GGBooks script {script_url}: {exc}")
            continue
        match = re.search(r"(/Areas/GGBooks/json/ggbooks-data-compressed-[^'\"\s]+\.json)", js)
        if match:
            return urljoin(GGBOOKS_ARCHIVE_URL, match.group(1))

    log("  warning: could not discover GGBooks JSON URL from scripts; using known current URL")
    return GGBOOKS_FALLBACK_JSON_URL


def parse_ggbooks_rows(page: PrizePage | None) -> list[dict[str, Any]]:
    json_url = discover_ggbooks_json_url()
    payload = json.loads(request_text(json_url))
    rows: list[dict[str, Any]] = []

    for year_text, categories in sorted(payload.items()):
        year = int(year_text)
        for category_key, by_language in categories.items():
            for language, category_payload in by_language.items():
                finalists = category_payload.get("finalists") or []
                for finalist in finalists:
                    if not finalist.get("winner"):
                        continue
                    author_raw = clean_text(finalist.get("author"))
                    if not author_raw:
                        continue
                    # The current JSON sometimes appends locations to recent
                    # author strings. Strip a trailing parenthetical location
                    # from the person/org name but preserve the raw entry text.
                    normalized_author = clean_text(re.sub(r"\s*\([^)]*\)\s*$", "", author_raw))
                    # The prize-pattern convention is one row per
                    # (prize x laureate). GGBooks uses comma-separated author
                    # strings for co-authored/co-illustrated winning books, so
                    # split those into one row per named laureate.
                    laureate_names = [
                        clean_text(part)
                        for part in re.split(r"\s*,\s+", normalized_author or "")
                        if clean_text(part)
                    ] or [normalized_author]
                    title = clean_text(finalist.get("title"))
                    publisher = clean_text(finalist.get("publisher"))
                    category_label = GG_CATEGORY_LABELS.get(category_key, category_key)
                    raw_entry = json.dumps(finalist, ensure_ascii=False, sort_keys=True)

                    for normalized_name in laureate_names:
                        given_name, family_name = split_name(normalized_name)
                        rows.append(
                            {
                                "source_type": "ggbooks_json",
                                "prize_slug": "governor-generals-literary-awards",
                                "prize_name": "Governor General's Literary Awards",
                                "prize_url": (
                                    page.prize_url
                                    if page
                                    else "https://canadacouncil.ca/funding/prizes/governor-generals-literary-awards"
                                ),
                                "source_url": json_url,
                                "source_description": page.description if page else None,
                                "source_amount_text": page.source_amount_text if page else None,
                                "year": year,
                                "category": category_label,
                                "language": language,
                                "laureate_name": normalized_name,
                                "laureate_given_name": given_name,
                                "laureate_family_name": family_name,
                                "is_organization_like": bool(ORG_WORD_RE.search(normalized_name or "")),
                                "work_title": title,
                                "publisher": publisher,
                                "institution": publisher,
                                "raw_entry_text": raw_entry,
                                "parser_name": "ggbooks_json_winners_v1",
                                "parse_confidence": "high",
                                "currency": DEFAULT_CURRENCY,
                                "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                            }
                        )

    if not rows:
        raise RuntimeError("Parsed 0 Governor General's Literary Award winners from GGBooks JSON.")
    return rows


def assign_funder_award_ids(rows: list[dict[str, Any]]) -> None:
    seen: set[str] = set()
    for row in rows:
        parts = [
            "canada-council",
            row.get("prize_slug") or "unknown-prize",
            str(row.get("year") or "unknown-year"),
            slugify(row.get("category") or row.get("language") or "general"),
            slugify(row.get("laureate_name")),
            slugify(row.get("work_title") or "no-work"),
        ]
        award_id = ":".join(parts)
        if award_id in seen:
            raise RuntimeError(f"Duplicate funder_award_id would be emitted: {award_id}")
        seen.add(award_id)
        row["funder_award_id"] = award_id


def write_outputs(
    rows: list[dict[str, Any]],
    skipped_sources: list[dict[str, Any]],
    output_dir: Path,
) -> Path:
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Parsed 0 Canada Council prize rows.")

    log(f"DataFrame: {df.shape[0]} rows x {df.shape[1]} cols")
    log(f"source_type counts: {dict(Counter(df['source_type']))}")
    log(f"year range: {df['year'].min()}-{df['year'].max()}")
    log(f"unique funder_award_id: {df['funder_award_id'].nunique()}")
    log(f"skipped official source pages: {len(skipped_sources)}")

    manifest_path = output_dir / "canada_council_prizes_skipped_sources.json"
    manifest_path.write_text(
        json.dumps(skipped_sources, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log(f"wrote skipped-source manifest {manifest_path}")

    # Runbook §1.2.5: force string dtype before to_parquet so pyarrow does not
    # infer null-heavy columns as numeric types.
    df = df.astype("string")
    parquet_path = output_dir / "canada_council_prizes.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"wrote {parquet_path} ({parquet_path.stat().st_size:,} bytes)")
    return parquet_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """
    Runbook §1.4 — never shrink the corpus on re-ingest. Read the existing
    S3 parquet's row count; if the new dataframe has fewer rows, abort.
    Returns True if it's safe to proceed; False if upload must be aborted.
    """
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for the §1.4 shrink-check; rerun with --skip-upload to bypass"
        ) from exc
    client = boto3.client("s3")
    log(f"§1.4 re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            log("  no existing parquet at S3 path — first ingest, no shrink check.")
            return True
        log(f"  [WARN] head_object failed ({code}); treating as first ingest")
        return True
    prev_path = output_dir / "_prev_canada_council_prizes.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        import pandas as pd
        prev_count = len(pd.read_parquet(prev_path))
    except Exception as e:
        log(f"  [ERROR] couldn't read existing parquet ({e}); aborting upload "
            f"to avoid clobbering unknown data. Re-run with --allow-shrink if "
            f"you've verified the previous file is corrupt or empty.")
        return False
    finally:
        prev_path.unlink(missing_ok=True)
    log(f"  previous count: {prev_count}   new count: {new_count}")
    if new_count < prev_count:
        if allow_shrink:
            log(f"  [OVERRIDE] new < previous but --allow-shrink set; proceeding.")
            return True
        log(
            f"  [ERROR] §1.4 violation: refusing to shrink corpus "
            f"({prev_count} -> {new_count}). Cause is almost always a "
            f"source-side partial outage, schema change, or pagination bug — "
            f"not a genuine retraction. Investigate first; re-run with "
            f"--allow-shrink if confirmed intentional."
        )
        return False
    log("  [OK] new corpus is not smaller; safe to overwrite.")
    return True


def upload_to_s3(parquet_path: Path) -> None:
    import boto3  # lazy import; only needed when uploading

    client = boto3.client("s3")
    log(f"uploading to s3://{S3_BUCKET}/{S3_KEY}")
    client.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("upload complete")


def collect_rows(output_dir: Path, *, no_cache: bool = False, limit_pages: int | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cache_dir = output_dir / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    log("Step 0/1: discovering official Canada Council prize pages")
    prize_pages = discover_prize_pages()
    by_slug = {page.slug: page for page in prize_pages}
    if limit_pages is not None:
        prize_pages = prize_pages[:limit_pages]
        log(f"--limit-pages active: processing first {len(prize_pages)} prize pages")

    rows: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    log("Step 1a: parsing official GGBooks JSON for Governor General's Literary Awards")
    gg_page = by_slug.get("governor-generals-literary-awards")
    rows.extend(parse_ggbooks_rows(gg_page))
    log(f"  parsed {sum(1 for row in rows if row['source_type'] == 'ggbooks_json')} GGBooks winners")

    log("Step 1b: parsing high-confidence official Canada Council cumulative PDFs")
    for i, page in enumerate(prize_pages, 1):
        if page.slug == "governor-generals-literary-awards":
            continue

        if page.slug in SKIPPED_PRIZE_SOURCES:
            reason = SKIPPED_PRIZE_SOURCES[page.slug]
            log(f"  [{i}/{len(prize_pages)}] SKIP {page.slug}: {reason}")
            skipped.append(
                {
                    "prize_slug": page.slug,
                    "prize_name": page.prize_name,
                    "prize_url": page.prize_url,
                    "pdf_urls": page.pdf_urls,
                    "reason": reason,
                }
            )
            continue

        if not page.pdf_urls:
            reason = "No official cumulative PDF linked from prize page."
            log(f"  [{i}/{len(prize_pages)}] SKIP {page.slug}: {reason}")
            skipped.append(
                {
                    "prize_slug": page.slug,
                    "prize_name": page.prize_name,
                    "prize_url": page.prize_url,
                    "pdf_urls": [],
                    "reason": reason,
                }
            )
            continue

        log(f"  [{i}/{len(prize_pages)}] PDF {page.slug}")
        try:
            pdf_rows, meta = parse_pdf_page(page, cache_dir, no_cache=no_cache)
        except Exception as exc:
            skipped.append(
                {
                    "prize_slug": page.slug,
                    "prize_name": page.prize_name,
                    "prize_url": page.prize_url,
                    "pdf_urls": page.pdf_urls,
                    "reason": f"Parser failed: {exc}",
                }
            )
            log(f"    ERROR: {exc}; source skipped")
            continue

        if not pdf_rows:
            reason = meta.get("skip_reason") or "Parser emitted 0 high-confidence rows."
            skipped.append(
                {
                    "prize_slug": page.slug,
                    "prize_name": page.prize_name,
                    "prize_url": page.prize_url,
                    "pdf_urls": page.pdf_urls,
                    "reason": reason,
                }
            )
            log(f"    SKIP: {reason}")
            continue

        log(f"    parsed {len(pdf_rows)} rows")
        rows.extend(pdf_rows)
        time.sleep(REQUEST_DELAY)

    log("Step 1c: assigning synthetic funder_award_id and checking collisions")
    assign_funder_award_ids(rows)
    return rows, skipped


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/canada_council_prizes"),
        help="Local working directory for cache, manifest, and parquet.",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Refetch official pages/PDFs and rerun pdftotext even if cached files exist.",
    )
    parser.add_argument(
        "--limit-pages",
        type=int,
        default=None,
        help="Smoke-test with the first N Canada Council prize pages.",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Write parquet locally but skip S3 upload.",
    )
    parser.add_argument(
        "--allow-shrink",
        action="store_true",
        help="Override the runbook §1.4 shrink-check. Only use after confirming a smaller corpus is intentional.",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    rows, skipped_sources = collect_rows(
        args.output_dir, no_cache=args.no_cache, limit_pages=args.limit_pages
    )
    parquet_path = write_outputs(rows, skipped_sources, args.output_dir)

    if args.skip_upload:
        log("--skip-upload: not uploading to S3.")
        return
    if not check_no_shrink(len(df), args.allow_shrink, Path(args.output_dir)):
        raise SystemExit("§1.4 shrink-check failed. See above; re-run with --allow-shrink if intentional.")
    upload_to_s3(parquet_path)


if __name__ == "__main__":
    main()
