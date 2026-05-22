#!/usr/bin/env python3
"""
Princess of Asturias Awards to S3 (PRIZE PATTERN)
=================================================

Fetches Princess/Prince of Asturias Award laureates from the awarding body's
official site. This follows the awards runbook's prize-pattern source-authority
rule: prize rows come from the funder/awarding body directly, not from
Wikipedia, Wikidata, or secondary biographies.

Official source pages:
  - https://www.fpa.es/en/cargarAplicacionPremiadoCompleto.do
    Full server-rendered laureate list.
  - https://www.fpa.es/en/area-of-communication-and-media/faqs/princess-of-asturias-awards/
    Current award-composition FAQ. Used only as raw provenance for the current
    EUR 50,000 rule; the notebook maps amount to NULL because historical
    per-year/per-laureate cash values are not exposed on the laureate pages.

Output:
  s3://openalex-ingest/awards/princess_asturias/princess_asturias_laureates.parquet

Awarding body in OpenAlex:
  Fundación Princesa de Asturias (F4320323780, DOI 10.13039/501100006336,
  ROR absent in the OpenAlex API result checked during Step 0)

Parsing notes:
  The FPA full list publishes one link per award/year/category. Several links
  name multiple co-laureates in the link text and detail-page heading. Per the
  prize pattern, this script emits one row per (prize x laureate), using
  documented split/no-split rules below. Institutions or groups are preserved
  as institutional laureates and are not expanded to their members; the FPA FAQ
  explicitly says members of a recognized group are not individually bestowed
  with the award.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). This
# crashes scrapers writing laureate names with non-ASCII chars (Polish ł,
# Turkish ğ, Greek μ, combining accents, zero-width spaces). Production
# runs on Linux/Databricks where UTF-8 is the default, but this fixes
# local validation on Windows without requiring contractors to set
# PYTHONUTF8=1 in their environment. See runbook §1.2.
import sys as _sys_utf8
try:
    _sys_utf8.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    _sys_utf8.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if _sys_utf8.platform == "win32":
    import builtins as _builtins_utf8
    import pathlib as _pathlib_utf8

    _orig_wt = _pathlib_utf8.Path.write_text
    def _wt(self, data, encoding=None, errors=None, newline=None):
        return _orig_wt(self, data, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.write_text = _wt

    _orig_rt = _pathlib_utf8.Path.read_text
    def _rt(self, encoding=None, errors=None, newline=None):
        return _orig_rt(self, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.read_text = _rt

    _orig_open = _builtins_utf8.open
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open_utf8
# --- end shim ---

BASE_URL = "https://www.fpa.es"
FULL_LIST_URL = "https://www.fpa.es/en/cargarAplicacionPremiadoCompleto.do"
FAQ_URL = "https://www.fpa.es/en/area-of-communication-and-media/faqs/princess-of-asturias-awards/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/princess_asturias/princess_asturias_laureates.parquet"
OUTPUT_FILE = "princess_asturias_laureates.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.15
RETRIES = 3

CURRENT_AWARD_AMOUNT_EUR = "50000"
CURRENCY = "EUR"
AMOUNT_NOTE = (
    "The current FPA FAQ says each Princess of Asturias Award includes EUR "
    "50,000 divided among laureates when shared, but historical per-year and "
    "per-laureate cash values are not exposed in the official laureate source; "
    "the Databricks notebook therefore maps amount to NULL."
)

# Exact institutional or title strings that contain commas/and but represent
# one official laureate. These are intentionally source-specific and logged via
# raw fields so reviewers can audit the split decision.
NO_SPLIT_EXACT = {
    '"Vuelta" the Review, headed by Octavio Paz',
    "Bill and Melinda Gates Foundation",
    "CAMFED, Campaign for Female Education",
    "Gavi, the Vaccine Alliance",
    "H. M. Husein I, King of Jordan",
    "International Red Cross and Red Crescent Movement",
    "Organization of Ibero-American States for Education, Science and Culture (official Spanish and Portuguese acronym, OEI)",
    "Spanish Missions in Rwanda and Burundi",
    "The City of Berlín, on the 20th Anniversary of the Fall of the Wall",
    "The National Network of Youth and Children Orchestras of Venezuela",
    "The archaeological Team of the Terra Cotta Warriors and Horses of Xi'an",
    "United Nations Framework Convention on Climate Change and the Paris Agreement",
    "Yad Vashem, the Holocaust Museum in Jerusalem",
}

# Exact strings where the site names multiple official laureates but naive comma
# or "and" splitting would either lose context or create invalid fragments.
CUSTOM_SPLITS = {
    "Alliance Française, Società Dante Alighieri, British Council, Goethe Institut, Instituto Cervantes and Instituto Camões": [
        "Alliance Française",
        "Società Dante Alighieri",
        "British Council",
        "Goethe Institut",
        "Instituto Cervantes",
        "Instituto Camões",
    ],
    "Centro de Investigação em Saúde in Manhiça, Ifakara Health Institute, in Tanzania, Malaria Research and Training Center and Kintampo Health Research Centre": [
        "Centro de Investigação em Saúde in Manhiça",
        "Ifakara Health Institute in Tanzania",
        "Malaria Research and Training Center",
        "Kintampo Health Research Centre",
    ],
    'Diario "El Espectador" and Diario "El Tiempo" from Colombia': [
        'Diario "El Espectador"',
        'Diario "El Tiempo" from Colombia',
    ],
    "EFE Agency and José Luis López Aranguren": [
        "EFE Agency",
        "José Luis López Aranguren",
    ],
    "El Colegio de México & Juan Iglesias Santos": [
        "El Colegio de México",
        "Juan Iglesias Santos",
    ],
    "Government of Guatemala and Guatemalan National Revolutionary Unity": [
        "Government of Guatemala",
        "Guatemalan National Revolutionary Unity",
    ],
    "International Union for the Conservation of Nature and Natural Resources and the World Wide Fund for Nature": [
        "International Union for the Conservation of Nature and Natural Resources",
        "World Wide Fund for Nature",
    ],
    "José Andrés and the NGO World Central Kitchen": [
        "José Andrés",
        "World Central Kitchen",
    ],
    "Journals Nature and Science": [
        "Nature",
        "Science",
    ],
    "Lawrence Roberts, Robert Kahn, Vinton Cerf & Tim Berners-Lee": [
        "Lawrence Roberts",
        "Robert Kahn",
        "Vinton Cerf",
        "Tim Berners-Lee",
    ],
    "Manuel Losada Villasante and National Biodiversity Institute of Costa Rica": [
        "Manuel Losada Villasante",
        "National Biodiversity Institute of Costa Rica",
    ],
    "Médicins sans Fronterières and Medicus Mundi": [
        "Médicins sans Fronterières",
        "Medicus Mundi",
    ],
    "National Movement of Street Children, Messengers of Peace and Save the Children": [
        "National Movement of Street Children",
        "Messengers of Peace",
        "Save the Children",
    ],
    "Olympic Refuge Foundation and IOC Refugee Olympic Team": [
        "Olympic Refuge Foundation",
        "IOC Refugee Olympic Team",
    ],
    "Pau and Marc Gasol": [
        "Pau Gasol",
        "Marc Gasol",
    ],
    "Peter Higgs, François Englert and European Organization for Nuclear Research CERN": [
        "Peter Higgs",
        "François Englert",
        "European Organization for Nuclear Research CERN",
    ],
    "Rainer Weiss, Kip S. Thorne, Barry C. Barish and and LIGO Scientific Collaboration": [
        "Rainer Weiss",
        "Kip S. Thorne",
        "Barry C. Barish",
        "LIGO Scientific Collaboration",
    ],
    "Royal Spanish Academy and Association of Academies of the Spanish Language": [
        "Royal Spanish Academy",
        "Association of Academies of the Spanish Language",
    ],
    "Salamanca and Coimbra Universities": [
        "Salamanca University",
        "Coimbra University",
    ],
    "Salman Khan and the Khan Academy": [
        "Salman Khan",
        "Khan Academy",
    ],
    "The Guadalajara International Book Fair and The Hay Festival of Literature & Arts": [
        "The Guadalajara International Book Fair",
        "The Hay Festival of Literature & Arts",
    ],
    "The Transplantation Society and the Spanish National Transplant Organization": [
        "The Transplantation Society",
        "Spanish National Transplant Organization",
    ],
    "Václav Havel and Cable News Network (CNN)": [
        "Václav Havel",
        "Cable News Network (CNN)",
    ],
}

ORG_TERMS = {
    "academy",
    "agency",
    "alliance",
    "association",
    "campaign",
    "camfed",
    "center",
    "centre",
    "cern",
    "college",
    "committee",
    "council",
    "daughters",
    "diario",
    "fair",
    "festival",
    "fondo",
    "foundation",
    "fund",
    "gavi",
    "goethe institut",
    "government",
    "grupo",
    "health africa",
    "institute",
    "institution",
    "instituto",
    "initiative",
    "journal",
    "magnum photos",
    "marathon",
    "medicus mundi",
    "meals",
    "messengers of peace",
    "museum",
    "nature",
    "network",
    "organization",
    "organisation",
    "orchestra",
    "orfeón",
    "people",
    "program",
    "programme",
    "red cross",
    "review",
    "save the children",
    "science",
    "society",
    "società",
    "studio",
    "team",
    "union",
    "university",
    "wikipedia",
    "world central kitchen",
}


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def collapse_text(value: str | None) -> str | None:
    if not value:
        return None
    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(r"\s+([,;.:])", r"\1", value)
    return value or None


def normalized_exact(value: str) -> str:
    return collapse_text(value) or ""


def slugify(value: str | None) -> str:
    value = (value or "").lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "unknown"


def split_person_name(name: str | None, is_org: bool) -> tuple[str | None, str | None]:
    if not name:
        return None, None
    if is_org:
        return None, name

    split_source = re.sub(r",\s*(King|Queen|Prince|Princess|headed by).*$", "", name)
    tokens = split_source.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def is_organization_name(name: str) -> bool:
    lowered = name.lower()
    if any(term in lowered for term in ORG_TERMS):
        return True
    if re.fullmatch(r"[A-Z0-9&.\- ]{3,}", name) and " " not in name.strip("."):
        return True
    return False


def split_laureate_text(laureate_text: str) -> list[str]:
    text = normalized_exact(laureate_text)
    text = text.replace(" and and ", " and ")
    if text in CUSTOM_SPLITS:
        return CUSTOM_SPLITS[text]
    if text in NO_SPLIT_EXACT:
        return [text]

    parts = [
        normalized_exact(part)
        for part in re.split(r",\s+|\s+(?:and|&)\s+", text)
        if normalized_exact(part)
    ]
    return parts or [text]


def request_html(session: requests.Session, url: str) -> str:
    last_err: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        started = time.time()
        try:
            response = session.get(url, headers=HEADERS, timeout=30)
            elapsed = time.time() - started
            log(f"GET {url} -> {response.status_code} {len(response.content)} bytes in {elapsed:.1f}s")
            response.raise_for_status()
            if not response.text:
                raise RuntimeError(f"Empty response from {url}")
            return response.text
        except Exception as exc:  # noqa: BLE001 - retry any transport/status failure.
            last_err = exc
            if attempt < RETRIES:
                sleep_s = 2 ** (attempt - 1)
                log(f"  retrying after {sleep_s}s: {exc}")
                time.sleep(sleep_s)
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


def load_checkpoint(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    pages = payload.get("pages") if isinstance(payload, dict) else None
    if not isinstance(pages, dict):
        return {}
    return {str(k): str(v) for k, v in pages.items()}


def save_checkpoint(path: Path, pages: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump({"pages": pages}, handle, indent=2, sort_keys=True)


def get_page(session: requests.Session, url: str, checkpoint_file: Path, use_cache: bool) -> str:
    cache = load_checkpoint(checkpoint_file)
    if use_cache and url in cache:
        log(f"cached {url} ({len(cache[url].encode('utf-8'))} bytes)")
        return cache[url]
    html = request_html(session, url)
    cache[url] = html
    save_checkpoint(checkpoint_file, cache)
    time.sleep(REQUEST_DELAY)
    return html


def parse_full_list(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, str]] = []
    title_re = re.compile(r"^(.+),\s*((?:Princess|Prince) of Asturias Award for .+)$")

    for heading in soup.select(".laureateslist_title"):
        year_match = re.search(r"\b((?:19|20)\d{2})\b", heading.get_text(" ", strip=True))
        if not year_match:
            continue
        award_year = year_match.group(1)
        list_node = heading.find_next_sibling(class_="laureateslist_list")
        if not list_node:
            continue
        for anchor in list_node.select("a[href]"):
            link_text = collapse_text(anchor.get_text(" ", strip=True))
            if not link_text:
                continue
            match = title_re.match(link_text)
            if not match:
                raise RuntimeError(f"Could not parse FPA award list item: {link_text}")
            laureate_text = collapse_text(match.group(1)) or ""
            prize_title = collapse_text(match.group(2)) or ""
            category = re.sub(r"^(?:Princess|Prince) of Asturias Award for ", "", prize_title)
            detail_url = urljoin(BASE_URL, anchor["href"])
            items.append({
                "award_year": award_year,
                "award_prefix": "Princess" if prize_title.startswith("Princess") else "Prince",
                "award_category": category,
                "prize_title": prize_title,
                "official_laureate_text": laureate_text,
                "detail_url": detail_url,
                "list_url": FULL_LIST_URL,
            })

    if not items:
        raise RuntimeError("No Princess of Asturias award items parsed from the official full list.")
    return items


def parse_detail_page(html: str, item: dict[str, str]) -> dict[str, str | None]:
    soup = BeautifulSoup(html, "html.parser")
    h1 = collapse_text(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else None)
    h2 = collapse_text(soup.find("h2").get_text(" ", strip=True) if soup.find("h2") else None)
    category_text = collapse_text(
        soup.select_one(".laureates_category").get_text(" ", strip=True)
        if soup.select_one(".laureates_category")
        else None
    )
    description = collapse_text(
        soup.select_one(".laureates_description").get_text(" ", strip=True)
        if soup.select_one(".laureates_description")
        else None
    )
    meta_description = None
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        meta_description = collapse_text(meta["content"])

    year_match = re.search(r"\b((?:19|20)\d{2})\b", h1 or category_text or "")
    detail_year = year_match.group(1) if year_match else None
    if detail_year and detail_year != item["award_year"]:
        raise RuntimeError(
            f"Year mismatch for {item['detail_url']}: list={item['award_year']} detail={detail_year}"
        )

    return {
        "detail_title": h2,
        "detail_prize_title": category_text or h1,
        "citation": description,
        "meta_description": meta_description,
    }


def fetch_rows(
    session: requests.Session,
    checkpoint_file: Path,
    use_cache: bool,
    limit_detail_pages: int | None,
) -> list[dict[str, str | None]]:
    full_list_html = get_page(session, FULL_LIST_URL, checkpoint_file, use_cache=use_cache)
    faq_html = get_page(session, FAQ_URL, checkpoint_file, use_cache=use_cache)
    faq_text = collapse_text(BeautifulSoup(faq_html, "html.parser").get_text(" ", strip=True)) or ""
    if "cash prize" not in faq_text or "€50 000" not in faq_text:
        raise RuntimeError("Could not verify the official FPA current cash-prize FAQ text.")

    items = parse_full_list(full_list_html)
    log(f"Discovered {len(items)} official award list items")
    if limit_detail_pages is not None:
        items = items[:limit_detail_pages]
        log(f"--limit-detail-pages set; detail fetch limited to {len(items)} official items")

    rows: list[dict[str, str | None]] = []
    split_audit: dict[int, int] = {}
    for item_index, item in enumerate(items, start=1):
        detail_html = get_page(session, item["detail_url"], checkpoint_file, use_cache=use_cache)
        detail = parse_detail_page(detail_html, item)
        laureates = split_laureate_text(detail.get("detail_title") or item["official_laureate_text"])
        split_audit[len(laureates)] = split_audit.get(len(laureates), 0) + 1
        detail_slug = item["detail_url"].rstrip("/").split("/")[-1]
        log(
            f"detail {item_index}/{len(items)} parsed: {item['award_year']} "
            f"{item['award_category']} -> {len(laureates)} row(s)"
        )

        for winner_index, laureate_name in enumerate(laureates, start=1):
            laureate_is_org = is_organization_name(laureate_name)
            given_name, family_name = split_person_name(laureate_name, laureate_is_org)
            funder_award_id = "-".join([
                "princess-asturias",
                item["award_year"],
                slugify(item["award_category"]),
                slugify(detail_slug),
                str(winner_index),
                slugify(laureate_name),
            ])
            rows.append({
                "source_url": item["list_url"],
                "landing_page_url": item["detail_url"],
                "faq_url": FAQ_URL,
                "award_year": item["award_year"],
                "award_prefix": item["award_prefix"],
                "award_category": item["award_category"],
                "prize_title": item["prize_title"],
                "detail_prize_title": detail["detail_prize_title"],
                "official_laureate_text": item["official_laureate_text"],
                "detail_title": detail["detail_title"],
                "laureate_name": laureate_name,
                "laureate_given_name": given_name,
                "laureate_family_name": family_name,
                "laureate_is_organization": str(laureate_is_org).lower(),
                "winner_index": str(winner_index),
                "winner_count": str(len(laureates)),
                "portion": f"1/{len(laureates)}" if len(laureates) != 1 else "1",
                "citation": detail["citation"],
                "meta_description": detail["meta_description"],
                "current_award_amount_eur": CURRENT_AWARD_AMOUNT_EUR,
                "source_award_amount": None,
                "currency": CURRENCY,
                "amount_note": AMOUNT_NOTE,
                "funder_award_id": funder_award_id,
                "downloaded_at": utc_now(),
            })

    log(f"Split audit (official items -> emitted rows): {split_audit}")
    return rows


def validate_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise RuntimeError("No Princess of Asturias rows parsed; stopping before parquet write.")
    seen: set[str] = set()
    for row in rows:
        award_id = row.get("funder_award_id")
        if not award_id:
            raise RuntimeError(f"Missing funder_award_id for row: {row}")
        if award_id in seen:
            raise RuntimeError(f"Duplicate funder_award_id would be emitted: {award_id}")
        seen.add(award_id)
        for column in ("award_year", "award_category", "laureate_name", "landing_page_url"):
            if not row.get(column):
                raise RuntimeError(f"Missing {column} for row: {row}")


def log_summary(df: pd.DataFrame) -> None:
    log(f"DataFrame shape: {df.shape}")
    log(
        "Coverage: "
        f"name={df.laureate_name.notna().sum()}, "
        f"year={df.award_year.notna().sum()}, "
        f"category={df.award_category.notna().sum()}, "
        f"citation={df.citation.notna().sum()}, "
        f"amount={df.source_award_amount.notna().sum()}, "
        f"currency={df.currency.notna().sum()}"
    )
    log(f"Year range: {df['award_year'].min()}-{df['award_year'].max()}")
    log(f"Rows by category: {df['award_category'].value_counts().sort_index().to_dict()}")
    log(f"Rows by winner_count: {df['winner_count'].value_counts().sort_index().to_dict()}")
    log(f"Institutional laureate rows: {(df['laureate_is_organization'] == 'true').sum()}")
    log("Mapped amount is intentionally NULL; raw current_award_amount_eur preserves the current FAQ rule.")


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
    prev_path = output_dir / "_prev_princess_asturias_laureates.parquet"
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


def upload_to_s3(local_path: Path) -> None:
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3

    s3 = boto3.client("s3")
    s3.upload_file(str(local_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Princess of Asturias Awards -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--checkpoint-file", type=Path, default=Path("/tmp/princess_asturias_checkpoint.json"))
    parser.add_argument("--limit-detail-pages", type=int, default=None, help="Smoke-test: fetch only first N award items")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument(
        "--allow-shrink",
        action="store_true",
        help="Override the runbook §1.4 shrink-check. Only use after confirming a smaller corpus is intentional.",
    )
    parser.add_argument("--no-cache", action="store_true", help="Refetch pages even when checkpoint cache exists")
    args = parser.parse_args()

    log("=" * 72)
    log("Princess of Asturias Awards -> S3 starting")
    log(f"Official source: {FULL_LIST_URL}")
    log(f"S3 target: s3://{S3_BUCKET}/{S3_KEY}")

    session = requests.Session()
    rows = fetch_rows(
        session=session,
        checkpoint_file=args.checkpoint_file,
        use_cache=not args.no_cache,
        limit_detail_pages=args.limit_detail_pages,
    )
    validate_rows(rows)

    df = pd.DataFrame(rows)
    if df["funder_award_id"].duplicated().any():
        examples = df.loc[
            df["funder_award_id"].duplicated(keep=False),
            ["funder_award_id", "landing_page_url", "laureate_name"],
        ]
        raise RuntimeError("Duplicate funder_award_id values found:\n" + examples.to_string(index=False))

    log_summary(df)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / OUTPUT_FILE
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {parquet_path}")

    if args.skip_upload:
        log("--skip-upload set; done.")
        return

    if not check_no_shrink(len(df), args.allow_shrink, Path(args.output_dir)):
        raise SystemExit("§1.4 shrink-check failed. See above; re-run with --allow-shrink if intentional.")
    upload_to_s3(parquet_path)


if __name__ == "__main__":
    main()
