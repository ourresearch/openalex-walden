#!/usr/bin/env python3
"""
TWAS Awards to S3 (PRIZE PATTERN)
=================================

Fetches TWAS Award / Prize recipients from the awarding body's official
website. This follows the awards runbook's prize-pattern source-authority rule:
use the awarding body directly and avoid third-party backfills.

Primary official source:
  - https://twas.org/recipients-twas-awards-and-prizes

That archive currently links official TWAS announcement pages through 2022.
The script also includes explicit official TWAS announcement pages for the
newer 2024 and 2026 TWAS Awards so the corpus does not silently stop at the
stale archive page:
  - https://twas.org/article/winners-2024-twas-awards-announced
  - https://twas.org/article/twas-announces-2024-slate-awards

Output:
  s3://openalex-ingest/awards/twas_awards/twas_awards.parquet

Awarding body in OpenAlex:
  The World Academy of Sciences (F4320321078, ROR https://ror.org/00twbd828,
  DOI 10.13039/501100002222)

Amount handling:
  Older archive rows and 2010-2011 announcement pages do not publish reliable
  per-recipient prize amounts, so amount fields are NULL. From 2012 onward,
  announcement pages often state a USD cash value. For page-level values such
  as "Each TWAS Prize carries USD15,000", the script apportions the amount
  across shared category groups. For 2026, the official page lists per-winner
  USD5,000 / USD10,000 lines, so those values are already per-laureate.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, NavigableString, Tag

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

BASE_URL = "https://twas.org"
ARCHIVE_URL = f"{BASE_URL}/recipients-twas-awards-and-prizes"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/twas_awards/twas_awards.parquet"
OUTPUT_FILE = "twas_awards.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.25
RETRIES = 3

EXTRA_OFFICIAL_ARTICLE_URLS = [
    f"{BASE_URL}/article/winners-2024-twas-awards-announced",
    f"{BASE_URL}/article/twas-announces-2024-slate-awards",
]


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def collapse_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()
    text = re.sub(r"\s+([,;.:])", r"\1", text)
    return text or None


def slugify(value: str | None) -> str:
    value = (value or "").lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "unknown"


def split_name(name: str | None) -> tuple[str | None, str | None]:
    """Canonical prize-ingest splitter used by Wolf/Kavli/HHMI patterns."""
    if not name:
        return None, None
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def request_html(session: requests.Session, url: str) -> str:
    last_err: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        started = time.time()
        try:
            response = session.get(url, headers=HEADERS, timeout=30)
            elapsed = time.time() - started
            log(f"GET {url} -> {response.status_code} {len(response.content)} bytes in {elapsed:.1f}s")
            response.raise_for_status()
            if not response.content:
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
    if not isinstance(payload, dict):
        return {}
    pages = payload.get("pages")
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


def page_body(soup: BeautifulSoup) -> Tag:
    bodies = soup.select(".field--name-body")
    if not bodies:
        raise RuntimeError("Could not find article body (.field--name-body)")
    return bodies[-1]


def page_title(soup: BeautifulSoup) -> str | None:
    h1 = soup.find("h1")
    return collapse_text(h1.get_text(" ", strip=True) if h1 else (soup.title.get_text(" ", strip=True) if soup.title else None))


def discover_linked_article_urls(archive_soup: BeautifulSoup) -> list[str]:
    body = page_body(archive_soup)
    urls: list[str] = []
    for anchor in body.find_all("a", href=True):
        href = anchor["href"]
        normalized_href = href.lstrip("/")
        if not (normalized_href.startswith("article/") or normalized_href.startswith("node/")):
            continue
        url = urljoin(ARCHIVE_URL, href)
        if url not in urls:
            urls.append(url)
    for url in EXTRA_OFFICIAL_ARTICLE_URLS:
        if url not in urls:
            urls.append(url)
    return urls


def clean_field(value: str | None) -> str | None:
    text = collapse_text(value)
    if not text:
        return None
    text = re.sub(r"^\d{4}\s+TWAS\s+(?:Prize|Award|Awards)\s+(?:in|for)\s+", "", text, flags=re.I)
    text = re.sub(r"^TWAS[- ]Celso Furtado Prize in\s+", "", text, flags=re.I)
    text = re.sub(r"\s*\((?:shared|Shared)\)\s*$", "", text).strip()
    text = re.sub(r",\s*shared$", "", text, flags=re.I).strip()
    text = text.strip(" :")
    replacements = {
        "Engineering Science": "Engineering Sciences",
        "Earth Sciences": "Earth Sciences",
    }
    return replacements.get(text, text) or None


def award_name_for(year: str, field: str | None, source_label: str | None = None) -> str:
    label = collapse_text(source_label)
    if label and re.search(r"\bTWAS\b", label, flags=re.I):
        return re.sub(r",\s*shared$", "", label, flags=re.I).strip()
    field = field or "Unknown Field"
    if int(year) <= 2002:
        return f"TWAS Award in Basic Sciences - {field}"
    if int(year) >= 2022:
        return f"TWAS Award in {field}"
    return f"TWAS Prize in {field}"


def extract_amount_from_text(text: str | None) -> tuple[str | None, str | None]:
    match = re.search(r"USD\s*([0-9][0-9,]*)", text or "", flags=re.I)
    if not match:
        return None, None
    return match.group(1).replace(",", ""), "USD"


def split_citation(text: str | None) -> tuple[str | None, str | None]:
    text = collapse_text(text)
    if not text:
        return None, None
    match = re.search(r",?\s+(for\s+.*)$", text, flags=re.I)
    if match:
        before = collapse_text(text[: match.start()])
        citation = collapse_text(match.group(1))
        return before, citation
    match = re.search(r",?\s+(is|are|was|were)\s+(?:honou?red|recognized|recognised)\s+(.*)$", text, flags=re.I)
    if match:
        before = collapse_text(text[: match.start()])
        return before, collapse_text(text[match.start():])
    return text, None


def parse_name_country(prefix: str | None) -> tuple[str | None, str | None, str | None]:
    """Return (name, country_or_nationality, affiliation_or_context)."""
    text = collapse_text(prefix)
    if not text:
        return None, None, None
    text = re.sub(r"^Winner:\s*", "", text, flags=re.I)
    text = re.sub(r"^\*+\s*", "", text).strip(" ,")

    # 2022-style: "NAME, Female, from Brazil" or "NAME, FTWAS, Male, from China".
    match = re.match(
        r"^(?P<name>.+?),\s*(?:(?:FTWAS|Male|Female),?\s*)*(?:from|of)\s+(?P<country>.+)$",
        text,
        flags=re.I,
    )
    if match:
        return collapse_text(match.group("name")), collapse_text(match.group("country")), None

    # Common announcement style: "NAME of India" / "NAME from China".
    match = re.match(r"^(?P<name>.+?)\s+(?:of|from)\s+(?P<country>[^,]+(?:,\s*China| and [^,]+)?)$", text, flags=re.I)
    if match:
        return collapse_text(match.group("name")), collapse_text(match.group("country")), None

    # 2011/2012 style: "NAME (Country), of Affiliation".
    match = re.match(r"^(?P<name>.+?)\s*\((?P<country>[^)]+)\),?\s*(?P<context>.*)$", text)
    if match:
        return (
            collapse_text(match.group("name")),
            collapse_text(match.group("country")),
            collapse_text(match.group("context").lstrip(", ")),
        )

    return text, None, None


def amount_per_laureate(amount: str | None, group_size: int | None, per_winner_amount: bool) -> str | None:
    if not amount:
        return None
    if per_winner_amount:
        return amount
    size = max(group_size or 1, 1)
    value = float(amount) / size
    if value.is_integer():
        return str(int(value))
    return f"{value:.6f}".rstrip("0").rstrip(".")


def portion_for(group_size: int | None) -> str:
    size = max(group_size or 1, 1)
    value = 1.0 / size
    return f"{value:.8f}".rstrip("0").rstrip(".")


def make_row(
    *,
    year: str,
    field: str | None,
    laureate_name: str,
    source_url: str,
    source_page_title: str | None,
    award_name: str | None = None,
    country: str | None = None,
    affiliation: str | None = None,
    citation: str | None = None,
    raw_text: str | None = None,
    source_award_amount: str | None = None,
    currency: str | None = None,
    group_size: int = 1,
    per_winner_amount: bool = False,
    parser_path: str,
) -> dict[str, Any]:
    name = collapse_text(laureate_name)
    if not name:
        raise ValueError("laureate_name is required")
    field = clean_field(field)
    award_name = award_name or award_name_for(year, field)
    given_name, family_name = split_name(name)
    funder_award_id = f"twas-awards-{year}-{slugify(field)}-{slugify(name)}"
    raw_text = collapse_text(raw_text)
    citation = collapse_text(citation)
    affiliation = collapse_text(affiliation)
    country = collapse_text(country)
    return {
        "funder_award_id": funder_award_id,
        "award_year": str(year),
        "award_name": award_name,
        "award_field": field,
        "laureate_name": name,
        "laureate_given_name": given_name,
        "laureate_family_name": family_name,
        "laureate_country_or_nationality": country,
        "laureate_affiliation_or_context": affiliation,
        "citation": citation,
        "description": citation or raw_text,
        "source_award_amount": source_award_amount,
        "amount_per_laureate": amount_per_laureate(source_award_amount, group_size, per_winner_amount),
        "currency": currency,
        "award_group_size": str(group_size),
        "portion": portion_for(group_size),
        "landing_page_url": source_url,
        "source_page_title": source_page_title,
        "source_url": source_url,
        "parser_path": parser_path,
        "raw_record_text": raw_text,
        "downloaded_at": utc_now(),
    }


def parse_direct_archive(archive_html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(archive_html, "html.parser")
    body = page_body(soup)
    rows: list[dict[str, Any]] = []
    current_year: str | None = None
    for child in body.children:
        if not isinstance(child, Tag):
            continue
        if child.name in {"h2", "h3", "h4"}:
            match = re.search(r"\b((?:19|20)\d{2})\b", child.get_text(" ", strip=True))
            current_year = match.group(1) if match else current_year
            continue
        if child.name != "p" or not current_year:
            continue
        field_el = child.find("em")
        name_el = child.find("strong")
        if not field_el or not name_el:
            continue
        field = clean_field(field_el.get_text(" ", strip=True))
        name = collapse_text(name_el.get_text(" ", strip=True))
        name = re.sub(r"^[•*-]\s*", "", name or "")
        text = collapse_text(child.get_text(" ", strip=True))
        rest = text or ""
        for prefix in [field or "", name or ""]:
            if prefix and rest.lower().startswith(prefix.lower()):
                rest = collapse_text(rest[len(prefix):]) or ""
        rest = rest.lstrip(" ,")
        affiliation, citation = split_citation(rest)
        rows.append(
            make_row(
                year=current_year,
                field=field,
                laureate_name=name,
                source_url=ARCHIVE_URL,
                source_page_title=page_title(soup),
                country=None,
                affiliation=affiliation,
                citation=citation,
                raw_text=text,
                parser_path="archive:em-strong-paragraph",
            )
        )
    return rows


def text_positions(text: str, names: list[str]) -> list[tuple[str, int, int]]:
    positions: list[tuple[str, int, int]] = []
    cursor = 0
    for name in names:
        idx = text.find(name, cursor)
        if idx < 0:
            idx = text.find(name)
        if idx >= 0:
            positions.append((name, idx, idx + len(name)))
            cursor = idx + len(name)
    return positions


def field_from_paragraph(text: str, fallback: str | None = None) -> str | None:
    patterns = [
        r"\b\d{4}\s+TWAS\s+Prize\s+(?:in|for)\s+(?:the\s+)?(?P<field>[^.]+?)\s*\.",
        r"\b\d{4}\s+TWAS\s+Award\s+(?:in|for)\s+(?:the\s+)?(?P<field>[^.]+?)\s*\.",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if match:
            return clean_field(match.group("field"))
    return clean_field(fallback)


def parse_strong_name_paragraphs(
    body: Tag,
    *,
    year: str,
    source_url: str,
    source_page_title: str | None,
    source_award_amount: str | None,
    currency: str | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for para in body.find_all("p", recursive=False):
        text = collapse_text(para.get_text(" ", strip=True))
        if not text or "TWAS Prize" not in text:
            continue
        names = [collapse_text(s.get_text(" ", strip=True)) for s in para.find_all("strong")]
        names = [name for name in names if name and "TWAS Prize" not in name]
        if not names:
            continue
        field = field_from_paragraph(text)
        group_size = len(names)
        positions = text_positions(text, names)
        award_phrase = re.search(r"\b\d{4}\s+TWAS\s+Prize\b.*?\.", text, flags=re.I)
        group_citation = collapse_text(text[award_phrase.end():]) if award_phrase else text
        for idx, (name, start, end) in enumerate(positions):
            next_start = positions[idx + 1][1] if idx + 1 < len(positions) else (award_phrase.start() if award_phrase else len(text))
            context = collapse_text(text[end:next_start].lstrip(" ,and"))
            country = None
            affiliation = context
            country_match = re.match(r"^\(([^)]+)\),?\s*(.*)$", context or "")
            if country_match:
                country = collapse_text(country_match.group(1))
                affiliation = collapse_text(country_match.group(2).lstrip(", "))
            rows.append(
                make_row(
                    year=year,
                    field=field,
                    laureate_name=name,
                    source_url=source_url,
                    source_page_title=source_page_title,
                    affiliation=affiliation,
                    country=country,
                    citation=group_citation,
                    raw_text=text,
                    source_award_amount=source_award_amount,
                    currency=currency,
                    group_size=group_size,
                    parser_path="article:strong-name-paragraph",
                )
            )
    return rows


COUNTRY_PAREN_NAME_RE = re.compile(r"(?P<name>[A-ZÀ-ÖØ-Þ][A-Za-zÀ-ÖØ-öø-ÿ.' -]+?\s+[A-ZÀ-ÖØ-Þ][A-ZÀ-ÖØ-Þ -]{1,})\s*\((?P<country>[^)]+)\)")


def parse_2012_h3_sections(
    body: Tag,
    *,
    source_url: str,
    source_page_title: str | None,
    source_award_amount: str | None,
    currency: str | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    headings = body.find_all("h3")
    for heading in headings:
        field = clean_field(heading.get_text(" ", strip=True))
        para = heading.find_next_sibling("p")
        if not para:
            continue
        text = collapse_text(para.get_text(" ", strip=True))
        matches = list(COUNTRY_PAREN_NAME_RE.finditer(text or ""))
        if not matches:
            continue
        group_size = len(matches)
        for match in matches:
            name = collapse_text(match.group("name"))
            if name and " and " in name:
                name = collapse_text(name.rsplit(" and ", 1)[1])
            country = collapse_text(match.group("country"))
            rows.append(
                make_row(
                    year="2012",
                    field=field,
                    laureate_name=name,
                    source_url=source_url,
                    source_page_title=source_page_title,
                    country=country,
                    citation=text,
                    raw_text=text,
                    source_award_amount=source_award_amount,
                    currency=currency,
                    group_size=group_size,
                    parser_path="article:2012-h3-country-parentheses",
                )
            )
    return rows


def category_block_pairs(body: Tag) -> Iterable[tuple[str, Tag]]:
    last_category: str | None = None
    for child in body.children:
        if not isinstance(child, Tag):
            continue
        text = collapse_text(child.get_text(" ", strip=True))
        if child.name == "p" and text and len(text) < 120 and not re.search(r"\bPrize carries\b", text, flags=re.I):
            strong_text = collapse_text(child.find("strong").get_text(" ", strip=True)) if child.find("strong") else text
            if strong_text and re.search(r"Science|Physics|Mathematics|Biology|Chemistry|Social", strong_text, flags=re.I):
                last_category = strong_text
                continue
        if child.name == "blockquote" and last_category:
            yield last_category, child
            last_category = None


def parse_list_item_like_text(
    text: str,
    *,
    year: str,
    field: str | None,
    source_url: str,
    source_page_title: str | None,
    source_award_amount: str | None,
    currency: str | None,
    group_size: int,
    parser_path: str,
) -> dict[str, Any] | None:
    text = collapse_text(text)
    if not text:
        return None
    prefix, citation = split_citation(text)
    name, country, affiliation = parse_name_country(prefix)
    if not name:
        return None
    return make_row(
        year=year,
        field=field,
        laureate_name=name,
        source_url=source_url,
        source_page_title=source_page_title,
        country=country,
        affiliation=affiliation,
        citation=citation,
        raw_text=text,
        source_award_amount=source_award_amount,
        currency=currency,
        group_size=group_size,
        parser_path=parser_path,
    )


def parse_blockquote_sections(
    body: Tag,
    *,
    year: str,
    source_url: str,
    source_page_title: str | None,
    source_award_amount: str | None,
    currency: str | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for category, blockquote in category_block_pairs(body):
        field = clean_field(category)
        list_items = blockquote.find_all("li")
        if list_items:
            group_size = len(list_items)
            for li in list_items:
                row = parse_list_item_like_text(
                    li.get_text(" ", strip=True),
                    year=year,
                    field=field,
                    source_url=source_url,
                    source_page_title=source_page_title,
                    source_award_amount=source_award_amount,
                    currency=currency,
                    group_size=group_size,
                    parser_path="article:blockquote-list-item",
                )
                if row:
                    rows.append(row)
            continue

        text = collapse_text(blockquote.get_text(" ", strip=True))
        names = [collapse_text(s.get_text(" ", strip=True)) for s in blockquote.find_all("strong")]
        names = [name for name in names if name]
        positions = text_positions(text or "", names)
        group_size = len(positions) or 1
        if positions:
            for idx, (name, _start, end) in enumerate(positions):
                next_start = positions[idx + 1][1] if idx + 1 < len(positions) else len(text or "")
                context = collapse_text((text or "")[end:next_start].lstrip(" ,and"))
                context_prefix, _ = split_citation(context)
                _, country, affiliation = parse_name_country(f"{name} {context_prefix or ''}")
                rows.append(
                    make_row(
                        year=year,
                        field=field,
                        laureate_name=name,
                        source_url=source_url,
                        source_page_title=source_page_title,
                        country=country,
                        affiliation=affiliation or context_prefix,
                        citation=text,
                        raw_text=text,
                        source_award_amount=source_award_amount,
                        currency=currency,
                        group_size=group_size,
                        parser_path="article:blockquote-strong-paragraph",
                    )
                )
    return rows


def parse_2022_h4_awards_section(
    body: Tag,
    *,
    source_url: str,
    source_page_title: str | None,
    source_award_amount: str | None,
    currency: str | None,
) -> list[dict[str, Any]]:
    heading = next((h for h in body.find_all("h4") if collapse_text(h.get_text(" ", strip=True)) == "2022 TWAS Awards"), None)
    if not heading:
        return []
    section_div = heading.find_next("div")
    if not section_div:
        return []
    rows: list[dict[str, Any]] = []
    current_field: str | None = None
    for child in section_div.children:
        if not isinstance(child, Tag):
            continue
        if child.name == "h5":
            current_field = clean_field(child.get_text(" ", strip=True))
            continue
        if child.name == "ul" and current_field:
            items = child.find_all("li", recursive=False)
            group_size = len(items)
            for li in items:
                name_el = li.find("strong")
                name = collapse_text(name_el.get_text(" ", strip=True) if name_el else None)
                citation_el = li.find("em")
                citation = collapse_text(citation_el.get_text(" ", strip=True) if citation_el else None)
                text = collapse_text(li.get_text(" ", strip=True))
                context = text
                if name:
                    context = collapse_text((context or "").replace(name, "", 1))
                if citation:
                    context = collapse_text((context or "").replace(citation, "", 1))
                _name, country, affiliation = parse_name_country(f"{name}, {context}" if context else name)
                rows.append(
                    make_row(
                        year="2022",
                        field=current_field,
                        laureate_name=name,
                        source_url=source_url,
                        source_page_title=source_page_title,
                        country=country,
                        affiliation=affiliation,
                        citation=citation,
                        raw_text=text,
                        source_award_amount=source_award_amount,
                        currency=currency,
                        group_size=group_size,
                        parser_path="article:2022-h4-h5-list",
                    )
                )
    return rows


def parse_2024_h4_sections(
    body: Tag,
    *,
    source_url: str,
    source_page_title: str | None,
    source_award_amount: str | None,
    currency: str | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for heading in body.find_all("h4"):
        label = collapse_text(heading.get_text(" ", strip=True))
        if not label or "2024 TWAS Award" not in label:
            continue
        field = clean_field(label)
        names_p = heading.find_next_sibling("p")
        citation_ul = names_p.find_next_sibling("ul") if names_p else None
        names_text = collapse_text(names_p.get_text(" ", strip=True) if names_p else None)
        if not names_text:
            continue
        name_parts = [part.strip() for part in re.split(r"\s*&\s*", names_text) if part.strip()]
        citations = [collapse_text(li.get_text(" ", strip=True)) for li in (citation_ul.find_all("li") if citation_ul else [])]
        group_size = len(name_parts)
        for idx, name_part in enumerate(name_parts):
            name, country, affiliation = parse_name_country(name_part)
            citation = citations[idx] if idx < len(citations) else collapse_text(" ".join(c for c in citations if c))
            rows.append(
                make_row(
                    year="2024",
                    field=field,
                    laureate_name=name,
                    source_url=source_url,
                    source_page_title=source_page_title,
                    award_name=re.sub(r",\s*shared$", "", label, flags=re.I),
                    country=country,
                    affiliation=affiliation,
                    citation=citation,
                    raw_text=f"{label} {names_text} {citation or ''}",
                    source_award_amount=source_award_amount,
                    currency=currency,
                    group_size=group_size,
                    parser_path="article:2024-h4-sections",
                )
            )
    return rows


def parse_2026_slate_sections(
    body: Tag,
    *,
    source_url: str,
    source_page_title: str | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    twas_awards_h2 = next((h for h in body.find_all("h2") if collapse_text(h.get_text(" ", strip=True)) == "TWAS Awards"), None)
    if not twas_awards_h2:
        return rows

    for para in twas_awards_h2.find_all_next("p"):
        text = collapse_text(para.get_text(" ", strip=True))
        if not text:
            continue
        if text.startswith("Tags"):
            break
        if not text.startswith("2026 TWAS Award"):
            continue
        strong_parts = [collapse_text(s.get_text(" ", strip=True)) for s in para.find_all("strong")]
        strong_parts = [part for part in strong_parts if part]
        if not strong_parts:
            continue
        award_label = strong_parts[0]
        amount, currency = extract_amount_from_text(strong_parts[1] if len(strong_parts) > 1 else text)
        winner_line = next((part for part in strong_parts if part.startswith("Winner:")), None)
        if not winner_line:
            winner_line = re.search(r"Winner:\s*(.+)$", text)
            winner_line = winner_line.group(0) if winner_line else None
        if not winner_line:
            continue
        name, country, affiliation = parse_name_country(winner_line)
        citation_p = para.find_next_sibling("p")
        citation = collapse_text(citation_p.get_text(" ", strip=True) if citation_p else None)
        field = clean_field(award_label)
        rows.append(
            make_row(
                year="2026",
                field=field,
                laureate_name=name,
                source_url=source_url,
                source_page_title=source_page_title,
                award_name=award_label,
                country=country,
                affiliation=affiliation,
                citation=citation,
                raw_text=f"{text} {citation or ''}",
                source_award_amount=amount,
                currency=currency,
                group_size=1,
                per_winner_amount=True,
                parser_path="article:2026-slate-twas-awards-section",
            )
        )
    return rows


def parse_article_page(url: str, html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    body = page_body(soup)
    title = page_title(soup)
    text = collapse_text(body.get_text(" ", strip=True))
    amount, currency = extract_amount_from_text(text)
    if not amount and "twas-announces-new-slate-award-winners" in url:
        amount, currency = extract_amount_from_text(html)
    rows: list[dict[str, Any]] = []

    if "2010" in title or "2011" in title:
        year = "2010" if "2010" in title else "2011"
        rows.extend(
            parse_strong_name_paragraphs(
                body,
                year=year,
                source_url=url,
                source_page_title=title,
                source_award_amount=amount,
                currency=currency,
            )
        )
    elif "2012" in title:
        rows.extend(
            parse_2012_h3_sections(
                body,
                source_url=url,
                source_page_title=title,
                source_award_amount=amount,
                currency=currency,
            )
        )
    elif "2022" in text and "2022 TWAS Awards" in text:
        rows.extend(
            parse_2022_h4_awards_section(
                body,
                source_url=url,
                source_page_title=title,
                source_award_amount=amount,
                currency=currency,
            )
        )
    elif "2024 TWAS Award" in text and "winners-2024-twas-awards" in url:
        rows.extend(
            parse_2024_h4_sections(
                body,
                source_url=url,
                source_page_title=title,
                source_award_amount=amount,
                currency=currency,
            )
        )
    elif "2026 TWAS Award" in text and "twas-announces-2024-slate-awards" in url:
        rows.extend(parse_2026_slate_sections(body, source_url=url, source_page_title=title))
    else:
        year_match = re.search(r"\b((?:19|20)\d{2})\b", title or url)
        if not year_match:
            year_match = re.search(r"\b((?:19|20)\d{2})\b", text or "")
        if not year_match:
            raise RuntimeError(f"Could not infer year for {url}")
        rows.extend(
            parse_blockquote_sections(
                body,
                year=year_match.group(1),
                source_url=url,
                source_page_title=title,
                source_award_amount=amount,
                currency=currency,
            )
        )

    if not rows:
        raise RuntimeError(f"Parser produced zero rows for {url}")
    return rows


def dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    duplicate_ids = (
        pd.Series([row["funder_award_id"] for row in rows])
        .value_counts()
        .loc[lambda counts: counts > 1]
    )
    if not duplicate_ids.empty:
        raise RuntimeError(f"Duplicate funder_award_id values: {duplicate_ids.head(10).to_dict()}")
    return rows


def fetch_all(use_cache: bool, checkpoint_file: Path) -> list[dict[str, Any]]:
    session = requests.Session()
    archive_html = get_page(session, ARCHIVE_URL, checkpoint_file, use_cache)
    archive_soup = BeautifulSoup(archive_html, "html.parser")
    rows = parse_direct_archive(archive_html)
    log(f"Parsed {len(rows)} rows from direct archive sections")

    article_urls = discover_linked_article_urls(archive_soup)
    log(f"Discovered {len(article_urls)} official TWAS article pages")
    for url in article_urls:
        article_html = get_page(session, url, checkpoint_file, use_cache)
        article_rows = parse_article_page(url, article_html)
        log(f"Parsed {len(article_rows)} rows from {url}")
        rows.extend(article_rows)

    deduped = dedupe_rows(rows)
    duplicate_ids = [key for key, count in pd.Series([row["funder_award_id"] for row in deduped]).value_counts().items() if count > 1]
    if duplicate_ids:
        raise RuntimeError(f"Duplicate funder_award_id values after dedupe: {duplicate_ids[:10]}")
    return deduped


def validate_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise RuntimeError("No rows parsed")
    required = ["funder_award_id", "award_year", "award_name", "award_field", "laureate_name", "landing_page_url"]
    missing = {col: sum(not row.get(col) for row in rows) for col in required}
    bad = {col: count for col, count in missing.items() if count}
    if bad:
        raise RuntimeError(f"Required fields missing values: {bad}")
    ids = [row["funder_award_id"] for row in rows]
    duplicates = pd.Series(ids).value_counts()
    duplicate_ids = duplicates[duplicates > 1]
    if not duplicate_ids.empty:
        raise RuntimeError(f"Duplicate funder_award_id values: {duplicate_ids.head(10).to_dict()}")
    years = [int(row["award_year"]) for row in rows]
    if min(years) < 1985 or max(years) > datetime.now().year + 2:
        raise RuntimeError(f"Unexpected award year range: {min(years)}-{max(years)}")


def build_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "funder_award_id",
        "award_year",
        "award_name",
        "award_field",
        "laureate_name",
        "laureate_given_name",
        "laureate_family_name",
        "laureate_country_or_nationality",
        "laureate_affiliation_or_context",
        "citation",
        "description",
        "source_award_amount",
        "amount_per_laureate",
        "currency",
        "award_group_size",
        "portion",
        "landing_page_url",
        "source_page_title",
        "source_url",
        "parser_path",
        "raw_record_text",
        "downloaded_at",
    ]
    df = pd.DataFrame(rows)
    for column in columns:
        if column not in df.columns:
            df[column] = None
    return df[columns].sort_values(["award_year", "award_field", "laureate_name"]).reset_index(drop=True).astype("string")


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
    prev_path = output_dir / "_prev_twas_awards.parquet"
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


def upload_to_s3(local_file: str) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for upload; rerun with --skip-upload without AWS credentials") from exc
    log(f"Uploading {local_file} to s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(local_file, S3_BUCKET, S3_KEY)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch official TWAS Awards recipient data")
    parser.add_argument("--output", default=OUTPUT_FILE, help="Local parquet path")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Alias for --output\'s parent dir; if set, write to {output-dir}/twas_awards.parquet. Matches the fleet convention used by other scrapers in scripts/local/.",
    )
    parser.add_argument("--checkpoint", default=".cache/twas_awards_pages.json", help="HTML cache/checkpoint path")
    parser.add_argument("--skip-upload", action="store_true", help="Write parquet locally but do not upload to S3")
    parser.add_argument(
        "--allow-shrink",
        action="store_true",
        help="Override the runbook §1.4 shrink-check. Only use after confirming a smaller corpus is intentional.",
    )
    parser.add_argument("--no-cache", action="store_true", help="Ignore cached HTML and refetch every page")
    args = parser.parse_args()

    # Resolve --output-dir to --output (fleet-fix 2026-05-22)
    if getattr(args, 'output_dir', None) is not None:
        args.output = args.output_dir / 'twas_awards.parquet'
    rows = fetch_all(use_cache=not args.no_cache, checkpoint_file=Path(args.checkpoint))
    validate_rows(rows)
    df = build_dataframe(rows)
    log(
        "Validated TWAS rows: "
        f"{len(df)} rows, {df['funder_award_id'].nunique()} distinct IDs, "
        f"years {df['award_year'].min()}-{df['award_year'].max()}, "
        f"{df['amount_per_laureate'].notna().sum()} rows with amount"
    )
    df.to_parquet(args.output, index=False)
    log(f"Wrote {len(df)} rows to {args.output}")
    if args.skip_upload:
        log("Skipping S3 upload because --skip-upload was set")
    else:
        if not check_no_shrink(len(df), args.allow_shrink, Path(args.output_dir)):
            raise SystemExit("§1.4 shrink-check failed. See above; re-run with --allow-shrink if intentional.")
        upload_to_s3(args.output)


if __name__ == "__main__":
    main()
