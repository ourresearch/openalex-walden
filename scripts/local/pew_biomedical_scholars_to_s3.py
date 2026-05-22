#!/usr/bin/env python3
"""
Pew Biomedical Scholars -> S3.

Source-authoritative ingest of The Pew Charitable Trusts' official
Biomedical Scholars directory:

    https://www.pew.org/en/projects/pew-biomedical-scholars/directory-of-pew-scholars

The directory page contains a Sitecore data model with the API endpoint,
root id, and year list. The script fetches that model, calls Pew's
year-filtered JSON API, then follows each scholar's official detail page for
research text, institution, department, research fields, keywords, and contact
metadata.

OpenAlex funder:
    F4320306148 - Pew Charitable Trusts
    ROR: https://ror.org/02xhk2825
    DOI: 10.13039/100000875

Output:
    s3://openalex-ingest/awards/pew_biomedical_scholars/pew_biomedical_scholars.parquet

Mapping notes:
    - One row per official scholar detail page.
    - `funder_award_id = pew-biomedical-scholar-{year}-{slug}`.
    - Duplicates raise, because duplicate funder_award_id values collapse rows
      downstream in the awards table.
    - `lead_investigator` in the notebook is the scholar; affiliation.name is
      the official detail-page institution.
    - Pew's directory does not publish historical per-scholar award amounts.
      The notebook leaves amount/currency NULL rather than applying a current
      program amount to historical cohorts.
    - All parquet columns are strings by design; Databricks casts explicitly.

Usage:
    python scripts/local/pew_biomedical_scholars_to_s3.py --skip-upload --max-years 1
    python scripts/local/pew_biomedical_scholars_to_s3.py --skip-upload
    python scripts/local/pew_biomedical_scholars_to_s3.py --skip-download --skip-upload
    python scripts/local/pew_biomedical_scholars_to_s3.py --allow-shrink

Requirements:
    pip install beautifulsoup4 pandas pyarrow requests boto3
"""

from __future__ import annotations

import argparse
import html
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.pew.org"
DIRECTORY_URL = (
    "https://www.pew.org/en/projects/pew-biomedical-scholars/"
    "directory-of-pew-scholars"
)
FUNDER_ID = 4320306148
FUNDER_DISPLAY_NAME = "Pew Charitable Trusts"
FUNDER_ROR = "https://ror.org/02xhk2825"
FUNDER_DOI = "10.13039/100000875"
PROVENANCE = "pew_biomedical_scholars_directory"

S3_BUCKET = "openalex-ingest"
S3_KEY = (
    "awards/pew_biomedical_scholars/"
    "pew_biomedical_scholars.parquet"
)

USER_AGENT = "openalex-walden-pew-biomedical-scholars/1.0 (+https://openalex.org)"
REQUEST_TIMEOUT = (10, 60)
REQUEST_SLEEP_SECONDS = 0.12
EXPECTED_MIN_ROWS = 750


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).replace("\xa0", " ").split())
    return text or None


def slug_from_item_url(item_url: str) -> str:
    path = urlparse(item_url).path.strip("/")
    slug = path.split("/")[-1]
    if not slug:
        raise ValueError(f"Cannot derive slug from Pew itemUrl: {item_url}")
    return slug


def strip_credentials(full_name: str | None) -> str | None:
    name = clean_text(full_name)
    if not name:
        return None
    # Degree suffixes are display credentials, not family names.
    name = re.sub(
        r",?\s+(Ph\.?\s*D\.?|M\.?\s*D\.?|D\.?\s*Phil\.?|Sc\.?\s*D\.?)\.?\s*$",
        "",
        name,
        flags=re.I,
    )
    return clean_text(name)


def split_name(full_name: str | None) -> tuple[str | None, str | None]:
    name = strip_credentials(full_name)
    if not name:
        return None, None
    parts = name.split()
    if len(parts) == 1:
        return parts[0], None
    suffixes = {"Jr.", "Jr", "Sr.", "Sr", "II", "III", "IV"}
    if parts[-1] in suffixes and len(parts) > 2:
        return " ".join(parts[:-2]), " ".join(parts[-2:])
    return " ".join(parts[:-1]), parts[-1]


def decode_cloudflare_email(cfemail: str | None) -> str | None:
    if not cfemail or not re.fullmatch(r"[0-9a-fA-F]+", cfemail):
        return None
    try:
        key = int(cfemail[:2], 16)
        chars = [
            chr(int(cfemail[i : i + 2], 16) ^ key)
            for i in range(2, len(cfemail), 2)
        ]
        return clean_text("".join(chars))
    except Exception:
        return None


class PewClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
                "Referer": DIRECTORY_URL,
            }
        )
        self._last_request = 0.0

    def get_text(self, url: str, *, accept: str = "text/html") -> str:
        elapsed = time.monotonic() - self._last_request
        if elapsed < REQUEST_SLEEP_SECONDS:
            time.sleep(REQUEST_SLEEP_SECONDS - elapsed)
        headers = {"Accept": accept}
        log(f"GET {url}")
        response = self.session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        self._last_request = time.monotonic()
        log(f"  -> {response.status_code} {len(response.content):,} bytes")
        response.raise_for_status()
        return response.text


def discover_directory_model(client: PewClient) -> dict[str, Any]:
    html_text = client.get_text(DIRECTORY_URL)
    soup = BeautifulSoup(html_text, "html.parser")
    mount = soup.select_one(".js-scholar-directory-mount[data-model]")
    if mount is None:
        raise RuntimeError("Could not find Pew scholar directory data-model")
    model = json.loads(html.unescape(mount["data-model"]))
    required = {"api", "id", "years"}
    missing = required - set(model)
    if missing:
        raise RuntimeError(f"Pew directory model missing keys: {sorted(missing)}")
    years = [clean_text(item.get("value")) for item in model["years"]]
    years = [year for year in years if year and re.fullmatch(r"\d{4}", year)]
    if not years:
        raise RuntimeError("Pew directory model did not expose any valid years")
    model["years"] = years
    model["directory_url"] = DIRECTORY_URL
    log(
        "Discovered Pew Scholar API model: "
        f"api={model['api']}, root={model['id']}, years={min(years)}-{max(years)} "
        f"({len(years)} years)"
    )
    return model


def fetch_year_rows(client: PewClient, model: dict[str, Any], year: str) -> list[dict[str, Any]]:
    api_url = urljoin(BASE_URL, model["api"])
    response_text = client.get_text(
        f"{api_url}?root={model['id']}&year={year}",
        accept="application/json",
    )
    rows = json.loads(response_text)
    if not isinstance(rows, list):
        raise RuntimeError(f"Pew API returned non-list payload for {year}: {type(rows)}")
    log(f"Fetched {len(rows):,} API rows for {year}")
    return rows


def parse_bio_info(soup: BeautifulSoup) -> dict[str, str]:
    info: dict[str, str] = {}
    for item in soup.select(".scholar-bio__info-item"):
        label = clean_text(item.select_one("dt").get_text(" ", strip=True) if item.select_one("dt") else None)
        value = clean_text(item.select_one("dd").get_text(" ", strip=True) if item.select_one("dd") else None)
        if label:
            info[label] = value
    return info


def parse_research_text(soup: BeautifulSoup) -> str | None:
    for rich_text in soup.select('[data-sitecore-component-name="Rich Text"]'):
        heading = rich_text.find(["h2", "h3", "h4"])
        heading_text = clean_text(heading.get_text(" ", strip=True) if heading else None)
        if heading_text and heading_text.lower() == "research":
            parts = [
                text
                for text in (clean_text(p.get_text(" ", strip=True)) for p in rich_text.find_all("p"))
                if text
            ]
            return "\n\n".join(parts) or None
    return None


def parse_keywords(soup: BeautifulSoup, year: str) -> list[str]:
    keywords: list[str] = []
    for section in soup.select(".scholar-details__section"):
        heading = clean_text(section.select_one(".scholar-details__heading").get_text(" ", strip=True) if section.select_one(".scholar-details__heading") else None)
        if not heading or heading == f"{year} Search Directory":
            continue
        if heading.lower() != "scholar keywords":
            continue
        for span in section.select(".scholar-details__list-item span"):
            text = clean_text(span.get_text(" ", strip=True))
            if text:
                keywords.append(text)
    return keywords


def parse_detail_page(url: str, html_text: str, api_row: dict[str, Any], year: str, downloaded_at: str) -> dict[str, Any]:
    soup = BeautifulSoup(html_text, "html.parser")
    title = clean_text(soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else None)
    full_name = clean_text(title or api_row.get("full_Name"))
    given_name, family_name = split_name(full_name)
    info = parse_bio_info(soup)

    detail_year = clean_text(info.get("Award year"))
    if detail_year and detail_year != year:
        raise RuntimeError(f"{url} API year {year} disagrees with detail year {detail_year}")
    if not detail_year:
        detail_year = year

    canonical = soup.find("link", rel="canonical")
    canonical_url = (
        urljoin(BASE_URL, canonical.get("href"))
        if canonical and canonical.get("href")
        else url
    )
    image = soup.select_one(".scholar-bio__media img")
    image_url = None
    if image is not None:
        if image.get("src"):
            image_url = urljoin(BASE_URL, image.get("src"))
        elif image.get("srcset"):
            first_src = clean_text(str(image.get("srcset")).split(",")[0].split()[0])
            if first_src:
                image_url = urljoin(BASE_URL, first_src)

    email = None
    email_span = soup.select_one(".__cf_email__[data-cfemail]")
    if email_span is not None:
        email = decode_cloudflare_email(email_span.get("data-cfemail"))

    website = None
    website_dd = None
    for item in soup.select(".scholar-bio__info-item"):
        label = clean_text(item.select_one("dt").get_text(" ", strip=True) if item.select_one("dt") else None)
        if label == "Website":
            website_dd = item.select_one("dd")
            break
    if website_dd is not None:
        link = website_dd.find("a")
        if link and link.get("href"):
            website = clean_text(link.get("href"))
        else:
            website = clean_text(website_dd.get_text(" ", strip=True))

    item_url = clean_text(api_row.get("itemUrl")) or url
    slug = slug_from_item_url(item_url)
    award_year = int(detail_year)
    start_date = f"{award_year}-01-01"
    end_date = f"{award_year + 3}-12-31"
    research_text = parse_research_text(soup)
    research_fields = [
        clean_text(part)
        for part in re.split(r",\s*", info.get("Research field") or "")
        if clean_text(part)
    ]
    keywords = parse_keywords(soup, detail_year)

    title_no_credentials = strip_credentials(full_name)
    display_name = f"Pew Biomedical Scholar: {title_no_credentials or full_name} ({award_year})"
    description_parts = []
    if research_text:
        description_parts.append(research_text)
    if research_fields:
        description_parts.append("Research field: " + "; ".join(research_fields))
    description = "\n\n".join(description_parts) or None

    return {
        "funder_award_id": f"pew-biomedical-scholar-{award_year}-{slug}",
        "slug": slug,
        "full_name": full_name,
        "name_without_credentials": title_no_credentials,
        "given_name": given_name,
        "family_name": family_name,
        "award_year": str(award_year),
        "start_date": start_date,
        "end_date": end_date,
        "display_name": display_name,
        "description": description,
        "research_text": research_text,
        "research_fields": json.dumps(research_fields, ensure_ascii=False),
        "research_fields_text": "; ".join(research_fields) if research_fields else None,
        "keywords": json.dumps(keywords, ensure_ascii=False),
        "keywords_text": "; ".join(keywords) if keywords else None,
        "job_title": clean_text(info.get("Title") or api_row.get("fellowship_Job_Title")),
        "department": clean_text(info.get("Department")),
        "institution": clean_text(info.get("Institution") or api_row.get("fellowship_Institution")),
        "address": clean_text(info.get("Address")),
        "city_state_zip": clean_text(info.get("City, State, ZIP")),
        "email": email,
        "website": website,
        "profile_url": canonical_url,
        "item_url": urljoin(BASE_URL, item_url),
        "api_image_url": urljoin(BASE_URL, api_row["imageUrl"]) if api_row.get("imageUrl") else None,
        "detail_image_url": image_url,
        "image_alt": clean_text(api_row.get("imageAlt")),
        "api_full_name": clean_text(api_row.get("full_Name")),
        "api_job_title": clean_text(api_row.get("fellowship_Job_Title")),
        "api_institution": clean_text(api_row.get("fellowship_Institution")),
        "source_api_root_id": None,
        "source_api_year": str(year),
        "source_directory_url": DIRECTORY_URL,
        "amount": None,
        "currency": None,
        "amount_note": "Pew directory does not publish historical per-scholar award amounts.",
        "downloaded_at": downloaded_at,
    }


def download_all(args: argparse.Namespace) -> list[dict[str, Any]]:
    client = PewClient()
    model = discover_directory_model(client)
    years = list(model["years"])
    if args.max_years is not None:
        years = years[: args.max_years]
        log(f"Smoke-test mode: limiting to {len(years)} year(s): {', '.join(years)}")

    downloaded_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows: list[dict[str, Any]] = []
    for year_index, year in enumerate(years, start=1):
        api_rows = fetch_year_rows(client, model, year)
        if not api_rows:
            raise RuntimeError(f"Pew API returned zero scholars for advertised year {year}")
        for row_index, api_row in enumerate(api_rows, start=1):
            item_url = clean_text(api_row.get("itemUrl"))
            if not item_url:
                raise RuntimeError(f"Pew API row missing itemUrl for {year}: {api_row}")
            detail_url = urljoin(BASE_URL, item_url)
            detail_html = client.get_text(detail_url)
            parsed = parse_detail_page(detail_url, detail_html, api_row, year, downloaded_at)
            parsed["source_api_root_id"] = model["id"]
            rows.append(parsed)
            if len(rows) == 1 or len(rows) % 25 == 0:
                log(
                    f"Processed {len(rows):,} scholars "
                    f"(year {year_index}/{len(years)}, row {row_index}/{len(api_rows)})"
                )

    raw_path = args.output_dir / "pew_biomedical_scholars_raw.json"
    raw_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2))
    log(f"Wrote raw checkpoint with {len(rows):,} rows to {raw_path}")
    return rows


def build_dataframe(rows: list[dict[str, Any]], *, full_run: bool) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    # Three official detail pages omit the Institution label. Keep those rows
    # with NULL affiliation instead of inferring from Department/Title fields.
    required = ["funder_award_id", "full_name", "award_year", "profile_url"]
    for column in required:
        missing = df[column].isna() | (df[column] == "")
        if missing.any():
            raise RuntimeError(f"{int(missing.sum())} rows missing required source field {column}")

    dup_mask = df["funder_award_id"].duplicated(keep=False)
    if dup_mask.any():
        raise RuntimeError(
            "Duplicate Pew funder_award_id values:\n"
            + df.loc[dup_mask, ["funder_award_id", "profile_url", "full_name"]].to_string(index=False)
        )

    years = pd.to_numeric(df["award_year"], errors="coerce")
    if years.isna().any() or years.min() < 1985 or years.max() > datetime.now().year + 1:
        raise RuntimeError(f"Unexpected Pew award year range: {years.min()}-{years.max()}")

    if full_run and len(df) < EXPECTED_MIN_ROWS:
        raise RuntimeError(f"Full crawl produced only {len(df):,} rows; expected at least {EXPECTED_MIN_ROWS:,}.")

    preferred = [
        "funder_award_id",
        "slug",
        "full_name",
        "name_without_credentials",
        "given_name",
        "family_name",
        "award_year",
        "start_date",
        "end_date",
        "display_name",
        "description",
        "research_text",
        "research_fields",
        "research_fields_text",
        "keywords",
        "keywords_text",
        "job_title",
        "department",
        "institution",
        "address",
        "city_state_zip",
        "email",
        "website",
        "profile_url",
        "item_url",
        "api_image_url",
        "detail_image_url",
        "image_alt",
        "api_full_name",
        "api_job_title",
        "api_institution",
        "source_api_root_id",
        "source_api_year",
        "source_directory_url",
        "amount",
        "currency",
        "amount_note",
        "downloaded_at",
    ]
    remaining = [column for column in df.columns if column not in preferred]
    df = df[preferred + remaining]
    return df.sort_values(["award_year", "slug"]).astype("string")


def write_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    output_path = output_dir / "pew_biomedical_scholars.parquet"
    df.to_parquet(output_path, index=False)
    log(f"Wrote {len(df):,} rows to {output_path} ({output_path.stat().st_size:,} bytes)")
    return output_path


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """Runbook §1.4: compare against the existing S3 parquet before upload."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError("boto3 is required for S3 upload; rerun with --skip-upload for local validation") from exc

    client = boto3.client("s3")
    log(f"§1.4 re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            log("No existing S3 parquet found; first ingest.")
            return True
        log(f"WARNING: S3 head_object failed with {code}; treating as first ingest.")
        return True

    prev_path = output_dir / "_prev_pew_biomedical_scholars.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        previous_count = len(pd.read_parquet(prev_path))
    except Exception as exc:
        log(f"ERROR: couldn't read existing S3 parquet for shrink check: {exc}")
        return False
    finally:
        prev_path.unlink(missing_ok=True)

    log(f"Existing S3 rows: {previous_count:,}; new rows: {new_count:,}")
    if new_count < previous_count:
        if allow_shrink:
            log("WARNING: --allow-shrink set; proceeding despite smaller corpus.")
            return True
        log(
            f"ERROR: Runbook §1.4 violation: refusing to shrink Pew corpus "
            f"from {previous_count:,} to {new_count:,} rows."
        )
        return False
    return True


def upload_to_s3(parquet_path: Path, df: pd.DataFrame, output_dir: Path, allow_shrink: bool) -> bool:
    if not check_no_shrink(len(df), allow_shrink, output_dir):
        return False
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    log(f"Uploading {parquet_path} -> {s3_uri}")
    try:
        subprocess.run(["aws", "s3", "cp", str(parquet_path), s3_uri], check=True)
    except FileNotFoundError:
        log("ERROR: aws CLI not found.")
        return False
    except subprocess.CalledProcessError as exc:
        log(f"ERROR: aws s3 cp failed with exit {exc.returncode}")
        return False
    log("Upload complete.")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Pew Biomedical Scholars directory -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/pew_biomedical_scholars"))
    parser.add_argument("--skip-download", action="store_true", help="Reuse pew_biomedical_scholars_raw.json")
    parser.add_argument("--skip-upload", action="store_true", help="Write local parquet only")
    parser.add_argument("--max-years", type=int, default=None, help="Smoke-test mode: limit advertised years")
    parser.add_argument("--allow-shrink", action="store_true", help="Override runbook §1.4 S3 shrink check")
    args = parser.parse_args()

    log("=" * 72)
    log("Pew Biomedical Scholars -> S3 starting")
    log(f"Source: {DIRECTORY_URL}")
    log(f"OpenAlex funder: F{FUNDER_ID} - {FUNDER_DISPLAY_NAME}")
    log(f"S3 target: s3://{S3_BUCKET}/{S3_KEY}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        raw_path = args.output_dir / "pew_biomedical_scholars_raw.json"
        if not raw_path.exists():
            raise RuntimeError(f"--skip-download requested but {raw_path} does not exist")
        rows = json.loads(raw_path.read_text())
        log(f"Loaded {len(rows):,} cached raw rows from {raw_path}")
    else:
        rows = download_all(args)

    df = build_dataframe(rows, full_run=args.max_years is None)
    log(
        "Coverage: "
        f"rows={len(df):,}, names={df['full_name'].notna().sum():,}, "
        f"years={df['award_year'].notna().sum():,}, "
        f"institutions={df['institution'].notna().sum():,}, "
        f"research_text={df['research_text'].notna().sum():,}, "
        f"research_fields={df['research_fields_text'].notna().sum():,}, "
        f"keywords={df['keywords_text'].notna().sum():,}, "
        f"year_range={df['award_year'].min()}-{df['award_year'].max()}"
    )
    parquet_path = write_parquet(df, args.output_dir)

    if args.skip_upload:
        log("--skip-upload set; manual upload command:")
        log(f"aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")
        return

    if not upload_to_s3(parquet_path, df, args.output_dir, args.allow_shrink):
        sys.exit(7)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted.")
        sys.exit(130)
