#!/usr/bin/env python3
"""
EDCTP (European & Developing Countries Clinical Trials Partnership) -> S3 Pipeline
=================================================================================

Downloads EDCTP2 grant records from EDCTP's own official public grants system at
https://www.edctpgrants.org/publicportal .

Source authority
----------------
EDCTP's project portal (edctp.org) is browsable HTML only with no export, and
CORDIS holds ONLY the umbrella EDCTP2 programme grant (one row), NOT the
individual sub-grants -- so the "slice CORDIS" path is a dead end. The
authoritative per-grant source is EDCTP's own grants-management public portal.

That portal is an AngularJS SPA (SmartSimple GMS) backed by a clean JSON search
endpoint discovered by inspecting its app bundle (`core-app.js`):

    GET https://www.edctpgrants.org/publicportal/search?pageSize=N&pageIndex=P

The response is JSON guarded by the AngularJS anti-JSON-hijacking prefix
`)]}',` which we strip before parsing. Shape:

    {"items": [ {grant}, ... ], "totalCount": 489, "pageCount": .., "pageIndex": .., "pageSize": ..}

Each grant carries everything we need directly (no per-grant detail fetch
required):

    reference                       <- stable EDCTP grant code, e.g. RIA2018D-2496
    acronym                         <- project acronym, e.g. DIAGMAL
    title                           <- project title
    hostOrganisationName            <- "Institution, Country" (lead institution + country)
    leadApplicantName               <- coordinator / PI (carries title prefixes)
    totalAward                      <- "EUR 2,999,448.75"  (EUR; symbol byte-encoding varies)
    publicSummary                   <- HTML abstract
    startDate / endDate             <- ISO datetimes
    duration                        <- months
    projectWebsite
    id                              <- portal GUID (used to build the landing-page URL
                                       and to dedup the handful of doubled rows)

NOTES / gotchas (verified 2026-06-04):
  * `totalAwardDecimal` is ALWAYS 0.0 in the public feed -- do NOT use it.
    The real amount is in the `totalAward` STRING. We parse it here (comma =
    thousands, dot = decimals) and ship a clean numeric `amount` column plus
    `total_award_raw` for audit. Currency is hardcoded EUR (EDCTP funds in euros;
    the currency glyph arrives in mixed cp1252/latin-9/utf-8 encodings, so we
    never read it -- only the digits).
  * The full result set returns a few EXACT duplicate rows (same GUID). We dedup
    on `id`; after dedup `reference` is unique and is used as funder_award_id.
  * Country sometimes uses a formal name containing a comma
    ("Tanzania, United Republic of") -- handled in split_host_org().

OpenAlex funder
---------------
F4320338462 - EDCTP (NL, ROR https://ror.org/031jv9v19, DOI 10.13039/501100001713)

Output
------
s3://openalex-ingest/awards/edctp/edctp_projects.parquet

Usage
-----
    python scripts/local/edctp_to_s3.py --limit 10 --skip-upload
    python scripts/local/edctp_to_s3.py --skip-upload
    python scripts/local/edctp_to_s3.py
    python scripts/local/edctp_to_s3.py --allow-shrink
"""

from __future__ import annotations

import argparse
import html as _htmllib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests


# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
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


BASE_URL = "https://www.edctpgrants.org"
PORTAL_URL = f"{BASE_URL}/publicportal"
SEARCH_URL = f"{BASE_URL}/publicportal/search"
FUNDER_ID = 4320338462
PROVENANCE = "edctp_grants_portal"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/edctp/edctp_projects.parquet"

HEADERS = {
    "User-Agent": "openalex-walden-edctp/1.0 (+https://openalex.org)",
    "Accept": "application/json, text/plain, */*",
    "X-Requested-With": "XMLHttpRequest",
}

DEFAULT_OUTPUT_DIR = Path("/tmp")
REQUEST_TIMEOUT = (10, 90)
MAX_RETRIES = 5
MAX_CONSECUTIVE_NON200 = 5
DEFAULT_PAGE_SIZE = 100
MAX_PAGES = 200  # safety cap; real corpus is ~5 pages at size 100

# AngularJS anti-JSON-hijacking prefix on the search response.
NGJSON_PREFIX = ")]}',"

# Leading honorific/title tokens to strip from coordinator names before the
# given/family split (verified against the live lead-name distribution).
TITLE_TOKENS = {
    "dr", "dr.", "prof", "prof.", "professor", "mr", "mr.", "mrs", "mrs.",
    "ms", "ms.", "miss", "assistant", "associate", "assoc", "assoc.",
    "sir", "dame", "mx", "mx.",
}
# Trailing degree/suffix tokens (canonical wolf_to_s3.py set; suffix list is the
# load-bearing part per runbook section 2.4.1).
SUFFIX_TOKENS = {
    "phd", "ph.d.", "md", "m.d.", "dphil", "dsc", "scd", "msc", "mph",
    "jr.", "sr.", "jr", "sr", "ii", "iii", "iv",
}

# Trailing comma-tokens that are the tail of a formal country name (so the real
# country sits in the preceding comma-token, e.g. "Tanzania, United Republic of").
FORMAL_COUNTRY_TAILS = {
    "united republic of",
    "the democratic republic of the",
    "democratic republic of the",
    "republic of",
    "bolivarian republic of",
    "plurinational state of",
    "islamic republic of",
    "province of china",
}


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    value = re.sub(r"\s+", " ", str(value)).strip()
    return value or None


def strip_html(value: Any) -> str | None:
    """publicSummary is HTML -> readable plain text."""
    if not value:
        return None
    t = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", str(value), flags=re.S | re.I)
    t = re.sub(r"<br\s*/?>", "\n", t, flags=re.I)
    t = re.sub(r"</p\s*>", "\n\n", t, flags=re.I)
    t = re.sub(r"<[^>]+>", " ", t)
    t = _htmllib.unescape(t)
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n\s*\n\s*\n+", "\n\n", t)
    return t.strip() or None


def split_name(name: str | None) -> tuple[str | None, str | None]:
    """Strip leading titles + trailing degree suffixes, then split given/family.
    Ported from research_council_norway_to_s3.py / wolf_to_s3.py per runbook 2.4.1.
    """
    if not name:
        return None, None
    tokens = re.split(r"\s+", name.strip())
    # strip leading honorifics (Dr / Prof / Professor / Assistant Professor / ...)
    while tokens and tokens[0].lower().strip(",.") in TITLE_TOKENS:
        tokens.pop(0)
    # strip trailing degree/suffix tokens (PhD / MD / Jr / ...)
    while tokens and tokens[-1].lower().strip(",.") in SUFFIX_TOKENS:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def split_host_org(host: str | None) -> tuple[str | None, str | None]:
    """'Institution name, Country' -> (institution, country).
    Handles formal country names that themselves contain a comma, e.g.
    'Kibong'oto Hospital, Tanzania, United Republic of'.
    """
    host = clean_text(host)
    if not host:
        return None, None
    parts = [p.strip() for p in host.split(",")]
    if len(parts) < 2:
        return host, None
    if parts[-1].lower() in FORMAL_COUNTRY_TAILS and len(parts) >= 2:
        country = parts[-2] or None
        institution = ", ".join(parts[:-2]).strip(" ,") or None
    else:
        country = parts[-1] or None
        institution = ", ".join(parts[:-1]).strip(" ,") or None
    return institution, country


_MONEY_RE = re.compile(r"([0-9][0-9,]*(?:\.[0-9]+)?)")


def parse_amount(raw: Any) -> float | None:
    """Parse the EUR amount out of strings like 'EUR 2,999,448.75' (comma=thousands,
    dot=decimals). totalAwardDecimal is always 0.0 in the public feed, so the
    string is the only source of truth."""
    if not raw:
        return None
    m = _MONEY_RE.search(str(raw).replace("\xa0", " "))
    if not m:
        return None
    try:
        val = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    return val if val > 0 else None


def iso_date(value: Any) -> str | None:
    if not value:
        return None
    s = str(value).strip()
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    return m.group(0) if m else None


def year_of(date_str: str | None) -> int | None:
    if not date_str:
        return None
    m = re.match(r"(\d{4})", date_str)
    return int(m.group(1)) if m else None


def scheme_prefix(reference: str | None) -> str | None:
    """Alpha prefix of the grant reference is the EDCTP call/scheme family,
    e.g. RIA2018D-2496 -> 'RIA', TMA2016-1778 -> 'TMA'."""
    if not reference:
        return None
    m = re.match(r"^([A-Za-z]+)", reference.strip())
    return m.group(1).upper() if m else None


def funding_type_for(reference: str | None) -> str:
    """TMA = Training & Mobility Actions (EDCTP fellowships); everything else is
    a research grant."""
    pref = scheme_prefix(reference)
    if pref and pref.startswith("TMA"):
        return "fellowship"
    return "research"


def fetch_page(session: requests.Session, page_index: int, page_size: int) -> dict[str, Any] | None:
    """One search page. Returns parsed JSON dict, or None on persistent failure."""
    params = {"generic": "", "pageSize": str(page_size), "pageIndex": str(page_index)}
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                sleep_s = float(r.headers.get("Retry-After") or 20 * attempt)
                log(f"  HTTP 429 on page {page_index}; sleeping {sleep_s:.0f}s")
                time.sleep(sleep_s)
                continue
            if r.status_code != 200:
                last_exc = requests.HTTPError(f"HTTP {r.status_code}")
                sleep_s = min(2 ** attempt, 30)
                log(f"  page {page_index}: HTTP {r.status_code}; retry {attempt}/{MAX_RETRIES} in {sleep_s}s")
                time.sleep(sleep_s)
                continue
            text = r.text
            if text.startswith(NGJSON_PREFIX):
                text = text[len(NGJSON_PREFIX):]
            return json.loads(text)
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last_exc = exc
            sleep_s = min(2 ** attempt, 30)
            log(f"  page {page_index}: {type(exc).__name__}; retry {attempt}/{MAX_RETRIES} in {sleep_s}s")
            time.sleep(sleep_s)
    log(f"  GIVING UP on page {page_index}: {last_exc}")
    return None


def crawl(args: argparse.Namespace) -> list[dict[str, Any]]:
    session = requests.Session()
    session.headers.update(HEADERS)
    # Establish the anonymous portal session cookie first (the search endpoint
    # 404s without it).
    try:
        session.get(PORTAL_URL, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        log(f"WARNING: portal warm-up request failed ({exc}); continuing anyway")

    by_id: dict[str, dict[str, Any]] = {}
    downloaded_at = datetime.now(timezone.utc).isoformat()
    total_count: int | None = None
    page_count: int | None = None
    consecutive_non200 = 0
    page = 1

    while page <= MAX_PAGES:
        payload = fetch_page(session, page, args.page_size)
        if payload is None:
            consecutive_non200 += 1
            if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError(
                    f"{consecutive_non200} consecutive failed pages at page {page}; "
                    "aborting rather than shipping a truncated corpus."
                )
            page += 1
            continue
        consecutive_non200 = 0

        if total_count is None:
            total_count = int(payload.get("totalCount") or 0)
            page_count = int(payload.get("pageCount") or 0)
            log(f"  feed reports totalCount={total_count:,}, pageCount={page_count:,}")

        items = payload.get("items") or []
        added = 0
        for it in items:
            gid = str(it.get("id") or "").strip()
            if not gid:
                # No GUID -> fall back to reference as the dedup key.
                gid = f"ref:{clean_text(it.get('reference')) or len(by_id)}"
            if gid not in by_id:
                by_id[gid] = it
                added += 1
        log(f"  page {page}/{page_count or '?'}: items={len(items)}, new={added}, unique_total={len(by_id)}")

        if args.limit is not None and len(by_id) >= args.limit:
            log(f"  reached --limit={args.limit}; stopping pagination")
            break
        # Authoritative terminator: the feed's own pageCount (NOT first-empty-page).
        if page_count and page >= page_count:
            break
        page += 1
        if args.page_sleep:
            time.sleep(args.page_sleep)

    records = list(by_id.values())
    if args.limit is not None:
        records = records[: args.limit]
    log(f"Collected {len(records)} unique grant records (deduped on portal id)")

    # Coverage guard for full runs: don't silently ship a partial corpus.
    if args.limit is None and total_count and len(records) < total_count * 0.98:
        raise RuntimeError(
            f"Full run produced {len(records)} unique rows, below 98% of the feed's "
            f"reported totalCount {total_count}. Refusing to ship a partial corpus."
        )
    return [flatten(it, downloaded_at) for it in records]


def flatten(it: dict[str, Any], downloaded_at: str) -> dict[str, Any]:
    reference = clean_text(it.get("reference"))
    gid = str(it.get("id") or "").strip() or None
    institution, country = split_host_org(it.get("hostOrganisationName"))
    lead_raw = clean_text(it.get("leadApplicantName"))
    given, family = split_name(lead_raw)
    amount = parse_amount(it.get("totalAward"))
    start_date = iso_date(it.get("startDate"))
    end_date = iso_date(it.get("endDate"))
    duration = it.get("duration")
    return {
        "funder_award_id": reference,
        "grant_guid": gid,
        "acronym": clean_text(it.get("acronym")),
        "display_name": clean_text(it.get("title")),
        "description": strip_html(it.get("publicSummary")),
        "host_organisation_raw": clean_text(it.get("hostOrganisationName")),
        "host_institution": institution,
        "host_country": country,
        "lead_name_raw": lead_raw,
        "lead_given_name": given,
        "lead_family_name": family,
        "total_award_raw": clean_text(it.get("totalAward")),
        "amount": ("%.2f" % amount) if amount is not None else None,
        "currency": "EUR" if amount is not None else None,
        "funding_type": funding_type_for(reference),
        "funder_scheme": scheme_prefix(reference),
        "start_date": start_date,
        "end_date": end_date,
        "start_year": str(year_of(start_date)) if year_of(start_date) else None,
        "end_year": str(year_of(end_date)) if year_of(end_date) else None,
        "duration_months": str(int(duration)) if isinstance(duration, (int, float)) and duration else None,
        "project_website": clean_text(it.get("projectWebsite")),
        "landing_page_url": f"{PORTAL_URL}/#/details/{gid}" if gid else None,
        "downloaded_at": downloaded_at,
    }


STRING_COLUMNS = [
    "funder_award_id", "grant_guid", "acronym", "display_name", "description",
    "host_organisation_raw", "host_institution", "host_country",
    "lead_name_raw", "lead_given_name", "lead_family_name",
    "total_award_raw", "amount", "currency", "funding_type", "funder_scheme",
    "start_date", "end_date", "start_year", "end_year", "duration_months",
    "project_website", "landing_page_url", "downloaded_at",
]


def build_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    for col in STRING_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[STRING_COLUMNS]

    # funder_award_id must be unique -> the awards table merges rows that share it.
    missing = df["funder_award_id"].isna().sum()
    if missing:
        raise RuntimeError(f"{missing} rows have a NULL funder_award_id (reference).")
    dup_mask = df["funder_award_id"].duplicated(keep=False)
    if dup_mask.any():
        examples = df.loc[dup_mask, ["funder_award_id", "grant_guid", "display_name"]].head(20)
        raise RuntimeError(f"Duplicate funder_award_id values detected:\n{examples}")

    # Force string dtype (runbook 1.2 item 5) so null-heavy cols don't infer int.
    return df.astype("string")


def validate(df: pd.DataFrame) -> None:
    n = len(df)
    log("=" * 72)
    log(f"Local validation: {n} rows")

    def pct(col: str) -> None:
        c = int(df[col].notna().sum())
        log(f"  {col:<22}{c:>6}/{n}  ({100.0 * c / n:5.1f}%)" if n else f"  {col}: 0")

    for col in ["display_name", "description", "host_institution", "host_country",
                "lead_given_name", "lead_family_name", "amount", "currency",
                "start_year", "end_year", "funder_scheme", "landing_page_url"]:
        pct(col)
    log(f"  unique funder_award_id {df['funder_award_id'].nunique()}/{n}")

    amts = pd.to_numeric(df["amount"], errors="coerce").dropna()
    if len(amts):
        log(f"  amount coverage {len(amts)}/{n} ({100.0 * len(amts) / n:.1f}%); "
            f"EUR {amts.min():,.0f}-{amts.max():,.0f}; total EUR {amts.sum():,.0f}")
    yrs = pd.to_numeric(df["start_year"], errors="coerce").dropna()
    if len(yrs):
        log(f"  start_year range {int(yrs.min())}-{int(yrs.max())}")
    ft = df["funding_type"].value_counts(dropna=False).to_dict()
    log(f"  funding_type breakdown {ft}")

    # Runbook 6.7: EDCTP publishes amounts, so >50% coverage is expected.
    if n and len(amts) < n * 0.5:
        log(f"  WARNING: amount coverage {100.0 * len(amts) / n:.1f}% is below 50% — "
            "investigate before the notebook's 6.7 amount check.")


def upload_to_s3(local_path: Path, allow_shrink: bool) -> None:
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError("boto3 is required for upload; use --skip-upload locally.") from exc

    client = boto3.client("s3")
    new_rows = len(pd.read_parquet(local_path))
    log(f"Runbook section 1.4 shrink-check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev_path = local_path.with_suffix(".prev.parquet")
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_rows = len(pd.read_parquet(prev_path))
        prev_path.unlink(missing_ok=True)
        log(f"  previous S3 rows={prev_rows:,}, new rows={new_rows:,}")
        if new_rows < prev_rows and not allow_shrink:
            raise RuntimeError(
                f"Refusing to shrink: new {new_rows:,} < previous {prev_rows:,}. "
                f"Use --allow-shrink to override."
            )
        if new_rows < prev_rows:
            log("  WARNING: new corpus is smaller, but --allow-shrink was set.")
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            log("  No existing S3 parquet found; treating this as first ingest.")
        else:
            raise

    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    client.upload_file(str(local_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="EDCTP grants portal -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit", type=int, default=None, help="Smoke-test: stop after N records")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--page-sleep", type=float, default=0.3)
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    if args.page_size < 1 or args.page_size > 2000:
        raise SystemExit("--page-size must be between 1 and 2000")

    log("=" * 72)
    log("EDCTP grants portal ingest starting")
    log(f"source={SEARCH_URL}")
    log(f"funder_id={FUNDER_ID}")
    log(f"provenance={PROVENANCE}")

    records = crawl(args)
    if not records:
        log("No records fetched; aborting.")
        sys.exit(1)

    df = build_dataframe(records)
    validate(df)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "edctp_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"Wrote {out} ({out.stat().st_size:,} bytes)")

    if args.skip_upload:
        log("--skip-upload set; not uploading to S3.")
        return
    upload_to_s3(out, args.allow_shrink)


if __name__ == "__main__":
    main()
