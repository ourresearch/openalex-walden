#!/usr/bin/env python3
"""
Research Council of Norway Project Bank -> S3 Pipeline
======================================================

Downloads project records from the Research Council of Norway's official
Project Bank at prosjektbanken.forskningsradet.no.

Source authority
----------------
The Project Bank is the Research Council's own public project database. Its
rendered Next.js pages expose an official JSON data route:

    /_next/data/{build_id}/en/explore/projects.json

This script requests only `Kilde=FORISS`, the Research Council source in the
Project Bank. It intentionally excludes the Project Bank's EU and SkatteFUNN
views, which are not direct Research Council awards.

OpenAlex funder
---------------
F4320323299 - Norges Forskningsrad / Research Council of Norway

Output
------
s3://openalex-ingest/awards/research_council_norway/research_council_norway_projects.parquet

Usage
-----
    python scripts/local/research_council_norway_to_s3.py --limit 10 --skip-upload
    python scripts/local/research_council_norway_to_s3.py --skip-upload
    python scripts/local/research_council_norway_to_s3.py --skip-download --skip-upload
    python scripts/local/research_council_norway_to_s3.py --allow-shrink
"""

from __future__ import annotations

import argparse
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


BASE_URL = "https://prosjektbanken.forskningsradet.no"
EXPLORE_URL = f"{BASE_URL}/en/explore/projects"
FUNDER_ID = 4320323299
PROVENANCE = "research_council_norway_project_bank"

S3_BUCKET = "openalex-ingest"
S3_KEY = (
    "awards/research_council_norway/"
    "research_council_norway_projects.parquet"
)

HEADERS = {
    "User-Agent": "openalex-walden-research-council-norway/1.0 (+https://openalex.org)",
    "Accept": "application/json, text/html;q=0.8",
}

DEFAULT_CHECKPOINT_DIR = Path("/tmp/research_council_norway_checkpoint")
DEFAULT_OUTPUT_DIR = Path("/tmp")
DEFAULT_PAGE_SIZE = 500
REQUEST_TIMEOUT = (10, 90)
MAX_RETRIES = 5

FILTER_PARAMS = {
    "view": "projects",
    "Kilde": "FORISS",
    "distribution": "Ar",
    "chart": "bar",
    "calcType": "funding",
    "Sprak": "en",
    "sortBy": "date",
    "sortOrder": "desc",
}

NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    flags=re.DOTALL,
)

SUFFIX_TOKENS = {
    "phd", "ph.d.", "md", "m.d.", "dphil", "dr.", "dr", "prof.", "prof",
    "jr.", "sr.", "jr", "sr", "ii", "iii", "iv",
}


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value)
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def json_string(value: Any) -> str | None:
    if value in (None, [], {}):
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def split_name(name: str | None) -> tuple[str | None, str | None]:
    if not name:
        return None, None
    tokens = re.split(r"\s+", name.strip())
    while tokens and tokens[0].lower().strip(",") in SUFFIX_TOKENS:
        tokens.pop(0)
    while tokens and tokens[-1].lower().strip(",") in SUFFIX_TOKENS:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def year_bounds(years: Any) -> tuple[str | None, str | None, str | None, str | None]:
    if not isinstance(years, list):
        return None, None, None, None
    parsed = []
    for value in years:
        try:
            year = int(value)
        except (TypeError, ValueError):
            continue
        if 1800 <= year <= 2100:
            parsed.append(year)
    if not parsed:
        return None, None, None, None
    start_year = min(parsed)
    end_year = max(parsed)
    return (
        str(start_year),
        str(end_year),
        f"{start_year:04d}-01-01",
        f"{end_year:04d}-12-31",
    )


def primary_organization(organizations: Any) -> str | None:
    if not isinstance(organizations, list) or not organizations:
        return None
    hierarchy = organizations[0]
    if not isinstance(hierarchy, list) or not hierarchy:
        return None
    # Project Bank hierarchy usually stores [sector, org_type, institution,
    # faculty, department]. The institution slot is the best affiliation name.
    if len(hierarchy) >= 3 and clean_text(hierarchy[2]):
        return clean_text(hierarchy[2])
    return clean_text(hierarchy[-1])


def project_url(source: str | None, project_id: str) -> str:
    source_part = clean_text(source) or "FORISS"
    return f"{BASE_URL}/en/project/{source_part}/{project_id}"


def fetch(session: requests.Session, url: str, params: dict[str, Any] | None = None) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if response.status_code == 429:
                sleep_s = float(response.headers.get("Retry-After") or 20 * attempt)
                log(f"HTTP 429; sleeping {sleep_s:.0f}s before retry {attempt}/{MAX_RETRIES}")
                time.sleep(sleep_s)
                continue
            if response.status_code >= 500:
                raise requests.HTTPError(f"HTTP {response.status_code}", response=response)
            return response
        except Exception as exc:  # noqa: BLE001 - request retry boundary
            last_error = exc
            if attempt < MAX_RETRIES:
                sleep_s = 2.0 * attempt
                log(f"Fetch failed ({type(exc).__name__}); retrying in {sleep_s:.1f}s")
                time.sleep(sleep_s)
    raise RuntimeError(f"Failed to fetch {url} after {MAX_RETRIES} attempts: {last_error}")


def discover_build_id(session: requests.Session) -> str:
    response = fetch(session, EXPLORE_URL)
    response.raise_for_status()
    match = NEXT_DATA_RE.search(response.text)
    if not match:
        raise RuntimeError("Could not find __NEXT_DATA__ in Project Bank explore page.")
    payload = json.loads(match.group(1))
    build_id = payload.get("buildId")
    if not build_id:
        raise RuntimeError("Project Bank __NEXT_DATA__ did not include buildId.")
    log(f"Discovered Next.js build id: {build_id}")
    return build_id


def list_url(build_id: str) -> str:
    return f"{BASE_URL}/_next/data/{build_id}/en/explore/projects.json"


def load_rows(rows_path: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    if not rows_path.exists():
        return rows
    for line in rows_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        rows[str(row["source_record_id"])] = row
    return rows


def append_row(path: Path, row: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def flatten_project(project: dict[str, Any], downloaded_at: str) -> dict[str, Any]:
    project_id = str(project.get("id") or "").strip()
    if not project_id:
        raise RuntimeError(f"Project row missing id: {project}")
    source = clean_text(project.get("source")) or "FORISS"
    source_record_id = f"{source}:{project_id}"
    funder_award_id = f"rcn-{source.lower()}-{project_id.lower()}"

    title = clean_text(project.get("title"))
    if not title:
        raise RuntimeError(f"Project {source_record_id} missing title.")

    years = project.get("yearsActive")
    start_year, end_year, start_date, end_date = year_bounds(years)
    duration = project.get("duration") if isinstance(project.get("duration"), dict) else {}
    if duration:
        start_year = start_year or clean_text(duration.get("startYear"))
        end_year = end_year or clean_text(duration.get("endYear"))
        if start_year and not start_date:
            start_date = f"{int(start_year):04d}-01-01"
        if end_year and not end_date:
            end_date = f"{int(end_year):04d}-12-31"

    amount = project.get("totalFunding")
    amount_text = clean_text(amount) if amount not in (None, "") else None
    lead_name = clean_text(project.get("leadName"))
    given_name, family_name = split_name(lead_name)
    organization = primary_organization(project.get("organisations"))
    current_activity = project.get("currentActivity") if isinstance(project.get("currentActivity"), dict) else {}

    return {
        "source_record_id": source_record_id,
        "project_id": project_id,
        "source": source,
        "funder_award_id": funder_award_id,
        "display_name": title,
        "teaser": clean_text(project.get("teaser")),
        "pop_sci_description": clean_text(project.get("popSciDescription")),
        "project_summary": clean_text(project.get("projectSummary")),
        "lead_name": lead_name,
        "lead_given_name": given_name,
        "lead_family_name": family_name,
        "primary_organization": organization,
        "organizations_json": json_string(project.get("organisations")),
        "geographies_json": json_string(project.get("geographies")),
        "disciplines_json": json_string(project.get("disciplines")),
        "current_activity_code": clean_text(current_activity.get("activity")),
        "current_activity_year": clean_text(current_activity.get("year")),
        "current_activity_json": json_string(current_activity),
        "years_active_json": json_string(years),
        "start_year": start_year,
        "end_year": end_year,
        "start_date": start_date,
        "end_date": end_date,
        "total_funding": amount_text,
        "currency": "NOK" if amount_text else None,
        "landing_page_url": project_url(source, project_id),
        "source_list_url": EXPLORE_URL,
        "downloaded_at": downloaded_at,
    }


def crawl(args: argparse.Namespace) -> pd.DataFrame:
    rows_path = args.checkpoint_dir / "research_council_norway_projects.jsonl"
    state_path = args.checkpoint_dir / "research_council_norway_state.json"
    args.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    if args.reset_checkpoint:
        rows_path.unlink(missing_ok=True)
        state_path.unlink(missing_ok=True)
        log("Reset checkpoint files.")

    rows = load_rows(rows_path)
    state = load_state(state_path)
    log(f"Loaded checkpoint: rows={len(rows):,}, done={bool(state.get('done'))}")

    if args.skip_download:
        if not rows:
            raise RuntimeError("--skip-download was set, but no checkpoint rows exist.")
        log("--skip-download set; building parquet from checkpoint only.")
        return build_dataframe(rows, args.limit)

    session = requests.Session()
    session.headers.update(HEADERS)
    build_id = discover_build_id(session)
    api_url = list_url(build_id)

    offset = int(state.get("next_offset") or 0)
    total_hits = state.get("total_hits")
    downloaded_at = datetime.now(timezone.utc).isoformat()
    page_size = args.page_size

    while True:
        if args.limit is not None and len(rows) >= args.limit:
            log(f"Reached --limit={args.limit:,}; stopping download.")
            break
        params = {
            **FILTER_PARAMS,
            "resultCount": str(page_size),
            "offset": str(offset),
        }
        started = time.monotonic()
        response = fetch(session, api_url, params=params)
        response.raise_for_status()
        payload = response.json()
        project_list = payload.get("pageProps", {}).get("initialState", {}).get("PROJECTLIST", {})
        projects = project_list.get("projects") or []
        pagination = project_list.get("pagination") or {}
        total_hits = int(pagination.get("totalHits") or total_hits or 0)

        added = 0
        for project in projects:
            row = flatten_project(project, downloaded_at)
            key = row["source_record_id"]
            if key not in rows:
                rows[key] = row
                append_row(rows_path, row)
                added += 1
            if args.limit is not None and len(rows) >= args.limit:
                break

        elapsed = time.monotonic() - started
        log(
            f"offset={offset:,}: raw_items={len(projects):,}, added={added:,}, "
            f"rows={len(rows):,}/{total_hits:,}, elapsed={elapsed:.1f}s"
        )

        next_offset = offset + len(projects)
        save_state(
            state_path,
            {
                "done": False,
                "next_offset": next_offset,
                "total_hits": total_hits,
                "build_id": build_id,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )

        if not projects:
            raise RuntimeError(
                f"Project Bank returned an empty page at offset {offset:,}; "
                "not treating this as end-of-corpus."
            )
        if args.limit is not None and len(rows) >= args.limit:
            break
        if total_hits and next_offset >= total_hits:
            save_state(
                state_path,
                {
                    "done": True,
                    "next_offset": next_offset,
                    "total_hits": total_hits,
                    "build_id": build_id,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            break

        offset = next_offset
        if args.page_sleep:
            time.sleep(args.page_sleep)

    return build_dataframe(rows, args.limit)


def build_dataframe(rows: dict[str, dict[str, Any]], limit: int | None = None) -> pd.DataFrame:
    values = list(rows.values())
    if limit is not None:
        values = values[:limit]
    df = pd.DataFrame(values)
    if df.empty:
        raise RuntimeError("No Research Council of Norway project rows were fetched.")

    dup_mask = df["funder_award_id"].duplicated(keep=False)
    if dup_mask.any():
        examples = df.loc[dup_mask, ["source_record_id", "funder_award_id", "display_name"]].head(20)
        raise RuntimeError(f"Duplicate funder_award_id values detected:\n{examples}")

    return df.sort_values(["source_record_id"]).astype("string")


def validate_dataframe(df: pd.DataFrame, expected_total: int | None, is_limited: bool) -> None:
    total = len(df)
    log("=" * 72)
    log(f"Local validation: {total:,} rows")
    for col in [
        "display_name",
        "project_summary",
        "pop_sci_description",
        "lead_name",
        "primary_organization",
        "total_funding",
        "currency",
        "start_year",
        "landing_page_url",
    ]:
        count = int(df[col].notna().sum()) if col in df else 0
        pct = (count * 100.0 / total) if total else 0.0
        log(f"  {col:<24} {count:>7,}/{total:<7,} ({pct:5.1f}%)")

    unique_ids = int(df["funder_award_id"].nunique())
    log(f"  unique funder_award_id {unique_ids:,}/{total:,}")
    if unique_ids != total:
        raise RuntimeError("funder_award_id is not unique.")

    years = pd.to_numeric(df["start_year"], errors="coerce")
    amounts = pd.to_numeric(df["total_funding"], errors="coerce")
    log(
        "  year range "
        f"{int(years.min()) if years.notna().any() else 'NULL'}-"
        f"{int(years.max()) if years.notna().any() else 'NULL'}"
    )
    if amounts.notna().any():
        log(
            "  amount range NOK "
            f"{amounts.min():,.0f}-{amounts.max():,.0f}; "
            f"total NOK {amounts.sum():,.0f}"
        )

    if expected_total and not is_limited and total < expected_total * 0.98:
        raise RuntimeError(
            f"Full run produced {total:,} rows, below 98% of advertised total "
            f"{expected_total:,}. Do not upload a partial corpus."
        )


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> None:
    """Runbook section 1.4: refuse to overwrite S3 with a smaller corpus."""
    if allow_shrink:
        log("--allow-shrink set; skipping shrink-check failure behavior.")
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError("boto3 is required for upload; use --skip-upload locally.") from exc

    client = boto3.client("s3")
    log(f"Runbook section 1.4 shrink-check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            log("No existing S3 parquet found; treating this as first ingest.")
            return
        raise

    previous_path = output_dir / "_previous_research_council_norway_projects.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(previous_path))
        previous_count = len(pd.read_parquet(previous_path))
    finally:
        previous_path.unlink(missing_ok=True)

    log(f"Existing S3 rows: {previous_count:,}; new rows: {new_count:,}")
    if new_count < previous_count and not allow_shrink:
        raise RuntimeError(
            f"Runbook section 1.4 violation: refusing to shrink Research Council "
            f"of Norway corpus from {previous_count:,} to {new_count:,} rows."
        )
    if new_count < previous_count:
        log("WARNING: new corpus is smaller, but --allow-shrink was set.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Research Council of Norway Project Bank -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--checkpoint-dir", type=Path, default=DEFAULT_CHECKPOINT_DIR)
    parser.add_argument("--limit", type=int, default=None, help="Smoke-test mode: stop after N records")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--page-sleep", type=float, default=0.0)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--reset-checkpoint", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    if args.page_size < 1 or args.page_size > 2000:
        raise SystemExit("--page-size must be between 1 and 2000")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "research_council_norway_projects.parquet"

    log("=" * 72)
    log("Research Council of Norway Project Bank ingest starting")
    log(f"source={EXPLORE_URL}")
    log(f"funder_id={FUNDER_ID}")
    log(f"provenance={PROVENANCE}")
    log(f"output={output_path}")

    df = crawl(args)
    expected_total = None
    state_path = args.checkpoint_dir / "research_council_norway_state.json"
    if state_path.exists():
        expected_total = load_state(state_path).get("total_hits")
        expected_total = int(expected_total) if expected_total else None

    validate_dataframe(df, expected_total, is_limited=args.limit is not None)
    df.to_parquet(output_path, index=False, engine="pyarrow")
    log(f"Wrote {output_path} ({output_path.stat().st_size:,} bytes)")

    if args.skip_upload:
        log("--skip-upload set; not uploading to S3.")
        return

    check_no_shrink(len(df), args.allow_shrink, args.output_dir)
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3

    boto3.client("s3").upload_file(str(output_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    main()
