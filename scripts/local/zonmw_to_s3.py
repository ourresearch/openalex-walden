#!/usr/bin/env python3
"""
ZonMw Projects to S3 (GRANT PATTERN)
====================================

Pulls project records from the official ZonMw Projects Drupal JSON:API:

    https://projecten.zonmw.nl/nl/jsonapi/node/project

ZonMw's public search page advertises roughly 17,980 project records. The
previous tracker note treated the site as web-only, but the JSON:API exposes
project-level records directly, including project number, title, summaries,
project dates, project leader, related program/subsidy relationships, taxonomy
subjects, responsible organization terms, and canonical paths.

Output:
    s3://openalex-ingest/awards/zonmw/zonmw_projects.parquet

OpenAlex funder:
    F4320321007 - ZonMw
    DOI: 10.13039/501100001826

Source / mapping notes:
    - ZonMw's native `field_project_number` is preserved as `project_number`.
      The shipped `funder_award_id` is `zonmw-{project_number}-{nid}` because
      the public API has a small number of real duplicate project numbers for
      distinct project pages. Duplicate shipped slugs still raise before
      writing because duplicate `funder_award_id` values silently merge rows
      downstream.
    - `field_project_budget` / `field_project_budget_api` are preserved, but
      smoke tests show they are usually null in the public API. The notebook
      maps a numeric budget only when ZonMw exposes one and otherwise leaves
      amount/currency null with an explicit source-field waiver.
    - All parquet columns are strings by design (`df.astype("string")`) to
      avoid pandas/pyarrow type inference problems with null-heavy fields.
      Databricks casts amount/date/year fields explicitly.

Run examples:
    python scripts/local/zonmw_to_s3.py --skip-upload --max-pages 2 --reset-checkpoint
    python scripts/local/zonmw_to_s3.py --skip-upload
    python scripts/local/zonmw_to_s3.py --allow-shrink  # admin-only override after manual verification

The script checkpoints JSONL rows plus pagination state, logs every API page,
and uses the API's `links.next` relation as the corpus terminator. It does not
treat a single empty page as end-of-corpus unless the API also omits `next`.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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

BASE_URL = "https://projecten.zonmw.nl"
SEARCH_URL = f"{BASE_URL}/nl/search"
API_URL = f"{BASE_URL}/nl/jsonapi/node/project"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/zonmw/zonmw_projects.parquet"

HEADERS = {
    "User-Agent": "openalex-walden/1.0 (openalex@ourresearch.org) python-requests",
    "Accept": "application/vnd.api+json, application/json;q=0.9, text/html;q=0.8",
}

INCLUDE_RELATIONSHIPS = ",".join(
    [
        "field_related_programs",
        "field_taxonomy_main_subject",
        "field_taxonomy_organisation",
        "field_taxonomy_sub_subjects",
    ]
)

SPARSE_FIELDS = {
    "fields[node--project]": ",".join(
        [
            "drupal_internal__nid",
            "drupal_internal__vid",
            "langcode",
            "title",
            "created",
            "changed",
            "published_at",
            "path",
            "field_application_summary",
            "field_application_summary_api",
            "field_description",
            "field_description_api",
            "field_endreport_results",
            "field_endreport_results_api",
            "field_endreport_summary",
            "field_endreport_summary_api",
            "field_keywords",
            "field_progress_results",
            "field_progress_results_api",
            "field_progress_summary",
            "field_progress_summary_api",
            "field_project_budget",
            "field_project_budget_api",
            "field_project_date",
            "field_project_number",
            "field_projectleader_name",
            "field_projectleader_name_api",
            "field_related_programs",
            "field_related_subsidy",
            "field_taxonomy_main_subject",
            "field_taxonomy_organisation",
            "field_taxonomy_sub_subjects",
        ]
    ),
    "fields[node--program]": "drupal_internal__nid,title,field_title_api",
    "fields[node--subsidy]": "drupal_internal__nid,title,field_title_api",
    "fields[taxonomy_term--organisation]": "drupal_internal__tid,name",
    "fields[taxonomy_term--subject]": "drupal_internal__tid,name",
}

PAGE_LIMIT = 50  # Drupal caps larger requests at 50.
REQUEST_TIMEOUT = (10, 60)
MAX_RETRIES = 5


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        value = value.get("processed") or value.get("value")
    if isinstance(value, list):
        value = " ".join(str(v) for v in value if v is not None)
    value = str(value)
    value = html.unescape(value)
    if "<" in value:
        value = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def json_string(value: Any) -> str | None:
    if value in (None, [], {}):
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def parse_year(date_value: str | None) -> str | None:
    if not date_value:
        return None
    match = re.match(r"(\d{4})", date_value)
    if not match:
        return None
    year = int(match.group(1))
    if 1900 <= year <= 2035:
        return str(year)
    return None


def normalize_project_number(value: str | None) -> str | None:
    value = clean_text(value)
    if not value:
        return None
    if value.lower().startswith("vul in dit veld"):
        return None
    return value.strip()


def stable_slug(project_number: str, drupal_internal_nid: Any, uuid: str | None) -> str:
    source_record_id = str(drupal_internal_nid or uuid or "").strip()
    if not source_record_id:
        raise RuntimeError(f"ZonMw project {project_number!r} has no Drupal nid or uuid for a unique key.")
    return f"zonmw-{project_number.lower()}-{source_record_id.lower()}"


def canonical_url(path: dict | None) -> str | None:
    if not path:
        return None
    alias = path.get("alias")
    if not alias:
        return None
    if alias.startswith("/nl/") or alias.startswith("/en/"):
        return f"{BASE_URL}{alias}"
    return f"{BASE_URL}/nl{alias}"


def fetch(session: requests.Session, url: str, params: dict | None = None) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                try:
                    sleep_s = float(retry_after) if retry_after else 20.0 * attempt
                except ValueError:
                    sleep_s = 20.0 * attempt
                log(f"HTTP 429 from ZonMw API; sleeping {sleep_s:.0f}s before retry {attempt}/{MAX_RETRIES}")
                time.sleep(sleep_s)
                continue
            if response.status_code >= 500:
                raise requests.HTTPError(f"HTTP {response.status_code}", response=response)
            return response
        except Exception as exc:  # noqa: BLE001 - retry boundary logs all request exceptions
            last_error = exc
            if attempt < MAX_RETRIES:
                time.sleep(1.5 * attempt)
    raise RuntimeError(f"Failed to fetch {url} after {MAX_RETRIES} attempts: {last_error}")


def fetch_json(session: requests.Session, url: str, params: dict | None = None) -> dict:
    response = fetch(session, url, params=params)
    response.raise_for_status()
    return response.json()


def advertised_project_total(session: requests.Session) -> int | None:
    response = fetch(session, SEARCH_URL)
    response.raise_for_status()
    text = BeautifulSoup(response.text, "html.parser").get_text(" ", strip=True)
    matches = re.findall(r"\bProject\s+([\d,.]+)\b", text, flags=re.I)
    if not matches:
        return None
    return max(int(re.sub(r"\D+", "", match)) for match in matches)


def included_map(payload: dict) -> dict[tuple[str, str], dict]:
    return {
        (item.get("type"), item.get("id")): item
        for item in payload.get("included", [])
        if item.get("type") and item.get("id")
    }


def label_for_resource(resource: dict | None) -> str | None:
    if not resource:
        return None
    attrs = resource.get("attributes", {})
    return clean_text(
        attrs.get("field_title_api")
        or attrs.get("title")
        or attrs.get("name")
        or attrs.get("field_name")
    )


def relationship_data(item: dict, relationship_name: str) -> list[dict]:
    data = item.get("relationships", {}).get(relationship_name, {}).get("data")
    if data is None:
        return []
    if isinstance(data, list):
        return data
    return [data]


def relationship_labels(item: dict, relationship_name: str, by_key: dict[tuple[str, str], dict]) -> list[str]:
    labels: list[str] = []
    for ref in relationship_data(item, relationship_name):
        label = label_for_resource(by_key.get((ref.get("type"), ref.get("id"))))
        if label:
            labels.append(label)
    return labels


def relationship_ids(item: dict, relationship_name: str) -> list[str]:
    ids: list[str] = []
    for ref in relationship_data(item, relationship_name):
        meta = ref.get("meta", {})
        target_id = meta.get("drupal_internal__target_id") or ref.get("id")
        if target_id is not None:
            ids.append(str(target_id))
    return ids


def flatten_project(item: dict, by_key: dict[tuple[str, str], dict], downloaded_at: str) -> dict | None:
    attrs = item.get("attributes", {})
    project_number = normalize_project_number(attrs.get("field_project_number"))
    title = clean_text(attrs.get("title"))
    if not project_number or not title or title.lower() == "project":
        return None

    date_range = attrs.get("field_project_date") or {}
    start_date = clean_text(date_range.get("value"))
    end_date = clean_text(date_range.get("end_value"))

    organizations = relationship_labels(item, "field_taxonomy_organisation", by_key)
    programs = relationship_labels(item, "field_related_programs", by_key)
    subsidies = relationship_labels(item, "field_related_subsidy", by_key)
    main_subjects = relationship_labels(item, "field_taxonomy_main_subject", by_key)
    sub_subjects = relationship_labels(item, "field_taxonomy_sub_subjects", by_key)

    budget_api = attrs.get("field_project_budget_api")
    budget_text = clean_text(attrs.get("field_project_budget"))
    amount = str(budget_api) if budget_api not in (None, "") else None

    path = attrs.get("path") or {}
    row = {
        "uuid": item.get("id"),
        "drupal_internal_nid": attrs.get("drupal_internal__nid"),
        "drupal_internal_vid": attrs.get("drupal_internal__vid"),
        "langcode": attrs.get("langcode"),
        "project_number": project_number,
        "slug": stable_slug(project_number, attrs.get("drupal_internal__nid"), item.get("id")),
        "display_name": title,
        "description": clean_text(attrs.get("field_description_api") or attrs.get("field_description")),
        "application_summary": clean_text(attrs.get("field_application_summary_api") or attrs.get("field_application_summary")),
        "progress_summary": clean_text(attrs.get("field_progress_summary_api") or attrs.get("field_progress_summary")),
        "progress_results": clean_text(attrs.get("field_progress_results_api") or attrs.get("field_progress_results")),
        "endreport_summary": clean_text(attrs.get("field_endreport_summary_api") or attrs.get("field_endreport_summary")),
        "endreport_results": clean_text(attrs.get("field_endreport_results_api") or attrs.get("field_endreport_results")),
        "keywords_json": json_string(attrs.get("field_keywords")),
        "project_leader_name": clean_text(attrs.get("field_projectleader_name_api") or attrs.get("field_projectleader_name")),
        "start_date": start_date,
        "end_date": end_date,
        "start_year": parse_year(start_date),
        "end_year": parse_year(end_date),
        "project_budget_text": budget_text,
        "project_budget_api": amount,
        "currency": "EUR" if amount else None,
        "related_programs_json": json_string(programs),
        "related_program_ids_json": json_string(relationship_ids(item, "field_related_programs")),
        "related_subsidies_json": json_string(subsidies),
        "related_subsidy_ids_json": json_string(relationship_ids(item, "field_related_subsidy")),
        "main_subject": main_subjects[0] if main_subjects else None,
        "sub_subjects_json": json_string(sub_subjects),
        "organizations_json": json_string(organizations),
        "primary_organization": organizations[0] if organizations else None,
        "path_alias": path.get("alias"),
        "landing_page_url": canonical_url(path),
        "source_api_url": item.get("links", {}).get("self", {}).get("href"),
        "created": attrs.get("created"),
        "changed": attrs.get("changed"),
        "published_at": attrs.get("published_at"),
        "downloaded_at": downloaded_at,
    }
    return row


def load_checkpoint(rows_path: Path, state_path: Path) -> tuple[dict[str, dict], dict]:
    rows: dict[str, dict] = {}
    if rows_path.exists():
        for line in rows_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            rows[row["uuid"]] = row
    state = {}
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
    return rows, state


def append_row(path: Path, row: dict) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def save_state(path: Path, state: dict) -> None:
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def build_dataframe(rows: dict[str, dict]) -> pd.DataFrame:
    df = pd.DataFrame(list(rows.values()))
    if df.empty:
        raise RuntimeError("No valid ZonMw project records were fetched.")

    df["slug"] = [
        stable_slug(project_number, drupal_internal_nid, uuid)
        for project_number, drupal_internal_nid, uuid in zip(
            df["project_number"],
            df["drupal_internal_nid"],
            df["uuid"],
        )
    ]
    df["start_year"] = [parse_year(value) for value in df["start_date"]]
    df["end_year"] = [parse_year(value) for value in df["end_date"]]

    dup_mask = df["slug"].duplicated(keep=False) & df["slug"].notna()
    if dup_mask.any():
        log("FATAL: duplicate ZonMw funder_award_id slugs detected:")
        log(str(df.loc[dup_mask, ["uuid", "drupal_internal_nid", "project_number", "display_name", "slug"]]))
        raise RuntimeError(
            f"{int(dup_mask.sum())} rows have duplicate ZonMw project-number+nid slugs; "
            "fix the key rule before shipping."
        )

    return df.sort_values(["project_number", "drupal_internal_nid", "uuid"]).astype("string")


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> None:
    """Runbook §1.4: refuse to overwrite S3 with a smaller corpus by default."""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("s3")
    log(f"§1.4 re-ingest safety check vs s3://{S3_BUCKET}/{S3_KEY}")

    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            log("No existing S3 parquet found; treating this as first ingest.")
            return
        log(f"WARNING: S3 head_object failed with {code}; treating as first ingest.")
        return

    prev_path = output_dir / "_prev_zonmw_projects.parquet"
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        previous_count = len(pd.read_parquet(prev_path))
    except Exception as exc:
        raise RuntimeError(
            f"Could not read existing S3 parquet for shrink check: {exc}. "
            "Refusing upload to avoid clobbering unknown data."
        ) from exc
    finally:
        prev_path.unlink(missing_ok=True)

    log(f"Existing S3 rows: {previous_count:,}; new rows: {new_count:,}")
    if new_count < previous_count and not allow_shrink:
        raise RuntimeError(
            f"Runbook §1.4 violation: refusing to shrink ZonMw corpus from "
            f"{previous_count:,} to {new_count:,} rows. Re-run with --allow-shrink "
            "only after confirming the source genuinely removed records."
        )
    if new_count < previous_count:
        log("WARNING: --allow-shrink set; proceeding despite smaller corpus.")


def crawl(args: argparse.Namespace) -> pd.DataFrame:
    session = requests.Session()
    session.headers.update(HEADERS)

    expected_total = advertised_project_total(session)
    log(f"Search page advertised Project total: {expected_total:,}" if expected_total else "Search page total not found")

    args.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    rows_path = args.checkpoint_dir / "zonmw_projects.jsonl"
    state_path = args.checkpoint_dir / "zonmw_state.json"

    if args.reset_checkpoint:
        rows_path.unlink(missing_ok=True)
        state_path.unlink(missing_ok=True)
        log("Reset checkpoint files.")

    rows, state = load_checkpoint(rows_path, state_path)
    log(f"Loaded checkpoint: rows={len(rows):,}, done={bool(state.get('done'))}")

    downloaded_at = datetime.now(timezone.utc).isoformat()
    if not state.get("done"):
        next_url = state.get("next_url") or API_URL
        next_params = None if state.get("next_url") else {
            "page[limit]": str(PAGE_LIMIT),
            "include": INCLUDE_RELATIONSHIPS,
            **SPARSE_FIELDS,
        }
        page_count = int(state.get("pages_fetched") or 0)

        while next_url:
            page_count += 1
            started = time.monotonic()
            payload = fetch_json(session, next_url, params=next_params)
            next_params = None

            data = payload.get("data") or []
            by_key = included_map(payload)
            added = 0
            for item in data:
                row = flatten_project(item, by_key, downloaded_at)
                if row and row["uuid"] not in rows:
                    rows[row["uuid"]] = row
                    append_row(rows_path, row)
                    added += 1

            next_link = payload.get("links", {}).get("next", {}).get("href")
            elapsed = time.monotonic() - started
            log(
                f"Page {page_count:,}: raw_items={len(data):,}, added={added:,}, "
                f"total_rows={len(rows):,}, next={bool(next_link)}, elapsed={elapsed:.1f}s"
            )

            save_state(
                state_path,
                {
                    "done": False,
                    "next_url": next_link,
                    "pages_fetched": page_count,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )

            if args.max_pages and page_count >= args.max_pages:
                log(f"Stopping after --max-pages={args.max_pages:,}")
                break

            if not data and next_link:
                log("WARNING: API returned an empty page but also supplied links.next; continuing.")

            if next_link and args.page_sleep:
                time.sleep(args.page_sleep)

            next_url = next_link

        if not args.max_pages and not next_url:
            save_state(
                state_path,
                {
                    "done": True,
                    "next_url": None,
                    "pages_fetched": page_count,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )

    df = build_dataframe(rows)

    if args.max_pages is None and expected_total and len(df) < expected_total * 0.95:
        raise RuntimeError(
            f"Crawl produced {len(df):,} rows, below 95% of the advertised project total "
            f"({expected_total:,}). Do not upload partial data; resume checkpoint or inspect pagination."
        )

    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="ZonMw Projects JSON:API -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--checkpoint-dir", type=Path, default=Path("/tmp/zonmw_checkpoint"))
    parser.add_argument("--skip-upload", action="store_true", help="Write parquet locally only; skip S3 upload")
    parser.add_argument("--max-pages", type=int, default=None, help="Smoke-test mode: stop after N API pages")
    parser.add_argument("--page-sleep", type=float, default=1.5, help="Seconds to sleep between JSON:API pages")
    parser.add_argument("--reset-checkpoint", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    log("=" * 72)
    log("ZonMw Projects -> S3 starting")
    log(f"API source: {API_URL}")
    log(f"Output: s3://{S3_BUCKET}/{S3_KEY}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    df = crawl(args)

    output_path = args.output_dir / "zonmw_projects.parquet"
    shrink_guard(output_path, len(df), args.allow_shrink)
    df.to_parquet(output_path, index=False)

    amount_nonnull = df["project_budget_api"].notna().sum() if "project_budget_api" in df else 0
    leader_nonnull = df["project_leader_name"].notna().sum() if "project_leader_name" in df else 0
    org_nonnull = df["primary_organization"].notna().sum() if "primary_organization" in df else 0
    log(f"Wrote {output_path} ({output_path.stat().st_size:,} bytes)")
    log(
        "Coverage: "
        f"rows={len(df):,}, amount={amount_nonnull:,}, leaders={leader_nonnull:,}, "
        f"primary_org={org_nonnull:,}, min_year={df['start_year'].min()}, max_year={df['start_year'].max()}"
    )

    if args.skip_upload:
        log("--skip-upload set; done.")
        return

    check_no_shrink(len(df), args.allow_shrink, args.output_dir)
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3

    boto3.client("s3").upload_file(str(output_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted.")
        sys.exit(130)
