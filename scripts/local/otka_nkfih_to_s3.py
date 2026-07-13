#!/usr/bin/env python3
"""
OTKA / NKFIH (Hungary) to S3 Data Pipeline
==========================================

Harvests the NKFI-EPR public basic-research project database
(http://nyilvanos.otka-palyazat.hu/) and uploads a parquet to S3 for
Databricks ingestion. ONE harvest, ONE provenance (`otka_nkfih`), covering
THREE OpenAlex funder entities split era-wise in the notebook (see
CreateOTKAAwards.ipynb §2.3.2 rule):

  - F4320321994  Hungarian Scientific Research Fund (OTKA)  — pre-2015 era
  - F4320326762  Nemzeti Kutatasi Fejlesztesi es Innovacios Hivatal (NKFIH)
                 — 2015+ era (NKFIH took over OTKA administration 2015-01-01)
  - F4320336675  National Research, Development and Innovation Office —
                 OpenAlex DUPLICATE of F4320326762 (same Wikidata Q30290711);
                 receives NO rows; reported as duplicate instead.

Data source / method
--------------------
The public search UI returns no result lists to non-JS clients, but project
detail pages are directly enumerable by integer id ("azonosito"):

    https://nyilvanos.otka-palyazat.hu/index.php?menuid=930&num={id}&lang=EN

The `lang=EN` variant is harvested because it carries BOTH Hungarian and
English titles/keywords/summaries AND presents person names comma-separated
as "Family, Given" (the Hungarian page uses bare family-first order, which
is ambiguous to split). Non-existent ids return a small (~7 KB) shell page
with no `dsLabel` data rows — an empty id is NOT end-of-corpus (ids are
sparse: only funded, public projects resolve).

⚠️ LOAD-SHED FLAKE (confirmed 2026-07-12): under sustained concurrent load
the server intermittently serves the SAME empty shell (HTTP 200) for REAL
project ids — during a sustained 12-worker sweep the per-fetch detection
probability dropped to ~50%, silently halving the corpus; sequential
fetches were ~100% reliable. Defenses in this script: (a) an id whose page
first parses as empty is CONFIRMED with EMPTY_CONFIRMATIONS extra spaced
fetches before being recorded as not-found; (b) default workers kept at 6;
(c) the §1.4 shrink-check blocks a shrunken refresh from overwriting S3.
NOTE: empty verdicts are checkpointed, so a resume does NOT re-probe them;
if a run still comes up short, delete the checkpoint's found=false lines
(or the whole checkpoint) and re-run. The 2026-07-12 initial ingest used a
separate canary-gated re-probe pass to recover shed ids.

Id space (probed 2026-07-12): real ids live in ~37200..157630 at ~9%
density (~11.5k projects; the site footer says "osszes projektek szama:
11533"). Default sweep range 30000..162000 adds safety margin on both
ends (probes at step 100 below 30000 found nothing). The DB contains
projects that ENDED in 2005 or later plus ongoing ones (per the homepage)
— older OTKA grants are only in print archives and are out of scope.

Amounts — UNIT WARNING
----------------------
The page reports "Funding (in million HUF)" (Hungarian label: "aktualis
osszeg (MFt)", MFt = millio forint). The raw string (e.g. "8.626", '.' is
the DECIMAL separator, resolution = thousand HUF) is preserved in
`funding_mhuf`; the script also ships `amount_huf` = value * 1,000,000
(whole forints) which is what the notebook maps to `amount` with
currency 'HUF'.

Name parsing
------------
The EN pages give "Family, Given" (e.g. "Simon, Peter" -> family=Simon,
given=Peter). We split on the comma — the runbook §2.4.1 `split_name`
helper is for Western "First Last" order and would be WRONG here. Rows
without a comma fall back to Hungarian convention (first token = family).

Output: s3://openalex-ingest/awards/otka_nkfih/otka_nkfih_projects.parquet

Usage
-----
    py -3 scripts/local/otka_nkfih_to_s3.py --limit 30 --skip-upload   # smoke
    py -3 -u scripts/local/otka_nkfih_to_s3.py                         # full (resumable)
    py -3 scripts/local/otka_nkfih_to_s3.py --allow-shrink

The harvest checkpoints every id (found or empty) to
{output-dir}/otka_nkfih_checkpoint.jsonl and resumes automatically.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import html as html_mod
import json
import os
import re
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Critical here: Hungarian titles/names are full of accented characters.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if sys.platform == "win32":
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

BASE = "https://nyilvanos.otka-palyazat.hu"
DETAIL_URL = BASE + "/index.php?menuid=930&num={num}&lang=EN"
LANDING_URL = BASE + "/index.php?menuid=930&num={num}"
PROVENANCE = "otka_nkfih"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/otka_nkfih/otka_nkfih_projects.parquet"
PARQUET_NAME = "otka_nkfih_projects.parquet"
CHECKPOINT_NAME = "otka_nkfih_checkpoint.jsonl"

DEFAULT_START = 30000
DEFAULT_END = 162000          # inclusive; probed real ids 37200..157630 + margin

HEADERS = {
    "User-Agent": "openalex-walden-otka/1.0 (OpenAlex awards ingest; mailto:kyle@ourresearch.org)",
    "Accept": "text/html",
}
REQUEST_TIMEOUT = 30
MAX_RETRIES = 4
MAX_CONSECUTIVE_FAILURES = 30   # ids failing ALL retries in a row => site down => raise
EMPTY_CONFIRMATIONS = 2         # extra spaced fetches before trusting an empty shell
EMPTY_CONFIRM_DELAY = 3.0       # seconds between confirmation fetches

WALDEN_ENV = Path(__file__).resolve().parents[2] / ".env"

# EN-page field labels -> our column names
LABEL_MAP = {
    "Identifier": "identifier",
    "Type": "type_code",
    "Principal investigator": "pi_name_raw",
    "Title in Hungarian": "title_hu",
    "Title in English": "title_en",
    "Keywords in Hungarian": "keywords_hu",
    "Keywords in English": "keywords_en",
    "Discipline": "discipline",
    "Panel": "panel",
    "Department or equivalent": "department",
    "Participants": "participants_raw",
    "Starting date": "start_date",
    "Closing date": "end_date",
    "Funding (in million HUF)": "funding_mhuf",
    "FTE (full time equivalent)": "fte",
    "state": "status",
    "Full text": "final_report_url",
}
SECTION_MAP = {
    "Summary in Hungarian": "summary_hu",
    "Summary": "summary_en",
    "Results in Hungarian": "results_hu",
    "Results in English": "results_en",
}

_print_lock = threading.Lock()


def log(message: str) -> None:
    with _print_lock:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def load_walden_env() -> None:
    """AWS creds live in openalex-walden/.env (never ~/.aws/)."""
    if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        return
    if not WALDEN_ENV.exists():
        return
    for line in WALDEN_ENV.read_text().splitlines():
        m = re.match(r"\s*(?:export\s+)?(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_DEFAULT_REGION)\s*=\s*['\"]?([^'\"#\s]+)", line)
        if m and not os.getenv(m.group(1)):
            os.environ[m.group(1)] = m.group(2)


def strip_tags(fragment: str) -> str | None:
    txt = re.sub(r"<br\s*/?>", "\n", fragment, flags=re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = html_mod.unescape(txt)
    txt = re.sub(r"[ \t\xa0]+", " ", txt)
    txt = re.sub(r"\n\s*\n+", "\n", txt)
    txt = txt.strip()
    return txt or None


def strip_bold_prompts(fragment: str) -> str:
    """Summary sections embed the application-form prompts inside (nested)
    <b>...</b> blocks; remove them iteratively (inner-first) so only the
    applicant's own text remains."""
    prev = None
    while prev != fragment:
        prev = fragment
        fragment = re.sub(r"<b>(?:(?!</?b>).)*</b>", " ", fragment, flags=re.S | re.I)
    return fragment


def split_family_given(raw: str | None) -> tuple[str | None, str | None]:
    """EN pages give 'Family, Given'. No comma => Hungarian order fallback
    (first token = family). Runbook §2.4.1 split_name is for Western
    'First Last' order and does not apply to this source."""
    if not raw:
        return None, None
    raw = raw.strip()
    family, sep, given = raw.partition(",")
    if sep:
        return (family.strip() or None), (given.strip() or None)
    parts = raw.split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return raw, None


def parse_amount_huf(funding_mhuf: str | None) -> str | None:
    """'8.626' (million HUF, '.' = decimal separator) -> '8626000' (HUF)."""
    if not funding_mhuf:
        return None
    s = funding_mhuf.replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        v = float(s)
    except ValueError:
        return None
    if v <= 0:
        return None
    return str(int(round(v * 1_000_000)))


def parse_detail(num: int, page: str) -> dict | None:
    """Parse one EN project page. Returns None if the page is the
    'no such project' shell (sparse id — normal, not an error)."""
    if 'class="dsLabel"' not in page:
        return None

    rec: dict = {"num": str(num)}
    extras: dict = {}

    # Label cells are plain text ([^<]*): this must NOT span tags, otherwise
    # the match can start at a dsJoin section's <div class="dsLabel"> and
    # swallow entire summary sections plus the following "Full text" row.
    for label, value in re.findall(
        r'class="dsLabel">([^<]*)</td>\s*<td[^>]*class="dsData">(.*?)</td>',
        page, re.S,
    ):
        label_txt = strip_tags(label) or ""
        col = LABEL_MAP.get(label_txt)
        if col == "participants_raw":
            names = [strip_tags(p) for p in re.split(r"<br\s*/?>", value, flags=re.I)]
            rec[col] = json.dumps([n for n in names if n], ensure_ascii=False)
        elif col == "final_report_url":
            m = re.search(r'href="([^"]+)"', value)
            rec[col] = m.group(1) if m else strip_tags(value)
        elif col:
            rec[col] = strip_tags(value)
        elif label_txt:
            extras[label_txt] = strip_tags(value)

    if "identifier" not in rec:
        return None

    for section, content in re.findall(
        r'class="dsJoin"><div class="dsLabel">(.*?)</div>(.*?)</td>', page, re.S
    ):
        sec_txt = strip_tags(section) or ""
        col = SECTION_MAP.get(sec_txt)
        if col:
            rec[col] = strip_tags(strip_bold_prompts(content))
        elif sec_txt:
            extras[sec_txt] = strip_tags(strip_bold_prompts(content))

    family, given = split_family_given(rec.get("pi_name_raw"))
    rec["pi_family_name"] = family
    rec["pi_given_name"] = given

    dept = rec.get("department")
    if dept:
        m = re.search(r"\(([^()]+)\)\s*$", dept)
        rec["institution"] = m.group(1).strip() if m else dept

    rec["amount_huf"] = parse_amount_huf(rec.get("funding_mhuf"))
    for dcol in ("start_date", "end_date"):
        v = rec.get(dcol)
        if v and not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            rec[dcol] = None
    rec["landing_page_url"] = LANDING_URL.format(num=num)
    if extras:
        rec["extras_json"] = json.dumps(extras, ensure_ascii=False)
    return rec


def fetch_page(session: requests.Session, num: int) -> str:
    """One id -> page HTML, with network/HTTP retries. Raises on exhaustion."""
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(DETAIL_URL.format(num=num), headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            r.encoding = "utf-8"
            return r.text
        except Exception as exc:                      # noqa: BLE001
            last_err = exc
            time.sleep(min(2 ** attempt, 10))
    raise RuntimeError(f"id {num}: all {MAX_RETRIES} attempts failed: {last_err}")


def fetch_one(session: requests.Session, num: int) -> dict:
    """Fetch + parse one id. An empty shell is re-confirmed with
    EMPTY_CONFIRMATIONS extra spaced fetches (load-shed flake defense — see
    module docstring). Returns {"num": N, "found": bool, "data": {...}|None}."""
    data = parse_detail(num, fetch_page(session, num))
    confirmations = 0
    while data is None and confirmations < EMPTY_CONFIRMATIONS:
        time.sleep(EMPTY_CONFIRM_DELAY)
        data = parse_detail(num, fetch_page(session, num))
        confirmations += 1
    return {"num": num, "found": data is not None, "data": data}


def load_checkpoint(path: Path) -> tuple[set[int], list[dict]]:
    done: set[int] = set()
    found_rows: list[dict] = []
    if not path.exists():
        return done, found_rows
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue          # torn tail line from an interrupted run
            done.add(int(row["num"]))
            if row.get("found") and row.get("data"):
                found_rows.append(row["data"])
    return done, found_rows


def crawl(args: argparse.Namespace, ckpt_path: Path) -> list[dict]:
    done, found_rows = load_checkpoint(ckpt_path)
    todo = [n for n in range(args.start, args.end + 1) if n not in done]
    if args.limit:
        todo = todo[: args.limit]
    log(f"id range {args.start}..{args.end}: {len(done):,} already checkpointed "
        f"({len(found_rows):,} projects), {len(todo):,} ids to probe")
    if not todo:
        return found_rows

    consecutive_failures = 0
    processed = 0
    t0 = time.time()
    sessions = threading.local()

    def fetch_with_local_session(n: int) -> dict:
        # one requests.Session per worker thread (Session isn't thread-safe)
        if not hasattr(sessions, "s"):
            sessions.s = requests.Session()
        return fetch_one(sessions.s, n)

    ckpt_fh = open(ckpt_path, "a", encoding="utf-8")
    try:
        with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(fetch_with_local_session, n): n for n in todo}
            for fut in cf.as_completed(futures):
                num = futures[fut]
                try:
                    row = fut.result()
                    consecutive_failures = 0
                except Exception as exc:              # noqa: BLE001
                    consecutive_failures += 1
                    log(f"  FAIL id {num} ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}): {exc}")
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        # don't silently truncate — abort loudly; run is resumable
                        for f in futures:
                            f.cancel()
                        raise RuntimeError(
                            f"{MAX_CONSECUTIVE_FAILURES} consecutive ids failed all "
                            f"retries — source down or blocking us. Re-run to resume."
                        ) from exc
                    continue
                ckpt_fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                if row["found"]:
                    found_rows.append(row["data"])
                processed += 1
                if processed % 1000 == 0:
                    ckpt_fh.flush()
                    rate = processed / max(time.time() - t0, 1e-9)
                    eta_min = (len(todo) - processed) / max(rate, 1e-9) / 60
                    log(f"  [{processed:,}/{len(todo):,}] {rate:.1f} ids/s — "
                        f"{len(found_rows):,} projects — ETA {eta_min:.0f} min")
    finally:
        ckpt_fh.close()
    log(f"crawl done: {processed:,} ids probed this run, {len(found_rows):,} projects total")
    return found_rows


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    cols = ["num", "identifier", "type_code", "pi_name_raw", "pi_family_name",
            "pi_given_name", "title_hu", "title_en", "keywords_hu", "keywords_en",
            "discipline", "panel", "department", "institution", "participants_raw",
            "start_date", "end_date", "funding_mhuf", "amount_huf", "fte", "status",
            "summary_hu", "summary_en", "results_hu", "results_en",
            "final_report_url", "landing_page_url", "extras_json"]
    df = pd.DataFrame(rows)
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    df = df.drop_duplicates(subset=["identifier"], keep="first")
    # runbook §1.2 item 5: force string dtype so pyarrow can't int-infer null-heavy cols
    df = df.astype("string")
    return df


def validate(df: pd.DataFrame) -> None:
    n = len(df)
    log(f"DataFrame: {n:,} rows, {len(df.columns)} columns")

    def pct(col: str) -> None:
        nn = int(df[col].notna().sum())
        log(f"  {col:18}: {nn:,}/{n:,} ({100 * nn / max(n, 1):.1f}%)")

    for col in ["title_en", "title_hu", "pi_family_name", "institution",
                "start_date", "end_date", "amount_huf", "summary_en", "type_code"]:
        pct(col)
    amts = pd.to_numeric(df["amount_huf"], errors="coerce").dropna()
    if len(amts):
        log(f"  amount HUF min {amts.min():,.0f} / max {amts.max():,.0f} / "
            f"avg {amts.mean():,.0f}; total {amts.sum():,.0f}")
    yrs = df["start_date"].dropna().str[:4]
    if len(yrs):
        log(f"  start years {yrs.min()}..{yrs.max()}")
    top_pi = df.groupby(["pi_family_name", "pi_given_name"]).size().sort_values(ascending=False).head(3)
    log(f"  top PI freq (6.4a sanity): {top_pi.to_dict()}")


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> None:
    import boto3
    from botocore.exceptions import ClientError
    client = boto3.client("s3")
    log(f"Runbook §1.4 shrink-check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            log("  No existing S3 parquet; first ingest.")
            return
        raise
    prev = output_dir / ("_previous_" + PARQUET_NAME)
    try:
        client.download_file(S3_BUCKET, S3_KEY, str(prev))
        prev_count = len(pd.read_parquet(prev))
    finally:
        prev.unlink(missing_ok=True)
    log(f"  previous rows {prev_count:,}, new rows {new_count:,}")
    if new_count < prev_count and not allow_shrink:
        raise RuntimeError(
            f"Runbook §1.4 violation: refusing to shrink OTKA/NKFIH corpus "
            f"{prev_count:,} -> {new_count:,}. Use --allow-shrink to override."
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="OTKA/NKFIH (NKFI-EPR) -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=Path("otka_nkfih_data"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Smoke-test: probe at most N (not-yet-checkpointed) ids")
    parser.add_argument("--start", type=int, default=DEFAULT_START)
    parser.add_argument("--end", type=int, default=DEFAULT_END)
    parser.add_argument("--workers", type=int, default=6,
                        help="Keep low: sustained high concurrency triggers server load-shedding "
                             "that silently serves empty shells for real ids (see docstring)")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    log("=" * 72)
    log("OTKA / NKFIH (NKFI-EPR public project DB) ingest starting")
    log(f"source={BASE}  provenance={PROVENANCE}")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    ckpt_path = args.output_dir / CHECKPOINT_NAME

    rows = crawl(args, ckpt_path)
    if not rows:
        log("No records; aborting.")
        sys.exit(1)

    df = build_dataframe(rows)
    validate(df)

    out = args.output_dir / PARQUET_NAME
    df.to_parquet(out, index=False, engine="pyarrow")
    log(f"Wrote {out} ({out.stat().st_size:,} bytes)")

    if args.skip_upload:
        log("--skip-upload set; not uploading to S3.")
        return
    if args.limit:
        log("Partial run (--limit); refusing to upload a partial corpus.")
        return
    load_walden_env()
    check_no_shrink(len(df), args.allow_shrink, args.output_dir)
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3
    boto3.client("s3").upload_file(str(out), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    main()
