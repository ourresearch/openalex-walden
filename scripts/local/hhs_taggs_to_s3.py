#!/usr/bin/env python3
"""
Download HHS TAGGS (Tracking Accountability in Government Grants System)
award-search exports to parquet for OpenAlex awards ingestion.

Why this source (ingest method ladder):
    TAGGS (taggs.hhs.gov) is HHS's own first-party grants register covering all
    HHS operating divisions (OPDIVs). It has no documented bulk-file download,
    but its Award Search page exposes a clean session-based CSV export
    (method 3-ish: a server-side filtered export, no HTML scraping, no browser):

        1. GET  /SearchAward                       -> establishes ASP.NET session cookie
        2. POST /UserSession/ajaxUpdateAwardSession -> stores the filter in the session
               (selectedFYs, selectedOPDIVs, selectedAATs, selectedStates, ...)
        3. POST /SearchAward/Export_AwardSearchFilter (OutputFormat=CSV) -> 302
        4. GET  /Export/ExportToOutput?OutPutFormat=CSV -> the CSV

    The CSV echoes the applied filter in its title line ("All Obligations;
    Data Fiscal Year = 2022; OPDIVs = ACF; ...") which this script verifies on
    every export to catch session desync, and ends with an "Exported on ..."
    footer line whose absence signals a truncated (capped) export.

Hard source constraints (verified empirically 2026-07-12):
    - Only the most recent 5 fiscal years are searchable/exportable
      (FY2022-FY2026 as of this writing). Older FYs return 0 rows.
    - Exports are capped at ~25,000 rows. This script slices by
      (FY, OPDIV) and re-slices by Award Activity Type and then State when a
      slice hits the cap.
    - Rows are OBLIGATION ACTIONS, not awards: one award (Award Number) has
      one row per obligation action. The notebook aggregates to award level.

OPDIV scoping (see notebooks/awards/CreateHHSTaggsAwards.ipynb header):
    Harvested + shipped in hhs_taggs_projects.parquet:
        SAMHSA, IHS, CMS, ASPR, DHHS/OS
    Harvested but staged separately (NOT shipped) in staging_fda.parquet:
        FDA  -- an FDA USAspending ingest is at Step 4 in another session
    Deliberately NOT harvested (already Complete or in-flight elsewhere):
        NIH (Complete first-party via ExPORTER, priority 3-tier),
        CDC (usaspending_cdc, prio 55), AHRQ (usaspending_ahrq, prio 54),
        HRSA (hrsa data warehouse, prio 57), ACF (usaspending_acf, prio 233),
        ACL (usaspending_acl, prio 235)

Output:
    s3://openalex-ingest/awards/hhs_taggs/hhs_taggs_projects.parquet
    s3://openalex-ingest/awards/hhs_taggs/staging_fda.parquet

Usage:
    python scripts/local/hhs_taggs_to_s3.py --limit 2 --skip-upload   # smoke
    python scripts/local/hhs_taggs_to_s3.py                           # full
    python scripts/local/hhs_taggs_to_s3.py --skip-download           # rebuild parquet from cached CSVs
"""

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22, runbook section 1.2) ---
import sys
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
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None,
                   newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open_utf8
# --- end shim ---

import argparse
import csv
import io
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

BASE = "https://taggs.hhs.gov"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

S3_BUCKET = "openalex-ingest"
S3_PREFIX = "awards/hhs_taggs"
MAIN_PARQUET = "hhs_taggs_projects.parquet"
FDA_PARQUET = "staging_fda.parquet"

# TAGGS only serves the most recent 5 fiscal years (verified 2026-07-12:
# FY2010/2015/1998 all return 0 rows; the FY listbox offers 2022-2026).
DEFAULT_FYS = ["2022", "2023", "2024", "2025", "2026"]

# OPDIVs shipped in the main parquet (attributed in the notebook).
MAIN_OPDIVS = ["ASPR", "CMS", "DHHS/OS", "IHS", "SAMHSA"]
# Harvested but staged separately; an FDA build is in flight elsewhere.
STAGED_OPDIVS = ["FDA"]

# Award Activity Type values from the SearchAward page listbox (FY22-26).
AATS = [
    "BLOCK",
    "CLOSED-ENDED",
    "COOPERATIVE AGREEMENT",
    "DIRECT PAYMENT FOR SPECIFIED USE",
    "DISCRETIONARY",
    "OPEN-ENDED",
]

# State/territory codes from the SearchAward page listbox (incl. OTF = other/foreign).
STATES = [
    "AL", "AK", "AS", "AZ", "AR", "CA", "CO", "MP", "CT", "DE", "DC", "FM",
    "FL", "GA", "GU", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MH", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "OTF", "PA", "PR", "PW", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VI", "VA", "WA", "WV", "WI", "WY",
]

# Exports are capped at ~25,000 rows (ACF FY2022 returned exactly 24,998 data
# rows with no footer). Treat anything at/over this as truncated and re-slice.
CAP_ROWS = 24_900

EXPECTED_COLUMNS = [
    "Issue FY", "Fund FY", "OPDIV", "ALN", "Assistance Listing", "State",
    "Award Number", "Award Title", "Award Code", "Award Class Type",
    "Budget Year", "Action Date", "Legal Entity Name", "ZIP Code",
    "Award Amount",
]

POLITE_SLEEP_S = 0.7
MAX_TRIES = 5


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def load_repo_dotenv() -> None:
    """Inject AWS creds from the repo .env if not already in the environment."""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip().strip('"').strip("'")
        if key.startswith("AWS_") and key not in os.environ and val:
            os.environ[key] = val


class SessionDesync(RuntimeError):
    """The export's echoed filter line did not match the filter we set."""


class TaggsClient:
    def __init__(self) -> None:
        self.sess: requests.Session | None = None
        self.new_session()

    def new_session(self) -> None:
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": UA})
        r = self.sess.get(f"{BASE}/SearchAward", timeout=60)
        r.raise_for_status()
        log(f"  session established (HTTP {r.status_code}, {len(r.text)} bytes)")

    def _export_once(self, fy: str, opdiv: str, aat: str | None, state: str | None) -> str:
        r = self.sess.post(
            f"{BASE}/UserSession/ajaxUpdateAwardSession",
            data={
                "selectedFYs": fy,
                "selectedCFDAs": "",
                "selectedOPDIVs": opdiv,
                "selectedStates": state or "",
                "tbKeywords": "",
                "cbKeywordRelation": "AND",
                "selectedCANFYs": "",
                "selectedAATs": aat or "",
            },
            timeout=60,
        )
        r.raise_for_status()

        r = self.sess.post(
            f"{BASE}/SearchAward/Export_AwardSearchFilter",
            data={
                "OutputFormat": "CSV",
                "isExportControl": "true",
                "ModelHash": "SearchAwardExport_AwardSearchFilter",
            },
            timeout=60,
            allow_redirects=False,
        )
        if r.status_code not in (200, 302):
            raise RuntimeError(f"export POST returned HTTP {r.status_code}")

        r = self.sess.get(f"{BASE}/Export/ExportToOutput?OutPutFormat=CSV", timeout=300)
        r.raise_for_status()
        if "text/csv" not in r.headers.get("Content-Type", ""):
            raise RuntimeError(f"export GET returned {r.headers.get('Content-Type')}")
        return r.text

    def export_slice(self, fy: str, opdiv: str, aat: str | None = None,
                     state: str | None = None) -> tuple[list[list[str]], bool]:
        """Return (data_rows, capped) for one filter slice, verifying the echo line."""
        last_exc: Exception | None = None
        for attempt in range(1, MAX_TRIES + 1):
            try:
                text = self._export_once(fy, opdiv, aat, state)
                rows, capped = self._parse_csv(text, fy, opdiv, aat, state)
                return rows, capped
            except (requests.RequestException, SessionDesync, RuntimeError) as exc:
                last_exc = exc
                log(f"  [retry {attempt}/{MAX_TRIES}] {type(exc).__name__}: {exc}")
                time.sleep(5 * attempt)
                try:
                    self.new_session()
                except requests.RequestException as sexc:
                    log(f"  [retry {attempt}/{MAX_TRIES}] session re-establish failed: {sexc}")
        raise RuntimeError(f"slice fy={fy} opdiv={opdiv} aat={aat} state={state} "
                           f"failed after {MAX_TRIES} tries") from last_exc

    @staticmethod
    def _parse_csv(text: str, fy: str, opdiv: str, aat: str | None,
                   state: str | None) -> tuple[list[list[str]], bool]:
        reader = csv.reader(io.StringIO(text))
        title = None
        header = None
        footer_seen = False
        data: list[list[str]] = []
        for row in reader:
            if not row:
                continue
            cell0 = row[0]
            if title is None and cell0.startswith("TAGGS Award Search Export"):
                title = cell0
                continue
            if header is None:
                if cell0 == "Issue FY":
                    header = row[: len(EXPECTED_COLUMNS)]
                continue
            if cell0.startswith("Exported on "):
                footer_seen = True
                continue
            data.append(row[: len(EXPECTED_COLUMNS)])

        if title is None or header is None:
            raise RuntimeError("export CSV missing title/header rows")
        if [h.strip() for h in header] != EXPECTED_COLUMNS:
            raise RuntimeError(f"unexpected export columns: {header}")

        # Verify the echoed filter — catches session desync between the
        # session-update POST and the export.
        checks = [f"Data Fiscal Year = {fy}", f"OPDIVs = {opdiv}"]
        if aat:
            checks.append(f"AATs = {aat}")
        if state:
            checks.append(f"States = {state}")
        for c in checks:
            if c not in title:
                raise SessionDesync(f"echo line missing {c!r}: {title[:200]!r}")

        capped = (len(data) >= CAP_ROWS) or (data and not footer_seen)
        return data, capped


def slice_key(fy: str, opdiv: str, aat: str | None, state: str | None) -> str:
    def san(s: str) -> str:
        return re.sub(r"[^A-Za-z0-9]+", "-", s).strip("-")
    parts = [f"fy{fy}", san(opdiv)]
    if aat:
        parts.append(san(aat))
    if state:
        parts.append(san(state))
    return "_".join(parts)


class Harvester:
    def __init__(self, out_dir: Path, limit: int | None) -> None:
        self.raw_dir = out_dir / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path = out_dir / "manifest.json"
        self.manifest: dict = {}
        if self.manifest_path.exists():
            self.manifest = json.loads(self.manifest_path.read_text())
            log(f"resuming: manifest has {len(self.manifest)} accepted slices")
        self.client: TaggsClient | None = None
        self.limit = limit
        self.n_fetched = 0
        self.t0 = time.time()

    def _client(self) -> TaggsClient:
        if self.client is None:
            self.client = TaggsClient()
        return self.client

    def _save_leaf(self, key: str, fy: str, opdiv: str, aat: str | None,
                   state: str | None, rows: list[list[str]]) -> None:
        path = self.raw_dir / f"{key}.csv"
        with open(path, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(EXPECTED_COLUMNS)
            w.writerows(rows)
        self.manifest[key] = {
            "file": path.name, "fy": fy, "opdiv": opdiv, "aat": aat,
            "state": state, "rows": len(rows),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        self.manifest_path.write_text(json.dumps(self.manifest, indent=1))

    def limit_reached(self) -> bool:
        return self.limit is not None and self.n_fetched >= self.limit

    def harvest(self, fy: str, opdiv: str, aat: str | None = None,
                state: str | None = None) -> None:
        """Fetch one slice; recurse into finer slices if capped."""
        if self.limit_reached():
            return
        key = slice_key(fy, opdiv, aat, state)
        if key in self.manifest:
            log(f"skip (cached): {key} ({self.manifest[key]['rows']} rows)")
            return

        rows, capped = self._client().export_slice(fy, opdiv, aat, state)
        self.n_fetched += 1
        elapsed = time.time() - self.t0
        log(f"[{elapsed:6.0f}s] {key}: {len(rows)} rows{' [CAPPED -> re-slicing]' if capped else ''}")

        if capped:
            if aat is None:
                for a in AATS:
                    self.harvest(fy, opdiv, aat=a)
            elif state is None:
                for s in STATES:
                    self.harvest(fy, opdiv, aat=aat, state=s)
            else:
                raise RuntimeError(f"slice {key} capped even at state level; "
                                   f"needs a finer slicing dimension")
        else:
            self._save_leaf(key, fy, opdiv, aat, state, rows)
        time.sleep(POLITE_SLEEP_S)

    def load_all(self) -> pd.DataFrame:
        frames = []
        for key, meta in sorted(self.manifest.items()):
            path = self.raw_dir / meta["file"]
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
            df["source_slice"] = key
            frames.append(df)
        if not frames:
            return pd.DataFrame(columns=EXPECTED_COLUMNS + ["source_slice"])
        out = pd.concat(frames, ignore_index=True)
        out.columns = [re.sub(r"[^A-Za-z0-9]+", "_", c).strip("_").lower()
                       for c in out.columns]
        return out


def check_no_shrink(new_count: int, s3_key: str, allow_shrink: bool, out_dir: Path) -> bool:
    """Runbook section 1.4: never overwrite the S3 parquet with fewer rows."""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("s3")
    log(f"section 1.4 shrink check vs s3://{S3_BUCKET}/{s3_key}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=s3_key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            log("  no existing parquet - first ingest")
            return True
        log(f"  [WARN] head_object failed ({code}); treating as first ingest")
        return True

    prev_path = out_dir / f"_previous_{Path(s3_key).name}"
    try:
        client.download_file(S3_BUCKET, s3_key, str(prev_path))
        previous_count = len(pd.read_parquet(prev_path))
    except Exception as exc:
        log(f"  [ERROR] could not read existing parquet ({exc}); aborting upload")
        return False
    finally:
        prev_path.unlink(missing_ok=True)

    log(f"  previous count: {previous_count}; new count: {new_count}")
    if new_count < previous_count:
        if allow_shrink:
            log("  [OVERRIDE] new corpus is smaller but --allow-shrink is set")
            return True
        log(f"  [ERROR] 1.4 violation - refusing to shrink corpus "
            f"({previous_count} -> {new_count})")
        return False
    log("  [OK] new corpus is not smaller")
    return True


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    df = df.astype("string")   # runbook section 1.2: force string dtype
    df.to_parquet(path, index=False)
    log(f"wrote {path} ({len(df)} rows, {path.stat().st_size/1024/1024:.1f} MB)")


def upload(path: Path, s3_key: str, df: pd.DataFrame, allow_shrink: bool,
           out_dir: Path) -> bool:
    if not check_no_shrink(len(df), s3_key, allow_shrink, out_dir):
        return False
    import boto3
    boto3.client("s3").upload_file(str(path), S3_BUCKET, s3_key)
    log(f"uploaded s3://{S3_BUCKET}/{s3_key}")
    return True


def main() -> None:
    ap = argparse.ArgumentParser(description="HHS TAGGS award exports -> parquet -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("./hhs_taggs_data"))
    ap.add_argument("--limit", type=int, default=None,
                    help="Smoke test: stop after N export requests")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--skip-download", action="store_true",
                    help="Rebuild parquet from cached CSVs only")
    ap.add_argument("--allow-shrink", action="store_true",
                    help="Override the runbook 1.4 no-shrink guard")
    ap.add_argument("--fys", default=",".join(DEFAULT_FYS))
    ap.add_argument("--opdivs", default=",".join(MAIN_OPDIVS + STAGED_OPDIVS))
    args = ap.parse_args()

    load_repo_dotenv()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    fys = [f.strip() for f in args.fys.split(",") if f.strip()]
    opdivs = [o.strip() for o in args.opdivs.split(",") if o.strip()]

    log("=" * 70)
    log("HHS TAGGS award-search export harvest")
    log(f"FYs: {fys}  OPDIVs: {opdivs}")
    log(f"S3 target: s3://{S3_BUCKET}/{S3_PREFIX}/")

    h = Harvester(args.output_dir, args.limit)
    if not args.skip_download:
        for fy in fys:
            for opdiv in opdivs:
                if h.limit_reached():
                    break
                h.harvest(fy, opdiv)
        log(f"harvest done: {h.n_fetched} exports fetched this run, "
            f"{len(h.manifest)} leaf slices total")

    df = h.load_all()
    log(f"combined action-level rows: {len(df)}")
    if df.empty:
        log("[ERROR] no rows harvested")
        sys.exit(1)

    df["downloaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Split: FDA staged separately (in-flight build elsewhere); rest = main.
    fda = df[df["opdiv"] == "FDA"].reset_index(drop=True)
    main_df = df[df["opdiv"] != "FDA"].reset_index(drop=True)

    for name, part in [("main", main_df), ("staged FDA", fda)]:
        if not part.empty:
            awards = part.groupby(["opdiv", "award_number"]).ngroups
            log(f"{name}: {len(part)} action rows, {awards} distinct (opdiv, award_number)")
            log(f"{name} by opdiv: {part['opdiv'].value_counts().to_dict()}")

    main_path = args.output_dir / MAIN_PARQUET
    fda_path = args.output_dir / FDA_PARQUET
    write_parquet(main_df, main_path)
    write_parquet(fda, fda_path)

    if args.skip_upload:
        log("[DONE] --skip-upload set; local parquet only.")
        return

    ok1 = upload(main_path, f"{S3_PREFIX}/{MAIN_PARQUET}", main_df, args.allow_shrink, args.output_dir)
    ok2 = upload(fda_path, f"{S3_PREFIX}/{FDA_PARQUET}", fda, args.allow_shrink, args.output_dir)
    if not (ok1 and ok2):
        log("[ERROR] one or more uploads did not complete")
        sys.exit(1)
    log("[DONE]")


if __name__ == "__main__":
    main()
