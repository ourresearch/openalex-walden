#!/usr/bin/env python3
"""
Tekes / Business Finland to S3 Data Pipeline
============================================

Harvests Business Finland's public research-project funding data
("Julkisen tutkimuksen projektit") from the tietopankki Qlik Sense databank
and uploads a parquet to S3.

Source app: https://tietopankki.businessfinland.fi/anonymous/extensions/
            Julkisen_tutkimuksen_projektit/julkisen_tutkimuksen_projektit.html
Qlik app id: 9fcbb7d4-6b35-487a-b078-ef0c17619553 (~7.3K rows, decision
years 2013-2026, EUR amounts, diary-number grant ids, public abstracts).

Access method: the Qlik Engine WebSocket is blocked for plain Python
clients (Azure Application Gateway WAF returns 403 on the handshake,
probed 2026-07-12), so this script drives a real headless Chromium via
Playwright and pulls a hypercube through the page's own Qlik Capability
API session (same pattern as rwj_to_s3.py / osf_to_s3.py).

Fields (Finnish source names -> parquet columns):
- %Key.project_id -> project_key      - Diari -> diary_number (native id)
- Rahoituksen paatosvuosi -> decision_year
- Organisaatio -> organisation        - Y-tunnus -> business_id
- BF:n ohjelma -> program             - Organisaatiomuoto -> organisation_type
- Projektin tila -> project_status    - Projektin tyyppi -> project_type
- Rahoituspalvelu -> funding_service  - Julkinen tiivistelma -> abstract_fi
- Sum(#research_funding_sum) -> amount_eur

The source publishes NO project titles and NO PI names (checked the app's
full field list) — NULL/composed downstream per runbook.

Funder attribution (multi-funder, runbook §2.3.2): decisions ≤2017 belong
to Tekes (F4320321855); Business Finland (F4320328501) began operating
2018-01-01, so decisions ≥2018 are Business Finland. Both funder ids are
stamped per row here via `openalex_funder_id`.

Output: s3://openalex-ingest/awards/business_finland/business_finland_projects.parquet

Usage:
    python business_finland_to_s3.py [--output-dir DIR] [--limit N]
                                     [--skip-download] [--skip-upload]
                                     [--allow-shrink]
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22, runbook §1.2 #7) ---
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
    def _open(file, mode="r", buffering=-1, encoding=None, errors=None,
              newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open
# --- end shim ---

MASHUP_URL = ("https://tietopankki.businessfinland.fi/anonymous/extensions/"
              "Julkisen_tutkimuksen_projektit/julkisen_tutkimuksen_projektit.html")
QLIK_APP_ID = "9fcbb7d4-6b35-487a-b078-ef0c17619553"

TEKES_FUNDER_ID = 4320321855          # decisions <= 2017
BUSINESS_FINLAND_FUNDER_ID = 4320328501  # decisions >= 2018

# NOTE: the app publishes NO project title and NO start/end dates — the
# master dimensions "Projekti"/"Alkupvm"/"Loppupvm"/"Tutkimustyyppi" in the
# app are broken leftovers (their fields raise qErrorCode 7000 and the app's
# own projects table doesn't display them). Only the decision year is dated.
FIELDS = [
    "%Key.project_id",
    "Diari",
    "Rahoituksen päätösvuosi",
    "Organisaatio",
    "Y-tunnus",
    "BF:n ohjelma",
    "Organisaatiomuoto",
    "Projektin tila",
    "Projektin tyyppi",
    "Rahoituspalvelu",
    "Julkinen tiivistelmä",
]
COLUMNS = [
    "project_key", "diary_number", "decision_year", "organisation",
    "business_id", "program", "organisation_type", "project_status",
    "project_type", "funding_service", "abstract_fi", "amount_eur",
]
# Awarded-funding measure: mirrors the app's master measure "Myönnetty
# rahoitus" — the raw field holds the literal string 'Luottamuksellinen'
# (= confidential) for projects whose amount is withheld; those come back as
# text and are stored as-is (the notebook NULLs them).
AMOUNT_MEASURE = ("If([#research_funding_sum] = 'Luottamuksellinen', 'Luottamuksellinen', "
                  "Sum({<[#research_funding_sum] -= {'Luottamuksellinen'}>}[#research_funding_sum]))")
PAGE_ROWS = 500   # 12 cols x 500 rows = 6000 cells/page (engine cap 10000)

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/business_finland/business_finland_projects.parquet"


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def load_repo_dotenv() -> None:
    """AWS creds live in the repo .env (never ~/.aws). Load if unset."""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


JS_OPEN_CUBE = """
(args) => new Promise((resolve, reject) => {
  const [appId, fields, pageRows, amountMeasure] = args;
  let tries = 0;
  const tryIt = () => {
    if (!window.qlik) {
      if (++tries > 120) { reject('qlik never loaded'); return; }
      setTimeout(tryIt, 500); return;
    }
    const app = window.qlik.openApp(appId, {
      host: window.location.hostname, prefix: '/anonymous/',
      port: window.location.port, isSecure: true});
    const dims = fields.map(f => ({qDef: {qFieldDefs: ['[' + f + ']']}}));
    app.createCube({
      qDimensions: dims,
      qMeasures: [{qDef: {qDef: amountMeasure}}],
      qInitialDataFetch: [{qTop: 0, qLeft: 0, qWidth: fields.length + 1, qHeight: pageRows}]
    }, () => {}).then(model => {
      window.__bfModel = model;
      resolve(JSON.stringify(model.layout.qHyperCube.qSize));
    }).catch(e => reject('createCube failed: ' + String(e)));
  };
  tryIt();
})
"""

JS_GET_PAGE = """
(args) => new Promise((resolve, reject) => {
  const [top, height, width] = args;
  window.__bfModel.getHyperCubeData('/qHyperCubeDef',
    [{qTop: top, qLeft: 0, qWidth: width, qHeight: height}]
  ).then(pages => {
    const rows = pages[0].qMatrix.map(r => r.map(c => {
      if (c.qText === undefined || c.qText === null) return null;
      return c.qText;
    }));
    resolve(JSON.stringify(rows));
  }).catch(e => reject('getHyperCubeData failed: ' + String(e)));
})
"""


def harvest(output_dir: Path, limit=None) -> Path:
    from playwright.sync_api import sync_playwright

    jsonl = output_dir / "business_finland_rows.jsonl"
    log(f"Opening {MASHUP_URL}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(MASHUP_URL, timeout=90000, wait_until="domcontentloaded")
        time.sleep(8)  # let require.js bootstrap qlik

        size = json.loads(page.evaluate(JS_OPEN_CUBE, [QLIK_APP_ID, FIELDS, PAGE_ROWS, AMOUNT_MEASURE]))
        total = size["qcy"]
        width = size["qcx"]
        log(f"Hypercube ready: {total:,} rows x {width} cols")
        if width != len(FIELDS) + 1:
            raise RuntimeError(f"unexpected cube width {width} (wanted {len(FIELDS)+1}) — field list changed?")

        target = min(total, limit) if limit else total
        fetched = 0
        t0 = time.time()
        with open(jsonl, "w", encoding="utf-8") as fh:
            top = 0
            while top < target:
                height = min(PAGE_ROWS, target - top)
                rows = json.loads(page.evaluate(JS_GET_PAGE, [top, height, width]))
                if not rows:
                    raise RuntimeError(f"empty page at qTop={top} with {fetched:,}/{target:,} fetched — refusing to truncate")
                for r in rows:
                    fh.write(json.dumps(r, ensure_ascii=False) + "\n")
                fetched += len(rows)
                top += len(rows)
                elapsed = time.time() - t0
                rate = fetched / elapsed if elapsed else 0
                eta = (target - fetched) / rate if rate else 0
                log(f"  fetched {fetched:,}/{target:,} rows ({fetched*100//max(target,1)}%) — ETA {eta:.0f}s")
        browser.close()

    if not limit and fetched < total:
        raise RuntimeError(f"fetched {fetched:,} < reported {total:,} — refusing to continue")
    log(f"Harvest done: {fetched:,} rows -> {jsonl}")
    return jsonl


def build_dataframe(jsonl: Path) -> pd.DataFrame:
    # NB: split on '\n' only — abstracts contain U+2028 line separators that
    # str.splitlines() would split on, corrupting JSONL lines.
    matrix = [json.loads(line) for line in jsonl.read_text().split("\n") if line.strip()]
    df = pd.DataFrame(matrix, columns=COLUMNS)
    # Qlik renders empty dims as "-"; normalize to NULL. 'Luottamuksellinen'
    # (confidential) amounts and 0-sums are preserved as-is; the notebook
    # maps both to NULL amount.
    df = df.replace({"-": None, "": None})
    log(f"  {len(df):,} raw rows")
    df = df[df["diary_number"].notna()].copy()
    # Era split (§2.3.2): <=2017 Tekes, >=2018 Business Finland
    yr = pd.to_numeric(df["decision_year"], errors="coerce")
    df["openalex_funder_id"] = [
        TEKES_FUNDER_ID if (y == y and y <= 2017) else BUSINESS_FINLAND_FUNDER_ID
        for y in yr
    ]
    # §1.2 #6: dedup by the column shipped as amount — consortium projects can
    # appear once per participating org; keep the max-amount row per diary id.
    df["_amt"] = pd.to_numeric(df["amount_eur"], errors="coerce")
    before = len(df)
    df = df.sort_values("_amt", ascending=False, na_position="last")
    df = df.drop_duplicates(subset=["diary_number"], keep="first").drop(columns=["_amt"]).reset_index(drop=True)
    log(f"  deduped {before - len(df):,} duplicate diary numbers -> {len(df):,} projects")
    df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df = df.astype("string")   # §1.2 #5: force string dtype before to_parquet
    return df


def check_no_shrink(new_count: int, allow_shrink: bool, output_dir: Path) -> bool:
    """§1.4 re-ingest safety: never overwrite S3 with a smaller corpus."""
    if allow_shrink:
        log("  --allow-shrink set; skipping §1.4 shrink-check")
        return True
    try:
        import boto3
        client = boto3.client("s3")
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev_path = output_dir / "_previous_s3.parquet"
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_count = len(pd.read_parquet(prev_path))
        log(f"  §1.4 shrink-check: previous S3 parquet has {prev_count:,} rows")
        if new_count < prev_count:
            log(f"  §1.4 FAIL: new ({new_count:,}) < previous ({prev_count:,}). Aborting upload.")
            return False
        log(f"  §1.4 OK: new {new_count:,} >= previous {prev_count:,}")
        return True
    except Exception as e:
        log(f"  §1.4 shrink-check skipped ({type(e).__name__}: {str(e)[:80]}) — normal on first run")
        return True


def upload_to_s3(local_file: Path) -> None:
    import boto3
    log(f"Uploading {local_file.name} -> s3://{S3_BUCKET}/{S3_KEY}")
    boto3.client("s3").upload_file(str(local_file), S3_BUCKET, S3_KEY)
    log("  upload OK")


def main() -> None:
    parser = argparse.ArgumentParser(description="Tekes/Business Finland (Qlik databank) -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path,
                        default=Path(__file__).parent / "business_finland_data",
                        help="Directory for downloaded/processed files")
    parser.add_argument("--limit", type=int, default=None,
                        help="Smoke test: fetch only the first N cube rows")
    parser.add_argument("--skip-download", action="store_true", help="Reuse existing JSONL")
    parser.add_argument("--skip-upload", action="store_true", help="Build parquet only")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override the §1.4 never-shrink safety check")
    args = parser.parse_args()

    load_repo_dotenv()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    jsonl = args.output_dir / "business_finland_rows.jsonl"
    if not args.skip_download or not jsonl.exists():
        jsonl = harvest(args.output_dir, limit=args.limit)
    else:
        log(f"--skip-download: using {jsonl}")

    df = build_dataframe(jsonl)
    out = args.output_dir / "business_finland_projects.parquet"
    df.to_parquet(out, index=False)
    amt = pd.to_numeric(df["amount_eur"], errors="coerce")
    n_amt = (amt > 0).sum()
    n_conf = (df["amount_eur"] == "Luottamuksellinen").sum()
    n_abs = df["abstract_fi"].notna().sum()
    tekes = (df["openalex_funder_id"] == str(TEKES_FUNDER_ID)).sum()
    bf = (df["openalex_funder_id"] == str(BUSINESS_FINLAND_FUNDER_ID)).sum()
    log(f"Wrote {len(df):,} rows -> {out} ({out.stat().st_size/1e6:.1f} MB)")
    log(f"  positive amount: {n_amt:,} | confidential: {n_conf:,} | abstract: {n_abs:,} | Tekes-era: {tekes:,} | BF-era: {bf:,}")

    if args.limit:
        log("--limit run: refusing to upload a truncated corpus (use full run for S3)")
        return
    if args.skip_upload:
        log("--skip-upload: done (no S3 write)")
        return
    if not check_no_shrink(len(df), args.allow_shrink, args.output_dir):
        sys.exit(2)
    upload_to_s3(out)


if __name__ == "__main__":
    main()
