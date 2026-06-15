#!/usr/bin/env python3
"""
Fondazione Cariplo (scientific research grants) to S3 Data Pipeline
==================================================================

Pulls Fondazione Cariplo's scientific-research grant portfolio and uploads a
parquet to S3 for Databricks ingestion. Cariplo is OpenAlex funder F4320321499
(9,411 works); existing award coverage is Crossref-derived shells, so the native
project records (id/title/PI/institution/EUR-amount) are net-new canonical data.

Data source: the "Ricerca Scientifica" interactive viz at
    http://ricercascientifica.fondazionecariplo.it loads its ENTIRE dataset as a
    single static JS file: http://ricercascientifica.fondazionecariplo.it/data/data.js
    which begins `var erogazioni = [ {...}, ... ]`. Extract the array literal and
    json.loads it -> 1,202 research projects (2000-2015). No WAF/auth/JS-render.

    This is the SCIENTIFIC-research portal only — Cariplo's social/arts/environment
    philanthropy lives in a separate system and is NOT in this file (no filtering
    needed). Italian field keys; PI is pre-split into Nome/Cognome.

    ⚠️ Coverage ends at 2015 (~2016-vintage snapshot). Post-2015 grants live in the
    main-site "Contributi Deliberati" DB (separate, not pinned) — a documented
    follow-up. This build is the clean 2000-2015 set.

Output: s3://openalex-ingest/awards/cariplo/cariplo_grants.parquet
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
DATA_URL = "http://ricercascientifica.fondazionecariplo.it/data/data.js"
LANDING = "http://ricercascientifica.fondazionecariplo.it/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/cariplo/cariplo_grants.parquet"

_NULLISH = {"", "nan", "none", "n/a", "na", "-", "null"}


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return None if s.lower() in _NULLISH else s


def extract_array(txt: str) -> list:
    """Extract the `var erogazioni = [ ... ]` array literal, respecting strings."""
    i = txt.index("erogazioni")
    j = txt.index("[", i)
    depth = 0
    instr = esc = False
    for k in range(j, len(txt)):
        c = txt[k]
        if instr:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                instr = False
        else:
            if c == '"':
                instr = True
            elif c == "[":
                depth += 1
            elif c == "]":
                depth -= 1
                if depth == 0:
                    return json.loads(txt[j:k + 1])
    raise ValueError("unbalanced erogazioni array")


def parse_amount(v):
    try:
        n = float(v)
        return str(int(round(n))) if n > 0 else None
    except (TypeError, ValueError):
        return None


def upload_to_s3(local_path: Path, bucket: str, key: str) -> bool:
    s3_uri = f"s3://{bucket}/{key}"
    print(f"\nUploading to {s3_uri}...")
    try:
        subprocess.run(["aws", "s3", "cp", str(local_path), s3_uri],
                       capture_output=True, text=True, check=True)
        print(f"Upload complete: {s3_uri}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Upload failed: {e.stderr}")
        return False


def main():
    ap = argparse.ArgumentParser(description="Fondazione Cariplo research grants to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/cariplo_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Fondazione Cariplo (scientific research) -> S3")
    print("=" * 60)

    print(f"Downloading {DATA_URL} ...")
    last = None
    for attempt in range(4):
        try:
            r = requests.get(DATA_URL, headers={"User-Agent": UA}, timeout=90)
            r.raise_for_status()
            data = extract_array(r.text)
            break
        except Exception as e:
            last = e
            print(f"  [retry {attempt+1}] {e}")
    else:
        raise RuntimeError(f"download/parse failed: {last}")
    print(f"  parsed {len(data)} project records")

    recs, seen = [], set()
    for d in data:
        aid = clean(d.get("ID"))
        if not aid or aid in seen:
            continue
        seen.add(aid)
        recs.append({
            "funder_award_id": aid,
            "title": clean(d.get("Titolo progetto")),
            "pi_given": clean(d.get("Nome (beneficiario)")),
            "pi_family": clean(d.get("Cognome (beneficiario)")),
            "institution": clean(d.get("Ente beneficiario")),
            "amount": parse_amount(d.get("Deliberato")),
            "currency": "EUR",
            "scheme": clean(d.get("Area tematica")),          # thematic research area
            "instrument": clean(d.get("Strumento erogativo")),  # bando / NOBEL / AGER
            "year_awarded": clean(d.get("Anno")),
            "landing_page_url": LANDING,
        })

    df = pd.DataFrame(recs).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "year_awarded", "scheme"):
        nn = df[c].notna().sum()
        print(f"  {c:14}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")

    if len(df) < 1000:
        print(f"[ERROR] only {len(df)} rows — expected ~1,202; data.js changed?")
        sys.exit(1)

    out = args.output_dir / "cariplo_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
