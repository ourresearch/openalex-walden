#!/usr/bin/env python3
"""
SweCRIS sweep to S3 Data Pipeline (multi-funder)
================================================

Downloads grants for the Swedish funders in SweCRIS (Sweden's national
research-grants registry, CC0, run by Vetenskapsradet) that are NOT already
first-party ingests in OpenAlex, and writes ONE parquet with an
`openalex_funder_id` column per row (runbook §2.3.2 multi-funder rule).

SweCRIS carries 13 funders (probed 2026-07-12). Excluded from this sweep:
- Vetenskapsradet, Vinnova, Formas, Forte, Riksbankens Jubileumsfond —
  already Complete first-party ingests.
- Statens energimyndighet (202100-5000) — separate ingest at its own
  provenance/priority (energimyndigheten_to_s3.py, priority 435).
- Ostersjostiftelsen (802400-4155) and IFAU (202100-4946) — first-party
  ingests already in openalex_awards_raw at priorities 327/338 (tracker
  Step 6, batch #8); double-ingesting here would duplicate awards.

Included funders (SweCRIS org nr -> OpenAlex funder):
- 202100-2585  Swedish National Space Agency (Rymdstyrelsen)      F4320321031
- 202100-1975  Naturvardsverket (Swedish EPA)                     F4320322579
- 802400-4213  The Knowledge Foundation (KK-stiftelsen)           F4320321759
- 202100-0712  Swedish Geotechnical Institute (SGI)               F4320316858
- 802423-4075  The Kamprad Family Foundation                      F4320325984

Data source: SweCRIS API (https://swecris-api.vr.se), public token.
Output: s3://openalex-ingest/awards/swecris/swecris_projects.parquet

Notes:
- SweCRIS projectId may carry a per-funder suffix ("..._SGI"); raw value is
  preserved, the notebook strips the trailing "_Xxx" token.
- PI name split uses the canonical split_name helper (wolf_to_s3.py).

Usage:
    python swecris_to_s3.py [--output-dir DIR] [--limit N]
                            [--skip-download] [--skip-upload] [--allow-shrink]
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

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

SWECRIS_API_BASE = "https://swecris-api.vr.se/v1"
# Public API token. NOTE: rotates yearly — "VRSwecrisAPI2025-1" stopped
# working; "VRSwecrisAPI2026-1" confirmed live 2026-07-12.
SWECRIS_API_TOKEN = "VRSwecrisAPI2026-1"

# SweCRIS org nr -> (OpenAlex funder_id numeric, canonical display name)
SWEEP_FUNDERS = {
    "202100-2585": (4320321031, "Swedish National Space Agency"),
    "202100-1975": (4320322579, "Naturvårdsverket"),
    "802400-4213": (4320321759, "Stiftelsen för Kunskaps- och Kompetensutveckling"),
    "202100-0712": (4320316858, "Statens geotekniska institut"),
    "802423-4075": (4320325984, "Familjen Kamprads Stiftelse"),
}

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/swecris/swecris_projects.parquet"

USER_AGENT = "OpenAlex awards ingest (mailto:kyle@ourresearch.org)"


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


def split_name(name: str):
    """Split 'James P. Eisenstein' -> ('James P.', 'Eisenstein').

    Canonical helper from wolf_to_s3.py (§2.4.1): strips trailing
    degree/suffix tokens before splitting; last token = family name.
    """
    if not name:
        return None, None
    tokens = str(name).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def download_funder(org_nr: str, output_dir: Path) -> Path:
    url = f"{SWECRIS_API_BASE}/projects/funders/{org_nr}"
    out = output_dir / f"swecris_raw_{org_nr}.json"
    log(f"Downloading {org_nr} from {url}")
    headers = {"Authorization": f"Bearer {SWECRIS_API_TOKEN}", "User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, timeout=600)
    r.raise_for_status()
    out.write_text(r.text)
    log(f"  saved {out.stat().st_size/1e6:.1f} MB -> {out.name}")
    return out


def parse_projects(json_path: Path, org_nr: str, limit=None) -> list[dict]:
    funder_id, funder_name = SWEEP_FUNDERS[org_nr]
    data = json.loads(json_path.read_text())
    if limit:
        data = data[:limit]
    rows = []
    for row in data:
        pi_given = pi_family = pi_orcid = pi_full = None
        for person in row.get("peopleList") or []:
            if person.get("roleEn") == "Principal Investigator":
                pi_full = person.get("fullName")
                pi_given, pi_family = split_name(pi_full)
                pi_orcid = person.get("orcId")
                break
        scbs = row.get("scbs") or []
        rows.append({
            "openalex_funder_id": funder_id,
            "funder_display_name": funder_name,
            "swecris_org_nr": org_nr,
            "project_id": row.get("projectId"),
            "title": row.get("projectTitleSv"),
            "title_english": row.get("projectTitleEn"),
            "abstract": row.get("projectAbstractSv"),
            "abstract_english": row.get("projectAbstractEn"),
            "start_date": row.get("projectStartDate"),
            "end_date": row.get("projectEndDate"),
            "coordinating_organisation_id": row.get("coordinatingOrganisationId"),
            "coordinating_organisation": row.get("coordinatingOrganisationNameEn") or row.get("coordinatingOrganisationNameSv"),
            "coordinating_organisation_type": row.get("coordinatingOrganisationTypeOfOrganisationEn"),
            "funding_organisation_id": row.get("fundingOrganisationId"),
            "funding_organisation": row.get("fundingOrganisationNameEn") or row.get("fundingOrganisationNameSv"),
            "amount": row.get("fundingsSek"),
            "funding_year": row.get("fundingYear"),
            "type_of_award_id": row.get("typeOfAwardId"),
            "type_of_award": row.get("typeOfAwardDescrEn") or row.get("typeOfAwardDescrSv"),
            "pi_full_name": pi_full,
            "pi_given_name": pi_given,
            "pi_family_name": pi_family,
            "pi_orcid": pi_orcid,
            "people_json": json.dumps(row.get("peopleList") or [], ensure_ascii=False),
            "main_discipline": scbs[0].get("scb5NameEn") if scbs else None,
            "main_discipline_level1": scbs[0].get("scb1NameEn") if scbs else None,
            "updated_date": row.get("updatedDate"),
            "loaded_date": row.get("loadedDate"),
        })
    return rows


def build_dataframe(all_rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(all_rows)
    for col in ("start_date", "end_date"):
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
    before = len(df)
    # §1.2 #6: dedup sorted by the column shipped as amount
    df = df.sort_values("amount", ascending=False, na_position="last")
    df = df.drop_duplicates(subset=["swecris_org_nr", "project_id"], keep="first").reset_index(drop=True)
    if len(df) < before:
        log(f"  deduped {before - len(df)} duplicate (funder, project_id) rows (kept max-amount)")
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
    parser = argparse.ArgumentParser(description="SweCRIS sweep (5 funders) -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path,
                        default=Path(__file__).parent / "swecris_data",
                        help="Directory for downloaded/processed files")
    parser.add_argument("--limit", type=int, default=None,
                        help="Smoke test: only process the first N projects per funder")
    parser.add_argument("--skip-download", action="store_true", help="Reuse existing raw JSONs")
    parser.add_argument("--skip-upload", action="store_true", help="Build parquet only")
    parser.add_argument("--allow-shrink", action="store_true",
                        help="Override the §1.4 never-shrink safety check")
    args = parser.parse_args()

    load_repo_dotenv()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    all_rows = []
    for org_nr in SWEEP_FUNDERS:
        raw = args.output_dir / f"swecris_raw_{org_nr}.json"
        if not args.skip_download or not raw.exists():
            raw = download_funder(org_nr, args.output_dir)
            time.sleep(1.0)
        else:
            log(f"--skip-download: using {raw.name}")
        rows = parse_projects(raw, org_nr, limit=args.limit)
        log(f"  {SWEEP_FUNDERS[org_nr][1]}: {len(rows):,} project rows")
        all_rows.extend(rows)

    if not all_rows:
        log("ERROR: no projects parsed"); sys.exit(1)

    df = build_dataframe(all_rows)
    out = args.output_dir / "swecris_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"Wrote {len(df):,} rows -> {out} ({out.stat().st_size/1e6:.1f} MB)")
    log("Per-funder counts after dedup:")
    for org_nr, (fid, name) in SWEEP_FUNDERS.items():
        sub = df[df["swecris_org_nr"] == org_nr]
        n_amt = sub["amount"].notna().sum()
        n_pi = sub["pi_family_name"].notna().sum()
        log(f"  F{fid} {name}: {len(sub):,} projects | amount {n_amt:,} | PI {n_pi:,}")

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
