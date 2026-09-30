#!/usr/bin/env python3
"""
Danmarks Frie Forskningsfond (DFF, Independent Research Fund Denmark) -- ONE council -> S3
=========================================================================================

DFF's "Se oversigt over stoettet forskning" page
(https://dff.dk/hvad-har-vi-stoettet/se-oversigt-over-stoettet-forskning/) is a Vue widget
over DFF's e-grant external API (p16-eksternapi.e-grant.dk/api/v2/Bevilling). The page ships
the API URL and its public, read-only widget key in its own data-model attribute (same
pattern as a public Algolia search key, runbook ladder item 3); this script reads them from
the page at run time rather than hard-coding them.

Every grant carries BevillingsgiverDK = "<fund name> | <council>", e.g.
"Danmarks Frie Forskningsfond | Sundhed og Sygdom" (2017+) and
"Det Frie Forskningsraad | Sundhed og Sygdom" (pre-2017 name), so one council's grants can be
pulled exactly. Fields: GoCaseId (e-grant case "6221-00029A"), Danish/English title +
abstract, instrument (Virkemiddel), fiscal year (Finanslovsaar), amount (BevilgetBeloeb, DKK),
applicant first/last name, organisation (+CVR), political theme. No start/end dates.

funder_award_id (runbook 2.1.1): DFF registers grant DOIs 10.46540/<case>B and tells grantees
to cite them; works citing this council write the B-form ("0602-02273B": 316 exact B-form
strings vs 118 bare and 40 A-form in openalex_awards_raw for F4320310483, 2026-09-30). So the
shipped id is the GoCaseId with its trailing application letter replaced by "B"; go_case_id
keeps the source value.

Default council is Sundhed og Sygdom (F4320310483). The same API serves Natur og Univers,
Teknologi og Produktion, Samfund og Erhverv, Kultur og Kommunikation and the cross-council
committee (--council).

Output: s3://openalex-ingest/awards/<slug>/<slug>_projects.parquet (slug dff_sss by default)
"""

import argparse
import html
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

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

PAGE_URL = "https://dff.dk/hvad-har-vi-stoettet/se-oversigt-over-stoettet-forskning/"
FUND_NAMES = ["Danmarks Frie Forskningsfond", "Det Frie Forskningsråd"]  # 2017+ name, older name
S3_BUCKET = "openalex-ingest"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
PAGE_SIZE = 100
REQUEST_DELAY = 0.5
MAX_CONSECUTIVE_NON200 = 5


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def widget_config() -> dict:
    r = requests.get(PAGE_URL, headers=HEADERS, timeout=60)
    r.raise_for_status()
    m = re.search(r"data-model='([^']+)'", r.text)
    if not m:
        raise SystemExit("could not find the grants widget data-model on the DFF page; layout changed?")
    cfg = json.loads(html.unescape(m.group(1)))
    if not cfg.get("apiUrl") or not cfg.get("apiKey"):
        raise SystemExit("widget data-model has no apiUrl/apiKey")
    return cfg


HONORIFIC_RE = re.compile(r"^(?:(?:professor|prof|dr|mr|mrs|ms)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook 2.4.1 helper (wolf_to_s3.py). Only used when the API
    lacks Fornavn/Efternavn."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def fetch_giver(cfg: dict, giver: str, limit: int | None) -> list[dict]:
    headers = {**HEADERS, "Content-Type": "application/json", "X-API-Key": cfg["apiKey"]}
    rows, page, total_pages, non200 = [], 1, None, 0
    while total_pages is None or page <= total_pages:
        params = [("Sidenummer", page), ("Sidestoerrelse", PAGE_SIZE),
                  ("BevillingsejerDK", "DFF"), ("BevillingsgiverDK", giver)]
        try:
            r = requests.get(cfg["apiUrl"], params=params, headers=headers, timeout=90)
            status = r.status_code
        except requests.RequestException as e:
            status, r = f"error {e}", None
        if status != 200:
            non200 += 1
            log(f"  {giver} page {page}: HTTP {status} ({non200}/{MAX_CONSECUTIVE_NON200}); retrying")
            if non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError(f"{MAX_CONSECUTIVE_NON200} consecutive failures on page {page}; not truncating silently")
            time.sleep(3 * non200)
            continue
        non200 = 0
        j = r.json()
        total_pages = int(j.get("AntalSider") or 0)  # source-reported terminator
        data = j.get("Data") or []
        rows += data
        log(f"  {giver} page {page}/{total_pages}: {len(data)} grants (total {len(rows)}/{j.get('AntalResultater')})")
        if limit and len(rows) >= limit:
            return rows[:limit]
        page += 1
        time.sleep(REQUEST_DELAY)
    expected = int(j.get("AntalResultater") or 0) if total_pages else 0
    if total_pages and len(rows) < expected:
        raise RuntimeError(f"{giver}: got {len(rows)} grants, API reports {expected}")
    return rows


def citable_id(case: str | None) -> str | None:
    """'6221-00029A' -> '6221-00029B' (the grant-DOI / citation form)."""
    if not case:
        return None
    c = case.strip()
    m = re.fullmatch(r"(\d{4}-\d{5})[A-Z]?", c)
    return f"{m.group(1)}B" if m else c


def flatten(item: dict) -> dict:
    b, a, o = item.get("Bevilling") or {}, item.get("Ansoeger") or {}, item.get("Organisation") or {}
    given, family = (a.get("Fornavn") or None), (a.get("Efternavn") or None)
    if not family and a.get("FuldNavn"):
        given, family = split_name(a["FuldNavn"])
    return {
        "go_case_id": b.get("GoCaseId"),
        "funder_award_id": citable_id(b.get("GoCaseId")),
        "title_da": b.get("TitelDK"),
        "title_en": b.get("TitelEN"),
        "description_da": b.get("BeskrivelseDK"),
        "description_en": b.get("BeskrivelseEN"),
        "abstract_da": b.get("AbstractDK"),
        "abstract_en": b.get("AbstractEN"),
        "grant_owner": b.get("BevillingsejerDK"),
        "grantor_da": b.get("BevillingsgiverDK"),
        "grantor_en": b.get("BevillingsgiverEN"),
        "instrument_da": b.get("VirkemiddelNavnDK"),
        "instrument_en": b.get("VirkemiddelNavnEN"),
        "fiscal_year": b.get("Finanslovsaar"),
        "amount": b.get("BevilgetBeloeb"),
        "currency": "DKK" if b.get("BevilgetBeloeb") is not None else None,
        "web_published_at": b.get("WebPubliceringstidspunkt"),
        "political_theme_da": b.get("PolitiskTemaDK"),
        "political_theme_en": b.get("PolitiskTemaEN"),
        "applicant_name": a.get("FuldNavn"),
        "applicant_given_name": given,
        "applicant_family_name": family,
        "applicant_role_da": a.get("AnsoegerRolleDK"),
        "organisation_da": o.get("OrganisationNavnDK"),
        "organisation_en": o.get("OrganisationNavnEN"),
        "organisation_administrator": o.get("Administrator"),
        "organisation_cvr": o.get("CVR"),
        "raw_json": json.dumps(item, ensure_ascii=False),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="DFF e-grant API (one council) -> parquet -> S3")
    ap.add_argument("--council", default="Sundhed og Sygdom")
    ap.add_argument("--slug", default="dff_sss")
    ap.add_argument("--limit", type=int, default=None, help="max grants per grantor name (smoke test)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true", help="override the 1.4 shrink guard")
    args = ap.parse_args()

    cfg = widget_config()
    log(f"Widget API: {cfg['apiUrl']}")
    items = []
    for fund in FUND_NAMES:
        giver = f"{fund} | {args.council}"
        got = fetch_giver(cfg, giver, args.limit)
        log(f"{giver}: {len(got)} grants")
        items += got
    df = pd.DataFrame([flatten(i) for i in items])
    if df.empty:
        raise SystemExit("no grants returned")
    bad = df[~df["grantor_da"].str.endswith(f"| {args.council}", na=False)]
    if len(bad):
        raise SystemExit(f"{len(bad)} rows from another council leaked into the result: {bad['grantor_da'].unique()[:5]}")

    before = len(df)
    df = df.drop_duplicates(subset=["go_case_id"], keep="first")
    log(f"{before} grants, {before - len(df)} duplicate case ids dropped")
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id after A->B mapping: {df.loc[dupes, 'go_case_id'].tolist()[:20]}")
    for c in ["title_da", "title_en", "abstract_en", "abstract_da", "amount", "fiscal_year",
              "applicant_family_name", "organisation_da", "instrument_da"]:
        log(f"  {c:22s} {df[c].notna().mean():6.1%}")
    log(f"  fiscal years {df['fiscal_year'].min()}-{df['fiscal_year'].max()}; total DKK {df['amount'].sum():,.0f}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    name = f"{args.slug}_projects.parquet"
    parquet_path = args.output_dir / name
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    key = f"awards/{args.slug}/{name}"
    # runbook 1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / f"_previous_{name}"
    try:
        s3.download_file(S3_BUCKET, key, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(df)}")
        if len(df) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{key}")
    s3.upload_file(str(parquet_path), S3_BUCKET, key)
    log("Done")


if __name__ == "__main__":
    main()
