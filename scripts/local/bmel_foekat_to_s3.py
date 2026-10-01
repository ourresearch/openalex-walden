#!/usr/bin/env python3
"""
German Federal Ministry of Food and Agriculture (BMEL, now BMLEH) awards from
the federal Förderkatalog -> S3
==========================================================================

The federal government's Förderkatalog (https://foerderportal.bund.de/foekat)
lists the funded projects of six federal ministries. Its search has a Ressort
field and a built-in "Ausgabe als Textdatei" CSV export of the hit list
(ladder step 0: the funder's own bulk export). This script runs the detail
search Ressort = BMLEH (Bundesministerium für Landwirtschaft, Ernährung und
Heimat -- BMEL's name since 2025; per the catalogue, completed projects are
listed under today's responsible ministry, so this covers BMELV/BMEL-era
projects too), including completed projects (suche.lfdVhb=N), and downloads
the CSV export in the same session.

Why not FISA (the tracker's source): fisaonline.de lists projects of several
federal and state funders and its project search/detail pages returned HTTP
500/503 on 2026-10-01 (home page up, every /projekte-finden/ URL down). The
Förderkatalog is the ministry-level register and is already routed by
ministry, so no cross-funder assignment is needed (runbook §2.3.2).

Fields: FKZ (Förderkennzeichen -- the number papers cite, e.g. 2819107716,
28DE103C22, 22003015), administering agency (BLE or FNR, BMEL's project
executing agencies), grantee + executing unit with city/state/country, topic,
Leistungsplansystematik, runtime, federal funding amount (EUR), funding profile,
project acronym, funding type. No PI names are published.

Scope filter: 2 bookkeeping rows are dropped -- a brochure payout ("nur zum
Zweck der Auszahlung") and a collective booking placeholder
("Sammelvorhabenbuchung", EUR 0). Everything else (research projects, model &
demonstration projects, BMEL doctoral programme, departmental research
contracts) is kept.

Output: s3://openalex-ingest/awards/bmel/bmel_projects.parquet
"""

import argparse
import io
import re
import time
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22, runbook §1.2) ---
# (equivalent of sys.stdout.reconfigure(encoding="utf-8") under an alias)
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

BASE = "https://foerderportal.bund.de/foekat/jsp/SucheAction.do"
LANDING = BASE + "?actionMode=view&fkz={fkz}"
RESSORT = "BMLEH"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/bmel/bmel_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
DROP_RE = re.compile(r"nur zum Zweck der Auszahlung|Sammelvorhabenbuchung", re.I)

COLUMNS = {  # CSV header -> parquet column (the CSV repeats municipality headers for the 2nd block)
    "FKZ": "fkz", "Ressort": "ressort", "Referat": "referat", "Administrierende Stelle": "admin_agency",
    "Arb.-Einh.": "work_unit", "Förderempfänger /Auftragnehmer": "recipient",
    "Gemeindekennziffer": "recipient_municipality_code", "Stadt/Gemeinde": "recipient_municipality",
    "Ort": "recipient_city", "Bundesland": "recipient_state", "Staat": "recipient_country",
    "Ausführende Stelle": "executing_unit", "Gemeindekennziffer.1": "executing_municipality_code",
    "Stadt/Gemeinde.1": "executing_municipality", "Ort.1": "executing_city", "Bundesland.1": "executing_state",
    "Staat.1": "executing_country", "Thema des geförderten Vorhabens": "title",
    "Leistungsplansystematik": "lps_code", "Klartext Leistungsplansystematik": "lps_text",
    "Laufzeit von": "start_raw", "Laufzeit bis": "end_raw",
    "Förder- / Auftragssumme (Anteil des Bundes) in EUR": "amount_raw",
    "Förderprofil": "funding_profile", "Projekt": "project_acronym", "Förderart": "funding_kind",
}
COUNTRY_ISO = {"deutschland": "DE", "österreich": "AT", "schweiz": "CH", "niederlande": "NL", "frankreich": "FR",
               "belgien": "BE", "dänemark": "DK", "polen": "PL", "italien": "IT", "vereinigtes königreich": "GB",
               "schweden": "SE", "finnland": "FI", "spanien": "ES", "tschechische republik": "CZ", "ungarn": "HU",
               "norwegen": "NO", "luxemburg": "LU", "irland": "IE", "portugal": "PT", "griechenland": "GR"}

T0 = time.time()


def log(msg: str) -> None:
    print(f"[{time.time() - T0:7.1f}s] {msg}", flush=True)


def with_retries(fn, what: str, retries: int = 5):
    last = None
    for attempt in range(1, retries + 1):
        try:
            r = fn()
            if r.status_code == 200:
                return r
            last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = repr(e)
        log(f"  {what} attempt {attempt}/{retries}: {last}")
        time.sleep(5 * attempt)
    raise RuntimeError(f"{what} failed: {last}")


def iso(d: str) -> str | None:
    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$", (d or "").strip())
    return f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}" if m else None


def eur(v: str) -> float | None:
    v = (v or "").strip().replace(".", "").replace(",", ".")
    try:
        x = float(v)
    except ValueError:
        return None
    return x if x > 0 else None  # EUR 0 = amount not disclosed / placeholder


def main() -> None:
    p = argparse.ArgumentParser(description="BMEL/BMLEH projects from the Förderkatalog -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="smoke test: keep only N rows")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp/bmel"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    s = requests.Session()
    s.headers.update(HEADERS)
    form = {
        "actionMode": "searchlist", "suche.detailSuche": "true", "suche.ressortSuche[0]": RESSORT,
        "suche.lfdVhb": "N",  # include completed projects
        "suche.listrowpersite": "10", "suche.listrowfrom": "1", "suche.orderby": "1", "suche.order": "asc",
        "submitAction": "Detailsuche starten",
    }
    with_retries(lambda: s.get(BASE, params={"actionMode": "searchmask"}, timeout=120), "search mask")  # session
    page = with_retries(lambda: s.post(BASE, data=form, timeout=180), "search").content.decode("iso-8859-15", "replace")
    flat = re.sub(r"\s+", " ", re.sub(r"&nbsp;", " ", re.sub(r"<[^>]+>", " ", page)))
    m = re.search(r"\(([\d.]+)\s*Treffer\)|([\d.]+)\s*Treffer insgesamt", flat)
    hits = int((m.group(1) or m.group(2)).replace(".", "")) if m else None
    log(f"Förderkatalog search Ressort={RESSORT} (incl. completed): {hits} hits")
    if not hits:
        raise SystemExit("no hit count on the result page -- search form changed?")

    r = with_retries(lambda: s.get(BASE, params={"actionMode": "print", "presentationType": "csv"}, timeout=900),
                     "csv export")
    text = r.content.decode("iso-8859-15", "replace")
    text = re.sub(r'="', '"', text)  # Excel-style ="..." text cells
    raw = pd.read_csv(io.StringIO(text), sep=";", dtype=str, keep_default_na=False)
    raw = raw.loc[:, ~raw.columns.str.startswith("Unnamed")]
    log(f"CSV export: {len(raw)} rows, {len(raw.columns)} columns")
    if len(raw) < hits:
        raise SystemExit(f"CSV export has {len(raw)} rows < {hits} hits -- refusing to truncate")
    missing = [c for c in COLUMNS if c not in raw.columns]
    if missing:
        raise SystemExit(f"CSV header changed, missing: {missing}")
    df = raw.rename(columns=COLUMNS)[list(COLUMNS.values())].copy()
    for c in df.columns:
        df[c] = df[c].str.strip()

    bad = df[df["ressort"] != RESSORT]
    if len(bad):
        raise SystemExit(f"{len(bad)} rows from another Ressort: {bad['ressort'].value_counts().to_dict()}")
    drop = df["title"].str.contains(DROP_RE)
    log(f"Dropping {drop.sum()} bookkeeping rows: {df.loc[drop, ['fkz', 'title']].values.tolist()}")
    df = df[~drop].copy()

    df["start_date"] = df["start_raw"].apply(iso)
    df["end_date"] = df["end_raw"].apply(iso)
    df["amount_eur"] = df["amount_raw"].apply(eur)
    df["recipient_country_iso"] = df["recipient_country"].str.lower().map(COUNTRY_ISO)
    df["landing_page_url"] = df["fkz"].apply(lambda f: LANDING.format(fkz=requests.utils.quote(f)))
    if args.limit:
        df = df.head(args.limit)

    dupes = df["fkz"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate FKZ: {df.loc[dupes, 'fkz'].tolist()[:20]}")
    for c in ["title", "recipient", "start_date", "end_date", "amount_eur", "recipient_country_iso", "project_acronym"]:
        log(f"  {c:22s} {df[c].replace('', None).notna().mean():6.1%}")
    log(f"  admin agencies: {df['admin_agency'].value_counts().to_dict()}")
    log(f"  total EUR {df['amount_eur'].sum():,.0f}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "bmel_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_bmel_projects.parquet"
    try:  # runbook §1.4: never shrink the corpus on re-ingest
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(df)}")
        if len(df) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
