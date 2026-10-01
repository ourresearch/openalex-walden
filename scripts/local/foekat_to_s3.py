#!/usr/bin/env python3
"""
German federal Förderkatalog -> S3, one ministry (Ressort) at a time
=====================================================================

The federal government's Förderkatalog (https://foerderportal.bund.de/foekat)
lists the funded projects of five federal ministries (2026-10-01, completed
projects included: BMFTR 194,108, BMWE 48,391, BMUKN 34,300, BMV 16,611,
BMLEH 8,633 = 302,043). Each row carries its Ressort. This script runs the
catalogue's detail search for one Ressort including completed projects
(suche.lfdVhb=N) and downloads the built-in CSV export ("Ausgabe als
Textdatei") of the hit list in the same session -- ladder step 0, the
funder's own bulk export. Generalises scripts/local/bmel_foekat_to_s3.py
(BMLEH), which stays as is.

Ressort -> OpenAlex funder (routing is done in the notebooks, oxjob #1491):
    BMFTR -> F4320321114 Bundesministerium für Bildung und Forschung
             (CreateBMBFAwards, priority 36; replaces scripts/local/bmbf_to_s3.py)
    BMWE  -> F4320323803 Bundesministerium für Wirtschaft und Energie (CreateBMWEAwards)
    BMV   -> F4320310476 Bundesministerium für Verkehr und digitale Infrastruktur (CreateBMVAwards)
    BMUKN -> F4320323948 Bundesministerium für Umwelt, Naturschutz und Reaktorsicherheit (CreateBMUKNAwards)
The catalogue lists completed projects under the ministry responsible TODAY,
so e.g. older BMWi/BMWK projects appear under BMWE.

Large hit lists: if one export returns fewer rows than the search reports,
the search is split by FKZ prefix (suche.fkzSuche wildcard, e.g. "01%",
"01A%") recursively until every slice's export is complete, and the slice
hit counts must add up to the Ressort total.

Fields: FKZ (Förderkennzeichen -- the number papers cite), administering
agency, grantee + executing unit with city/state/country, topic,
Leistungsplansystematik, runtime, federal funding amount (EUR), funding
profile, project acronym, funding type. No PI names are published.

Scope filter: bookkeeping rows are dropped (brochure payouts "nur zum Zweck
der Auszahlung", collective booking placeholders "Sammelvorhabenbuchung").

Output: s3://openalex-ingest/awards/foekat/<ressort>_projects.parquet
        (e.g. awards/foekat/bmftr_projects.parquet)

Usage:
    py -3.13 scripts/local/foekat_to_s3.py --ressort BMV --skip-upload
    py -3.13 scripts/local/foekat_to_s3.py --ressort BMFTR
"""

import argparse
import io
import re
import string
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
RESSORTS = ("BMFTR", "BMWE", "BMV", "BMUKN", "BMLEH")
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/foekat/{ressort}_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
DROP_RE = re.compile(r"nur zum Zweck der Auszahlung|Sammelvorhabenbuchung", re.I)
PREFIX_CHARS = string.digits + string.ascii_uppercase

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


def search_and_export(ressort: str, fkz_pattern: str) -> tuple[int, pd.DataFrame]:
    """One detail search (Ressort + optional FKZ wildcard, completed projects included) and its CSV export."""
    s = requests.Session()
    s.headers.update(HEADERS)
    form = {
        "actionMode": "searchlist", "suche.detailSuche": "true", "suche.ressortSuche[0]": ressort,
        "suche.lfdVhb": "N",  # include completed projects
        "suche.listrowpersite": "10", "suche.listrowfrom": "1", "suche.orderby": "1", "suche.order": "asc",
        "submitAction": "Detailsuche starten",
    }
    if fkz_pattern != "%":
        form["suche.fkzSuche[0]"] = fkz_pattern
    with_retries(lambda: s.get(BASE, params={"actionMode": "searchmask"}, timeout=120), "search mask")  # session
    page = with_retries(lambda: s.post(BASE, data=form, timeout=180), f"search {fkz_pattern}").content.decode(
        "iso-8859-15", "replace")
    flat = re.sub(r"\s+", " ", re.sub(r"&nbsp;", " ", re.sub(r"<[^>]+>", " ", page)))
    m = re.search(r"\(([\d.]+)\s*Treffer\)|([\d.]+)\s*Treffer insgesamt", flat)
    if not m:
        if re.search(r"keine\s+(Vorhaben|Treffer|Daten)", flat, re.I):
            return 0, pd.DataFrame(columns=list(COLUMNS))
        raise SystemExit(f"no hit count on the result page for {fkz_pattern!r} -- search form changed?")
    hits = int((m.group(1) or m.group(2)).replace(".", ""))
    if hits == 0:
        return 0, pd.DataFrame(columns=list(COLUMNS))
    r = with_retries(lambda: s.get(BASE, params={"actionMode": "print", "presentationType": "csv"}, timeout=1800),
                     f"csv export {fkz_pattern}")
    text = r.content.decode("iso-8859-15", "replace")
    text = re.sub(r'="', '"', text)  # Excel-style ="..." text cells
    raw = pd.read_csv(io.StringIO(text), sep=";", dtype=str, keep_default_na=False)
    raw = raw.loc[:, ~raw.columns.str.startswith("Unnamed")]
    return hits, raw


def fetch_ressort(ressort: str) -> tuple[int, pd.DataFrame]:
    """Total hit count and every row, splitting by FKZ prefix where an export comes back short."""
    total, df = search_and_export(ressort, "%")
    log(f"Förderkatalog search Ressort={ressort} (incl. completed): {total} hits, export {len(df)} rows")
    if len(df) >= total:
        return total, df
    parts, sliced_hits, queue = [], 0, [""]
    while queue:
        stem = queue.pop(0)
        for c in PREFIX_CHARS:
            pattern = stem + c + "%"
            hits, part = search_and_export(ressort, pattern)
            if hits == 0:
                continue
            if len(part) < hits:
                log(f"  slice {pattern}: {hits} hits, export {len(part)} -> splitting")
                queue.append(stem + c)
                continue
            log(f"  slice {pattern}: {hits} hits, export {len(part)}")
            parts.append(part)
            sliced_hits += hits
            time.sleep(1)
    if sliced_hits != total:
        raise SystemExit(f"FKZ-prefix slices cover {sliced_hits} hits but the Ressort search reports {total}: "
                         "some FKZs start with a character outside 0-9A-Z -- extend PREFIX_CHARS")
    return total, pd.concat(parts, ignore_index=True)


def main() -> None:
    p = argparse.ArgumentParser(description="One Förderkatalog Ressort -> parquet -> S3")
    p.add_argument("--ressort", required=True, choices=RESSORTS)
    p.add_argument("--limit", type=int, default=None, help="smoke test: keep only N rows")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp/foekat"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()
    ressort = args.ressort

    hits, raw = fetch_ressort(ressort)
    if len(raw) < hits:
        raise SystemExit(f"CSV export has {len(raw)} rows < {hits} hits -- refusing to truncate")
    missing = [c for c in COLUMNS if c not in raw.columns]
    if missing:
        raise SystemExit(f"CSV header changed, missing: {missing}")
    df = raw.rename(columns=COLUMNS)[list(COLUMNS.values())].copy()
    for c in df.columns:
        df[c] = df[c].str.strip()

    bad = df[df["ressort"] != ressort]
    if len(bad):
        raise SystemExit(f"{len(bad)} rows from another Ressort: {bad['ressort'].value_counts().to_dict()}")
    drop = df["title"].str.contains(DROP_RE)
    log(f"Dropping {drop.sum()} bookkeeping rows: {df.loc[drop, ['fkz', 'title']].values.tolist()[:20]}")
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
    log(f"  admin agencies (top 10): {df['admin_agency'].value_counts().head(10).to_dict()}")
    log(f"  total EUR {df['amount_eur'].sum():,.0f}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / f"{ressort.lower()}_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    key = S3_KEY.format(ressort=ressort.lower())
    previous = args.output_dir / f"_previous_{ressort.lower()}_projects.parquet"
    try:  # runbook §1.4: never shrink the corpus on re-ingest
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
