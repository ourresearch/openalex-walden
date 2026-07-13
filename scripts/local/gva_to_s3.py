#!/usr/bin/env python3
"""
Generalitat Valenciana (GVA) research subsidies to S3
=====================================================

Pulls the "Ayudas y subvenciones concedidas por la Generalitat Valenciana"
bulk open-data files (ALL regional subsidies, monthly CSVs per year) from the
GVA CKAN portal and scopes them to research / R&D+i awards.

Data source (CKAN, method 1 on the ingest ladder):
  https://dadesobertes.gva.es/api/3/action/package_show?id=eco-gvo-subv-{YYYY}
  Datasets: eco-gvo-subv-2022 .. eco-gvo-subv-2026 (GVA only publishes the
  4 years following the concession year, so the corpus is a rolling window).
  Catalogued nationally at datos.gob.es (a10002983-ayudas-y-subvenciones-...).

INCLUSION RULE (research scoping) — a row is kept iff:
  (a) cd_finalidad == '17'  ("Investigacion, desarrollo e innovacion" — the
      GVA budget-policy classification for R&D+i), OR
  (b) the convocatoria / titulo_extracto / linea text matches the research
      regex (RESEARCH_RE below): predoctoral/postdoctoral fellowships,
      research-group and research-project calls (PROMETEO, GenT, Santiago
      Grisolia, ACIF, AVI innovation programmes, etc.) that GVA files under
      other budget policies (mostly "Ensenanza"/education and health), AND
  (c) NOT matched by the exclusion regex (EXCLUDE_RE) that removes
      non-research rows the broad regex would drag in ("innovacion comercial"
      retail lines, "innovacion educativa" school teaching-innovation aids,
      "cooperacion educativa" student placements). Ministry names embedding
      "Investigacion" (2015-2019 education conselleria) are stripped before
      matching so they don't blanket-match their school subsidies.
  The rule was derived by reviewing every distinct (cd_finalidad,
  convocatoria) pair in the 2022-2026 corpus.

ANONYMIZED PERSON ROWS ARE EXCLUDED (AGAUR precedent): GVA publishes
  physical-person beneficiaries GDPR-redacted (beneficiario NIF empty,
  nombre = "PERSONA FISICA QUE (NO) DESARROLLA ACTIVIDAD ECONOMICA"), so
  individual fellowships (ACIF/APOSTD/Grisolia awarded to the fellow) are
  unusable as grant records and are dropped; institutional/company research
  grants (universities, institutes, PROMETEO groups via host university,
  AVI company programmes) carry named beneficiaries and are kept.

Amounts: EUR, whole units (importe column; no minor-unit encoding).

Award identity: the source has NO per-grant id. funder_award_id =
  "{cod_convocatoria}:{beneficiario-NIF}" (call code + beneficiary tax id),
  rows aggregated on that key (sum importe, min fecha_concesion). Collisions
  are therefore aggregated by construction, never silently merged downstream.

No PI field exists in the source, and person beneficiaries are anonymized
(see above) - all shipped rows are named institutions/companies, mapped to
lead_investigator.affiliation only (AGAUR pattern).

Output: s3://openalex-ingest/awards/gva/gva_projects.parquet

Usage:
    py -3 gva_to_s3.py --output-dir C:/tmp/gva --skip-upload
    py -3 gva_to_s3.py --limit 2000 --skip-upload     # smoke test
"""
import argparse
import builtins
import csv
import io
import json
import re
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------- UTF-8 shim
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass
if sys.platform == "win32":
    _orig_open = builtins.open
    def _utf8_open(file, mode="r", buffering=-1, encoding=None, *a, **kw):
        if "b" not in str(mode) and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, *a, **kw)
    builtins.open = _utf8_open
    _orig_write_text = Path.write_text
    def _wt(self, data, encoding=None, errors=None, newline=None):
        return _orig_write_text(self, data, encoding or "utf-8", errors, newline)
    Path.write_text = _wt
    _orig_read_text = Path.read_text
    def _rt(self, encoding=None, errors=None):
        return _orig_read_text(self, encoding or "utf-8", errors)
    Path.read_text = _rt

import pandas as pd

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/gva/gva_projects.parquet"
CKAN_PACKAGE_SHOW = "https://dadesobertes.gva.es/api/3/action/package_show?id=eco-gvo-subv-{year}"
FIRST_YEAR = 2020          # probe from here; portal only keeps a rolling window
UA = {"User-Agent": "OpenAlex-GVA/1.0 (mailto:support@openalex.org)"}
MAX_CONSECUTIVE_FAIL = 5   # per §1: transient non-200s are flakes, not EOF

# --- INCLUSION RULE -----------------------------------------------------
# (a) budget-policy code 17 = "Investigacion, desarrollo e innovacion"
RESEARCH_FINALIDAD_CD = "17"
# (b) research-call text (accent-insensitive: text is casefolded + de-accented
#     before matching)
RESEARCH_RE = re.compile(
    r"(investigacio|investigacion"          # research (val/cast)
    r"|i\+d|\bidi\b|i\+i"                   # R&D+i abbreviations (\bidi\b: standalone 'IDI' only, not 'juridi...')
    r"|cientific|cientifiq"                 # scientific
    r"|predoctoral|postdoctoral|posdoctoral|doctorand"
    r"|prometeo|santiago grisolia|grisolia" # GVA excellence/fellowship programmes
    r"|plan gent\b|pla gent\b"              # GenT talent-attraction plan (NOT bare 'gent' - Valencian for 'people')
    r"|\bacif\b|apostd|cidegent|cdeigent|ciaico|cipromete|\baico\b"  # GVA research call codes
    r"|grupos de investigacion|grups d'investigacio"
    r"|instituts superiors d'investigacio|institutos superiores de investigacion"
    r"|personal investigador|contratacion de investigadores"
    r"|infraestructuras? cientific|equipamiento cientific"
    r"|agencia valenciana de la innovacion|valenciana de la innovacio)",
    re.IGNORECASE,
)
# (c) matched-but-not-research lines to drop again
EXCLUDE_RE = re.compile(
    r"(innovacion comercial|innovacio comercial"   # retail modernisation
    r"|innovacion educativa|innovacio educativa"   # school teaching-innovation aids
    r"|cooperacion educativa|cooperacio educativa" # student-placement conventions
    r"|dana\b)",                                   # flood-relief aid
    re.IGNORECASE,
)
# Ministry names embed "Investigacion" (e.g. "Conselleria de Educacion,
# Investigacion, Cultura y Deporte", 2015-2019) and would match the research
# regex on every school subsidy they issue - strip them before matching.
MINISTRY_NAME_RE = re.compile(
    r"(educacion, investigacion, cultura y deporte"
    r"|educacio, investigacio, cultura i esport)",
    re.IGNORECASE,
)

_ACCENTS = str.maketrans("áéíóúàèìòùäëïöüâêîôûñç", "aeiouaeiouaeiouaeiounc")


def deaccent(s: str) -> str:
    return (s or "").lower().translate(_ACCENTS)


def is_anonymized_person(row: dict) -> bool:
    """GDPR-redacted physical persons: no NIF, placeholder instead of a name."""
    nombre = deaccent(row.get("nombre") or "")
    return (not (row.get("beneficiario") or "").strip()
            or nombre.startswith("persona fisica"))


def is_research(row: dict) -> bool:
    if is_anonymized_person(row):
        return False
    if (row.get("cd_finalidad") or "").strip() == RESEARCH_FINALIDAD_CD:
        return True
    blob = deaccent(" | ".join(
        (row.get(k) or "")
        for k in ("convocatoria", "titulo_extracto_c", "titulo_extracto_v",
                  "ds_linea_c", "ds_concedente_c", "linea_agregada_c")
    ))
    blob = MINISTRY_NAME_RE.sub(" ", blob)
    if EXCLUDE_RE.search(blob):
        return False
    return bool(RESEARCH_RE.search(blob))


# ------------------------------------------------------------------ download
def http_get(url: str, timeout: int = 120) -> bytes:
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=timeout).read()


def stream_to_file(url: str, local: Path, timeout: int = 120) -> None:
    """Stream a large file to disk with Range-based resume across retries.

    GVA's portal forcibly resets long transfers under load ([WinError 10054]);
    a single-shot read() then loses the whole file. Appending to a .part file
    and resuming with a Range header survives resets.
    """
    part = local.with_suffix(local.suffix + ".part")
    for attempt in range(1, 8):
        have = part.stat().st_size if part.exists() else 0
        hdrs = dict(UA)
        if have:
            hdrs["Range"] = f"bytes={have}-"
        req = urllib.request.Request(url, headers=hdrs)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                if have and resp.status != 206:
                    # server ignored Range - start over
                    have = 0
                mode = "ab" if have else "wb"
                with open(part, mode) as out:   # binary mode: shim passes through
                    while True:
                        chunk = resp.read(1 << 20)
                        if not chunk:
                            break
                        out.write(chunk)
            if part.stat().st_size < 200:
                raise IOError(f"suspiciously small response ({part.stat().st_size} B)")
            part.rename(local)
            return
        except Exception as e:
            print(f"    stream attempt {attempt}/7 failed at "
                  f"{part.stat().st_size/1e6 if part.exists() else 0:.1f} MB: {e}")
            time.sleep(min(60, 8 * attempt))
    raise IOError(f"could not download {url} after 7 attempts")


def discover_resources() -> list:
    """CKAN package_show per year -> list of (year, resource_name, url)."""
    out = []
    this_year = datetime.now(timezone.utc).year
    for year in range(FIRST_YEAR, this_year + 2):
        url = CKAN_PACKAGE_SHOW.format(year=year)
        try:
            data = json.loads(http_get(url, timeout=60))
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            print(f"  {year}: no dataset ({e})")
            continue
        if not data.get("success"):
            print(f"  {year}: CKAN success=false")
            continue
        n = 0
        for res in data["result"]["resources"]:
            if (res.get("format") or "").upper() != "CSV":
                continue
            out.append((year, res.get("name") or res["id"], res["url"]))
            n += 1
        print(f"  {year}: {n} monthly CSV resources")
    return out


def download_all(resources: list, cache_dir: Path) -> list:
    """Download every monthly CSV with checkpointing. Returns local paths."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    paths, failed = [], []
    t0 = time.time()
    for i, (year, name, url) in enumerate(resources, 1):
        fname = url.rsplit("/", 1)[-1]
        local = cache_dir / f"{year}_{fname}"
        if local.exists() and local.stat().st_size > 1000:
            paths.append(local)
            continue
        try:
            stream_to_file(url, local, timeout=180)
        except Exception as e:
            failed.append(fname)
            print(f"  [{i}/{len(resources)}] FAILED: {fname}: {e}")
            continue
        paths.append(local)
        el = time.time() - t0
        print(f"  [{i}/{len(resources)}] {fname} ok "
              f"({local.stat().st_size/1e6:.1f} MB, {el:.0f}s elapsed)")
        time.sleep(2)   # politeness between large file downloads
    if failed:
        # A missing month silently truncates the corpus (§1/§1.4) - fail closed.
        # Re-running resumes from cache, so only the failed files are retried.
        raise RuntimeError(f"{len(failed)} monthly files failed to download: {failed}. "
                           "Re-run to resume from the cache.")
    return paths


# ----------------------------------------------------------------- transform
KEEP_COLS = [
    "cd_ejercicio", "fecha_concesion", "beneficiario", "nombre",
    "cod_convocatoria", "convocatoria", "cd_tipo_beneficiario",
    "ds_tipo_beneficiario_c", "cd_finalidad", "ds_finalidad_c",
    "cd_concedente", "ds_concedente_c", "ds_conselleria_c",
    "ds_centro_gestor_c", "linea_agregada_c", "ds_linea_c", "importe",
    "pagado", "url_base_c", "url_publi", "cd_bdns", "titulo_extracto_c",
    "ds_programa_c",
]

def parse_rows(paths: list, limit: int = 0) -> pd.DataFrame:
    rows, nread = [], 0
    for p in paths:
        raw = p.read_bytes()
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = raw.decode("cp1252")
        rdr = csv.DictReader(io.StringIO(text), delimiter=";")
        fr = 0
        for row in rdr:
            nread += 1
            if is_research(row):
                rows.append({k: (row.get(k) or "").strip() or None for k in KEEP_COLS})
                fr += 1
        print(f"  {p.name}: kept {fr} research rows")
        if limit and nread >= limit:
            print(f"  --limit {limit} reached, stopping parse")
            break
    print(f"  parsed {nread:,} source rows -> {len(rows):,} research rows")
    return pd.DataFrame(rows)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["amount_row"] = pd.to_numeric(
        df["importe"].str.replace(",", ".", regex=False), errors="coerce")
    df["fecha_dt"] = pd.to_datetime(df["fecha_concesion"], format="%d/%m/%Y",
                                    errors="coerce")
    df["award_key"] = (df["cod_convocatoria"].fillna("NOCONV") + ":" +
                       df["beneficiario"].fillna("NOBENEF"))
    # Anonymized persons are excluded at filter time; every kept beneficiary
    # is a named legal entity (or a named person acting as a company, PFA).
    df["is_person"] = False

    agg = df.groupby("award_key", dropna=False).agg(
        cod_convocatoria=("cod_convocatoria", "first"),
        convocatoria=("convocatoria", "first"),
        titulo_extracto=("titulo_extracto_c", "first"),
        beneficiario_nif=("beneficiario", "first"),
        beneficiario_nombre=("nombre", "first"),
        is_person=("is_person", "first"),
        tipo_beneficiario=("ds_tipo_beneficiario_c", "first"),
        cd_finalidad=("cd_finalidad", "first"),
        finalidad=("ds_finalidad_c", "first"),
        concedente=("ds_concedente_c", "first"),
        conselleria=("ds_conselleria_c", "first"),
        centro_gestor=("ds_centro_gestor_c", "first"),
        linea=("ds_linea_c", "first"),
        linea_agregada=("linea_agregada_c", "first"),
        programa=("ds_programa_c", "first"),
        amount=("amount_row", "sum"),
        first_date=("fecha_dt", "min"),
        last_date=("fecha_dt", "max"),
        ejercicio=("cd_ejercicio", "min"),
        url_base=("url_base_c", "first"),
        url_publi=("url_publi", "first"),
        cd_bdns=("cd_bdns", "first"),
        n_rows=("award_key", "size"),
    ).reset_index().rename(columns={"award_key": "funder_award_id"})

    # 0/neg amounts -> NULL (§6.7 hygiene)
    agg["amount"] = agg["amount"].where(agg["amount"] > 0, other=pd.NA)

    # No person names survive GVA's GDPR redaction (anonymized rows are
    # excluded at filter time) - all beneficiaries ship as institutions.
    agg["lead_given_name"] = pd.NA
    agg["lead_family_name"] = pd.NA
    agg["institution_name"] = agg["beneficiario_nombre"]

    agg["start_date"] = agg["first_date"].dt.strftime("%Y-%m-%d")
    agg["end_date"] = agg["last_date"].dt.strftime("%Y-%m-%d")
    agg = agg.drop(columns=["first_date", "last_date"])
    agg["provenance"] = "gva"
    agg["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    dup = agg["funder_award_id"].duplicated().sum()
    if dup:
        raise RuntimeError(f"funder_award_id collision after aggregation: {dup}")

    for c in agg.columns:
        if c != "amount":
            agg[c] = agg[c].astype("string")
    return agg


# ------------------------------------------------------------------- output
def check_no_shrink(df: pd.DataFrame, allow_shrink: bool):
    try:
        import boto3
        s3 = boto3.client("s3")
        s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev = pd.read_parquet(f"s3://{S3_BUCKET}/{S3_KEY}")
        if len(df) < len(prev) and not allow_shrink:
            raise SystemExit(f"§1.4 shrink-check FAILED: new {len(df):,} < "
                             f"existing {len(prev):,}. Use --allow-shrink to override.")
        print(f"  §1.4 shrink-check OK (new {len(df):,} >= existing {len(prev):,})")
    except SystemExit:
        raise
    except Exception as e:
        print(f"  §1.4 shrink-check: no prior object / not comparable ({type(e).__name__})")


def main():
    ap = argparse.ArgumentParser(description="GVA research subsidies (CKAN) -> S3")
    ap.add_argument("--output-dir", type=Path, default=Path("gva_out"))
    ap.add_argument("--cache-dir", type=Path, default=None,
                    help="where monthly CSVs are cached (default: output-dir/cache)")
    ap.add_argument("--limit", type=int, default=0,
                    help="stop after parsing N source rows (smoke test)")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    a = ap.parse_args()
    cache = a.cache_dir or (a.output_dir / "cache")
    a.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 64)
    print("Generalitat Valenciana (GVA) research subsidies -> S3")
    print("=" * 64)
    print("[1/4] Discovering yearly datasets via CKAN")
    resources = discover_resources()
    if not resources:
        raise RuntimeError("CKAN discovery returned 0 resources - portal down or renamed")
    print(f"      {len(resources)} monthly CSVs")
    if a.limit:
        resources = resources[:3]
        print(f"      --limit smoke mode: truncated to {len(resources)} files")
    print("[2/4] Downloading (checkpointed)")
    paths = download_all(resources, cache)
    print("[3/4] Parsing + research filter")
    df_rows = parse_rows(paths, a.limit)
    if df_rows.empty:
        raise RuntimeError("0 research rows - inclusion rule or source broke")
    print("[4/4] Aggregating to one row per (convocatoria, beneficiary)")
    df = transform(df_rows)

    out = a.output_dir / "gva_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"\nSaved {out}: {len(df):,} awards, {out.stat().st_size/1e6:.1f} MB")

    nn = lambda c: 100 * df[c].notna().sum() / len(df)
    print("\nCoverage:")
    print(f"  funder_award_id   100%")
    print(f"  title(convocatoria) {nn('convocatoria'):.1f}%")
    print(f"  amount (EUR)      {nn('amount'):.1f}%")
    print(f"  beneficiary name  {nn('beneficiario_nombre'):.1f}%")
    print(f"  institution_name  {nn('institution_name'):.1f}%")
    print(f"  start_date        {nn('start_date'):.1f}%")
    amt = pd.to_numeric(df["amount"], errors="coerce")
    print(f"  EUR total {amt.sum():,.0f}, median {amt.median():,.0f}, max {amt.max():,.0f}")
    print(f"  finalidad split: {df['finalidad'].value_counts().head(8).to_dict()}")

    if not a.skip_upload:
        check_no_shrink(df, a.allow_shrink)
        import shutil
        import subprocess
        aws = shutil.which("aws")
        if not aws:
            raise RuntimeError("aws CLI not found; rerun with --skip-upload and upload manually")
        subprocess.run([aws, "s3", "cp", str(out), f"s3://{S3_BUCKET}/{S3_KEY}"], check=True)
        print(f"Uploaded to s3://{S3_BUCKET}/{S3_KEY}")
    print("\nNext: notebooks/awards/CreateGVAAwards.ipynb")


if __name__ == "__main__":
    main()
