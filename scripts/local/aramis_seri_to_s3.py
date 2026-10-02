#!/usr/bin/env python3
"""
Swiss State Secretariat for Education, Research and Innovation (SERI / SBFI)
awards from ARAMIS -> S3
==========================================================================

ARAMIS (https://www.aramis.admin.ch) is the Swiss federal administration's
database of research, innovation and evaluation projects funded or run by
the federal offices since 1997. SBFI publishes the whole database as an open
data export on opendata.swiss (CKAN dataset "aramis", terms_open):

    https://datenausgabe.aramis.admin.ch/full_export.json   (~48,000 projects)

discovered through the CKAN API (package_show?id=aramis), ladder step 1.

ARAMIS is SHARED across ~60 federal research units (Innosuisse, BFE, BAFU,
BAG ...), so rows are routed by `research_unit_short_name` (runbook §2.3.2).
This script keeps only the projects SERI funded:

  * SBFI  -- SERI itself (2013-): EU framework-programme participation it
             funds directly (EURA section; numbers like 15.0262 / 22.00187,
             the form papers cite), international programmes (Eurostars, AAL,
             ESA ...), national R&I, vocational-education research, evaluations
  * SBF   -- Staatssekretariat für Bildung und Forschung, SERI's legal
             predecessor (renamed SBFI on 2013-01-01; no separate OpenAlex or
             Crossref funder row), 1992-2012
  * COST  -- Swiss participation in COST actions (numbers C05.xxxx-C16.xxxx)
             funded by SBF/SBFI. COST numbers C91-C04 belong to the BBW era
             (Bundesamt für Bildung und Wissenschaft, its own OpenAlex funder
             F4320326458) and are NOT routed to SERI.

Not routed to SERI (counted in the log): EU FRP (BBW-funded EU FP
participation 1994-2004 -> BBW F4320326458), BBT (-> F4320326462), Innosuisse,
and every other federal office.

Scope filter (grant-level only): ARAMIS also records SERI's BUDGET ALLOCATIONS
-- the federal block grant to the SNSF (CHF 5bn lines), mandatory
contributions to Horizon Europe / EURATOM, membership of CERN, ESO, ESA,
EMBL, ESRF, ILL, core subsidies to academies and Art. 15 research
institutions, Leading House mandates. These are not awards to a project or
grantee and are excluded: in the institutional sections (Beiträge, Nationale
Forschung und Innovation, Internationale Programme und Organisationen,
Raumfahrt, Hochschulen, ...) only rows carrying a project-level number
(Eurostars/AAL project numbers, 10-digit project ids, NN.NNNN contract
numbers) are kept, and "Pflichtbeitrag" rows are dropped everywhere. Excluded
rows are written to excluded_allocations.csv next to the parquet for audit.

PI: ARAMIS publishes a free-text contact block (name, institution, postal
address, e-mail). Only the person's name and institution are kept; e-mail and
address are dropped. Contacts who are federal staff (admin.ch e-mail or a
federal office as institution) are programme officers, not PIs -> no PI.

Output: s3://openalex-ingest/awards/aramis_seri/aramis_seri_projects.parquet
"""

import argparse
import json
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

CKAN = "https://ckan.opendata.swiss/api/3/action/package_show?id=aramis"
FALLBACK_JSON = "https://datenausgabe.aramis.admin.ch/full_export.json"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/aramis_seri/aramis_seri_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}

SERI_UNITS = {"SBFI", "SBF"}
# SERI's own programme officers (contact = the office, not a PI). Federal RESEARCH institutes
# (Agroscope, WSL, Empa, PSI ...) are real research performers and are kept.
FEDERAL_RE = re.compile(r"Staatssekretariat|\bSBFI\b|\bSEFRI\b|\bSERI\b|\bSBF\b|Secr[ée]tariat d.[ÉE]tat|"
                        r"Segreteria di Stato|Bundesamt für (Bildung|Berufsbildung)|Office fédéral de l.éducation", re.I)

INSTITUTIONAL_SECTIONS = {"Beiträge", "Forschung auf internationaler Ebene", "Forschung auf nationaler Ebene",
                          "Nationale Forschung und Innovation", "Internationale Programme und Organisationen",
                          "Raumfahrt", "Bilaterale Zusammenarbeit in Forschung", "Hochschulen", "Internationale Beziehungen"}
PROJECT_NUMBER_RE = re.compile(r"^(P20\d\d-(Eurostars|AAL)-\d+|\d{10}|\d{2}\.\d{4,5}(-\d+)?)$", re.I)

T0 = time.time()


def allocation_reason(title: str | None, section: str | None, number: str | None) -> str | None:
    """Non-award budget allocation (block grant / membership / core subsidy), else None."""
    if re.search(r"Pflichtbeitrag", title or "", re.I):
        return "mandatory_contribution"
    if (section or "").strip() in INSTITUTIONAL_SECTIONS and not PROJECT_NUMBER_RE.match((number or "").strip()):
        return "institutional_or_programme_allocation"
    return None


def log(msg: str) -> None:
    print(f"[{time.time() - T0:7.1f}s] {msg}", flush=True)


def fetch(url: str, retries: int = 5) -> requests.Response:
    last = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=600)
            if r.status_code == 200:
                return r
            last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = repr(e)
        log(f"  GET {url} attempt {attempt}/{retries}: {last}")
        time.sleep(5 * attempt)
    raise RuntimeError(f"GET {url} failed: {last}")


def export_url() -> str:
    try:
        pkg = fetch(CKAN).json()["result"]
        for r in pkg["resources"]:
            if (r.get("format") or "").upper() == "JSON":
                u = r.get("download_url") or r.get("url")
                log(f"CKAN resource: {u} (dataset modified {pkg.get('metadata_modified')})")
                return u
    except Exception as e:  # noqa: BLE001
        log(f"WARNING CKAN lookup failed ({e!r}); using {FALLBACK_JSON}")
    return FALLBACK_JSON


def route(unit: str, number: str) -> str | None:
    """Return 'SERI' if this ARAMIS row was funded by SERI (or SBF, its predecessor), else None."""
    if unit in SERI_UNITS:
        return "SERI"
    if unit == "COST":
        m = re.match(r"^C(\d{2})\.", number or "")
        if m and 5 <= int(m.group(1)) <= 30:  # C05..C16 = SBF/SBFI era; C9x..C04 = BBW era
            return "SERI"
    return None


HONORIFIC_LINE = re.compile(r"^(?:(?:professor|professeur|professore|professorin|prof|dr|pd|med|phil|nat|sc|rer|techn|ing|dipl|lic|mme|m|mr|mrs|ms|frau|herr|madame|monsieur)\.?\s*)+$", re.I)
HONORIFIC_RE = re.compile(r"^(?:(?:prof|professor|dr|pd|med|phil|nat|sc|rer|techn|dipl|ing|lic|mme|mr|mrs|ms|frau|herr|madame|monsieur)\.?\s+)+", re.I)


def split_name(name: str | None) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py) with a leading-title strip."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).replace(",", " ").split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_contact(block: str | None) -> dict:
    out = {"contact_name": None, "contact_institution": None, "contact_is_federal_staff": None}
    if not block:
        return out
    lines = [ln.strip() for ln in block.replace("\r", "").split("\n") if ln.strip()]
    lines = [ln for ln in lines if "@" not in ln]          # never keep e-mail addresses
    while lines and HONORIFIC_LINE.match(lines[0]):
        lines.pop(0)
    if not lines:
        return out
    out["contact_name"] = lines[0]
    inst = lines[1] if len(lines) > 1 else None
    # institution names wrapped over two lines ("Eidg. Materialprüfungs- und" / "Forschungsanstalt")
    if inst and len(lines) > 2 and re.search(r"(-|\bund|\band|\bet|\bfür|\bde|,)$", inst):
        inst = (inst[:-1] if inst.endswith("-") and not inst.endswith(" -") else inst + " ") + lines[2]
        inst = re.sub(r"- und", "- und", inst)
    out["contact_institution"] = inst
    out["contact_is_federal_staff"] = bool(FEDERAL_RE.search(inst or "")) or bool(FEDERAL_RE.search(lines[0]))
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="ARAMIS (SERI-funded projects) -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp/aramis_seri"))
    p.add_argument("--input-json", type=Path, default=None, help="use a local copy of full_export.json")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    if args.input_json:
        data = json.loads(args.input_json.read_text())
    else:
        url = export_url()
        r = fetch(url)
        log(f"Downloaded {len(r.content) / 1e6:.1f} MB")
        data = r.json()
    log(f"ARAMIS projects in export: {len(data):,}")

    counts: dict[str, int] = {}
    rows = []
    for x in data:
        unit = (x.get("research_unit_short_name") or "").strip()
        number = (x.get("number") or "").strip()
        counts[unit] = counts.get(unit, 0) + 1
        if route(unit, number) != "SERI":
            continue
        c = parse_contact(x.get("contact_person_full_address"))
        is_pi = bool(c["contact_name"]) and not c["contact_is_federal_staff"]
        given, family = split_name(c["contact_name"]) if is_pi else (None, None)
        rows.append({
            "aramis_number": number,
            "research_unit": unit,
            "section": (x.get("section") or "").strip() or None,
            "project_state": x.get("project_state_name"),
            "title_de": x.get("title_de") or None,
            "title_en": x.get("title_en") or None,
            "title_fr": x.get("title_fr") or None,
            "title_it": x.get("title_it") or None,
            "abstract": x.get("abstract_text") or None,
            "categories": x.get("category_names") or None,
            "nabs_policy_domain": x.get("nabs_policy_domain") or None,
            "research_disciplines": x.get("research_discipline_names") or None,
            "start_date": (x.get("export_start_date") or "")[:10] or None,
            "end_date": (x.get("export_end_date") or "")[:10] or None,
            "granted_total_costs_chf": x.get("granted_total_costs"),
            "contact_name": c["contact_name"],
            "contact_institution": c["contact_institution"],
            "contact_is_federal_staff": c["contact_is_federal_staff"],
            "lead_given_name": given,
            "lead_family_name": family,
            "lead_institution": c["contact_institution"] if is_pi else None,
        })
        if args.limit and len(rows) >= args.limit:
            break

    log("Rows per ARAMIS research unit (top 25): " + json.dumps(
        dict(sorted(counts.items(), key=lambda kv: -kv[1])[:25]), ensure_ascii=False))
    df = pd.DataFrame(rows)
    log(f"Routed to SERI: {len(df):,} ({df['research_unit'].value_counts().to_dict()})")
    excluded_cost = sum(1 for x in data if (x.get('research_unit_short_name') or '') == 'COST'
                        and route('COST', x.get('number') or '') is None)
    log(f"COST rows left to the BBW era (C9x-C04): {excluded_cost}")

    title_any = df["title_de"].fillna(df["title_en"]).fillna(df["title_fr"]).fillna(df["title_it"])
    df["allocation_reason"] = [allocation_reason(t, sec, n) for t, sec, n in
                               zip(title_any, df["section"], df["aramis_number"])]
    alloc = df["allocation_reason"].notna()
    amt = pd.to_numeric(df["granted_total_costs_chf"], errors="coerce")
    log(f"Excluding {alloc.sum()} budget-allocation rows (CHF {amt[alloc].sum():,.0f}): "
        f"{df.loc[alloc, 'allocation_reason'].value_counts().to_dict()}")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    df[alloc].drop(columns=["contact_name", "contact_institution"]).to_csv(
        args.output_dir / "excluded_allocations.csv", index=False)
    df = df[~alloc].drop(columns=["allocation_reason"]).copy()
    log(f"Kept {len(df):,} grant-level SERI rows ({df['research_unit'].value_counts().to_dict()})")

    dupes = df["aramis_number"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate ARAMIS numbers: {df.loc[dupes, 'aramis_number'].tolist()[:20]}")
    for c in ["title_de", "title_en", "abstract", "start_date", "end_date", "granted_total_costs_chf",
              "lead_family_name", "lead_institution"]:
        log(f"  {c:24s} {df[c].notna().mean():6.1%}")
    log(f"  total CHF {pd.to_numeric(df['granted_total_costs_chf']).sum():,.0f}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "aramis_seri_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_aramis_seri_projects.parquet"
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
