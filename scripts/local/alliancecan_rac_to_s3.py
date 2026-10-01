#!/usr/bin/env python3
"""
Resource Allocation Competition (RAC) compute/storage allocations to S3
======================================================================

The national advanced research computing (ARC) platform's annual Resource Allocation Competition
(RAC) awards IN-KIND allocations (CPU core-years, GPU/RGU-years, project/nearline/dCache storage,
cloud vCPUs) to Canadian research groups. Each year's results report links a public Google Sheet
"List of Resource Allocation Competition <year> Awards" with one row per awarded application:
App ID, stream (Resources for Research Groups (RRG) / RRG Fast Track / Research Platforms and
Portals (RPP)), PI, institution, department, research area, project title, allocations by resource
type, and (2025+) the project summary. Method 0/4 on the runbook ladder (the funder's own bulk
export: Google Sheets CSV export, robots-allowed under docs.google.com /spreadsheet).

Sheets used (all public; ids found on the results pages / reports):
  2021  Compute Canada results page (Wayback 20210412) -> sheet 1L-En7e2...
  2022  alliancecan.ca 2022 results page (Wayback 20220814) -> sheet 17-2d3_rV...
  2023  alliancecan.ca 2023 results page (Wayback 20230322) -> sheet 1np9kRxA...
  2024  alliancecan.ca 2024 results page (Wayback 20240613) -> sheet 17V1H0_t...
  2025  "Report of RAC 2025 results (EN)" PDF (docs.alliancecan.ca/wiki/RAC_Annual_Reports) -> 1fIgEq1K...
  2026  "2026 RAC Report English" PDF (same wiki page) -> 1WB3SS75...
Not available: RAC 2019 / 2020 (their sheets now render only "#REF!"), RAC <= 2018 (xlsx not
archived). The 2023/2024 results pages on alliancecan.ca now 404; the sheets they linked still work.

Who made the allocation (funder routing): Compute Canada ran the RAC through RAC 2022 (results
March 2022; the Alliance took over on April 1, 2022). So awards from competitions <= 2022 go to
Compute Canada (F4320314000) and competitions >= 2023 to the Digital Research Alliance of Canada
(F4320331257). An RPP is a multi-year award under one App ID; it is routed by its application
year (the year in the stream label, e.g. "RPP 2020", "(2024 application renewal)", else the first
year it appears).

funder_award_id (runbook 2.1.1): papers cite the App ID with its stream, e.g. "RRG 4073",
"RPP 772", "RRG 1541 RAC 2021" (citation stubs under both funders), so we ship "RRG <id>" /
"RPP <id>". Fast Track applications are an RRG pathway ("Resources for Research Groups (Fast
Track)") and are cited as "RRG <id>" too. RPP App IDs persist across the RPP's years -> one award
per RPP id, allocations listed per year. RRG/FT App IDs are new each year but recycled after a
few years (2021 ids reappear in 2025/2026 for other PIs); since those always fall under different
funders the key is unique per funder, and the script RAISES on any within-funder collision.

amount: NULL (in-kind; the reports publish only per-unit "financial value" rates, not per-award
values). Allocations go into `allocation_text` (-> description).

Output: s3://openalex-ingest/awards/alliancecan_rac/alliancecan_rac_projects.parquet
"""

import argparse
import io
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# sys.stdout.reconfigure(...) + file-I/O utf-8 defaults; no-op on Linux/Databricks. Runbook §1.2.
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

    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open_utf8
# --- end shim ---

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/alliancecan_rac/alliancecan_rac_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}

# year -> (sheet id, gid, expected minimum rows)
SHEETS = {
    2021: ("1L-En7e2JFqTii2pDMcUka7ksNgArEGP9GXUq1ldaMGo", "0", 600),
    2022: ("17-2d3_rV62gvoKIPRMzgqgg-lE0RJMepjEU4PINnMmI", "0", 650),
    2023: ("1np9kRxAeksLUvVRsc_7oBSsYG5ZYMQSIMnw415eVaeA", "0", 650),
    2024: ("17V1H0_tjTHs-alXiOY1Un6gLPp2sE8q-rb_pvDMzSQA", "0", 600),
    2025: ("1fIgEq1KN1qu33qX77si9QRZRUKP7NzuOir8KvLkivFQ", "1312257149", 650),
    2026: ("1WB3SS75rKEgxST4zbACr3RfZm1LW872AvCcSTKCKPI4", "1660914536", 700),
}
COMPUTE_CANADA = "4320314000"
ALLIANCE = "4320331257"
LAST_COMPUTE_CANADA_YEAR = 2022

# resource column -> (label, unit); matched on the lower-cased header text
RESOURCES = [
    ("cpu", r"\bcpu\b", "CPU", "core-years"),
    ("gpu", r"^(?:20\d\d )?gpu|^gpu", "GPU", None),             # GPU-years (<=2023) / RGU-years (2024+)
    ("project_storage", r"project storage", "project storage", "TB"),
    ("nearline_storage", r"nearline", "nearline storage", "TB"),
    ("dcache_storage", r"dcache", "dCache storage", "TB"),
    ("volume_storage", r"volume and snapshot", "cloud volume/snapshot storage", "TB"),
    ("object_storage", r"object storage", "object storage", "TB"),
    ("shared_fs_storage", r"shared filesystem", "shared filesystem storage", "TB"),
    ("compute_cloud_vcpu", r"compute cloud.*vcpu", "compute-cloud vCPUs", None),
    ("compute_cloud_vgpu", r"compute cloud.*vgpu", "compute-cloud vGPUs", None),
    ("persistent_cloud_vcpu", r"persistent cloud.*vcpu", "persistent-cloud vCPUs", None),
]


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last = None
    for attempt in range(4):
        try:
            r = requests.get(url, headers=HEADERS, timeout=120)
            if r.status_code == 200:
                r.encoding = "utf-8"
                return r.text
            last = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last = e
        log(f"  retry {attempt + 1} for {url}: {last}")
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py) plus a leading honorific strip."""
    if not name:
        return None, None
    name = re.sub(r"^(?:(?:dr|prof|professor)\.?\s+)+", "", name.strip(), flags=re.I)
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


MOJIBAKE = re.compile("‚Ä|Œ|√|¬")   # MacRoman-decoded UTF-8 markers


def demojibake(s: str) -> str:
    """Some sheet cells (mostly RAC 2021) hold UTF-8 text that was decoded as MacRoman
    (e.g. "high‚Äêperformance" for "high-performance"). Reverse it only when
    the whole cell round-trips; otherwise keep the cell as published."""
    if not MOJIBAKE.search(s):
        return s
    try:
        return s.encode("mac_roman").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


def clean(v) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = re.sub(r"\s+", " ", demojibake(str(v)).replace("\xa0", " ")).strip()
    return s or None


def num(v) -> float | None:
    s = clean(v)
    if s is None:
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def fmt(x: float) -> str:
    return f"{x:,.0f}" if x == int(x) else f"{x:,.2f}".rstrip("0").rstrip(".")


def load_year(year: int) -> pd.DataFrame:
    sid, gid, min_rows = SHEETS[year]
    url = f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&gid={gid}"
    raw = get(url)
    df = pd.read_csv(io.StringIO(raw), dtype=str, header=None)
    hdr = next((i for i in range(6) if any(str(v).strip().lower() in ("app id", "application id")
                                           for v in df.iloc[i].values)), None)
    if hdr is None:
        raise SystemExit(f"RAC {year}: header row not found (sheet changed?)")
    cols = [str(c).strip() for c in df.iloc[hdr].values]
    d = df.iloc[hdr + 1:].copy()
    d.columns = cols
    d = d[d[[c for c in cols if c.lower() in ("app id", "application id")][0]].notna()]
    log(f"RAC {year}: {len(d)} rows ({url})")
    if len(d) < min_rows:
        raise SystemExit(f"RAC {year}: only {len(d)} rows (< {min_rows}); sheet changed?")

    def col(*names, required=True):
        for c in cols:
            if c.lower().strip() in names:
                return c
        if required:
            raise SystemExit(f"RAC {year}: none of {names} in {cols}")
        return None

    c_id = col("app id", "application id")
    c_proc = col("competition", "application process")
    c_pi = col("pi fullname", "applicant", "applicant's name", "principal investigator")
    c_title = col("project title")
    c_inst = col("institution")
    c_dept = col("department", required=False)
    c_area = col("research area", "general research area")
    c_spec = col("specific research area", required=False)
    c_summ = col("project summary", "proejct summary", required=False)
    res_cols = {}
    for key, pat, _, _ in RESOURCES:
        hits = [c for c in cols if re.search(pat, re.sub(r"^20\d\d ", "", c.lower()))
                and "allocat" in c.lower()]
        if key == "gpu":
            hits = [c for c in hits if "cloud" not in c.lower()]
        if key == "cpu":
            hits = [c for c in hits if "cloud" not in c.lower() and "vcpu" not in c.lower()]
        if len(hits) > 1:
            raise SystemExit(f"RAC {year}: ambiguous columns for {key}: {hits}")
        res_cols[key] = (hits[0] if hits else None)
    gpu_unit = "RGU-years" if res_cols["gpu"] and "rgu" in res_cols["gpu"].lower() else "GPU-years"
    vgpu_unit = "RGU-years" if res_cols["compute_cloud_vgpu"] and "rgu" in res_cols["compute_cloud_vgpu"].lower() else "vGPU-years"
    vcpu_unit = "vCPU-years" if any(res_cols[k] and "year" in res_cols[k].lower()
                                    for k in ("compute_cloud_vcpu", "persistent_cloud_vcpu")) else "vCPUs"
    units = {"gpu": gpu_unit, "compute_cloud_vgpu": vgpu_unit, "compute_cloud_vcpu": vcpu_unit,
             "persistent_cloud_vcpu": vcpu_unit}

    out = []
    for _, r in d.iterrows():
        proc = clean(r[c_proc]) or ""
        pl = proc.lower()
        stream = "FT" if "fast" in pl else ("RPP" if ("rpp" in pl or "platform" in pl) else "RRG")
        alloc = {}
        for key, _, label, unit in RESOURCES:
            c = res_cols[key]
            v = num(r[c]) if c else None
            if v:
                alloc[key] = {"value": v, "label": label, "unit": units.get(key, unit)}
        out.append({
            "year": year, "app_id": str(int(float(clean(r[c_id])))), "stream": stream, "application_process": proc,
            # one 2024 row carries its App ID in the applicant column ("2731"): no PI rather than a number
            "pi_name": None if re.fullmatch(r"[\d\s.,]+", clean(r[c_pi]) or "0") else clean(r[c_pi]),
            "title": clean(r[c_title]), "institution": clean(r[c_inst]),
            "department": clean(r[c_dept]) if c_dept else None, "research_area": clean(r[c_area]),
            "specific_research_area": clean(r[c_spec]) if c_spec else None,
            "summary": clean(r[c_summ]) if c_summ else None, "alloc": alloc,
            "sheet_url": f"https://docs.google.com/spreadsheets/d/{sid}/edit#gid={gid}",
        })
    return pd.DataFrame(out)


def app_year(stream: str, labels: list[str], years: list[int]) -> int:
    if stream != "RPP":
        return min(years)
    yrs = [min(years)]
    for lab in labels:
        m = re.search(r"\((20\d\d)\)|RPP (20\d\d)|(20\d\d) application renewal", lab)
        if m:
            yrs.append(int(next(g for g in m.groups() if g)))
    return min(yrs)


def alloc_text(year: int, alloc: dict) -> str:
    if not alloc:
        return f"RAC {year}: no resources listed"
    return f"RAC {year}: " + "; ".join(f"{a['label']} {fmt(a['value'])} {a['unit']}" for a in alloc.values())


def main() -> None:
    ap = argparse.ArgumentParser(description="RAC in-kind compute/storage allocations -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None, help="keep only the first N awards (smoke test)")
    ap.add_argument("--years", type=str, default=None, help="comma-separated subset of years")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    args = ap.parse_args()

    years = [int(y) for y in args.years.split(",")] if args.years else sorted(SHEETS)
    rows = pd.concat([load_year(y) for y in years], ignore_index=True)
    rows["grp"] = rows["stream"].map({"FT": "RRG", "RRG": "RRG", "RPP": "RPP"})
    # RRG / Fast Track: one award per (year, id). RPP: one award per id across its years.
    rows["award_key"] = rows.apply(lambda r: f"RPP {r.app_id}" if r.grp == "RPP" else f"RRG {r.app_id}@{r.year}", axis=1)

    awards = []
    for key, g in rows.sort_values("year").groupby("award_key", sort=False):
        g = g.sort_values("year")
        last = g.iloc[-1]
        yrs = g["year"].tolist()
        ay = app_year(last.stream if last.grp == "RRG" else "RPP", g["application_process"].tolist(), yrs)
        funder_id = COMPUTE_CANADA if ay <= LAST_COMPUTE_CANADA_YEAR else ALLIANCE
        given, family = split_name(last.pi_name if isinstance(last.pi_name, str) else "")
        streams = set(g["stream"])
        if last.grp == "RPP":
            scheme = "Resource Allocation Competition (RAC): Research Platforms and Portals (RPP)"
        elif streams == {"FT"}:
            scheme = "Resource Allocation Competition (RAC): Resources for Research Groups (RRG), Fast Track"
        else:
            scheme = "Resource Allocation Competition (RAC): Resources for Research Groups (RRG)"
        summary = next((s for s in reversed(g["summary"].tolist()) if s), None)
        atext = " | ".join(alloc_text(y, a) for y, a in zip(g["year"], g["alloc"]))
        alloc_json = json.dumps({str(y): {k: v["value"] for k, v in a.items()} for y, a in zip(g["year"], g["alloc"])})
        awards.append({
            "funder_award_id": f"{last.grp} {last.app_id}",
            "app_id": last.app_id, "stream": "/".join(sorted(streams)), "funder_scheme": scheme,
            "funder_id": funder_id, "funder_org": "Compute Canada" if funder_id == COMPUTE_CANADA else "Digital Research Alliance of Canada",
            "application_year": str(ay), "allocation_years": ";".join(str(y) for y in yrs),
            "first_year": str(min(yrs)), "last_year": str(max(yrs)),
            "application_process": " | ".join(dict.fromkeys(g["application_process"])),
            "title": last.title, "summary": summary,
            "pi_name": last.pi_name, "lead_given_name": given, "lead_family_name": family,
            "pi_names_all": " | ".join(dict.fromkeys(p for p in g["pi_name"] if isinstance(p, str) and p)),
            "institution": last.institution, "department": last.department,
            "research_area": last.research_area, "specific_research_area": last.specific_research_area,
            "allocation_text": atext, "allocations_json": alloc_json,
            "landing_page_url": last.sheet_url,
        })
    df = pd.DataFrame(awards)
    dup = df.duplicated(["funder_id", "funder_award_id"], keep=False)
    if dup.any():
        raise SystemExit(f"within-funder funder_award_id collision:\n{df[dup].sort_values('funder_award_id').head(20)}")
    if args.limit:
        df = df.head(args.limit)

    log(f"{len(df)} awards from {len(rows)} sheet rows; by funder: {df['funder_org'].value_counts().to_dict()}")
    log(f"  by scheme: {df['funder_scheme'].str.replace('Resource Allocation Competition (RAC): ', '').value_counts().to_dict()}")
    for c in ["title", "summary", "pi_name", "lead_family_name", "institution"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")  # runbook §1.2 item 5
    args.output_dir.mkdir(parents=True, exist_ok=True)
    path = args.output_dir / "alliancecan_rac_projects.parquet"
    df.to_parquet(path, index=False)
    log(f"Wrote {len(df)} rows to {path}")
    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    prev = args.output_dir / "_previous_alliancecan_rac_projects.parquet"
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(prev))
        n = len(pd.read_parquet(prev))
        log(f"Shrink check: previous {n}, new {len(df)}")
        if len(df) < n and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({n} -> {len(df)})")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    s3.upload_file(str(path), S3_BUCKET, S3_KEY)
    log(f"Uploaded s3://{S3_BUCKET}/{S3_KEY}")


if __name__ == "__main__":
    main()
