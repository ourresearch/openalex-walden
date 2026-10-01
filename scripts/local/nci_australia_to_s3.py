#!/usr/bin/env python3
"""
National Computational Infrastructure (NCI Australia) merit allocations to S3
============================================================================

NCI (F4320312169) awards IN-KIND computing time (kilo service units, kSU) on its Gadi supercomputer
through two published merit schemes:

  * NCMAS - National Computational Merit Allocation Scheme (run by NCI with Pawsey). Outcomes
    2021-2026 as a CSV export per year: https://my.nci.org.au/mancini/ncmas/<year>/outcomes/csv
    (Project ID "NCMAS-2026-4", Lead CI, Institution, Title, FOR codes, kSU per facility).
    Only projects with a Gadi (NCI) allocation are kept: a project allocated only on another
    facility (Pawsey Magnus/Setonix, Monash MASSIVE, UQ FlashLite) is that facility's allocation,
    not NCI's, and those facilities are not routed here (Pawsey is not an OpenAlex funder).
    Other-facility kSU of a mixed project are listed in the description for completeness.
  * ANUMAS - ANU Merit Allocation Scheme, the merit allocation of ANU's partner share of NCI.
    Outcomes 2020-2026 at https://anumas.nci.org.au/outcomes/<year>/ (NCI project code, Lead CI,
    ANU school, Gadi kSU; project titles only in 2020-2021).
Other partner shares (CSIRO, BoM, GA, university consortia) publish no per-project lists.
NCMAS <= 2019 (Raijin era, keyed by project code) exists only in the Wayback Machine and 2020 is
not retrievable, so it is left out.

robots.txt: nci.org.au disallows ClaudeBot/Claude-Web/anthropic-ai. my.nci.org.au and
anumas.nci.org.au serve no robots.txt (HTTP 404), which RFC 9309 treats as "no restrictions";
the www site is never fetched by this script.

funder_award_id (runbook 2.1.1): papers cite NCMAS proposals as "NCMAS-2024-59" (stubs under
F4320312169) and NCI projects by code ("Project y89", "project fk5"). So:
  * NCMAS: the Project ID with spaces removed ("NCMAS-2026-4"), one award per proposal-year.
  * ANUMAS: the NCI project code ("y89"), one award per code with its yearly allocations.
    A first-year project is listed as "NEW" (code not yet assigned): it is merged into the code
    that first appears the following year under the same lead CI when that match is unique, else
    kept as "ANUMAS-<year>-NEW-<lead-slug>".

amount: NULL (in-kind). Allocations are spelled out in `allocation_text`.

Output: s3://openalex-ingest/awards/nci_australia/nci_australia_projects.parquet
"""

import argparse
import html
import io
import json
import re
import time
import unicodedata
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
S3_KEY = "awards/nci_australia/nci_australia_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
NCMAS_CSV = "https://my.nci.org.au/mancini/ncmas/{y}/outcomes/csv"
NCMAS_PAGE = "https://my.nci.org.au/mancini/ncmas/{y}/outcomes"
ANUMAS_PAGE = "https://anumas.nci.org.au/outcomes/{y}/"
NCMAS_YEARS = range(2021, 2027)
ANUMAS_YEARS = range(2020, 2027)
FACILITY = {"gadi": "NCI Gadi", "setonix": "Pawsey Setonix", "magnus": "Pawsey Magnus",
            "massive": "Monash MASSIVE", "flashlite": "UQ FlashLite"}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, ok404: bool = False) -> str | None:
    last = None
    for attempt in range(4):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            if r.status_code == 200:
                r.encoding = "utf-8"
                return r.text
            if r.status_code == 404 and ok404:
                return None
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
    name = re.sub(r"^(?:(?:dr|prof|professor|a/prof|assoc\. prof|associate professor)\.?\s+)+", "", name.strip(), flags=re.I)
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def clean(v) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = re.sub(r"\s+", " ", str(v).replace("\xa0", " ").replace("​", "")).strip()
    return s or None


def num(v) -> float | None:
    s = clean(v)
    if s is None or s in {"-", "--"}:
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def fmt(x: float) -> str:
    return f"{x:,.0f}" if x == int(x) else f"{x:,.1f}"


def person_key(name: str) -> str:
    n = re.sub(r"^(?:(?:dr|prof|professor)\.?\s+)+", "", name or "", flags=re.I)
    n = unicodedata.normalize("NFKD", n).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z]+", " ", n).strip()


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


# ---------------------------------------------------------------------------------------------
def ncmas() -> tuple[list[dict], dict]:
    out, dropped = [], {}
    for y in NCMAS_YEARS:
        raw = get(NCMAS_CSV.format(y=y))
        d = pd.read_csv(io.StringIO(raw), dtype=str)
        fac_cols = [c for c in d.columns if re.search(r"allocation ksu", c, re.I) and not c.lower().startswith("total")]
        if not any(c.lower().startswith("gadi") for c in fac_cols):
            raise SystemExit(f"NCMAS {y}: no Gadi column in {list(d.columns)}")
        for_col = next((c for c in d.columns if c.upper().startswith("FOR")), None)
        n_kept = 0
        for _, r in d.iterrows():
            alloc = {}
            for c in fac_cols:
                v = num(r[c])
                if v:
                    alloc[c.replace(" Allocation kSU", "").strip()] = v
            gadi = sum(v for k, v in alloc.items() if k.lower().startswith("gadi"))
            if not gadi:
                dropped[y] = dropped.get(y, 0) + 1
                continue
            pid = re.sub(r"\s+", "", clean(r["Project ID"]) or "")
            if not re.fullmatch(r"NCMAS-20\d\d-\d+", pid):
                raise SystemExit(f"NCMAS {y}: unexpected Project ID {r['Project ID']!r}")
            others = {k: v for k, v in alloc.items() if not k.lower().startswith("gadi")}
            text = f"NCMAS {y}: NCI Gadi {fmt(gadi)} kSU"
            if others:
                text += " (other NCMAS facilities for this project: " + "; ".join(
                    f"{'Pawsey ' if re.match(r'(setonix|magnus)', k, re.I) else ''}{k} {fmt(v)} kSU" for k, v in others.items()) + ")"
            fors = clean(r[for_col]) if for_col else None
            n_kept += 1
            out.append({
                "scheme": "NCMAS", "funder_award_id": pid, "years": [y], "lead_ci": clean(r["Lead CI"]),
                "institution": clean(r["Institution"]), "school": None, "title": clean(r["Title"]),
                "fields_of_research": "; ".join(x.strip() for x in fors.split("\n")) if fors else None,
                "gadi_ksu_by_year": {str(y): gadi}, "other_facilities_ksu": json.dumps(others) if others else None,
                "allocation_text": text, "landing_page_url": NCMAS_PAGE.format(y=y),
            })
        log(f"NCMAS {y}: {len(d)} projects, {n_kept} with a Gadi allocation, {dropped.get(y, 0)} other-facility only")
        if n_kept < 50:
            raise SystemExit(f"NCMAS {y}: only {n_kept} Gadi projects; format changed?")
        time.sleep(1)
    return out, dropped


def anumas_rows(y: int) -> list[dict]:
    page = get(ANUMAS_PAGE.format(y=y))
    tables = []
    for tb in re.findall(r"<table.*?</table>", page, re.S):
        rows = [[clean(html.unescape(re.sub(r"<[^>]+>", " ", c))) or "" for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", tr, re.S)]
                for tr in re.findall(r"<tr.*?</tr>", tb, re.S)]
        if rows and rows[0] and rows[0][0].lower() == "project code":
            tables.append(rows)
    if len(tables) != 1:
        raise SystemExit(f"ANUMAS {y}: expected one project table, found {len(tables)}")
    hdr = [h.lower() for h in tables[0][0]]
    i_code, i_ci, i_school = 0, 1, 2
    i_title = next((i for i, h in enumerate(hdr) if "title" in h), None)
    i_ksu = next(i for i, h in enumerate(hdr) if "ksu" in h)
    out = []
    for r in tables[0][1:]:
        if len(r) <= i_ksu or not r[i_ci] or r[i_ci].upper() == "TOTAL" or "total" in " ".join(r).lower():
            continue
        ksu = num(r[i_ksu])
        if not ksu:
            continue
        # codes are published as "x77", "X69", "xc17 (12720)", "pb82 (NEW)" or bare "NEW"
        m = re.match(r"^\s*([A-Za-z]{1,3}\d{1,3})\b", r[i_code])
        if m:
            code = m.group(1).lower()
        elif r[i_code].strip().upper() == "NEW":
            code = "NEW"
        else:
            raise SystemExit(f"ANUMAS {y}: unexpected project code {r[i_code]!r}")
        out.append({"year": y, "code": code, "code_as_published": r[i_code].strip(),
                    "lead_ci": r[i_ci], "school": r[i_school] or None,
                    "title": (r[i_title] or None) if i_title is not None else None, "ksu": ksu})
    log(f"ANUMAS {y}: {len(out)} allocations ({sum(1 for o in out if o['code'].upper() == 'NEW')} NEW)")
    if len(out) < 30:
        raise SystemExit(f"ANUMAS {y}: only {len(out)} rows; page changed?")
    return out


def anumas() -> tuple[list[dict], dict]:
    rows = []
    for y in ANUMAS_YEARS:
        rows += anumas_rows(y)
        time.sleep(1)
    coded = [r for r in rows if r["code"].upper() != "NEW"]
    first_year = {}
    for r in coded:
        first_year[r["code"]] = min(first_year.get(r["code"], 9999), r["year"])
    groups: dict[str, list[dict]] = {}
    for r in coded:
        groups.setdefault(r["code"], []).append(r)
    stats = {"new_rows": 0, "new_merged": 0, "new_kept_synthetic": 0}
    for r in rows:
        if r["code"].upper() != "NEW":
            continue
        stats["new_rows"] += 1
        cands = [c for c, fy in first_year.items() if fy == r["year"] + 1
                 and any(person_key(x["lead_ci"]) == person_key(r["lead_ci"]) for x in groups[c] if x["year"] == r["year"] + 1)]
        if len(cands) == 1:
            groups[cands[0]].append({**r, "code": cands[0], "was_new": True})
            stats["new_merged"] += 1
        else:
            key = f"ANUMAS-{r['year']}-NEW-{slug(re.sub(r'^(?:(?:dr|prof|professor)\.?\s+)+', '', r['lead_ci'], flags=re.I))}"
            groups.setdefault(key, []).append({**r, "code": key})
            stats["new_kept_synthetic"] += 1
    out = []
    for code, g in groups.items():
        g = sorted(g, key=lambda x: x["year"])
        last = g[-1]
        title = next((x["title"] for x in reversed(g) if x.get("title")), None)
        parts = [f"ANUMAS {x['year']}: NCI Gadi {fmt(x['ksu'])} kSU" + (" (as a new project)" if x.get("was_new") else "") for x in g]
        out.append({
            "scheme": "ANUMAS", "funder_award_id": code, "years": sorted({x["year"] for x in g}), "lead_ci": last["lead_ci"],
            "institution": "Australian National University", "school": last["school"], "title": title,
            "fields_of_research": None, "gadi_ksu_by_year": {str(x["year"]): x["ksu"] for x in g},
            "other_facilities_ksu": None, "allocation_text": "; ".join(parts),
            "landing_page_url": ANUMAS_PAGE.format(y=last["year"]),
        })
    if any(len({x["year"] for x in groups[c]}) != len(groups[c]) for c in groups):
        raise SystemExit("ANUMAS: a project code has two allocations in one year")
    log(f"ANUMAS: {len(out)} awards from {len(rows)} yearly allocations; NEW rows {stats}")
    return out, stats


def main() -> None:
    ap = argparse.ArgumentParser(description="NCI Australia merit allocations (NCMAS + ANUMAS) -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None, help="keep only the first N awards (smoke test)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    args = ap.parse_args()

    nc, dropped = ncmas()
    an, stats = anumas()
    log(f"NCMAS other-facility-only projects not routed to NCI: {dropped} (total {sum(dropped.values())})")
    awards = nc + an
    for a in awards:
        a["lead_given_name"], a["lead_family_name"] = split_name(a["lead_ci"] or "")
        a["first_year"], a["last_year"] = str(min(a["years"])), str(max(a["years"]))
        a["allocation_years"] = ";".join(str(y) for y in a["years"])
        a["gadi_ksu_by_year"] = json.dumps(a["gadi_ksu_by_year"])
        del a["years"]
    df = pd.DataFrame(awards)
    if df["funder_award_id"].duplicated().any():
        raise SystemExit(f"duplicate funder_award_id: {df[df['funder_award_id'].duplicated(keep=False)]}")
    if args.limit:
        df = df.groupby("scheme", group_keys=False).head(args.limit)
    log(f"{len(df)} awards; by scheme {df['scheme'].value_counts().to_dict()}")
    for c in ["title", "lead_ci", "lead_family_name", "institution"]:
        log(f"  {c:16s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")  # runbook §1.2 item 5
    args.output_dir.mkdir(parents=True, exist_ok=True)
    path = args.output_dir / "nci_australia_projects.parquet"
    df.to_parquet(path, index=False)
    log(f"Wrote {len(df)} rows to {path}")
    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    prev = args.output_dir / "_previous_nci_australia_projects.parquet"
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
