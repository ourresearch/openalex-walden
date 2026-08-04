#!/usr/bin/env python3
"""
CAPES DRI international scholarships to S3 (citable process-number registry)
============================================================================

Harvests CAPES' "[2017 a 2025] Bolsistas dos Programas da Diretoria de
Relacoes Internacionais (DRI)" open-data dataset (CKAN, method-1) and builds
a per-process-number award parquet.

Why this dataset (probe 2026-08-03, all 93 CKAN datasets swept):
- It is the ONLY CAPES open-data source at individual-grant level whose rows
  carry the canonical citable process number (`ID_PROCESSO`,
  `8888d.dddddd/yyyy-dd`) — the exact id publishers deposit in Crossref
  funding metadata (measured: 23% of process-format crossref deposits under
  CAPES match this registry; the remainder are domestic-program numbers not
  present anywhere in CAPES open data).
- The existing `capes_cooperacao_internacional` source (SCBA cooperation
  projects) has NO native id — its rows mint synthetic hashes and can never
  match a deposit. This dataset complements it; it does not replace it.
- Covers PrInt, PDSE, BRAFITEC, COFECUB, Ciencia sem Fronteiras, MARCA,
  CAPES-Humboldt etc. — precisely the programs acknowledged in papers.

Two source schemas:
- 2017–2019 file: AN_INICIO/ME_INICIO + AN_FIM/ME_FIM (year/month), origin
  IES named `NM_IES_ORIGEM_PRINCIPAL_DA`.
- 2020+ files: DT_INICIO/DT_TERMINO as `ddMMMyyyy` (e.g. `01DEC2019`),
  origin IES `NM_IES_ORIGEM`.
Rows repeat per benefit line/period → dedupe to ONE row per ID_PROCESSO:
earliest start, latest end, VL_TOTAL_RECEBIDO_MOEDA summed when every line
shares one currency (else amount NULL), first beneficiary/program.

Output columns follow the gen_awards_nb.py all-string contract:
funder_award_id, title, description, amount, currency, institution,
pi_given, pi_family, scheme, start_date_raw (yyyy-MM-dd), end_date_raw,
landing_page_url, country.

Output: s3://openalex-ingest/awards/capes_dri/capes_dri_grants.parquet

Usage:
    python capes_dri_to_s3.py [--output-dir DIR] [--skip-upload]
"""

import argparse, csv, io, json, os, re, sys, unicodedata
from datetime import datetime

import pandas as pd
import urllib.request

CKAN_PKG = ("https://dadosabertos.capes.gov.br/api/3/action/package_show"
            "?id=b4d8eff4-329c-4eee-8801-746e743df0d3")
S3_KEY = "awards/capes_dri/capes_dri_grants.parquet"
PROC = re.compile(r"^8888\d\.\d{6}/\d{4}-\d{2}$")
MONTHS = {m: i + 1 for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"])}
MIN_EXPECTED = 30000  # shrink guard: 2026-08-03 build produced 35,499 distinct processes


def fetch(url, timeout=120):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (openalex-awards)"})
    with urllib.request.urlopen(req, timeout=timeout) as f:
        return f.read()


def parse_date(row):
    """Return (start_iso, end_iso) from either schema, or (None, None) parts."""
    dt_i, dt_t = row.get("DT_INICIO"), row.get("DT_TERMINO")

    def conv(v):
        v = (v or "").strip().upper()
        m = re.match(r"^(\d{2})([A-Z]{3})(\d{4})$", v)
        if m and m.group(2) in MONTHS:
            return f"{m.group(3)}-{MONTHS[m.group(2)]:02d}-{m.group(1)}"
        return None
    if dt_i or dt_t:
        return conv(dt_i), conv(dt_t)
    def ym(y, mth):
        y, mth = (y or "").strip(), (mth or "").strip()
        if re.match(r"^\d{4}$", y):
            mm = mth if re.match(r"^\d{1,2}$", mth) and 1 <= int(mth) <= 12 else "1"
            return f"{y}-{int(mm):02d}-01"
        return None
    return ym(row.get("AN_INICIO"), row.get("ME_INICIO")), ym(row.get("AN_FIM"), row.get("ME_FIM"))


def split_name(full):
    parts = (full or "").strip().split()
    if not parts:
        return None, None
    if len(parts) == 1:
        return None, parts[0]  # unsplittable -> family per runbook checklist
    return parts[0], " ".join(parts[1:])


def money(v):
    v = (v or "").strip().replace(".", "").replace(",", ".") if v else ""
    try:
        f = float(v)
        return f if f > 0 else None
    except ValueError:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default=".")
    ap.add_argument("--skip-upload", action="store_true")
    a = ap.parse_args()

    pkg = json.loads(fetch(CKAN_PKG, 40))
    csvs = [r for r in pkg["result"]["resources"] if (r.get("format") or "").upper() == "CSV"]
    print(f"{len(csvs)} CSV resources in the DRI dataset")

    grants = {}
    for r in csvs:
        raw = fetch(r["url"]).decode("latin-1", errors="replace")
        rd = csv.DictReader(io.StringIO(raw), delimiter=";")
        n = kept = 0
        for row in rd:
            n += 1
            p = (row.get("ID_PROCESSO") or "").strip()
            if not PROC.match(p):
                continue
            kept += 1
            sd, ed = parse_date(row)
            cur = (row.get("CD_MOEDA") or "").strip().upper() or None
            amt = money(row.get("VL_TOTAL_RECEBIDO_MOEDA"))
            g = grants.get(p)
            if g is None:
                given, family = split_name(row.get("NM_BENEFICIARIO"))
                inst = (row.get("NM_IES_ORIGEM") or row.get("NM_IES_ORIGEM_PRINCIPAL_DA") or "").strip() or None
                prog = (row.get("NM_PROGRAMA") or "").strip() or None
                level = (row.get("NM_NIVEL") or "").strip()
                yr = (sd or "")[:4]
                title = f"CAPES {prog or 'DRI'} {level or 'scholarship'} for {(row.get('NM_BENEFICIARIO') or '').strip()}"
                if yr:
                    title += f" ({yr})"
                grants[p] = {
                    "funder_award_id": p, "title": title, "description": None,
                    "amounts": [], "currencies": set(),
                    "institution": inst, "pi_given": given, "pi_family": family,
                    "scheme": prog, "starts": [], "ends": [],
                    "landing_page_url": None, "country": "Brazil",
                }
                g = grants[p]
            if sd: g["starts"].append(sd)
            if ed: g["ends"].append(ed)
            if amt is not None and cur:
                g["amounts"].append(amt); g["currencies"].add(cur)
        print(f"  {r['name'][:60]}: rows={n} process-format={kept}")

    rows = []
    for g in grants.values():
        amount = None; currency = None
        if g["amounts"] and len(g["currencies"]) == 1:
            amount = f"{sum(g['amounts']):.2f}"; currency = next(iter(g["currencies"]))
        rows.append({
            "funder_award_id": g["funder_award_id"], "title": g["title"],
            "description": g["description"], "amount": amount, "currency": currency,
            "institution": g["institution"], "pi_given": g["pi_given"],
            "pi_family": g["pi_family"], "scheme": g["scheme"],
            "start_date_raw": min(g["starts"]) if g["starts"] else None,
            "end_date_raw": max(g["ends"]) if g["ends"] else None,
            "landing_page_url": g["landing_page_url"], "country": g["country"],
        })
    df = pd.DataFrame(rows).astype("string")
    assert df["funder_award_id"].is_unique, "duplicate process numbers after dedupe"
    if len(df) < MIN_EXPECTED:
        raise SystemExit(f"SHRINK GUARD: {len(df)} rows < {MIN_EXPECTED} expected minimum")
    out = os.path.join(a.output_dir, "capes_dri_grants.parquet")
    df.to_parquet(out, index=False)
    cov = {c: f"{df[c].notna().mean() * 100:.1f}%" for c in df.columns}
    print(f"wrote {out}: {len(df)} rows; coverage {cov}")

    if not a.skip_upload:
        import boto3
        boto3.client("s3").upload_file(out, "openalex-ingest", S3_KEY)
        print(f"uploaded s3://openalex-ingest/{S3_KEY}")


if __name__ == "__main__":
    main()
