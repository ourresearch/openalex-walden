#!/usr/bin/env python3
"""
NCN (Narodowe Centrum Nauki, Poland) to S3 — RAD-on source pivot
================================================================

SOURCE PIVOT (2026-08-03, oxjob #690 citable-ref gap): the original NCN
ingest (scripts/local/ncn_to_s3.py, dane.gov.pl ranking-list XLSX) carries
NO citable id — its `project_id` is a synthetic `{edition}_{COMPETITION}_{row}`
code, so crossref-deposited NCN ids (UMO registration numbers like
`UMO-2018/29/B/HS1/02676`, ~24.5k plausible deposits) can never match the
registry. RAD-on (radon.nauka.gov.pl — POLON, the national research registry;
same source and same open API as the approved NCBR pivot, see
scripts/local/ncbr_to_s3.py) exposes `projectNumber` = the UMO number, plus
titles (PL/EN), abstracts, ISO start/end dates, PLN amounts, PIs, and
institutions — all absent from the ranking lists.

Filter: projectNumber matches the UMO chassis
`^\\d{4}/\\d{2}/[A-Z]{1,2}/[A-Z]{2,3}\\d{1,2}/\\d{5}$` (NCN-specific format;
belt-and-braces with financingInstitutions name-match on "Narodowe Centrum
Nauki" when the field is present). Caveat shared with NCBR: POLON is
institution-reported, so a small tail of projects may be missing or
duplicated across reporting entities — dedupe keeps one row per UMO number
(max nationalFunds).

Output columns follow the gen_awards_nb.py all-string contract.
Output: s3://openalex-ingest/awards/ncn/ncn_grants.parquet
  (NEW key — the legacy ncn_projects.parquet is left untouched until the
   pivot is approved and CreateNCNAwards switches over.)

Usage:
    python ncn_radon_to_s3.py [--output-dir DIR] [--from-jsonl FILE]
                              [--skip-upload]
"""

import argparse, json, os, re, time, urllib.request

import pandas as pd

BASE = "https://radon.nauka.gov.pl/opendata/polon/projects?resultNumbers=100"
S3_KEY = "awards/ncn/ncn_grants.parquet"
UMO = re.compile(r"^\d{4}/\d{2}/[A-Z]{1,2}/[A-Z]{2,3}\d{1,2}/\d{5}$")
MIN_EXPECTED = 12000  # shrink guard (2026-08-03 build: see tracker row)


def get(url, tries=4):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "openalex-awards/1.0"})
            with urllib.request.urlopen(req, timeout=60) as f:
                return json.load(f)
        except Exception:
            time.sleep(3 * (i + 1))
            if i == tries - 1:
                raise


def pull():
    token, page, kept, total = None, 0, [], 0
    while True:
        d = get(BASE + (f"&token={token}" if token else ""))
        rs = d.get("results", [])
        total += len(rs)
        kept += [r for r in rs if UMO.match((r.get("projectNumber") or "").strip())]
        page += 1
        if page % 50 == 0:
            print(f"page {page}: scanned {total}, kept {len(kept)}", flush=True)
        tok = (d.get("pagination") or {}).get("token")
        if not rs or not tok or tok == token:
            break
        token = tok
        time.sleep(0.15)
    print(f"DONE: pages {page}, scanned {total}, NCN(UMO) kept {len(kept)}")
    return kept


def first_manager(rec):
    for m in rec.get("projectManagers") or []:
        given = (m.get("firstName") or "").strip() or None
        family = (m.get("lastName") or "").strip() or None
        if given or family:
            return given, family
    return None, None


def first_institution(rec):
    for it in rec.get("implementingInstitutions") or []:
        n = (it.get("institutionName") or "").strip()
        if n:
            return n
    return (rec.get("entityShowingAchievementsName") or "").strip() or None


def to_row(rec):
    title = (rec.get("projectTitleEn") or "").strip() or (rec.get("projectTitlePl") or "").strip() or None
    desc = (rec.get("projectAbstractEn") or "").strip() or (rec.get("projectAbstractPl") or "").strip() or None
    if desc and desc.lower() == "none":
        desc = None
    if title and title.lower() == "none":
        title = (rec.get("projectTitlePl") or "").strip() or None
    given, family = first_manager(rec)
    funds = (str(rec.get("nationalFunds")) if rec.get("nationalFunds") is not None else "").strip()
    amount = funds if re.match(r"^\d+(\.\d+)?$", funds) and float(funds) > 0 else None
    return {
        "funder_award_id": rec["projectNumber"].strip(),
        "title": title, "description": desc,
        "amount": amount, "currency": "PLN" if amount else None,
        "institution": first_institution(rec),
        "pi_given": given, "pi_family": family,
        "scheme": None,
        "start_date_raw": (rec.get("projectStartDate") or "").strip() or None,
        "end_date_raw": (rec.get("projectEndDate") or "").strip() or None,
        "landing_page_url": None, "country": "Poland",
        "_funds_num": float(funds) if amount else 0.0,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default=".")
    ap.add_argument("--from-jsonl", help="reuse a previous raw pull instead of re-crawling")
    ap.add_argument("--skip-upload", action="store_true")
    a = ap.parse_args()

    if a.from_jsonl:
        recs = [json.loads(l) for l in open(a.from_jsonl)]
        recs = [r for r in recs if UMO.match((r.get("projectNumber") or "").strip())]
        print(f"loaded {len(recs)} records from {a.from_jsonl}")
    else:
        recs = pull()

    rows = {}
    for rec in recs:
        r = to_row(rec)
        k = r["funder_award_id"]
        if k not in rows or r["_funds_num"] > rows[k]["_funds_num"]:
            rows[k] = r
    for r in rows.values():
        r.pop("_funds_num")
    df = pd.DataFrame(list(rows.values())).astype("string")
    assert df["funder_award_id"].is_unique
    if len(df) < MIN_EXPECTED:
        raise SystemExit(f"SHRINK GUARD: {len(df)} rows < {MIN_EXPECTED}")
    out = os.path.join(a.output_dir, "ncn_grants.parquet")
    df.to_parquet(out, index=False)
    cov = {c: f"{df[c].notna().mean() * 100:.1f}%" for c in df.columns}
    print(f"wrote {out}: {len(df)} rows; coverage {cov}")

    if not a.skip_upload:
        import boto3
        boto3.client("s3").upload_file(out, "openalex-ingest", S3_KEY)
        print(f"uploaded s3://openalex-ingest/{S3_KEY}")


if __name__ == "__main__":
    main()
