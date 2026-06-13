#!/usr/bin/env python3
"""
Health and Medical Research Fund (Hong Kong) to S3 Data Pipeline
================================================================

Harvests the HK Health Bureau Research Fund Secretariat funded-project database
and uploads a parquet to S3 for Databricks ingestion.

Data source: first-party REST API behind the rfs Angular SPA
(https://rfs1.healthbureau.gov.hk/search/#/fundedsearch/basicsearch):

    GET  /app/rest/fundedsearch/generateCsrfToken
         -> token in the X-XSRF-REQUEST-TOKEN *response header* (echo it back
            as the X-XSRF-REQUEST-TOKEN request header on every POST)
    POST /app/rest/fundedsearch/quickSearch
         {"searchText":"","researchType":"all","pageNo":0,"pageSize":N,
          "sortBy":"refNo","sortOrder":"asc"}
         -> {"projectList":[{fpID,refNo,fund,...}], "resultCount": 3966}
    POST /app/rest/fundedsearch/getProjectDetailByFPID   (body = raw fpID int)
         -> refNo, projectTitle, callYear, approvedAmount (HKD), fund, status,
            actCode, heaCat, abstractProposal/abstractFinal,
            applicantsList (PIs, ordered), affiliationsList (ordered)

The fund covers HMRF proper plus the predecessor funds folded into it in 2011
(RFCID, HHSRF, HCPF) — the `fund` column carries the source label so the
notebook can map/annotate. Verified live 2026-06-12: resultCount=3966,
pageSize=1000 accepted.

Output: s3://openalex-ingest/awards/hmrf/hmrf_projects.parquet

Usage:
    python hmrf_to_s3.py                  # full harvest (~3,966 detail calls)
    python hmrf_to_s3.py --limit 5        # smoke test
    python hmrf_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

HOST = "https://rfs2.healthbureau.gov.hk"
CSRF_URL = HOST + "/app/rest/fundedsearch/generateCsrfToken"
SEARCH_URL = HOST + "/app/rest/fundedsearch/quickSearch"
DETAIL_URL = HOST + "/app/rest/fundedsearch/getProjectDetailByFPID"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/hmrf/hmrf_projects.parquet"
PAGE_SIZE = 1000
EXPECTED_MIN = 3500          # live resultCount 2026-06-12 was 3,966
DETAIL_SLEEP = 0.25
MAX_CONSECUTIVE_FAIL = 5


def clean(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


_HK_TITLE_RE = re.compile(r"^(?:Dr|Prof|Professor|Mr|Mrs|Ms|Miss|Mx)\.?\s+", re.I)


def split_hk_name(raw):
    """Split an HMRF applicant name into (given, family).

    HMRF applicant names are **family-name-first** (HK / Chinese convention),
    in several shapes seen in the source:
        'Wong RNS'            -> given 'RNS',            family 'Wong'   (initials)
        'Dr Jin Dong-yan'     -> given 'Dong-yan',       family 'Jin'
        'Kwan Patrick Kwok L' -> given 'Patrick Kwok L', family 'Kwan'
        'WONG Lawrence K.S.'  -> given 'Lawrence K.S.',  family 'WONG'   (caps surname)
        'SOO Yannie, Oi Yan'  -> given 'Yannie',         family 'SOO'
    Rule: strip any leading title, take the part before the first comma, then
    treat a leading ALL-CAPS run as the (explicit) surname; otherwise the first
    whitespace token is the surname and the remainder is the given name/initials.
    This is the inverse of the Western last-token-is-family split, and is correct
    for the dominant family-first convention in this source.
    """
    if not raw:
        return None, None
    n = _HK_TITLE_RE.sub("", str(raw)).strip()
    n = n.split(",")[0].strip()
    tokens = n.split()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    # Leading ALL-CAPS run (len>1) flags an explicitly-marked surname.
    if tokens[0].isupper() and tokens[0].isalpha() and len(tokens[0]) > 1 \
            and not (len(tokens) >= 2 and tokens[1].isupper()):
        return " ".join(tokens[1:]), tokens[0]
    # Default HK family-first: first token = family, rest = given/initials.
    return " ".join(tokens[1:]), tokens[0]


class RfsClient:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        self.token = None
        self.refresh_token()

    def refresh_token(self):
        r = self.s.get(CSRF_URL, timeout=30)
        r.raise_for_status()
        self.token = r.headers.get("X-XSRF-REQUEST-TOKEN")
        if not self.token:
            raise RuntimeError("generateCsrfToken returned no X-XSRF-REQUEST-TOKEN header")

    def post(self, url, payload):
        r = self.s.post(url, json=payload, timeout=120,
                        headers={"X-XSRF-REQUEST-TOKEN": self.token})
        if r.status_code in (401, 403, 419):
            print(f"  HTTP {r.status_code} -> refreshing CSRF token and retrying")
            self.refresh_token()
            r = self.s.post(url, json=payload, timeout=120,
                            headers={"X-XSRF-REQUEST-TOKEN": self.token})
        return r


def fetch_project_list(client):
    """Page through quickSearch; resultCount is the authoritative terminator."""
    projects, page = [], 0
    result_count = None
    consecutive_fail = 0
    while True:
        payload = {"searchText": "", "researchType": "all", "pageNo": page,
                   "pageSize": PAGE_SIZE, "sortBy": "refNo", "sortOrder": "asc"}
        r = client.post(SEARCH_URL, payload)
        if r.status_code != 200:
            consecutive_fail += 1
            print(f"  page {page}: HTTP {r.status_code} "
                  f"({consecutive_fail}/{MAX_CONSECUTIVE_FAIL}); continuing")
            if consecutive_fail >= MAX_CONSECUTIVE_FAIL:
                raise RuntimeError(f"quickSearch page {page}: {MAX_CONSECUTIVE_FAIL} "
                                   "consecutive non-200s — refusing to silently truncate")
            time.sleep(3)
            continue
        consecutive_fail = 0
        d = r.json()
        if result_count is None:
            result_count = d.get("resultCount")
            print(f"  source reports resultCount={result_count}")
        batch = d.get("projectList") or []
        projects.extend(batch)
        print(f"  page {page}: +{len(batch)} ({len(projects)}/{result_count})")
        if len(projects) >= (result_count or 0) or not batch:
            break
        page += 1
        time.sleep(0.5)
    return projects, result_count


def parse_detail(d):
    applicants = sorted(d.get("applicantsList") or [],
                        key=lambda a: a.get("affiliationOrder") or 0)
    affiliations = sorted(d.get("affiliationsList") or [],
                          key=lambda a: a.get("displayOrder") or 0)
    lead_name = clean(applicants[0].get("enTitle")) if applicants else None
    lead_given, lead_family = split_hk_name(lead_name)
    lead_affiliation = clean(affiliations[0].get("enTitle")) if affiliations else None
    return {
        "fp_id": clean(d.get("fpID")),
        "ref_no": clean(d.get("refNo")),
        "fund": clean(d.get("fund")),
        "status": clean(d.get("status")),
        "call_year": clean(d.get("callYear")),
        "project_title": clean(d.get("projectTitle")),
        "activity_code": clean(d.get("actCode")),
        "health_category": clean(d.get("heaCat")),
        "specialty": clean(d.get("specialty")),
        "approved_amount_hkd": clean(d.get("approvedAmount")),
        "abstract_proposal": clean(d.get("abstractProposal")),
        "abstract_final": clean(d.get("abstractFinal")),
        "remarks": clean(d.get("remarks")),
        "lead_name": lead_name,
        "lead_given_name": lead_given,
        "lead_family_name": lead_family,
        "lead_affiliation": lead_affiliation,
        "applicants": json.dumps(
            [{"name": clean(a.get("enTitle")), "order": a.get("affiliationOrder")}
             for a in applicants if clean(a.get("enTitle"))],
            ensure_ascii=False),
        "affiliations": json.dumps(
            [{"name": clean(a.get("enTitle")), "order": a.get("displayOrder")}
             for a in affiliations if clean(a.get("enTitle"))],
            ensure_ascii=False),
        "final_report_count": clean(len(d.get("finalRptList") or [])),
        "diss_report_count": clean(len(d.get("dissRptList") or [])),
    }


def upload_to_s3(local_path: Path, bucket: str, key: str) -> bool:
    s3_uri = f"s3://{bucket}/{key}"
    print(f"\nUploading to {s3_uri}...")
    try:
        subprocess.run(["aws", "s3", "cp", str(local_path), s3_uri],
                       capture_output=True, text=True, check=True)
        print(f"Upload complete: {s3_uri}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Upload failed: {e.stderr}")
        return False


def main():
    ap = argparse.ArgumentParser(description="HMRF (Hong Kong) funded projects to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/hmrf_data"))
    ap.add_argument("--limit", type=int, default=None,
                    help="smoke test: only fetch N project details")
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = args.output_dir / "hmrf_details.jsonl"

    print("=" * 60)
    print("Health and Medical Research Fund (HK) -> S3")
    print("=" * 60)

    client = RfsClient()
    print("\nStep A: paging quickSearch list...")
    projects, result_count = fetch_project_list(client)
    fp_ids = [p["fpID"] for p in projects if p.get("fpID") is not None]
    print(f"List complete: {len(fp_ids)} fpIDs (resultCount={result_count})")

    done = {}
    if checkpoint.exists():
        with open(checkpoint, encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                done[rec["fpID"]] = rec
        print(f"Checkpoint: {len(done)} details already fetched, resuming")

    todo = [i for i in fp_ids if i not in done]
    if args.limit:
        todo = todo[: max(0, args.limit - len(done))]
        print(f"--limit {args.limit}: fetching {len(todo)} details this run")

    print(f"\nStep B: fetching {len(todo)} project details...")
    t0 = time.time()
    consecutive_fail = 0
    with open(checkpoint, "a", encoding="utf-8") as ck:
        for n, fpid in enumerate(todo, 1):
            r = client.post(DETAIL_URL, fpid)
            if r.status_code != 200:
                consecutive_fail += 1
                print(f"  fpID {fpid}: HTTP {r.status_code} "
                      f"({consecutive_fail}/{MAX_CONSECUTIVE_FAIL}); continuing")
                if consecutive_fail >= MAX_CONSECUTIVE_FAIL:
                    raise RuntimeError("too many consecutive detail failures")
                time.sleep(3)
                continue
            consecutive_fail = 0
            d = r.json()
            d["fpID"] = fpid
            done[fpid] = d
            ck.write(json.dumps(d, ensure_ascii=False) + "\n")
            if n % 100 == 0 or n == len(todo):
                ck.flush()
                rate = n / max(time.time() - t0, 1)
                eta_min = (len(todo) - n) / max(rate, 0.01) / 60
                print(f"  [{time.strftime('%H:%M:%S')}] {n}/{len(todo)} details "
                      f"({len(done)} total) - ETA {eta_min:.0f}m")
            time.sleep(DETAIL_SLEEP)

    rows = [parse_detail(d) for d in done.values()]
    rows = [r for r in rows if r["ref_no"]]
    if not args.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} projects — expected ~{result_count}; "
              "refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("project_title", "call_year", "approved_amount_hkd", "applicants",
              "affiliations", "health_category"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / len(df))}%)")
    print(f"  dupes on ref_no: {df.ref_no.duplicated().sum()}")
    out = args.output_dir / "hmrf_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if args.limit:
        print("\nSmoke run complete (no upload with --limit).")
        return
    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
