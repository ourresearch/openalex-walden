#!/usr/bin/env python3
"""
KWF Kankerbestrijding (Dutch Cancer Society) to S3 Data Pipeline
================================================================

Pulls the KWF "Onderzoeksdatabase" (research database, all supported projects
from start-year 2017) and uploads a parquet to S3 for Databricks ingestion.
KWF is OpenAlex funder F4320322777 (11,186 works) with ~0 structured awards in
OpenAlex today — a high net-new target.

Data source: the public research database at
    https://www.kwf.nl/onderzoek/onderzoeksdatabase renders client-side from a
    **Solr** backend exposed (unauthenticated) at https://www.kwf.nl/odb_query.
    POST an application/x-www-form-urlencoded Solr query
    (q=*:* fq=index_id:(kwf_odb) wt=json) and it returns JSON docs. rows=9 is
    just the UI page size; rows>numFound pulls the whole corpus in one request.

    Per-doc fields used (Solr search_api field names, incl. the X3b=';' encoding):
      ss_field_project_number          -> native grant id (e.g. 18208)
      tm_X3b_nl_field_project_title    -> title (list, Dutch)
      twm_X3b_nl_field_project_leader  -> PI (list, title-prefixed "dr. ...")
      ss_project_institute             -> institution
      tm_X3b_nl_project_summary        -> abstract (list)  [ss_project_summary_raw = HTML]
      ds_field_project_start_date      -> start date (ISO)
      ss_project_primary_modality      -> research modality (-> funder_scheme)
      sm_project_disease_site_code_name-> cancer type (list)
      ss_url                           -> detail-page path
    NB: no funding AMOUNT is published in the database (§6.7 amount waiver).

Output: s3://openalex-ingest/awards/kwf/kwf_grants.parquet
"""

import argparse
import html
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
SOLR_URL = "https://www.kwf.nl/odb_query"
SOLR_BODY = ("q=*:*&fq=index_id:(kwf_odb)&sort=its_field_project_number_sort desc"
             "&rows={rows}&start=0&wt=json")
LANDING_BASE = "https://www.kwf.nl"
LANDING = "https://www.kwf.nl/onderzoek/onderzoeksdatabase"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/kwf/kwf_grants.parquet"

# leading Dutch/academic honorific tokens stripped off the PI name
_TITLE_TOKENS = {
    "dr", "dr.", "dr.ir.", "drs", "drs.", "ir", "ir.", "prof", "prof.",
    "prof.dr.", "prof.dr.ir.", "mr", "mr.", "mw", "mw.", "dhr", "dhr.", "em",
    "em.", "de", "ing", "ing.", "bsc", "msc", "phd",
}
_NULLISH = {"", "nan", "none", "n/a", "na", "-", "onbekend"}
# Dutch surname particles (tussenvoegsel) — belong to the FAMILY name
_DUTCH_PARTICLES = {"van", "de", "der", "den", "ten", "ter", "te", "het",
                    "'t", "op", "von", "du", "vande", "vander"}


def first(v):
    if isinstance(v, list):
        return v[0] if v else None
    return v


def clean(v):
    if v is None:
        return None
    s = html.unescape(re.sub(r"\s+", " ", str(v)).strip())
    return None if s.lower() in _NULLISH else s


def strip_html(v):
    # Solr text fields are multi-value lists (headings + paragraphs); JOIN them
    # — taking only the first item truncates the abstract to its first heading.
    s = " ".join(str(x) for x in v if x) if isinstance(v, list) else v
    if not s:
        return None
    s = re.sub(r"<[^>]+>", " ", str(s))
    return clean(s)


def parse_pi(raw):
    name = clean(first(raw))
    if not name:
        return None, None
    toks = name.split()
    while toks and toks[0].lower().strip(".,") in {t.strip(".") for t in _TITLE_TOKENS}:
        toks.pop(0)
    if not toks or not re.search(r"[A-Za-zÀ-ÿ]{2}", " ".join(toks)):
        return None, None
    if len(toks) < 2:
        return None, toks[0]
    # family name starts at the first tussenvoegsel: "Maaike van der Aa" ->
    # given "Maaike", family "van der Aa"
    for i in range(1, len(toks)):
        if toks[i].lower().strip(".") in _DUTCH_PARTICLES:
            return " ".join(toks[:i]), " ".join(toks[i:])
    return " ".join(toks[:-1]), toks[-1]


def parse_date(v):
    s = clean(first(v))
    if not s:
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    return m.group(1) if m else None


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


def fetch_docs() -> list:
    print(f"POST Solr query to {SOLR_URL} ...")
    last = None
    for attempt in range(4):
        try:
            r = requests.post(SOLR_URL, data=SOLR_BODY.format(rows=5000),
                              headers={"User-Agent": UA,
                                       "Content-Type": "application/x-www-form-urlencoded"},
                              timeout=90)
            r.raise_for_status()
            j = r.json()
            resp = j.get("response", {})
            print(f"  numFound={resp.get('numFound')}, docs returned={len(resp.get('docs', []))}")
            return resp.get("docs", [])
        except Exception as e:
            last = e
            print(f"  [retry {attempt+1}] {e}")
    raise RuntimeError(f"Solr fetch failed: {last}")


def main():
    ap = argparse.ArgumentParser(description="KWF research database to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/kwf_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("KWF Kankerbestrijding (Dutch Cancer Society) -> S3")
    print("=" * 60)

    docs = fetch_docs()
    recs, seen = [], set()
    for d in docs:
        aid = clean(first(d.get("ss_field_project_number")))
        if not aid or aid in seen:
            continue
        seen.add(aid)
        given, family = parse_pi(d.get("twm_X3b_nl_field_project_leader"))
        url = first(d.get("ss_url"))
        recs.append({
            "funder_award_id": aid,
            "title": clean(first(d.get("tm_X3b_nl_field_project_title"))),
            "pi_given": given,
            "pi_family": family,
            "institution": clean(d.get("ss_project_institute")),
            "scheme": clean(d.get("ss_project_primary_modality")),
            "disease_site": clean(first(d.get("sm_project_disease_site_code_name"))),
            "status": clean(d.get("ss_project_status")),
            "funding_partner": clean(d.get("ss_name_funding_partner")),
            "start_date_raw": parse_date(d.get("ds_field_project_start_date")),
            "description": ((strip_html(d.get("tm_X3b_nl_project_summary"))
                             or strip_html(d.get("ss_project_summary_raw")) or "")[:2000] or None),
            "landing_page_url": (LANDING_BASE + url) if url and url.startswith("/") else LANDING,
        })

    df = pd.DataFrame(recs).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "scheme", "start_date_raw", "description"):
        nn = df[c].notna().sum()
        print(f"  {c:16}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")

    if len(df) < 800:
        print(f"[ERROR] only {len(df)} rows — expected ~1,003; Solr query changed?")
        sys.exit(1)

    out = args.output_dir / "kwf_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
