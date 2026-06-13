#!/usr/bin/env python3
"""
Fondation ARC pour la Recherche sur le Cancer (France) to S3
============================================================

Harvests the Fondation ARC laureate lists (fondation-arc.org) and uploads a
parquet to S3 for Databricks ingestion.

Data source: per-round laureate PDFs linked from the WordPress results index
    https://www.fondation-arc.org/espace-chercheur/resultats-des-appels-a-projets/
    -> ~109 PDF URLs under /wp-content/uploads/.../*.pdf (directly downloadable).
Each PDF holds a table with French headers:
    Bénéficiaire | Institut | Ville | Titre du projet de recherche | Durée (mois)
Name format is "FAMILY Firstname" (uppercase family first). ~1,000+ laureates
across schemes (doctoral, post-doc, mobility, projets, etc.). Amounts are
standard per-scheme (not per-row) -> NULL (§6.7 waiver). French-language titles
ingested as-is. Year + scheme parsed from the PDF filename. provenance
`fondation_arc`, priority 332. F4320322361 (Path A).

Output: s3://openalex-ingest/awards/fondation_arc/fondation_arc_grants.parquet

Usage:
    python fondation_arc_to_s3.py
    python fondation_arc_to_s3.py --limit 5      # smoke test (first N PDFs)
    python fondation_arc_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
import io
import re
import subprocess
import sys
import time
import unicodedata
from pathlib import Path

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

import pdfplumber

INDEX = "https://www.fondation-arc.org/espace-chercheur/resultats-des-appels-a-projets/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/fondation_arc/fondation_arc_grants.parquet"
EXPECTED_MIN = 500
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

INST_KW = re.compile(r"INSTITUT|UNIVERSIT|CENTRE|CNRS|INSERM|HOPITAL|HÔPITAL|"
                     r"FACULT|LABORATOIRE|ECOLE|ÉCOLE|CEA|IGR|CURIE|PASTEUR|CHU", re.I)


def deaccent(s):
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).replace("\n", " ").strip()
    return s or None


# A valid laureate cell is "FAMILY Firstname" — CAPS family token(s) then a
# capitalised given name, short. Jumbled/merged cells (sentence fragments from
# mis-extracted tables) fail this and cause the whole row to be skipped.
VALID_NAME = re.compile(r"^[A-ZÀ-Ÿ][A-ZÀ-Ÿ'’\-]{1,}(?:\s+[A-ZÀ-Ÿ][A-ZÀ-Ÿ'’\-]+)*"
                        r"\s+[A-ZÀ-Ÿ][a-zà-ÿ][\wà-ÿ'’\-]*", re.U)


def is_valid_name(cell):
    if not cell:
        return False
    c = cell.strip()
    if len(c) > 45 or len(c.split()) > 6:
        return False
    return bool(VALID_NAME.match(c))


def split_arc_name(raw):
    """'ADAMS Caroline' -> (given 'Caroline', family 'ADAMS').

    ARC lists names family-first with the family name in CAPS. Leading all-caps
    tokens (len>1) form the family name; the rest is the given name.
    """
    raw = clean(raw)
    if not raw:
        return None, None
    raw = re.sub(r"\([^)]*\)", "", raw).strip()
    toks = raw.split()
    fam, i = [], 0
    while i < len(toks) and (toks[i].isupper() or toks[i] in ("-", "DE", "LE", "LA")) and len(toks[i]) > 1:
        fam.append(toks[i])
        i += 1
    if fam and i < len(toks):
        return " ".join(toks[i:]), " ".join(fam)
    # fallback: first token family
    if len(toks) == 1:
        return None, toks[0]
    return " ".join(toks[1:]), toks[0]


def slugify(*parts):
    s = " ".join(p for p in parts if p)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")[:110]


def year_scheme_from_url(url):
    fn = url.rsplit("/", 1)[-1]
    stem = re.sub(r"\.pdf$", "", fn, flags=re.I)
    ym = re.search(r"[ap_/-](20\d{2})", fn) or re.search(r"(20\d{2})", url)
    year = int(ym.group(1)) if ym else None
    scheme_map = [("doc", "Doctoral"), ("mob", "Mobility"), ("pga", "Projet Fondation ARC"),
                  ("attract", "ATTRACT"), ("mobitech", "MobiTech"), ("pdf_acceptes", "Projets acceptés"),
                  ("mddoc", "4e année de thèse"), ("laureat", "Laureates")]
    scheme = next((label for key, label in scheme_map if key in stem.lower()), None)
    return year, scheme or stem


def get_pdf_urls(session):
    r = session.get(INDEX, timeout=60)
    r.raise_for_status()
    urls = sorted(set(re.findall(r'https?://[^\s"<>]+\.pdf', r.text)))
    return [u for u in urls if "/wp-content/uploads/" in u]


def find_header_cols(table):
    """Return (header_row_idx, {field:col}) by matching French headers, or None."""
    for ri, row in enumerate(table[:4]):
        cells = [deaccent(c) for c in row]
        joined = " | ".join(cells)
        if "beneficiaire" in joined and ("titre" in joined or "institut" in joined):
            cols = {}
            for ci, c in enumerate(cells):
                if "beneficiaire" in c:
                    cols["name"] = ci
                elif "institut" in c and "name" in cols:
                    cols["institut"] = ci
                elif c == "ville" or "ville" in c:
                    cols["ville"] = ci
                elif "titre" in c:
                    cols["titre"] = ci
            if "name" in cols:
                return ri, cols
    return None


def parse_pdf(content, url):
    year, scheme = year_scheme_from_url(url)
    rows = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        cols = None
        for page in pdf.pages:
            for table in page.extract_tables() or []:
                if not table:
                    continue
                hdr = find_header_cols(table)
                start = 0
                if hdr:
                    start, cols = hdr[0] + 1, hdr[1]
                if not cols:
                    continue
                ci_name = cols.get("name", 0)
                ci_inst = cols.get("institut", 1)
                ci_ville = cols.get("ville", 2)
                ci_titre = cols.get("titre", 3)
                for row in table[start:]:
                    if len(row) <= ci_name:
                        continue
                    name = clean(row[ci_name])
                    if not name or "beneficiaire" in deaccent(name):
                        continue
                    if not is_valid_name(name):
                        continue  # jumbled/merged cell — skip (and effectively skip messy PDFs)
                    titre = clean(row[ci_titre]) if len(row) > ci_titre else None
                    inst = clean(row[ci_inst]) if len(row) > ci_inst else None
                    ville = clean(row[ci_ville]) if len(row) > ci_ville else None
                    given, family = split_arc_name(name)
                    if not family:
                        continue
                    affiliation = ", ".join(p for p in (inst, ville) if p) or None
                    rows.append({
                        "name_raw": name, "given": given, "family": family,
                        "institution": affiliation, "title": titre,
                        "year": year, "scheme": scheme, "url": url,
                    })
    return rows


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
    ap = argparse.ArgumentParser(description="Fondation ARC laureates to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/fondation_arc_data"))
    ap.add_argument("--limit", type=int, default=None, help="smoke: first N PDFs")
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Fondation ARC laureates -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers["User-Agent"] = UA
    pdf_urls = get_pdf_urls(s)
    print(f"PDF URLs on index: {len(pdf_urls)}")
    if args.limit:
        pdf_urls = pdf_urls[: args.limit]

    rows, seen = [], set()
    for n, url in enumerate(pdf_urls, 1):
        try:
            r = s.get(url, timeout=60)
            if r.status_code != 200 or r.content[:4] != b"%PDF":
                print(f"  [{n}] skip {url.rsplit('/',1)[-1]} (HTTP {r.status_code})")
                continue
            recs = parse_pdf(r.content, url)
        except Exception as e:
            print(f"  [{n}] error {url.rsplit('/',1)[-1]}: {e}")
            continue
        new = 0
        for rec in recs:
            aid = f"arc-{slugify(rec['scheme'] or '', str(rec['year']), rec['family'], rec['given'] or '', rec['institution'] or '')}"
            if aid in seen:
                continue
            seen.add(aid)
            rec["funder_award_id"] = aid
            rows.append(rec)
            new += 1
        if n % 10 == 0 or new:
            print(f"  [{n}/{len(pdf_urls)}] {url.rsplit('/',1)[-1][:40]}: +{new} ({len(rows)} total)")
        time.sleep(0.2)

    if not args.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} laureates — expected >={EXPECTED_MIN}; refusing partial ship")
        sys.exit(1)

    df = pd.DataFrame(rows)
    df = df.rename(columns={"given": "pi_given", "family": "pi_family", "scheme": "funder_scheme"})
    df["start_year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df = df[["funder_award_id", "title", "pi_given", "pi_family", "institution",
             "funder_scheme", "start_year", "url"]].rename(columns={"url": "landing_page_url"})
    for c in df.columns:
        if c != "start_year":
            df[c] = df[c].astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "funder_scheme", "start_year"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    print(f"  dupes: {df.funder_award_id.duplicated().sum()}  years: {df.start_year.min()}-{df.start_year.max()}")
    out = args.output_dir / "fondation_arc_grants.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if args.limit:
        print("\nSmoke run complete (no upload with --limit).")
        return
    if not args.skip_upload:
        if not upload_to_s3(out, S3_BUCKET, S3_KEY):
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
