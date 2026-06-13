#!/usr/bin/env python3
"""
Academy of Medical Sciences (UK) to S3 Data Pipeline
====================================================

Scrapes the Academy of Medical Sciences (acmedsci.ac.uk) per-scheme awardee
pages and uploads a parquet to S3 for Databricks ingestion.

Data source: server-rendered HTML scheme pages (default UA OK, no JS/Playwright).
The Academy publishes no grants API, no awardee CSV, and is NOT a 360Giving
publisher — static HTML is the only source. Each scheme page lists awardees as
`<p>` rows with the awardee name in `<strong>`; the row grammar differs per
scheme (handled by per-scheme parsers below):

  - Springboard          NAME, INSTITUTION, PROJECT TITLE        (the bulk, ~400+)
  - Starter Grants       NAME, INSTITUTION, PROJECT TITLE        (clinical lecturers)
  - Newton International  NAME working with MENTOR at INST, TITLE
  - Daniel Turnberg       NAME from HOME-INST visiting HOST-INST  (no project title)
  - Networking Grants     LEAD (INST) and CO-APPLICANT (INST) NETWORK TITLE

Review-panel / committee members are intermixed on several pages (senior
professors listed name + institution only, no project title). They are screened
out by requiring a project title (Springboard/Starter/Newton) or the scheme's
distinctive "from … visiting" / paired-lead grammar (Turnberg/Networking).

Amounts are NOT published for any scheme (section 6.7 waiver — UK fellowship
convention). No native per-award id is published, so funder_award_id is
slugify(scheme + name + institution).

Output: s3://openalex-ingest/awards/acmedsci/acmedsci_awards.parquet

Usage:
    python acmedsci_to_s3.py
    python acmedsci_to_s3.py --limit 20     # smoke test (rows per scheme cap)
    python acmedsci_to_s3.py --skip-upload

Author: OpenAlex Team
"""

import argparse
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

from bs4 import BeautifulSoup

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/acmedsci/acmedsci_awards.parquet"
EXPECTED_MIN = 300

SCHEMES = {
    "Springboard": {
        "url": "https://acmedsci.ac.uk/grants-and-schemes/grant-schemes/springboard/springboard-awardees",
        "parser": "comma3",
    },
    "Starter Grant for Clinical Lecturers": {
        "url": "https://acmedsci.ac.uk/grants-and-schemes/grant-schemes/starter-grants",
        "parser": "comma3",
    },
    "Newton International Fellowship": {
        "url": "https://acmedsci.ac.uk/grants-and-schemes/grant-schemes/newton-international-fellowships",
        "parser": "newton",
    },
    "Daniel Turnberg Travel Fellowship": {
        "url": "https://acmedsci.ac.uk/grants-and-schemes/grant-schemes/daniel-turnberg-travel-fellowship",
        "parser": "turnberg",
    },
    "Networking Grant": {
        "url": "https://acmedsci.ac.uk/networking-grants",
        "parser": "networking",
    },
}

LEAD_TITLE_RE = re.compile(
    r"^(?:Dr|Professor|Prof|Mr|Mrs|Ms|Miss|Mx|Sir|Dame)\.?\s+", re.I)
# Post-nominal honours/fellowships to strip from the tail of a name.
POSTNOMINALS = {
    "fmedsci", "frs", "frse", "frcp", "frcpath", "frcpe", "frcs", "frcpsych",
    "frcgp", "mbe", "obe", "cbe", "kbe", "dbe", "phd", "md", "dphil", "dsc",
    "scd", "msc", "ba", "bsc", "mb", "bch", "bchir", "mrcp", "facss", "fba",
    "frcr", "frcog", "frca", "ffph", "frcpch",
}
SUFFIXES = {"jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip().strip(",").strip()
    return s or None


def strip_name(name):
    """Strip leading title (Dr/Prof/Sir...) and trailing post-nominals/suffixes."""
    if not name:
        return None
    n = LEAD_TITLE_RE.sub("", name).strip()
    # remove parenthetical role e.g. "(Chair)"
    n = re.sub(r"\([^)]*\)", "", n).strip()
    tokens = n.split()
    while tokens and tokens[-1].lower().strip(",.") in (POSTNOMINALS | SUFFIXES):
        tokens.pop()
    return " ".join(tokens).strip(",").strip() or None


def split_name(name):
    """'James P. Eisenstein' -> ('James P.', 'Eisenstein'). Suffix-aware."""
    n = strip_name(name)
    if not n:
        return None, None
    tokens = n.split()
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def slugify(*parts):
    s = " ".join(p for p in parts if p)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")
    return s[:120]


# ---- per-scheme row parsers -------------------------------------------------

# Institution continuation: a comma-field that *begins* one of these is part of
# the institution, not the start of the project title ("…, University of London").
INST_CONT = re.compile(
    r"^(University|College|Faculty|Medical School|Institute of|School of Medicine)\b", re.I)


def parse_comma3(full, strong):
    """'[Title] NAME, INSTITUTION, PROJECT TITLE' -> dict, or None.

    Requires >=2 commas (name | institution | title) so review-panel rows
    (name, institution only = 1 comma) are screened out. The name is the first
    comma-field (full "Dr First Last" — the <strong> is unreliable here, it
    sometimes bolds only the first name). The institution can itself span a
    comma (e.g. "City St. George's, University of London"), so any following
    field that begins with an institution-continuation keyword is folded in
    before the remainder becomes the title.
    """
    parts = [p.strip() for p in full.split(",")]
    if len(parts) < 3:
        return None
    name = parts[0]
    inst_fields = [parts[1]]
    ti = 2
    while ti < len(parts) - 1 and INST_CONT.match(parts[ti]):
        inst_fields.append(parts[ti])
        ti += 1
    institution = ", ".join(inst_fields)
    title = ", ".join(parts[ti:]).strip()
    if not title or len(title) < 8:
        return None
    return {"name": name, "institution": clean(institution), "title": clean(title),
            "mentor": None}


def parse_newton(full, strong):
    """'NAME working with MENTOR at INSTITUTION, PROJECT TITLE'."""
    m = re.match(r"(?P<name>.+?)\s+working with\s+(?P<mentor>.+?)\s+at\s+"
                 r"(?:the\s+)?(?P<inst>.+?),\s*(?P<title>.+)$", full, re.I)
    if not m:
        return None
    # strong wraps only the honorific ("Dr") on Newton rows — use the captured name.
    return {"name": m.group("name"), "institution": clean(m.group("inst")),
            "title": clean(m.group("title")), "mentor": clean(m.group("mentor"))}


def parse_turnberg(full, strong):
    """'NAME from HOME-INST visiting HOST-INST' (no project title)."""
    m = re.match(r"(?P<name>.+?)\s+from\s+(?P<inst>.+?)\s+visiting\s+(?P<host>.+)$",
                 full, re.I)
    if not m:
        return None
    host = clean(m.group("host"))
    return {"name": m.group("name"), "institution": clean(m.group("inst")),
            "title": f"Daniel Turnberg Travel Fellowship visiting {host}" if host else None,
            "mentor": None}


def parse_networking(full, strong):
    """'LEAD (INST) and CO-APPLICANT (INST) NETWORK TITLE' -> take UK-side lead.

    Networking grants are paired (overseas lead + UK co-applicant). We capture
    one row keyed on the UK co-applicant where determinable, else the first PI.
    """
    m = re.match(r"(?P<a>.+?)\s*\((?P<ai>[^)]+)\)\s+and\s+(?P<b>.+?)\s*\((?P<bi>[^)]+)\)\s*(?P<title>.*)$",
                 full, re.I)
    if not m:
        return None
    title = clean(m.group("title"))
    if not title:
        return None
    # Co-applicant (second) is the UK side by scheme design.
    return {"name": clean(m.group("b")), "institution": clean(m.group("bi")),
            "title": title, "mentor": clean(m.group("a"))}


PARSERS = {"comma3": parse_comma3, "newton": parse_newton,
           "turnberg": parse_turnberg, "networking": parse_networking}


def is_candidate_row(full):
    if not full or len(full) < 18 or len(full) > 500:
        return False
    if full.endswith((":",)):
        return False
    # prose paragraphs tend to be full sentences; awardee rows rarely end with a period
    return True


def scrape_scheme(session, scheme, cfg, limit=None):
    r = session.get(cfg["url"], timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    parser = PARSERS[cfg["parser"]]
    rows, seen = [], set()
    for p in soup.find_all("p"):
        st = p.find("strong")
        if not st:
            continue
        strong = st.get_text(" ", strip=True)
        full = p.get_text(" ", strip=True)
        if not is_candidate_row(full):
            continue
        rec = parser(full, strong)
        if not rec:
            continue
        given, family = split_name(rec["name"])
        if not family:
            continue
        award_id = f"acmedsci-{slugify(scheme, rec['name'], rec['institution'])}"
        if award_id in seen:
            continue
        seen.add(award_id)
        rows.append({
            "funder_award_id": award_id,
            "title": rec["title"],
            "pi_given": given,
            "pi_family": family,
            "institution": rec["institution"],
            "mentor": rec["mentor"],
            "funder_scheme": scheme,
            "landing_page_url": cfg["url"],
        })
        if limit and len(rows) >= limit:
            break
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
    ap = argparse.ArgumentParser(description="Academy of Medical Sciences awards to S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/acmedsci_data"))
    ap.add_argument("--limit", type=int, default=None,
                    help="smoke test: cap rows per scheme")
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Academy of Medical Sciences (UK) awards -> S3")
    print("=" * 60)

    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    all_rows = []
    for scheme, cfg in SCHEMES.items():
        try:
            rows = scrape_scheme(s, scheme, cfg, limit=args.limit)
        except Exception as e:
            print(f"  {scheme}: ERROR {e}")
            continue
        print(f"  {scheme}: {len(rows)} awardees")
        all_rows.extend(rows)
        time.sleep(0.5)

    if not args.limit and len(all_rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(all_rows)} awardees — expected >={EXPECTED_MIN}; "
              "refusing partial ship (scheme page structure may have changed)")
        sys.exit(1)

    df = pd.DataFrame(all_rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows, {len(df.columns)} columns")
    for c in ("title", "pi_family", "institution", "funder_scheme"):
        nn = df[c].notna().sum()
        print(f"  {c}: {nn}/{len(df)} ({round(100 * nn / max(len(df), 1))}%)")
    print(f"  dupes on funder_award_id: {df.funder_award_id.duplicated().sum()}")
    print("  by scheme:")
    for sch, cnt in df.funder_scheme.value_counts().items():
        print(f"    {sch}: {cnt}")
    out = args.output_dir / "acmedsci_awards.parquet"
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
