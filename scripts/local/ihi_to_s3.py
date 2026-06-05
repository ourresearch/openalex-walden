#!/usr/bin/env python3
"""
IHI (Innovative Health Initiative, formerly IMI) to S3 Data Pipeline
====================================================================

Scrapes the IHI project factsheet pages on ihi.europa.eu.

Data Source:
  Listing: https://www.ihi.europa.eu/projects-results/project-factsheets
    — single page with all ~243 factsheet anchors `/projects-results/project-factsheets/{slug}`
  Detail: https://www.ihi.europa.eu/projects-results/project-factsheets/{slug}
    — Drupal-rendered page with rendered text fields:
        Total Cost, Start Date, End Date, Grant agreement number, Project coordinator
    Plus a participants table with per-org EU funding.

Output: s3://openalex-ingest/awards/ihi/ihi_projects.parquet

Schema notes:
  - funder_award_id = the IHI Grant Agreement Number (e.g. 831434) when available,
    falling back to `ihi-{slug}` for older IMI projects without a GA number.
  - amount = the first "Total Cost" value (IHI/IMI funding contribution); fall
    back to NULL if not parseable.
  - lead_investigator = "Project coordinator" name + first participant institution.
  - description = the og:description meta tag (project abstract).
  - title = og:title meta tag (full project name).
  - acronym = derived from the URL slug.

Usage:
    python ihi_to_s3.py --skip-upload          # full scrape
    python ihi_to_s3.py --limit 20             # smoke
"""
import argparse, json, re, ssl, sys, time, urllib.request
import certifi
from datetime import datetime, timezone
from pathlib import Path
try:
    sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
except Exception: pass
import pandas as pd

S3_BUCKET = "openalex-ingest"
S3_KEY    = "awards/ihi/ihi_projects.parquet"
BASE      = "https://www.ihi.europa.eu"
LIST_URL  = f"{BASE}/projects-results/project-factsheets"
DELAY     = 0.20

UA = "OpenAlex-IHI/1.0 (contact@openalex.org)"

def _fetch(url, tries=3):
    """Use system curl — anaconda Python's OpenSSL has trouble with ihi.europa.eu cert chain."""
    import subprocess
    for a in range(tries):
        try:
            r = subprocess.run(["/usr/bin/curl", "-sL", "--max-time", "30", "-A", UA, url],
                              capture_output=True, timeout=45, check=True)
            return r.stdout.decode("utf-8", "replace")
        except Exception:
            if a == tries-1: raise
            time.sleep(2**a)

def list_factsheet_slugs():
    html = _fetch(LIST_URL)
    slugs = set(re.findall(r'href="/projects-results/project-factsheets/([a-z0-9_-]+)"', html))
    return sorted(slugs)

def _strip(html):
    """Drupal HTML → newline-separated text, nbsp normalized."""
    t = re.sub(r"<[^>]+>", "\n", html)
    t = t.replace("&nbsp;", " ").replace("&amp;", "&").replace("&#039;", "'")
    return [l.strip() for l in t.split("\n") if l.strip()]

def _value_after(lines, label):
    """Find label exactly, return next non-empty line."""
    for i, line in enumerate(lines):
        if line == label:
            for j in range(i+1, min(len(lines), i+5)):
                if lines[j]: return lines[j]
    return None

def _parse_date_eu(s):
    """`dd/mm/yyyy` → ISO yyyy-mm-dd, returns None on failure."""
    if not s: return None
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s.strip())
    if not m: return None
    d, mo, y = m.groups()
    return f"{y}-{int(mo):02d}-{int(d):02d}"

def _parse_amount(s):
    """`40 273 196` → 40273196.0. Returns None on failure."""
    if not s: return None
    cleaned = re.sub(r"[^\d.,]", "", s.replace(" ", "").replace(",", "."))
    try:
        v = float(cleaned)
        return v if v > 0 else None
    except ValueError:
        return None

def parse_factsheet(slug, html):
    lines = _strip(html)
    # og:title (full title) + og:description (abstract)
    m_title = re.search(r'<meta property="og:title" content="([^"]+)"', html)
    m_desc  = re.search(r'<meta property="og:description" content="([^"]+)"', html)
    title = m_title.group(1) if m_title else None
    description = m_desc.group(1) if m_desc else None
    total_cost_raw = _value_after(lines, "Total Cost")
    start_raw  = _value_after(lines, "Start Date")
    end_raw    = _value_after(lines, "End Date")
    ga_no      = _value_after(lines, "Grant agreement number")
    coordinator = _value_after(lines, "Project coordinator")
    # First Participants row = the coordinator org (Drupal renders them in order)
    m_first_participant = re.search(r"<th[^>]*>Participants</th>.*?<tr><td>([^<]+)</td>", html, re.S)
    coord_org = m_first_participant.group(1).strip() if m_first_participant else None
    return {
        "slug": slug,
        "acronym": slug.upper(),
        "title": title,
        "description": description,
        "amount": _parse_amount(total_cost_raw),
        "start_date": _parse_date_eu(start_raw),
        "end_date": _parse_date_eu(end_raw),
        "grant_agreement_no": (ga_no or "").strip() or None,
        "coordinator_name": coordinator,
        "coordinator_org": coord_org,
        "landing_url": f"{BASE}/projects-results/project-factsheets/{slug}",
    }

def scrape(output_dir: Path, limit=None):
    print(f"[1/2] Fetching factsheet list")
    slugs = list_factsheet_slugs()
    print(f"      found {len(slugs)} factsheet slugs")
    if limit: slugs = slugs[:limit]
    out_rows = []
    cache = output_dir / "factsheets"
    cache.mkdir(parents=True, exist_ok=True)
    for i, slug in enumerate(slugs):
        cp = cache / f"{slug}.html"
        if not cp.exists():
            html = _fetch(f"{BASE}/projects-results/project-factsheets/{slug}")
            cp.write_text(html, encoding="utf-8")
            time.sleep(DELAY)
        else:
            html = cp.read_text(encoding="utf-8")
        rec = parse_factsheet(slug, html)
        out_rows.append(rec)
        if (i+1) % 25 == 0:
            print(f"      parsed {i+1}/{len(slugs)}")
    return out_rows

def transform(rows):
    df = pd.DataFrame(rows)
    # funder_award_id: GA number when present, else `ihi-{slug}` (older IMI without GA)
    df["funder_award_id"] = df["grant_agreement_no"].where(
        df["grant_agreement_no"].notna() & (df["grant_agreement_no"].str.strip() != ""),
        "ihi-" + df["slug"].astype(str),
    )
    # Split coordinator into given/family
    def _split(name):
        if name is None or pd.isna(name): return (None, None)
        tokens = name.strip().split()
        # Strip degree suffixes (canonical split_name)
        sfx = {"phd","md","dphil","dsc","msc","jr","jr.","sr","sr.","ii","iii","iv"}
        while tokens and tokens[-1].lower().strip(",.") in sfx:
            tokens.pop()
        if len(tokens) >= 2:
            return " ".join(tokens[:-1]), tokens[-1]
        return (None, tokens[0]) if tokens else (None, None)
    pi = df["coordinator_name"].astype("string").map(_split)
    df["pi_given_name"]  = pi.map(lambda t: t[0])
    df["pi_family_name"] = pi.map(lambda t: t[1])
    df["start_year"] = pd.to_datetime(df["start_date"], errors="coerce").dt.year.astype("Int64")
    df["end_year"]   = pd.to_datetime(df["end_date"], errors="coerce").dt.year.astype("Int64")
    df["provenance"] = "ihi_factsheets"
    df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    # Slug-collision guard
    dup = df["funder_award_id"].duplicated().sum()
    if dup:
        raise RuntimeError(f"§slug-collision: {dup} duplicates in funder_award_id")
    # future-year cap
    yr_now = datetime.now(timezone.utc).year
    df.loc[df["start_year"] > yr_now + 1, "start_year"] = pd.NA
    df.loc[df["end_year"] > yr_now + 1, "end_year"] = pd.NA
    for c in df.columns:
        if df[c].dtype == object: df[c] = df[c].astype("string")
    return df

def check_no_shrink(df, allow_shrink):
    try:
        import boto3
        s3 = boto3.client("s3")
        s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev = pd.read_parquet(f"s3://{S3_BUCKET}/{S3_KEY}")
        if len(df) < len(prev) and not allow_shrink:
            raise SystemExit(f"§1.4 shrink FAILED: new {len(df):,} < {len(prev):,}")
        print(f"  §1.4 OK")
    except SystemExit: raise
    except Exception as e:
        print(f"  §1.4: no prior ({type(e).__name__})")

def main():
    ap = argparse.ArgumentParser(description="IHI factsheet scraper → S3")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/ihi"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    ap.add_argument("--limit", type=int, default=None, help="cap factsheets (smoke)")
    a = ap.parse_args()
    a.output_dir.mkdir(parents=True, exist_ok=True)
    print("="*60); print("IHI Factsheets (ihi.europa.eu) -> S3"); print("="*60)
    raw = scrape(a.output_dir, limit=a.limit)
    df = transform(raw)
    out = a.output_dir / "ihi_projects.parquet"
    df.to_parquet(out, index=False)
    print(f"\n[2/2] Saved {out.name}: {len(df):,} rows, {out.stat().st_size/1e6:.1f} MB")
    nn = lambda c: 100 * df[c].notna().sum() / len(df)
    print(f"\nCoverage:")
    for c in ["funder_award_id","title","description","amount","pi_family_name","coordinator_org","start_year","end_year"]:
        if c in df.columns:
            print(f"  {c:24s} {df[c].notna().sum():,} ({nn(c):.1f}%)")
    if df["amount"].notna().any():
        print(f"\n  EUR amount median: {float(df['amount'].median()):,.0f}, max: {float(df['amount'].max()):,.0f}")
    if df["start_year"].notna().any():
        print(f"  Year range: {int(df['start_year'].min())}-{int(df['start_year'].max())}")
    if not a.skip_upload:
        check_no_shrink(df, a.allow_shrink)
        import subprocess, shutil
        aws = shutil.which("aws")
        if aws: subprocess.run([aws,"s3","cp",str(out),f"s3://{S3_BUCKET}/{S3_KEY}"], check=False)
        else: print(f"  [manual] aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
    print(f"\nNext: notebooks/awards/CreateIHIAwards.ipynb")

if __name__ == "__main__":
    main()
