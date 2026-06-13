#!/usr/bin/env python3
"""
Cancer Research UK (recently funded research) to S3 Data Pipeline
================================================================

Scrapes CRUK's recently-funded-research committee award pages and uploads a
parquet to S3.

Data source: 5 committee award pages under
    cancerresearchuk.org/for-researchers/apply-for-and-manage-your-funding/recently-funded-research/
Each award: an <h4> project title followed by one or more chakra-card divs, each
holding "PI name | Institution". (Childhood has no awardee page — scheme-only.)
Recent awards only (full historical portfolio is behind Flexi-Grant login).
Amounts/grant-ids not published -> NULL (§6.7 waiver). provenance `cruk`,
priority 342. F4320319985 (Path A).

Output: s3://openalex-ingest/awards/cruk/cruk_awards.parquet
Usage: python cruk_to_s3.py [--limit N] [--skip-upload]
Author: OpenAlex Team
"""
import argparse, re, subprocess, sys, time
from pathlib import Path
import pandas as pd, requests
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass
from bs4 import BeautifulSoup

BASE = "https://www.cancerresearchuk.org/for-researchers/apply-for-and-manage-your-funding/recently-funded-research/"
COMMITTEES = ["discovery-research-committee-awards", "early-detection-diagnosis-committee-awards",
              "prevention-population-research-committee-awards", "clinical-research-committee-awards",
              "research-careers-committee-awards"]
S3_BUCKET, S3_KEY = "openalex-ingest", "awards/cruk/cruk_awards.parquet"
EXPECTED_MIN = 40
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
INST_KEY = re.compile(r"Universit|College|Institute|Imperial|UCL|King'?s|Hospital|"
                      r"School|Centre|Center|Trust|Queen|Cancer", re.I)
_TITLE_RE = re.compile(r"^(Dr|Professor|Prof|Mr|Mrs|Ms|Miss|Dame|Sir)\.?\s+", re.I)


def clean(v):
    if v is None: return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


def split_name(raw):
    n = _TITLE_RE.sub("", clean(raw) or "").strip()
    toks = n.split()
    if not toks: return None, None
    if len(toks) == 1: return None, toks[0]
    return " ".join(toks[:-1]), toks[-1]


def slug(s): return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:90]


def parse_page(html, committee):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    cur_title = None
    # Walk h4 titles and chakra-card recipients in document order
    for el in soup.find_all(["h3", "h4", "div"]):
        cls = " ".join(el.get("class") or [])
        if el.name in ("h3", "h4") and "chakra-heading" in cls:
            t = clean(el.get_text(" ", strip=True))
            if t and not re.match(r"^(20\d\d|January|February|March|April|May|June|July|August|September|October|November|December)", t):
                cur_title = t
        elif "chakra-card" in cls and "chakra-card__" not in cls:
            txt = [clean(x) for x in el.stripped_strings]
            txt = [x for x in txt if x]
            if not txt:
                continue
            pi = txt[0]
            institution = next((x for x in txt[1:] if INST_KEY.search(x)), txt[1] if len(txt) > 1 else None)
            g, f = split_name(pi)
            if not f or not cur_title:
                continue
            rows.append({"funder_award_id": f"cruk-{slug(cur_title[:40])}-{slug(f)}",
                         "title": cur_title, "pi_given": g, "pi_family": f,
                         "institution": clean(institution), "funder_scheme": committee.replace("-", " ").title(),
                         "landing_page_url": BASE + committee})
    return rows


def up(p, b, k):
    print(f"\nUploading to s3://{b}/{k}...")
    try:
        subprocess.run(["aws", "s3", "cp", str(p), f"s3://{b}/{k}"], capture_output=True, text=True, check=True)
        print("Upload complete"); return True
    except subprocess.CalledProcessError as e:
        print("Upload failed:", e.stderr); return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/cruk_data"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    a = ap.parse_args(); a.output_dir.mkdir(parents=True, exist_ok=True)
    print("Cancer Research UK recently funded research -> S3")
    s = requests.Session(); s.headers["User-Agent"] = UA
    rows, seen = [], set()
    for c in COMMITTEES:
        try:
            r = s.get(BASE + c, timeout=40)
        except Exception:
            continue
        if r.status_code != 200:
            print(f"  {c}: HTTP {r.status_code}"); continue
        for rec in parse_page(r.text, c):
            if rec["funder_award_id"] in seen:
                continue
            seen.add(rec["funder_award_id"])
            rows.append(rec)
        print(f"  {c}: {len(rows)} total")
        time.sleep(0.3)
    if not a.limit and len(rows) < EXPECTED_MIN:
        print(f"[ERROR] only {len(rows)} — expected >={EXPECTED_MIN}"); sys.exit(1)
    if a.limit: rows = rows[:a.limit]
    df = pd.DataFrame(rows).astype("string")
    print(f"\nDataFrame: {len(df)} rows")
    for col in ("title", "pi_family", "institution", "funder_scheme"):
        print(f"  {col}: {df[col].notna().sum()}/{len(df)}")
    print("  dupes:", df.funder_award_id.duplicated().sum())
    out = a.output_dir / "cruk_awards.parquet"; df.to_parquet(out, index=False); print("Wrote", out)
    for _, r2 in df.head(4).iterrows():
        print("   ", repr(r2.pi_given), repr(r2.pi_family), "|", repr(str(r2.institution)[:30]))
    if a.limit: print("smoke ok"); return
    if not a.skip_upload and not up(out, S3_BUCKET, S3_KEY): sys.exit(1)
    print("Pipeline complete!")


if __name__ == "__main__":
    main()
