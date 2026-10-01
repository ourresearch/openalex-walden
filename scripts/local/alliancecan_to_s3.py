#!/usr/bin/env python3
"""
Digital Research Alliance of Canada (the Alliance, F4320331257) funding-programme awards to S3
==============================================================================================

The Alliance's main "awards" are in-kind compute/storage allocations from the annual Resource
Allocation Competition (RAC: RRG / RPP project ids such as "RRG 4073", "RAPI xvj-290-01"); those are
NOT ingested here (no public per-project list on the current site, and whether in-kind allocations
belong in /awards is a coordinator decision).

What the Alliance does publish at grant level (recipient person, institution, project title, year)
for its own FUNDING programmes, both on alliancecan.ca (robots.txt allows; /api/ is not used):

  * DRI EDIA Champions pilot program (call June 5, 2024; projects Sept 2024 - March 31, 2025;
    up to $35,000 each; 82 recipients)
    - table "DRI EDIA Champions recipients" on /en/initiatives/dri-investments
      (Recipient name | Institution | Project title)
  * Data Champions Pilot Project Call (call January 6, 2022; $916,000 total)
    - "Award Recipients" list on /en/opportunities/data-champions-pilot-project-call
      (Applicant / Team / Project / Institution)

Not ingested (no PI-level data): the institutional DRI investments (ARC renewal contributions,
DMP Assistant, Lunaris, ...: news prose, institution only), the CoreTrustSeal certification cohort
(repository list, no PI), the 2026 Research Software AI Enhancement recipients (prose, no PI).

funder_award_id: the Alliance prints no award number for these programmes, and the ~84 citation
stubs under F4320331257 are RAC allocation ids, so a stable synthetic key is used:
ALLIANCE-EDIA-2024-<name-slug> / ALLIANCE-DATACHAMP-2022-<name-slug>.
Per-recipient amounts are not published (amount NULL).

Output: s3://openalex-ingest/awards/alliancecan/alliancecan_projects.parquet
"""

import argparse
import html
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

BASE = "https://alliancecan.ca/en/"
EDIA_URL = BASE + "initiatives/dri-investments"
DATACHAMP_URL = BASE + "opportunities/data-champions-pilot-project-call"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/alliancecan/alliancecan_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last = None
    for attempt in range(4):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code == 200:
                r.encoding = "utf-8"
                return r.text
            last = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def text(fragment: str) -> str:
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("​", "").replace("\xa0", " ")
    return re.sub(r"\s+", " ", t).strip()


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


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def edia() -> list[dict]:
    page = get(EDIA_URL)
    out = []
    for t in re.findall(r"<table.*?</table>", page, re.S):
        rows = [[text(c) for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", tr, re.S)]
                for tr in re.findall(r"<tr.*?</tr>", t, re.S)]
        if not rows or [h.lower() for h in rows[0][:3]] != ["recipient name", "institution", "project title"]:
            continue
        for r in rows[1:]:
            if len(r) < 3 or not r[0] or not r[2]:
                continue
            out.append({"programme": "DRI EDIA Champions pilot program", "programme_code": "EDIA",
                        "year": "2024", "end_date": "2025-03-31", "recipient": r[0], "team": None, "institution": r[1] or None,
                        "title": r[2], "landing_page_url": EDIA_URL})
    log(f"EDIA Champions: {len(out)} recipients")
    if len(out) < 80:   # the page states 82
        raise SystemExit("EDIA Champions table shorter than expected; page changed?")
    return out


def data_champions() -> list[dict]:
    page = get(DATACHAMP_URL)
    i = page.find("Award Recipients")
    if i < 0:
        raise SystemExit("Data Champions: 'Award Recipients' section not found")
    # one line per HTML text node; each recipient is "Applicant: / [Team:] / Project: / Institution:"
    lines = [ln.strip() for ln in html.unescape(re.sub(r"<[^>]+>", "\n", page[i:])).replace("\xa0", " ").split("\n")]
    lines = [ln for ln in lines if ln]
    out, cur, lab = [], None, None
    for ln in lines[1:]:
        m = re.match(r"^(Applicant|Team|Project|Institution)\s*:\s*(.*)$", ln)
        if m:
            lab = m.group(1)
            if lab == "Applicant":
                cur = {"Applicant": "", "Team": None, "Project": None, "Institution": None}
                out.append(cur)
            cur[lab] = m.group(2).strip()
            continue
        if cur is None:
            continue
        if lab == "Institution" and cur["Institution"]:
            break                                   # first text node after the last institution = end of list
        cur[lab] = f"{cur[lab] or ''} {ln}".strip()
    out = [{"programme": "Data Champions Pilot Project", "programme_code": "DATACHAMP", "year": "2022", "end_date": None,
            "recipient": o["Applicant"], "team": o["Team"], "institution": o["Institution"] or None,
            "title": o["Project"], "landing_page_url": DATACHAMP_URL}
           for o in out if o["Applicant"] and o["Project"]]
    log(f"Data Champions: {len(out)} recipients")
    if len(out) < 10:
        raise SystemExit("Data Champions list shorter than expected; page changed?")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Alliance funding-programme awards -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    args = ap.parse_args()

    rows = edia() + data_champions()
    if args.limit:
        rows = rows[: args.limit]
    for r in rows:
        lead = re.split(r"\s+and\s+|\s*&\s*|,\s*", r["recipient"])[0].strip()   # "Tim Murphy and Paul Pavlidis"
        lead = re.sub(r"^(?:(?:dr|prof|professor)\.?\s+)+", "", lead, flags=re.I)
        r["lead_name"] = lead
        if re.search(r"\b(?:Networks?|Canada|University|Institute|Centre|Center|Inc|Ltd|Society|Association)\b", lead):
            r["lead_given_name"], r["lead_family_name"] = None, None   # organization applicant ("Ocean Networks Canada")
        else:
            r["lead_given_name"], r["lead_family_name"] = split_name(lead)
        r["co_applicants"] = r["recipient"] if lead != r["recipient"] else None
        r["funder_award_id"] = f"ALLIANCE-{r['programme_code']}-{r['year']}-{slug(lead)}"
    df = pd.DataFrame(rows)
    dup = df["funder_award_id"].duplicated(keep=False)
    if dup.any():   # same person twice in one programme -> disambiguate by title
        df.loc[dup, "funder_award_id"] = df.loc[dup, "funder_award_id"] + "-" + df.loc[dup, "title"].map(lambda t: slug(t)[:30])
    if df["funder_award_id"].duplicated().any():
        raise SystemExit("duplicate funder_award_id")
    for c in ["recipient", "institution", "title", "lead_family_name"]:
        log(f"  {c:16s} {df[c].notna().mean():6.1%}")
    log(f"  programmes: {df['programme'].value_counts().to_dict()}")

    df = df.astype("string")  # runbook §1.2 item 5
    args.output_dir.mkdir(parents=True, exist_ok=True)
    path = args.output_dir / "alliancecan_projects.parquet"
    df.to_parquet(path, index=False)
    log(f"Wrote {len(df)} rows to {path}")
    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    prev = args.output_dir / "_previous_alliancecan_projects.parquet"
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
