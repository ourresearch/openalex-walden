#!/usr/bin/env python3
"""
Canada Excellence Research Chairs (CERC) to S3 Data Pipeline
============================================================

The CERC program (Tri-agency Institutional Programs Secretariat, cerc.gc.ca)
publishes:

1. Chairholders (current):  /chairholders-titulaires/index-eng.aspx
2. Laureates (former):      /chairholders-titulaires/former-ancien-eng.aspx
   One <tr> per chair: surname (hidden span), full name, institution | agency,
   chair title, description. The chair's photo path carries its competition
   number (/images/chairholders/comp{N}/Surname__Given.jpg).
3. Competition results:     /results-resultats/index-eng.aspx
   Names, institution | agency and chair title for Competitions 2, 3 and 4
   (Competition 4 chairs are "added as each of their terms begin").

Rows from (1)+(2) are the base; results-page chairs not found there are added
(name, institution, agency, title, competition only). Method 5 (static HTML).
Ladder item 0 checked 2026-09-30: no export on the site.

Not published per chair: award amount, start/end dates, award number. So
amount/dates are NULL and funder_scheme names the competition.

funder_award_id: synthetic, prefixed so it cannot collide with the ~299
acknowledgement-derived CERC ids already in openalex_awards_raw:
    CERC-C{competition}-{name-slug}

Output: s3://openalex-ingest/awards/cerc/cerc_chairs.parquet
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
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). No-op on
# Linux/Databricks. See runbook §1.2.
import sys as _sys_utf8  # _sys_utf8 is sys: this block is sys.stdout.reconfigure(...) + file-I/O utf-8 defaults
try:
    _sys_utf8.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    _sys_utf8.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if _sys_utf8.platform == "win32":
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

BASE = "https://www.cerc.gc.ca"
PAGES = {
    "current": BASE + "/chairholders-titulaires/index-eng.aspx",
    "former": BASE + "/chairholders-titulaires/former-ancien-eng.aspx",
}
RESULTS_URL = BASE + "/results-resultats/index-eng.aspx"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/cerc/cerc_chairs.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 1.0
RETRIES = 4


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=120)
            if r.status_code == 200:
                r.encoding = "utf-8"
                time.sleep(REQUEST_DELAY)
                return r.text
            last_err = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after {RETRIES} tries: {last_err}")


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<br\s*/?>|</p>", "\n", fragment)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"[ \t\r\f\v]+", " ", t)
    t = re.sub(r"\s*\n\s*", "\n", t).strip()
    return t or None


def fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", s).strip()


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", fold(s)).strip("-")


def split_with_surname(full: str | None, surname: str | None) -> tuple[str | None, str | None]:
    """Use the site's own surname span ("Di Marzo") when the full name ends with it;
    otherwise the canonical last-token rule (runbook §2.4.1, wolf_to_s3.py)."""
    if not full:
        return None, None
    full = re.sub(r"\s+", " ", full).strip()
    if surname and full.endswith(surname.strip()) and len(full) > len(surname.strip()):
        return full[: -len(surname.strip())].strip() or None, surname.strip()
    tokens = re.sub(r"^(?:dr|prof)\.?\s+", "", full, flags=re.I).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_chairholders(page: str, status: str, url: str) -> list[dict]:
    out = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.S):
        head = re.search(r"<p>\s*<strong>(.*?)</strong>\s*<br\s*/?>(.*?)</p>", tr, re.S)
        if not head:
            continue
        surname = re.search(r'<span class="wb-(?:hide|inv)">(.*?)</span>', tr, re.S)
        comp = re.search(r"/chairholders/comp(\d+)/", tr)
        inst, _, agency = (text(head.group(2)) or "").partition("|")
        title = re.search(r'class="color-red"[^>]*>(.*?)</(?:p|span)>', tr, re.S)
        title_txt = text(title.group(1)) if title else None
        body = tr[head.end():].split("Related")[0]
        paras = [text(x) for x in re.findall(r"<p[^>]*>(.*?)</p>", body, re.S)]
        desc = [x for x in paras if x and x != title_txt]
        full = text(head.group(1))
        sn = text(surname.group(1)) if surname else None
        given, family = split_with_surname(full, sn)
        out.append({
            "source": f"chairholders_{status}",
            "status": status,
            "competition": comp.group(1) if comp else None,
            "chairholder": full,
            "lead_given_name": given,
            "lead_family_name": family,
            "institution": inst.strip() or None,
            "agency": agency.strip() or None,
            "chair_title": title_txt,
            "description": "\n\n".join(desc) or None,
            "landing_page_url": url,
        })
    return out


def parse_results(page: str) -> list[dict]:
    """Competition sections (h2 "Competition N"); each chair is a <p> holding
    <a href="...?filter=Surname"><strong>Name</strong></a><br/> plus two lines:
    "Institution | AGENCY" and the chair title (Competitions 3-4), or the chair
    title and the institution (Competition 2)."""
    out = []
    secs = list(re.finditer(r"<h2[^>]*>\s*Competition\s*(?:&nbsp;)?(\d+)\s*</h2>", page))
    for i, s in enumerate(secs):
        end = secs[i + 1].start() if i + 1 < len(secs) else page.find("Date modified", s.end())
        block = page[s.end(): end]
        for m in re.finditer(r"<p[^>]*>(.*?)</p>", block, re.S):
            para = m.group(1)
            a = re.search(r'href="[^"]*filter=([^"]+)"[^>]*>\s*<strong>(.*?)</strong>', para, re.S)
            if not a:
                continue
            lines = [text(x) for x in re.split(r"<br\s*/?>", para[a.end():])]
            lines = [x for x in lines if x and x != "</a>" and x.strip("<>/a ")]
            title = next((x for x in lines if "Chair in" in x), None)
            inst_ag = next((x for x in lines if "|" in x), None)
            if inst_ag:
                inst, _, agency = inst_ag.partition("|")
            else:
                inst = next((x for x in lines if x != title), None) or ""
                agency = ""
            full = text(a.group(2))
            surname = text(requests.utils.unquote(a.group(1)).replace("+", " "))
            given, family = split_with_surname(full, surname)
            out.append({
                "source": "competition_results",
                "status": None,
                "competition": s.group(1),
                "chairholder": full,
                "lead_given_name": given,
                "lead_family_name": family,
                "institution": inst.strip() or None,
                "agency": agency.strip() or None,
                "chair_title": title,
                "landing_page_url": RESULTS_URL,
            })
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Canada Excellence Research Chairs -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="keep only the first N chairs (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    rows = []
    for status, url in PAGES.items():
        got = parse_chairholders(get(url), status, url)
        log(f"{status} chairholders: {len(got)}")
        if not got:
            raise SystemExit(f"no chairs parsed from {url}; layout changed?")
        rows += got
    base = pd.DataFrame(rows)
    res = pd.DataFrame(parse_results(get(RESULTS_URL)))
    log(f"competition results entries: {len(res)} {res['competition'].value_counts().to_dict() if len(res) else {}}")

    base["name_key"] = base["chairholder"].fillna("").map(slug)
    if len(res):
        res["name_key"] = res["chairholder"].fillna("").map(slug)
        extra = res[(res["name_key"] != "") & ~res["name_key"].isin(set(base["name_key"]))].copy()
        if len(extra):
            split = [split_with_surname(n, None) for n in extra["chairholder"]]
            extra["lead_given_name"] = [s[0] for s in split]
            extra["lead_family_name"] = [s[1] for s in split]
            log(f"  results-only chairs added: {len(extra)} {extra['chairholder'].tolist()}")
        # fill a missing competition number from the results page
        comp_by_name = dict(zip(res["name_key"], res["competition"]))
        base["competition"] = base["competition"].fillna(base["name_key"].map(comp_by_name))
        df = pd.concat([base, extra], ignore_index=True)
    else:
        df = base
    if args.limit:
        df = df.head(args.limit)
    df = df[df["name_key"] != ""]
    df["funder_award_id"] = [f"CERC-C{c or 'X'}-{n}" for c, n in zip(df["competition"], df["name_key"])]
    df["funder_scheme"] = df["competition"].map(lambda c: f"Canada Excellence Research Chairs, Competition {c}" if c else "Canada Excellence Research Chairs")
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    log(f"Total {len(df)} chairs; by competition {df['competition'].value_counts(dropna=False).to_dict()}")
    for c in ["chair_title", "institution", "agency", "description", "lead_family_name", "competition"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")  # runbook §1.2 item 5
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "cerc_chairs.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_cerc_chairs.parquet"
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(df)}")
        if len(df) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
