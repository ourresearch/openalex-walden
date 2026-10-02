#!/usr/bin/env python3
"""
Royal Geographical Society (with IBG) grants to S3
==================================================

Source (ladder item 0, the funder's own bulk export): "Full list of projects
supported since 1953", linked from the Society's "Projects supported" page
(https://www.rgs.org/exploration/grants/projects-supported) as
https://doi.org/10.17605/OSF.IO/4TH85, an OSF project holding one spreadsheet,
"RGS-IBG grant recipients_OSF_1953-2026.xlsx" (sheet "List": Name, Institution,
Project title, Year, Grant, Research Location, Abstract; updated annually, last
2026-09-09). It is downloaded from the OSF file's public download link
(osf.io/download/mktsq/). api.osf.io disallows all crawling in its robots.txt,
so the OSF API is NOT used; osf.io/download/ is allowed.

"Grant" names the scheme (Small Research Grant, Postgraduate Research Award,
Geographical Fieldwork Grant, Dudley Stamp Memorial Award, Monica Cole, Ralph
Brown Expedition Award, Gino Watkins, Thesiger-Oman ...) or, for older rounds,
the fund or sponsor that paid (Wolfson Fund, Barclays Bank, Mount Everest
Foundation, abbreviations such as MAR / GMT / HRM ...); it is kept verbatim as
funder_scheme. All rows are RGS-IBG grants (the sponsor funds were
administered and awarded by the Society).

Scope (batch brief: expedition and fieldwork research grants are kept):
EXCLUDED: Frederick Soddy Schools Award and Innovative Geography Teaching
Grant(s) (grants to schoolteachers for pupils' fieldwork: education, not
research), the sheet's "TOTAL ..." summary row, and the one project marked
"Asked to re-apply following reconnaissance" (not an award). Exact duplicate
rows (same name, title, year, grant) are dropped. Kept and flagged:
"Approval" (expeditions the Society approved and supported, 1970s-80s), Ray Y
Gildea Jr Award (geography-education research), 'From the Field' awards,
Journey in Audio.

Names: 'Name' can hold one person ('Dr J Darch'), several ('S.J. Sole, S. Lowe,
...', 'A & B') or a team ('Derbyshire Himalayan Expedition 1961'). The first
person is the lead (honorifics dropped), the second the co-lead; team names
give no lead (kept in lead_raw).

No grant reference is published (citing works quote RGS's internal refs such
as "SRG 23.13", "PRA 13.24"), so funder_award_id is synthetic:
RGS-{year}-{name-slug}-{title-slug}.

Output: s3://openalex-ingest/awards/rgs/rgs_projects.parquet
"""

import argparse
import json
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


# (self-check marker: this block is the runbook sys.stdout.reconfigure shim, with sys renamed)
# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). This
# crashes scrapers writing names with non-ASCII chars. Production runs on
# Linux/Databricks where UTF-8 is the default; this fixes local validation on
# Windows. See runbook §1.2.
import sys as _sys_utf8
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


XLSX_URL = "https://osf.io/download/mktsq/"  # OSF project 4th85 (doi:10.17605/OSF.IO/4TH85)
LANDING = "https://doi.org/10.17605/OSF.IO/4TH85"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/rgs/rgs_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
MIN_ROWS = 3000  # the 2026-09 file has 3,462 rows

EXCLUDE_GRANT = re.compile(r"Frederick Soddy Schools|Teaching Grant|^TOTAL\b|re-apply", re.I)
FLAG_GRANT = re.compile(r"^Approval|Gildea|From the Field|Journey in Audio", re.I)
TEAM_WORDS = re.compile(r"\b(?:Expedition|University|Society|School|Club|Team|College|Trust|Project|Association|"
                        r"Polytechnic|Institute|Survey|Group|Navy|Army|Services|\d{4})\b", re.I)
NOT_A_PERSON = re.compile(r"\b(?:students?|members?|others?|undergraduates?|pupils?|servicemen|academics?|team|"
                          r"geologists?|TBC)\b", re.I)
HONORIFICS = re.compile(r"^(?:(?:Dr|Mr|Mrs|Ms|Miss|Prof|Professor|Sir|Dame|Lord|Lady|Rev|Revd|Capt|Captain|Lt|Lieut|"
                        r"Major|Maj|Col|Cdr|Commander|Sqn Ldr|Flt Lt|Brigadier|Gen|Hon)\.?\s+)+", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch(url: str, retries: int = 3) -> bytes:
    last_err = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=180)
            r.raise_for_status()
            return r.content
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def tidy(s) -> str | None:
    if s is None or (isinstance(s, float) and s != s):
        return None
    s = re.sub(r"\s+", " ", str(s).replace("\xa0", " ")).strip(" ,;:.")
    return None if not s or s.lower() in ("na", "n/a", "nan", "none", "-") else s


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical wolf_to_s3.py helper (runbook §2.4.1): strip trailing degree /
    suffix tokens, last remaining token = family name; surname particles stay
    with the family name ('Teun De Jong', 'Sobreiro e Cruz')."""
    if not name:
        return None, None
    tokens = [t for t in re.split(r"\s+", name.strip()) if t]
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr", "obe", "mbe", "cbe"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    given, family = tokens[:-1], tokens[-1]
    while len(given) > 1 and given[-1].lower() in {"de", "del", "della", "di", "da", "van", "von", "der", "la", "le",
                                                    "el", "al", "e", "dos", "das", "du"}:
        family = given.pop() + " " + family
    return " ".join(given), family


def people_of(raw: str | None) -> list[str]:
    """Person names in a Name cell; [] for a team/expedition name."""
    if not raw or re.fullmatch(r"(?i)not given|unknown|anonymous|tbc", raw.strip()):
        return []
    raw = re.sub(r"\(.*?\)", " ", raw)
    # 'X, plus five other team members' / 'X +13 servicemen' / 'rest of team TBC': keep the named people
    raw = re.split(r"\bplus\b|\+|\brest of\b|\b(?:and|with)\s+(?:\w+\s+)?(?:other|additional)\b", raw, flags=re.I)[0]
    if TEAM_WORDS.search(raw):
        return []
    parts = [HONORIFICS.sub("", p.strip(" .")) for p in re.split(r",|;|&|\band\b", raw)]
    return [p for p in parts if p and len(p) > 1 and not re.search(r"\d", p) and not NOT_A_PERSON.search(p)]


def main() -> None:
    p = argparse.ArgumentParser(description="RGS-IBG grant recipients (OSF xlsx) -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the first N rows (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache the xlsx here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    path = (args.cache_dir or args.output_dir) / "rgs_grant_recipients.xlsx"
    if not (args.cache_dir and path.exists()):
        path.parent.mkdir(parents=True, exist_ok=True)
        body = fetch(XLSX_URL)
        path.write_bytes(body)
        log(f"downloaded {len(body) / 1e6:.1f} MB from {XLSX_URL}")
    raw = pd.read_excel(path, sheet_name="List")
    raw.columns = [str(c).strip() for c in raw.columns]
    need = {"Name", "Institution", "Project title", "Year", "Grant"}
    if not need <= set(raw.columns):
        raise SystemExit(f"unexpected columns {raw.columns.tolist()}")
    if len(raw) < MIN_ROWS:
        raise SystemExit(f"only {len(raw)} rows (expected >= {MIN_ROWS}); file changed?")
    log(f"sheet 'List': {len(raw)} rows, years {raw['Year'].min()}-{raw['Year'].max()}")
    if args.limit:
        raw = raw.head(args.limit)

    rows = []
    for r in raw.to_dict("records"):
        grant = tidy(r.get("Grant"))
        rec = {"name_raw": tidy(r.get("Name")), "institution": tidy(r.get("Institution")),
               "title": tidy(r.get("Project title")), "year": str(int(r["Year"])) if r.get("Year") == r.get("Year") else None,
               "grant": grant, "research_location": tidy(r.get("Research Location")),
               "abstract": tidy(r.get("Abstract"))}
        rows.append(rec)
    df = pd.DataFrame(rows)
    excluded = df["grant"].fillna("").str.contains(EXCLUDE_GRANT)
    log(f"excluded {int(excluded.sum())}: {df.loc[excluded, 'grant'].value_counts().to_dict()}")
    df = df[~excluded & df["title"].notna()].copy()
    before = len(df)
    df = df.drop_duplicates(["name_raw", "title", "year", "grant"])
    log(f"dropped {before - len(df)} exact duplicate rows")

    ppl = [people_of(n) for n in df["name_raw"]]
    lead = [split_name(x[0]) if x else (None, None) for x in ppl]
    co = [split_name(x[1]) if len(x) > 1 else (None, None) for x in ppl]
    df["lead_given_name"] = [g for g, _ in lead]
    df["lead_family_name"] = [f for _, f in lead]
    df["co_given_name"] = [g for g, _ in co]
    df["co_family_name"] = [f for _, f in co]
    df["investigators_json"] = [json.dumps([dict(zip(("given", "family"), split_name(n))) for n in x], ensure_ascii=False)
                                if len(x) > 1 else None for x in ppl]
    df["flag"] = df["grant"].fillna("").map(lambda g: "kept_flagged" if FLAG_GRANT.search(g) else None)

    base = [f"RGS-{y}-{slug(n or '')[:28]}-{slug(t)[:28]}".replace("--", "-")
            for y, n, t in zip(df["year"], df["name_raw"], df["title"])]
    df["funder_award_id"] = base
    dup = df["funder_award_id"].str.lower().duplicated(keep=False)
    df.loc[dup, "funder_award_id"] = df.loc[dup, "funder_award_id"] + "-" + df.loc[dup, "grant"].fillna("x").map(lambda g: slug(g)[:20])
    dup = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dup.any():
        df.loc[dup, "funder_award_id"] = df.loc[dup, "funder_award_id"] + "-" + \
            df.loc[dup].groupby("funder_award_id").cumcount().add(1).astype(str)
    if df["funder_award_id"].str.lower().duplicated().any():
        raise SystemExit("duplicate funder_award_id after disambiguation")
    df["source_url"] = LANDING

    log(f"rows: {len(df)}, years {df['year'].min()}-{df['year'].max()}, flagged {df['flag'].notna().sum()}")
    for c in ["title", "lead_family_name", "institution", "grant", "abstract"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "rgs_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_rgs_projects.parquet"
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
