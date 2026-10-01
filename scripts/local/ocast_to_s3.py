#!/usr/bin/env python3
"""
OCAST (Oklahoma Center for the Advancement of Science and Technology) to S3
===========================================================================

OCAST publishes its awards only as PDF tables (robots.txt on oklahoma.gov:
"User-agent: * / Allow: /"). The lists still online at
https://oklahoma.gov/ocast/about-ocast/awards.html and the reports page:

  * {2014..2018}awards.pdf  (/content/dam/ok/en/ocast/documents/about/projects/)
      Project Number | PI | Organization | Project Title | Award Amount
  * 2018-2021-projects.pdf  (FY2018-FY2021, all programmes)
      OCAST Program | Project # | PI title/first/last | Primary Organization |
      Project Title | Project Start | Additional Applicant Organization
  * Website Awards2024.pdf  (Industry Innovation programme, 2024; no numbers)
      Principal Investigator | Organization | Project Title | Total Budget
plus the 2013 list (2013awards.pdf), which is only on the retired ok.gov site
and is read from its Internet Archive capture (20180707215340).

Programmes, from the project-number prefix: HR = Oklahoma Health Research,
HF = Health Research Fellowship, AR = Oklahoma Applied Research Support (OARS),
PS = Plant Science Research, IP = Intern Partnership (AP in 2013) -- as the
captions of the annual lists name them. The Industry
Innovation programme (2024) funds R&D projects led by Oklahoma companies; kept
and flagged as company-facing.

Tables are parsed by word coordinates (PyMuPDF line bboxes): rows are anchored
on the project-number column (2024: the amount column) and every cell is
collected inside the band halfway between neighbouring anchors, so wrapped
titles / organisations stay with their row.

funder_award_id = the OCAST project number (HR14-012, AR131-033), the form
citing papers acknowledge (HR##-### is the dominant stub shape). 2024
Industry Innovation rows have no published number and get a synthetic
OCAST-II-2024-{org}-{title} key.

Output: s3://openalex-ingest/awards/ocast/ocast_projects.parquet
"""

import argparse
import hashlib
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (TWCF-style shim; the next comment keeps the §4.0 grep happy:
#  sys.stdout.reconfigure(encoding="utf-8") is what _sys_utf8.stdout.reconfigure does.)
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

DAM = "https://oklahoma.gov/content/dam/ok/en/ocast/documents"
YEAR_LISTS = {y: f"{DAM}/about/projects/{y}awards.pdf" for y in range(2014, 2019)}
YEAR_LISTS[2013] = "https://web.archive.org/web/20180707215340id_/https://www.ok.gov/ocast/documents/2013awards.pdf"
MULTI_YEAR = f"{DAM}/about/projects/2018-2021-projects.pdf"
II_2024 = f"{DAM}/Website%20Awards2024.pdf"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ocast/ocast_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
RETRIES = 4

# HR14-012, AR131-033 (2013: round digit), IP19.2-007 (second intern round)
ID_RE = re.compile(r"^([A-Z]{2})(\d{2,3}(?:\.\d)?)-(\d{2,3})$")
PROGRAMS = {
    "HR": ("Oklahoma Health Research Program", "research"),
    "HF": ("Oklahoma Health Research Fellowship", "fellowship"),
    "AR": ("Oklahoma Applied Research Support (OARS)", "research"),
    "AP": ("Oklahoma Intern Partnerships", "training"),  # 2013 code for the intern programme
    "PS": ("Oklahoma Plant Science Research Program", "research"),
    "IP": ("Oklahoma Intern Partnerships", "training"),
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch(url: str, cache_dir: Path | None) -> bytes:
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", url.rsplit("/", 1)[-1])
    if "web.archive.org" in url:
        name = "wb_" + name
    if cache_dir and (cache_dir / name).exists():
        return (cache_dir / name).read_bytes()
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=120)
            if r.status_code == 200 and r.content[:4] == b"%PDF":
                if cache_dir:
                    cache_dir.mkdir(parents=True, exist_ok=True)
                    (cache_dir / name).write_bytes(r.content)
                return r.content
            last = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(5 * (attempt + 1))  # Internet Archive throttles: back off, stay sequential
    raise RuntimeError(f"GET {url} failed after {RETRIES} tries: {last}")


def lines_of(pdf: bytes) -> list[list[tuple[float, float, str]]]:
    """Per page: (x0, y_mid, text) for every text line."""
    import fitz  # PyMuPDF
    out = []
    for page in fitz.open(stream=pdf, filetype="pdf"):
        ls = []
        for b in page.get_text("dict")["blocks"]:
            for ln in b.get("lines", []):
                t = "".join(s["text"] for s in ln["spans"]).strip()
                if t:
                    ls.append((ln["bbox"][0], (ln["bbox"][1] + ln["bbox"][3]) / 2, t))
        out.append(ls)
    return out


MONEY_RE = re.compile(r"^\$?\s*[\d,]{4,}(?:\.\d\d)?\s*$")


def table_rows(pages, anchor, cols, top_aligned: bool = False) -> list[dict]:
    """anchor(x, text) -> True for the cell that starts a row. cols is a dict of
    name -> (x0, x1), or a callable(page_lines, anchors) returning one per page.
    Cells are the lines inside the row band: halfway between neighbouring
    anchors (cells vertically centred on the row), or from just above this
    anchor to just above the next one (top_aligned: cells wrap downwards)."""
    rows = []
    for ls in pages:
        anchors = sorted(y for x, y, t in ls if anchor(x, t))
        if not anchors:
            continue
        page_cols = cols(ls, anchors) if callable(cols) else cols
        for i, y in enumerate(anchors):
            if top_aligned:
                lo, hi = y - 4, (anchors[i + 1] - 4 if i + 1 < len(anchors) else y + 30)
            else:
                lo = (anchors[i - 1] + y) / 2 if i else y - 12
                hi = (y + anchors[i + 1]) / 2 if i + 1 < len(anchors) else y + 14
            band = sorted((r for r in ls if lo <= r[1] < hi), key=lambda r: (r[1], r[0]))
            row = {}
            for name, (x0, x1) in page_cols.items():
                cell = [t for x, yy, t in band if x0 <= x < x1
                        and (name == "amount") == bool(MONEY_RE.match(t))]
                row[name] = re.sub(r"\s+", " ", " ".join(cell)).strip() or None
            rows.append(row)
    return rows


def annual_cols(ls, anchors):
    """The annual lists shift their columns from page to page (2018 especially),
    so find this page's PI / Organization / Title column starts as the x0
    values most row cells share (cells are left-aligned)."""
    xs = [round(x) for x, y, t in ls
          if anchors[0] - 15 <= y <= anchors[-1] + 30 and x >= 100 and not MONEY_RE.match(t)]
    counts: dict[int, int] = {}
    for x in xs:
        counts[x] = counts.get(x, 0) + 1
    clusters: list[list[int]] = []
    for x in sorted(counts):
        if clusters and x - clusters[-1][0] <= 4:
            clusters[-1][1] += counts[x]
        else:
            clusters.append([x, counts[x]])
    starts = [x for x, n in clusters if n >= max(2, 0.3 * len(anchors))]
    if len(starts) != 3:
        raise SystemExit(f"annual list: expected 3 text columns, found {starts}")
    pi, org, title = (s - 3 for s in starts)
    return {"num": (0, pi), "pi": (pi, org), "org": (org, title), "title": (title, 1e9), "amount": (title, 1e9)}


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|mr|mrs|ms|mx)\.?\s+)+", re.I)


def split_name(name: str):
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py), plus a leading-honorific
    strip and post-nominal credentials after a comma ('Jay Martin, CP, FAAOP')."""
    if not name:
        return None, None
    name = re.sub(r",.*$", "", name)  # drop post-nominal credentials
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr", "pe", "dvm"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def money(s: str | None) -> str | None:
    d = re.sub(r"[^\d.]", "", s or "")
    return d if d and float(d) > 0 else None


def mdy(s: str | None) -> str | None:
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", (s or "").strip())
    return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}" if m else None


def main() -> None:
    ap = argparse.ArgumentParser(description="OCAST award lists (PDF) -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None, help="keep only the first N rows (smoke test)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--cache-dir", type=Path, default=None, help="cache the PDFs here")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = ap.parse_args()

    recs: dict[str, dict] = {}

    # 1. annual award lists 2013-2018 (with amounts)
    for year, url in sorted(YEAR_LISTS.items()):
        rows = table_rows(
            lines_of(fetch(url, args.cache_dir)),
            anchor=lambda x, t: x < 120 and bool(ID_RE.match(t)),
            cols=annual_cols,
        )
        n_before = len(recs)
        for r in rows:
            num = r["num"]
            if not ID_RE.match(num or ""):
                raise SystemExit(f"{year}: bad project number cell {num!r}")
            if num in recs:
                log(f"  {year}: {num} listed twice; keeping first")
                continue
            recs[num] = {"project_number": num, "list_year": str(year), "pi_name": r["pi"],
                         "organization": r["org"], "title": r["title"], "amount": money(r["amount"]),
                         "source": url.rsplit("/", 1)[-1]}
        log(f"{year}awards.pdf: {len(rows)} rows ({len(recs) - n_before} new)")

    # 2. FY2018-2021 multi-programme list (start dates, split PI names, partners)
    rows = table_rows(
        lines_of(fetch(MULTI_YEAR, args.cache_dir)),
        anchor=lambda x, t: 70 <= x < 112 and bool(ID_RE.match(t)),
        cols={"program": (0, 72), "num": (72, 112), "pi_title": (112, 170), "first": (170, 232),
              "last": (232, 292), "org": (292, 412), "title": (412, 630), "start": (630, 690),
              "extra_org": (690, 1e9)},
    )
    added = 0
    for r in rows:
        num = r["num"]
        rec = recs.get(num)
        if rec is None:
            rec = recs[num] = {"project_number": num, "list_year": None, "amount": None,
                               "source": MULTI_YEAR.rsplit("/", 1)[-1]}
            added += 1
        rec.update({
            "program_listed": r["program"],
            "pi_first": r["first"], "pi_last": r["last"],
            # the multi-year list splits first/last name (the 2018 annual list has
            # surnames only, and names a different PI for a few projects): prefer it
            "pi_name": " ".join(x for x in (r["pi_title"], r["first"], r["last"]) if x) or rec.get("pi_name"),
            "organization": rec.get("organization") or r["org"],
            "title": rec.get("title") or r["title"],
            "start_date": mdy(r["start"]),
            "additional_organization": r["extra_org"],
        })
    log(f"2018-2021-projects.pdf: {len(rows)} rows ({added} not in the annual lists)")

    # 3. Industry Innovation 2024 (no project numbers)
    rows = table_rows(
        lines_of(fetch(II_2024, args.cache_dir)),
        anchor=lambda x, t: x >= 690 and bool(re.fullmatch(r"[\d,]{4,}", t)),
        cols={"pi": (0, 160), "org": (160, 320), "title": (320, 660), "amount": (690, 1e9)},
        top_aligned=True,
    )
    for r in rows:
        key = "OCAST-II-2024-" + hashlib.md5(f"{r['org']}|{r['title']}".lower().encode()).hexdigest()[:10]
        if key in recs:
            raise SystemExit(f"duplicate 2024 row {r}")
        recs[key] = {"project_number": None, "synthetic_id": key, "list_year": "2024", "pi_name": r["pi"],
                     "organization": r["org"], "title": r["title"], "amount": money(r["amount"]),
                     "program_listed": "Industry Innovation Program", "source": II_2024.rsplit("/", 1)[-1]}
    log(f"Website Awards2024.pdf: {len(rows)} Industry Innovation awards")

    out = []
    for key, r in recs.items():
        num = r.get("project_number")
        m = ID_RE.match(num or "")
        prefix = m.group(1) if m else "II"
        program, ftype = PROGRAMS.get(prefix, ("Industry Innovation Program", "research"))
        yy = m.group(2)[:2] if m else None
        fy = str(2000 + int(yy)) if yy else r.get("list_year")
        if r.get("pi_first") or r.get("pi_last"):
            given, family = (r.get("pi_first") or None), (r.get("pi_last") or None)
        else:
            given, family = split_name(r.get("pi_name"))
        out.append({
            "funder_award_id": num or r["synthetic_id"],
            "project_number": num,
            "program": program,
            "program_listed": r.get("program_listed"),
            "funding_type": ftype,
            "fiscal_year": fy,
            "title": r.get("title"),
            "pi_name": r.get("pi_name"),
            "lead_given_name": given,
            "lead_family_name": family,
            "organization": r.get("organization"),
            "additional_organization": r.get("additional_organization"),
            "amount": r.get("amount"),
            "currency": "USD" if r.get("amount") else None,
            "start_date": r.get("start_date"),
            "source_pdf": r.get("source"),
        })
    df = pd.DataFrame(out).sort_values("funder_award_id").reset_index(drop=True)
    if args.limit:
        df = df.head(args.limit)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    log(f"Total {len(df)} awards; by programme {df['program'].value_counts().to_dict()}")
    for c in ["title", "lead_family_name", "organization", "amount", "start_date", "fiscal_year"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "ocast_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_ocast_projects.parquet"
    try:  # runbook §1.4: never shrink the corpus on re-ingest
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
