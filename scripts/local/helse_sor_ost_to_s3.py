#!/usr/bin/env python3
"""
Helse Sør-Øst RHF (South-Eastern Norway Regional Health Authority) to S3
========================================================================

Helse Sør-Øst publishes one consolidated PDF of its regional research-fund
allocations 2011-2023 ("Tildelinger - regionale forskningsmidler 2011 til
2023", 105 pages), assembled from the yearly board-case annexes:
https://www.helse-sorost.no/4aee52/contentassets/7c84e81a540f46809f6b17cb87483707/tildelinger---regionale-forskningsmidler-2011-til-2023.pdf

Each year is a set of tables, grouped by applicant hospital: applicant
(the project leader), project title, application type (doctoral /
postdoctoral / researcher fellowship, open project support, research
network, ...), duration, and the amount allocated for that year (NOK; in
2021-2023 printed in thousands). The layout changes almost every year
(~15 variants), so columns are mapped by header name, and the hospital is
read from the group-header row inside the table (2011, 2013) or from the
text line printed just above each table.

Notes:
- `amount_year` is the allocation for the award year (the first year of the
  project), which is what the lists print. 2021 also prints the commitment
  for the whole project period (`amount_total`).
- 2022/2023 lists are the board's recommendation lists ('innstillingsliste')
  included in the authority's own allocation compilation.
- Some rows are funded via the Samarbeidsorganet (SO) / Ministry (HOD) lines
  that Helse Sør-Øst administers; `funding_line` keeps that column.
- 2024+ allocations are only in later board papers (not parsed: follow-up).

funder_award_id: the lists print no project number (citing works write the
7-digit Helse Sør-Øst project number, e.g. 2017096), so a stable synthetic
key 'HSO-<year>-<leader slug>-<sha1(title)[:6]>' is used. Collisions raise.

Output: s3://openalex-ingest/awards/helse_sor_ost/helse_sor_ost_projects.parquet
"""

import argparse
import hashlib
import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import pdfplumber
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (runbook 4.0 grep marker: sys.stdout.reconfigure)
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). Production
# runs on Linux/Databricks where UTF-8 is the default. See runbook 1.2.
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

PDF_URL = ("https://www.helse-sorost.no/4aee52/contentassets/7c84e81a540f46809f6b17cb87483707/"
           "tildelinger---regionale-forskningsmidler-2011-til-2023.pdf")
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/helse_sor_ost/helse_sor_ost_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}

HOSPITAL_RE = re.compile(r"(\bHF\b|sykehus|Sykehus|universitet|Hospital|\bAS\b|Diakonale|RHF|Klinikk|klinikk|Senter|senter)")
NOT_HOSPITAL_RE = re.compile(r"(SO\s*=|forskningsmidler|Beløp|Søker|Sak \d|nnstilling|vedlegg|Tildel|prioriterte|Prosjekt)", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def norm(s) -> str | None:
    if s is None:
        return None
    s = str(s).replace("­", "").replace("‐", "-").replace("‑", "-")
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def col_role(h: str) -> str | None:
    h = (norm(h) or "").lower()
    if h in ("søker", "søkernavn"):
        return "name"
    if h.startswith("tildelt søknadstyp"):
        return "awarded_type"
    if h.startswith("søknadstype"):
        return "type"
    if h in ("tildelt",):
        return "awarded_type"
    if h.startswith("prosjekt tittel") or h.startswith("prosjekttittel"):
        return "title"
    if h.startswith("søkerinstitusjon") or h == "institusjon":
        return "institution"
    if "varighet" in h:
        return "duration"
    if h.startswith("oppstart"):
        return "start"
    if h.startswith("tilsagn for hele"):
        return "amount_total"
    if re.search(r"(beløp|tildeling 20|tildelt beløp|^20\d\d \()", h):
        return "amount_year"
    if h.startswith("prioritert"):
        return "priority_area"
    if h.startswith("kilde") or h.startswith("tildeling so") or h.startswith("so =") or h.startswith("hod ="):
        return "funding_line"
    return None


def parse_amount(s: str | None, thousands: bool) -> float | None:
    d = re.sub(r"[^\d]", "", s or "")
    if not d:
        return None
    v = float(d)
    if thousands or v < 20000:  # 2021-2023 lists print NOK thousands
        v *= 1000
    return v


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook 2.4.1 helper (wolf_to_s3.py): last token = family."""
    tokens = (name or "").split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def slug(s: str | None) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-") or "x"


def parse_pdf(path: Path, limit_pages: int | None) -> list[dict]:
    rows = []
    roles, compact, year, thousands, hospital, heading_type = None, None, None, False, None, None
    with pdfplumber.open(path) as pdf:
        pages = pdf.pages[:limit_pages] if limit_pages else pdf.pages
        for pno, pg in enumerate(pages):
            # some pages (2012) draw every glyph twice (bold/shadow): 'VVeesssseellaa'
            pg = pg.dedupe_chars()
            text = pg.extract_text() or ""
            first = norm(text.split("\n")[0]) or ""
            page_thousands = bool(re.search(r"i hele 1000|1000 kroner", text))
            prev_bottom = 0
            for t in pg.find_tables():
                data = t.extract()
                # hospital: last hospital-looking text line between the previous table and this one
                x0, y0, x1, _ = pg.bbox
                top = max(prev_bottom, y0)
                above = pg.crop((x0, top, x1, max(t.bbox[1], top + 1)), strict=False).extract_text() or ""
                # the hospital line can also sit inside the table box, above its header row
                inside = (pg.crop(t.bbox, strict=False).extract_text() or "").split("\n")
                head_lines = []
                if any(re.search(r"\bSøker", line) for line in inside):  # only tables that carry a header
                    for line in inside:
                        if re.search(r"\bSøker", line):
                            break
                        head_lines.append(line)
                for line in reversed(above.split("\n") + head_lines):
                    line = re.sub(r"\s*Beløp i hele 1000.*$", "", norm(line) or "")
                    if line and HOSPITAL_RE.search(line) and not NOT_HOSPITAL_RE.search(line):
                        hospital = re.sub(r"\s*Beløp i hele 1000.*$", "", line)
                        break
                prev_bottom = t.bbox[3]
                for r in data:
                    cells = [norm(c) for c in r]
                    found = [col_role(c or "") for c in cells]
                    if "name" in found and ("title" in found or "amount_year" in found):
                        roles = found
                        # continuation pages drop the header's empty spacer columns
                        compact = [f for f, c in zip(found, cells) if c]
                        hdr_amount = next((c for c, f in zip(cells, found) if f == "amount_year"), "") or ""
                        # award year: the amount header ('Beløp 2022'), else a heading just
                        # above the table ('Karrierestipend for 2022'), else the page title
                        # ('fra 1.1.2022'; a board case 'Sak 141-2021' allocates the next year)
                        heading = " ".join((above or "").split("\n")[-2:])
                        ym = (re.search(r"(20\d\d)", hdr_amount) or re.search(r"\bfor (20\d\d)", heading)
                              or re.search(r"fra 1\.1\.(20\d\d)", first))
                        if ym:
                            year = int(ym.group(1))
                        else:
                            sak = re.search(r"Sak \d+-(20\d\d)", first)
                            ym = re.search(r"(20\d\d)(?!.*20\d\d)", first)
                            year = int(sak.group(1)) + 1 if sak else (int(ym.group(1)) if ym else year)
                        heading_type = "Karrierestipend" if re.search(r"Karrierestipend", heading) else None
                        thousands = page_thousands or "1000" in hdr_amount
                        continue
                    if roles is None:
                        continue
                    nonempty = [c for c in cells if c]
                    # group-header row inside the table: just the hospital name
                    if len(nonempty) == 1 and HOSPITAL_RE.search(nonempty[0]) and not NOT_HOSPITAL_RE.search(nonempty[0]):
                        hospital = nonempty[0]
                        continue
                    rec = {}
                    use = compact if len(cells) != len(roles) and len(cells) == len(compact) else roles
                    for c, f in zip(cells, use):
                        if f and c and f not in rec:
                            rec[f] = c
                    if not rec.get("name") or not rec.get("title"):
                        continue
                    if re.match(r"(Søker|Sum|Totalt|Total)\b", rec["name"]):
                        continue
                    rows.append({
                        "year": year, "page": pno + 1, "section": first,
                        "leader": rec.get("name"), "title": rec.get("title"),
                        "application_type": rec.get("type"), "awarded_type": rec.get("awarded_type") or heading_type,
                        "institution": rec.get("institution") or hospital,
                        "duration": rec.get("duration"), "start": rec.get("start"),
                        "amount_year": parse_amount(rec.get("amount_year"), thousands),
                        "amount_total": parse_amount(rec.get("amount_total"), thousands),
                        "priority_area": rec.get("priority_area"), "funding_line": rec.get("funding_line"),
                    })
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description="Helse Sør-Øst regional research-fund allocations -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="parse only the first N PDF pages (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp/helse_sor_ost"))
    p.add_argument("--pdf", type=Path, default=None, help="use a local copy of the PDF")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the 1.4 shrink guard")
    args = p.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    path = args.pdf
    if path is None:
        path = args.output_dir / "tildelinger_2011_2023.pdf"
        r = requests.get(PDF_URL, headers=HEADERS, timeout=120)
        r.raise_for_status()
        path.write_bytes(r.content)
        log(f"downloaded {len(r.content):,} bytes")
    rows = parse_pdf(path, args.limit)
    log(f"{len(rows)} table rows parsed")

    seen, recs, dups = set(), [], 0
    for r in rows:
        k = (r["year"], slug(r["leader"]), slug(r["title"]))
        if k in seen:  # the 2017 list is repeated in a board-case annex
            dups += 1
            continue
        seen.add(k)
        given, family = split_name(r["leader"])
        h = hashlib.sha1(slug(r["title"]).encode()).hexdigest()[:6]
        fid = f"HSO-{r['year']}-{slug(r['leader'])[:40]}-{h}"
        m = re.search(r"(\d+)\s*(?:år|mnd|måneder)", r["duration"] or "")
        months = None
        if m:
            months = int(m.group(1)) * (1 if re.search(r"mnd|måned", r["duration"]) else 12)
        start = None
        sm = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", r["start"] or "")
        if sm:
            start = f"{sm.group(3)}-{sm.group(2)}-{sm.group(1)}"
        recs.append({**r, "funder_award_id": fid, "lead_given_name": given, "lead_family_name": family,
                     "duration_months": months, "start_date": start,
                     "funder_scheme": r["awarded_type"] or r["application_type"]})
    df = pd.DataFrame(recs)
    if dups:
        log(f"dropped {dups} repeated rows (same year, leader, title)")
    dup = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dup.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dup, 'funder_award_id'].tolist()[:10]}")
    log(f"{len(df)} projects; per year {df['year'].value_counts().sort_index().to_dict()}")
    for c in ["title", "amount_year", "amount_total", "institution", "funder_scheme", "duration_months", "start_date"]:
        log(f"  {c:16s} {df[c].notna().mean():6.1%}")
    log(f"  NOK allocated (award year) {df['amount_year'].sum():,.0f}")
    log(f"  schemes {df['funder_scheme'].value_counts().head(12).to_dict()}")
    top = df.groupby(["lead_given_name", "lead_family_name"]).size().sort_values(ascending=False).head(5)
    log(f"  6.4a top PI combos: {top.to_dict()}")

    df = df.astype("string")
    for c in ("amount_year", "amount_total"):
        df[c] = pd.to_numeric(df[c])
    out = args.output_dir / "helse_sor_ost_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"Wrote {len(df)} rows to {out}")
    if args.skip_upload:
        return
    if args.limit:
        raise SystemExit("refusing to upload a --limit smoke file to S3")

    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_helse_sor_ost_projects.parquet"
    try:  # runbook 1.4: never shrink the corpus on re-ingest
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev}, new {len(df)}")
        if len(df) < prev and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(out), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
