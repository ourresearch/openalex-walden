#!/usr/bin/env python3
"""
Cancer Research Society (Société de recherche sur le cancer, Canada) to S3
==========================================================================

The Cancer Research Society (CRS) publishes the results of each annual
Operating Grants competition as a PDF table (Chercheur / Titre du projet /
Montant accordé): PI "Family, Given", institution, project title, amount
"120 000 $/ 2 ans", and, for co-funded grants, a "Financé en partenariat
avec ..." note naming the partner (CIHR institutes, Fondation du cancer du
sein du Québec, named CRS donor funds, ...).

Source: the French results PDFs CRS hosts on its own WordPress site
(cancerresearchsociety.ca/content/uploads/2024/04/OG-Resultats-{2015..2023}.pdf,
enumerated via the public WP media API). The English versions and the
2024/2025 results are only published as app.box.com shared links, whose
download endpoint robots.txt disallows for all user agents, so they are not
fetched here (see the notebook header for the gap).

Parsing: each page's header row gives the three column x-positions; every
amount line in the third column anchors one grant, and all lines between it
and the next anchor are assigned to that grant by column.

Output: s3://openalex-ingest/awards/cancer_research_society/cancer_research_society_projects.parquet
"""

import argparse
import hashlib
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). See runbook §1.2.
# (grep anchor for the §4.0 self-check: sys.stdout.reconfigure)
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

SITE = "https://cancerresearchsociety.ca"
MEDIA_API = f"{SITE}/wp-json/wp/v2/media"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/cancer_research_society/cancer_research_society_projects.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"}
RETRIES = 3

CELL_GAP = 18  # pt; line spacing inside a cell is ~10-16 pt, gaps between grants are >= 25 pt
AMOUNT_RE = re.compile(  # "120 000 $/ 2 ans" (fr) or "$119,899/ 2 years" (en, one 2017 row)
    r"(?:\$\s*(\d[\d\s  .,]*\d)|(\d[\d\s  .,]*\d)\s*\$)\s*/\s*(\d+)\s*(?:an|year)", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, **kw) -> requests.Response:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=120, **kw)
            log(f"GET {r.url} -> {r.status_code} ({len(r.content)} bytes)")
            r.raise_for_status()
            return r
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def result_pdfs() -> dict[int, str]:
    """year -> URL of 'OG-Resultats-{year}.pdf' from the WP media library."""
    out = {}
    items = get(MEDIA_API, params={"search": "OG-Resultats", "per_page": 100}).json()
    for it in items:
        m = re.search(r"/OG-Resultats-(\d{4})\.pdf$", it.get("source_url", ""))
        if m:
            out[int(m.group(1))] = it["source_url"]
    return dict(sorted(out.items()))


def norm(s: str) -> str:
    s = s.replace(" ", " ").replace(" ", " ").replace("’", "'")
    return re.sub(r"\s+", " ", s).strip()


HONORIFIC_RE = re.compile(r"^(?:(?:dr|dre|prof|professor|mr|mrs|ms)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py), used for the rare
    'Given Family' cell that lacks the usual 'Family, Given' comma."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def parse_pi(cell: str) -> tuple[str | None, str | None]:
    """'Bahlis, Nizar Jacques' -> ('Nizar Jacques', 'Bahlis')."""
    cell = norm(HONORIFIC_RE.sub("", cell))
    parts = [p.strip() for p in re.split(r"\s*,\s*", cell, maxsplit=1)]
    if len(parts) == 2 and parts[0] and parts[1]:
        return parts[1], parts[0]
    return split_name(cell)


def page_lines(page) -> list[dict]:
    out = []
    for b in page.get_text("dict")["blocks"]:
        for ln in b.get("lines", []):
            t = norm("".join(s["text"] for s in ln["spans"]))
            if t:
                out.append({"x0": ln["bbox"][0], "y0": ln["bbox"][1], "x1": ln["bbox"][2], "y1": ln["bbox"][3], "text": t})
    return out


def blocks(lines: list[dict], gap: float = None) -> list[list[dict]]:
    """Group lines of one column into blocks of consecutive lines (y gap <= CELL_GAP)."""
    gap = CELL_GAP if gap is None else gap
    out = []
    for l in sorted(lines, key=lambda l: l["y0"]):
        if out and l["y0"] - out[-1][-1]["y0"] <= gap:
            out[-1].append(l)
        else:
            out.append([l])
    return out


def centre(blk: list[dict]) -> float:
    return (blk[0]["y0"] + blk[-1]["y1"]) / 2


def nearest(centres: list[float], y: float) -> int:
    return min(range(len(centres)), key=lambda i: abs(centres[i] - y))


def parse_pdf(year: int, url: str, data: bytes) -> list[dict]:
    import fitz  # PyMuPDF

    doc = fitz.open(stream=data, filetype="pdf")
    rows = []
    for pno in range(doc.page_count):
        lines = page_lines(doc[pno])
        hdr_c = next((l for l in lines if l["text"].startswith("Chercheur")), None)
        hdr_t = next((l for l in lines if l["text"].startswith("Titre du projet")), None)
        hdr_m = next((l for l in lines if l["text"].startswith("Montant accord")), None)
        if not hdr_c or not hdr_t or not hdr_m:
            continue  # cover/statistics page
        # Column boundaries halfway between the header cells: the body text is
        # left-aligned up to ~20 pt left of its (centred) header on some pages.
        x_title = (hdr_c["x0"] + hdr_t["x0"]) / 2
        x_amt = (hdr_t["x0"] + hdr_m["x0"]) / 2
        y_top = hdr_t["y0"] + 8
        body = [l for l in lines if l["y0"] > y_top and not re.match(
            r"^(Page \d+|Recherche\w*Cancer\.ca|subventions@|Résultats du concours)", l["text"], re.I)]
        # Rows are anchored on the PI cell (column 1). Table rows are
        # vertically centred, so the amount/partner block (column 3) and the
        # title block (column 2) can start above or below the PI cell: group
        # each column into blocks of consecutive lines and give every block to
        # the PI cell whose vertical centre is nearest.
        col1 = [l for l in body if l["x0"] < x_title]
        col2 = [l for l in body if x_title <= l["x0"] < x_amt]
        col3 = []
        for blk in blocks([l for l in body if l["x0"] >= x_amt]):
            # one block per amount line (a block never holds two grants' amounts)
            cur = []
            for l in blk:
                if AMOUNT_RE.search(l["text"]) and any(AMOUNT_RE.search(c["text"]) for c in cur):
                    col3.append(cur)
                    cur = []
                cur.append(l)
            col3.append(cur)
        cells = blocks(col1)
        if not cells:
            continue
        centres = [centre(c) for c in cells]
        titles = {i: [] for i in range(len(cells))}
        amounts = {i: [] for i in range(len(cells))}
        for blk in blocks(col2):
            titles[nearest(centres, centre(blk))] += blk
        for blk in col3:
            amounts[nearest(centres, centre(blk))].append(blk)
        for i, cell in enumerate(cells):
            pi_cell = cell[0]["text"]
            c3 = [l for blk in amounts[i] for l in blk]
            amt = [l for l in c3 if AMOUNT_RE.search(l["text"])]
            if len(amt) != 1:
                raise RuntimeError(f"{year} p{pno + 1}: PI cell {pi_cell!r} got {len(amt)} amount lines")
            m = AMOUNT_RE.search(amt[0]["text"])
            amount = parse_amount(m.group(1) or m.group(2))
            if "," not in pi_cell:
                log(f"  WARNING {year} p{pno + 1}: PI cell without 'Family, Given' comma: {pi_cell!r}")
            given, family = parse_pi(pi_cell)
            note = norm(" ".join(l["text"] for l in c3 if l is not amt[0])) or None
            rows.append({
                "competition_year": str(year),
                "pi_raw": pi_cell,
                "lead_given_name": given,
                "lead_family_name": family,
                "institution": norm(" ".join(l["text"] for l in cell[1:])) or None,
                "title": norm(" ".join(l["text"] for l in sorted(titles[i], key=lambda l: l["y0"]))) or None,
                "amount_text": norm(amt[0]["text"]),
                "amount": amount,
                "duration_years": m.group(3),
                "currency": "CAD",
                "partner_note": note,
                "source_pdf": url,
                "source_page": str(pno + 1),
            })
    return rows


def parse_amount(s: str) -> float:
    """'120 000' / '119 426.8' / '119,899' -> float. A trailing 1-2 digit group
    after '.' or ',' is a decimal part; other separators are thousands."""
    s = re.sub(r"[\s  ]", "", s)
    m = re.fullmatch(r"(.*?)[.,](\d{1,2})", s)
    whole, frac = (m.group(1), m.group(2)) if m else (s, "0")
    return float(re.sub(r"[^\d]", "", whole) + "." + frac)


def partner_of(note: str | None) -> str | None:
    """'Financé en partenariat avec la Fondation du cancer du sein du Québec' -> 'Fondation du cancer du sein du Québec'."""
    if not note:
        return None
    m = re.search(r"(?:en partenariat avec|grâce à|par)\s+(?:les\s+|la\s+|le\s+|l['’]\s*)?(.+?)\.?$", note, re.I)
    return norm(m.group(1)) if m else note


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def main() -> None:
    p = argparse.ArgumentParser(description="Cancer Research Society operating-grant results -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only parse the first N competition PDFs")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    pdfs = result_pdfs()
    log(f"Media library: {len(pdfs)} result PDFs: {list(pdfs)}")
    if not pdfs:
        raise SystemExit("no OG-Resultats PDFs found in the media library")
    years = list(pdfs)[: args.limit] if args.limit else list(pdfs)

    rows = []
    for y in years:
        recs = parse_pdf(y, pdfs[y], get(pdfs[y]).content)
        log(f"  {y}: {len(recs)} grants, CAD {sum(r['amount'] for r in recs):,.0f}")
        rows += recs
        time.sleep(1.0)

    df = pd.DataFrame(rows)
    df["partner"] = df["partner_note"].map(partner_of)
    # Source error: one 2021 row (Lévesque) repeats the project title in the
    # institution cell; never ship a title as an affiliation.
    same = df["institution"].fillna("").map(slug) == df["title"].fillna("").map(slug)
    for fid in df.loc[same & df["institution"].notna(), "pi_raw"]:
        log(f"  institution cell repeats the title for {fid}; institution set to NULL")
    df.loc[same, "institution"] = None
    key = df["competition_year"] + "-" + (df["lead_family_name"].fillna("") + " " + df["lead_given_name"].fillna("")).map(slug)
    # Synthetic, stable key (CRS's results PDFs print no grant number):
    # CRS-OG-{competition year}-{family-given}. A PI with two grants in one
    # competition (5 cases 2015-2018) gets a short hash of the title appended.
    multi = key.duplicated(keep=False)
    key = key.where(~multi, key + "-" + df["title"].fillna("").map(
        lambda t: hashlib.md5(slug(t).encode()).hexdigest()[:6]))
    df["funder_award_id"] = "CRS-OG-" + key
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")

    for c in ["title", "institution", "lead_family_name", "amount", "partner_note"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  CIHR/IRSC co-funded: {df['partner_note'].fillna('').str.contains('IRSC|CIHR|Instituts de recherche en santé').sum()}")
    log(f"  total CAD {df['amount'].sum():,.0f}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "cancer_research_society_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_cancer_research_society_projects.parquet"
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
