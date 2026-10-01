#!/usr/bin/env python3
"""
INdAM-GNFM (Gruppo Nazionale per la Fisica Matematica) Progetti Giovani to S3
==============================================================================

GNFM, one of the national research groups of the Istituto Nazionale di Alta
Matematica "F. Severi" (INdAM), funds one-year "Progetti Giovani GNFM": small
research projects led by a young coordinator (RTD / postdoc / research
contract) with 1-4 participants; the money pays the members' missions and
scientific collaborations.

Source: the group's own approved-project lists, PDFs in the GNFM WordPress
media library on https://www.altamatematica.it/gnfm/ (found via
/gnfm/wp-json/wp/v2/media; the "Archivio: Progetti Giovani GNFM" page lists the
years 2006-2011 but links nothing, and the pre-2016 Drupal site never posted
lists). Only three years are public:

  2018  PROGETTI-GIOVANI-GNFM-2018.pdf            N., title, RESPONSABILE, PARTECIPANTI
  2019  PROGETTI-GIOVANI-GNFM-ANNO-2019-APPROVATI  N., COGNOME, NOME, UNITA' DI RICERCA, TITOLO
  2025  GNFM-PROGETTI-GIOVANI-2025-APPROVATI       COORDINATORE, TITOLO, FINANZIAMENTO, PARTECIPANTE

(PROGETTI-GIOVANI-GNFM-2019.pdf is the 2019 call text, not a list.) The script
re-reads the media library and fails if a new "PROGETTI GIOVANI" PDF appears
that is not in YEAR_PDFS, so a future list is not silently skipped.

Parsing reuses the INdAM-group approach of gnampa_to_s3.py: ruled table rows
from pdfplumber, words assigned to columns by x-position, and coordinator
names resolved against the GNFM members lists (aderenti, COGNOME/NOME given
separately), which also supply the coordinator's institution where the PDF
has none (2018, 2025).

No per-project code is published (citing works write "Progetto Giovani GNFM
2023" or the year's shared INdAM CUP), so funder_award_id is synthetic:
GNFM-{year}-{coordinator-family-slug}.

Output: s3://openalex-ingest/awards/gnfm/gnfm_projects.parquet
"""

import argparse
import html
import json
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import pdfplumber
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


BASE = "https://www.altamatematica.it"
MEDIA_API = f"{BASE}/gnfm/wp-json/wp/v2/media?per_page=100&page={{page}}&_fields=id,source_url,date"
ADERENTI_URL = f"{BASE}/gnfm/aderenti/aderenti-{{year}}/"
UPLOADS = f"{BASE}/gnfm/wp-content/uploads/sites/6"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/gnfm/gnfm_projects.parquet"

# Approved-project lists (static historical files) and their layout.
YEAR_PDFS = {
    2018: f"{UPLOADS}/2019/02/PROGETTI-GIOVANI-GNFM-2018.pdf",
    2019: f"{UPLOADS}/2019/07/PROGETTI-GIOVANI-GNFM-ANNO-2019-APPROVATI.pdf",
    2025: f"{UPLOADS}/2025/08/GNFM-PROGETTI-GIOVANI-2025-APPROVATI.pdf",
}
# Media-library PDFs that mention projects but are not funded lists.
NOT_LISTS = {
    f"{UPLOADS}/2019/04/PROGETTI-GIOVANI-GNFM-2019.pdf",                   # 2019 call (bando) text
    f"{UPLOADS}/2025/08/GNFM-PROGETTI-GIOVANI-2025-APPROVATI-1.pdf",       # byte-identical duplicate upload
}
EXPECTED = {2018: 10, 2019: 12, 2025: 18}  # rows per list (2025: 15 + 3 added after extra funding, "*")

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 1.0
RETRIES = 3

PARTICLES = {"di", "de", "del", "della", "dello", "delle", "degli", "dei", "da", "dal", "dalla",
             "dall'", "d'", "lo", "la", "li", "le", "van", "von", "mc"}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, binary: bool = False):
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            r.raise_for_status()
            if binary:
                return r.content
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def cached(cache_dir: Path | None, name: str, url: str, binary: bool = False):
    path = cache_dir / name if cache_dir else None
    if path and path.exists():
        return path.read_bytes() if binary else path.read_text()
    body = get(url, binary)
    if path:
        cache_dir.mkdir(parents=True, exist_ok=True)
        if binary:
            path.write_bytes(body)
        else:
            path.write_text(body)
    time.sleep(REQUEST_DELAY)
    return body


def norm(s: str) -> str:
    s = (s or "").replace("’", "'").replace("`", "'")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    s = re.sub(r"(?<=[aeiou])'(?=\s|$)", "", s)  # "MARO'", "LIGABO'": accent typed as apostrophe
    return re.sub(r"[^a-z']+", " ", s).strip()


def tidy(s: str | None) -> str | None:
    if not s:
        return None
    s = html.unescape(s).replace("‐", "-").replace("‑", "-").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip(" |;,*")
    return s or None


def proper(s: str | None) -> str | None:
    """'MISSERONI' -> 'Misseroni', "MARO'" -> "Maro'"; mixed-case input is kept as printed."""
    if not s:
        return s
    return s.title() if (s.isupper() or s.islower()) else s


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def check_media_library() -> None:
    """Fail if GNFM uploaded a project list this script doesn't know about."""
    known = set(YEAR_PDFS.values()) | NOT_LISTS
    page, seen = 1, 0
    while True:
        try:
            items = json.loads(get(MEDIA_API.format(page=page)))
        except RuntimeError:
            break  # past the last page (WP answers 400)
        if not isinstance(items, list) or not items:
            break
        seen += len(items)
        for m in items:
            u = m.get("source_url") or ""
            if re.search(r"progett", u, re.I) and u.lower().endswith(".pdf") and u not in known:
                raise SystemExit(f"new GNFM project PDF in media library, add it to YEAR_PDFS/NOT_LISTS: {u}")
        page += 1
    log(f"Media library: {seen} items checked, no unknown project PDFs")


def members(cache_dir: Path | None, years) -> dict[int, list[tuple[str, str, str | None]]]:
    """GNFM aderenti per year: (FAMILY, Given, affiliation). 2018-2019 pages print
    'FAMILY, Given | Dip. - Ente'; 2025 prints COGNOME | NOME | Università cells."""
    out = {}
    for y in years:
        try:
            page = cached(cache_dir, f"aderenti_{y}.html", ADERENTI_URL.format(year=y))
        except RuntimeError as e:
            log(f"  aderenti {y}: {e}")
            continue
        rows = []
        for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.S):
            cells = [tidy(re.sub(r"<[^>]+>", " ", c)) for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)]
            cells = [c for c in cells if c]
            if cells and re.fullmatch(r"\d+\.?", cells[0]):
                cells = cells[1:]
            if not cells or cells[0].upper() in ("COGNOME", "COGNOME, NOME"):
                continue
            if "," in cells[0] and len(cells) <= 2:
                fam, given = cells[0].split(",", 1)
                rows.append((fam.strip(), given.strip(), cells[1] if len(cells) > 1 else None))
            elif len(cells) >= 2:
                rows.append((cells[0], cells[1], cells[2] if len(cells) > 2 else None))
        out[y] = rows
        log(f"  aderenti {y}: {len(rows)} members")
    return out


def split_coord(name: str, member_keys: set[tuple[str, str]]) -> tuple[str | None, str | None]:
    """Split a one-cell 'Family Given' coordinator name ('De Castro Motta Julia').
    First choice: the split that matches a GNFM member (members lists carry COGNOME
    and NOME separately). Fallback: family first, keeping Italian particles with
    the surname. Degree suffixes are stripped first (runbook §2.4.1)."""
    tokens = [t for t in re.split(r"\s+", (name or "").strip()) if t]
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, proper(tokens[0])
    for k in range(1, len(tokens)):
        for given, family in ((" ".join(tokens[k:]), " ".join(tokens[:k])),
                              (" ".join(tokens[:k]), " ".join(tokens[k:]))):
            if (norm(family), norm(given)) in member_keys:
                return proper(given), proper(family)
    k = 1
    while k < len(tokens) - 1 and tokens[k - 1].lower() in PARTICLES:
        k += 1
    return proper(" ".join(tokens[k:])), proper(" ".join(tokens[:k]))


# Known artefacts of the source PDFs (printed HTML entity split by a space).
TITLE_FIXES = {"Becker and Dö ring model": "Becker and Döring model"}


def tidy_title(s: str | None) -> str | None:
    s = tidy(s)
    for a, b in TITLE_FIXES.items():
        s = s.replace(a, b) if s else s
    return s


FOREIGN_INST = re.compile(r"Universit[äé]t|Universit[eé]\b|University|Universidad", re.I)


def parse_amount(s: str | None) -> float | None:
    # "2000,00" -> 2000.0 (Italian decimal comma, '.' thousands)
    if not s:
        return None
    digits = re.sub(r"[^\d,.]", "", s).strip(",.")
    if not digits:
        return None
    m = re.search(r",(\d{1,2})$", digits)
    whole = re.sub(r"[.,]", "", digits[: m.start()] if m else digits)
    try:
        return float(f"{whole}.{m.group(1)}" if m else whole)
    except ValueError:
        return None


def row_words(pdf_path: Path, cols: list[tuple[float, str]]) -> list[dict[str, list[str]]]:
    """Ruled-table rows; each word goes to the column whose x-start it is past.
    Returns one dict {field: [lines]} per table row (header row skipped by caller)."""
    out = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for pg in pdf.pages:
            words = pg.extract_words()
            for tb in pg.find_tables():
                for r in tb.rows:
                    x0, top, x1, bottom = r.bbox
                    ws = [w for w in words if top - 1 <= (w["top"] + w["bottom"]) / 2 <= bottom + 1]
                    rec: dict[str, list] = {}
                    for w in ws:
                        f = cols[0][1]
                        for cx, fld in cols:
                            if w["x0"] >= cx:
                                f = fld
                        rec.setdefault(f, []).append(w)
                    flds = {}
                    for f, wl in rec.items():
                        wl.sort(key=lambda w: (round(w["top"] / 3), w["x0"]))
                        flds[f] = [w["text"] for w in wl]
                    out.append(flds)
    return out


def parse_2018(path: Path) -> list[dict]:
    # N. | Titolo progetto | RESPONSABILE (Given at x~254, FAMILY at x~352) | PARTECIPANTI (FAMILY Given ...)
    cols = [(0, "num"), (118, "title"), (250, "given"), (348, "family"), (445, "participants")]
    recs = []
    for f in row_words(path, cols):
        num = " ".join(f.get("num", []))
        if not re.fullmatch(r"\d+", num):
            continue
        recs.append({"seq": num, "title": " ".join(f.get("title", [])),
                     "given": " ".join(f.get("given", [])), "family": " ".join(f.get("family", [])),
                     "participants": " ".join(f.get("participants", []))})
    return recs


def parse_2019(path: Path) -> list[dict]:
    # Numero | COGNOME | NOME | UNITA' DI RICERCA INDAM | TITOLO
    cols = [(0, "num"), (180, "family"), (288, "given"), (395, "inst"), (528, "title")]
    recs = []
    for f in row_words(path, cols):
        num = " ".join(f.get("num", []))
        if not re.fullmatch(r"\d+", num):
            continue
        recs.append({"seq": num, "title": " ".join(f.get("title", [])),
                     "family": " ".join(f.get("family", [])), "given": " ".join(f.get("given", [])),
                     "inst": " ".join(f.get("inst", []))})
    return recs


def parse_2025(path: Path) -> list[dict]:
    # COORDINATORE ('Family Given') | TITOLO PROGETTO | FINANZIAMENTO | PARTECIPANTE ; '*' = added after extra funding
    recs = []
    with pdfplumber.open(str(path)) as pdf:
        for pg in pdf.pages:
            for tb in pg.find_tables():
                for row in tb.extract():
                    cells = [(c or "").replace("\n", " ").strip() for c in row]
                    if len(cells) < 4 or not cells[0] or cells[0].upper() == "COORDINATORE" or not cells[1]:
                        continue
                    recs.append({"seq": str(len(recs) + 1), "coord": cells[0], "title": cells[1],
                                 "amount_text": cells[2], "participants": cells[3],
                                 "note": "added after extra funding (*)" if cells[0].startswith("*") else None})
    return recs


PARSERS = {2018: parse_2018, 2019: parse_2019, 2025: parse_2025}


def main() -> None:
    p = argparse.ArgumentParser(description="INdAM-GNFM Progetti Giovani PDFs -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the N most recent lists (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache PDFs/HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--skip-media-check", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    if not args.skip_media_check:
        check_media_library()
    years = sorted(YEAR_PDFS)
    if args.limit:
        years = years[-args.limit:]

    mem = members(args.cache_dir, sorted({y for y in years} | {y - 1 for y in years} | {y + 1 for y in years if y < 2026}))
    member_keys = {(norm(f), norm(g)) for rows in mem.values() for f, g, _ in rows}
    member_spelling = {(norm(f), norm(g)): f for rows in mem.values() for f, g, _ in rows}
    member_aff = {}
    for y, rows in mem.items():
        for f, g, a in rows:
            member_aff[(y, norm(f), norm(g))] = a

    rows = []
    for year in years:
        url = YEAR_PDFS[year]
        pdf_bytes = cached(args.cache_dir, f"progetti_giovani_{year}.pdf", url, binary=True)
        tmp = (args.cache_dir or args.output_dir) / f"_progetti_giovani_{year}.pdf"
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_bytes(pdf_bytes)
        recs = PARSERS[year](tmp)
        if len(recs) != EXPECTED[year]:
            raise SystemExit(f"{year}: parsed {len(recs)} projects, expected {EXPECTED[year]}")
        for r in recs:
            if "coord" in r:
                given, family = split_coord(tidy(r["coord"]), member_keys)
            else:
                given, family = proper(tidy(r["given"])), proper(tidy(r["family"]))
                if given and family and (norm(family), norm(given)) not in member_keys \
                        and (norm(given), norm(family)) in member_keys:
                    given, family = family, given
            # accent typed as apostrophe in the PDF ("MARO'"): use the members-list spelling ("MARÒ")
            if family and re.search(r"[aeiouAEIOU]'$", family):
                fam = member_spelling.get((norm(family), norm(given or "")))
                if fam:
                    family = proper(fam)
            inst, aff_src = tidy(r.get("inst")), None
            if inst:
                aff_src = "pdf"
            elif family:
                for yy in (year, year - 1, year + 1):
                    a = member_aff.get((yy, norm(family), norm(given or "")))
                    if a:
                        inst, aff_src = a, f"aderenti_{yy}"
                        break
            rows.append({
                "project_year": str(year),
                "page_seq": r["seq"],
                "title": tidy_title(r["title"]),
                "coordinator_raw": tidy(r.get("coord") or f"{r.get('given', '')} {r.get('family', '')}"),
                "lead_given_name": given,
                "lead_family_name": family,
                "lead_institution": inst,
                "lead_institution_source": aff_src,
                "lead_country": None if (inst and FOREIGN_INST.search(inst)) else "IT",
                "amount_text": tidy(r.get("amount_text")),
                "amount": parse_amount(r.get("amount_text")),
                "participants_raw": tidy(r.get("participants")),
                "note": r.get("note"),
                "source_pdf": url,
            })
        log(f"  {year}: {len(recs)} projects from {url.rsplit('/', 1)[-1]}")

    df = pd.DataFrame(rows)
    df = df[df["title"].notna() & df["lead_family_name"].notna()].copy()
    base = [f"GNFM-{y}-{slug(f)}" for y, f in zip(df["project_year"], df["lead_family_name"])]
    dup = pd.Series(base).duplicated(keep=False).values
    df["funder_award_id"] = [f"{b}-{slug(g)}" if d else b for b, d, g in zip(base, dup, df["lead_given_name"].fillna(""))]
    if df["funder_award_id"].str.lower().duplicated().any():
        raise SystemExit("duplicate funder_award_id after disambiguation")

    log(f"Parsed {len(df)} projects across {df['project_year'].nunique()} years")
    for c in ["title", "lead_given_name", "lead_family_name", "lead_institution", "amount", "participants_raw"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  total amount EUR {df['amount'].sum():,.0f}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "gnfm_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_gnfm_projects.parquet"
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
