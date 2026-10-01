#!/usr/bin/env python3
"""
Fondazione Compagnia di San Paolo (CSP) research grants to S3
=============================================================

CSP publishes no grants database, API or export. Its only grant-level
publication is the annual "Elenco Beneficiari" PDF on the transparency page
(https://www.compagniadisanpaolo.it/it/documenti-istituzionali/trasparenza):
every grant deliberated that year, grouped by Obiettivo / Missione, with
beneficiary organisation, seat, project title and amount (EUR).

RESEARCH FILTER: only grants listed under the Planet goal mission
"Valorizzare la ricerca" (Harnessing the value of research: research calls,
doctoral/fellowship funding, research equipment and infrastructure, research
institutes). All culture / social / welfare / innovation / environment
missions are excluded, as are the "Progetti operativi e accantonamenti" rows
(CSP internal allocations and provisions, not grants).

YEARS: 2023-2025 only. Earlier lists are not usable with the same filter:
2019-2021 use a different sector taxonomy and layout ("Ricerca e istruzione
superiore"), 2022 another layout, and 2015-2018 publish only per-beneficiary
totals (not grant-level).

No PI names are published (beneficiary organisation only) and no grant number:
funder_award_id = "CSP-{year}-{hash of beneficiary|title|amount}".

Output: s3://openalex-ingest/awards/compagnia_san_paolo/compagnia_san_paolo_projects.parquet
"""

import argparse
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (runbook §1.2 item 7; equivalent to sys.stdout.reconfigure(encoding="utf-8") + open() default utf-8)
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
UPLOADS = "https://www.compagniadisanpaolo.it/wp-content/uploads/"
# Annual "Elenco Beneficiari" PDFs linked from
# https://www.compagniadisanpaolo.it/it/documenti-istituzionali/trasparenza
# Only the 2023-2025 lists are grant-level AND organised by the current mission
# taxonomy (with "Valorizzare la ricerca"); 2019-2022 use other layouts/taxonomies
# and 2015-2018 publish only per-beneficiary totals (see module docstring).
PDFS = {
    2023: ("CSP_Elenco-Beneficiari-2023-per-rapporto_14_07.pdf", "rows"),
    2024: ("CSP_Elenco-Beneficiari_2024_19-05.ok_.pdf", "grid"),
    2025: ("CSP_Elenco-Beneficiari_2025_06-03.pdf", "grid"),
}
MISSION = "Valorizzare la ricerca"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/compagnia_san_paolo/compagnia_san_paolo_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
AMOUNT_RE = re.compile(r"^\d{1,3}(?:\.\d{3})*,\d{2}$")
# internal allocations / provisions, not grants to an external grantee
EXCLUDE_ENTE = re.compile(r"PROGETTI OPERATIVI E ACCANTONAMENTI|^FONDAZIONE COMPAGNIA DI SAN PAOLO$", re.I)
EXCLUDE_PROJECT = re.compile(r"accantonament|^Costi di comunicazione", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_pdf(name: str, cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / name
    if path.exists() and path.stat().st_size > 0:
        return path
    for attempt in range(3):
        try:
            r = requests.get(UPLOADS + name, headers=HEADERS, timeout=120)
            r.raise_for_status()
            path.write_bytes(r.content)
            log(f"  GET {name}: {len(r.content):,} bytes")
            return path
        except Exception as e:  # noqa: BLE001
            err = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {name} failed: {err}")


def parse_amount(s: str) -> float | None:
    s = s.replace("€", "").replace("¤", "").strip()
    if not AMOUNT_RE.match(s):
        return None
    return float(s.replace(".", "").replace(",", "."))


def clean(s: str) -> str | None:
    s = re.sub(r"\s+", " ", s or "").strip()
    return s or None


def mission_pages(pdf) -> list:
    out = []
    for pg in pdf.pages:
        head = (pg.extract_text() or "")[:300]
        if MISSION.lower() in head.lower():
            out.append(pg)
    return out


def grid_research_rows(pdf) -> tuple[int, list]:
    """2024/2025: a mission can start mid-page under a '/ Missione X' heading, so track
    the current mission line by line and keep only rows inside MISSION."""
    current, out, n_pages = None, [], 0
    for pg in pdf.pages:
        marks = []
        for l in pg.extract_text_lines():
            m = re.match(r"^/ Missione (.+)$", l["text"].strip())
            if m:
                marks.append((l["top"], m.group(1).strip()))
        if current is None and not marks:
            continue
        spans = [(-1.0, current)] + marks
        if not any(MISSION.lower() in (name or "").lower() for _, name in spans):
            current = spans[-1][1]
            continue
        n_pages += 1
        for g in parse_grid_page(pg):
            name = [nm for top, nm in spans if top <= g["_y"]][-1]
            if name and MISSION.lower() in name.lower():
                g["pdf_page"] = str(pg.page_number)
                out.append(g)
        current = spans[-1][1]
    return n_pages, out


def column_of(x0: float, edges: list[float]) -> int:
    for i, e in enumerate(edges):
        if x0 < e:
            return i
    return len(edges)


def parse_grid_page(pg) -> list[dict]:
    """2024/2025 layout: ruled table ENTE | SEDE | PROGETTO | DELIBERATO. Each grant is
    one ruled row in the PROGETTO/DELIBERATO columns; a beneficiary with several grants
    is one merged cell in ENTE/SEDE, bounded by rules that span the ENTE column."""
    vs = sorted({round(l["x0"]) for l in pg.lines if abs(l["x0"] - l["x1"]) < 1})
    if len(vs) < 3:
        return []
    ente_x1, sede_x1, proj_x1 = vs[0], vs[1], vs[2]
    hs = [l for l in pg.lines if abs(l["top"] - l["bottom"]) < 1]
    row_ys = sorted({round(l["top"], 1) for l in hs if l["x0"] <= sede_x1 + 2 and l["x1"] >= proj_x1 - 2})
    blk_ys = sorted({round(l["top"], 1) for l in hs if l["x0"] <= 60 and l["x1"] >= ente_x1 - 2})
    words = pg.extract_words()

    def text_in(x_lo, x_hi, y_lo, y_hi):
        ws = [w for w in words if x_lo <= w["x0"] < x_hi and y_lo <= (w["top"] + w["bottom"]) / 2 < y_hi]
        ws.sort(key=lambda w: (round(w["top"]), w["x0"]))
        return clean(" ".join(w["text"] for w in ws))

    grants = []
    for y0, y1 in zip(row_ys, row_ys[1:]):
        amt_txt = text_in(proj_x1, 10_000, y0, y1)
        amount = parse_amount(amt_txt or "")
        if amount is None:
            continue
        b0 = max([b for b in blk_ys if b <= y0 + 0.5], default=y0)
        b1 = min([b for b in blk_ys if b >= y1 - 0.5], default=y1)
        grants.append({
            "_y": y0,
            "beneficiary": text_in(0, ente_x1, b0, b1),
            "seat": text_in(ente_x1, sede_x1, b0, b1),
            "title": text_in(sede_x1, proj_x1, y0, y1),
            "amount": amount,
        })
    return grants


def parse_rows_page(pg) -> list[dict]:
    """2023 layout: ENTE on its own (shaded) line(s); the grant lines follow with
    PROGETTO | DELIBERATO | SEDE. Several amounts under one ENTE = several grants;
    wrapped title lines attach to the nearest amount line."""
    words = pg.extract_words()
    hdr = {}
    for w in sorted(words, key=lambda w: w["top"]):  # header = FIRST occurrence (ENTE names can contain "PROGETTO")
        if w["text"] in ("PROGETTO", "DELIBERATO", "SEDE") and w["text"] not in hdr:
            hdr[w["text"]] = w
    if not {"PROGETTO", "DELIBERATO", "SEDE"} <= set(hdr):
        return []
    edges = [hdr["PROGETTO"]["x0"] - 2, hdr["DELIBERATO"]["x0"] - 40, hdr["SEDE"]["x0"] - 45]
    top_hdr = hdr["PROGETTO"]["bottom"] + 8
    lines: dict[int, list] = {}
    for w in words:
        if w["top"] < top_hdr or w["top"] > pg.height - 40:
            continue
        lines.setdefault(round(w["top"]), []).append(w)
    rows = []
    for t in sorted(lines):
        cells = ["", "", "", ""]
        for w in sorted(lines[t], key=lambda w: w["x0"]):
            cells[column_of(w["x0"], edges)] += w["text"] + " "
        rows.append((t, [c.strip() for c in cells]))
    grants, ente, pending_titles, block = [], [], [], []

    def flush():
        # assign title lines of the finished block to its nearest amount line
        amts = [(t, c) for t, c in block if parse_amount(c[2])]
        for t, c in amts:
            grants.append({"beneficiary": clean(" ".join(ente)), "seat": clean(c[3]),
                           "title_parts": [], "amount": parse_amount(c[2]), "_t": t})
        mine = grants[len(grants) - len(amts):]
        for t, c in block:
            if c[1] and mine:
                g = min(mine, key=lambda g: abs(g["_t"] - t))
                g["title_parts"].append((t, c[1]))

    in_ente = False
    for t, c in rows:
        if c[0] and not c[1] and not c[2]:
            if not in_ente:
                flush()
                ente, block = [], []
                in_ente = True
            ente.append(c[0])
        else:
            in_ente = False
            block.append((t, c))
    flush()
    for g in grants:
        g["title"] = clean(" ".join(x for _, x in sorted(g.pop("title_parts"))))
        g.pop("_t")
    return grants


def main() -> None:
    p = argparse.ArgumentParser(description="Compagnia di San Paolo research grants (annual beneficiary PDFs) -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="parse only the first N research pages per year")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=Path("/tmp/csp_pdf_cache"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()
    import pdfplumber

    rows = []
    for year, (name, layout) in sorted(PDFS.items()):
        path = fetch_pdf(name, args.cache_dir)
        with pdfplumber.open(path) as pdf:
            if layout == "grid":
                n_pages, parsed = grid_research_rows(pdf)
            else:  # 2023: every mission starts on a new page with a running head
                pages = mission_pages(pdf)
                n_pages, parsed = len(pages), []
                for pg in pages:
                    for g in parse_rows_page(pg):
                        g["pdf_page"] = str(pg.page_number)
                        parsed.append(g)
            if args.limit:
                parsed = parsed[: args.limit]
            for g in parsed:
                g.pop("_y", None)
                g.update({"year": str(year), "mission": MISSION, "source_pdf": UPLOADS + name})
                rows.append(g)
            log(f"{year}: {n_pages} pages with '{MISSION}' rows -> {len(parsed)} amount rows")
            if not parsed:
                raise RuntimeError(f"{year}: no '{MISSION}' rows found; layout changed?")

    df = pd.DataFrame(rows)
    excl = df["beneficiary"].fillna("").str.contains(EXCLUDE_ENTE) | df["title"].fillna("").str.contains(EXCLUDE_PROJECT)
    log(f"Excluding {int(excl.sum())} internal allocations/provisions (EUR {df.loc[excl, 'amount'].sum():,.0f})")
    df = df[~excl].copy()
    bad = df["beneficiary"].isna() | df["title"].isna()
    if bad.any():
        log(f"  {int(bad.sum())} rows missing beneficiary or title:")
        for r in df[bad].head(10).to_dict("records"):
            log(f"    {r}")
    # Synthetic key: CSP publishes no grant number in these lists (citing works quote
    # internal ROL / protocol numbers such as 2018.0059 or CSTO164394).
    # Key = year + short hash of (beneficiary, title, amount): stable if the PDF is re-laid out.
    import hashlib
    df["funder_award_id"] = [
        f"CSP-{y}-" + hashlib.sha1(f"{b}|{t}|{a}".encode("utf-8")).hexdigest()[:10].upper()
        for y, b, t, a in zip(df["year"], df["beneficiary"], df["title"], df["amount"])]
    n = df.groupby("funder_award_id").cumcount()
    df.loc[n > 0, "funder_award_id"] = df.loc[n > 0, "funder_award_id"] + "-" + (n[n > 0] + 1).astype(str)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit("duplicate funder_award_id")
    log(f"{len(df)} research grants; by year: {df['year'].value_counts().sort_index().to_dict()}")
    for c in ["beneficiary", "seat", "title", "amount"]:
        log(f"  {c:12s} {df[c].notna().mean():6.1%}")
    log(f"  total EUR {df['amount'].sum():,.0f}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "compagnia_san_paolo_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")
    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_compagnia_san_paolo_projects.parquet"
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
