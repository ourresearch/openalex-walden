#!/usr/bin/env python3
"""
Gobierno de Aragón: research group subsidies (grupos de investigación reconocidos) to S3
=========================================================================================

The Government of Aragón funds every research group it recognises for a
three-year period ("grupos de investigación reconocidos por el Gobierno de
Aragón"). The concession resolutions are published in the Boletín Oficial de
Aragón (BOA) with an Anexo I table listing, per beneficiary centre, each
funded group: group reference code, group name, category (Referencia /
Desarrollo / Emergente), area, principal investigator (and co-PI from 2023),
score or number of PhD researchers, and the amount granted (EUR).

Why this subset of the Aragón subsidies data: the coordinator pointed at the
opendata.aragon.es "convocatorias-ayudas-subvenciones-gobierno-aragon"
dataset, but that dataset is the list of BOA *call* announcements (one row per
call, full legal text, no beneficiaries), so it has no grant-level rows. The
grant-level lists for research are these group-funding resolutions. They are
also the Aragón award Aragon grantees actually cite: >400 of the ~1,450
acknowledgement stubs for F4320326208 are group codes like "E46_20R".

Research filter: only the research-group funding programme (I+D+i research
groups, Dirección General de Investigación e Innovación). No non-research
subsidies are included. Predoctoral contracts and other I+D calls are NOT
covered (their resolutions list names only, or are not in the BOA).

Periods covered (one award = one group x one funding period):
  2017-2019  Resolución de 19 de junio de 2018  (BOA 27/06/2018; Anexo I only --
             the correction of 03/07/2018 deletes a stray Anexo I/II on pp.
             21075-21077, so parsing stops at the first "ANEXO II")
  2020-2022  Resolución de 17 de agosto de 2021 (BOA 27/08/2021)
  2023-2025  Resolución de 20 de abril de 2023  (BOA 28/04/2023; table is
             printed rotated/transposed)
Earlier annual resolutions (2013-2016) use bare group codes (e.g. "E96")
with no period suffix, which are not a citable award id, so they are skipped.

funder_award_id = the group reference code with its period suffix, exactly as
printed and as cited ("T08_20R", "B05_17D").

Output: s3://openalex-ingest/awards/aragon_research_groups/aragon_research_groups_projects.parquet
"""

import argparse
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (TWCF-style shim; the canonical call is sys.stdout.reconfigure(encoding="utf-8"),
#  done below through the renamed module alias.) See runbook §1.2.
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

import fitz  # PyMuPDF

BOA_OBJ = "https://www.boa.aragon.es/cgi-bin/EBOA/BRSCGI?CMD=VEROBJ&MLKOB={}"
RESOLUTIONS = [
    # (period, BOA object id, resolution title, BOA publication date, expected total EUR or None, layout)
    ("2017-2019", "1027772164747", "Resolución de 19 de junio de 2018 (proyectos estratégicos de los grupos de investigación 2017-2019)", "2018-06-27", None, "rows"),
    ("2020-2022", "1181523502929", "Resolución de 17 de agosto de 2021 (actividad investigadora de los grupos de investigación 2020-2022)", "2021-08-27", None, "rows"),
    ("2023-2025", "1273885200606", "Resolución de 20 de abril de 2023 (actividad investigadora de los grupos de investigación 2023-2025)", "2023-04-28", 9999999.91, "columns"),
]
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/aragon_research_groups/aragon_research_groups_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
CODE_RE = re.compile(r"^([A-Z]{1,3}\d{2,3})\s*_?\s*(\d{2}[A-Z])$")


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def norm(s):
    if s is None:
        return None
    s = re.sub(r"\s+", " ", str(s).replace("\xad", "").replace("-\n", "-")).strip()
    return s or None


def norm_code(s):
    if not s:
        return None
    s = re.sub(r"\s+", " ", s.replace("\n", " ")).strip()
    s = re.sub(r"\s*_\s*", "_", s)          # "A21 20R _" -> "A21 20R_"
    s = s.rstrip("_").strip()
    m = CODE_RE.match(s.replace("_", " ").strip()) or CODE_RE.match(s)
    return f"{m.group(1)}_{m.group(2)}" if m else None


def eur(s):
    if not s:
        return None
    s = re.sub(r"[€\s]", "", s)
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


PARTICLES = {"de", "del", "la", "las", "los", "y", "i", "san", "santa", "da", "do", "dos", "van", "von", "le"}
SUFFIXES = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}


def split_name(name):
    """Spanish-name splitter (runbook §2.4.1 suffix set kept). Spanish names carry
    two surnames, so the canonical 'last token = family' rule is wrong here:
    'Pilar María Muñoz Álvaro' -> ('Pilar María', 'Muñoz Álvaro'). Particles
    glue to the following token ('Jesús Martínez De La Fuente' ->
    ('Jesús', 'Martínez De La Fuente'); 'Carlos López San Juan' -> ('Carlos',
    'López San Juan')). 2 tokens -> given + family; 3 tokens -> given + 2
    surnames. Compound given names with only one surname remain ambiguous."""
    if not name:
        return None, None
    toks = name.replace(",", " ").split()
    while toks and toks[-1].lower().strip(".") in SUFFIXES:
        toks.pop()
    if not toks:
        return None, None
    # glue particles to the next token
    units, buf = [], []
    for t in toks:
        buf.append(t)
        if t.lower() not in PARTICLES:
            units.append(" ".join(buf))
            buf = []
    if buf:
        units[-1:] = [" ".join(units[-1:] + buf)] if units else [" ".join(buf)]
    if len(units) == 1:
        return None, units[0]
    if len(units) == 2:
        return units[0], units[1]
    return " ".join(units[:-2]), " ".join(units[-2:])


def fetch(obj_id: str, cache_dir: Path) -> Path:
    path = cache_dir / f"boa_{obj_id}.pdf"
    if not path.exists():
        url = BOA_OBJ.format(obj_id)
        log(f"GET {url}")
        r = requests.get(url, headers=HEADERS, timeout=300)
        r.raise_for_status()
        if not r.content.startswith(b"%PDF"):
            raise SystemExit(f"{url}: not a PDF ({r.headers.get('content-type')})")
        path.write_bytes(r.content)
        log(f"  {len(r.content):,} bytes")
    return path


def annex_pages(doc):
    """Pages from the 'ANEXO I' heading (concedidos) up to, not including, the page
    carrying the 'ANEXO II' heading. Headings are matched as whole text lines, so
    in-text mentions ("se detalla en el anexo II") don't count. In all three
    resolutions each annex starts on a new page."""
    pages, on = [], False
    for i, p in enumerate(doc):
        heads = [re.sub(r"\s+", " ", ln).strip().upper() for ln in p.get_text().splitlines()]
        heads = [h for h in heads if re.match(r"^ANEXO\s+[IVX]+\b", h)]
        if on and any(re.match(r"^ANEXO\s+(II|III|IV|V|VI)\b", h) for h in heads):
            break
        if not on and any(re.match(r"^ANEXO\s+I\b", h) for h in heads):
            on = True
        if on:
            pages.append((i, "all"))
    return pages


def parse_rows(doc, period):
    """2017/2020 layout: one row per group; institution in a single-cell row above the header."""
    out, inst = [], None
    for i, _ in annex_pages(doc):
        for t in doc[i].find_tables().tables:
            for r in t.extract():
                cells = [norm(c) if c else None for c in r]
                filled = [c for c in cells if c]
                if len(filled) == 1 and not filled[0].upper().startswith("TOTAL") and not CODE_RE.search(filled[0]):
                    inst = filled[0]
                    continue
                code = norm_code(r[1]) if len(r) > 1 else None
                if not code:
                    continue
                out.append({
                    "period": period, "funder_award_id": code, "institution": inst,
                    "area": cells[0], "group_name": cells[2], "pi_name": cells[3],
                    "co_pi_name": None, "category": None,
                    "score": cells[4] if len(cells) > 4 else None, "n_phd_researchers": None,
                    "amount": eur(cells[5]) if len(cells) > 5 else None, "page": i + 1,
                })
    return out


def parse_columns(doc, period):
    """2023 layout: transposed tables (one column per group), institution in cell [0][0]."""
    # Row order (bottom-up as extracted), identical in labelled tables (first table of an
    # institution: col 0 = institution, col 1 = labels) and unlabelled continuation tables.
    order = ["Importe subvención", "Nº investigadores/as con doctorado", "Nombre CO IP", "Nombre IP",
             "Área", "Categoría", "Denominación", "Nº_Código del Grupo"]
    out, inst = [], None
    for i, _ in annex_pages(doc):
        for t in doc[i].find_tables().tables:
            rows = t.extract()
            if not rows:
                continue
            k = max(range(len(rows)), key=lambda r: sum(1 for c in rows[r] if norm_code(c)))
            if k < 7 or not any(norm_code(c) for c in rows[k]):
                continue
            labelled = norm(rows[k][1]) == "Nº_Código del Grupo" if len(rows[k]) > 1 else False
            if labelled:
                labels = [norm(rows[k - 7 + n][1]) for n in range(8)]
                if labels != order:
                    raise SystemExit(f"page {i + 1}: unexpected row labels {labels}")
                if rows[0][0]:
                    inst = norm(rows[0][0])
            lab = {name: rows[k - 7 + n] for n, name in enumerate(order)}
            for j in range(len(rows[k])):
                code = norm_code(rows[k][j])
                if not code:
                    continue
                g = lambda key: norm(lab[key][j]) if j < len(lab[key]) else None
                out.append({
                    "period": period, "funder_award_id": code, "institution": inst,
                    "area": g("Área"), "group_name": g("Denominación"), "pi_name": g("Nombre IP"),
                    "co_pi_name": g("Nombre CO IP"), "category": g("Categoría"), "score": None,
                    "n_phd_researchers": g("Nº investigadores/as con doctorado"),
                    "amount": eur(g("Importe subvención")), "page": i + 1,
                })
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Aragón research group subsidies (BOA) -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="keep only the first N rows (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for period, obj, title, pub, expected_total, layout in RESOLUTIONS:
        doc = fitz.open(fetch(obj, args.output_dir))
        got = parse_rows(doc, period) if layout == "rows" else parse_columns(doc, period)
        total = sum(r["amount"] or 0 for r in got)
        log(f"{period}: {len(got)} groups, EUR {total:,.2f} (pages {sorted({r['page'] for r in got})[:1]}..)")
        if not got:
            raise SystemExit(f"{period}: parsed 0 groups")
        if expected_total and abs(total - expected_total) > 1:
            raise SystemExit(f"{period}: parsed total {total:,.2f} != published total {expected_total:,.2f}")
        for r in got:
            r.update({"resolution": title, "boa_publication_date": pub,
                      "source_url": BOA_OBJ.format(obj),
                      "start_date": f"{period[:4]}-01-01", "end_date": f"{period[-4:]}-12-31"})
        rows += got

    df = pd.DataFrame(rows)
    df["period_suffix_ok"] = df.apply(lambda r: r["funder_award_id"][-3:-1] == r["period"][2:4], axis=1)
    bad = df[~df["period_suffix_ok"]]
    if len(bad):
        raise SystemExit(f"code/period mismatch: {bad[['period', 'funder_award_id']].values.tolist()[:10]}")
    df = df.drop(columns=["period_suffix_ok"])
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()[:20]}")
    df[["pi_given_name", "pi_family_name"]] = df["pi_name"].apply(lambda n: pd.Series(split_name(n)))
    df[["co_pi_given_name", "co_pi_family_name"]] = df["co_pi_name"].apply(lambda n: pd.Series(split_name(n)))
    df["currency"] = "EUR"

    if args.limit:
        df = df.head(args.limit)

    for c in ["group_name", "pi_name", "institution", "amount", "area"]:
        log(f"  {c:14s} {df[c].notna().mean():6.1%}")
    log(f"  total EUR {df['amount'].sum():,.0f}")

    df = df.astype("string")
    parquet_path = args.output_dir / "aragon_research_groups_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_aragon_research_groups_projects.parquet"
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
