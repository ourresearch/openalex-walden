#!/usr/bin/env python3
"""
Louisiana Board of Regents Support Fund (BoRSF) – Research Competitiveness Subprogram (RCS)
awards to S3
=============================================================================================

The Louisiana Board of Regents (F4320309392) publishes, for every BoRSF competition, the final
panel ("consultant") report: rsi.laregents.edu/wp-content/uploads/... (WordPress media; listed via
/wp-json/wp/v2/media). The RCS report lists every proposal "highly recommended for funding"
(the funded set the Board approves) in Table I / Appendix A with the recommended amount for each
year, followed by one comment block per funded proposal carrying the proposal number, title,
institution and principal investigator. Reports for the FY 2019-20 ... FY 2025-26 competitions
are on the site (older ones are not).

funder_award_id (runbook §2.1.1): papers cite the BoRSF CONTRACT number,
"LEQSF(2024-27)-RD-A-28" = LEQSF(<first FY>-<last FY>)-RD-A-<NN> (RD-A = R&D program,
RCS). The reports print only the proposal number (097A-24), but the contract sequence NN is the
proposal's rank in ascending proposal-number order among that competition's funded proposals,
and the year span is the award's duration (1-year awards -> (2024-25), 3-year -> (2024-27),
one sequence across both). Checked 2026-10-01 against the works citing 2024-27/2024-25 RD-A
contracts: 13 of 14 cited numbers have the predicted PI among the citing works' authors (the
14th, RD-A-3, is cited by RD-A-31's PI - a typo). The notebook ships the derived contract number;
the proposal number is kept as a column. The competition for "FY 2023-24" starts contracts in
2024 (start year = second year of the FY label).

Shipped: contracts starting 2023-2026 (competitions FY 2022-23 .. FY 2025-26), where the
Priority-One list is the contracted set (see VALIDATED_FROM). The FY 2019-20 .. 2021-22 reports are
parsed but not shipped: their recommended lists include proposals that were never contracted, so
neither the award set nor the derived contract numbers can be trusted.

RCS only: the other BoRSF subprograms (ITRS RD-B, PoCP RD-D, ATLAS, Enhancement ENH-*,
graduate fellowships GF, endowed chairs/professorships, SREB) are separate reports in other
layouts and are not parsed here.

Output: s3://openalex-ingest/awards/la_borsf_rcs/la_borsf_rcs_projects.parquet
"""

import argparse
import json
import logging
import re
import time
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

logging.getLogger("pdfminer").setLevel(logging.ERROR)

MEDIA = "https://rsi.laregents.edu/wp-json/wp/v2/media"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/la_borsf_rcs/la_borsf_rcs_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
DASH = "[-‐‑–—]"
# Contract years whose Priority-One list IS the contracted set (derived contract numbers checked
# against citing works 2026-10-01: 2023 11/12, 2024 15/16, 2025 3/3, 2026 1/1 citing-PI matches).
# For the FY 2019-20 .. 2021-22 competitions (contracts 2020-2022) the citations show gaps: e.g.
# 5 of the 36 recommended FY 2019-20 proposals were never contracted (cited RD-A-31 is the 36th
# recommended), so those lists are not the awarded set and are not shipped.
VALIDATED_FROM = 2023


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, **kw) -> requests.Response:
    last = None
    for attempt in range(4):
        try:
            r = requests.get(url, headers=HEADERS, timeout=120, **kw)
            if r.status_code == 200:
                return r
            last = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py) plus a leading honorific strip."""
    if not name:
        return None, None
    name = re.sub(r"^(?:(?:dr|prof|professor|mr|mrs|ms)\.?\s+)+", "", name.strip(), flags=re.I)
    name = re.sub(r",.*$", "", name)                       # "Sangho Yu, Ph.D." -> "Sangho Yu"
    tokens = name.split()
    suffixes = {"phd", "ph.d.", "ph.d", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


# ---------------------------------------------------------------------------
# report discovery + text extraction
# ---------------------------------------------------------------------------
def list_reports() -> list[dict]:
    out, page, pages = [], 1, None
    while pages is None or page <= pages:
        r = get(MEDIA, params={"per_page": 100, "page": page, "mime_type": "application/pdf",
                               "_fields": "id,date,source_url"})
        pages = int(r.headers["X-WP-TotalPages"])
        out += r.json()
        page += 1
        time.sleep(0.5)
    reps = [m for m in out if re.search(r"RCS.*FINAL|FINAL.*RCS", m["source_url"].rsplit("/", 1)[-1], re.I)
            and not re.search(r"RFP", m["source_url"], re.I)]
    log(f"media: {len(out)} PDFs, {len(reps)} RCS final reports")
    return sorted(reps, key=lambda m: m["date"])


def pdf_pages(path: Path) -> list[dict]:
    import pdfplumber
    pages = []
    with pdfplumber.open(str(path)) as pdf:
        for page in pdf.pages:
            rows = []
            for t in page.extract_tables():
                for row in t:
                    cells = [re.sub(r"\s+", " ", c or "").strip() for c in row]
                    if any(re.match(r"^\d{3}A" + DASH + r"?\d{0,2}$", c) for c in cells):
                        rows.append(cells)
            pages.append({"text": page.extract_text() or "", "rows": rows})
    return pages


# ---------------------------------------------------------------------------
# parsing
# ---------------------------------------------------------------------------
PN_RE = re.compile(r"(?<![\d$,])(\d{3})\s?A(?:" + DASH + r"?(\d{2}))?\b")
MONEY = re.compile(r"\$\s*([\d,]+(?:\.\d\d)?)|\$\s*-{2,}|-{3,}")
SEC_START = re.compile(r"^\s*(?:APPENDIX A\b|Table I\b(?!I))", re.I | re.M)
SEC_STOP = re.compile(r"^\s*(?:APPENDIX B\b|Table II\b|.*Review Panelists)", re.I | re.M)
# "A" is optional: the FY 2024-25 report prints one block as "Proposal # 051-25"
BLOCK_RE = re.compile(r"^\s*(?:PROPOSAL:|Proposal\s*#|Proposal No\.|Proposal Number)\s*(\d{3})\s?A?(?:" + DASH
                      + r"?\d{2})?\b", re.M)
LABELS = r"(?:TITLE|Title|INSTITUTION|Institution|PRINCIPAL INVESTIGATOR|PI|COMMENTS|Requested|Recommended|RANK|Rank)\b"


def table_one(pages: list[dict]) -> dict[str, list[float]]:
    """Funded list with per-year recommended amounts: Table I / Appendix A text lines,
    else pdfplumber table rows on the pages before the comment blocks."""
    text, on = [], False
    for p in pages:
        t = p["text"]
        if not on:
            m = SEC_START.search(t)
            if not m:
                continue
            on, t = True, t[m.end():]
        s = SEC_STOP.search(t)
        if s:
            text.append(t[: s.start()])
            break
        text.append(t)
    out = {}
    for line in "\n".join(text).split("\n"):
        m = PN_RE.search(line)
        if not m:
            continue
        amts = [float(x.group(1).replace(",", "")) if x.group(1) else 0.0 for x in MONEY.finditer(line[m.end():])]
        if amts:
            out.setdefault(m.group(1), amts)
            if m.group(2):   # carry-over proposals keep their own year ("015A-23" in the FY 2023-24 list)
                FULL_PN.setdefault(m.group(1), f"{m.group(1)}A-{m.group(2)}")
    if not out:
        for p in pages[:8]:
            for cells in p["rows"]:
                c = [x for x in cells if x]
                for i, x in enumerate(c):
                    m = PN_RE.fullmatch(x.replace(" ", ""))
                    if m:
                        amts = [float(y.group(1).replace(",", "")) if y.group(1) else 0.0
                                for y in MONEY.finditer(" ".join(c[i + 1:]))]
                        if amts:
                            out.setdefault(m.group(1), amts)
                            if m.group(2):
                                FULL_PN.setdefault(m.group(1), f"{m.group(1)}A-{m.group(2)}")
                        break
    return out


FULL_PN: dict[str, str] = {}   # reset per report in parse_report


def norm_institution(s: str | None) -> str | None:
    if not s:
        return None
    s = re.sub(r"\s+[–-]\s+", " ", s).replace("A & M", "A&M")   # "LSU – Agricultural Center"
    s = re.sub(r"Health Sciences Center (New Orleans|Shreveport)", r"Health Sciences Center - \1", s)
    return s.strip()


def field(block: str, label_re: str, multiline: bool) -> str | None:
    if multiline:  # value may wrap: runs to the next label line
        m = re.search(r"^\s*(?:" + label_re + r")\s*:?\s+(.+?)(?=^\s*" + LABELS + r"|\Z)", block, re.M | re.S)
    else:
        m = re.search(r"^\s*(?:" + label_re + r")\s*:?\s+(.+)$", block, re.M)
    if not m:
        return None
    v = re.sub(r"\s+", " ", m.group(1)).strip()
    return v or None


def blocks(pages: list[dict]) -> dict[str, dict]:
    text = "\n".join(p["text"] for p in pages)
    starts = list(BLOCK_RE.finditer(text))
    out = {}
    for k, m in enumerate(starts):
        b = text[m.start(): starts[k + 1].start() if k + 1 < len(starts) else len(text)]
        b = b[:4000]
        title = field(b, r"TITLE|Title", True)
        inst = field(b, r"INSTITUTION|Institution", False)
        pi = field(b, r"PRINCIPAL INVESTIGATOR|PI", False)
        rec = re.search(r"^\s*Recommended\b(.*)$((?:\n\s*\(.*\))?)", b, re.M)
        rec_amts = [float(x.replace(",", "")) for x in re.findall(r"\$\s*([\d,]+)", (rec.group(1) + rec.group(2)) if rec else "")]
        if title and pi and m.group(1) not in out:
            out[m.group(1)] = {"title": title, "institution": inst, "pi": pi, "rec_amounts": rec_amts}
    return out


def competition_fy(pages: list[dict]) -> int:
    """'FY 2023-24' / 'FY 2025–26' on the cover -> contract start year (2024 / 2026)."""
    t = " ".join(p["text"] for p in pages[:3])
    m = re.search(r"FY\s*(20\d\d)\s*" + DASH + r"\s*(\d{2})", t)
    if not m:
        raise ValueError("no FY label on the report cover")
    return int(m.group(1)) + 1


def parse_report(pages: list[dict], url: str) -> list[dict]:
    start = competition_fy(pages)
    yy = str(start)[2:]
    FULL_PN.clear()
    t1 = table_one(pages)
    bl = blocks(pages)
    funded = sorted(set(t1) | (set(bl) if not t1 else set()))
    missing = [p for p in funded if p not in bl]
    if missing:
        log(f"  {url.rsplit('/', 1)[-1]}: no comment block for {missing}")
    rows = []
    for nn, pn in enumerate(funded, 1):
        b = bl.get(pn, {})
        yearly = t1.get(pn) or b.get("rec_amounts") or []
        yearly = [a for a in yearly if a > 0][:3]
        if start >= 2026:            # FY 2025-26 RFP: one-year awards only; report gives the total
            years = 1
        else:
            years = max(1, len(yearly))
        end = start + years
        contract = f"LEQSF({start}-{str(end)[2:]})-RD-A-{nn:02d}"
        given, family = split_name(b.get("pi") or "")
        rows.append({
            "proposal_number": FULL_PN.get(pn) or f"{pn}A-{yy}",
            "contract_number": contract,
            "contract_sequence": str(nn),
            "competition": f"FY {start - 1}-{yy}",
            "title": b.get("title"),
            "institution": norm_institution(b.get("institution")),
            "pi_name": b.get("pi"),
            "lead_given_name": given,
            "lead_family_name": family,
            "recommended_yearly_json": json.dumps(yearly),
            "amount": f"{sum(yearly):.2f}" if yearly else None,
            "currency": "USD" if yearly else None,
            "start_date": f"{start}-06-01",           # BoRSF contracts run June 1 - May 31
            "end_date": f"{end}-05-31",
            "duration_years": str(years),
            "report_url": url,
        })
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="LA BoRSF RCS awards -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None, help="only the N most recent reports (smoke test)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--cache-dir", type=Path, default=None)
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    args = ap.parse_args()

    reps = list_reports()
    by_fy = {}
    cache = args.cache_dir or args.output_dir / "pdf"
    cache.mkdir(parents=True, exist_ok=True)
    for m in reps:                                   # same FY uploaded more than once -> keep the latest
        f = cache / m["source_url"].split("/uploads/", 1)[1].replace("/", "_")
        if not f.exists():
            f.write_bytes(get(m["source_url"]).content)
            time.sleep(0.5)
        pages = pdf_pages(f)
        by_fy[competition_fy(pages)] = (m["source_url"], pages)
    skipped = [fy for fy in by_fy if fy < VALIDATED_FROM]
    log(f"  competitions with contracts starting {skipped}: not shipped (Priority-One list includes "
        f"proposals that were never contracted; see VALIDATED_FROM)")
    fys = sorted(fy for fy in by_fy if fy >= VALIDATED_FROM)
    fys = fys[-args.limit:] if args.limit else fys
    rows = []
    for fy in fys:
        url, pages = by_fy[fy]
        r = parse_report(pages, url)
        stated = re.search(r"recommended\s+(?:[a-z\-]+\s+)?\(?(\d+)\)?\s+proposals",
                           " ".join(p["text"] for p in pages[:6]), re.I)
        log(f"  contracts {fy}: {len(r)} funded (report states {stated.group(1) if stated else '?'}) "
            f"from {url.rsplit('/', 1)[-1]}")
        if stated and int(stated.group(1)) != len(r):
            log(f"  WARNING count mismatch for {fy}")
        rows += r
    df = pd.DataFrame(rows)
    if df["contract_number"].str.lower().duplicated().any() or df["proposal_number"].duplicated().any():
        raise SystemExit("duplicate contract/proposal numbers")
    for c in ["title", "institution", "pi_name", "lead_family_name", "amount"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  total USD {pd.to_numeric(df['amount']).sum():,.0f}")

    df = df.astype("string")  # runbook §1.2 item 5
    args.output_dir.mkdir(parents=True, exist_ok=True)
    path = args.output_dir / "la_borsf_rcs_projects.parquet"
    df.to_parquet(path, index=False)
    log(f"Wrote {len(df)} rows to {path}")
    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    prev = args.output_dir / "_previous_la_borsf_rcs_projects.parquet"
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
