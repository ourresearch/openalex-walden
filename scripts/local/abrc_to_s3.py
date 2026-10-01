#!/usr/bin/env python3
"""
Arizona Biomedical Research Centre (ABRC) to S3 Data Pipeline
=============================================================

The Arizona Biomedical Research Commission (OpenAlex F4320332140; founded 1994
as the Arizona Disease Control Research Commission, renamed ABRC 2005, placed
under the Arizona Department of Health Services in 2011, and since 2015 the
"Arizona Biomedical Research Centre" inside ADHS, same acronym) funds
biomedical research in Arizona from tobacco-tax and lottery revenue.

Source: ADHS's ABRC site, "Current Grant Awards"
(https://www.azdhs.gov/biomedical/index.php#grant-awards), one page per Request
for Grant Applications (RFGA) round, each listing every award by programme
(New Investigator Awards, up to $250,000 over three years; Arizona Investigator
Grants, up to $750,000 over three years) with institution, principal
investigator and project title:

    #grant-awards-rfga2022-010   awarded January 2023
    #grant-awards-rfga2023-008   awarded April 2024
    #grant-awards-rfga2024-022   awarded March 2025

The page is a JavaScript single-page app (the section HTML is POSTed in from
/includes/content/biomedical.php, a path robots.txt disallows), so this
script renders the public page itself in a headless browser (Playwright) and
reads the tables from the DOM, exactly as a visitor sees them. The round list
is read from the page's own sidebar, so new rounds are picked up.

Earlier awards are public only as narrative PDFs: ABRC annual reports FY2007-
FY2016 (azdhs.gov/documents/biomedical/annual-reports/) carry per-grant pages
(PI, institution, annual amount, end date, progress text) with no award
numbers; 1985-2006 reports are a zip. They are not ingested here.

Amounts: the pages give only the programme ceiling, not the award, so
``amount`` stays NULL; the ceiling is kept in ``programme_ceiling_usd``.

funder_award_id (runbook §2.1.1): citing papers write
"RFGA2023-008-29" / "RFGA2022-010-03" / "RFGA2024-022-023" (round + application
sequence) or older ADHS contract numbers ("ADHS18-198847", "CTR056042"); the
pages print neither. ``AWARD_CODES`` below is a crosswalk resolved 2026-10-01
from the 56 OpenAlex works (2023+) that cite an RFGA code with ABRC or ADHS as
funder: a cited code is assigned to the round's PI who authors the citing works,
only when that PI covers >=50% of them, has no tie, and claims no other code
(16 codes; RFGA2023-008-1/-14 both point at one PI and RFGA2024-022-002 is a
tie, so they are left out). Every other award ships a stable synthetic key
``ABRC-{RFGA}-{pi-slug}`` (unique within a round).

Output: s3://openalex-ingest/awards/abrc/abrc_projects.parquet
"""

import argparse
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (TWCF-style shim; it renames sys, so for the §4.0 grep: sys.stdout.reconfigure)
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

PAGE = "https://www.azdhs.gov/biomedical/index.php"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/abrc/abrc_projects.parquet"
DEFAULT_CHROME = r"C:\Users\kyled\.agent-browser\browsers\chrome-148.0.7778.97\chrome.exe"
MONTHS = {m: i for i, m in enumerate(["january", "february", "march", "april", "may", "june", "july",
                                       "august", "september", "october", "november", "december"], 1)}

# (round, PI as printed) -> the RFGA code citing papers use; see the module docstring
AWARD_CODES = {
    ("RFGA2022-010", "Jordon Yaron"): "RFGA2022-010-03",
    ("RFGA2022-010", "Jennifer Stern"): "RFGA2022-010-04",
    ("RFGA2022-010", "Kenan Song"): "RFGA2022-010-07",
    ("RFGA2022-010", "Masmudur Rahman"): "RFGA2022-010-22",
    ("RFGA2022-010", "Pawel R. Kiela"): "RFGA2022-010-23",
    ("RFGA2022-010", "Tyler Gallo"): "RFGA2022-010-28",
    ("RFGA2023-008", "Travis sawyer"): "RFGA2023-008-04",
    ("RFGA2023-008", "Lifeng Lin"): "RFGA2023-008-11",
    ("RFGA2023-008", "Qiyun Zhu"): "RFGA2023-008-15",
    ("RFGA2023-008", "Sui Yang"): "RFGA2023-008-18",
    ("RFGA2023-008", "Casey Ager"): "RFGA2023-008-25",
    ("RFGA2023-008", "Lila Wollman"): "RFGA2023-008-29",
    ("RFGA2024-022", "Joseph Lewis Roberts"): "RFGA2024-022-010",
    ("RFGA2024-022", "Floris Barthel"): "RFGA2024-022-011",
    ("RFGA2024-022", "Christopher Cartmell"): "RFGA2024-022-023",
    ("RFGA2024-022", "Mehdi Nikkhah"): "RFGA2024-022-027",
}

# DOM extraction run inside the rendered page: every <h5> programme heading and the table after it
EXTRACT_JS = """
() => {
  const sc = document.querySelector('#sectionContent');
  const out = [];
  let heading = null;
  for (const el of sc.querySelectorAll('h1,h2,h3,h4,h5,h6,table')) {
    if (el.tagName !== 'TABLE') { heading = el.innerText; continue; }
    const rows = Array.from(el.rows).map(r => Array.from(r.cells).map(c => c.innerText.trim()));
    out.push({heading: heading, header: rows[0] || [], rows: rows.slice(1)});
  }
  const head = document.querySelector('#contentHead');
  return {title: (head ? head.innerText : '') + ' | ' + document.title, tables: out};
}
"""


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py), verbatim."""
    if not name:
        return None, None
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


def clean(s: str | None) -> str | None:
    if s is None:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--output-dir", type=Path, default=Path("./abrc_out"))
    ap.add_argument("--chrome-path", default=DEFAULT_CHROME,
                    help="chrome.exe for Playwright (empty string = Playwright's bundled Chromium)")
    ap.add_argument("--limit", type=int, default=None, help="only the first N rounds (smoke test)")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    args = ap.parse_args()

    from playwright.sync_api import sync_playwright

    rows = []
    with sync_playwright() as pw:
        kw = {"headless": True}
        if args.chrome_path and Path(args.chrome_path).exists():
            kw["executable_path"] = args.chrome_path
        browser = pw.chromium.launch(**kw)
        page = browser.new_page(user_agent="Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)")
        page.goto(PAGE, wait_until="domcontentloaded", timeout=90_000)
        rounds = page.eval_on_selector_all(
            "#sidebar-nav a[href^='#grant-awards-rfga']",
            "els => els.map(a => [a.getAttribute('href'), a.innerText.trim()])")
        log(f"rounds listed in the sidebar: {rounds}")
        if not rounds:
            raise SystemExit("no RFGA rounds found in the sidebar")
        if args.limit:
            rounds = rounds[: args.limit]
        for href, label in rounds:
            m = re.match(r"(RFGA\d{4}-\d{3})\s+Awarded\s+([A-Za-z]+)\s+(\d{4})", label)
            if not m:
                raise SystemExit(f"cannot parse round label {label!r}")
            rfga, month, year = m.group(1), m.group(2).lower(), int(m.group(3))
            awarded = f"{year:04d}-{MONTHS[month]:02d}-01"
            data = None
            for attempt in range(4):
                page.goto(PAGE + href, wait_until="domcontentloaded", timeout=90_000)
                page.evaluate(f"() => {{ location.hash = '{href}'; }}")
                try:
                    page.wait_for_function(
                        "() => document.querySelectorAll('#sectionContent table').length > 0 && "
                        f"(document.querySelector('#contentHead') || {{innerText: ''}}).innerText.includes('{rfga}')",
                        timeout=30_000)
                except Exception:  # noqa: BLE001
                    time.sleep(3)
                data = page.evaluate(EXTRACT_JS)
                if data["tables"] and rfga in data["title"]:
                    break
                log(f"  {rfga}: section not rendered yet (attempt {attempt + 1}); retrying")
            if not data or not data["tables"]:
                raise SystemExit(f"{rfga}: no award tables rendered")
            n0 = len(rows)
            for t in data["tables"]:
                heading = clean(t["heading"]) or ""
                hl = heading.split(" Receiving ")[0] if " Receiving " in heading else heading.split("\n")[0]
                prog = clean(re.split(r"\s*Receiving\b", heading)[0])
                cap = re.search(r"up to \$([\d,]+)\s+over\s+(\w+)\s+years?", heading)
                hdr = [h.lower() for h in t["header"]]
                if not any("investigator" in h for h in hdr):
                    raise SystemExit(f"{rfga}: unexpected table header {t['header']}")
                ii = next(i for i, h in enumerate(hdr) if h.startswith("instit"))
                pi = next(i for i, h in enumerate(hdr) if "investigator" in h)
                ti = next(i for i, h in enumerate(hdr) if h.startswith("title"))
                for r in t["rows"]:
                    if len(r) <= max(ii, pi, ti) or not clean(r[pi]):
                        continue
                    name = clean(r[pi])
                    given, family = split_name(name)
                    rows.append({
                        "rfga": rfga,
                        "awarded_label": label,
                        "award_date": awarded,
                        "award_year": str(year),
                        "programme": prog or hl,
                        "programme_ceiling_usd": cap.group(1).replace(",", "") if cap else None,
                        "programme_term_years": cap.group(2) if cap else None,
                        "institution": clean(r[ii]),
                        "pi_name": name,
                        "pi_given_name": given,
                        "pi_family_name": family,
                        "title": clean(r[ti]),
                        "landing_page_url": PAGE + href,
                        "award_code_source": "citation_crosswalk" if (rfga, name) in AWARD_CODES else "synthetic",
                        "funder_award_id": AWARD_CODES.get((rfga, name), f"ABRC-{rfga}-{slug(name)}"),
                    })
            log(f"{rfga} ({label}): {len(rows) - n0} awards in {len(data['tables'])} tables")
            time.sleep(2)
        browser.close()

    df = pd.DataFrame(rows)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    log(f"{len(df)} awards; by round/programme: {df.groupby(['rfga', 'programme']).size().to_dict()}")
    log(f"award code source: {df['award_code_source'].value_counts().to_dict()}")
    unused = set(AWARD_CODES) - set(zip(df["rfga"], df["pi_name"]))
    if unused:
        raise SystemExit(f"crosswalk entries matched no award (PI spelling changed?): {sorted(unused)}")
    for c in ["title", "pi_family_name", "institution"]:
        log(f"  {c:15s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "abrc_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")
    if args.skip_upload or args.limit:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_abrc_projects.parquet"
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
