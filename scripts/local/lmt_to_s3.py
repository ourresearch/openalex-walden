#!/usr/bin/env python3
"""
Lietuvos mokslo taryba (LMT, Research Council of Lithuania) to S3
=================================================================

LMT's public project register "Spektras" (https://spektras.lmt.lt/, "Lietuvos mokslo tarybos
gautu paraisku, vykdytu ir vykdomu projektu savadas") lists every funded project with contract
number, project leader, title, institution, partner, field, period and funds per year. Each
search result has a built-in Excel export (rodyti.php -> an HTML table served as .xls), which
is ladder item 0: one export per (activity group, status) instead of paging HTML.
(projektai.lmt.lt, the tracker's first URL, is the login-walled application system; its
"Viesos ataskaitos" page is empty.)

The form is a PHP session form: POST index.php with the activity-group checkbox, its option
list, and Bukle (status), then GET rodyti.php in the same session. The host only negotiates
TLS 1.2 with the legacy AES128-SHA cipher, so the session uses an SSL context with
SECLEVEL=0 (certificate verification stays on).

Scope: research-award groups only --
  A  VALSTYBES UZSAKYMAI (national research programmes, commissioned research)
  B  MOKSLININKU INICIJUOTI PROJEKTAI (researcher-initiated: MIP, ERC-level, PRO, ...)
  C  TARPTAUTINES ... PROGRAMOS (bilateral / ERA-NET national shares)
Excluded: D1 researcher training (postdocs, student research, doctoral stipends: each
sub-programme has its own export layout keyed by APPLICATION number, e.g. P-PD-22-116, with
inconsistent name order), D2 travel/mobility, D3 publication/event support, D4 infrastructure.
Any A/B/C export whose header is not the standard contract-number layout is reported as
skipped rather than silently parsed as zero rows.
Statuses: "Vykdomi projektai" (ongoing) and "Baigti projektai" (completed).

funder_award_id (runbook 2.1.1): the contract number ("Sutarties Nr.", e.g. MIP-047/2014,
S-MIP-17-37, S-PD-24-73) -- the form grantees cite (e.g. "S-MIP-24-105", "MIP-099/2012" in
openalex_awards_raw for F4320322689). Application numbers (P-MIP-...) are not published here.

Amount: EUR, the register's per-year funds columns and their total ("IS VISO"). For
completed projects these are funds used; for ongoing projects the funds allocated so far.

Output: s3://openalex-ingest/awards/lmt/lmt_projects.parquet
"""

import argparse
import html
import re
import ssl
import time
from datetime import datetime
from pathlib import Path

import certifi
import pandas as pd
import requests
from requests.adapters import HTTPAdapter

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). This
# crashes scrapers writing laureate names with non-ASCII chars (Polish ł,
# Turkish ğ, Greek μ, combining accents, zero-width spaces). Production
# runs on Linux/Databricks where UTF-8 is the default, but this fixes
# local validation on Windows without requiring contractors to set
# PYTHONUTF8=1 in their environment. See runbook §1.2.
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

BASE = "https://spektras.lmt.lt"
ENC = "cp1257"  # the site is windows-1257 end to end
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/lmt/lmt_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
STATUSES = ["Vykdomi projektai", "Baigti projektai"]
# group code -> (checkbox value, select name, human label)
GROUPS = {
    "A": ("A", "Veikla_A_kryptys[]", "Valstybes uzsakymai"),
    "B": ("B", "Veikla_B_kryptys[]", "Mokslininku inicijuoti projektai"),
    "C": ("C", "Veikla_C_kryptys[]", "Tarptautines programos"),
}
SKIPPED: list[str] = []
RETRIES = 4


class LegacyTLS(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context(cafile=certifi.where())
        ctx.set_ciphers("DEFAULT:@SECLEVEL=0")  # server offers only AES128-SHA
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def session() -> requests.Session:
    s = requests.Session()
    s.mount("https://", LegacyTLS())
    s.headers.update(HEADERS)
    return s


def call(fn, what: str):
    last = None
    for attempt in range(RETRIES):
        try:
            r = fn()
            r.raise_for_status()
            return r
        except Exception as e:  # noqa: BLE001
            last = e
            log(f"  {what}: attempt {attempt + 1} failed ({e}); retrying")
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"{what} failed after {RETRIES} attempts: {last}")


def options(page: str, select_name: str) -> list[str]:
    m = re.search(r"name=" + re.escape(select_name) + r"[^>]*>(.*?)</select>", page, re.S)
    if not m:
        raise SystemExit(f"select {select_name} not found; form layout changed?")
    return [html.unescape(v) for v in re.findall(r"value='([^']*)'", m.group(1))]


def text(fragment: str) -> str | None:
    t = html.unescape(re.sub(r"<[^>]+>", " ", fragment)).replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return None if t in ("", "-") else t


def amount(s: str | None) -> float | None:
    if not s:
        return None
    try:
        return float(s.replace(" ", "").replace(",", "."))
    except ValueError:
        return None


def parse_export(page: str, group: str, status: str) -> list[dict]:
    """Rows are <tr><td>... with 11 fixed cells, then per-year amount cells and the total
    ("IS VISO"), and (completed projects only) a final summary-link cell. The number of year
    columns differs between exports (5 numbered years for completed projects, calendar years
    2022-2029 for ongoing ones) and the header colspan is unreliable, so the money cells are
    read positionally: every numeric cell after the period, last one = total."""
    activity = None
    act = re.search(r"<br><b>[^<]+</b><br>(.*?)<br><b>Projekto b", page, re.S)
    if act:
        activity = text(act.group(1))
    rows = []
    for tr in re.split(r"<tr[^>]*>", page)[1:]:
        cells = re.findall(r"<td[^>]*>(.*?)(?=<td|$)", tr, re.S)
        if len(cells) < 13 or not re.fullmatch(r"\s*\d+\s*", re.sub(r"<[^>]+>", "", cells[0])):
            continue
        link = re.search(r"href=(anotacija\.php\?[^\s>]+)", cells[4])
        tail = [c for c in cells[11:] if "REZ_santrauka" not in c]
        money = [amount(text(c)) for c in tail]
        while money and money[-1] is None:
            money.pop()
        if not money:
            money = [None]
        period = text(cells[10]) or ""
        py = re.findall(r"(\d{4})", period)
        rows.append({
            "row_no": text(cells[0]),
            "call_no": text(cells[1]),
            "contract_no": text(cells[2]),
            "project_leader": text(cells[3]),
            "title": text(cells[4]),
            "institution": text(cells[5]),
            "partner_institution": text(cells[6]),
            "partner_country": text(cells[7]),
            "science_area": text(cells[8]),
            "science_field": text(cells[9]),
            "period": period or None,
            "start_year": py[0] if py else None,
            "end_year": py[-1] if py else None,
            "amounts_by_year": "|".join("" if a is None else f"{a:.2f}" for a in money[:-1]),
            "amount_total": money[-1],
            "annotation_path": link.group(1) if link else None,
            "activity_group": group,
            "activity": activity,
            "status": status,
        })
    return rows


HONORIFIC_RE = re.compile(r"^(?:(?:prof|habil|dr|doc|akad|m\.?\s*dr)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook 2.4.1 helper (wolf_to_s3.py), plus a leading strip of Lithuanian
    academic titles ("habil. dr.", "prof. dr.", "dr.")."""
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


def fetch(group: str, status: str, limit_options: int | None) -> list[dict]:
    s = session()
    home = call(lambda: s.get(BASE + "/", timeout=90), "GET /").content.decode(ENC, "replace")
    cb, select, label = GROUPS[group]
    opts = options(home, select)
    if limit_options:
        opts = opts[:limit_options]
    out = []
    # one activity option per POST: keeps each export small and lets us label the activity
    for o in opts:
        fields = [("SS1", "4"), (f"Veikla_{cb}", cb), (select, o), ("SS4", "4"), ("Bukle", status), ("Submit", "ieškoti")]
        files = [(k, (None, v.encode(ENC))) for k, v in fields]
        res = call(lambda: s.post(BASE + "/index.php", files=files, timeout=180), f"POST {group}/{o[:40]}")
        body = res.content.decode(ENC, "replace")
        if "Būtina pasirinkti" in body:
            raise RuntimeError(f"form rejected ({group}/{o}): required criterion missing")
        exp = call(lambda: s.get(BASE + "/rodyti.php", timeout=300), f"export {group}/{o[:40]}")
        page = exp.content.decode(ENC, "replace")
        has_rows = "anotacija.php?" in page
        if has_rows and "Sutarties Nr." not in page:
            SKIPPED.append(f"{status} | {group} | {o.strip()}")
            log(f"  SKIP non-standard export layout: {status} | {group} | {o.strip()[:70]}")
            continue
        rows = parse_export(page, group, status)
        if has_rows and not rows:
            raise RuntimeError(f"export for {group}/{o} has project rows but none parsed")
        for r in rows:
            r["activity"] = o.strip().lstrip("_").strip()
        log(f"  {status} | {group} | {o.strip()[:70]}: {len(rows)} projects ({len(exp.content)} bytes)")
        out += rows
        time.sleep(0.5)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="LMT Spektras register -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None, help="only the first N activity options per group (smoke test)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true", help="override the 1.4 shrink guard")
    args = ap.parse_args()

    rows = []
    for status in STATUSES:
        for group in GROUPS:
            got = fetch(group, status, args.limit)
            log(f"{status} | {group}: {len(got)} projects")
            rows += got
    for sk in SKIPPED:
        log(f"  skipped (non-standard layout): {sk}")
    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("no projects parsed")
    df["contract_no"] = df["contract_no"].str.strip()
    before = len(df)
    df = df[df["contract_no"].notna()]
    log(f"{before} rows, {before - len(df)} without a contract number dropped")
    # the same contract can be listed under two activities (e.g. MIP parent + sub-programme)
    df = df.sort_values(["contract_no", "status", "activity"]).drop_duplicates(subset=["contract_no"], keep="first")
    log(f"{len(df)} distinct contracts")
    df["funder_award_id"] = df["contract_no"]
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"case-insensitive duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()[:20]}")
    # some leaders are entered in capitals ("VIDMANTAS GULBINAS"): title-case only those
    leader = df["project_leader"].fillna("").map(lambda n: n.title() if n.isupper() else n)
    names = leader.map(split_name)
    df["lead_given_name"] = [g for g, _ in names]
    df["lead_family_name"] = [f for _, f in names]
    df["currency"] = df["amount_total"].map(lambda a: "EUR" if pd.notna(a) and a > 0 else None)
    df["landing_page_url"] = df["annotation_path"].map(lambda p: f"{BASE}/{p}" if isinstance(p, str) else None)

    for c in ["title", "project_leader", "lead_family_name", "institution", "start_year", "amount_total", "annotation_path"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  years {df['start_year'].min()}-{df['start_year'].max()}; total EUR {df['amount_total'].sum():,.0f}")
    log(f"  by group/status: {df.groupby(['activity_group', 'status']).size().to_dict()}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "lmt_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook 1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_lmt_projects.parquet"
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
