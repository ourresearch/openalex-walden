#!/usr/bin/env python3
"""
Shared helpers for Chinese provincial S&T award-list ingests
============================================================

Common code for the provincial NSF / S&T-department bulk-file scrapers
(``zhejiang_nsf``, ``fujian_nsf``, ``chongqing_nsf``, ``heilongjiang_nsf``,
``hainan_nsf``, plus the two Sichuan programmes). Each provincial portal
publishes annual award-list attachments (立项/结题 announcement xls / xlsx /
doc / docx / pdf). The per-province scripts download those files, parse the
implicit tables, and normalise to the shared awards parquet shape.

Conventions (per the funder-ingest runbook + the S7 W1-B assignment):

* **PI names are Chinese family-first.** Following the NSFC/MOST precedent in
  the repo (``taiwan_most_grb_to_s3.py`` / ``CreateNSFCAwards.ipynb`` handoff),
  the whole Chinese personal name goes in ``family_name`` and ``given_name`` is
  NULL. We do NOT guess the surname split (compound surnames + 2-char given
  names make a naive ``name[:1]`` split wrong often enough to matter). Latin /
  romanised names ARE split on whitespace (last token = family).
* **Amounts are 万元 (×10,000 CNY)** on every provincial list that publishes
  them. Convert to whole CNY. 0 / blank → NULL (§6.7).
* ``funder_award_id`` = the native 立项编号 / 批准号 / 项目编号 grant number where
  the list carries one; else NULL.
* All columns forced to pandas ``string`` dtype before ``to_parquet`` (§1.2).
* Windows UTF-8 shim installed on import.
"""
import sys

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if sys.platform == "win32":
    import builtins as _builtins
    import pathlib as _pathlib

    _orig_wt = _pathlib.Path.write_text
    def _wt(self, data, encoding=None, errors=None, newline=None):
        return _orig_wt(self, data, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib.Path.write_text = _wt

    _orig_rt = _pathlib.Path.read_text
    def _rt(self, encoding=None, errors=None, newline=None):
        return _orig_rt(self, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib.Path.read_text = _rt

    _orig_open = _builtins.open
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins.open = _open_utf8
# --- end shim ---

import re
import subprocess
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

S3_BUCKET = "openalex-ingest"

_LATIN_RE = re.compile(r"[A-Za-z]")
_CN_NUM = "一二三四五六七八九十百"
_SECTION_RE = re.compile(rf"^[（(]?[{_CN_NUM}][）)]?[、\.．]")


def clean(v):
    """Trim, collapse internal whitespace, drop NaN/placeholder-empty."""
    if v is None:
        return None
    s = str(v).replace("　", " ").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    if s == "" or s.lower() == "nan":
        return None
    return s


def split_name(name):
    """Return (given_name, family_name).

    Chinese personal names: whole name in family_name, given_name NULL (do not
    guess the surname split). Latin/romanised names: split on whitespace.
    """
    n = clean(name)
    if not n:
        return (None, None)
    # strip a leading list index / punctuation that sometimes bleeds in
    n = n.strip("·.,，、 ")
    if _LATIN_RE.search(n):
        parts = n.split()
        if len(parts) > 1:
            return (" ".join(parts[:-1]), parts[-1])
        return (None, n)
    # Chinese: keep the whole name as family_name
    return (None, n)


def is_section_header(cell0, rest_empty):
    """A section/scheme heading row: col0 like '一、面上项目...' and the rest of
    the row is empty."""
    c = clean(cell0)
    if not c:
        return False
    return bool(_SECTION_RE.match(c)) and rest_empty


def scheme_from_heading(heading):
    """Extract the programme/scheme name from a section heading like
    '一、面上项目174项，经费1392万元' -> '面上项目'."""
    c = clean(heading)
    if not c:
        return None
    # drop the leading 一、 / （一）
    c = re.sub(rf"^[（(]?[{_CN_NUM}]+[）)]?[、\.．]\s*", "", c)
    # cut at the first count/amount clause
    c = re.split(r"[，,（(]|\d+项|共\d|经费", c)[0].strip()
    return c or None


def parse_amount_wan(v):
    """Parse a 万元 amount -> whole CNY float, or None. 0/neg -> None."""
    s = clean(v)
    if not s:
        return None
    s = s.replace(",", "").replace("，", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        wan = float(m.group(0))
    except ValueError:
        return None
    if wan <= 0:
        return None
    return wan * 10000.0


def parse_date_range(v):
    """Parse '2024-03-01至2027-02-28' or '2024.03-2027.02' -> (start, end) as
    'YYYY-MM-DD' strings (or None). Also handles a bare year."""
    s = clean(v)
    if not s:
        return (None, None)
    s = s.replace("．", ".").replace("。", ".")
    dates = re.findall(r"(\d{4})[-/.年]\s*(\d{1,2})?[-/.月]?\s*(\d{1,2})?", s)
    out = []
    for y, m, d in dates:
        if not y:
            continue
        mm = int(m) if m else 1
        dd = int(d) if d else 1
        try:
            out.append(f"{int(y):04d}-{mm:02d}-{dd:02d}")
        except ValueError:
            pass
    start = out[0] if out else None
    end = out[1] if len(out) > 1 else None
    return (start, end)


def year_from_date(s):
    if not s:
        return None
    m = re.match(r"(\d{4})", str(s))
    return int(m.group(1)) if m else None


def finalize_df(records, provenance, out_dir, filename, extra_cols=None):
    """Build the dataframe, force string dtype, shrink-guard, write parquet.

    ``records`` is a list of dicts with at least: funder_award_id, display_name,
    funder_scheme, institution, given_name, family_name, amount, currency,
    start_date, end_date, start_year, end_year, landing_page_url, source_year.
    """
    df = pd.DataFrame(records)
    yr_now = datetime.now(timezone.utc).year
    if "start_year" in df.columns:
        df.loc[df["start_year"] > yr_now + 1, ["start_year", "start_date"]] = pd.NA
    # Stable per-row key for OpenAlex award-id generation. Prefer the native
    # grant number; else a content hash (title|PI|institution|year|scheme).
    # Sources with no native grant number (e.g. Chongqing 拟立项 lists) rely on
    # this so every row hashes to a distinct award id instead of colliding on a
    # shared NULL funder_award_id.
    import hashlib

    def _rk(r):
        aid = clean(r.get("funder_award_id"))
        if aid:
            return aid.lower()
        basis = "|".join(str(r.get(k) or "") for k in
                         ("display_name", "family_name", "institution",
                          "source_year", "funder_scheme"))
        return f"{provenance}-{hashlib.md5(basis.encode('utf-8')).hexdigest()[:16]}"
    if "row_key" not in df.columns:
        df["row_key"] = [_rk(r) for r in records]
    df["provenance"] = provenance
    df["country_code"] = "CN"
    df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for c in df.columns:
        df[c] = df[c].astype("string")

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / filename
    if out.exists():
        try:
            prev = len(pd.read_parquet(out))
            if len(df) < prev * 0.95:
                print(f"  [ABORT] shrink guard: new {len(df):,} < 95% of prev {prev:,}")
                sys.exit(1)
        except Exception:
            pass
    df.to_parquet(out, index=False)
    print(f"  saved {out.name}: {len(df):,} rows, {out.stat().st_size/1e6:.2f} MB")
    _coverage(df)
    return out


def _coverage(df):
    n = len(df)
    print(f"\n  Coverage ({n:,} rows):")
    for c in ["funder_award_id", "display_name", "funder_scheme", "institution",
              "family_name", "amount", "start_date", "start_year"]:
        if c in df.columns:
            nn = df[c].notna().sum()
            print(f"    {c:18s} {nn:>7,} ({100*nn/n:5.1f}%)")
    if "source_year" in df.columns:
        vc = df["source_year"].value_counts().sort_index()
        print("  by source_year:", {str(k): int(v) for k, v in vc.items()})
    if "amount" in df.columns:
        amt = pd.to_numeric(df["amount"], errors="coerce")
        tot = amt.sum(skipna=True)
        if tot and tot > 0:
            print(f"  amount total: {tot/1e8:.3f} 亿 CNY ({int(amt.notna().sum()):,} rows priced)")


def upload_to_s3(path, s3_key):
    print(f"\n{'='*60}\nUpload to S3\n{'='*60}")
    aws = shutil.which("aws")
    uri = f"s3://{S3_BUCKET}/{s3_key}"
    if not aws:
        print(f"  [ERROR] aws CLI not found. Manual: aws s3 cp {path} {uri}")
        return False
    try:
        subprocess.run([aws, "s3", "cp", str(path), uri], check=True, capture_output=True, text=True)
        print(f"  [OK] {Path(path).name} -> {uri}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] {e.stderr}")
        return False


def http_get(url, dest, referer=None, tries=4, timeout=180):
    """Download a URL to dest with retries, browser UA. Returns True on 2xx with
    non-HTML content (guards against the 403/404 HTML error bodies these gov
    portals return with a 200-ish shell)."""
    import time
    import urllib.request
    import urllib.parse

    # percent-encode non-ASCII path chars (Chinese filenames) but keep the URL
    # structure intact
    def _enc(u):
        parts = urllib.parse.urlsplit(u)
        path = urllib.parse.quote(parts.path, safe="/%")
        return urllib.parse.urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))

    hdrs = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    if referer:
        hdrs["Referer"] = referer
    dest = Path(dest)
    for a in range(tries):
        try:
            req = urllib.request.Request(_enc(url), headers=hdrs)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                data = r.read()
            head = data[:64].lstrip().lower()
            if head.startswith(b"<html") or head.startswith(b"<!doctype html"):
                raise RuntimeError(f"HTML error body ({len(data)} bytes)")
            dest.write_bytes(data)
            return True
        except Exception as e:
            if a == tries - 1:
                print(f"    [download-fail] {url[:90]} :: {e}")
                return False
            time.sleep(2 ** a)
    return False
