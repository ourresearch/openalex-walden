#!/usr/bin/env python3
"""
INdAM (Istituto Nazionale di Alta Matematica "Francesco Severi") fellowships to S3
==================================================================================

INdAM's own fellowship programmes co-funded by the EU Marie Curie COFUND action:
  * INdAM-COFUND 2008 (FP7): incoming / outgoing / reintegration post-doc fellows
  * INdAM-COFUND 2012 (FP7, PCOFUND-GA-2012-600198): three calls, by fellow type
  * INdAM-DP-COFUND 2015 (H2020): doctoral programme fellows, two calls
Each programme publishes its full list of fellows on the INdAM-COFUND site
(https://www.altamatematica.it/indam-cofund/); 2008 fellows' own pages add the
project title, fellowship dates, host organisation and abstract. Method 5
(static HTML), ~25 requests.

Not included (see notebook header): INdAM's student scholarships (enrolment /
master's 'avviamento alla ricerca' / study-abroad), research-assistant 'assegni',
visiting professors and the 2017/2024 INdAM research-project calls - published
only as per-call ranking PDFs whose funded subset is not stated explicitly.
The national groups' projects (GNAMPA, GNCS, ...) are separate funders.

No per-fellow code is published (citing works write the programme name, e.g.
"INdAM-COFUND-2012"), so funder_award_id is synthetic:
{INDAM-COFUND-2008 | INDAM-COFUND-2012 | INDAM-DP-COFUND-2015}-{fellow-slug}.

Output: s3://openalex-ingest/awards/indam/indam_projects.parquet
"""

import argparse
import html
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


# (self-check marker: this block is the runbook sys.stdout.reconfigure shim, with sys renamed)
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


BASE = "https://www.altamatematica.it/indam-cofund"
PROGRAMMES = [
    # (programme, list page, award-id prefix)
    ("INdAM-COFUND 2008", f"{BASE}/cofund2008/fellows-of-indam-cofund-2008/", "INDAM-COFUND-2008"),
    ("INdAM-COFUND 2012", f"{BASE}/cofund2012/indam-cofund-2012-fellows/", "INDAM-COFUND-2012"),
    ("INdAM-DP-COFUND 2015", f"{BASE}/cofund2015/indam-dp-cofund-2015-fellows/", "INDAM-DP-COFUND-2015"),
]
EXPECTED = {"INdAM-COFUND 2008": 27, "INdAM-COFUND 2012": 30, "INdAM-DP-COFUND 2015": 20}
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/indam/indam_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 1.0
RETRIES = 3


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def cached(cache_dir: Path | None, url: str) -> str:
    path = cache_dir / (re.sub(r"[^A-Za-z0-9]+", "_", url.split("//", 1)[-1])[-120:] + ".html") if cache_dir else None
    if path and path.exists():
        return path.read_text()
    body = get(url)
    if path:
        cache_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
    time.sleep(REQUEST_DELAY)
    return body


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = demojibake(html.unescape(t)).replace("\xa0", " ").replace("​", "")
    t = re.sub(r"\s+", " ", t).strip(" ,;")
    return t or None


def article(page: str) -> str:
    m = re.search(r"<article.*?</article>", page, re.S)
    if not m:
        raise RuntimeError("no <article> on page")
    body = m.group(0)
    cut = body.find("Cofunded by")
    return body[:cut] if cut > 0 else body


PARTICLES = {"van", "von", "der", "den", "de", "di", "da", "del", "della", "dalla", "delle", "dei", "degli",
             "dos", "das", "le", "la"}
# Published names whose order/structure the generic split cannot recover (2012 list
# prints these two family-first; two Iberian double surnames on the 2008/2012 lists).
NAME_FIXES = {"Hari Lysianne": ("Lysianne", "Hari"), "Masuti Shreedevi": ("Shreedevi", "Masuti"),
              "Dixan Peña Peña": ("Dixan", "Peña Peña"), "Oscar Fernandez Ramos": ("Oscar", "Fernandez Ramos")}
DP_RANKINGS = [f"{BASE}/cofund-2015/first-call-cofund-2015/", f"{BASE}/cofund2015/second-call-cofund-2015/"]


def fold(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower().strip(".")


def near(a: str, b: str) -> bool:
    """equal, or one edit apart for longer tokens ('Gonzales' ~ 'GONZALEZ')"""
    if a == b:
        return True
    if min(len(a), len(b)) < 5 or abs(len(a) - len(b)) > 1:
        return False
    if len(a) == len(b):
        return sum(x != y for x, y in zip(a, b)) == 1
    short, long_ = sorted((a, b), key=len)
    return any(long_[:i] + long_[i + 1:] == short for i in range(len(long_)))


def ranking_families(cache_dir: Path | None) -> list[set[str]]:
    """DP-COFUND 2015 ranking pages print surnames in capitals ('CRUZ BLÁZQUEZ Sergio');
    return the capitalised-token sets of every ranked applicant."""
    out = []
    for url in DP_RANKINGS:
        t = demojibake(text(article(cached(cache_dir, url)))) or ""
        for entry in re.split(r"\s(?=\d{1,2}\s)", t):
            caps = {fold(w) for w in re.findall(r"[A-ZÀ-Ý][A-ZÀ-Ý'\-]{1,}", entry)}
            if caps:
                out.append(caps)
    return out


def split_with_caps(name: str, families: list[set[str]]) -> tuple[str | None, str | None] | None:
    toks = re.split(r"\s+", name.strip())
    folded = [fold(t) for t in toks]
    best = None
    for caps in families:
        hits = [i for i, f in enumerate(folded) if i > 0 and any(near(f, c) for c in caps)]
        if hits and hits == list(range(hits[0], len(toks))) and (best is None or len(hits) > len(best)):
            best = hits
    if not best:
        return None
    k = best[0]
    while k > 1 and toks[k - 1].lower() in PARTICLES:
        k -= 1
    return " ".join(toks[:k]), " ".join(toks[k:])


def demojibake(s: str | None) -> str | None:
    """Repair UTF-8 that the site stored as Latin-1 ('UniversitÃ<nbsp>' -> 'Università',
    'BLÃ<0x81>ZQUEZ' -> 'BLÁZQUEZ'), one byte sequence at a time."""
    def fix(m):
        try:
            return m.group(0).encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return m.group(0)
    return re.sub(r"[\xc2-\xf4][\x80-\xbf]{1,3}", fix, s) if s else s


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py): strip degree suffixes, last
    token = family. Surname particles (van, von, de, di, da, del, dos) stay with
    the family name ('Ulrich von der Ohe', 'Chris Van de Ven')."""
    if not name:
        return None, None
    tokens = name.strip().split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    k = len(tokens) - 1
    while k > 1 and tokens[k - 1].lower() in PARTICLES:
        k -= 1
    return " ".join(tokens[:k]), " ".join(tokens[k:])


def parse_2008_fellow(page: str) -> dict:
    """Linked 2008 fellow pages: '<type> fellow between YYYY-MM-DD and YYYY-MM-DD',
    project title, host organisations, abstract."""
    body = article(page)
    out = {}
    m = re.search(r"INdAM-COFUND\s+(\w+)\s+fellow\s+between\s+(\d{4}-\d{2}-\d{2})\s+and\s+(\d{4}-\d{2}-\d{2})", text(body) or "")
    if m:
        out.update(fellow_type=m.group(1).title(), start_date=m.group(2), end_date=m.group(3))
    t = re.search(r"<h4>\s*Title\s*</h4>\s*<p>(.*?)</p>", body, re.S)
    if t:
        out["title"] = text(t.group(1))
    host = re.search(r'<p class="name">(.*?)</p>', body, re.S)
    if host:
        # two side-by-side columns padded with non-breaking spaces: keep the first
        # (the host during the fellowship; the second is the return host)
        lines = [re.split(r"(?:\xa0|&nbsp;|\s){3,}", html.unescape(l).strip())[0]
                 for l in re.split(r"<br\s*/?>", host.group(1))]
        out["host_institution"] = text(", ".join(l for l in lines if text(l)))
    a = re.search(r"<h4>\s*Abstract\s*</h4>(.*?)(?:<h3|<h4)", body, re.S)
    if a:
        out["abstract"] = text(a.group(1))
    return out


def parse_programme(name: str, url: str, cache_dir: Path | None, limit: int | None) -> list[dict]:
    body = article(cached(cache_dir, url))
    rows = []
    if name.endswith("2008"):
        for li in re.findall(r"<li>(.*?)</li>", body, re.S):
            link = re.search(r'href="([^"]+)"', li)
            rows.append({"name": text(li), "call": None, "fellow_type": None,
                         "landing_page_url": link.group(1) if link else url})
    elif name.endswith("2012"):
        # <h2>First Call</h2> ... 'Incoming Fellows' / 'Outgoing Fellows' / 'Reintegration Fellow' lists
        for call, section in re.findall(r"<h2[^>]*>(.*?)</h2>(.*?)(?=<h2|$)", body, re.S):
            for kind, items in re.findall(r"((?:Incoming|Outgoing|Reintegration) Fellows?)(.*?)</ul>", section, re.S):
                for li in re.findall(r"<li>(.*?)</li>", items, re.S):
                    nm = text(re.sub(r"\(?\s*<a .*?</a>\s*\)?", "", li, flags=re.S))
                    rep = re.search(r'href="([^"]+\.pdf)"', li)
                    rows.append({"name": nm, "call": text(call), "fellow_type": kind.replace(" Fellows", "").replace(" Fellow", ""),
                                 "landing_page_url": url, "final_report_url": rep.group(1) if rep else None})
    else:
        for call, section in re.findall(r"<h2[^>]*>(.*?)</h2>(.*?)(?=<h2|$)", body, re.S):
            for strong, after in re.findall(r"<strong>(.*?)</strong>(.*?)(?=<strong>|$)", section, re.S):
                home = re.search(r'href="([^"]+/indam-dp-cofund-2015-fellows/[^"]+)"', after)
                rows.append({"name": text(strong), "call": text(call), "fellow_type": "Doctoral",
                             "landing_page_url": home.group(1) if home else url})
    rows = [r for r in rows if r["name"]]
    if limit:
        rows = rows[:limit]
    for r in rows:
        r["programme"] = name
        if name.endswith("2008") and r["landing_page_url"] != url:
            r.update(parse_2008_fellow(cached(cache_dir, r["landing_page_url"])))
    return rows


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def main() -> None:
    p = argparse.ArgumentParser(description="INdAM-COFUND fellowship programmes -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the first N fellows per programme (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    rows = []
    for name, url, prefix in PROGRAMMES:
        fellows = parse_programme(name, url, args.cache_dir, args.limit)
        log(f"  {name}: {len(fellows)} fellows")
        if not args.limit and abs(len(fellows) - EXPECTED[name]) > 0:
            raise SystemExit(f"{name}: parsed {len(fellows)} fellows, expected {EXPECTED[name]} (page changed?)")
        families = ranking_families(args.cache_dir) if "DP-COFUND" in name else []
        for f in fellows:
            f["host_institution"] = demojibake(f.get("host_institution"))
            given_family = NAME_FIXES.get(f["name"]) or (split_with_caps(f["name"], families) if families else None)
            given, family = given_family or split_name(f["name"])
            f.update(lead_given_name=given, lead_family_name=family,
                     funder_award_id=f"{prefix}-{slug(f['name'])}")
            rows.append(f)

    df = pd.DataFrame(rows)
    for c in ["title", "abstract", "start_date", "end_date", "host_institution", "final_report_url"]:
        if c not in df:
            df[c] = None
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    log(f"Parsed {len(df)} fellowships")
    for c in ["title", "start_date", "host_institution", "lead_family_name"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "indam_projects.parquet"
    df = df.astype("string")
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_indam_projects.parquet"
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
