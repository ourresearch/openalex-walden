#!/usr/bin/env python3
"""
Gauss Centre for Supercomputing (GCS) Large-Scale Project allocations to S3
==========================================================================

GCS (F4320331625) awards IN-KIND computing time on the national supercomputers of its three
centres (HLRS Stuttgart, JSC Juelich, LRZ Garching) through its twice-yearly Large-Scale Project
calls, allocated by GCS's own peer review. Every call's approved projects are listed at
https://www.gauss-centre.eu/results/large-scale-projects (one static page per call, 33 calls
linked: 1-3 and 6-35; calls 4 and 5 are not linked). Method 5 (static HTML), robots.txt allows.

Each entry: "Project title" [(project id[s])] / PI, institution / [HPC platform: ...], grouped
under "at HLRS:" / "at JSC:" / "at LRZ:" headings (calls >= 13; earlier pages are ungrouped).
Project ids are printed for calls 19-24 (and some later), e.g. (pr53ju), (chwu08, pn34mu),
(GCS-HQCD). No core-hours, no money: amount NULL, allocation described in `description`.

funder: every Large-Scale allocation is made by GCS (the GCS steering committee / peer review),
so all rows go to GCS F4320331625, not to the hosting centre (JSC F3307039080 and LRZ F4320336863
have their own funder rows for their own, non-GCS allocations; HLRS has none).

funder_award_id (runbook 2.1.1): papers cite the centre project id ("pn49yi", "pr74su",
"Project: pn49ha", "hbn26", "VSK33"), so when GCS prints the id we ship it (lower-cased; the
first id when a project has accounts at two centres, the others go in the description). A project
continued in a later call under the same id stays ONE award listing all its calls. Entries without
a printed id get a stable synthetic key "GCS-LSP-C<call>-<title-slug>". Collisions RAISE.

Output: s3://openalex-ingest/awards/gauss_centre/gauss_centre_projects.parquet
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

BASE = "https://www.gauss-centre.eu"
INDEX = BASE + "/results/large-scale-projects"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/gauss_centre/gauss_centre_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
CENTRES = ["HLRS", "JSC", "LRZ"]
MONTHS = {m: i for i, m in enumerate(["january", "february", "march", "april", "may", "june", "july", "august",
                                      "september", "october", "november", "december"], 1)}
QUOTES = "\"“”„‟«»"
TITLE_RE = re.compile(r"^[\s" + QUOTES + r"]*[" + QUOTES + r"](.+?)[" + QUOTES + r"]\s*(?:\(([^()]*)\))?\s*(.*)$", re.S)
HONORIFIC = re.compile(r"^(?:(?:jun\.?-?\s?prof\.?|prof\.?|professor|priv\.-doz\.|pd|apl\.|dr\.?-ing\.?|dr\.?\s?rer\.?\s?nat\.?|dr\.?|ph\.?d\.?)\s*)+", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last = None
    for attempt in range(4):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code == 200:
                r.encoding = "utf-8"
                return r.text
            last = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last = e
        log(f"  retry {attempt + 1} for {url}: {last}")
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py) plus a leading honorific strip."""
    if not name:
        return None, None
    name = HONORIFIC.sub("", name.strip())
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def text(fragment: str) -> str:
    t = re.sub(r"<br\s*/?>", "\n", fragment)
    t = html.unescape(re.sub(r"<[^>]+>", "", t)).replace("\xa0", " ").replace("​", "")
    return "\n".join(re.sub(r"[ \t]+", " ", ln).strip() for ln in t.split("\n"))


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def parse_date(s: str) -> str | None:
    m = re.match(r"([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})", s.strip())
    if not m or m.group(1).lower() not in MONTHS:
        return None
    return f"{int(m.group(3)):04d}-{MONTHS[m.group(1).lower()]:02d}-{int(m.group(2)):02d}"


def call_pages() -> list[str]:
    idx = get(INDEX)
    urls = sorted(set(re.findall(r'href="(/results/large-scale-projects/[^"#?]+)"', idx)))
    log(f"index: {len(urls)} call pages")
    if len(urls) < 30:
        raise SystemExit("fewer call pages than expected; index changed?")
    return [BASE + u for u in urls]


def parse_pi_platform(rest: list[str], plat_re: str) -> dict:
    platform = next((re.sub(plat_re + r"\s*", "", ln, flags=re.I) for ln in rest if re.match(plat_re, ln, re.I)), None)
    # skip platform lines, "Project report" links and title-line notes such as "/ project extension"
    pi_line = next((ln for ln in rest if not re.match(plat_re + r"|^project report|^\s*[/–-]", ln, re.I)), None)
    pi_line = re.sub(r"^Principal Investi\w*:\s*", "", (pi_line or "").strip(" ,;"), flags=re.I).strip() or None
    pi_name = institution = None
    if pi_line:
        parts = [p.strip() for p in pi_line.split(",")]
        # "Prof. Dr. A and Prof. Dr. B, Inst": the first named PI leads
        pi_name = re.split(r"\s+(?:and|und|&)\s+", parts[0])[0] or None
        institution = ", ".join(p for p in parts[1:] if p) or None
    return {"pi_line": pi_line, "pi_name": pi_name, "institution": institution, "platform": platform}


def parse_call(url: str) -> list[dict]:
    page = get(url)
    head = re.search(r'<h3 itemprop="headline">(.*?)</h3>', page, re.S)
    heading = text(head.group(1)).strip() if head else ""
    m = re.search(r"Call\s+(\d+)", heading)
    if not m:
        raise SystemExit(f"{url}: no call number in heading {heading!r}")
    call = int(m.group(1))
    ym = re.search(r"(20\d\d)", heading)
    call_year = int(ym.group(1)) if ym else None
    i = page.find('<div class="project-detail">')
    j = page.find("<!-- Related news records -->", i)
    body = page[i:j if j > 0 else None]
    period_start = period_end = None
    pm = re.search(r"Computing time period for all projects of this call:\s*([A-Za-z]+ \d{1,2},? \d{4})\s*(?:-|–|to)\s*([A-Za-z]+ \d{1,2},?\s*\d{4})",
                   text(body).replace("\n", " "))
    if pm:
        period_start, period_end = parse_date(pm.group(1)), parse_date(pm.group(2))
    blocks = re.findall(r"<(h[1-6]|p|li)\b[^>]*>(.*?)</\1>", body, re.S)
    centres: list[str] = []
    out = []
    for tag, inner in blocks:
        t = text(inner).strip()
        flat = re.sub(r"\s+", " ", t)
        if re.match(r"^(?:at|both at|at both)\b[^.]{0,40}:$", flat, re.I):
            centres = [c for c in CENTRES if re.search(rf"\b{c}\b", flat)]
            continue
        if tag.startswith("h"):
            continue
        quoted = re.match(r"^[\s" + QUOTES + r"]*[" + QUOTES + r"]", t)
        lines = [ln.strip() for ln in t.split("\n") if ln.strip(" ,;")]
        plat_re = r"^(?:HPC\s+)?platforms?\s*[:.]"
        # call 28 splits one entry over two <li>: the title, then "Principal Investigator: ...": attach
        if not quoted and lines and re.match(r"^Principal Investi", lines[0], re.I):
            if out and out[-1]["pi_line"] is None:
                out[-1].update(parse_pi_platform(lines, plat_re))
            continue
        # an entry is a quoted title, or (a few unquoted ones, e.g. "Nuclear Lattice Simulations (chfz02)")
        # a list item / paragraph whose next line names the PI
        if not quoted and not (len(lines) >= 2 and (tag == "li" or re.match(r"^Principal Investi", lines[1], re.I))):
            continue
        if quoted:
            tm = TITLE_RE.match(t)
            if not tm:
                raise SystemExit(f"call {call}: cannot parse entry {flat[:120]!r}")
            title, id_str, rest_txt = tm.group(1), tm.group(2), tm.group(3)
        else:
            um = re.match(r"^(.*?)\s*(?:\(([^()]*)\))?\s*$", lines[0])
            title, id_str, rest_txt = um.group(1), um.group(2), "\n".join(lines[1:])
        title = re.sub(r"\s+", " ", title).strip().rstrip(",")
        ids = [x.strip() for x in re.split(r"[,;/]|\band\b", id_str or "") if x.strip()]
        rest = [ln.strip() for ln in rest_txt.split("\n") if ln.strip(" ,;")]   # call 1/2: '"Title",<br> PI'
        out.append({
            "call": call, "call_heading": heading, "call_year": call_year, "landing_page_url": url,
            "period_start": period_start, "period_end": period_end, "centres": centres[:],
            "title": title, "project_ids": ids, **parse_pi_platform(rest, plat_re),
        })
    log(f"call {call:2d} ({heading}): {len(out)} projects; ids on {sum(1 for o in out if o['project_ids'])}; centres {sorted({c for o in out for c in o['centres']})}")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="GCS Large-Scale Projects -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None, help="parse only the first N call pages (smoke test)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    args = ap.parse_args()

    urls = call_pages()
    if args.limit:
        urls = urls[: args.limit]
    rows = []
    for u in urls:
        rows += parse_call(u)
        time.sleep(1)
    if not args.limit and len(rows) < 400:
        raise SystemExit(f"only {len(rows)} projects parsed; page format changed?")

    groups: dict[str, list[dict]] = {}
    for r in rows:
        if r["project_ids"] and re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{2,30}", r["project_ids"][0]):
            key = r["project_ids"][0].lower()
        else:
            key = f"GCS-LSP-C{r['call']:02d}-{slug(r['title'])[:60].rstrip('-')}"
        groups.setdefault(key, []).append(r)
    awards = []
    for key, g0 in groups.items():
        # a multi-centre project can be listed once per centre heading in the same call (call 27:
        # pr74yo under JSC and under LRZ): fold those into one entry; different titles = real clash
        by_call: dict[int, dict] = {}
        for x in sorted(g0, key=lambda x: x["call"]):
            if x["call"] in by_call:
                y = by_call[x["call"]]
                if slug(y["title"]) != slug(x["title"]):
                    raise SystemExit(f"funder_award_id collision inside call {x['call']}: {key} -> {y['title']!r} / {x['title']!r}")
                y["centres"] = sorted(set(y["centres"]) | set(x["centres"]))
                y["project_ids"] = list(dict.fromkeys(y["project_ids"] + x["project_ids"]))
                y["platform"] = " / ".join(dict.fromkeys(p for p in (y["platform"], x["platform"]) if p)) or None
            else:
                by_call[x["call"]] = dict(x)
        g = list(by_call.values())
        last = g[-1]
        given, family = split_name(last["pi_name"] or "")
        calls = [x["call"] for x in g]
        centres = sorted({c for x in g for c in x["centres"]})
        all_ids = list(dict.fromkeys(i for x in g for i in x["project_ids"]))
        alloc = "; ".join(
            f"Call {x['call']}" + (f" ({x['period_start']} to {x['period_end']})" if x["period_start"] else
                                   (f" ({x['call_year']})" if x["call_year"] else ""))
            + (f" at {' and '.join(x['centres'])}" if x["centres"] else "")
            + (f", platform {x['platform']}" if x["platform"] else "")
            for x in g)
        years = [x["call_year"] for x in g if x["call_year"]]
        awards.append({
            "funder_award_id": (g[0]["project_ids"][0] if not key.startswith("GCS-LSP-") else key), "project_ids_all": ", ".join(all_ids) or None,
            "calls": ";".join(str(c) for c in calls), "first_call": str(min(calls)), "last_call": str(max(calls)),
            "centres": ", ".join(centres) or None, "title": last["title"],
            "pi_line": last["pi_line"], "pi_name": last["pi_name"], "lead_given_name": given, "lead_family_name": family,
            "institution": last["institution"], "platform": last["platform"],
            "start_date": g[0]["period_start"], "end_date": last["period_end"],
            "start_year": str(min(years)) if years else None,
            "end_year": (last["period_end"] or "")[:4] or (str(max(years)) if years else None),
            "allocation_text": "GCS Large-Scale Project computing time: " + alloc,
            "landing_page_url": last["landing_page_url"],
        })
    df = pd.DataFrame(awards)
    if df["funder_award_id"].str.lower().duplicated().any():
        raise SystemExit("duplicate funder_award_id")
    log(f"{len(df)} awards from {len(rows)} call entries; with a GCS project id: "
        f"{(~df['funder_award_id'].str.startswith('GCS-LSP-')).sum()}; multi-call: {(df['calls'].str.contains(';')).sum()}")
    for c in ["title", "pi_name", "lead_family_name", "institution", "centres", "start_date", "start_year"]:
        log(f"  {c:16s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")  # runbook §1.2 item 5
    args.output_dir.mkdir(parents=True, exist_ok=True)
    path = args.output_dir / "gauss_centre_projects.parquet"
    df.to_parquet(path, index=False)
    log(f"Wrote {len(df)} rows to {path}")
    if args.skip_upload or args.limit:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    prev = args.output_dir / "_previous_gauss_centre_projects.parquet"
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
