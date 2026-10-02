#!/usr/bin/env python3
"""
Canadian Cancer Society (CCS / CCS Research Institute) funding results to S3
============================================================================

CCS's searchable grants database (all CCS grants since 1999) was hosted on
CIHR's CRIS web application (webapps.cihr-irsc.gc.ca/funding/Search?p_version=CCS);
CIHR retired CRIS and the link now redirects to a CIHR notices page, so the
full historical portfolio is no longer published. What CCS does publish is
one "funding results" announcement per competition, indexed at
https://cancer.ca/en/research/for-researchers/funding-results, in four layouts:

  1. HTML recipient rows (competitions 2019-2022): "Family, Given /
     institution / title" + per-fiscal-year amounts ("2019/2020 : $300,000").
  2. "table" PDFs (competitions 2016-2019, "View Award Recipients"): the same
     content as text.
  3. Award-recipient PDFs (competitions 2022-2026): one block per grant with
     the applicant + institution (left), total amount + duration "2023-2025"
     (right) and the bold project title followed by a lay summary (middle).
  4. The "Partner-led Grants and Awards" page: one key/value HTML table per
     grant (PI, amount, duration, partner, institution, cancer site) under a
     programme heading.

Abstract/summary-only PDFs, statistics charts and the annual report are not
recipient lists and are skipped; so are Awards for Excellence (prizes named
in prose, no grant table). No CCS grant number is published in any layout.

robots.txt (cancer.ca, cdn.cancer.ca; 2026-10-01): "User-agent: * Disallow:"
(everything allowed), no AI-crawler rules.

Output: s3://openalex-ingest/awards/ccs/ccs_projects.parquet
"""

import argparse
import hashlib
import html
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

INDEX = "https://cancer.ca/en/research/for-researchers/funding-results"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ccs/ccs_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.7
RETRIES = 3
# link text on a results page that points at a recipient list (not abstracts/charts/report)
RECIPIENT_LINK = re.compile(r"recipient|full list|list of (?:grants|competition)|funded pr", re.I)

FY_RE = re.compile(r"(\d{4})(?:\s*/\s*(\d{4}))?\s*:?\s*\$\s*([\d,]+)")
# "Family, Given" PI cell; both sides start with a capital (a title containing a
# comma, e.g. "Social deprivation and environment, and risk of ...", must not match)
NAME_COMMA_RE = re.compile(r"^[A-ZÀ-Ý][^,\d$]{0,35},\s*[A-ZÀ-Ý][^,\d$]{0,35}$")


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, binary: bool = False, cache_dir: Path | None = None):
    cache = None
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        stem = re.sub(r"[^a-zA-Z0-9]+", "_", url.split("//", 1)[1].split("?")[0])[-50:]
        cache = cache_dir / f"{stem}_{hashlib.md5(url.encode()).hexdigest()[:10]}"
        if cache.exists():
            return cache.read_bytes() if binary else cache.read_text()
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            r.raise_for_status()
            time.sleep(REQUEST_DELAY)
            if binary:
                if cache:
                    cache.write_bytes(r.content)
                return r.content
            r.encoding = "utf-8"
            if cache:
                cache.write_text(r.text)
            return r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            log(f"GET {url} failed ({e}); retry {attempt + 1}/{RETRIES}")
            time.sleep(4 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace(" ", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def slug(s: str | None) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


HONORIFIC_RE = re.compile(r"^(?:(?:dr|dre|prof|professor|mr|mrs|ms)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py) plus a leading-honorific strip."""
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


def parse_pi(raw: str | None) -> tuple[str | None, str | None]:
    """'Bhatia, Mick*' -> ('Mick', 'Bhatia'); 'Dr Ananya Banerjee' -> ('Ananya', 'Banerjee')."""
    if not raw:
        return None, None
    s = re.sub(r"[*]+", "", raw)
    s = re.sub(r"\s*\([^)]*\)", "", s)  # 'Puts, Maria (Martine)'
    s = re.sub(r"\s+", " ", HONORIFIC_RE.sub("", s)).strip(" ,")
    if "," in s:
        fam, giv = [p.strip() for p in s.split(",", 1)]
        return (giv or None), (fam or None)
    return split_name(s)


def programme_of(label: str) -> str:
    """'Innovation Grants Competition January 2019' -> 'Innovation Grants'."""
    p = re.sub(r"\b(19|20)\d{2}\b", "", label)
    p = re.sub(r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b", "", p)
    p = re.sub(r"\bCompetition\b|\bResults?( of( the)?)?\b", "", p, flags=re.I)
    return re.sub(r"\s+", " ", p).strip(" -–")


def fy_amounts(s: str) -> tuple[float | None, int | None, int | None]:
    """'2019/2020 : $300,000 ...' -> (total, first FY start year, last FY end year)."""
    hits = FY_RE.findall(s or "")
    if not hits:
        return None, None, None
    total = sum(float(a.replace(",", "")) for _, _, a in hits)
    start = int(hits[0][0])
    end = int(hits[-1][1] or hits[-1][0])
    return total, start, end


# ---------- layout 1: HTML statistic-block rows ----------
def parse_html_rows(page: str, label: str, url: str) -> list[dict]:
    rows = []
    for chunk in page.split('<div class="statistic-block__table-row">')[1:]:
        lab = re.search(r'statistic-block__label">\s*<div class="\'wysiwig">(.*?)</div>\s*</div>', chunk, re.S)
        val = re.search(r'statistic-block__value">\s*<div class="\'wysiwig">(.*?)</div>\s*</div>', chunk, re.S)
        if not lab:
            continue
        parts = [text(p) for p in re.split(r"<br\s*/?>", re.sub(r"</?div[^>]*>", "<br/>", lab.group(1)))]
        parts = [p for p in parts if p]
        if len(parts) < 2:
            continue
        notes = [p for p in parts[2:] if re.match(r"^\(.*\)$|^\*", p)]
        title_parts = [p for p in parts[2:] if p not in notes]
        amount, y0, y1 = fy_amounts(text(val.group(1)) if val else "")
        rows.append(_row("html", label, url, parts[0], parts[1], " ".join(title_parts) or None,
                         amount, y0, y1, " ".join(notes) or None))
    return rows


def _row(layout, label, url, pi_raw, inst, title, amount, y0, y1, note, partner=None, scheme=None):
    given, family = parse_pi(pi_raw)
    return {
        "layout": layout,
        "competition": label,
        "programme": scheme or programme_of(label),
        "source_url": url,
        "pi_raw": pi_raw,
        "lead_given_name": given,
        "lead_family_name": family,
        "institution": inst,
        "title": title,
        "amount": amount,
        "start_year": y0,
        "end_year": y1,
        "note": note,
        "partner": partner,
    }


# ---------- PDF helpers ----------
def pdf_lines(data: bytes) -> list[list[dict]]:
    import fitz  # PyMuPDF
    doc = fitz.open(stream=data, filetype="pdf")
    pages = []
    for p in doc:
        out = []
        for b in p.get_text("dict")["blocks"]:
            for ln in b.get("lines", []):
                spans = [s for s in ln["spans"] if s["text"].strip()]
                if not spans:
                    continue
                t = re.sub(r"\s+", " ", "".join(s["text"] for s in ln["spans"])).strip()
                bold = all(("bold" in s["font"].lower() or "black" in s["font"].lower() or s["flags"] & 16) for s in spans)
                italic = all(("italic" in s["font"].lower() or s["flags"] & 2) for s in spans)
                out.append({"x0": ln["bbox"][0], "y0": ln["bbox"][1], "y1": ln["bbox"][3], "text": t,
                            "bold": bold, "italic": italic})
        pages.append(sorted(out, key=lambda l: (round(l["y0"]), l["x0"])))
    return pages


# ---------- layout 2: "table" PDFs (2016-2019) ----------
def parse_table_pdf(data: bytes, label: str, url: str) -> list[dict]:
    rows = []
    for lines in pdf_lines(data):
        award_hdr = next((l for l in lines if l["text"] == "Award"), None)
        x_split = award_hdr["x0"] - 10 if award_hdr else 400
        left = [l for l in lines if l["x0"] < x_split and l["text"] not in ("Applicant", "Institution", "Title")
                and not re.match(r"^(Last m|Page \d|\d+$)", l["text"])]
        amts = [l for l in lines if l["x0"] >= x_split and l["text"] != "Award"]
        starts = [i for i, l in enumerate(left) if NAME_COMMA_RE.match(re.sub(r"\s*\([^)]*\)", "", l["text"]).rstrip("*").strip())
                  and len(l["text"].split()) <= 7 and (i == 0 or not left[i - 1]["text"].endswith((",", "-")))]
        for k, i in enumerate(starts):
            blk = left[i: starts[k + 1] if k + 1 < len(starts) else len(left)]
            y_lo = blk[0]["y0"] - 4
            y_hi = left[starts[k + 1]]["y0"] - 4 if k + 1 < len(starts) else 10_000
            a = " ".join(l["text"] for l in amts if y_lo <= l["y0"] < y_hi)
            # title lines, then an optional "*co-funded ..." note that may wrap
            body = [l["text"] for l in blk[2:]]
            cut = next((j for j, t in enumerate(body) if t.startswith("*")), len(body))
            notes, title = body[cut:], " ".join(body[:cut]) or None
            amount, y0, y1 = fy_amounts(a)
            rows.append(_row("table_pdf", label, url, blk[0]["text"], blk[1]["text"] if len(blk) > 1 else None,
                             title, amount, y0, y1, " ".join(notes) or None))
    return rows


# ---------- layout 3: award-recipient PDFs (2022-2026) ----------
AMT_RE = re.compile(r"^\$[\d,]+(?:\.\d+)?$")
DUR_RE = re.compile(r"^((?:19|20)\d{2})(?:\s*[-–]\s*((?:19|20)\d{2}))?$")


def parse_award_pdf(data: bytes, label: str, url: str) -> list[dict]:
    rows = []
    for lines in pdf_lines(data):
        anchors = [l for l in lines if l["x0"] > 430 and AMT_RE.match(l["text"])]
        for k, a in enumerate(anchors):
            y_next = anchors[k + 1]["y0"] - 4 if k + 1 < len(anchors) else 10_000
            dur = next((l for l in lines if l["x0"] > 430 and a["y0"] < l["y0"] <= a["y0"] + 20
                        and DUR_RE.match(l["text"])), None)
            if not dur:
                continue  # an amount without a duration line is a chart value, not a grant
            # applicant column: the bold line nearest the anchor in the left-most text column
            cand = [l for l in lines if l["x0"] < 300 and l["bold"] and not AMT_RE.match(l["text"])
                    and a["y0"] - 16 <= l["y0"] < min(y_next, a["y0"] + 60)]
            if not cand:
                continue
            name = min(cand, key=lambda l: abs(l["y0"] - a["y0"]))
            col = [l for l in lines if abs(l["x0"] - name["x0"]) < 6 and name["y0"] < l["y0"] < y_next]
            inst_lines, own_title = [], []
            for l in col:
                if l["bold"] or len(inst_lines) == 2 or l["italic"]:
                    break
                inst_lines.append(l)
            # single-column layout (RTA 2023): italic title right under the institution
            for l in col[len(inst_lines):]:
                if not l["italic"]:
                    break
                own_title.append(l["text"])
            left = [name] + inst_lines
            mid = [l for l in lines if name["x0"] + 60 <= l["x0"] <= 430 and a["y0"] - 4 <= l["y0"] < y_next]
            # title: first run of bold middle-column lines (stop at "Problem:" style headings)
            title, started = [], False
            for l in mid:
                if l["bold"] and not re.match(r"^(Problem|Solution|Impact|Why is this)", l["text"]):
                    title.append(l["text"])
                    started = True
                elif started:
                    break
            partner = next((l["text"] for l in mid if re.match(r"^(Funding )?Partner", l["text"], re.I)), None)
            m = DUR_RE.match(dur["text"]) if dur else None
            y0 = int(m.group(1)) if m else None
            y1 = int(m.group(2) or m.group(1)) if m else None
            inst = " ".join(l["text"] for l in left[1:]) or None
            rows.append(_row("award_pdf", label, url, left[0]["text"], inst,
                             re.sub(r"([a-z])- ([a-z])", r"\1\2",  # "ovarian can- cer" (PDF line-break hyphen)
                                    re.sub(r"\s+", " ", " ".join(title or own_title))).strip() or None,
                             float(a["text"].strip("$").replace(",", "")), y0, y1, None, partner))
    return rows


# ---------- layout 4: partner-led key/value tables ----------
def parse_partner_page(page: str, url: str) -> list[dict]:
    rows = []
    sections = re.split(r'class="expand-collapse__title expand-collapse__title--toggle h3"[^>]*>', page)[1:]
    for sec in sections:
        prog = text(sec[: sec.find("</button>")])
        body = sec[sec.find("</button>"):]
        for m in re.finditer(r"<h4[^>]*>(.*?)</h4>.*?<table[^>]*>(.*?)</table>", body, re.S):
            kv = {}
            for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", m.group(2), re.S):
                cells = [text(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
                if len(cells) >= 2 and cells[0]:
                    kv[cells[0].rstrip(":")] = cells[1]
            amt = re.sub(r"[^\d.]", "", kv.get("Funding Amount") or "")
            dur = re.findall(r"(?:19|20)\d{2}", kv.get("Grant Duration") or "")
            partner = kv.get("Partner(s)")
            partner = None if not partner or partner in {"-", "—", "–"} else partner
            prog_name = re.sub(r"\s+(19|20)\d{2}$", "", prog or "").strip() or None
            rows.append(_row("partner_table", f"Partner-led: {prog}", url, kv.get("Principal Investigator") or kv.get("Awardee"),
                             kv.get("Institution"), text(m.group(1)), float(amt) if amt else None,
                             int(dur[0]) if dur else None, int(dur[-1]) if dur else None,
                             kv.get("Cancer Site(s)"), partner, scheme=prog_name))
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description="CCS funding results -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only process the first N results pages")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache fetched pages/PDFs here")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    idx = get(INDEX, cache_dir=args.cache_dir)
    links = []
    for u, lab in re.findall(r'href="([^"]+)"[^>]*>(.*?)</a>', idx, re.S):
        if "/funding-results/" not in u or u.rstrip("/").endswith("funding-results"):
            continue
        u = u.split("?")[0].split("#")[0]
        u = "https://cancer.ca" + u if u.startswith("/") else u
        if u not in [x[0] for x in links]:
            links.append((u, text(lab)))
    log(f"Index: {len(links)} results pages")
    if args.limit:
        links = links[: args.limit]

    rows, skipped = [], []
    for u, lab in links:
        page = get(u, cache_dir=args.cache_dir)
        h1 = text((re.search(r"<h1[^>]*>(.*?)</h1>", page, re.S) or [None, None])[1]) or lab
        label = re.sub(r"^Results of (the )?", "", h1)
        if "partnered-led-grants-and-awards" in u:
            recs = parse_partner_page(page, u)
        else:
            recs = parse_html_rows(page, label, u)
            for pu, t in re.findall(r'href="(https://cdn\.cancer\.ca[^"]+\.pdf)[^"]*"[^>]*>(.*?)</a>', page, re.S):
                if not RECIPIENT_LINK.search(text(t) or "") or "chart" in pu.lower():
                    continue  # abstracts, statistics charts, annual report
                data = get(html.unescape(pu), binary=True, cache_dir=args.cache_dir)
                got = parse_table_pdf(data, label, pu) if "/table---" in pu else parse_award_pdf(data, label, pu)
                if not got:
                    skipped.append(pu)
                recs += got
        log(f"  {len(recs):3d}  {label[:80]}")
        if not recs:
            skipped.append(u)
        rows += recs

    df = pd.DataFrame(rows)
    # the partner-led page repeats a few grants also listed on a competition page
    df["_k"] = df["lead_family_name"].map(slug) + "|" + df["title"].map(slug)
    dup = df["_k"].duplicated(keep="first") & df["title"].notna()
    log(f"Dropped {dup.sum()} rows repeated across pages (same PI + title)")
    df = df[~dup].drop(columns="_k")
    # ... and sometimes under a different (lay) title: a partner-led table row with the
    # same PI + exact amount as a competition listing (start years within one year)
    # is the same grant -> keep the competition listing. Only across those two
    # sources: standard amounts ($200,000) recur for one PI across competitions.
    df["_a"] = df["lead_family_name"].map(slug) + "|" + df["amount"].map(lambda a: f"{a:.0f}" if pd.notna(a) else "")
    comp = df[df["layout"] != "partner_table"]
    drop = []
    for i, r in df[df["layout"] == "partner_table"].iterrows():
        m = comp[(comp["_a"] == r["_a"]) & ((comp["start_year"] - r["start_year"]).abs() <= 1)]
        if len(m):
            drop.append(i)
            log(f"  partner-led row repeats {m.iloc[0]['competition']!r} / {r['pi_raw']} / {r['amount']:.0f}; dropped")
    df = df.drop(index=drop).drop(columns="_a")
    df = df[df["lead_family_name"].notna() | df["title"].notna()]

    # Synthetic, stable key: CCS-{start year}-{programme}-{family-given}
    base = ("CCS-" + df["start_year"].map(lambda y: str(int(y)) if pd.notna(y) else "na") + "-"
            + df["programme"].map(slug).str[:40] + "-"
            + (df["lead_family_name"].fillna("") + " " + df["lead_given_name"].fillna("")).map(slug))
    multi = base.duplicated(keep=False)
    base = base.where(~multi, base + "-" + df["title"].map(lambda t: hashlib.md5(slug(t).encode()).hexdigest()[:6]))
    df["funder_award_id"] = base
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")

    log(f"Parsed {len(df)} grants; by layout: {df['layout'].value_counts().to_dict()}")
    for c in ["title", "lead_family_name", "institution", "amount", "start_year", "end_year", "partner"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  total CAD {df['amount'].sum():,.0f}; start years {df['start_year'].min()}-{df['start_year'].max()}")
    for s in skipped:
        log(f"  no recipients parsed from: {s}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "ccs_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_ccs_projects.parquet"
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
