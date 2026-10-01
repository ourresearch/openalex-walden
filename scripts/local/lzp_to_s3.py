#!/usr/bin/env python3
"""
Latvian Council of Science (Latvijas Zinātnes padome, LZP) to S3 Data Pipeline
=============================================================================

LZP publishes every project it funds on its own site (Latvian government
unified web platform, Drupal) at https://www.lzp.gov.lv/lv/projekti, one page
per project at /lv/projekts/<slug>. Each page carries a category badge
(FLPP = Fundamental and Applied Research Projects, VPP = National Research
Programme and its sub-programmes, Swiss-Latvian cooperation programme, ...)
and a free-text block with:

    Sākums / Noslēgums (start / end date), Projekta numurs (e.g.
    lzp-2020/1-0088 -- the reference papers cite), Finansējums (EUR),
    Projekta vadītājs (PI), Projektu īsteno (implementing institution),
    science field, Kopsavilkums (summary), Projektu konkurss (call)

Method 5 (static HTML) on the runbook ladder. The national R&D information
system NZDIS (sciencelatvia.gov.lv) is login-only for its project catalogue,
so it is not used.

Scope filter: projects in the category "LZP dalība projektos" (LZP's OWN
participation as a partner in EU/ERDF projects) are not LZP-funded awards and
are excluded. Everything LZP funds (FLPP, VPP, Swiss-Latvian programme,
post-doc and other research pathways) is kept.

Output: s3://openalex-ingest/awards/lzp/lzp_projects.parquet

Usage:
    py -3.13 lzp_to_s3.py --skip-upload --limit 10 --cache-dir /tmp/lzp_cache   # smoke
    py -3.13 lzp_to_s3.py --cache-dir /tmp/lzp_cache                            # full
"""

import argparse
import hashlib
import html as htmllib
import re
import time
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22, runbook §1.2) ---
# (equivalent of sys.stdout.reconfigure(encoding="utf-8") under an alias)
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

BASE = "https://www.lzp.gov.lv"
LIST_URL = BASE + "/lv/projekti?page={page}"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/lzp/lzp_projects.parquet"
EXCLUDED_CATEGORIES = {"LZP dalība projektos"}  # LZP's own partner role in EU projects, not LZP grants

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5
RETRIES = 4
MAX_PAGES = 400

T0 = time.time()


def log(msg: str) -> None:
    print(f"[{time.time() - T0:7.1f}s] {msg}", flush=True)


def get(url: str, cache_dir: Path | None) -> str:
    if cache_dir:
        f = cache_dir / (hashlib.sha1(url.encode()).hexdigest() + ".html")
        if f.exists():
            return f.read_text()
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            time.sleep(REQUEST_DELAY)
            if r.status_code == 200:
                r.encoding = "utf-8"
                if cache_dir:
                    f.write_text(r.text)
                return r.text
            last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = repr(e)
        log(f"  GET {url} attempt {attempt}/{RETRIES}: {last}")
        time.sleep(3 * attempt)
    raise RuntimeError(f"GET {url} failed: {last}")


def list_project_urls(cache_dir: Path | None, limit: int | None) -> list[str]:
    first = get(LIST_URL.format(page=0), None)
    pages = [int(p) for p in re.findall(r'href="\?page=(\d+)"', first)]
    last_page = max(pages) if pages else 0
    log(f"Listing: pager says last page = {last_page}")
    urls: list[str] = []
    consecutive_empty = 0
    page = 0
    while page <= min(last_page, MAX_PAGES):
        s = first if page == 0 else get(LIST_URL.format(page=page), None)
        found = []
        for m in re.finditer(r'href="(/lv/projekts/[^"#?]+)"', s):
            u = BASE + m.group(1)
            if u not in urls and u not in found:
                found.append(u)
        if not found:
            consecutive_empty += 1
            log(f"  page {page}: no project links ({consecutive_empty}/3); continuing")
            if consecutive_empty >= 3:
                break
        else:
            consecutive_empty = 0
        urls.extend(found)
        if page % 10 == 0:
            log(f"  page {page}/{last_page}: {len(urls)} project urls so far")
        if limit and len(urls) >= limit:
            break
        page += 1
    return urls[:limit] if limit else urls


def to_text(fragment: str) -> str:
    fragment = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", fragment, flags=re.S)
    fragment = re.sub(r"<br\s*/?>|</(p|li|ul|div|h\d|tr|td)>|<(p|li|ul|div|h\d|tr|td)[^>]*>", "\n", fragment)
    t = htmllib.unescape(re.sub(r"<[^>]+>", " ", fragment)).replace("\xa0", " ")
    t = ZERO_WIDTH_RE.sub("", t)
    lines =[re.sub(r"[ \t]+", " ", ln).strip() for ln in t.split("\n")]
    return "\n".join(ln for ln in lines if ln)


DATE = r"(\d{1,2})[./]\s?(\d{1,2})[./]\s?(\d{4})"  # 01.01.2026 / 01/12/2018


def iso(d, m, y) -> str | None:
    try:
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}" if 1 <= int(m) <= 12 and 1 <= int(d) <= 31 else None
    except ValueError:
        return None


def after_label(text: str, label_re: str) -> str | None:
    """Value on the same line after the label, else the next line."""
    m = re.search(label_re + r"\s*:?\s*(.*)", text, re.I)
    if not m:
        return None
    val = m.group(1).strip(" :")
    if val:
        return val
    rest = text[m.end():].lstrip("\n").split("\n")
    return rest[0].strip(" :") if rest and rest[0].strip() else None


def parse_amount(s: str | None) -> tuple[float | None, str | None]:
    if not s:
        return None, None
    cur = "EUR" if re.search(r"EUR|€|eiro", s, re.I) else None
    m = re.search(r"(\d[\d  .,]*\d|\d)", s)
    if not m:
        return None, cur
    num = m.group(1).replace(" ", "").replace(" ", "")
    if re.search(r",\d{2}$", num):  # 299 999,50
        num = num.replace(".", "").replace(",", ".")
    else:
        num = num.replace(",", "").replace(".", "") if re.search(r"[.,]\d{3}$", num) else num.replace(",", ".")
    try:
        return float(num), cur or "EUR"
    except ValueError:
        return None, cur


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|asoc\.?\s*prof|profesors|profesore|akad|hab|habil|phd|dr\.\s*habil)\.?,?\s+)+", re.I)


def split_name(name: str | None) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py) with a leading-title strip."""
    if not name:
        return None, None
    tokens = HONORIFIC_RE.sub("", name.strip()).replace(",", " ").split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr",
                "ph.d.", "dr.", "habil.", "dr.habil."}
    while tokens and tokens[-1].lower().strip(",.") in suffixes | {s.strip(".") for s in suffixes}:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


PROJECT_NO_RE = re.compile(r"\b(lzp[-‐–\s]?\d{4}/\d[-‐–]\d{3,5}|VPP[-‐–][A-Z0-9ĀČĒĢĪĶĻŅŠŪŽ\-‐–]+[-‐–]\d{4}/\d[-‐–]\d{3,5}|"
                           r"Nr\.\s*\S+)", re.I)


def parse_project(url: str, page: str) -> dict:
    title_m = re.search(r'<h1[^>]*>(.*?)</h1>', page, re.S)
    title = to_text(title_m.group(1)) if title_m else None
    art_s = page.find('<article class="node-project')
    art_e = page.find("</article>", art_s)
    art = page[art_s:art_e] if art_s >= 0 else page
    cats = [htmllib.unescape(c).strip() for c in re.findall(r'class="badge">([^<]+)</a>', art)]
    status = re.search(r'project__project-status">([^<]+)<', art)
    published = re.search(r"Publicēts:\s*" + DATE, art)
    text = to_text(art)

    # older FLPP pages give month precision: "Sākums : 12/2020 Noslēgums : 12/2021"
    start_m = re.search(r"Sākums\s*:?\s*(\d{1,2})\s*/\s*(\d{4})", text, re.I)
    end_m = re.search(r"(?:Noslēgums|Beigas)\s*:?\s*(\d{1,2})\s*/\s*(\d{4})", text, re.I)
    start = re.search(r"(?:Sākums\s*:?|īstenošanas periods\s*:?\s*(?:no)?)\s*" + DATE, text, re.I)
    end = re.search(r"(?:(?:Noslēgums|Beigas)\s*:?|līdz|īstenošanas periods\s*:?\s*(?:no)?\s*\d{1,2}[./]\d{1,2}[./]\d{4}\.?\s*-)\s*" + DATE, text, re.I)
    project_no = after_label(text, r"(?:Projekta|Platformas)\s+(?:numurs|Nr\.?)")  # IVPP pages say "Platformas"
    # distinct project-number tokens on the page ("Projekta numurs" and "Projekta nr." often label the same one)
    n_numbers = len({t.lower() for t in re.findall(r"\b(?:lzp-?)?\d{4}/\d-\d{3,5}\b", text, re.I)})
    amount_raw = after_label(text, r"(?:Platformas\s+kopējais\s+finansējums|Projekta\s+finansējums|Piešķirtais finansējums|Kopējais finansējums|Finansējums)")
    pi = strip_contact(after_label(text, r"(?:Projekta|Platformas)\s+vadītāj[sa]"))
    inst = strip_contact(after_label(text, r"(?:(?:Projektu|Projekta)\s+(?:īstenotāj[si]|īsteno)(?![a-zāēī])(?!\s*tīmekļa)|Platformas\s+vadošais\s+partneris|"
                                           r"Projektu\s+realizējoš\w*\s+institūcij\w*\b(?!\s+tīmekļa))"))
    partners = strip_contact(after_label(text, r"(?:Projekta\s+sadarbības\s+partner[ia]s?|Sadarbības partneri|Partneri)"))
    field_group = after_label(text, r"Zinātnes nozaru grupa")
    field_main = after_label(text, r"(?:Projekta pamata zinātnes nozare|Zinātnes nozare)")
    call = after_label(text, r"Projektu konkurss")
    named_title = after_label(text, r"(?:Projekta|Platformas)\s+nosaukums")
    summary = None
    ms = re.search(r"(?:(?:Projekta|Platformas)\s+)?[Kk]opsavilkums\s*:?\s*\n?(.*?)(?:\nProjektu konkurss|\Z)", text, re.S)
    if ms:
        summary = ms.group(1).strip() or None
    amount, currency = parse_amount(amount_raw)
    given, family = split_name(pi)
    return {
        "url": url,
        "slug": url.rsplit("/", 1)[-1],
        # the page heading is authoritative; a "Projekta nosaukums" label further down can name a
        # different (parent/related) project on some VPP pages
        "title_lv": title or named_title,
        "page_title": title,
        "labelled_title": named_title,
        "n_project_numbers_on_page": n_numbers,
        "categories": "; ".join(cats) or None,
        "status": status.group(1).strip() if status else None,
        "published_date": iso(*published.groups()) if published else None,
        "project_number_raw": project_no,
        "amount_raw": amount_raw,
        "amount": amount,
        "currency": currency,
        "pi_name": pi,
        "pi_given_name": given,
        "pi_family_name": family,
        "institution": inst,
        "partners": partners,
        "science_group": field_group,
        "science_field": field_main,
        "call": call,
        "summary_lv": summary,
        "start_date": iso(*start.groups()) if start else month_date(start_m, first=True),
        "end_date": iso(*end.groups()) if end else month_date(end_m, first=False),
        "text": text,
    }


ZERO_WIDTH_RE = re.compile("[" + "".join(map(chr, (0x200B, 0x200C, 0x200D, 0xFEFF))) + "]")
DASHES = {0x2010: "-", 0x2011: "-", 0x2013: "-", 0x2014: "-"}  # unicode hyphen/dashes -> ASCII
EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(\.[\w-]+)+")


def strip_contact(v: str | None) -> str | None:
    """Drop e-mail addresses (', e-pasts: x@y.lv') -- personal contact data is never shipped."""
    if not v:
        return v
    v = re.split(r",?\s*e-?pasts?\s*:", v, flags=re.I)[0]
    v = EMAIL_RE.sub("", v).strip(" ,;:")
    return v or None


def month_date(m, first: bool) -> str | None:
    if not m:
        return None
    import calendar
    mon, year = int(m.group(1)), int(m.group(2))
    if not 1 <= mon <= 12:
        return None
    day = 1 if first else calendar.monthrange(year, mon)[1]
    return f"{year:04d}-{mon:02d}-{day:02d}"


def norm_number(raw: str | None) -> str | None:
    """'lzp‐2021/1‐0227' / 'No. lzp-2021/1-0227' -> 'lzp-2021/1-0227' (ASCII hyphens, lower prefix)."""
    if not raw:
        return None
    s = ZERO_WIDTH_RE.sub("", raw).translate(DASHES).strip()
    m = re.search(r"lzp\s*-?\s*(\d{4})\s*/\s*(\d)\s*-\s*(\d{3,5})", s, re.I)
    if m:
        return f"lzp-{m.group(1)}/{m.group(2)}-{m.group(3)}"
    s = re.sub(r"^(Projekta\s+)?(Nr\.|No\.)\s*", "", s, flags=re.I).strip()
    m = re.fullmatch(r"(\d{4})\s*/\s*(\d)\s*-\s*(\d{3,5})", s)
    if m:  # FLPP number published without its "lzp-" prefix (e.g. "2021/1-0290")
        return f"lzp-{m.group(1)}/{m.group(2)}-{m.group(3)}"
    return s or None


def main() -> None:
    p = argparse.ArgumentParser(description="LZP funded projects -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp/lzp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache detail HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()
    if args.cache_dir:
        args.cache_dir.mkdir(parents=True, exist_ok=True)

    urls = list_project_urls(args.cache_dir, args.limit)
    log(f"{len(urls)} project pages to fetch")
    rows = []
    for i, u in enumerate(urls, 1):
        rows.append(parse_project(u, get(u, args.cache_dir)))
        if i % 50 == 0:
            log(f"  fetched {i}/{len(urls)}")
    df = pd.DataFrame(rows)
    log(f"Parsed {len(df)} pages; categories: {df['categories'].value_counts().to_dict()}")

    excl = df["categories"].fillna("").apply(lambda c: any(x in c for x in EXCLUDED_CATEGORIES))
    log(f"Excluding {excl.sum()} pages in {sorted(EXCLUDED_CATEGORIES)} (LZP as project partner, not funder)")
    df = df[~excl].copy()
    # programme-overview pages (e.g. the Swiss-Latvian programme description) carry no project
    # number and no PI -> not grant-level records
    prog = df["project_number_raw"].isna() & df["pi_name"].isna()
    log(f"Excluding {prog.sum()} programme-level pages without project number or PI: {df.loc[prog, 'slug'].tolist()[:15]}")
    df = df[~prog].copy()
    multi = df["n_project_numbers_on_page"] > 1
    if multi.any():
        log(f"  WARNING {multi.sum()} pages list >1 project number (only the first is parsed): {df.loc[multi, 'slug'].tolist()[:15]}")

    df["funder_award_id"] = df["project_number_raw"].apply(norm_number)
    # pages without a published project number get a stable synthetic key from the page slug
    df["award_id_is_synthetic"] = df["funder_award_id"].isna()
    df.loc[df["award_id_is_synthetic"], "funder_award_id"] = "LZP-" + df.loc[df["award_id_is_synthetic"], "slug"]
    # Shared project numbers: the same project published twice (same PI + institution) -> keep one;
    # two DIFFERENT projects under one number (a site data-entry error) -> neither keeps the number,
    # both get the synthetic slug key so no award is attributed to the wrong project.
    key = df["funder_award_id"].str.lower()
    for k, grp in df[key.duplicated(keep=False)].groupby(key[key.duplicated(keep=False)]):
        same = grp[["pi_name", "institution"]].fillna("").drop_duplicates().shape[0] == 1
        if same:
            log(f"  {k}: same project published {len(grp)}x ({grp['slug'].tolist()}) -> keeping first")
            df = df.drop(index=grp.index[1:])
        else:
            log(f"  {k}: {len(grp)} DIFFERENT projects share this number ({grp['pi_name'].tolist()}) -> synthetic keys")
            df.loc[grp.index, "funder_award_id"] = "LZP-" + df.loc[grp.index, "slug"]
            df.loc[grp.index, "award_id_is_synthetic"] = True
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        log(f"  {dupes.sum()} rows share a project number: {df.loc[dupes, ['funder_award_id', 'slug']].values.tolist()[:20]}")
        raise SystemExit("duplicate funder_award_id -- resolve before upload")

    for c in ["title_lv", "funder_award_id", "amount", "pi_family_name", "institution", "start_date",
              "end_date", "summary_lv", "call"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  synthetic ids: {int(df['award_id_is_synthetic'].sum())}")
    log(f"  total EUR {df['amount'].sum():,.0f}")

    df = df.drop(columns=["text"]).astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "lzp_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_lzp_projects.parquet"
    try:  # runbook §1.4: never shrink the corpus on re-ingest
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
