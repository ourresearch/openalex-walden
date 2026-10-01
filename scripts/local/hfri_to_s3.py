#!/usr/bin/env python3
"""
HFRI (Hellenic Foundation for Research and Innovation, ELIDEK) to S3
====================================================================

HFRI publishes, for every call, its official "Κατάλογος Χρηματοδότησης"
(funding list: the proposals approved for funding) as PDF announcements, plus
numbered amendments ("Τροποποίηση") when beneficiaries decline and reserves
move up. The lists are grant-level tables:

    Α/Α | Αριθμός Πρότασης | Τίτλος | Ονοματεπώνυμο ΕΥ/ΜΕ-ΕΥ | Φορέας (host) |
    [Συνεργαζόμενοι Φορείς] | Εγκεκριμένος Προϋπολογισμός (€)

Sources (both public, no login):
  * current site  https://www.elidek.gr/details/?id=N  — one page per call
    ("Δράσεις"); the "Κατάλογοι Χρηματοδότησης" tab links the list PDFs.
  * legacy site   https://old.elidek.gr/call/...  — calls 2017-2023, enumerated
    by the WordPress sitemap wp-sitemap-posts-call-1.xml; list PDFs live in
    old.elidek.gr/wp-content/uploads/.
The HFRI grant-management portal (portal.hfri.gr/Projects) the tracker named
is login-only (Vue SPA, every route requiresAuth) and is NOT used.

Only tables whose header carries a proposal-number column plus a title or
name column are read, and only from documents that are funding lists
(ranking tables "Πίνακες Κατάταξης" list number + grade only and are skipped;
"Οριστικά Αποτελέσματα" documents are used only when they carry a funding
table with titles/budgets). A proposal is keyed on (call, proposal number);
when it appears in the base list and in amendments, the latest amendment's
row wins.

Funder: HFRI, except calls HFRI ran with General Secretariat for Research
and Innovation money flagged "(ΓΓΕΚ)" in the call title, which route to GSRT
(F4320321917) — column `funder_key`.

Method 4/5 on the runbook ladder (bulk files + static HTML discovery).

Output: s3://openalex-ingest/awards/hfri/hfri_projects.parquet
"""

import argparse
import hashlib
import html
import json
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, unquote

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). See runbook §1.2.
# (= sys.stdout.reconfigure(encoding="utf-8", line_buffering=True) + utf-8 file I/O.)
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

NEW = "https://www.elidek.gr"
OLD = "https://old.elidek.gr"
MAX_ACTION_ID = 150
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/hfri/hfri_projects.parquet"
# the elidek.gr WAF resets connections from curl-like clients; a browser UA works
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 (openalex-walden; +https://openalex.org)",
           "Accept-Language": "el-GR,el;q=0.9,en;q=0.8"}
RETRIES = 4
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

LIST_TEXT = re.compile(r"Κατάλογ|Χρηματοδότησ|Οριστικ", re.I)
LIST_FILE = re.compile(r"(?i)katalog|xrhm|xrimat|oristik|telika|χρηματοδ|κατάλογ|οριστικ")
SKIP_TEXT = re.compile(r"Προκήρυξη|Οδηγ|Πρότυπα|FAQ|Συχνές", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, binary: bool = False):
    last = None
    for attempt in range(RETRIES):
        try:
            r = SESSION.get(url, timeout=120)
            if r.status_code == 200:
                if binary:
                    return r.content
                r.encoding = "utf-8"
                return r.text
            if r.status_code == 404:
                return None
            last = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last = str(e)
        time.sleep(3 * (attempt + 1))
    log(f"  GET {url} failed: {last}")
    return None


def cached(cache_dir: Path | None, key: str, fetch, binary: bool = False):
    path = cache_dir / key if cache_dir else None
    if path and path.exists():
        return path.read_bytes() if binary else path.read_text()
    data = fetch()
    if path and data is not None:
        path.write_bytes(data) if binary else path.write_text(data)
    return data


def clean(s: str | None) -> str | None:
    if s is None:
        return None
    t = html.unescape(re.sub(r"<[^>]+>", " ", s))
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


# ---------------------------------------------------------------- discovery

def new_site_actions(cache_dir: Path | None) -> list[dict]:
    docs = []
    for i in range(1, MAX_ACTION_ID + 1):
        page = cached(cache_dir, f"action_{i}.html", lambda: get(f"{NEW}/details/?id={i}"))
        if not page or "No record found" in page:
            continue
        m = re.search(r"<main.*", page, re.S)
        body = m.group(0) if m else page
        t = re.search(r"<h[123][^>]*>(.*?)</h[123]>", body, re.S)
        call = clean(t.group(1)) if t else f"action {i}"
        for href, txt in re.findall(r'<a[^>]+href="([^"]+\.(?:pdf|PDF))"[^>]*>(.*?)</a>', body, re.S):
            txt = clean(txt) or ""
            if LIST_TEXT.search(txt) and not SKIP_TEXT.search(txt):
                url = href if href.startswith("http") else NEW + href
                docs.append({"call": call, "call_source": f"{NEW}/details/?id={i}", "doc_label": txt, "url": url})
    log(f"elidek.gr: {len(docs)} candidate list PDFs")
    return docs


def old_site_calls(cache_dir: Path | None) -> list[dict]:
    sm = get(f"{OLD}/wp-sitemap-posts-call-1.xml") or ""
    urls = re.findall(r"<loc>([^<]+)</loc>", sm)
    if not urls:
        raise RuntimeError("old.elidek.gr call sitemap returned no URLs")
    docs = []
    for u in urls:
        key = "oldcall_" + hashlib.md5(u.encode()).hexdigest() + ".html"
        page = cached(cache_dir, key, lambda: get(u))
        if not page:
            continue
        # the h1 is the site logo; the call title is the first non-generic h2
        heads = [clean(h) for h in re.findall(r"<h2[^>]*>(.*?)</h2>", page, re.S)]
        heads = [h for h in heads if h and len(h) > 15 and not re.match(r"Προκηρύξεις|Επισκόπηση", h)]
        call = heads[0] if heads else u
        for pdf in sorted(set(re.findall(r"https?://old\.elidek\.gr/wp-content/uploads/[^\"'\s<>]+\.pdf", page))):
            if LIST_FILE.search(unquote(pdf).rsplit("/", 1)[-1]):
                docs.append({"call": call, "call_source": u, "doc_label": unquote(pdf).rsplit("/", 1)[-1], "url": pdf})
    log(f"old.elidek.gr: {len(urls)} call pages, {len(docs)} candidate list PDFs")
    return docs


# ---------------------------------------------------------------- parsing

def norm(s) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def fold(s) -> str:
    """lower-case, accent-free (headers appear both as 'Τίτλος' and 'ΤΙΤΛΟΣ')"""
    import unicodedata
    t = unicodedata.normalize("NFD", norm(s).lower())
    return "".join(ch for ch in t if unicodedata.category(ch) != "Mn")


def colmap(row: list) -> dict | None:
    m = {}
    for i, c in enumerate(row):
        t = fold(c)
        if not t:
            continue
        if re.search(r"αρ(ιθμος|\.)\s*(προτασης|αιτησης|προτ)|κωδικ", t) and "num" not in m:
            m["num"] = i
        elif "τιτλος" in t and "title" not in m:
            m["title"] = i
        elif re.search(r"ονοματεπωνυμο|υπευθυν|υποτροφ", t) and "name" not in m:
            m["name"] = i
        elif "συνεργαζ" in t:
            m["partners"] = i
        elif re.search(r"φορεας|ιδρυμα", t) and "host" not in m:
            m["host"] = i
        elif re.search(r"προυπολογ|ποσο", t):
            m["budget"] = i
    return m if "num" in m and ("title" in m or "name" in m) else None


def parse_pdf(path: Path) -> tuple[list[dict], str]:
    import pdfplumber
    rows, cm, ncols, head = [], None, None, ""
    with pdfplumber.open(path) as p:
        head = norm(p.pages[0].extract_text() or "")[:1500] if p.pages else ""
        for pi, pg in enumerate(p.pages):
            for tb in pg.extract_tables():
                section = None
                for r in tb:
                    cells = [c for c in r if c]
                    if len(cells) == 1 and not re.fullmatch(r"\d+\.?", norm(cells[0])):
                        section = norm(cells[0])  # table caption, e.g. "ΚΑΤΑΛΟΓΟΣ ΧΡΗΜΑΤΟΔΟΤΗΣΗΣ Ε.Π. 4 ..."
                        continue
                    m = colmap(r)
                    if m:
                        cm, ncols = m, len(r)
                        continue
                    if not cm or len(r) != ncols:
                        continue
                    num = re.sub(r"(?<=\d)\s+(?=\d)", "", norm(r[cm["num"]]))  # "1 026" -> "1026"
                    if not re.fullmatch(r"\d{2,6}", num):
                        continue
                    rec = {k: norm(r[i]) or None for k, i in cm.items()}
                    rec["num"] = num
                    rec["section"] = section
                    rec["page"] = pi + 1
                    rows.append(rec)
    return rows, head


def is_funding_list(label: str, head: str, rows: list[dict]) -> bool:
    """Funding lists name themselves 'Κατάλογος (των προς) Χρηματοδότηση(ς)'.
    Final-results announcements are accepted only when their table carries a
    budget column (i.e. it is the funded table, not a ranking)."""
    lab = label.lower()
    txt = head.upper()
    if re.search(r"κατάλογ|katalog|xrhmatodot|xrimatodot", lab) or ("ΚΑΤΑΛΟΓ" in txt and "ΧΡΗΜΑΤΟΔΟΤ" in txt):
        return True
    secs = " ".join(r.get("section") or "" for r in rows).upper()
    if "ΚΑΤΑΛΟΓ" in secs and "ΧΡΗΜΑΤΟΔΟΤ" in secs:
        return True
    return any(r.get("budget") for r in rows)


def amendment_rank(label: str) -> int:
    lab = label.lower()
    if not re.search(r"τροποποί|trop", lab):
        return 0
    m = re.search(r"(\d+)\s*(?:η|h|ης)?[\s_-]*(?:τροποποί|trop)", lab)
    return int(m.group(1)) if m else 1


def money(s: str | None) -> float | None:
    if not isinstance(s, str) or not s:
        return None
    t = re.sub(r"[^\d.,]", "", s)
    if not t:
        return None
    # Lists mix Greek (99.698,00), English (277,761.00) and odd (20.700.00,
    # 180.000) formats: a trailing separator + 1-2 digits is the decimal part,
    # every other separator is a thousands separator.
    m = re.search(r"[.,](\d{1,2})$", t)
    whole, dec = (t[: m.start()], m.group(1)) if m else (t, "0")
    whole = re.sub(r"[.,]", "", whole)
    try:
        return float(f"{whole or 0}.{dec}")
    except ValueError:
        return None


SUFFIXES = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}


def greek_case(s: str) -> str:
    t = s.title()
    return re.sub(r"σ\b", "ς", t)


def split_surname_first(name: str | None) -> tuple[str | None, str | None]:
    """HFRI lists print 'SURNAME GIVEN [GIVEN2]' in capitals
    ("ΔΕΡΒΙΣΗ ΕΙΡΗΝΗ ΦΩΤΕΙΝΗ"). First token = family name (compound surnames
    are hyphenated, one token), the rest = given. Trailing degree/suffix tokens
    are dropped with the canonical runbook §2.4.1 set. (wolf_to_s3.split_name
    assumes 'Given Family' order, so it can't be reused verbatim here.)"""
    if not isinstance(name, str) or not name.strip():
        return None, None
    tokens = re.sub(r"^(δρ|dr|prof|καθ)\.?\s+", "", name.strip(), flags=re.I).split()
    while tokens and tokens[-1].lower().strip(",.") in SUFFIXES:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, greek_case(tokens[0])
    return greek_case(" ".join(tokens[1:])), greek_case(tokens[0])


FM17 = re.compile(r"(?<!\d)1(η|ης)?\s*Προκήρυξη(ς)?\s+Ερευνητικών\s+Έργων.*μελών\s+ΔΕΠ"
                  r"|1i-prokiryxi-erevnitikon-ergon-elidek-gia-tin-enischysi-ton-melon-dep"
                  r"|1-lt-sup-gt-is-lt-sup-gt-prokiryxis-ereynitikon-ergon-elidek-gia-tin-enischysi-ton-melon-dep", re.I)
FM20 = re.compile(r"(?<!\d)2(η|ης)?\s*Προκήρυξη(ς)?\s+Ερευνητικών\s+Έργων.*μελών\s+ΔΕΠ"
                  r"|2i-prokiryxi-ereynitikon-ergon-el-id-e-k-gia-tin-enischysi-melon-dep", re.I)


def main() -> None:
    p = argparse.ArgumentParser(description="HFRI/ELIDEK funding-list PDFs -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="smoke test: parse only the first N list PDFs")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache HTML/PDFs here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()
    if args.cache_dir:
        args.cache_dir.mkdir(parents=True, exist_ok=True)

    docs = new_site_actions(args.cache_dir) + old_site_calls(args.cache_dir)
    seen, uniq = set(), []
    for d in docs:
        if d["url"] not in seen:
            seen.add(d["url"])
            uniq.append(d)
    docs = uniq[: args.limit] if args.limit else uniq
    log(f"{len(docs)} distinct candidate PDFs")

    tmp = (args.cache_dir or args.output_dir) / "pdf"
    tmp.mkdir(parents=True, exist_ok=True)
    rows, used, failed = [], 0, []
    for n, d in enumerate(docs, 1):
        url = quote(unquote(d["url"]), safe=":/?=&%")
        path = tmp / (hashlib.md5(d["url"].encode()).hexdigest() + ".pdf")
        if not path.exists():
            data = get(url, binary=True)
            if not data or data[:4] != b"%PDF":
                failed.append(d["url"])
                continue
            path.write_bytes(data)
        try:
            recs, head = parse_pdf(path)
        except Exception as e:  # noqa: BLE001
            log(f"  parse error {d['url']}: {e}")
            failed.append(d["url"])
            continue
        if recs and is_funding_list(d["doc_label"], head, recs):
            used += 1
            rank = amendment_rank(d["doc_label"])
            for r in recs:
                rows.append(r | {"call": d["call"], "call_source": d["call_source"], "doc_label": d["doc_label"],
                                 "doc_url": d["url"], "amendment_rank": rank, "doc_seq": n})
        if n % 25 == 0:
            log(f"  {n}/{len(docs)} PDFs, {used} funding lists, {len(rows)} rows")
    log(f"PDFs: {len(docs)} candidates, {used} funding lists, {len(failed)} failed")
    for u in failed:
        log(f"  failed: {unquote(u)}")
    if len(failed) > 0.05 * max(len(docs), 1):
        raise SystemExit(f"{len(failed)} PDFs failed (>5%); rerun (cache resumes)")

    df = pd.DataFrame(rows)
    df["proposal_number"] = df["num"].str.strip()
    # latest amendment wins; ties -> the later document on the page
    df = df.sort_values(["call", "proposal_number", "amendment_rank", "doc_seq"])
    n_all = len(df)
    df = df.drop_duplicates(["call", "proposal_number"], keep="last").copy()
    log(f"{n_all} list rows -> {len(df)} distinct (call, proposal) awards")

    df["funder_key"] = ["gsrt" if "ΓΓΕΚ" in c else "hfri" for c in df["call"]]
    # §2.1.1: citing works write the bare proposal number ("16718") for the
    # 5+-digit numbers of HFRI's unified submission portal. The 2016-2019
    # calls used short per-call numbers that REPEAT across calls (e.g. 1216 is
    # a different project in the 1st and 2nd post-doc calls); only the two
    # faculty-members calls have an official call-coded form that grantees cite
    # (HFRI-FM17-<n>, HFRI-FM20-<n>). Other short-number rows would be
    # ambiguous, so they are dropped (logged) rather than given an invented key.
    def award_id(n: str, c: str, s: str) -> str | None:
        key = c + " " + s
        if FM17.search(key):
            return f"HFRI-FM17-{n}"
        if FM20.search(key):
            return f"HFRI-FM20-{n}"
        return n if re.fullmatch(r"\d{5,}", n) else None
    df["funder_award_id"] = [award_id(n, c, s) for n, c, s in zip(df["proposal_number"], df["call"], df["call_source"])]
    short = df["funder_award_id"].isna()
    for c, n in df.loc[short, "call"].value_counts().items():
        log(f"  dropped {n:4d} short-number rows (no citable call-coded form): {c[:100]}")
    df = df[~short].copy()
    # The same call is often published on both sites (current-site call page and
    # a legacy-site results post), so a proposal can appear under two "calls".
    # Proposal numbers of 5+ digits come from HFRI's single submission-portal
    # sequence, so the same number is the same proposal (titles can differ
    # between a Greek and an English list): prefer the current-site listing,
    # then the latest amendment. Short (<5 digit) numbers from the 2016-2017
    # calls restart per call, so a clash with a different title raises.
    df["_tkey"] = df["title"].fillna("").str.lower().str.replace(r"[^0-9a-zα-ωά-ώ]", "", regex=True).str[:40]
    df["_new"] = df["call_source"].str.startswith(NEW).astype(int)
    df = df.sort_values(["funder_award_id", "_new", "amendment_rank", "doc_seq"])
    conflict = df.groupby("funder_award_id")["_tkey"].nunique()
    conflict = conflict[(conflict > 1) & ~conflict.index.str.fullmatch(r"\d{5,}")]
    if len(conflict):
        raise SystemExit("same short proposal number with different titles: "
                         f"{df[df['funder_award_id'].isin(conflict.index)][['funder_award_id', 'call', 'title']].head(20).to_dict('records')}")
    n_before = len(df)
    df = df.drop_duplicates("funder_award_id", keep="last").drop(columns=["_tkey", "_new"]).copy()
    log(f"{n_before} -> {len(df)} after merging the same proposal listed on both sites")
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        # same proposal number in two different calls would merge in the awards table
        raise SystemExit("duplicate funder_award_id across calls: "
                         f"{df.loc[dupes, ['funder_award_id', 'call']].head(20).to_dict('records')}")
    df["amount"] = df["budget"].map(money) if "budget" in df else None
    df["currency"] = df["amount"].map(lambda a: "EUR" if a is not None and a == a else None)
    names = df["name"].map(split_surname_first)
    df["lead_given_name"] = names.map(lambda x: x[0])
    df["lead_family_name"] = names.map(lambda x: x[1])
    df = df.rename(columns={"name": "pi_name", "host": "institution", "section": "list_section"})
    for c in ["title", "amount", "pi_name", "institution"]:
        if c in df:
            log(f"  {c:14s} {df[c].notna().mean():6.1%}")
    log(f"  total amount EUR {pd.to_numeric(df['amount']).sum():,.0f}")
    for c, n in df["call"].value_counts().items():
        log(f"  {n:5d}  {c[:110]}")

    keep = ["funder_award_id", "proposal_number", "funder_key", "call", "call_source", "list_section", "title",
            "pi_name", "lead_given_name", "lead_family_name", "institution", "partners", "budget", "amount",
            "currency", "doc_label", "doc_url", "amendment_rank", "page"]
    df = df[[c for c in keep if c in df.columns]].astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "hfri_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_hfri_projects.parquet"
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(df)}")
        if len(df) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(df)}); "
                             "rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
