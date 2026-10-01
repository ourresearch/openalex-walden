#!/usr/bin/env python3
"""
Finnish Foundation for Cardiovascular Research (Sydäntutkimussäätiö) to S3
===========================================================================

Source: the foundation's own "Myönnetyt apurahat" (grants awarded) page,
https://www.sydantutkimussaatio.fi/apurahat/myonnetyt-apurahat, which links
one PDF per grant year (2010-2020, 2022-2026; the foundation published no
2021 list, and earlier years are "available from the office" only).

Every PDF has the same layout:

    <Fund name> rahasto  N kpl, [yhteensä] TOTAL €        (fund header, bold)
        <title(s)> <NAME IN BOLD> AMOUNT [€]               (grant line)
        <institution>, <city>                              (roman)
        <project title>                                    (bold 2014+, roman 2010-13)

The grantee's name is exactly the bold run inside the grant line, so the
academic title prefix ("Professori, ylilääkäri") is split off by font, not
by guessing. Each fund header prints its grant count and euro total; the
parser checks every fund against both and fails if a PDF does not reconcile
(--allow-mismatch to override for debugging only).

Research.fi (ladder item 0) does not carry this foundation. Grants to
individual researchers from the foundation's named funds; amounts in EUR.
No grant number is published: funder_award_id is a synthetic
'SYD-<year>-<family>-<given>' slug ('-b', '-c' on a same-year repeat).
Citing works write 6-digit application numbers (200061 style) that the
foundation never publishes.

Output: s3://openalex-ingest/awards/sydantutkimussaatio/sydantutkimussaatio_projects.parquet
"""

import argparse
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import pdfplumber
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (runbook 4.0 grep marker: sys.stdout.reconfigure)
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

SITE = "https://www.sydantutkimussaatio.fi"
INDEX = f"{SITE}/apurahat/myonnetyt-apurahat"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/sydantutkimussaatio/sydantutkimussaatio_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 1.0

FUND_RE = re.compile(r"^(?P<fund>.+?)\s+(?P<n>\d+)\s*kpl,\s*(?:yhteensä\s*)?(?P<total>\d{1,3}(?:[.\s]\d{3})*)\s*€?\s*$")
FUND_NOCOUNT_RE = re.compile(r"^(?P<fund>.+\brahasto)$")
AMOUNT_TAIL_RE = re.compile(r"\s(?P<amt>\d{1,3}(?:\.\d{3})+|\d{3,})\s*€?\s*$")
SKIP_RE = re.compile(r"^(SYDÄNTUTKIMUSSÄÄTIÖ\b|Apurahat \d{4}$|APURAHANSAAJAT\b|Apurahansaajat\b|Apurahasaajat\b|LISÄKSI:?$|Lisäksi:?$|\d{1,2}$)")
# Fund headers whose printed grant count is wrong in the source PDF while the
# printed euro total reconciles exactly with the grants listed under it.
KNOWN_HEADER_COUNT_ERRORS = {(2011, "Juhlavuoden rahasto"): 35}   # header says 34 kpl, 35 grants listed, EUR 982,200 matches


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, dest: Path | None = None) -> bytes:
    if dest and dest.exists():
        return dest.read_bytes()
    last = None
    for attempt in range(4):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            log(f"GET {url} -> {r.status_code} ({len(r.content)} bytes)")
            r.raise_for_status()
            time.sleep(REQUEST_DELAY)
            if dest:
                dest.write_bytes(r.content)
            return r.content
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after retries: {last}")


def split_name(name: str) -> tuple[str | None, str | None]:
    """Split 'James P. Eisenstein' -> ('James P.', 'Eisenstein').
    Verbatim port of wolf_to_s3.py (runbook 2.4.1).

    Strips trailing degree/suffix tokens (PhD, MD, Jr., Sr., II, III) before
    splitting. Last whitespace-separated token = family name; rest = given.
    """
    if not name:
        return None, None
    # Drop trailing degree/suffix tokens
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def euros(s: str) -> float:
    return float(re.sub(r"[^\d]", "", s))


def pdf_lines(path: Path) -> list[dict]:
    """Text lines in reading order across pages, each with a per-character bold
    mask ('B' bold, 'r' roman, ' ' space). Built from words split on font
    change, so the bold run of a grant line is exactly the grantee's name."""
    out = []
    with pdfplumber.open(path) as pdf:
        for pno, page in enumerate(pdf.pages):
            words = page.extract_words(extra_attrs=["fontname"], keep_blank_chars=False)
            rows: list[list[dict]] = []
            for w in sorted(words, key=lambda w: (round(w["top"]), w["x0"])):
                if rows and abs(rows[-1][0]["top"] - w["top"]) < 3:
                    rows[-1].append(w)
                else:
                    rows.append([w])
            for ws in rows:
                ws.sort(key=lambda w: w["x0"])
                text, mask, prev_x1 = "", "", None
                for w in ws:
                    b = "B" if "bold" in w["fontname"].lower() else "r"
                    # words split only by a font/kerning change (gap < 1pt) are one word: '7.50' + '0'
                    sep = "" if prev_x1 is not None and w["x0"] - prev_x1 < 1.0 else " "
                    if prev_x1 is not None:
                        text, mask = text + sep, mask + sep
                    text, mask, prev_x1 = text + w["text"], mask + b * len(w["text"]), w["x1"]
                out.append({"page": pno + 1, "top": ws[0]["top"], "x0": ws[0]["x0"],
                            "text": text, "chars": text, "bold": mask})
    return out


def bold_runs(line: dict) -> tuple[str, str, str]:
    """(roman prefix, bold run, roman suffix) of a grant line."""
    s, b = line["chars"], line["bold"]
    if "B" not in b:
        return s, "", ""
    i, j = b.index("B"), b.rindex("B") + 1
    return s[:i], s[i:j], s[j:]


TITLE_TOKEN_RE = re.compile(r"^([A-ZÅÄÖ][A-Za-zÅÄÖåäö]{0,4}\.?|[a-zåäö].*|.*,)$")


def guess_name(text: str) -> tuple[str, str]:
    """Fallback for the rare grant line printed without a bold name: peel
    leading degree abbreviations / lower-case title words / comma-terminated
    titles off the front, keep at least two tokens as the name."""
    toks = text.split()
    k = 0
    while len(toks) - k > 2 and TITLE_TOKEN_RE.match(toks[k]) and not re.match(r"^[A-ZÅÄÖ][a-zåäö]+-[A-ZÅÄÖ]", toks[k]):
        k += 1
    return " ".join(toks[:k]).strip(" ,"), " ".join(toks[k:])


def classify(ln: dict) -> str:
    t, b = ln["text"], ln["bold"]
    has_b, has_r = "B" in b, "r" in b
    if FUND_RE.match(t) or (FUND_NOCOUNT_RE.match(t) and not has_r):
        return "fund"
    if t.startswith("*"):
        return "footnote"         # '*Dosentti X:n apuraha on myönnetty kahdesta eri rahastosta
    am = AMOUNT_TAIL_RE.search(t)
    if am and has_b and b.rstrip()[-1:] == "r":
        return "grant"            # [roman titles] BOLD NAME roman-amount
    if has_b and has_r:
        return "name_noamt"       # titles + bold name, amount missing or on the next line
    if has_b:
        return "bold"
    return "roman_amt" if am else "roman"


def parse_pdf(path: Path, year: int, allow_mismatch: bool) -> tuple[list[dict], list[str]]:
    lines = [ln for ln in pdf_lines(path) if ln["text"] and not SKIP_RE.match(ln["text"])]
    for i, ln in enumerate(lines):
        prev = lines[i - 1] if i and lines[i - 1]["page"] == ln["page"] else None
        ln["gap_before"] = ln["top"] - prev["top"] if prev else 999.0
        ln["kind"] = classify(ln)
    for i, ln in enumerate(lines):
        ln["gap_after"] = lines[i + 1]["gap_before"] if i + 1 < len(lines) else 999.0

    funds, grants, problems = [], [], []
    fund, cur, pending = None, None, []

    def start(name_lines: list[dict], amount: float | None, how: str) -> dict:
        nonlocal cur
        prefix_bits, name_bits = [], []
        for x in name_lines:
            if "B" in x["bold"]:
                pre, name, _ = bold_runs(x)
                prefix_bits.append(pre)
                name_bits.append(name)
            else:
                prefix_bits.append(x["text"])
        name = re.sub(r"\s+", " ", " ".join(n.strip() for n in name_bits)).strip(" ,*")
        name = name.partition(",")[0].strip(" *")  # 2011: bold run 'Sohvi Hörkkö, Oulu' carries the city
        if name.islower():
            name = name.title()                    # 2015: 'jukka lehtonen'
        prefix = re.sub(r"\s+", " ", " ".join(p.strip() for p in prefix_bits)).strip(" ,")
        cur = {"year": year, "fund": fund["fund"] if fund else None, "title_prefix": prefix, "grantee": name,
               "amount": amount, "amount_source": "line", "parse_rule": how, "body": [], "page": name_lines[-1]["page"]}
        grants.append(cur)
        if fund is None:
            problems.append(f"{year}: grant before first fund header: {name}")
        else:
            fund["got"].append(cur)
        return cur

    i = 0
    while i < len(lines):
        ln, k = lines[i], lines[i]["kind"]
        nxt = lines[i + 1] if i + 1 < len(lines) else None
        if k == "fund":
            fm = FUND_RE.match(ln["text"])
            if fm:
                fund = {"fund": fm.group("fund").strip(), "n": int(fm.group("n")), "total": euros(fm.group("total")), "got": []}
            else:  # 2011 'Suomen Sydänliiton Helmikuun 19. päivän rahasto' header prints no count/total
                fund = {"fund": ln["text"].strip(), "n": None, "total": None, "got": []}
            funds.append(fund)
            cur, pending = None, []
        elif k == "grant":
            start(pending + [ln], euros(AMOUNT_TAIL_RE.search(ln["text"]).group("amt")),
                  "bold_name" if not pending else "bold_name_wrapped")
            pending = []
        elif k == "name_noamt" and nxt is not None and nxt["kind"] == "grant" and ln["gap_after"] < ln["gap_before"]:
            pending.append(ln)                      # name wrapped onto the next line
        elif k == "name_noamt":
            g = start(pending + [ln], None, "bold_name_no_amount")
            pending = []
            if nxt is not None and nxt["kind"] == "roman_amt":   # amount printed on the institution line
                am = AMOUNT_TAIL_RE.search(nxt["text"])
                g["amount"] = euros(am.group("amt"))
                g["parse_rule"] = "bold_name_amount_on_next_line"
                g["body"].append({"text": nxt["text"][: am.start()].strip(), "bold": False, "top": nxt["top"], "page": nxt["page"]})
                i += 1
        elif (k == "roman" and nxt is not None and nxt["kind"] == "grant" and not bold_runs(nxt)[0].strip()
              and ln["gap_before"] > ln["gap_after"] + 3):
            pending.append(ln)                      # title prefix printed on its own line
        elif k == "roman_amt" and cur is None and fund is not None:
            am = AMOUNT_TAIL_RE.search(ln["text"])
            prefix, name = guess_name(ln["text"][: am.start()].strip())
            cur = {"year": year, "fund": fund["fund"], "title_prefix": prefix, "grantee": name,
                   "amount": euros(am.group("amt")), "amount_source": "line", "parse_rule": "no_bold_heuristic",
                   "body": [], "page": ln["page"]}
            grants.append(cur)
            fund["got"].append(cur)
        elif k == "footnote":
            pass
        elif cur is None:
            problems.append(f"{year}: orphan line before any grant: {ln['text']}")
        else:
            cur["body"].append({"text": ln["text"], "bold": k == "bold", "top": ln["top"], "page": ln["page"]})
        i += 1

    for f in funds:
        if f["n"] is None:
            continue
        missing = [g for g in f["got"] if g["amount"] is None]
        known = sum(g["amount"] for g in f["got"] if g["amount"] is not None)
        if len(missing) == 1 and len(f["got"]) == f["n"]:
            missing[0]["amount"] = f["total"] - known      # the fund header prints the total
            missing[0]["amount_source"] = "fund_total_residual"
        n, tot = len(f["got"]), sum(g["amount"] or 0 for g in f["got"])
        expected_n = KNOWN_HEADER_COUNT_ERRORS.get((year, f["fund"]), f["n"])
        if n != expected_n or abs(tot - f["total"]) > 0.5:
            problems.append(f"{year} fund '{f['fund']}': header {f['n']} grants / {f['total']:,.0f} EUR, parsed {n} / {tot:,.0f}")
    if problems and not allow_mismatch:
        raise SystemExit(f"{path.name} does not reconcile:\n  " + "\n  ".join(problems))
    return grants, problems


INST_CONT_RE = re.compile(r"(,|/|-|–|&|\bja|\band|\bof|\bfor|\bthe|\bde|\bin|\bsekä|\bja\.)\s*$", re.I)
CITY_TAIL_RE = re.compile(r",\s*[A-ZÅÄÖ][\w\-.]+$")   # ', Helsinki' / ', USA'


def split_body(g: dict) -> None:
    """Institution vs project title. 2014+ PDFs set titles in bold; 2010-13
    set both in roman, where the institution is the first line, extended to
    the next one while the previous visibly continues (',', 'ja', 'sekä'),
    or, when the previous has no ', <City>' tail yet, while the next starts
    in lower case or ends in ', <City>'. At least one title line is kept."""
    body = g.pop("body")
    if any(b["bold"] for b in body):
        inst = [b["text"] for b in body if not b["bold"]]
        title = [b["text"] for b in body if b["bold"]]
    else:
        k = 1 if body else 0
        while k < len(body) - 1 and (
                INST_CONT_RE.search(body[k - 1]["text"])
                or (body[k]["text"][:1].islower() and CITY_TAIL_RE.search(body[k]["text"]))  # '..., Helsingin' / 'yliopisto, Helsinki'
                or (not CITY_TAIL_RE.search(body[k - 1]["text"])
                    and (body[k]["text"][:1].islower() or CITY_TAIL_RE.search(body[k]["text"])))):
            k += 1
        inst, title = [b["text"] for b in body[:k]], [b["text"] for b in body[k:]]
    join = lambda xs: re.sub(r"\s+", " ", re.sub(r"(\w)-\s+(\w)", r"\1-\2", " ".join(xs))).strip() or None
    g["institution"] = join(inst)
    g["title"] = join(title)


def slug(s: str | None) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-") or "x"


def main() -> None:
    p = argparse.ArgumentParser(description="Sydäntutkimussäätiö grants (PDF lists) -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="parse only the N most recent year PDFs (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp/sydantutkimussaatio"))
    p.add_argument("--cache-dir", type=Path, default=None, help="keep downloaded PDFs here")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-mismatch", action="store_true", help="debug only: do not fail on fund count/total mismatch")
    p.add_argument("--allow-shrink", action="store_true", help="override the 1.4 shrink guard")
    args = p.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    cache = args.cache_dir or (args.output_dir / "pdf")
    cache.mkdir(parents=True, exist_ok=True)

    index = get(INDEX).decode("utf-8")
    links = sorted(set(re.findall(r'href="(/uploads/[^"]+\.pdf)"', index)))
    pdfs = {}
    for link in links:
        m = re.search(r"(20\d{2})\.pdf$", link)
        if m:
            pdfs[int(m.group(1))] = SITE + link
    years = sorted(pdfs, reverse=True)
    log(f"index lists {len(years)} grant-year PDFs: {sorted(years)}")
    if not years:
        raise SystemExit("no grant PDFs found on the index page")
    if args.limit:
        years = years[: args.limit]

    grants, all_problems = [], []
    for y in years:
        dest = cache / Path(pdfs[y]).name
        get(pdfs[y], dest)
        got, probs = parse_pdf(dest, y, args.allow_mismatch)
        all_problems += probs
        for g in got:
            split_body(g)
            g["pdf_url"] = pdfs[y]
        log(f"{y}: {len(got)} grants, EUR {sum(g['amount'] or 0 for g in got):,.0f}, "
            f"{len({g['fund'] for g in got})} funds" + (f", {len(probs)} PROBLEMS" if probs else ", reconciles"))
        grants += got
    for pr in all_problems:
        log(f"  PROBLEM {pr}")

    recs, seen = [], set()
    for g in grants:
        given, family = split_name(g["grantee"])
        base = f"SYD-{g['year']}-{slug(family)[:40]}-{slug(given)[:24]}"
        fid, i = base, 0
        while fid.lower() in seen:
            i += 1
            fid = f"{base}-{chr(ord('a') + i)}"
        if fid.lower() in seen:
            raise SystemExit(f"duplicate funder_award_id {fid}")
        seen.add(fid.lower())
        recs.append({
            "funder_award_id": fid,
            "grant_year": g["year"],
            "fund": g["fund"],
            "grantee": g["grantee"],
            "grantee_title": g["title_prefix"] or None,
            "lead_given_name": given,
            "lead_family_name": family,
            "institution": g["institution"],
            "title": g["title"],
            "amount": g["amount"],
            "currency": "EUR",
            "pdf_page": g["page"],
            "landing_page_url": g["pdf_url"],
        })
    df = pd.DataFrame(recs)
    log(f"{len(df)} grants, {df['grant_year'].min()}-{df['grant_year'].max()}, EUR {df['amount'].sum():,.0f}")
    for c in ["title", "institution", "lead_family_name", "lead_given_name", "grantee_title"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    top = df.groupby(["lead_given_name", "lead_family_name"]).size().sort_values(ascending=False).head(8)
    log(f"  6.4a top grantees: {top.to_dict()}")
    log(f"  funds: {df['fund'].nunique()}; top {df['fund'].value_counts().head(5).to_dict()}")

    df = df.astype("string")
    df["amount"] = pd.to_numeric(df["amount"])
    out = args.output_dir / "sydantutkimussaatio_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"Wrote {len(df)} rows to {out}")
    if args.skip_upload:
        return
    if args.limit:
        raise SystemExit("refusing to upload a --limit smoke file to S3")

    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_sydantutkimussaatio_projects.parquet"
    try:  # runbook 1.4: never shrink the corpus on re-ingest
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev}, new {len(df)}")
        if len(df) < prev and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(out), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
