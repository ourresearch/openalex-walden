#!/usr/bin/env python3
"""
Kungliga Fysiografiska Sällskapet i Lund (Royal Physiographic Society in Lund) to S3
====================================================================================

What the society publishes at grant level (fysiografen.se, robots.txt allows):

1. "Anslagstilldelning"
   (https://www.fysiografen.se/sv/stipendierochanslag/Anslagstilldelning/):
   the decision lists of the travel grants for young researchers ("Yngre
   forskares resor"), one PDF (one xlsx) per decision round, 2024-2026:
   recipient (mostly "Family, Given"), granted amount in SEK, and in some
   rounds the trip type or the recipient's department.
2. "Tidigare mottagare av anslaget Horisont": the society's Horisont research
   grant, two recipients a year 2023-2025 (name, institution; no amount).

NOT published: the society's main research grants ("Forskningsanslag" from
~20 named funds). The "Anslagsförteckning" page the tracker pointed at lists
the funds and their rules, not recipients; citing works' 5-digit application
numbers (e.g. 40730) appear nowhere on the site. Those grants are not covered.

Parsing: PDF words are grouped into rows by vertical position and assigned to
columns by the header's x positions (name | optional trip-type/department |
amount). A name cell that wraps ("Mohamed Inamdeen," / "Mohamed Fainaz ...")
is joined with the next row.

funder_award_id: synthetic KFS-YFR-<decision date>-<name slug> (travel) and
KFS-HORISONT-<year>-<name slug>; no grant numbers are published.

Output: s3://openalex-ingest/awards/fysiografen/fysiografen_projects.parquet
"""

import argparse
import html
import io
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
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

BASE = "https://www.fysiografen.se"
ALLOC_URL = f"{BASE}/sv/stipendierochanslag/Anslagstilldelning/"
HORISONT_URL = f"{BASE}/sv/stipendierochanslag/tidigare-mottagare-av-anslaget-horisont/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/fysiografen/fysiografen_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
RETRIES = 3
HEADER_WORDS = {"namn", "sökanden"}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> requests.Response:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            r.raise_for_status()
            return r
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def clean(s) -> str | None:
    if s is None:
        return None
    s = re.sub(r"\s+", " ", html.unescape(str(s)).replace("\xa0", " ")).strip()
    return s or None


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|docent)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py): 'Given ... Family'."""
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


def person(name: str) -> tuple[str | None, str | None]:
    if "," in name:
        fam, giv = name.split(",", 1)
        return clean(giv), clean(fam)
    return split_name(name)


def alloc_links(page: str) -> list[tuple[str, str]]:
    """(decision date, file url) for every 'Yngre forskares resor ... tilldelning <date>' link."""
    out = []
    for href, txt in re.findall(r'<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', page, re.S):
        txt = clean(re.sub(r"<[^>]+>", " ", txt)) or ""
        m = re.search(r"tilldelning\s+(\d{4}-\d{2}-\d{2})", txt, re.I)
        if m and re.search(r"\.(pdf|xlsx?)$", href, re.I):
            out.append((m.group(1), BASE + href if href.startswith("/") else href))
    return out


MONTHS = {m: i for i, m in enumerate(["januari", "februari", "mars", "april", "maj", "juni", "juli", "augusti",
                                      "september", "oktober", "november", "december"], 1)}


def heading_date(b: bytes) -> str | None:
    """Decision date printed in the list's own heading ('beslut 6 maj 2025',
    'utdelning 2025-03-10'). Two 2025 links on the page carry each other's
    dates, so the file's heading wins over the link text."""
    import pdfplumber
    with pdfplumber.open(io.BytesIO(b)) as pdf:
        head = " ".join((pdf.pages[0].extract_text() or "").splitlines()[:3])
    m = re.search(r"(\d{4}-\d{2}-\d{2})", head)
    if m:
        return m.group(1)
    m = re.search(r"(\d{1,2})\s+(" + "|".join(MONTHS) + r")\s+(\d{4})", head, re.I)
    if m:
        return f"{m.group(3)}-{MONTHS[m.group(2).lower()]:02d}-{int(m.group(1)):02d}"
    return None


def parse_pdf(b: bytes) -> list[dict]:
    import pdfplumber
    rows_out = []
    with pdfplumber.open(io.BytesIO(b)) as pdf:
        if not any(re.search(r"(?im)^(namn|sökanden)\b.*beviljat", pg.extract_text() or "") for pg in pdf.pages):
            # headerless list ('Totallista ... utdelning 2025-03-10'): 'Family, Given 11567' lines
            for pg in pdf.pages:
                for ln in (pg.extract_text() or "").splitlines():
                    m = re.match(r"^(?P<name>[^,\d]+,[^\d]+?)\s+(?P<amt>\d[\d ]*\d)$", ln.strip())
                    if m:
                        rows_out.append({"recipient_raw": clean(m.group("name")), "detail": None, "note": None,
                                         "amount": float(re.sub(r"\D", "", m.group("amt")))})
            return rows_out
        cols = None
        for pg in pdf.pages:
            lines: dict[int, list] = {}
            for w in pg.extract_words():
                w["text"] = unicodedata.normalize("NFC", w["text"])  # PDFs give decomposed 'ö'
                key = next((k for k in lines if abs(k - w["top"]) < 3), None)
                lines.setdefault(key if key is not None else w["top"], []).append(w)
            carry = ""
            for top in sorted(lines):
                ws = sorted(lines[top], key=lambda w: w["x0"])
                texts = [w["text"] for w in ws]
                if texts and texts[0].lower() in HEADER_WORDS and any(t.lower() == "beviljat" for t in texts):
                    xs = [w["x0"] for w in ws]
                    amt_x = next(w["x0"] for w in ws if w["text"].lower() == "beviljat")
                    mid = [w["x0"] for w in ws[1:] if w["x0"] < amt_x - 5]
                    cols = {"mid": mid[0] - 4 if mid else amt_x - 15, "amt": amt_x - 15,
                            "other": next((w["x0"] - 5 for w in ws if w["text"].lower() == "övrigt" and w["x0"] > amt_x), 10_000)}
                    continue
                if cols is None:
                    continue
                name = " ".join(w["text"] for w in ws if w["x0"] < cols["mid"])
                mid = " ".join(w["text"] for w in ws if cols["mid"] <= w["x0"] < cols["amt"])
                amt = "".join(w["text"] for w in ws if cols["amt"] <= w["x0"] < cols["other"] and re.fullmatch(r"\d+", w["text"]))
                other = " ".join(w["text"] for w in ws if w["x0"] >= cols["other"])
                if name and not amt:
                    if rows_out and rows_out[-1].pop("_open", False):
                        rows_out[-1]["recipient_raw"] = clean(rows_out[-1]["recipient_raw"] + " " + name)
                    elif name.endswith(","):
                        carry = name + " "  # wrapped name cell
                    continue
                if amt and not name and carry:
                    # amount printed between the two lines of a wrapped name
                    rows_out.append({"recipient_raw": clean(carry), "detail": clean(mid), "note": clean(other),
                                     "amount": float(amt), "_open": True})
                    carry = ""
                    continue
                if not name or not amt or re.match(r"(?i)^(summa|totalt?)\b", name):
                    continue
                rows_out.append({"recipient_raw": clean(carry + name), "detail": clean(mid),
                                 "note": clean(other), "amount": float(amt)})
                carry = ""
    return rows_out


def parse_xlsx(b: bytes) -> list[dict]:
    x = pd.read_excel(io.BytesIO(b), header=None, dtype=str).fillna("")
    out, started = [], False
    for row in x.values.tolist():
        cells = [clean(c) for c in row]
        if cells[0] and cells[0].lower() in HEADER_WORDS:
            started = True
            continue
        if started and cells[0]:
            amt = next((c for c in cells[1:] if c and re.fullmatch(r"\d[\d ]*", c)), None)
            out.append({"recipient_raw": cells[0], "detail": None,
                        "note": next((c for c in cells[1:] if c and c != amt), None),
                        "amount": float(re.sub(r"\D", "", amt)) if amt else None})
    return out


def parse_horisont(page: str) -> list[dict]:
    body = page[page.find("Tidigare har följande mottagare"):]
    body = body[: body.find("Användarvillkor")]
    text = clean(re.sub(r"<[^>]+>", "\n", body)) or ""
    lines = [clean(x) for x in html.unescape(re.sub(r"<[^>]+>", "\n", body)).split("\n") if clean(x)]
    out, year = [], None
    for ln in lines:
        if re.fullmatch(r"(19|20)\d\d", ln):
            year = int(ln)
            continue
        m = re.match(r"^([^,]+),\s*(.+)$", ln)
        if year and m and not re.search(r"\d", ln):
            out.append({"year": year, "recipient_raw": m.group(1), "institution": m.group(2)})
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Fysiografiska Sällskapet grant lists -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the first N decision lists")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None)
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    def fetch(url: str, name: str) -> bytes:
        cache = args.cache_dir / name if args.cache_dir else None
        if cache and cache.exists():
            return cache.read_bytes()
        b = get(url).content
        if cache:
            args.cache_dir.mkdir(parents=True, exist_ok=True)
            cache.write_bytes(b)
        time.sleep(0.5)
        return b

    alloc_page = fetch(ALLOC_URL, "anslagstilldelning.html").decode("utf-8", "replace")
    links = alloc_links(alloc_page)
    log(f"Anslagstilldelning: {len(links)} decision lists")
    if args.limit:
        links = links[: args.limit]

    recs = []
    for date, url in links:
        b = fetch(url, url.rsplit("/", 1)[-1])
        is_xls = url.lower().endswith((".xlsx", ".xls"))
        rows = parse_xlsx(b) if is_xls else parse_pdf(b)
        hd = None if is_xls else heading_date(b)
        if hd and hd != date:
            log(f"  {url.rsplit('/', 1)[-1]}: linked as {date}, heading says {hd}; using {hd}")
            date = hd
        log(f"  {date}: {len(rows)} grants, SEK {sum(r['amount'] or 0 for r in rows):,.0f}  {url}")
        for r in rows:
            given, family = person(r["recipient_raw"])
            recs.append({
                "programme": "Yngre forskares resor", "decision_date": date, "year": date[:4],
                "recipient_raw": r["recipient_raw"], "lead_given_name": given, "lead_family_name": family,
                "institution": r["detail"] if r["detail"] and not re.search(r"(?i)möte|meeting|visit|research|institution$|fältarbete|fieldwork|kurs|course", r["detail"]) else None,
                "trip_type": r["detail"] if r["detail"] and re.search(r"(?i)möte|meeting|visit|research|institution$|fältarbete|fieldwork|kurs|course", r["detail"]) else None,
                "note": r["note"], "amount": r["amount"], "currency": "SEK" if r["amount"] is not None else None,
                "funding_type": "travel", "source_url": url,
            })
    hor = parse_horisont(fetch(HORISONT_URL, "horisont.html").decode("utf-8", "replace"))
    log(f"Horisont: {len(hor)} recipients")
    for r in hor:
        given, family = split_name(r["recipient_raw"])
        recs.append({
            "programme": "Forskningsanslaget Horisont", "decision_date": None, "year": str(r["year"]),
            "recipient_raw": r["recipient_raw"], "lead_given_name": given, "lead_family_name": family,
            "institution": r["institution"], "trip_type": None, "note": None, "amount": None, "currency": None,
            "funding_type": "research", "source_url": HORISONT_URL,
        })

    df = pd.DataFrame(recs)
    keys, used = [], {}
    for r in df.itertuples():
        k = (f"KFS-YFR-{r.decision_date}-{slug(r.recipient_raw)}" if r.programme == "Yngre forskares resor"
             else f"KFS-HORISONT-{r.year}-{slug(r.recipient_raw)}")
        used[k] = used.get(k, 0) + 1
        keys.append(k if used[k] == 1 else f"{k}-{used[k]}")
    df["funder_award_id"] = keys
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    log(f"Total {len(df)} awards; by year {df.groupby('year').size().to_dict()}; "
        f"amount {df['amount'].notna().mean():.1%}, SEK {df['amount'].sum():,.0f}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "fysiografen_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_fysiografen_projects.parquet"
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
