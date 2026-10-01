#!/usr/bin/env python3
"""
Einstein Stiftung Berlin (Einstein Foundation Berlin) to S3 Data Pipeline
========================================================================

The Einstein Foundation Berlin publishes every scientist and initiative it
has funded in its "Fellows & Projects" directory
(https://www.einsteinfoundation.de/en/fellows-projects, TYPO3, server-side
HTML, 25 paginated list pages of tiles; robots.txt disallows only /typo3/).
Each tile gives the detail URL, the name (person or project), the Berlin
host university, the academic field and the funding kind (CSS class
promotion_people / promotion_project / promotion_structure). The funding
programme comes from the detail URL path (einstein-visiting-fellows,
einstein-professors, einstein-research-projects, einstein-centers, ...).
Method 5 (static HTML) on the runbook ladder.

Detail pages are semi-structured; the script reads only labelled facts:
- people: the "Short Vita" entry whose text names the Einstein programme
  ("2010 - 2014 | Einstein Visiting Fellow at Berlin Graduate School ...",
  "Since April 2025 | Einstein Professor") -> start/end year;
- projects / structures: "Funding period:" / "Duration:" / "Laufzeit:" ->
  start/end, and the first person after "Applicant:" / "Contact:" /
  "Contact person:" -> lead investigator (honorifics stripped).
The first prose paragraph is kept as the description. No amounts are
published per award.

Award id: citing works quote the foundation's internal numbers
(EVF-2020-571, A-2012_114, IPF-2012-148, EZ-2014-224), which the site does
not show, so funder_award_id is the synthetic, stable key
"ESB-{programme path}/{item slug}".

Output: s3://openalex-ingest/awards/einstein_foundation/einstein_foundation_projects.parquet
"""

import argparse
import html
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Same block as twcf_to_s3.py; the grep in runbook §4.0 looks for
# sys.stdout.reconfigure (this shim calls it via the renamed module).
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

BASE = "https://www.einsteinfoundation.de"
LIST_URL = f"{BASE}/en/fellows-projects"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/einstein_foundation/einstein_foundation_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5
RETRIES = 4

# programme path segment -> (funder_scheme, funding_type)
PROGRAMMES = {
    "einstein-visiting-fellows": ("Einstein Visiting Fellow", "fellowship"),
    "einstein-bih-visiting-fellows": ("Einstein BIH Visiting Fellow", "fellowship"),
    "einstein-buaoxford-visiting-fellows": ("Einstein BUA/Oxford Visiting Fellow", "fellowship"),
    "einstein-professors": ("Einstein Professorship", "fellowship"),
    "einstein-strategic-professorship": ("Einstein Strategic Professorship", "fellowship"),
    "einstein-international-postdoctoral-fellows": ("Einstein International Postdoctoral Fellow", "fellowship"),
    "einstein-junior-fellows": ("Einstein Junior Fellow", "fellowship"),
    "einstein-research-fellows": ("Einstein Research Fellow", "fellowship"),
    "einstein-postdoctoral-grant": ("Einstein Postdoctoral Grant", "fellowship"),
    "einstein-starting-researcher": ("Einstein Starting Researcher", "fellowship"),
    "einstein-research-projects": ("Einstein Research Project", "research"),
    "einstein-berlin-huji-forschungsvorhaben": ("Einstein Berlin/HUJI Research Project", "research"),
    "einstein-berlin": ("Einstein Berlin/HUJI Research Project", "research"),
    "einstein-berlin-huji-research-project": ("Einstein Berlin/HUJI Research Project", "research"),
    "einstein-professoren": ("Einstein Professorship", "fellowship"),
    "einstein-centers": ("Einstein Center", "research"),
    "einstein-circle": ("Einstein Circle", "research"),
    "einstein-research-units": ("Einstein Research Unit (BUA)", "research"),
    "einstein-foundation-doctoral-programme": ("Einstein Foundation Doctoral Program", "training"),
}

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august", "september",
     "october", "november", "december"], 1)}
MONTHS.update({m: i for i, m in enumerate(
    ["januar", "februar", "märz", "april", "mai", "juni", "juli", "august", "september",
     "oktober", "november", "dezember"], 1)})

PERIOD_LABELS = ("funding period", "funding periods", "duration", "laufzeit", "funding duration",
                 "förderzeitraum", "förderdauer")
PERSON_LABELS = ("applicant", "applicants", "antragsteller", "antragstellerin", "contact", "contacts",
                 "contact person", "contact persons", "ansprechpartner", "ansprechpartnerin", "spokesperson",
                 "spokespersons", "sprecher", "sprecherin", "contac")
STOP_PREFIXES = ("photo:", "foto:", "short vita", "kurzvita", "more about", "mehr über", "contact", "funding period",
                 "duration:", "applicant", "partner:", "partners:", "cooperation partner", "laufzeit", "this page will")
HONORIFIC_RE = re.compile(
    r"^(?:(?:univ\.-prof\.|prof\.|professor|professorin|dr\.(?:-ing\.)?|med\.|vet\.|phil\.|habil\.|rer\.|nat\.|"
    r"pd|jun\.-prof\.|dipl\.-[a-z]+\.?|mr\.?|ms\.?|mrs\.?)\s+)+", re.I)
ORG_WORDS = re.compile(r"universit|institut|center|centre|zentrum|charit|berlin|school|klinik|hospital|"
                       r"department|faculty|fakultät|e-?mail|phone|tel\.|str\.|straße|\d", re.I)


UNIVERSITIES = [  # tile spellings -> one name per Berlin host institution
    (r"^(fu berlin|freie universit)", "Freie Universität Berlin"),
    (r"^humboldt", "Humboldt-Universität zu Berlin"),
    (r"^(tu berlin|technische universit)", "Technische Universität Berlin"),
    (r"^charit", "Charité – Universitätsmedizin Berlin"),
    (r"^universität der künste", "Universität der Künste Berlin"),
    (r"^berlin institute of health", "Berlin Institute of Health"),
]


def norm_university(s: str | None) -> str | None:
    if not s:
        return None
    out = []
    for part in [p.strip() for p in s.split(",") if p.strip()]:
        out.append(next((v for k, v in UNIVERSITIES if re.match(k, part, re.I)), part))
    return ", ".join(dict.fromkeys(out))


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            if r.status_code == 200:
                r.encoding = "utf-8"
                return r.text
            last_err = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def cached_get(url: str, cache: Path | None) -> str:
    if cache and cache.exists():
        return cache.read_text()
    t = get(url)
    if cache:
        cache.write_text(t)
    time.sleep(REQUEST_DELAY)
    return t


def clean(s: str | None) -> str | None:
    if s is None:
        return None
    t = html.unescape(re.sub(r"<[^>]+>", " ", s))
    t = t.replace("‬", "").replace("​", "").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py), after stripping
    German/English academic honorifics ("Prof. Dr. Dr. Thomas Schildhauer")."""
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


def list_tiles(cache_dir: Path | None) -> list[dict]:
    """Walk the paginated directory; page links carry TYPO3 cHash values, so
    they are followed from the pages themselves."""
    todo, done, tiles = {1: LIST_URL}, set(), {}
    last_page = None
    while True:
        pend = sorted(n for n in todo if n not in done)
        if not pend:
            break
        n = pend[0]
        page = cached_get(todo[n], cache_dir / f"list_{n:02d}.html" if cache_dir else None)
        done.add(n)
        for m in re.finditer(r'href="(/en/fellows-projects\?pagenr=(\d+)&amp;cHash=[0-9a-f]+)', page):
            todo.setdefault(int(m.group(2)), BASE + html.unescape(m.group(1)))
            last_page = max(last_page or 0, int(m.group(2)))
        found = 0
        for m in re.finditer(r'<a class="sitemap_tile[^"]*" href="([^"]+)">(.*?)</a>', page, re.S):
            b = m.group(2)

            def span(cls: str) -> str | None:
                x = re.search(r'<span class="%s[^"]*">(.*?)</span>' % cls, b, re.S)
                return clean(x.group(1)) if x else None
            kind = re.search(r'tile_bild[^"]*?promotion_(\w+)', b)
            url = m.group(1) if m.group(1).startswith("http") else BASE + m.group(1)
            tiles.setdefault(url, {"url": url, "tile_name": span("name"), "tile_university": span("uni"),
                                   "academic_field": span("fachbereich-value"),
                                   "promotion": kind.group(1) if kind else None, "list_page": n})
            found += 1
        log(f"list page {n}: {found} tiles ({len(tiles)} total)")
    if last_page and len(done) < last_page:
        raise RuntimeError(f"walked {len(done)} list pages but pagination names page {last_page}")
    return list(tiles.values())


def main_lines(page: str) -> list[str]:
    body = re.sub(r"<script.*?</script>|<style.*?</style>|<svg.*?</svg>|<header.*?</header>|"
                  r"<footer.*?</footer>|<nav.*?</nav>", "", page, flags=re.S)
    i = body.find("<main")
    body = body[i:] if i >= 0 else body
    x = html.unescape(re.sub(r"<[^>]+>", "\n", body))
    x = x.replace("‬", "").replace("​", "").replace("\xa0", " ")
    return [re.sub(r"[ \t]+", " ", l).strip() for l in x.split("\n") if l.strip()]


def year_range(s: str) -> tuple[int | None, int | None, bool]:
    """'2010 - 2014', '2012–2016', 'Since June 2023', 'since 2011', '2015' -> (start, end, open)."""
    ys = [int(y) for y in re.findall(r"(?:19|20)\d{2}", s)]
    if not ys:
        return None, None, False
    if re.match(r"^\s*(since|seit)\b", s, re.I):
        return ys[0], None, True
    return ys[0], (ys[1] if len(ys) > 1 else None), False


def parse_date(s: str, end: bool = False) -> str | None:
    s = s.strip()
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.((?:19|20)\d{2})", s)
    if m:
        return f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    m = re.search(r"([A-Za-zä]+)\s+(\d{1,2}),?\s+((?:19|20)\d{2})", s)
    if m and m.group(1).lower() in MONTHS:
        return f"{m.group(3)}-{MONTHS[m.group(1).lower()]:02d}-{int(m.group(2)):02d}"
    m = re.search(r"(\d{1,2})\s*/\s*((?:19|20)\d{2})", s)
    if m and 1 <= int(m.group(1)) <= 12:
        mo, y = int(m.group(1)), int(m.group(2))
        if end:
            nxt = datetime(y + (mo == 12), mo % 12 + 1, 1)
            return (nxt - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        return f"{y}-{mo:02d}-01"
    m = re.search(r"([A-Za-zä]+)\s+((?:19|20)\d{2})", s)
    if m and m.group(1).lower() in MONTHS:
        mo, y = MONTHS[m.group(1).lower()], int(m.group(2))
        if end:
            nxt = datetime(y + (mo == 12), mo % 12 + 1, 1)
            return (nxt - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        return f"{y}-{mo:02d}-01"
    return None


def labelled(lines: list[str], labels: tuple[str, ...], span: int = 1) -> tuple[int, str] | None:
    """(index, value) for the first 'Label: value' or 'Label:' + next line."""
    for i, l in enumerate(lines):
        m = re.match(r"^([^:]{2,40}?)\s*(?::\s*(.*))?$", l)
        if m and m.group(1).strip().lower() in labels:
            if (m.group(2) or "").strip():
                return i, m.group(2).strip()
            if i + 1 < len(lines):
                return i, " ".join(lines[i + 1:i + 1 + span]).lstrip(": ").strip()
    return None


def looks_like_person(s: str) -> bool:
    if HONORIFIC_RE.match(s):
        return not re.search(r"\d|@|\(at\)", s)
    toks = s.split()
    return (2 <= len(toks) <= 5 and not ORG_WORDS.search(s)
            and all(t[:1].isupper() or t.lower() in {"von", "van", "de", "der", "zu", "da", "di"} for t in toks))


def parse_detail(tile: dict, page: str) -> dict:
    lines = main_lines(page)
    path = re.split(r"/fellows-(?:projects|projekte)/", tile["url"], maxsplit=1)[-1].strip("/").split("/")
    prog_slug = next((p for p in path if p in PROGRAMMES), path[-2] if len(path) > 1 else path[0])
    scheme, ftype = PROGRAMMES.get(prog_slug, (None, "research"))
    title = lines[0] if lines else tile["tile_name"]
    # description: prose after the title up to the first labelled block
    desc = []
    for l in lines[1:]:
        if l.lower().startswith(STOP_PREFIXES) or re.match(r"^[^:]{2,40}:$", l):
            break
        if len(l) > 60:
            desc.append(l)
        if len(desc) >= 3:
            break
    rec = {"page_title": title, "description": " ".join(desc) or None, "programme_slug": prog_slug,
           "funder_scheme": scheme, "funding_type": ftype, "vita_line": None, "period_text": None,
           "start_date": None, "end_date": None, "start_year": None, "end_year": None,
           "lead_name": None, "lead_source": None}
    if tile["promotion"] == "people" or prog_slug in {k for k, v in PROGRAMMES.items() if v[1] == "fellowship"}:
        rec["lead_name"], rec["lead_source"] = tile["tile_name"], "tile"
        low = [l.lower() for l in lines]
        k = next((i for i, l in enumerate(low) if l in ("short vita", "kurzvita")), None)
        if k is not None:
            for j in range(k + 1, min(k + 60, len(lines) - 1)):
                if (re.match(r"^(since\s+|seit\s+)?([A-Za-zä]+\s+)?(19|20)\d{2}", lines[j], re.I)
                        and "einstein" in lines[j + 1].lower()):
                    s, e, _ = year_range(lines[j])
                    rec.update(vita_line=f"{lines[j]} | {lines[j + 1]}", start_year=s, end_year=e)
                    break
    lab = labelled(lines, PERIOD_LABELS)
    if lab:
        txt = lab[1]
        parts = re.split(r"\s+(?:-|–|—|to|bis)\s+|\s*[-–—]\s*(?=\d)", txt, maxsplit=1)
        rec["period_text"] = txt
        sd = parse_date(parts[0])
        ed = parse_date(parts[1], end=True) if len(parts) > 1 else None
        s, e, _ = year_range(txt)
        rec.update(start_date=sd, end_date=ed,
                   start_year=int(sd[:4]) if sd else (rec["start_year"] or s),
                   end_year=int(ed[:4]) if ed else (rec["end_year"] or e))
    if not lab:  # unlabelled "February 15, 2017 to February 29, 2020" (doctoral programme pages)
        for l in lines:
            m = re.match(r"^([A-Za-z]+ \d{1,2}, (?:19|20)\d{2}) to ([A-Za-z]+ \d{1,2}, (?:19|20)\d{2})$", l)
            if m:
                sd, ed = parse_date(m.group(1)), parse_date(m.group(2), end=True)
                rec.update(period_text=l, start_date=sd, end_date=ed,
                           start_year=int(sd[:4]) if sd else None, end_year=int(ed[:4]) if ed else None)
                break
    # Centres and doctoral programmes list an office/coordinator as contact, not a PI.
    no_lead = prog_slug in {"einstein-centers", "einstein-foundation-doctoral-programme"}
    if rec["lead_name"] is None and not no_lead:
        for i, l in enumerate(lines):
            m = re.match(r"^([^:]{2,40}?)\s*(?::\s*(.*))?$", l)
            if not (m and m.group(1).strip().lower() in PERSON_LABELS):
                continue
            first = (m.group(2) or "").strip()
            cands = ([first] if first else []) + lines[i + 1:i + 5]
            for c in cands:
                c = re.split(r",|;| \(| und | and ", c)[0].strip()
                if looks_like_person(c) and HONORIFIC_RE.match(c):  # titled names only: skips coordinators
                    rec["lead_name"], rec["lead_source"] = c, m.group(1).strip()
                    break
            if rec["lead_name"]:
                break
    g, f = split_name(rec["lead_name"] or "")
    rec["lead_given_name"], rec["lead_family_name"] = g, f
    return rec


def main() -> None:
    ap = argparse.ArgumentParser(description="Einstein Foundation Berlin fellows & projects -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = ap.parse_args()
    detail_dir = None
    if args.cache_dir:
        detail_dir = args.cache_dir / "detail"
        detail_dir.mkdir(parents=True, exist_ok=True)

    tiles = list_tiles(args.cache_dir)
    log(f"{len(tiles)} tiles")
    if args.limit:
        tiles = tiles[: args.limit]
    rows = []
    for i, t in enumerate(tiles, 1):
        slug = t["url"].rstrip("/").rsplit("/", 1)[-1][:90]
        page = cached_get(t["url"], detail_dir / f"{slug}.html" if detail_dir else None)
        rec = parse_detail(t, page)
        path = re.split(r"/fellows-(?:projects|projekte)/", t["url"], maxsplit=1)[-1].strip("/")
        person = rec["lead_source"] == "tile"
        # people: "{name} — {programme}" (HHMI pattern, §2.3.1); projects/structures: the page h1
        # (tile names are truncated with "...")
        award_title = f"{t['tile_name']} — {rec['funder_scheme']}" if person else (rec["page_title"] or t["tile_name"])
        rows.append({**t, **rec, "path": path, "funder_award_id": f"ESB-{path}", "award_title": award_title,
                     "university": norm_university(t["tile_university"])})
        if i % 50 == 0:
            log(f"  {i}/{len(tiles)} detail pages")

    df = pd.DataFrame(rows)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    log(f"{len(df)} awards")
    for c in ["award_title", "university", "description", "funder_scheme", "start_year", "end_year",
              "start_date", "lead_family_name"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log("  schemes: " + "; ".join(f"{k}={v}" for k, v in df["funder_scheme"].value_counts(dropna=False).items()))
    log("  lead source: " + "; ".join(f"{k}={v}" for k, v in df["lead_source"].value_counts(dropna=False).items()))

    for c in ["start_year", "end_year"]:
        df[c] = df[c].map(lambda v: None if pd.isna(v) else str(int(v)))
    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "einstein_foundation_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_einstein_foundation_projects.parquet"
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
