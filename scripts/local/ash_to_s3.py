#!/usr/bin/env python3
"""
American Society of Hematology (ASH) to S3 Data Pipeline
========================================================

ASH publishes a recipients page for each of its research, career-development and
training award programmes under https://www.hematology.org/awards/ (server-rendered
Sitecore pages, listed in https://www.hematology.org/sitemap.xml; robots.txt only
disallows Sitecore system paths). This script reads the funding programmes:

  research grants        ASH Bridge Grant (2012-2025), ASH Award for Research Careers in
                         Hematology (ARCH, 2025-), ASH Global Research Award, ASH Research
                         Restart Award (2020), Treating Fairly Award
  career development     ASH Scholar Award (1985-), ASH-Amos (Harold Amos) Medical Faculty
                         Development award (AMFDP / AIMFD)
  fellowship / training  Research Training Award for Fellows (RTAF), HONORS Award, Medical
                         Student Physician-Scientist Award, Graduate Hematology Award, the
                         Hematology Inclusion Pathway awards (HIP Fellow, HIP Graduate
                         Student, HIP Medical Student, HIP Resident), and the funded training
                         visits (Visitor Training Program, Latin American Training Program,
                         African Visitor Training Program)

Not read (recognition, not research funding; listed in the notebook header): honorific
prizes and lectures (Dameshek, Ranney, Rowley, Stratton, Thomas, Beutler, Ham-Wasserman,
Coulter, Mentor / Sadler-Forget, Scott-Drew, Advancing Inclusive Excellence), the Abstract
Achievement / travel awards (incl. the National Partner Society, Kwok, Silver, Toohey,
Bouroncle, IPIG, lymphoma, Gibson and Bigi abstract awards) and the CRTI / Medical
Educators Institute course participant-and-faculty lists.

Pages share one layout: <h3> year (or "Fifteenth Round (2020)" / "2013 Recipients"),
optional <h4> / <strong> category, then one recipient per <li> (<strong>name</strong>,
<br>-separated institution, location or project title), per <p> (Treating Fairly), per
<tr> (training visits: participant + topic, host) or per <br> line (Scholar Award, Amos).

ASH publishes no award numbers, so funder_award_id is the synthetic
"ASH-{programme code}-{year}-{recipient slug}".

Output: s3://openalex-ingest/awards/ash/ash_projects.parquet
"""

import argparse
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup, NavigableString, Tag

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22; see runbook 1.2) ---
# (renamed-sys variant; the canonical call is sys.stdout.reconfigure(encoding="utf-8"))
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

BASE = "https://www.hematology.org/awards/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/ash/ash_projects.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"}
REQUEST_DELAY = 1.0
RETRIES = 4

# (path, programme, code, funding_type, meaning of the 2nd text line after the name)
PROGRAMMES = [
    ("career-enhancement-and-training/bridge-grant/bridge-grant-award-recipients",
     "ASH Bridge Grant", "BRIDGE", "research", "location"),
    ("award-recipients/award-for-research-careers-in-hematology-recipients",
     "ASH Award for Research Careers in Hematology (ARCH)", "ARCH", "research", "title"),
    ("award-recipients/global-research-award-recipients",
     "ASH Global Research Award", "GRA", "research", "location"),
    ("award-recipients/research-restart-award-recipients",
     "ASH Research Restart Award", "RESTART", "research", "location"),
    ("award-recipients/treating-fairly",
     "ASH Treating Fairly Award", "TF", "research", "title"),
    ("award-recipients/scholar-award",
     "ASH Scholar Award", "SCHOLAR", "career_development", "location"),
    ("award-recipients/amos-institute-for-medical-faculty-development-award",
     "ASH-Amos Medical Faculty Development Award (AMFDP / AIMFD)", "AMFDP", "career_development", "location"),
    ("career-enhancement-and-training/research-training-award-for-fellows/previous-award-recipients",
     "ASH Research Training Award for Fellows (RTAF)", "RTAF", "fellowship", "location"),
    ("award-recipients/ash-honors-award",
     "ASH HONORS Award", "HONORS", "training", "location"),
    ("award-recipients/physician-scientist-award",
     "ASH Medical Student Physician-Scientist Award", "PSA", "training", "location"),
    ("award-recipients/ash-graduate-hematology-award-recipients",
     "ASH Graduate Hematology Award", "GHA", "training", "location"),
    ("award-recipients/hip-fellow-award-recipients",
     "HIP Fellow Award", "HIPF", "training", "location"),
    ("award-recipients/hip-graduate-award-recipients",
     "HIP Graduate Student Award", "HIPG", "training", "location"),
    ("award-recipients/hip-medical-student-award",
     "HIP Medical Student Award", "HIPM", "training", "location"),
    ("award-recipients/hip-resident-hematology",
     "HIP Resident Award", "HIPR", "training", "location"),
    ("award-recipients/visitor-training-program",
     "ASH Visitor Training Program", "VTP", "training", "title"),
    ("award-recipients/latin-american-training-program",
     "ASH Latin American Training Program", "LATP", "training", "title"),
    ("award-recipients/african-visitor-training-program-participants",
     "ASH African Visitor Training Program", "AVTP", "training", "title"),
]

YEAR_RE = re.compile(r"\b(19[89]\d|20[0-3]\d)\b")
DEGREE_TOKENS = {"md", "phd", "dphil", "dsc", "scd", "mbbs", "mbchb", "mbbch", "ms", "msc", "mph",
                 "mhs", "mhsc", "mba", "mha", "ma", "bs", "ba", "do", "dvm", "pharmd", "rph", "rn",
                 "np", "mscr", "mpharm", "mphil", "frcpath", "frcpc", "frcp", "mrcp", "dm", "mmed",
                 "frcpa", "bpharm", "bpharma", "mbbs", "mtr", "mscci", "mcr", "mmsc", "mas", "bsc",
                 "jr", "jr.", "sr", "ii", "iii", "iv", "facp", "mpa", "dnp", "aprn", "pa", "bsn",
                 "msn", "cscs", "mmedsc", "ms-hpe", "mse", "meng", "mdiv", "jd", "dds", "dmd"}
JUNK_RE = re.compile(r"^(press release|read the press release|ash news daily|profile|participants?|"
                     r"host mentor|participant and training topic|year|recipients|information|\.)$", re.I)
INST_RE = re.compile(r"universit|college|institut|school|hospital|cent(er|re)|clinic|foundation|"
                     r"laborator|medicine|health|children|research|cancer|blood|mayo|kaiser|"
                     r"memorial|sloan|nih|national", re.I)
HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|mr|mrs|ms)\.?\s+)+", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last = e
            log(f"  GET {url} attempt {attempt + 1} failed: {e}")
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook 2.4.1 helper (wolf_to_s3.py): strip degree / suffix tokens,
    last remaining token = family name."""
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


def clean_person(raw: str) -> tuple[str | None, str | None, str | None, str | None]:
    """'Sophia Adamia, PhD,' / 'Kashif Ali, MPhil, PharmD, RPh (Pakistan)' /
    'Danielle Kirkey, MD (ASH-Peter Steelman Scholar Award recipient)' / 'Léon Kautz, PhD*'
    -> (given, family, degrees, country-in-parentheses)."""
    s = re.sub(r"\s+", " ", raw or "").strip().strip(",;").strip()
    country = None
    for par in re.findall(r"\(([^)]*)\)", s):
        if re.search(r"award|recipient|scholar|winner", par, re.I):
            continue
        if s.strip().endswith(f"({par})") and not re.search(r"\d", par):
            country = par.strip()
    s = re.sub(r"\((?:[^)]*(?:award|recipient|scholar|winner)[^)]*)\)", "", s, flags=re.I)
    if country:
        s = s.replace(f"({country})", "")
    s = re.sub(r"[*†‡]+", "", s).strip().strip(",").strip()
    s = re.sub(r"^\((\w+)\)\s*", "", s)  # '(Justin) Ching Ting Loke' -> preferred-name prefix
    parts = [p.strip() for p in s.split(",")]
    person = parts[0]
    degs = [p for p in parts[1:] if p]
    # degrees glued without a comma: 'Kentson Lam MD, PhD', 'Susanna Curtis MD, PhD'
    toks = person.split()
    # only upper-case / known mixed-case degree tokens: 'Yuhong Ma' keeps its surname
    while len(toks) > 1 and toks[-1].lower().strip(".") in DEGREE_TOKENS and (
            toks[-1].strip(".") == toks[-1].strip(".").upper() or
            toks[-1].strip(".") in {"PhD", "DPhil", "PharmD", "MBChB", "MBBCh", "DSc", "ScD", "MSc",
                                    "BSc", "MMed", "Jr", "Sr", "MPhil", "BPharm"}):
        degs.insert(0, toks.pop())
    person = " ".join(toks)
    given, family = split_name(person)
    return given, family, ", ".join(degs) or None, country


def is_name_line(t: str) -> bool:
    t = t.strip()
    if not t or JUNK_RE.match(t) or len(t) > 80:
        return False
    if re.match(r"^\d+$", t) or t.startswith("*") or re.match(r"^also received", t, re.I):
        return False
    if re.search(r"award|memorial|scholar|recipient|winner|program|round|sponsored|participant|"
                 r"training topic|host mentor|institution$", t, re.I):
        return False
    if YEAR_RE.search(t):
        return False
    return True


def text_lines(el: Tag) -> list[str]:
    """Text of an element split at <br> and block boundaries."""
    # html.parser nests the text after a non-self-closing <br> inside that <br>, so keep the
    # children: insert a newline and unwrap rather than replace.
    for br in el.find_all("br"):
        br.insert_before("\n")
        br.unwrap()
    return [re.sub(r"\s+", " ", l).strip() for l in el.get_text("").split("\n") if l.strip()]


def heading_year(t: str) -> str | None:
    m = YEAR_RE.search(t)
    return m.group(1) if m else None


def parse_page(html_text: str, programme: str, line2: str) -> list[dict]:
    # lxml repairs the unclosed / misspelt tags some pages carry (e.g. "</srong>"), which
    # html.parser turns into nested <li> elements
    soup = BeautifulSoup(html_text, "lxml")
    main = soup.find("main") or soup.body
    for bad in main.select(".testimonial-container, script, style"):
        bad.decompose()
    out = []
    year, category = None, None

    def emit(name_raw, inst=None, extra=None, kind=None, host=None):
        if re.search(r"universit|hospital|health system|institute|college|school of", name_raw.split(",")[0], re.I):
            return  # an institution line that lost its recipient (malformed list item)
        given, family, degs, country = clean_person(name_raw)
        if not family:
            return
        rec = {"programme": programme, "year": year, "category": category,
               "name_raw": re.sub(r"\s+", " ", name_raw).strip(), "lead_given_name": given,
               "lead_family_name": family, "degrees": degs, "institution": inst,
               "location": None, "title": None, "host": host, "country": country}
        if extra:
            if (kind or line2) == "title":
                rec["title"] = extra
            else:
                rec["location"] = extra
        out.append(rec)

    for el in main.find_all(["h2", "h3", "h4", "li", "tr", "p"]):
        if el.find_parent(["li", "tr"]) and el.name in ("p",):
            continue
        txt = re.sub(r"\s+", " ", el.get_text(" ")).strip()
        if el.name in ("h2", "h3"):
            y = heading_year(txt)
            if y:
                year, category = y, None
            continue
        if el.name == "h4":
            y = heading_year(txt)
            if y and len(txt) < 40:
                year, category = y, None
            else:
                category = txt or None
            continue
        if year is None:
            continue
        if el.name == "tr":
            tds = el.find_all("td")
            if len(tds) < 2:
                continue
            a, b = text_lines(tds[0]), text_lines(tds[1])
            if not a or not is_name_line(a[0].split(",")[0]):
                continue
            topic = " ".join(a[1:]) or None
            emit(a[0], inst=None, extra=topic, kind="title", host="; ".join(b) or None)
            continue
        if el.name == "li":
            strong = el.find("strong")
            lines = text_lines(el)
            if not lines:
                continue
            name_raw = strong.get_text("").strip() if strong and strong.get_text(strip=True) else lines[0]
            name_raw = re.sub(r"\s+", " ", name_raw).strip()
            strong_name = name_raw
            if lines[0].startswith(name_raw[:15]):
                name_raw, rest = lines[0], lines[1:]  # 'Name , MD' when degrees sit outside <strong>
            else:
                rest = [l for l in lines if l.strip().strip(",") != name_raw.strip().strip(",")]
            # ARCH layout: '<strong>Name, PhD, </strong><em>Institution,</em> Title' on one line
            em = el.find("em")
            if em and em.get_text(strip=True):
                inst = em.get_text(" ").strip().strip(",").strip()
                tail = el.get_text(" ")
                tail = tail[tail.find(em.get_text(" ")) + len(em.get_text(" ")):]
                emit(strong_name, inst=inst, extra=re.sub(r"\s+", " ", tail).strip().strip(",") or None)
            elif not rest and line2 == "title" and name_raw.count(",") >= 2:
                # 'Name, PhD, Institution, Project title' all inside <strong> (two ARCH 2025 rows)
                parts = [x.strip() for x in name_raw.split(",")]
                k = 1
                while k < len(parts) and parts[k].lower().strip(".") in DEGREE_TOKENS:
                    k += 1
                emit(", ".join(parts[:k]), inst=parts[k] if k < len(parts) else None,
                     extra=", ".join(parts[k + 1:]) or None)
            else:
                emit(name_raw, inst=rest[0].strip(",") if rest else None,
                     extra=" ".join(rest[1:]).strip() or None)
            continue
        # <p>: either a year / category label, a '<strong>name</strong><br>inst<br>title'
        # block (Treating Fairly) or a <br>-separated list of names (Scholar Award, Amos).
        y = heading_year(txt)
        if y and len(txt) < 30:
            year, category = y, None
            continue
        lines = text_lines(el)
        strongs = [s.get_text("").strip() for s in el.find_all("strong") if s.get_text(strip=True)]
        if strongs and lines and lines[0] == strongs[0] and len(strongs) == 1 and len(lines) >= 2 \
                and is_name_line(lines[0]) and "," in lines[0]:
            emit(lines[0], inst=lines[1], extra=" ".join(lines[2:]) or None)
            continue
        if any(re.search(r"the following|formerly|named|partnership|was renamed", l, re.I) for l in lines):
            continue
        prev = None
        for l in lines:
            if l in strongs and not ("," in l and is_name_line(l)):
                category = l.rstrip(":")
                continue
            if not is_name_line(l):
                continue
            looks_inst = bool(INST_RE.search(l)) and not re.search(r",\s*(MD|PhD|DO|MBBS)\b", l)
            if looks_inst and prev is not None and prev.get("institution") is None:
                prev["institution"] = l
                continue
            n = len(out)
            emit(l)
            prev = out[-1] if len(out) > n else None

    # Older Scholar Award years (1985-2012) are bare text after the <h3>: names separated by
    # <br> or newlines, not wrapped in <p>/<ul>, so the element walk above never sees them.
    for h in main.find_all(["h2", "h3"]):
        y = heading_year(h.get_text(" "))
        if not y:
            continue
        buf, block = [], False
        for sib in h.next_siblings:
            if isinstance(sib, Tag) and sib.name in ("h2", "h3"):
                break
            if isinstance(sib, Tag) and sib.name in ("p", "ul", "ol", "table", "div", "h4"):
                block = True
                break
            if isinstance(sib, Tag) and sib.name in ("small", "sup", "script"):
                continue
            if isinstance(sib, Tag) and sib.name == "br":
                buf.append("\n" + "\n".join(sib.stripped_strings))
            else:
                buf.append(sib.get_text("") if isinstance(sib, Tag) else str(sib))
        if block:
            continue
        year, category = y, None
        for l in "".join(buf).split("\n"):
            l = re.sub(r"\s+", " ", l.replace("\xa0", " ")).strip()
            if is_name_line(l):
                emit(l)
    return out


def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def main() -> None:
    p = argparse.ArgumentParser(description="ASH award recipients -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the first N programme pages (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache page HTML here")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the 1.4 shrink guard")
    args = p.parse_args()

    progs = PROGRAMMES[: args.limit] if args.limit else PROGRAMMES
    rows = []
    for path, programme, code, ftype, line2 in progs:
        url = BASE + path
        cache = args.cache_dir / (path.replace("/", "_") + ".html") if args.cache_dir else None
        if cache and cache.exists():
            page = cache.read_text()
        else:
            page = get(url)
            if cache:
                args.cache_dir.mkdir(parents=True, exist_ok=True)
                cache.write_text(page)
            time.sleep(REQUEST_DELAY)
        got = parse_page(page, programme, line2)
        for g in got:
            g.update({"programme_code": code, "funding_type": ftype, "landing_page_url": url})
        yrs = sorted({g["year"] for g in got})
        log(f"{programme}: {len(got)} recipients, years {yrs[0] if yrs else '-'}-{yrs[-1] if yrs else '-'}")
        if not got:
            raise SystemExit(f"{url} parsed to 0 recipients; layout changed?")
        rows += got

    df = pd.DataFrame(rows)
    # RTAF lists each fellow again under 'Second Year Awardees' the following year: same award.
    second = df["category"].fillna("").str.contains(r"second[- ]year", case=False)
    log(f"dropping {int(second.sum())} RTAF 'Second Year Awardees' repeats")
    df = df[~second].copy()
    # The ARCH page lists the 2025 Bridge Grant round (its predecessor) again, with project
    # titles: keep one Bridge row per person and give it the title.
    arch = df["programme_code"] == "ARCH"
    bridge_keys = {(y, f): i for i, y, f in zip(df.index[df["programme_code"] == "BRIDGE"],
                                                df.loc[df["programme_code"] == "BRIDGE", "year"],
                                                df.loc[df["programme_code"] == "BRIDGE", "lead_family_name"])}
    drop = []
    for i, y, f, t in zip(df.index[arch], df.loc[arch, "year"], df.loc[arch, "lead_family_name"], df.loc[arch, "title"]):
        j = bridge_keys.get((y, f))
        if j is not None:
            if t and not df.at[j, "title"]:
                df.at[j, "title"] = t
            drop.append(i)
    log(f"merging {len(drop)} ARCH rows that repeat the same year's Bridge Grant round")
    df = df.drop(index=drop)
    key = df["programme_code"] + "|" + df["year"] + "|" + df["lead_family_name"].map(slug) + "|" + \
        df["lead_given_name"].fillna("").map(slug)
    dup = key.duplicated()
    if dup.any():
        log(f"dropping {int(dup.sum())} exact repeats (same person, programme and year)")
        df = df[~dup].copy()
    df["funder_award_id"] = ["ASH-{}-{}-{}".format(c, y, slug(f"{g or ''} {f}"))
                             for c, y, g, f in zip(df["programme_code"], df["year"],
                                                   df["lead_given_name"], df["lead_family_name"])]
    if df["funder_award_id"].duplicated().any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[df['funder_award_id'].duplicated(), 'funder_award_id'].tolist()[:10]}")
    # country: '(Kenya)' after a training-visit participant, else the location line
    # ('Kumasi, Ghana' -> Ghana; 'Boston, MA' -> United States)
    def loc_country(loc):
        if not isinstance(loc, str) or "," not in loc:
            return None
        last = loc.rsplit(",", 1)[1].strip()
        return "United States" if re.fullmatch(r"[A-Z]{2}", last) else (last or None)
    df["country"] = [c if isinstance(c, str) and c else loc_country(l)
                     for c, l in zip(df["country"], df["location"])]
    df["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log(f"{len(df)} awards")
    for c in ["lead_given_name", "institution", "title", "location", "category", "country"]:
        log(f"  {c:16s} {df[c].notna().mean():6.1%}")
    for prog, n in df["programme"].value_counts().items():
        log(f"    {n:5d}  {prog}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "ash_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload or args.limit:
        log("Upload skipped")
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_ash_projects.parquet"
    try:  # runbook 1.4: never shrink the corpus on re-ingest
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
