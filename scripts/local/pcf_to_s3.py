#!/usr/bin/env python3
"""
Prostate Cancer Foundation (PCF) research awards to S3
======================================================

PCF publishes one page per funded project under
https://www.pcf.org/our-impact/the-work-we-fund/<program>/<class>/<slug>/
for Young Investigator Awards (classes 2020-2025), Challenge Awards (2019-2025),
TACTICAL Awards (2022, 2025), Creativity Awards (2009-2011, 2016), PCF Dream
Teams (2012) and PCF-VA (VAlor) awards. All pages are listed in the WordPress
Yoast page sitemap (https://www.pcf.org/page-sitemap.xml), which is how this
script enumerates them. Each page has the project title, the investigators
("Principal Investigators: Name, degrees (Institution), ..." or, for Young
Investigators, the awardee name + institution + mentors), and a description.
Many pages carry a named-award heading ("2024 Neil DeFeo - PCF Young
Investigator Award"), which is kept as award_name.

PCF publishes no amounts and no award numbers on these pages. Citing works use
PCF's internal numbers ("17CHAL06", "18YOUN19"), which cannot be mapped to the
pages, so funder_award_id is "PCF-<WordPress page id>" (from the page's
shortlink ?p=<id>).

Output: s3://openalex-ingest/awards/pcf/pcf_projects.parquet
"""

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
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

SITEMAP = "https://www.pcf.org/page-sitemap.xml"
DETAIL_RE = re.compile(r"^https://www\.pcf\.org/our-impact/the-work-we-fund/([^/]+)/([^/]+)/(?!page-\d+/$)([^/]+)/$")
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/pcf/pcf_projects.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"}
REQUEST_DELAY = 0.6
RETRIES = 3
MAX_CONSECUTIVE_NON200 = 5

PROGRAMS = {
    "young-investigator-awards": "Young Investigator Award",
    "challenge-awards": "Challenge Award",
    "tactical-awards": "TACTICAL Award",
    "creativity-awards": "Creativity Award",
    "dream-teams": "Dream Team",
    "va-awards": "PCF-VA Award",
}
LABEL_RE = re.compile(r"^\s*([A-Z][A-Za-z /&-]{2,40}?)\s*:\s*(.*)$", re.S)
PI_LABEL_RE = re.compile(r"principal investigator|^pi$|^pis$|project lead|team lead|leader|^awardee", re.I)
CO_LABEL_RE = re.compile(r"co-?investigator|young investigator|co-?pi|co-?lead|team member|investigators?$", re.I)
SKIP_LABEL_RE = re.compile(r"description|mentor|goal|aim|summary|impact|objective|background|results?|update", re.I)
INST_RE = re.compile(r"Universit|Institute|Cent(?:er|re)|Hospital|School|College|Clinic|Foundation|Harvard|UCLA|UCSF|"
                     r"Mayo|\bNCI\b|Hutch|City of Hope|Sloan|Moffitt|Lifehouse|Mount Sinai|Health|Agency", re.I)
POSITION_RE = re.compile(r"Professor|Fellow|Instructor|Lecturer|Scientist|Resident|Director|Chief|Assistant|Associate", re.I)
NAME_LINE_RE = re.compile(r"^[A-Z][^,:\d]{2,60},\s*(?:Ph\.?\s?D|M\.?D|MBBS|MSc?|DO|PharmD|DrPH|ScD|MPH|DVM|DDS|MBChB|MB|BSc?|MA)\b")
DEGREE_RE = re.compile(
    r"^(Ph\.?D\.?|M\.?D\.?|MBA|MPH|MSc?|MS|MA|BS|BA|ScD|DrPH|MBBS|FRACP|FRCPC|FRCP|PharmD|DVM|RN|MD/MPH|"
    r"MD-PhD|MD/PhD|DPhil|MHS|MSCE|MSCI|MSCR|MAS|FACS|MMSc|DSc|BSc|MBChB|PhD\(c\)|MRCP|MSPH|JD|MHA|MEd|"
    r"FASCO|FAAP|DO|CCRP|MB|BCh|BAO|Jr\.?|Sr\.?|II|III|IV)$", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> tuple[int, str]:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            log(f"GET {url} -> {r.status_code} ({len(r.content)} bytes)")
            if r.status_code in (429, 502, 503, 504):
                time.sleep(5 * (attempt + 1))
                continue
            r.encoding = "utf-8"
            return r.status_code, r.text
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(3 * (attempt + 1))
    log(f"GET {url} failed: {last}")
    return 0, ""


def clean(s: str | None) -> str | None:
    if s is None:
        return None
    s = s.replace("\xa0", " ").replace("​", "").replace("﻿", "")
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\s*\n\s*", "\n", s).strip(" ,;-\n")  # keep ":" (label detection)
    return s or None


def split_name(name: str) -> tuple[str | None, str | None]:
    """Split 'James P. Eisenstein' -> ('James P.', 'Eisenstein') (wolf_to_s3.py canonical)."""
    if not name:
        return None, None
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def person(chunk: str, inst: str | None = None) -> dict | None:
    """'Paul Boutros, PhD, MBA' (+ institution) -> name parts. Degrees dropped."""
    chunk = clean(chunk) or ""
    m = re.search(r"\(([^()]*)\)?\s*$", chunk)
    if m and inst is None:
        inst = clean(m.group(1))
        chunk = chunk[: m.start()]
    parts = [p.strip() for p in re.split(r",|\s[–—-]\s*|(?<=[A-Za-z.])[–—]", chunk) if p.strip()]
    if not parts:
        return None
    if inst is None and len(parts) > 1:  # "Name, MD, PhD - Assistant Member, Fred Hutchinson Cancer Research Center, University of Washington"
        inst = next((re.split(r"\sat\s", q)[-1] for q in parts[1:] if INST_RE.search(q)
                     and (" at " in q or not re.search(r"Professor|Member|Fellow|Director", q))), None)
    toks = parts[0].split()
    while len(toks) > 1 and DEGREE_RE.match(toks[-1].strip(",")):  # "Robert Reiter MD"
        toks.pop()
    name = " ".join(toks).strip()
    if not name or len(name.split()) > 6 or re.search(r"\d|University|Institute|Center|Hospital", name):
        return None
    given, family = split_name(name)
    return {"name": name, "given_name": given, "family_name": family, "institution": inst}


def people_list(text: str) -> list[dict]:
    """'A, PhD (Yale), B, MD, (Yale), C (UCLA)' -> 3 people; also handles ';' separators."""
    text = clean(text) or ""
    out = []
    if "(" in text:
        for ch in re.split(r"(?<=\))\s*[,;]\s*|(?<=\))\s+(?:and\s+)?(?=[A-Z])", text):
            p = person(ch)
            if p:
                out.append(p)
    else:
        for ch in re.split(r";|\n|,\s*(?=[A-Z][a-z]+\s+[A-Z])", text):
            p = person(ch)
            if p:
                out.append(p)
    return out


def lead_from(lead: dict | None, ppl: list[dict]) -> dict | None:
    """First listed PI; replaces a bare awardee-line lead of the same person (it has the institution)."""
    if not ppl:
        return lead
    if lead is None or (lead["family_name"] == ppl[0]["family_name"] and not lead["institution"]):
        return ppl[0]
    return lead


def is_label(x: str) -> bool:
    """A 'Label: ...' line with a known label (a title like 'Self-Seeding: A New Strategy' is not one)."""
    lm = LABEL_RE.match(x)
    return bool(lm) and bool(PI_LABEL_RE.search(lm.group(1)) or CO_LABEL_RE.search(lm.group(1))
                             or SKIP_LABEL_RE.search(lm.group(1)) or re.search(r"investigator", lm.group(1), re.I))


def parse_page(url: str, page: str) -> dict | None:
    m = DETAIL_RE.match(url)
    soup = BeautifulSoup(page, "lxml")
    sl = soup.find("link", rel="shortlink")
    pid = re.search(r"[?&]p=(\d+)", sl["href"]) if sl and sl.get("href") else None
    content = soup.select_one("div.entry-content.wp-block-post-content")
    h1 = clean(soup.find("h1").get_text(" ")) if soup.find("h1") else None
    if not m or not pid or content is None:
        return None
    for br in content.find_all("br"):
        br.replace_with("\n")
    blocks = [clean(el.get_text("")) for el in content.find_all(["p", "h2", "h3", "h4", "li"])
              if not el.find_parent("li")]
    blocks = [b for b in blocks if b]
    # Single-awardee layout (Young Investigators): a "Name, PhD" line, optionally followed by
    # position / institution lines; the project title is a separate block before or after it.
    lead = None
    def is_name_block(x: str) -> bool:
        ls = x.split("\n")
        return bool(NAME_LINE_RE.match(ls[0])) or (  # "Carissa Chu\nUniversity of California, San Francisco"
            len(ls) >= 2 and bool(INST_RE.search(ls[1])) and not INST_RE.search(ls[0])
            and bool(re.fullmatch(r"[A-Z][\w.'’ -]{2,40}", ls[0])) and 2 <= len(ls[0].split()) <= 5)
    name_i = next((i for i, x in enumerate(blocks[:6]) if is_name_block(x)), None)
    used = set()
    if name_i is not None:
        used.add(name_i)
        lines = blocks[name_i].split("\n")
        cands = lines[1:]
        j = name_i + 1
        while j < len(blocks) and j <= name_i + 3 and not LABEL_RE.match(blocks[j]) \
                and not NAME_LINE_RE.match(blocks[j]) and len(blocks[j]) < 160 and not cands:
            used.add(j)
            if INST_RE.search(blocks[j]) or not POSITION_RE.search(blocks[j]):  # "BC Cancer Agency"
                cands.append(blocks[j])
            j += 1
        inst = next((c for c in cands if INST_RE.search(c)), cands[0] if cands else None)
        if inst:
            inst = re.split(r",\s*(?=[^,]*(?:Universit|Institute|Cent(?:er|re)|Hospital|Hutch))", inst)[-1]
        lead = person(lines[0], inst=clean(inst) if inst else None)
    # Title: first non-label, non-person block (else the h1). A different h1 is a named award.
    title = None
    for i, x in enumerate(blocks[:6]):
        if i in used or (is_label(x) and re.match(r"\s*Mentors?\s*:", x, re.I)):
            continue
        if is_label(x) or len(x) > 300:  # past the header: no separate title block, use the h1
            break
        if len(x) >= 12:
            title = re.sub(r"^Project Title:\s*", "", x.replace("\n", " "))
            used.add(i)
        break
    blocks = [x for i, x in enumerate(blocks) if i not in used]
    title = title or h1
    named_award = h1 if h1 and h1 != title else None
    cos, mentors = [], []
    desc_parts, in_desc, expect_label = [], False, None
    for i, b in enumerate(blocks):
        lm = LABEL_RE.match(b)
        if lm and not in_desc:
            label, rest = lm.group(1), lm.group(2)
            expect_label = label
            if SKIP_LABEL_RE.search(label):
                if re.search(r"description|summary|background", label, re.I):
                    in_desc = True
                    if rest:
                        desc_parts.append(rest)
                elif re.search(r"mentor", label, re.I):
                    mentors += [p["name"] for p in people_list(rest)] if rest else []
                continue
            if rest:
                ppl = people_list(rest)
                if "(" not in rest and re.fullmatch(r"(co-?)?investigator", label.strip(), re.I):
                    # single-investigator line (Creativity Awards): "Name, MD - Position, Center, University"
                    one = person(rest)
                    if one and lead is None and not label.lower().startswith("co"):
                        lead = one
                    elif one:
                        cos.append(one)
                elif PI_LABEL_RE.search(label):
                    lead = lead_from(lead, ppl)
                    cos += ppl[1:]
                elif CO_LABEL_RE.search(label):
                    cos += ppl
                expect_label = None
            continue
        if in_desc or ((lead is not None or cos) and len(b) > 150):  # unlabeled narrative after the investigators
            desc_parts.append(b)
            continue
        if expect_label:  # label on its own line, list in the next block
            ppl = people_list(b)
            if PI_LABEL_RE.search(expect_label):
                lead = lead_from(lead, ppl)
                cos += ppl[1:]
            elif CO_LABEL_RE.search(expect_label):
                cos += ppl
            elif re.search(r"mentor", expect_label, re.I):
                mentors += [p["name"] for p in ppl]
            expect_label = None
            continue
    if lead is None and cos:  # pages listing only "Co-Investigator:" lines (Creativity Awards 2009-2011)
        lead, cos = cos[0], cos[1:]
    return {
        "page_id": pid.group(1),
        "program_slug": m.group(1),
        "program": PROGRAMS.get(m.group(1), m.group(1)),
        "class_slug": m.group(2),
        "class_year": (re.search(r"(\d{4})", m.group(2)) or [None, None])[1],
        "title": title,
        "named_award": named_award,
        "pi_name": lead["name"] if lead else None,
        "pi_given_name": lead["given_name"] if lead else None,
        "pi_family_name": lead["family_name"] if lead else None,
        "pi_institution": lead["institution"] if lead else None,
        "co_investigators": json.dumps(cos, ensure_ascii=False),
        "mentors": json.dumps(mentors, ensure_ascii=False),
        "description": clean("\n".join(desc_parts)),
        "landing_page_url": url,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="PCF 'The Work We Fund' award pages -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None)
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the section 1.4 shrink guard")
    args = p.parse_args()

    status, xml = get(SITEMAP)
    if status != 200:
        raise RuntimeError(f"sitemap HTTP {status}")
    urls = sorted({u.strip() for u in re.findall(r"<loc>([^<]+)</loc>", xml) if DETAIL_RE.match(u.strip())})
    # Dream Teams (2012) are SU2C-PCF-AACR co-funded team grants published only as press-release
    # prose (no structured title/PI), so they are excluded.
    urls = [u for u in urls if DETAIL_RE.match(u).group(1) in PROGRAMS and DETAIL_RE.match(u).group(1) != "dream-teams"]
    log(f"Sitemap: {len(urls)} award detail pages")
    if args.limit:
        urls = urls[: args.limit]
    rows, skipped, non200 = [], [], 0
    for i, url in enumerate(urls, 1):
        key = re.sub(r"[^a-z0-9]+", "_", url.split("work-we-fund/")[1].lower()).strip("_")[:150]
        cache = args.cache_dir / f"{key}.html" if args.cache_dir else None
        if cache and cache.exists():
            status, page = 200, cache.read_text()
        else:
            status, page = get(url)
            time.sleep(REQUEST_DELAY)
        if status != 200:
            non200 += 1
            skipped.append(url)
            if non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError("too many consecutive non-200 pages; refusing to truncate")
            continue
        non200 = 0
        if cache:
            args.cache_dir.mkdir(parents=True, exist_ok=True)
            cache.write_text(page)
        rec = parse_page(url, page)
        if rec is None:
            skipped.append(url)
        else:
            rows.append(rec)
        if i % 50 == 0:
            log(f"{i}/{len(urls)} pages, {len(rows)} awards")
    log(f"Parsed {len(rows)} awards, {len(skipped)} pages skipped")
    for u in skipped:
        log(f"  skipped: {u}")

    df = pd.DataFrame(rows)
    df["funder_award_id"] = "PCF-" + df["page_id"]
    dupes = df["funder_award_id"].duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    for c in ["title", "pi_family_name", "pi_institution", "class_year", "description"]:
        log(f"  {c:16s} {df[c].notna().mean():6.1%}")
    log(df.groupby(["program", "class_year"]).size().to_string())

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "pcf_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_pcf_projects.parquet"
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
