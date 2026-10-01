#!/usr/bin/env python3
"""
Arthritis Foundation (US) to S3 Data Pipeline
=============================================

The Arthritis Foundation (AF) has no grants database or export. It announces
each funding round on arthritis.org, and the newer announcements end with a
structured awardee list (investigator, degrees, institution, then programme /
amount / project title). This script reads ONLY those structured lists, from a
fixed set of AF's own announcement pages (PAGES below; method 5, static HTML).
Prose-only announcements (single-investigator stories, the institution-level
Clinical Rheumatology Fellowships, Workforce Enrichment grants, Community Health
awards) are not parsed: their grant details would have to be reconstructed from
narrative text.

Four list layouts are handled:
  person   "N. Name, MD, PhD, (of) Institution" / [programme] / [$amount] / title
           (RA Research Program 2023-2025, FastOA Hip 2024, CARRA-AF 2023-2026)
  colon    "Name, Institution (City, ST):" / title [($amount grant award)]
           under section headings (CARRA-AF 2018 large/small grants, spring 2018)
  labeled  Name / Institution / "Project Title: ..." / "Award Amount: $..." /
           "Lay Summary: ..." (spring 2019 childhood research grants)
  title    "Project title:" / "Name, Degree, Institution, and Name, ..." (AF/AOFAS
           Ankle Arthritis Think Tank 2023)

CARRA-AF grants are the joint Arthritis Foundation / Childhood Arthritis and
Rheumatology Research Alliance programme (AF-funded, CARRA-administered); some
are noted "(funded by AOII)", a donor whose gift AF passes through. AF/AOFAS
Ankle grants are co-funded with the American Orthopaedic Foot & Ankle Society.

AF registers some grant DOIs with Crossref itself (prefix 10.57104, ProposalCentral
award numbers, 33 deposits, mainly the RA Research Program and OA grants), which
are already in OpenAlex as `crossref_work` award rows with title / amount / dates /
PI. A parsed grant whose lead investigator (family name + first initial) and title
(token overlap >= 0.5) match one of those deposits is written with excluded=1,
exclusion_reason=covered_by_crossref_grant, crossref_award=<award number>, and not
shipped by the notebook.

funder_award_id: AF publishes no grant number on these pages, so the key is the
synthetic AF-{year}-{programme code}-{lead family name}-{given initial}.

Output: s3://openalex-ingest/awards/arthritis_foundation/arthritis_foundation_projects.parquet
"""

import argparse
import html
import json
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O. See runbook §1.2. (grep anchor: sys.stdout.reconfigure)
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

BASE = "https://www.arthritis.org"
FUNDER_DOI = "10.13039/100000980"  # Arthritis Foundation, OpenAlex F4320306237
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/arthritis_foundation/arthritis_foundation_projects.parquet"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 openalex-walden/1.0"}
REQUEST_DELAY = 0.5

# (path, layout, programme, programme code, year, co-funder note)
PAGES = [
    ("/news/press-releases-and-statements/2023-grants-to-advance-ra-research", "person",
     "Rheumatoid Arthritis Research Program", "RA", 2023, None),
    ("/news/press-releases-and-statements/2024-grants-to-advance-ra-research", "person",
     "Rheumatoid Arthritis Research Program", "RA", 2024, None),
    ("/news/press-releases-and-statements/2025-rheumatoid-arthritis-research-grants", "person",
     "Rheumatoid Arthritis Research Program", "RA", 2025, None),
    ("/news/news-and-events/fastoa-hip-osteoarthritis-grants", "person",
     "FastOA Initiative, Hip Cohorts", "FASTOA", 2024, None),
    ("/news/press-releases-and-statements/press-release-af-carra-awards-2023", "person",
     "CARRA-Arthritis Foundation Grant Program", "CARRA", 2023, "CARRA"),
    ("/news/press-releases-and-statements/arthritis-foundation-carra-awards-2024", "person",
     "CARRA-Arthritis Foundation Grant Program", "CARRA", 2024, "CARRA"),
    ("/news/press-releases-and-statements/arthritis-foundation-carra-awards-2025", "person",
     "CARRA-Arthritis Foundation Grant Program", "CARRA", 2025, "CARRA"),
    ("/news/press-releases-and-statements/arthritis-foundation-carra-awards-2026", "person",
     "CARRA-Arthritis Foundation Grant Program", "CARRA", 2026, "CARRA"),
    ("/news/carra-arthritis-foundation-grant-awardees", "colon",
     "CARRA-Arthritis Foundation Grant Program", "CARRA", 2018, "CARRA"),
    ("/news/spring-2018-childhood-research-grants", "colon",
     "CARRA-Arthritis Foundation Childhood Research Grants", "CARRA", 2018, "CARRA"),
    ("/news/spring-2019-childhood-research-grants-announced", "labeled",
     "CARRA-Arthritis Foundation Childhood Research Grants", "CARRA", 2019, "CARRA"),
    ("/news/press-releases-and-statements/aofas-arthritis-foundation-ankle-arthritis-grants", "title",
     "AF/AOFAS Ankle Arthritis Think Tank Research Grants", "AOFAS", 2023, "AOFAS"),
]

DEGREES = {
    "md", "phd", "mph", "ms", "msc", "msce", "mscs", "mhs", "scd", "pharmd", "pt", "do", "mbbs",
    "dpt", "rn", "ma", "drph", "msed", "mmsc", "mba", "facr", "frcpc", "otr/l", "bsn", "mphil",
    "dphil", "mbchb", "mres", "facp", "frcp", "mpp", "phd(c)", "ccrp", "cphq", "mhsc", "mmed",
}
STOP_RE = re.compile(r"^(About the |Read Previous|Read Next|Related Content|Related News|To read the full|"
                     r"Visit the|CARRA will continue|Stay in the Know|Give & Get)", re.I)
AMOUNT_RE = re.compile(r"^\$\s*([\d,]+(?:\.\d+)?)$")
SCHEME_RE = re.compile(r"(?i)^[A-Za-z\- ’'()/&]*(?:grant|award)$")
BLOCK_TAGS = r"p|br|li|ul|ol|h[1-6]|div|tr|td|th|section|article|table|blockquote"


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def page_lines(page: str) -> list[str]:
    m = re.search(r"<main.*?</main>", page, re.S)
    s = m.group(0) if m else page
    s = re.sub(r"<(script|style)\b.*?</\1>", "", s, flags=re.S | re.I)
    s = re.sub(rf"</?(?:{BLOCK_TAGS})\b[^>]*>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)  # inline tags (a, em, strong, span, i, b) vanish without a break
    s = html.unescape(s).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    return [re.sub(r"\s+", " ", l).strip() for l in s.split("\n") if l.strip()]


def is_degree(tok: str) -> bool:
    """'MD', 'Ph.D.', and space-joined runs like 'MD MSCE'."""
    words = tok.strip().split()
    return bool(words) and all(w.strip(".").lower().replace(".", "") in DEGREES for w in words)


def parse_people(line: str) -> tuple[list[str], str | None]:
    """'Michael Willey, MD, and Jessica Goetz, PhD, University of Iowa' ->
    (['Michael Willey', 'Jessica Goetz'], 'University of Iowa')."""
    line = re.sub(r"^\d+\.\s*", "", line.strip())
    parts = [p.strip() for p in re.split(r",|\s&\s", line) if p.strip()]
    people, i = [], 0
    while i < len(parts):
        name = re.sub(r"^(?:and|&)\s+", "", parts[i])
        if i + 1 < len(parts) and is_degree(parts[i + 1]) and not is_degree(name):
            people.append(name)
            i += 1
            while i < len(parts) and is_degree(parts[i]):
                i += 1
            continue
        break
    inst = ", ".join(parts[i:]).strip() or None
    if inst:
        inst = re.sub(r"^(?:of|from)\s+", "", inst)
        inst = re.split(r",\s+and\s+", inst)[0].strip()
    return people, inst


HONORIFIC_RE = re.compile(r"^(?:(?:dr|prof|professor|sir|dame|mr|mrs|ms)\.?\s+)+", re.I)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py / twcf_to_s3.py)."""
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


def grant(people, inst, title, amount=None, scheme=None, note=None, description=None, co_inst=None) -> dict:
    return {"people": people, "institution": inst, "co_institution": co_inst, "title": title, "amount": amount,
            "scheme": scheme, "note": note, "description": description}


def parse_person_layout(L: list[str]) -> list[dict]:
    # the list heading: "2025 ... Awardees", "Congratulations to the 2026 ... Awardees:",
    # "2024 Awardees for the FastOA Initiative, Hip Cohorts" (short line, last one wins)
    starts = [i for i, l in enumerate(L) if re.search(r"(?i)\bawardees\b", l) and len(l) < 110]
    if not starts:
        return []
    out, cur = [], None
    for l in L[starts[-1] + 1:]:
        if STOP_RE.match(l):
            break
        if re.fullmatch(r"\d+\.", l):
            continue
        people, inst = parse_people(l)
        if people and inst:
            if cur and not cur["rest"] and not re.match(r"^\d+\.", l):
                # multi-PI grant: further investigator lines before the programme/title
                cur["people"] += people
                cur.setdefault("institutions", []).append(inst)
                continue
            if cur:
                out.append(cur)
            cur = {"people": people, "institution": inst, "rest": []}
        elif cur is not None:
            cur["rest"].append(l)
    if cur:
        out.append(cur)
    grants = []
    for g in out:
        rest, amount, scheme, title_parts, note = g["rest"], None, None, [], None
        j = 0
        while j < len(rest):
            l = rest[j]
            m = AMOUNT_RE.match(l.replace("$ ", "$"))
            if m:
                amount = float(m.group(1).replace(",", ""))
            elif scheme is None and not title_parts and j + 1 < len(rest) and SCHEME_RE.match(f"{l} {rest[j + 1]}") \
                    and not SCHEME_RE.match(l):
                scheme = f"{l} {rest[j + 1]}"  # programme name broken over two lines
                j += 1
            elif scheme is None and not title_parts and SCHEME_RE.match(l) and len(rest) > j + 1:
                scheme = l
            elif re.fullmatch(r"\(funded by [^)]+\)", l, re.I):
                note = l.strip("()")
            else:
                title_parts.append(l)
            j += 1
        grants.append(grant(g["people"], g["institution"], " ".join(title_parts) or None, amount, scheme, note,
                            co_inst=(g.get("institutions") or [None])[0]))
    return grants


def parse_colon_layout(L: list[str]) -> list[dict]:
    grants, scheme, section_amount = [], None, None
    i = 0
    while i < len(L):
        l = L[i]
        if STOP_RE.match(l):
            break
        if re.search(r"(?i)^(large grant awardees|small grants|spring small grants|fellows small grants)$", l):
            scheme, section_amount = l.title().replace("Awardees", "").strip(), None
        m_amt = re.search(r"receive \$([\d,]+) awards", l)
        if m_amt:
            section_amount = float(m_amt.group(1).replace(",", ""))
        m_k = re.search(r"grants? of \$(\d+)k each", l)
        if m_k and "up to" not in l:  # "Three small grants of $25k each" (exact), not "up to $25k"
            section_amount = float(m_k.group(1)) * 1000
        # "Name, Institution (City, ST): Title [($24,343 grant award)] [(funded by AOII)]"
        # (one line) or the same split over two lines after the colon
        m = re.match(r"^(?P<name>[^,:]+),\s*(?P<inst>[^:]+?)(?:\s*\([^)]*\))?:\s*(?P<title>.*)$", l)
        if m and scheme and not re.search(r"(?i)congratulations|were awarded|selected to", l):
            title, step = m.group("title").strip(), 1
            if not title and i + 1 < len(L):
                title, step = L[i + 1], 2
            note = None
            fb = re.search(r"\s*\((funded by [^)]+)\)\s*$", title, re.I)
            if fb:
                note, title = fb.group(1), title[: fb.start()].strip()
            elif i + step < len(L) and re.fullmatch(r"\(funded by [^)]+\)", L[i + step], re.I):
                note, step = L[i + step].strip("()"), step + 1
            amt = re.search(r"\(\$([\d,]+) grant award\)?\s*$", title)
            amount = float(amt.group(1).replace(",", "")) if amt else section_amount
            title = re.sub(r"\s*\(\$[\d,]+ grant award\)?\s*$", "", title)
            if title:
                grants.append(grant([m.group("name").strip()], m.group("inst").strip(), title, amount, scheme, note))
            i += step
            continue
        i += 1
    return grants


def parse_labeled_layout(L: list[str]) -> list[dict]:
    grants = []
    for i, l in enumerate(L):
        if not re.match(r"(?i)^project title\s*:?", l) or i < 2:
            continue
        title = re.sub(r"(?i)^project title\s*:?\s*", "", l).strip(" “”\"")
        people, _ = parse_people(L[i - 2])
        inst = L[i - 1]
        if not people:
            continue
        amount = description = None
        for k in range(i + 1, min(i + 4, len(L))):
            if re.match(r"(?i)^award amount", L[k]):
                a = re.search(r"\$([\d,]+)", L[k])
                amount = float(a.group(1).replace(",", "")) if a else None
            if re.match(r"(?i)^lay summary", L[k]):
                description = re.sub(r"(?i)^lay summary\s*:?\s*", "", L[k]).strip()
        co_inst = None
        if len(people) > 1:  # "University of Iowa & University of Liverpool": one per investigator
            insts = [x.strip() for x in re.split(r"\s+&\s+", inst)]
            inst, co_inst = insts[0], (insts[1] if len(insts) > 1 else None)
        grants.append(grant(people, inst, title, amount, None, None, description, co_inst=co_inst))
    return grants


def parse_title_layout(L: list[str]) -> list[dict]:
    starts = [i for i, l in enumerate(L) if re.search(r"(?i)recipients:$", l)]
    if not starts:
        return []
    grants, i = [], starts[-1] + 1
    while i + 1 < len(L):
        l = L[i]
        if STOP_RE.match(l) or l.startswith("“"):
            break
        if l.endswith(":"):
            people, inst = parse_people(L[i + 1])
            if people:
                grants.append(grant(people, inst, l.rstrip(":").strip(), None, None, None))
                i += 2
                continue
        i += 1
    return grants


PARSERS = {"person": parse_person_layout, "colon": parse_colon_layout,
           "labeled": parse_labeled_layout, "title": parse_title_layout}


def fold(s: str | None) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(ch for ch in s if not unicodedata.combining(ch)).lower()


def tokens(s: str | None) -> set[str]:
    return {t for t in re.findall(r"[a-z0-9]+", fold(s)) if len(t) > 2}


def crossref_grants() -> list[dict]:
    out, cursor = [], "*"
    while True:
        r = requests.get("https://api.crossref.org/works",
                         params={"filter": f"type:grant,award.funder:{FUNDER_DOI}", "rows": 1000,
                                 "cursor": cursor, "mailto": "team@ourresearch.org"},
                         headers=HEADERS, timeout=120)
        r.raise_for_status()
        msg = r.json()["message"]
        for item in msg["items"]:
            for proj in item.get("project", []):
                title = " ".join(t.get("title", "") for t in proj.get("project-title", []))
                for li in proj.get("lead-investigator", []):
                    out.append({"award": (item.get("award") or "").strip(), "title": title,
                                "given": li.get("given"), "family": li.get("family")})
        if not msg["items"]:
            return out
        cursor = msg["next-cursor"]


def main() -> None:
    p = argparse.ArgumentParser(description="Arthritis Foundation award announcements -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="parse only the first N pages (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    pages = PAGES[: args.limit] if args.limit else PAGES
    rows = []
    for path, layout, programme, code, year, cofunder in pages:
        url = BASE + path
        cache = args.cache_dir / (path.rstrip("/").rsplit("/", 1)[-1] + ".html") if args.cache_dir else None
        if cache and cache.exists() and cache.stat().st_size > 0:
            page = cache.read_text()
        else:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code != 200:
                raise SystemExit(f"{url}: HTTP {r.status_code}")
            r.encoding = "utf-8"
            page = r.text
            if cache:
                args.cache_dir.mkdir(parents=True, exist_ok=True)
                cache.write_text(page)
            time.sleep(REQUEST_DELAY)
        grants = PARSERS[layout](page_lines(page))
        log(f"  {len(grants):3d} grants  {layout:8s} {path}")
        if not grants:
            raise SystemExit(f"no grants parsed from {url}; layout changed?")
        for g in grants:
            lead_given, lead_family = split_name(g["people"][0])
            co = g["people"][1] if len(g["people"]) > 1 else None
            co_given, co_family = split_name(co) if co else (None, None)
            rows.append({
                "programme": programme, "programme_code": code, "year": str(year),
                "scheme": g["scheme"] or programme, "title": g["title"], "description": g["description"],
                "amount": g["amount"], "currency": "USD" if g["amount"] else None,
                "lead_name": g["people"][0], "lead_given_name": lead_given, "lead_family_name": lead_family,
                "co_lead_name": co, "co_lead_given_name": co_given, "co_lead_family_name": co_family,
                "co_lead_institution": (g.get("co_institution") or g["institution"]) if co else None,
                "investigators": json.dumps(g["people"], ensure_ascii=False),
                "institution": g["institution"], "co_funder": cofunder, "note": g["note"],
                "landing_page_url": url,
            })
    df = pd.DataFrame(rows)

    def key(r):
        fam = re.sub(r"[^a-z]", "", fold(r["lead_family_name"]))
        ini = re.sub(r"[^a-z]", "", fold(r["lead_given_name"]))[:1]
        return f"AF-{r['year']}-{r['programme_code']}-{fam.upper()}-{ini.upper()}"
    df["funder_award_id"] = df.apply(key, axis=1)
    dup = df.groupby("funder_award_id").cumcount()
    df.loc[dup > 0, "funder_award_id"] = df["funder_award_id"] + "-" + (dup + 1).astype(str)

    xref = crossref_grants()
    log(f"Crossref: {len(xref)} AF grant deposits")
    matched = []
    for _, r in df.iterrows():
        hit = None
        for g in xref:
            same_pi = (re.sub(r"[^a-z]", "", fold(g["family"])) == re.sub(r"[^a-z]", "", fold(r["lead_family_name"]))
                       and fold(g["given"])[:1] == fold(r["lead_given_name"])[:1])
            a, b = tokens(g["title"]), tokens(r["title"])
            if same_pi and a and b and len(a & b) / len(a | b) >= 0.5:
                hit = g["award"]
                break
        matched.append(hit)
    df["crossref_award"] = matched
    df["exclusion_reason"] = df["crossref_award"].map(lambda a: "covered_by_crossref_grant" if a else None)
    df.loc[df["title"].isna() & df["exclusion_reason"].isna(), "exclusion_reason"] = "no_title_parsed"
    df["excluded"] = df["exclusion_reason"].notna().map(lambda b: "1" if b else "0")
    if df["funder_award_id"].str.lower().duplicated().any():
        raise SystemExit("duplicate funder_award_id")
    kept = df[df["excluded"] == "0"]
    log(f"Parsed {len(df)} grants; excluded {len(df) - len(kept)} "
        f"({df['exclusion_reason'].value_counts().to_dict()}); kept {len(kept)}")
    for c in ["title", "amount", "lead_family_name", "institution", "description"]:
        log(f"  {c:18s} {kept[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "arthritis_foundation_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_arthritis_foundation_projects.parquet"
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
