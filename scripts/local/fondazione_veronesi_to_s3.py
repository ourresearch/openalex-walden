#!/usr/bin/env python3
"""
Fondazione Umberto Veronesi (FUV) to S3 Data Pipeline
=====================================================

Fondazione Umberto Veronesi (Milan) funds cancer and biomedical research,
mainly through annual research fellowships (borse di ricerca: post-doctoral
fellowships, "Pink is Good" fellowships, etc.) awarded per call (bando). The
foundation publishes every fellowship holder as a researcher profile at
https://www.fondazioneveronesi.it/ricerca-sui-tumori/i-nostri-ricercatori/<slug>
("I nostri ricercatori" = "Borse di ricerca" in the site menu), 2016 onwards.

1. Enumerate profiles from the site's WordPress REST API
   (back.fondazioneveronesi.it/wp-json/wp/v2/researchers; the backend's
   robots.txt explicitly allows /wp-json/*). It gives post id, slug, name
   ("Family Given") and the researcher_year terms, but no project fields.
2. Read each public profile page (www.fondazioneveronesi.it, robots allows
   all but /cerca). The page is server-rendered by Nuxt and embeds the
   profile's ACF data in <script id="__NUXT_DATA__"> (devalue-encoded):
   biography and projects_list[] = {title, description, years[], duration,
   where, areas}. One output row per (researcher x project).

Award id: the site publishes no fellowship number (citing works quote mixed
forms such as "FUV 2017-1072" / "2018-1914" that are not on the site), so
funder_award_id is the synthetic key "FUV-{post id}-{first funded year}",
with a "-{n}" suffix only if one researcher has two projects starting the
same year.

Names: the title is "Family Given" while the slug is "given-family", so the
family/given boundary is the rotation that maps one onto the other. When the
slug is not a rotation of the title (same order, typos, extra tokens) the
first title token is the family name, extended over Italian particles
(De, Di, Del, Della, Lo, La, ...), unless the first token is a given name
seen in the slug-confirmed splits and the last is not (then "Given Family").

No amounts are published per fellowship (calls state programme amounts only).

Output: s3://openalex-ingest/awards/fondazione_veronesi/fondazione_veronesi_projects.parquet
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

API = "https://back.fondazioneveronesi.it/wp-json/wp/v2"
SITE = "https://www.fondazioneveronesi.it"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/fondazione_veronesi/fondazione_veronesi_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5
RETRIES = 4
MAX_CONSECUTIVE_NON200 = 5

QA_TITLE_RE = re.compile(r"^\s*(progetto\s*$|se ti dico ricerca|secondo te perch|perch[eèé] hai scelto)", re.I)
QA_START_RE = re.compile(r"(se ti dico ricerca|secondo te perch|perch[eèé] hai scelto)", re.I)

PARTICLES = {"de", "di", "del", "della", "dello", "dei", "degli", "delle", "da", "dal", "dalla",
             "dalle", "lo", "la", "li", "le", "van", "von", "der", "den", "dos", "das", "du", "mc", "san", "st."}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, params: dict | None = None) -> requests.Response:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=90)
            if r.status_code in (200, 404):
                return r
            last_err = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} {params} failed: {last_err}")


def text(s) -> str | None:
    if s is None or s is False:
        return None
    t = re.sub(r"<[^>]+>", " ", str(s))
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def unflatten(arr: list):
    """Decode Nuxt 3's devalue payload (__NUXT_DATA__) into plain JSON."""
    cache: dict[int, object] = {}
    wrappers = {"ShallowReactive", "Reactive", "Ref", "ShallowRef", "EmptyRef", "EmptyShallowRef"}

    def h(i):
        if not isinstance(i, int) or i < 0:
            return None
        if i in cache:
            return cache[i]
        v = arr[i]
        if isinstance(v, list):
            if v and isinstance(v[0], str) and v[0] in wrappers:
                r = h(v[1]) if len(v) > 1 else None
            elif v and isinstance(v[0], str) and v[0] == "Set":
                r = [h(x) for x in v[1:]]
            elif v and isinstance(v[0], str) and v[0] == "Date":
                r = v[1]
            else:
                r = [h(x) for x in v]
        elif isinstance(v, dict):
            r = {k: h(x) for k, x in v.items()}
        else:
            r = v
        cache[i] = r
        return r

    return h(0)


def list_researchers() -> list[dict]:
    out, page, total_pages, non200 = [], 1, None, 0
    while total_pages is None or page <= total_pages:
        r = get(f"{API}/researchers", {"per_page": 100, "page": page,
                                       "_fields": "id,slug,link,title,researcher_year,date,modified"})
        log(f"researchers page {page}/{total_pages}: HTTP {r.status_code}")
        if r.status_code != 200:
            non200 += 1
            if non200 >= MAX_CONSECUTIVE_NON200:
                raise RuntimeError("too many non-200 responses listing researchers")
            page += 1
            continue
        non200 = 0
        total_pages = int(r.headers.get("X-WP-TotalPages", "1"))
        out += r.json()
        page += 1
    return out


def norm(tok: str) -> str:
    t = unicodedata.normalize("NFKD", tok.lower())
    return re.sub(r"[^a-z0-9]", "", "".join(c for c in t if not unicodedata.combining(c)))


def split_title_name(title: str, slug: str) -> tuple[str | None, str | None, str]:
    """Title is 'Family Given'; slug is 'given-family'. Returns (given, family, method)."""
    toks = title.split()
    if len(toks) < 2:
        return None, (toks[0] if toks else None), "single"
    nt = [norm(t) for t in toks]
    ns = [s for s in re.sub(r"-\d+$", "", slug).split("-") if s]
    # tokens like "D'Amico" become one slug token "damico"; compare joined strings per split
    for k in range(1, len(toks)):
        fam, giv = "".join(nt[:k]), "".join(nt[k:])
        if "".join(ns) == giv + fam and giv != fam:
            return " ".join(toks[k:]), " ".join(toks[:k]), "slug_rotation"
    k = 1
    while k < len(toks) - 1 and norm(toks[k - 1]) in PARTICLES:
        k += 1
    return " ".join(toks[k:]), " ".join(toks[:k]), "first_token"


def fetch_profile(link: str, cache: Path | None) -> dict | None:
    if cache and cache.exists():
        return json.loads(cache.read_text())
    url = link if link.startswith("http") else SITE + link
    r = get(url)
    time.sleep(REQUEST_DELAY)
    if r.status_code != 200:
        return None
    r.encoding = "utf-8"
    m = re.search(r'<script[^>]*id="__NUXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
    if not m:
        return None
    data = unflatten(json.loads(m.group(1)))
    page = next((v for v in (data.get("data") or {}).values() if isinstance(v, dict) and v.get("type") == "researcher"), None)
    if page is None:
        return None
    rec = {"id": page.get("id"), "acf": page.get("acf")}
    if cache:
        cache.write_text(json.dumps(rec, ensure_ascii=False))
    return rec


def main() -> None:
    ap = argparse.ArgumentParser(description="Fondazione Umberto Veronesi researchers -> parquet -> S3")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    ap.add_argument("--cache-dir", type=Path, default=None, help="cache decoded profile JSON here")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = ap.parse_args()
    if args.cache_dir:
        args.cache_dir.mkdir(parents=True, exist_ok=True)

    researchers = list_researchers()
    log(f"{len(researchers)} researcher profiles listed")
    years_tax = {}
    r = get(f"{API}/researcher_year", {"per_page": 100})
    for t in r.json():
        years_tax[t["id"]] = t["name"]
    if args.limit:
        researchers = researchers[: args.limit]

    rows, missing, no_projects = [], [], []
    t0 = time.time()
    for i, res in enumerate(researchers, 1):
        cache = args.cache_dir / f"{res['id']}.json" if args.cache_dir else None
        prof = fetch_profile(res["link"], cache)
        name = text(res["title"]["rendered"]) or ""
        given, family, method = split_title_name(name, res["slug"])
        if prof is None or not isinstance(prof.get("acf"), dict):
            missing.append(res["link"])
            continue
        projects = prof["acf"].get("projects_list") or []
        if not projects:
            no_projects.append(res["link"])
        for j, p in enumerate(projects):
            yrs = sorted({int(y["name"]) for y in (p.get("years") or []) if isinstance(y, dict)
                          and str(y.get("name", "")).isdigit()})
            if not yrs:  # fall back to the profile's year terms
                yrs = sorted({int(years_tax[y]) for y in res.get("researcher_year") or [] if y in years_tax})
            areas = p.get("areas")
            area_names = [a.get("name") for a in areas if isinstance(a, dict)] if isinstance(areas, list) else []
            rows.append({
                "researcher_post_id": str(res["id"]),
                "project_index": str(j),
                "slug": res["slug"],
                "researcher_name": name,
                "lead_given_name": given,
                "lead_family_name": family,
                "name_split_method": method,
                "biography": text(prof["acf"].get("biography")),
                "title": text(p.get("title")),
                "title_raw": None,
                "description": text(p.get("description")),
                "years": json.dumps(yrs),
                "first_year": str(yrs[0]) if yrs else None,
                "last_year": str(yrs[-1]) if yrs else None,
                "duration": text(p.get("duration")),
                "where": text(p.get("where")),
                "areas": "; ".join(a for a in area_names if a) or None,
                "landing_page_url": res["link"],
                "profile_modified": res.get("modified"),
            })
        if i % 50 == 0:
            el = time.time() - t0
            log(f"  {i}/{len(researchers)} profiles, {len(rows)} projects; ETA {el / i * (len(researchers) - i) / 60:.1f} min")

    df = pd.DataFrame(rows)
    # A few titles are "Given Family" (slug in the same order, so no rotation).
    # Learn given names from the slug-confirmed splits and flip those rows when
    # the first title token is a known given name and the last one is not.
    ok = df["name_split_method"] == "slug_rotation"
    given_lex = {norm(t) for g in df.loc[ok, "lead_given_name"].dropna() for t in g.split()[:1]}
    for i in df.index[~ok]:
        toks = (df.at[i, "researcher_name"] or "").split()
        if len(toks) >= 2 and norm(toks[0]) in given_lex and norm(toks[-1]) not in given_lex:
            df.at[i, "lead_given_name"] = " ".join(toks[:-1])
            df.at[i, "lead_family_name"] = toks[-1]
            df.at[i, "name_split_method"] = "given_first_lexicon"
    # 2016-2017 profiles were migrated with an interview block in the project
    # fields: the "title" is a question ("Se ti dico ricerca, cosa ti viene in
    # mente?") or the word "Progetto", "where" holds the answer (or, for
    # "Progetto", the real project title) and the description starts with the
    # project title followed by the question. Recover the title; drop the
    # interview text from where/description.
    qa = df["title"].fillna("").str.match(QA_TITLE_RE)
    for i in df.index[qa]:
        t, w, d = df.at[i, "title"], df.at[i, "where"], df.at[i, "description"] or ""
        marker = d.rfind("PROGETTO ")
        m = QA_START_RE.search(d)
        if marker >= 0:  # "...answer... PROGETTO <title>"
            real = d[marker + len("PROGETTO "):].strip(" .:")
        elif m and m.start() > 0:  # "<title> <question> <answer>"
            real = d[: m.start()].strip(" .:")
        elif t.strip().lower() == "progetto":
            real = w
        else:
            real = None
        df.at[i, "title_raw"] = t
        df.at[i, "title"] = real or None
        df.at[i, "where"] = None
        df.at[i, "description"] = None
    df["qa_repaired"] = qa.map(lambda x: "true" if x else None)
    log(f"repaired {int(qa.sum())} interview-style project entries "
        f"({int(df.loc[qa, 'title'].notna().sum())} with a recovered title)")
    # A few profiles list the same project-year twice; keep one (prefer the copy with a host)
    df["_has_where"] = df["where"].notna()
    df = df.sort_values(["researcher_post_id", "first_year", "_has_where"], ascending=[True, True, False])
    key = df["researcher_post_id"] + "|" + df["first_year"].fillna("") + "|" + df["title"].fillna("").str.lower().str.strip()
    before = len(df)
    df = df[~key.duplicated(keep="first")].drop(columns="_has_where").sort_index()
    log(f"dropped {before - len(df)} duplicate project-years within a profile")
    # funder_award_id: FUV-{post id}-{first year}, "-{n}" only for a same-year second project
    base = "FUV-" + df["researcher_post_id"] + "-" + df["first_year"].fillna("na")
    n = base.groupby(base).cumcount()
    df["funder_award_id"] = [b if k == 0 else f"{b}-{k + 1}" for b, k in zip(base, n)]
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    if missing:
        log(f"{len(missing)} profiles without readable data, e.g. {missing[:5]}")
        if not args.limit and len(missing) > 0.02 * len(researchers):
            raise SystemExit("more than 2% of profiles unreadable; refusing to write a partial corpus")
    log(f"{len(df)} projects from {df['researcher_post_id'].nunique()} researchers "
        f"({len(no_projects)} profiles list no project)")
    for c in ["title", "description", "first_year", "duration", "where", "lead_family_name", "areas"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log("  name split: " + "; ".join(f"{k}={v}" for k, v in df["name_split_method"].value_counts().items()))
    log("  first_year: " + "; ".join(f"{k}={v}" for k, v in df["first_year"].value_counts().sort_index().items()))

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "fondazione_veronesi_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_fondazione_veronesi_projects.parquet"
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
