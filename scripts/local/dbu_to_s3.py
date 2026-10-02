#!/usr/bin/env python3
"""
Deutsche Bundesstiftung Umwelt (DBU) to S3 Data Pipeline
=========================================================

DBU runs its site on WordPress and exposes its three funding databases as
public WP REST custom post types (ladder item 2):

  /wp-json/wp/v2/projektdatenbank      ~11,100 funded projects since 1991
      meta_box: dbu_projektdatenbank_az_ges (Aktenzeichen "34025/01"),
      _bsumme (EUR, German format "347.638,00"), _firma (grantee org, multi-line),
      _ort/_plz/_bundesland, _p_von/_p_bis (project period), _foerderber
      (internal funding-area code), _ist_nachbewilligung_von / _hat_nachbewilligung;
      tags = Bundesland + themes.
  /wp-json/wp/v2/promotionsstipendium  ~1,750 PhD scholarships (AZ "20013/247")
      meta_box: dbu_stipendiaten_az, _vorname/_nachname/_titel, _fbeginn/_fende,
      _e_anschrif (host institution), _betreuer (supervisor); content = abstract.
  /wp-json/wp/v2/moe-fellowship        ~1,350 MOE fellowships (AZ "30026/036")
      same stipendiaten fields (fellows from Central/Eastern Europe placed
      at German environmental institutions).

Personal e-mail / phone / street fields are NOT copied into the parquet.

funder_award_id = the DBU Aktenzeichen exactly as DBU prints it ("34025/01",
"20013/247"), the form grantees cite ("AZ 34025/01").

Output: s3://openalex-ingest/awards/dbu/dbu_projects.parquet (one row per
project / scholarship / fellowship; `record_type` says which).
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
# See runbook section 1.2. (grep marker: sys.stdout.reconfigure)
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

BASE = "https://www.dbu.de/wp-json/wp/v2"
TYPES = ["projektdatenbank", "promotionsstipendium", "moe-fellowship"]
FIELDS = "id,link,slug,title,content,tags,meta_box,date,modified"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/dbu/dbu_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}

# DBU theme tags (the other tags are Bundeslaender)
THEMES = {"Umwelttechnik", "Ressourcenschonung", "Klimaschutz", "Umweltforschung", "Umweltkommunikation",
          "Landnutzung", "Naturschutz", "Kulturgüter", "Internationale Aktivitäten", "Umweltpreis",
          "Stipendienprogramm"}
RESEARCH_ORG_RE = (r"(?i)universit|hochschule|\bTU\b|fachhochschule|institut|forschung|fraunhofer|helmholtz"
                   r"|leibniz|max[- ]planck|akademie der wissenschaften|senckenberg|\bUFZ\b")
RESEARCH_TITLE_RE = (r"(?i)forschung|untersuch|studie|analyse|evaluation|evaluierung|wissenschaft|erhebung"
                     r"|monitoring|erprobung|bewertung|konzept|methode|verfahren|technolog|entwicklung")
PER_PAGE = 100
REQUEST_DELAY = 0.5
RETRIES = 6


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, params: dict) -> requests.Response:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=180)
            if r.status_code == 200:
                return r
            last_err = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last_err = e
        log(f"  GET {url} {params.get('page')} -> {last_err}; retry {attempt + 1}/{RETRIES}")
        time.sleep(5 * (attempt + 1))
    # fail closed: never write a partial corpus (runbook section 1.4)
    raise RuntimeError(f"GET {url} page {params.get('page')} failed: {last_err}")


def fetch_type(ptype: str, cache_dir: Path | None, limit: int | None) -> list[dict]:
    url = f"{BASE}/{ptype}"
    first = get(url, {"per_page": PER_PAGE, "page": 1, "_fields": FIELDS, "orderby": "id", "order": "asc"})
    total, pages = int(first.headers["X-WP-Total"]), int(first.headers["X-WP-TotalPages"])
    log(f"{ptype}: X-WP-Total {total}, {pages} pages")
    out = list(first.json())
    for page in range(2, pages + 1):
        if limit and len(out) >= limit:
            break
        cache = cache_dir / f"{ptype}_p{page}.json" if cache_dir else None
        if cache and cache.exists():
            items = json.loads(cache.read_text())
        else:
            items = get(url, {"per_page": PER_PAGE, "page": page, "_fields": FIELDS,
                              "orderby": "id", "order": "asc"}).json()
            if cache:
                cache_dir.mkdir(parents=True, exist_ok=True)
                cache.write_text(json.dumps(items, ensure_ascii=False))
            time.sleep(REQUEST_DELAY)
        out += items
        if page % 10 == 0:
            log(f"  {ptype}: page {page}/{pages}, {len(out)} records")
    if limit:
        return out[:limit]
    ids = {x["id"] for x in out}
    # X-WP-Total is the terminator; a shortfall means pages shifted mid-crawl
    if len(ids) < total:
        raise RuntimeError(f"{ptype}: got {len(ids)} distinct records, X-WP-Total says {total}")
    return out


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<br\s*/?>|</p>", "\n", fragment)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("​", "").replace("﻿", "").replace("\xa0", " ")
    t = "\n".join(re.sub(r"[ \t]+", " ", ln).strip() for ln in t.split("\n"))
    t = re.sub(r"\n{2,}", "\n", t).strip()
    return t or None


def parse_eur(s: str | None) -> float | None:
    """German format '347.638,00' -> 347638.0"""
    if not s or not str(s).strip():
        return None
    s = str(s).strip().replace("€", "").replace(" ", "")
    if re.fullmatch(r"\d{1,3}(\.\d{3})*(,\d+)?", s) or re.fullmatch(r"\d+(,\d+)?", s):
        return float(s.replace(".", "").replace(",", "."))
    return None


def ymd(s: str | None) -> str | None:
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s or "")
    return m.group(1) if m and not m.group(1).startswith("0000") else None


HONORIFIC_RE = re.compile(
    r"^(?:(?:prof|professor|priv\.?-?\s*doz|apl\.?\s*prof|jun\.?-?\s*prof|dr|dipl\.?-?\s*[a-z]+|mag|ing|herr|frau)\.?"
    r"(?:\s*(?:rer|med|phil|nat|agr|ing|habil|forest|oec|pol|soc|vet)\.?)*(?:\s+|$))+",
    re.I,
)


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook section 2.4.1 helper (wolf_to_s3.py) plus a leading
    German academic-title strip ("Prof. Dr. rer. nat.")."""
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


def project_row(x: dict, tagnames: dict) -> dict:
    m = x.get("meta_box") or {}
    firma = text((m.get("dbu_projektdatenbank_firma") or "").replace("\r", ""))
    org_lines = [ln for ln in (firma or "").split("\n") if ln.strip()]
    return {
        "record_type": "project",
        "wp_id": str(x["id"]),
        "az": (m.get("dbu_projektdatenbank_az_ges") or "").strip() or None,
        "title": text(x["title"]["rendered"]),
        "description": text((x.get("content") or {}).get("rendered")),
        "amount_text": m.get("dbu_projektdatenbank_bsumme") or None,
        "amount": parse_eur(m.get("dbu_projektdatenbank_bsumme")),
        "org_name": org_lines[0] if org_lines else None,
        "org_unit": "; ".join(org_lines[1:]) or None,
        "org_full": firma,
        "city": (m.get("dbu_projektdatenbank_ort_str") or "").strip() or None,
        "postcode": (m.get("dbu_projektdatenbank_plz_str") or "").strip() or None,
        "bundesland": (m.get("dbu_projektdatenbank_bundesland") or "").strip() or None,
        "start_date": ymd(m.get("dbu_projektdatenbank_p_von")),
        "end_date": ymd(m.get("dbu_projektdatenbank_p_bis")),
        "duration_text": m.get("dbu_projektdatenbank_laufzeit") or None,
        "foerderbereich_code": (m.get("dbu_projektdatenbank_foerderber") or "").strip() or None,
        "website": (m.get("dbu_projektdatenbank_inet") or "").strip() or None,
        "final_report_url": (m.get("dbu_projektdatenbank_ab_bericht") or "").strip() or None,
        "follow_on_of": (m.get("dbu_projektdatenbank_ist_nachbewilligung_von") or "").strip() or None,
        "has_follow_on": (m.get("dbu_projektdatenbank_hat_nachbewilligung") or "").strip() or None,
        "tags": json.dumps([tagnames.get(t, str(t)) for t in x.get("tags") or []], ensure_ascii=False),
        "landing_page_url": x.get("link"),
        "person_title": None, "given_name": None, "family_name": None, "supervisor": None,
        "wp_modified": x.get("modified"),
    }


def fellow_row(x: dict, rtype: str, tagnames: dict) -> dict:
    m = x.get("meta_box") or {}
    inst = text((m.get("dbu_stipendiaten_e_anschrif") or "").replace("\r", ""))
    inst_lines = [ln for ln in (inst or "").split("\n") if ln.strip()]
    # academic titles live in dbu_stipendiaten_titel, but a few slip into the first name ("Dr. Robin")
    given = HONORIFIC_RE.sub("", (m.get("dbu_stipendiaten_vorname") or "").strip()).strip() or None
    family = (m.get("dbu_stipendiaten_nachname") or "").strip() or None
    if not family and given:
        given, family = split_name(given)
    return {
        "record_type": rtype,
        "wp_id": str(x["id"]),
        "az": (m.get("dbu_stipendiaten_az") or "").strip() or None,
        "title": text(x["title"]["rendered"]),
        "description": text((x.get("content") or {}).get("rendered")),
        "amount_text": None, "amount": None,
        "org_name": inst_lines[0] if inst_lines else None,
        "org_unit": "; ".join(inst_lines[1:]) or None,
        "org_full": inst,
        "city": None, "postcode": None, "bundesland": None,
        "start_date": ymd(m.get("dbu_stipendiaten_fbeginn")),
        "end_date": ymd(m.get("dbu_stipendiaten_fende")),
        "duration_text": None, "foerderbereich_code": None, "website": None,
        "final_report_url": None, "follow_on_of": None, "has_follow_on": None,
        "tags": json.dumps([tagnames.get(t, str(t)) for t in x.get("tags") or []], ensure_ascii=False),
        "landing_page_url": x.get("link"),
        "person_title": (m.get("dbu_stipendiaten_titel") or "").strip() or None,
        "given_name": given, "family_name": family,
        "supervisor": text(m.get("dbu_stipendiaten_betreuer")),
        "wp_modified": x.get("modified"),
    }


def fetch_tags() -> dict:
    out, page = {}, 1
    while True:
        r = get(f"{BASE}/tags", {"per_page": 100, "page": page})
        for t in r.json():
            out[t["id"]] = html.unescape(t["name"])
        if page >= int(r.headers.get("X-WP-TotalPages", 1)):
            return out
        page += 1


def main() -> None:
    p = argparse.ArgumentParser(description="DBU project + scholarship databases -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="records per type (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache REST pages here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the section 1.4 shrink guard")
    args = p.parse_args()

    tagnames = fetch_tags()
    log(f"tags: {len(tagnames)}")
    rows = []
    for ptype in TYPES:
        items = fetch_type(ptype, args.cache_dir, args.limit)
        if ptype == "projektdatenbank":
            rows += [project_row(x, tagnames) for x in items]
        else:
            rows += [fellow_row(x, "phd_scholarship" if ptype == "promotionsstipendium" else "moe_fellowship", tagnames)
                     for x in items]
    df = pd.DataFrame(rows)
    log(f"Records: {len(df)} ({df['record_type'].value_counts().to_dict()})")

    # Same record exposed twice by WP (re-saved posts) -> keep the newest
    df = df.sort_values("wp_modified").drop_duplicates(subset=["wp_id"], keep="last")
    no_az = df["az"].isna()
    log(f"Records without Aktenzeichen: {no_az.sum()} (dropped)")
    df = df[~no_az].copy()
    df["funder_award_id"] = df["az"].str.replace(r"\s+", "", regex=True)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        d = df[dupes].sort_values("funder_award_id")
        for r in d.itertuples():
            log(f"  duplicate AZ {r.funder_award_id}: wp {r.wp_id} {r.record_type} {str(r.title)[:60]!r} modified {r.wp_modified}")
        # Same AZ published in two post types (2026-08: 20025/057 and /058 are in
        # both the PhD and the MOE database). Keep the post whose type matches
        # the AZ series (200xx/nnn = PhD scholarship, 300xx/nnn = MOE fellowship),
        # then the most recently modified one.
        series_ok = (((df["record_type"] == "phd_scholarship") & df["funder_award_id"].str.match(r"^200\d\d/"))
                     | ((df["record_type"] == "moe_fellowship") & df["funder_award_id"].str.match(r"^300\d\d/")))
        df = (df.assign(_ok=series_ok.astype(int)).sort_values(["_ok", "wp_modified"])
                .drop_duplicates(subset=["funder_award_id"], keep="last").drop(columns="_ok"))
        log(f"  resolved {dupes.sum()} duplicated rows")
    if df["funder_award_id"].str.lower().duplicated().any():
        raise SystemExit("duplicate funder_award_id after dedup")

    # --- scope (Kyle 2026-10-01: keep every research pathway incl. scholarships,
    # fellowships and start-up grants; drop only what is clearly not research) ---
    themes = df["tags"].map(lambda t: [x for x in json.loads(t) if x in THEMES])
    df["theme_tags"] = themes.map(lambda l: json.dumps(sorted(l), ensure_ascii=False))
    df["programme"] = "Projektförderung"
    df.loc[df["funder_award_id"].str.match(r"^3550\d/"), "programme"] = "Green Start-up"
    df.loc[df["record_type"] == "phd_scholarship", "programme"] = "Promotionsstipendium"
    df.loc[df["record_type"] == "moe_fellowship", "programme"] = "MOE-Fellowship"
    # Pure environmental education / communication: DBU's own theme tags say
    # ONLY "Umweltkommunikation" (no research, technology, conservation or
    # climate theme) AND neither the grantee nor the title signals research.
    # (School programmes, exhibitions, conferences, environmental centres,
    # TV/radio/theatre productions.) Rows stay in the parquet, flagged.
    comm_only = themes.map(lambda l: l == ["Umweltkommunikation"]) & (df["record_type"] == "project")
    research_org = df["org_full"].fillna("").str.contains(RESEARCH_ORG_RE)
    research_title = df["title"].fillna("").str.contains(RESEARCH_TITLE_RE)
    excl = comm_only & ~research_org & ~research_title
    df["scope"] = "in"
    df.loc[excl, "scope"] = "excluded_environmental_education"
    log(f"Scope: {excl.sum()} environmental-education-only projects flagged out "
        f"(EUR {df.loc[excl, 'amount'].sum():,.0f}); {(~excl).sum()} in scope "
        f"({df.loc[~excl, 'programme'].value_counts().to_dict()})")

    for c in ["title", "description", "amount", "org_name", "start_date", "end_date",
              "foerderbereich_code", "family_name"]:
        log(f"  {c:20s} {df[c].notna().mean():6.1%}")
    log(f"  total amount EUR {df['amount'].sum():,.0f}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "dbu_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook section 1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_dbu_projects.parquet"
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(df)}")
        if len(df) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound", "403"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
