#!/usr/bin/env python3
"""
Oesterreichische Nationalbank (OeNB) Jubilaeumsfonds to S3 Data Pipeline
========================================================================

The OeNB Anniversary Fund (Jubilaeumsfonds, since 1966) publishes its approved
research projects in the "Projektabfrage" on
https://www.oenb.at/daten-und-forschung/forschungsfoerderung/jubilaeumsfonds/projektabfrage.html
The page is a thin client over a public JSON API (the same calls the page makes):

  POST /fomis/suche?page=0&size=N   {"keywords":"","volltext":"","projektNr":"","projektleiter":""}
       -> Spring page of all projects (id, projektNr, title, PI, institute, cluster, keywords)
  GET  /fomis/detail/{id}
       -> eingereichtDate / bewilligtDate (submitted / approved, [y, m, d]),
          PI name + academic title, institute, department, city, website,
          inhalt (project description), bericht (short report), publikationen,
          wissenschaftsgebiet (discipline or, from ~2024, thematic cluster).

The database covers the projects approved "in recent years" (project numbers
14744-19171, approvals since ~2012); the fund's earlier ~14,000 projects back
to 1966 are not online. OeNB publishes NO per-project amounts (neither in the
database nor in the "Vergabesitzung" PDFs), so amount stays NULL.

funder_award_id = the Jubilaeumsfonds project number as a bare integer string
("18740"), the dominant form in citing works' acknowledgements
("OeNB Anniversary Fund, project number 18740").

E-mail addresses, street address and the case-officer contact are NOT copied.

Output: s3://openalex-ingest/awards/oenb_jubilaeumsfonds/oenb_jubilaeumsfonds_projects.parquet
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

BASE = "https://www.oenb.at"
SEARCH_URL = f"{BASE}/fomis/suche"
DETAIL_URL = f"{BASE}/fomis/detail/"
LANDING = f"{BASE}/daten-und-forschung/forschungsfoerderung/jubilaeumsfonds/projektabfrage.html"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/oenb_jubilaeumsfonds/oenb_jubilaeumsfonds_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5
RETRIES = 5

# Thematic clusters (2024+ calls), names as published on the Jubilaeumsfonds page.
CLUSTERS = {
    "CLUSTER_1": "Funktionen von Zentralbanken",
    "CLUSTER_2": "Preisstabilität und Geldpolitik",
    "CLUSTER_3": "Geldwesen und Zahlungsverkehr",
    "CLUSTER_4": "Finanzmarkt und Finanztechnologien",
    "CLUSTER_5": "Finanzmarktstabilität",
    "CLUSTER_6": "Öffentliche Finanzen und Haushalte",
    "CLUSTER_7": "Investitions-, Wachstums- und Digitalisierungsstrategien",
    "CLUSTER_8": "Wettbewerbsfähigkeit, Standort- und Wirtschaftspolitik",
    "CLUSTER_9": "Europäische Wirtschafts- und Währungsintegration",
    "CLUSTER_10": "Makroökonomische Konjunktur- und Länderanalysen",
    "CLUSTER_11": "Internationale Handels- und Wirtschaftsbeziehungen",
    "CLUSTER_12": "Arbeitsmärkte und Arbeitsmarktpolitik",
    "CLUSTER_13": "Vermögensmärkte",
    "CLUSTER_14": "Fragen des nachhaltigen Wirtschaftens",
    "CLUSTER_15": "Regulatorische Rahmenbedingungen ökonomischer Systeme",
    "CLUSTER_16": "Finanzbildung und Wirtschaftskompetenz",
    "CLUSTER_17": "Wirtschaftsgeschichte",
    "CLUSTER_18": "Methodische Grundlagen der Wirtschaftsforschung",
    "CLUSTER_19": "Ausgewählte Themenstellungen",
    "MEDIZINISCHE_WISSENSCHAFTEN": "Medizinische Wissenschaften",
    "WIRTSCHAFTSWISSENSCHAFTEN": "Wirtschaftswissenschaften",
    "WIRTSCHAFTSWISSENSCHAFTEN_SCHWERPUNKT": "Wirtschaftswissenschaften (Schwerpunkt)",
    "GEISTESWISSENSCHAFTEN": "Geisteswissenschaften",
    "SOZIALWISSENSCHAFTEN": "Sozialwissenschaften",
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def call(method: str, url: str, **kw) -> requests.Response:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.request(method, url, headers=HEADERS, timeout=120, **kw)
            if r.status_code == 200:
                return r
            last_err = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last_err = e
        log(f"  {method} {url} -> {last_err}; retry {attempt + 1}/{RETRIES}")
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{method} {url} failed: {last_err}")  # fail closed (section 1.4)


def text(s: str | None) -> str | None:
    if s is None:
        return None
    t = re.sub(r"<br\s*/?>|</p>", "\n", str(s))
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("\r", "").replace("​", "").replace("\xa0", " ")
    t = "\n".join(re.sub(r"[ \t]+", " ", ln).strip() for ln in t.split("\n"))
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    return t or None


def ymd(v) -> str | None:
    if isinstance(v, list) and len(v) >= 3:
        return f"{int(v[0]):04d}-{int(v[1]):02d}-{int(v[2]):02d}"
    if isinstance(v, str) and re.match(r"\d{4}-\d{2}-\d{2}", v):
        return v[:10]
    return None


def main() -> None:
    p = argparse.ArgumentParser(description="OeNB Jubilaeumsfonds Projektabfrage -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache detail JSON here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the section 1.4 shrink guard")
    args = p.parse_args()

    body = {"keywords": "", "volltext": "", "projektNr": "", "projektleiter": ""}
    first = call("POST", SEARCH_URL, params={"page": 0, "size": 5}, json=body).json()
    total = first["totalElements"]
    res = call("POST", SEARCH_URL, params={"page": 0, "size": total + 50}, json=body).json()
    hits = res["content"]
    log(f"Search: totalElements {total}, got {len(hits)}")
    if len(hits) < total:
        raise SystemExit(f"search returned {len(hits)} < totalElements {total}")
    if args.limit:
        hits = hits[: args.limit]

    rows = []
    for i, h in enumerate(hits, 1):
        cache = args.cache_dir / f"{h['id']}.json" if args.cache_dir else None
        if cache and cache.exists():
            d = json.loads(cache.read_text())
        else:
            d = call("GET", DETAIL_URL + str(h["id"])).json()
            if cache:
                args.cache_dir.mkdir(parents=True, exist_ok=True)
                cache.write_text(json.dumps(d, ensure_ascii=False))
            time.sleep(REQUEST_DELAY)
        area = d.get("wissenschaftsgebiet") or h.get("wissenschaftsgebiet")
        rows.append({
            "fomis_id": str(d.get("id") or h["id"]),
            "project_number": str(d.get("projektNr") or h["projektNr"]),
            "title": text(d.get("titel") or h.get("titel")),
            "submitted_date": ymd(d.get("eingereichtDate")),
            "approved_date": ymd(d.get("bewilligtDate")),
            "pi_salutation": d.get("projektleiterAnrede"),
            "pi_title": text(d.get("projektleiterTitel")),
            "pi_given_name": text(d.get("projektleiterVorname")),
            "pi_family_name": text(d.get("projektleiterNachname")),
            "institute": text(d.get("institut") or h.get("institut")),
            "department": text(d.get("abteilung")),
            "city": text(d.get("ort")),
            "postcode": text(d.get("plz")),
            "website": text(d.get("website")),
            "description": text(d.get("inhalt")),
            "short_report": text(d.get("bericht")),
            "publications": text(d.get("publikationen") or h.get("publikationen")),
            "discipline_code": area,
            "discipline": CLUSTERS.get(area or "", area),
            "keywords": text(d.get("keywords") or h.get("keywords")),
            "landing_page_url": LANDING,
        })
        if i % 50 == 0:
            log(f"  {i}/{len(hits)} details")

    df = pd.DataFrame(rows)
    df["funder_award_id"] = df["project_number"].str.strip()
    dupes = df["funder_award_id"].duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate project numbers: {df.loc[dupes, 'funder_award_id'].tolist()}")
    unknown = sorted(set(df["discipline_code"].dropna()) - set(CLUSTERS))
    if unknown:
        log(f"  unmapped discipline codes (kept raw): {unknown}")
    for c in ["title", "approved_date", "submitted_date", "pi_family_name", "institute",
              "description", "short_report", "keywords"]:
        log(f"  {c:16s} {df[c].notna().mean():6.1%}")
    log(f"  approval years: {df['approved_date'].str[:4].value_counts().sort_index().to_dict()}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "oenb_jubilaeumsfonds_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook section 1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_oenb_jubilaeumsfonds_projects.parquet"
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
