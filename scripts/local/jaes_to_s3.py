#!/usr/bin/env python3
"""
Jane and Aatos Erkko Foundation (Jane ja Aatos Erkon säätiö, JAES) to S3
========================================================================

Two first-party sources, merged:

1. **jaes.fi "Donations granted" pages** (https://jaes.fi/en/donations-granted/
   donations<YEAR>/, 2004-2025): one WordPress page per year, one
   `<div class="wp-block-group avustus [category]">` block per grant with
   grantee (h3), institution, purpose/title ('... 4 years.'), amount (h3) and
   an optional 'Read more' abstract. The current year (2026, plus late 2025
   decisions) is rendered client-side from the site's own JSON endpoint
   /wp-json/custom/v1/grants, which carries the same fields plus category.
2. **research.fi** (Finland's national research information hub, public
   portal API researchfi-api-production.2.rahtiapp.fi/portalapi/funding),
   to which JAES itself reports its science grants since 2023
   (source_description 'jane_ja_aatos_erkon_saatio'): grant number
   (funderProjectNumber, 'A866'), leader with ORCID, amount. 136 of its 137
   JAES rows match a jaes.fi row on (year, amount); the match supplies the
   grant number, ORCID and a clean given/family split.

Scope filter (JAES is a mixed foundation: medicine, science and technology,
plus arts/culture and societal causes):
- 2023+ rows carry the foundation's own category: keep medicine,
  technology, other science; drop art and culture, societal activity.
- 2004-2022 rows have no category. Keep a row when the grantee is a named
  researcher (academic/medical title, or a person with an institution and a
  research purpose), or an institution with an explicitly research purpose
  (research, professorship, doctoral school, PET/imaging centre, ...).
  Drop everything else (arts, music, restorations, hospital equipment,
  school scholarships, galas). Rows kept by the institution rule are
  flagged `scope_rule = 'institution_research'` for review.
- Co-funded call totals ('Jane and Aatos Erkko Foundation and Technology
  Industries of Finland Centennial ...') are programme totals, not grants,
  and are dropped.

funder_award_id: the research.fi grant number (A-number) where matched;
otherwise (pre-2023, and recent rows not yet on research.fi) a synthetic
'JAES-<year>-<lead family>-<lead given>' key (ASCII slug, '-b' on a
same-year repeat). Citing works write JAES's 6-digit application numbers
(200063 style) that neither source publishes.

Output: s3://openalex-ingest/awards/jaes/jaes_projects.parquet
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
# (runbook 4.0 grep marker: sys.stdout.reconfigure)
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). Production
# runs on Linux/Databricks where UTF-8 is the default. See runbook 1.2.
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

SITE = "https://jaes.fi"
INDEX = f"{SITE}/en/donations-granted/donations2024/"
GRANTS_JSON = f"{SITE}/wp-json/custom/v1/grants"
RFI_API = "https://researchfi-api-production.2.rahtiapp.fi/portalapi/funding/_search"
RFI_FUNDER = "Jane ja Aatos Erkon säätiö"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/jaes/jaes_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 1.0

RESEARCH_CATEGORIES = {"medicine", "medicin", "technology", "other-science", "other science", "other-sciences"}

TITLE_RE = re.compile(
    r"^(?:(?:academy research fellow|academy professor|research professor|research director|associate professor|"
    r"assistant professor|adjunct professor|professor|professori|prof\.|docent|dosentti|ph\.?\s?d\.?|d\.\s?sc\.?(?:\s?\(tech\.?\))?|"
    r"dsc|dr\.?\s?tech\.?|dr\.?|dmsc|md|m\.?\s?sc\.?|ma|m\.a\.|fat|lt|ft|tkt|associate|assistant|senior researcher|"
    r"researcher|research fellow|\(tenure track\)|tenure track)[,\s]+)+", re.I)
ACADEMIC_RE = re.compile(
    r"\b(professor|professori|prof\.|docent|dosentti|ph\.?\s?d|d\.\s?sc|dsc|dr\.|dr\b|dmsc|md\b|academy research fellow|"
    r"research director|researcher|m\.?\s?sc|fat\b|ma\b)", re.I)
RESEARCH_PURPOSE_RE = re.compile(
    r"(research|professorship|doctoral|post-?doc|post-doctorate|laborator|institute of|centre for|center for|"
    r"PET|imaging|vaccine|disease|genetic|clinical|scien|study|studies)", re.I)
INSTITUTION_RE = re.compile(r"(universit|yliopisto|institute|instituutti|hospital|HUS\b|HYKS|academy|collegium|faculty|ETLA|centre|center)", re.I)
NON_RESEARCH_RE = re.compile(r"(fundrais|capitali[sz]ation|scholarships? for|stipends|gala|upper secondary school|emergency aid to arts)", re.I)
PROGRAMME_TOTAL_RE = re.compile(r"Jane and Aatos Erkko Foundation and Technology Industries", re.I)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, cache: Path | None) -> str:
    if cache and cache.exists():
        return cache.read_text()
    last = None
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            r.raise_for_status()
            r.encoding = "utf-8"
            time.sleep(REQUEST_DELAY)
            if cache:
                cache.write_text(r.text)
            return r.text
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def clean(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = html.unescape(re.sub(r"<br\s*/?>", "\n", fragment))
    t = re.sub(r"<[^>]+>", " ", t)
    t = "\n".join(re.sub(r"[ \t ]+", " ", x).strip() for x in t.split("\n"))
    t = re.sub(r"\n+", "\n", t).strip()
    return t or None


def parse_amount(s: str | None) -> tuple[float | None, str | None]:
    if not s:
        return None, None
    cur = "USD" if "USD" in s else ("EUR" if "€" in s or "EUR" in s else None)
    d = re.sub(r"[^\d]", "", s)
    return (float(d), cur) if d else (None, cur)


def split_purpose(text: str | None) -> tuple[str | None, int | None]:
    """'Future forests: ... 4 years.' -> ('Future forests: ...', 4)."""
    if not text:
        return None, None
    # '4 years.', '2–3 years.', '3 vuotta', and a cut-off '1–' at the very end
    m = re.search(r"[.,]?\s*(?:\d\s*[–-]\s*)?(\d)\s*(?:years?|vuotta|år)\.?\s*$|[.,]?\s*\d\s*[–-]\s*$", text, re.I)
    years = int(m.group(1)) if m and m.group(1) else None
    title = text[: m.start()] if m else text
    return title.strip(" .\n") or None, years


def parse_page(year: int, page: str) -> list[dict]:
    rows = []
    # stop before the page footer/menus so the last block does not swallow them
    end = min([i for i in (page.find("</article>"), page.find("<footer")) if i > 0] or [len(page)])
    for b in re.split(r'<div class="wp-block-group avustus', page[:end])[1:]:
        cls = b[: b.find('"')].strip()
        if "item." in cls or "'" in cls:  # the JS template on the current-year page
            continue
        h3 =[clean(x) for x in re.findall(r"<h3[^>]*>(.*?)</h3>", b, re.S)]
        ps = [clean(x) for x in re.findall(r"<p[^>]*>(.*?)</p>", b, re.S)]
        acc = re.search(r'getwid-accordion__content">(.*?)</div>', b, re.S)
        abstract = clean(acc.group(1)) if acc else None
        body_ps = [p for p in ps if p and p != "Read more" and (not abstract or p not in abstract)]
        if len(h3) < 2:
            continue
        amount, cur = parse_amount(h3[-1])
        # institution paragraphs are styled uppercase; the purpose is the last plain paragraph
        inst = re.findall(r'<p[^>]*text-transform:uppercase[^>]*>(.*?)</p>', b, re.S)
        inst = ((clean(inst[0]) or "").replace("\n", "; ") or None) if inst else None
        purposes = [p for p in body_ps if not inst or p.replace("\n", "; ") != inst]
        title, years = split_purpose(purposes[-1] if purposes else None)
        rows.append({"year": year, "category": cls or None, "grantee": h3[0], "institution": inst,
                     "title": title, "duration_years": years, "abstract": abstract,
                     "amount": amount, "currency": cur, "source": "jaes.fi"})
    return rows


def parse_json(items: list[dict]) -> list[dict]:
    rows = []
    for it in items:
        kesto = int(it.get("myonnettykesto") or 0)
        title, _ = split_purpose(it.get("hankenimi_eng") or it.get("hankenimi_fin"))
        rows.append({"year": int(it["vuosi"]), "category": (it.get("kasittelyryhma_eng") or "").lower() or None,
                     "grantee": clean(it.get("nimi_eng") or it.get("nimi_fin")),
                     "institution": clean(it.get("organisaatio_eng") or it.get("organisaatio_fin")),
                     "title": title, "duration_years": (kesto // 12) or None,
                     "abstract": clean(it.get("tiivistelma_eng") or it.get("tiivistelma_fin")),
                     "amount": float(int(re.sub(r"\D", "", str(it.get("myonnettysumma") or "0")) or 0)) or None,
                     "currency": "EUR", "source": "jaes.fi/wp-json"})
    return rows


def fetch_research_fi() -> list[dict]:
    body = {"size": 1000, "query": {"term": {"funderNameFi.keyword": RFI_FUNDER}}, "sort": [{"projectId": "asc"}]}
    r = requests.post(RFI_API, json=body, headers=HEADERS, timeout=60)
    r.raise_for_status()
    d = r.json()
    total = d["hits"]["total"]["value"]
    hits = [h["_source"] for h in d["hits"]["hits"]]
    if len(hits) != total:
        raise SystemExit(f"research.fi returned {len(hits)} of {total} JAES fundings; paginate")
    return hits


def people_from(grantee: str | None) -> list[dict]:
    """'Professor Sarah Butcher, professor Varpu Marjomäki, docent Minna
    Hankaniemi' -> three people (given-first; canonical split, runbook 2.4.1)."""
    out = []
    for seg in re.split(r",\s*|\s+and\s+|\s+&\s+|\s+ja\s+", grantee or ""):
        seg = TITLE_RE.sub("", seg.strip()).strip(" ,.")
        toks = seg.split()
        if len(toks) < 2 or not all(t[:1].isupper() or t.lower() in {"von", "van", "de", "der", "af"} for t in toks):
            continue
        suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
        while toks and toks[-1].lower().strip(",.") in suffixes:
            toks.pop()
        if len(toks) < 2:
            continue
        out.append({"name": " ".join(toks), "given_name": " ".join(toks[:-1]), "family_name": toks[-1], "orcid": None})
    return out


def scope(r: dict) -> str | None:
    """Return the rule that keeps the row as research, or None to drop it."""
    if PROGRAMME_TOTAL_RE.search(r["grantee"] or ""):
        return None
    if r["category"]:
        return "category" if r["category"].strip().lower() in RESEARCH_CATEGORIES else None
    text = " ".join(filter(None, [r["title"], r["institution"], r["abstract"]]))
    if NON_RESEARCH_RE.search(" ".join(filter(None, [r["grantee"], text]))):
        return None
    if ACADEMIC_RE.search(r["grantee"] or "") and people_from(r["grantee"]):
        return "named_researcher"
    if people_from(r["grantee"]) and r["institution"] and RESEARCH_PURPOSE_RE.search(text):
        return "named_researcher"
    if INSTITUTION_RE.search(r["grantee"] or "") and RESEARCH_PURPOSE_RE.search(text):
        return "institution_research"
    return None


def slug(s: str | None) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-") or "x"


def main() -> None:
    p = argparse.ArgumentParser(description="Jane and Aatos Erkko Foundation grants -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="parse only the N most recent year pages (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp/jaes"))
    p.add_argument("--cache-dir", type=Path, default=None)
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the 1.4 shrink guard")
    args = p.parse_args()
    if args.cache_dir:
        args.cache_dir.mkdir(parents=True, exist_ok=True)
    cache = (lambda name: args.cache_dir / name) if args.cache_dir else (lambda name: None)

    index = get(INDEX, cache("2024.html"))
    pages = {int(y): u for u, y in re.findall(r'href="(https://jaes\.fi/en/donations-granted/donations(\d{4})/)"', index)}
    pages[2024] = INDEX
    if "/2007-2/" in index:
        pages[2007] = f"{SITE}/en/donations-granted/2007-2/"
    years = sorted(pages, reverse=True)
    if args.limit:
        years = years[: args.limit]
    site = []
    for y in years:
        got = parse_page(y, get(pages[y], cache(f"{y}.html")))
        log(f"{y}: {len(got)} grant blocks")
        site += got
    js = parse_json(json.loads(get(GRANTS_JSON, cache("grants.json"))))
    have = {(r["year"], r["amount"]) for r in site}
    js_new = [r for r in js if (r["year"], r["amount"]) not in have]
    log(f"wp-json grants endpoint: {len(js)} rows, {len(js_new)} not already on a year page")
    site += js_new

    for r in site:
        r["scope_rule"] = scope(r)
    kept = [r for r in site if r["scope_rule"]]
    log(f"site rows: {len(site)}; kept as research: {len(kept)} "
        f"({sum(r['scope_rule'] == 'institution_research' for r in kept)} by the institution rule)")

    rfi = fetch_research_fi()
    by_key = {}
    for s in rfi:
        by_key.setdefault((int(s["fundingStartYear"]), float(s["amount_in_EUR"])), []).append(s)
    matched = 0
    for r in kept:
        cands = by_key.get((r["year"], r["amount"])) or []
        r["rfi"] = cands.pop(0) if cands else None
        matched += r["rfi"] is not None
    leftover = [s for v in by_key.values() for s in v]
    log(f"research.fi: {len(rfi)} JAES fundings, {matched} matched to jaes.fi rows, {len(leftover)} unmatched (added)")
    for s in leftover:
        kept.append({"year": int(s["fundingStartYear"]), "category": None, "grantee": None, "institution": None,
                     "title": (s.get("projectNameEn") or s.get("projectNameFi") or "").strip() or None,
                     "duration_years": None, "abstract": None, "amount": float(s["amount_in_EUR"]),
                     "currency": "EUR", "source": "research.fi", "scope_rule": "research.fi", "rfi": s})

    recs, seen = [], set()
    for r in kept:
        people = people_from(r["grantee"])
        s = r.get("rfi")
        award_no = None
        if s:
            award_no = (s.get("funderProjectNumber") or "").strip() or None
            grp = [g for g in s.get("fundingGroupPerson") or [] if (g.get("fundingGroupPersonLastName") or "").strip()]
            lead = next((g for g in grp if g.get("roleInFundingGroup") == "leader"), grp[0] if grp else None)
            if lead:
                lp = {"name": f"{lead['fundingGroupPersonFirstNames'].strip()} {lead['fundingGroupPersonLastName'].strip()}",
                      "given_name": lead["fundingGroupPersonFirstNames"].strip() or None,
                      "family_name": lead["fundingGroupPersonLastName"].strip(),
                      "orcid": (lead.get("fundingGroupPersonOrcid") or "").strip() or None}
                rest = [q for q in people if slug(q["family_name"]) != slug(lp["family_name"])]
                people = [lp] + rest
            if not r["title"]:
                r["title"] = (s.get("projectNameEn") or "").strip() or None
        lead = people[0] if people else {}
        if award_no:
            fid = award_no
        else:
            fid = base = f"JAES-{r['year']}-{slug(lead.get('family_name') or r['grantee'])[:40]}-{slug(lead.get('given_name'))[:20]}"
            i = 0
            while fid.lower() in seen:
                i += 1
                fid = f"{base}-{chr(ord('a') + i)}"
        if fid.lower() in seen:
            raise SystemExit(f"duplicate funder_award_id {fid}")
        seen.add(fid.lower())
        yrs = r["duration_years"]
        recs.append({
            "funder_award_id": fid,
            "research_fi_number": award_no,
            "research_fi_project_id": str(s["projectId"]) if s else None,
            "decision_date": (s.get("fundingApprovalDate") or None) if s else None,
            "grant_year": r["year"],
            "category": r["category"],
            "scope_rule": r["scope_rule"],
            "grantee": r["grantee"],
            "institution": r["institution"],
            "title": r["title"],
            "abstract": r["abstract"],
            "duration_years": yrs,
            "amount": r["amount"],
            "currency": r["currency"],
            "lead_name": lead.get("name"),
            "lead_given_name": lead.get("given_name"),
            "lead_family_name": lead.get("family_name"),
            "lead_orcid": lead.get("orcid"),
            "people": json.dumps(people, ensure_ascii=False),
            "source": r["source"] + ("+research.fi" if s and r["source"] != "research.fi" else ""),
            "landing_page_url": pages.get(r["year"]) or f"{SITE}/en/donations-granted/donations{r['year']}/",
        })
    df = pd.DataFrame(recs)
    log(f"{len(df)} research grants, {df['grant_year'].min()}-{df['grant_year'].max()}")
    log(f"  scope_rule {df['scope_rule'].value_counts().to_dict()}")
    log(f"  source {df['source'].value_counts().to_dict()}")
    for c in ["title", "amount", "lead_family_name", "lead_orcid", "institution", "abstract", "research_fi_number"]:
        log(f"  {c:20s} {df[c].notna().mean():6.1%}")
    log(f"  currencies {df['currency'].value_counts(dropna=False).to_dict()}; total EUR {df.loc[df.currency == 'EUR', 'amount'].sum():,.0f}")
    top = df.groupby(["lead_given_name", "lead_family_name"]).size().sort_values(ascending=False).head(5)
    log(f"  6.4a top PI combos: {top.to_dict()}")

    df = df.astype("string")
    df["amount"] = pd.to_numeric(df["amount"])
    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "jaes_projects.parquet"
    df.to_parquet(out, index=False)
    pd.DataFrame([{k: v for k, v in r.items() if k != "rfi"} for r in site]).astype("string").to_csv(
        args.output_dir / "jaes_all_site_rows_with_scope.csv", index=False)
    log(f"Wrote {len(df)} rows to {out}")
    if args.skip_upload:
        return
    if args.limit:
        raise SystemExit("refusing to upload a --limit smoke file to S3")

    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_jaes_projects.parquet"
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
