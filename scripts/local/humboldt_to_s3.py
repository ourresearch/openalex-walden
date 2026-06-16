#!/usr/bin/env python3
"""
SOURCE-SCOUT — Alexander von Humboldt-Stiftung (F4320308269) + DAAD (F4320320875).

READ-ONLY cold-test for the OpenAlex awards pipeline. Headless (requests only).
All scratch in /tmp. No repo/S3/Databricks/git writes.

FINDINGS
========
HUMBOLDT (F4320308269)  -> BUILD candidate.
  The public "Explore the Humboldt Network" search is backed by TYPO3 Solr
  (tx_solr / rsmavhsolr). A wildcard query returns the FULL historical index of
  recipients (paginated 10/page; last page observed = 2815 -> ~28,150 rows),
  spanning at least back to 2019. Every result row is fully STRUCTURED:
    name (anchor text) | Program (scheme) | Selection date (Month YYYY)
    | Business address at selection | Current contact address (host institution)
    | Area of expertise | Keywords
  Native person/award id = the singleview/<ID> number in the result URL.
  Amount: ABSENT (fellowships/awards -> §6.7 amount-waiver). PI = person.

  Endpoint (GET):
    https://www.humboldt-foundation.de/en/connect/explore-the-humboldt-network
      ?tx_solr[q]=*&tx_solr[page]=<N>
  Landing page per fellow:
    https://www.humboldt-foundation.de/en/connect/explore-the-humboldt-network/singleview/<ID>/<slug>

DAAD (F4320320875)  -> SKIP.
  No structured public awardee/funded-persons database exists.
  - daad.de "Scholarship Database" = a funding-OPPORTUNITY finder for prospective
    applicants ("information about DAAD scholarship programmes ... offers from other
    funding organisations"), NOT a list of past awardees.
  - api.daad.de/api/... = image/asset CDN only (not a data API).
  - Sitemap (26,146 URLs) has no awardee DB; only narrative press releases, an
    alumni landing page (site-search only, no directory), and an admission DB.
  - OpenAIRE projects?funder=DAAD -> total=0 (not itemized in the EU CRIS).
  Only narrative/aggregate data is public => valid SKIP.

COLUMN CONTRACT (all string, null where absent):
  funder_award_id, title, pi_full, pi_given, pi_family, institution, amount,
  currency, scheme, start_date_raw, end_date_raw, description, landing_page_url
"""
import re
import sys
import time
import html as ihtml
import urllib.parse

import requests

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
BASE = "https://www.humboldt-foundation.de"
EXPLORE = BASE + "/en/connect/explore-the-humboldt-network"
SINGLE = EXPLORE + "/singleview/{id}/{slug}"

MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], 1)}


def _clean(s):
    if s is None:
        return None
    s = ihtml.unescape(re.sub(r"<[^>]+>", " ", s))
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def split_name(full):
    """Split a display name into given/family. Drops a leading 'Dr.'/title token.
    Heuristic: last whitespace token = family, the rest (minus title) = given."""
    if not full:
        return None, None, None
    disp = full.strip()
    toks = disp.split()
    # strip leading academic titles
    titles = {"Dr.", "Dr", "Prof.", "Prof", "Professor", "PD", "Priv.-Doz.",
              "Dr.-Ing.", "Dr.med.", "M.D.", "Ph.D.", "PhD", "Mr.", "Ms.", "Mrs."}
    core = [t for t in toks if t.rstrip(".") + "." not in titles and t not in titles]
    if not core:
        core = toks
    if len(core) == 1:
        return disp, None, core[0]
    given = " ".join(core[:-1])
    family = core[-1]
    return disp, given, family


def parse_selection_date(s):
    """'May 2026' -> ('2026-01-01', 'YEAR_FROM_SELECTION_DATE:May 2026'). We use
    year only and force YYYY-01-01 per pipeline convention (flag in description)."""
    if not s:
        return None, None
    m = re.search(r"([A-Za-z]+)\s+(\d{4})", s)
    if not m:
        return None, None
    year = m.group(2)
    return f"{year}-01-01", f"selection_date={s.strip()}"


def fetch_page(page, session, q="*", retries=4):
    params = {"tx_solr[q]": q, "tx_solr[page]": str(page)}
    url = EXPLORE + "?" + urllib.parse.urlencode(params)
    last = None
    for attempt in range(retries):
        try:
            r = session.get(url, headers={"User-Agent": UA}, timeout=40)
            if r.status_code == 200:
                return r.text
            last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = str(e)
        time.sleep(1.5 * (attempt + 1))
    sys.stderr.write(f"[warn] page {page} failed: {last}\n")
    return None


def _norm_label(lbl):
    """Normalise a field label: drop zero-width spaces, collapse, lowercase."""
    lbl = lbl.replace("​", "")
    return re.sub(r"\s+", " ", lbl).strip().rstrip(":").lower()


def parse_rows(html):
    """Yield one structured dict per result row on a listing page.

    Each row is a <li class="list-item--person"> ... </li> block whose data fields
    are rendered as <strong>Label: </strong>Value pairs inside an inner <ul>.
    """
    rows = []
    # Split the page into per-person <li> blocks (each contains exactly one
    # singleview link).  Use the singleview marker as the row boundary, then cut
    # the block at the start of the NEXT person's headline so values don't bleed.
    parts = re.split(r"(?=<a [^>]*href=\"[^\"]*singleview/\d+/)", html)
    for p in parts:
        mid = re.search(r"singleview/(\d+)/([a-z0-9-]+)", p)
        if not mid:
            continue
        fid, slug = mid.group(1), mid.group(2)
        # name = anchor text of the singleview link at the head of this block
        nm = re.search(
            r'href="[^"]*singleview/' + re.escape(fid) +
            r'/[a-z0-9-]+"[^>]*>(.*?)</a>', p, re.S)
        name = _clean(nm.group(1)) if nm else None

        # collect <strong>Label:</strong>Value pairs; value runs until the next
        # tag (</li>, <strong>, etc.).
        fields = {}
        for fm in re.finditer(
                r"<strong>(.*?)</strong>\s*(.*?)\s*(?=<)", p, re.S):
            lbl = _norm_label(_clean(fm.group(1)) or "")
            val = _clean(fm.group(2))
            if lbl and val:
                fields[lbl] = val

        rows.append({
            "id": fid, "slug": slug, "name": name,
            "program": fields.get("program"),
            "seldate": fields.get("selection date"),
            "biz": fields.get("business address (at time of selection)"),
            "contact": fields.get("current contact address"),
            "expertise": fields.get("area of expertise"),
            "keywords": fields.get("keywords"),
        })
    return rows


def to_contract(rec):
    """Map a raw Humboldt row to the column contract."""
    full, given, family = split_name(rec.get("name"))
    start_raw, date_flag = parse_selection_date(rec.get("seldate"))
    # institution = current host (contact address); fall back to business address.
    institution = rec.get("contact") or rec.get("biz")
    # title: fellowships have no project title -> synthesize from programme.
    scheme = rec.get("program")
    yr = (rec.get("seldate") or "").strip()
    title = None
    if scheme:
        title = f"{scheme}" + (f" ({yr})" if yr else "")
    # description: pack expertise + keywords + provenance flags.
    desc_bits = []
    if rec.get("expertise"):
        desc_bits.append("Area of expertise: " + rec["expertise"])
    if rec.get("keywords"):
        desc_bits.append("Keywords: " + rec["keywords"])
    if rec.get("biz"):
        desc_bits.append("Business address at selection: " + rec["biz"])
    if date_flag:
        desc_bits.append("[" + date_flag + "; start_date forced to YYYY-01-01]")
    description = " | ".join(desc_bits) or None
    landing = SINGLE.format(id=rec["id"], slug=rec["slug"])
    return {
        "funder_award_id": "humboldt:" + rec["id"],  # native singleview id
        "title": title,
        "pi_full": full,
        "pi_given": given,
        "pi_family": family,
        "institution": institution,
        "amount": None,            # fellowships -> amount absent (§6.7 waiver)
        "currency": "EUR",
        "scheme": scheme,
        "start_date_raw": start_raw,
        "end_date_raw": None,
        "description": description,
        "landing_page_url": landing,
    }


COLUMNS = ["funder_award_id", "title", "pi_full", "pi_given", "pi_family",
           "institution", "amount", "currency", "scheme", "start_date_raw",
           "end_date_raw", "description", "landing_page_url"]


def harvest_humboldt(max_pages, session, polite=0.7):
    seen = set()
    out = []
    for page in range(1, max_pages + 1):
        html = fetch_page(page, session)
        if not html:
            continue
        rows = parse_rows(html)
        if not rows:
            sys.stderr.write(f"[info] page {page}: 0 rows (stop?)\n")
            break
        for raw in rows:
            if raw["id"] in seen:
                continue
            seen.add(raw["id"])
            out.append(to_contract(raw))
        sys.stderr.write(f"[info] page {page}: +{len(rows)} rows "
                         f"(total {len(out)})\n")
        time.sleep(polite)
    return out


def detect_total_pages(session):
    html = fetch_page(1, session)
    if not html:
        return None
    pages = [int(x) for x in re.findall(
        r"tx_solr(?:%5Bpage%5D|\[page\])=(\d+)", html)]
    return max(pages) if pages else None


def main():
    import pandas as pd
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    s = requests.Session()
    total = detect_total_pages(s)
    sys.stderr.write(f"[info] Humboldt last page = {total} "
                     f"(~{(total or 0)*10} rows)\n")
    recs = harvest_humboldt(n, s)
    df = pd.DataFrame(recs, columns=COLUMNS).astype("string")
    # strip residual German academic title prefixes (Dr./Prof./Priv.-Doz./habil./…) that the
    # name parser leaves on ~2% of fellows, e.g. "Priv. Hans" -> "Hans"
    _TITLE = re.compile(r"^\s*(?:Prof\.?|Professor|Dr\.?|PD|Priv\.?-?Doz\.?|Priv\.?|Doz\.?|habil\.?|h\.?c\.?|apl\.?|Dipl\.?-?[A-Za-z]*\.?|Mag\.?|Ing\.?)(?:\s+|\b)")
    def _strip_titles(v):
        if not isinstance(v, str):
            return v
        prev = None
        while prev != v:
            prev = v
            v = _TITLE.sub("", v).strip()
        return v or None
    for _c in ("pi_given", "pi_full"):
        df[_c] = df[_c].map(_strip_titles).astype("string")
    out = "/tmp/humboldt_grants.parquet"
    df.to_parquet(out, index=False)
    import os as _os, subprocess as _sp
    if not _os.environ.get('SKIP_UPLOAD'):
        _sp.run(['aws','s3','cp',out,'s3://openalex-ingest/awards/humboldt/humboldt_grants.parquet'], check=True)
        print('uploaded s3://openalex-ingest/awards/humboldt/humboldt_grants.parquet')
    sys.stderr.write(f"[done] wrote {len(df)} rows -> {out}\n")
    print(df.head(3).to_string())
    # DAAD: no structured source -> no parquet emitted (SKIP). See module docstring.


if __name__ == "__main__":
    main()
