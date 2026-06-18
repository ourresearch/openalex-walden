#!/usr/bin/env python3
"""
Fondazione Telethon (Italy) -> clean awards parquet for the OpenAlex Walden awards pipeline.

Funder:    Fondazione Telethon  (OpenAlex F4320321179 / numeric 4320321179)
ROR:       04xraxn18   country: IT   currency: EUR
Provenance slug: telethon

Source mechanism (two-step, no auth):
  1. ENUMERATE via WordPress REST:
       https://back.fondazionetelethon.it/wp-json/wp/v2/projects?per_page=100&page=N
     Paginate 1..X-WP-TotalPages. Each record -> id (native WP post id, stable),
     slug, title.rendered (IT), content.rendered (IT abstract),
     wpml_translations[0].post_title (EN title).
  2. PER-GRANT DETAIL (rich fields live in SSR HTML, not the REST JSON):
       https://www.fondazionetelethon.it/cosa-facciamo/ricerca/progetti-finanziati/{slug}/
     Parse:
       - PI:          <dl.project-details__list> dt "Ricercatore/Coordinatore"
       - institution: dt "Istituto ospitante"
       - disease:     dt "Malattie"
       - status:      dt "Stato progetto"
       - amount:      <ul.projects-figures> li whose <span> == "Totale Fondi" -> "767.574€"
                      (Italian thousands sep "." -> strip to integer euros)
       - dates:       li whose <span> starts "Anni YYYY/YYYY" -> start_year / end_year
       - papers:      <h2.publications__heading> sibling EuropePMC/DOI links
     Detail fetches use a ThreadPool (~8 workers) with polite delay + retry/backoff + browser UA.

Output (13 cols, all string/nullable, null where absent):
  funder_award_id, title, pi_full, pi_given, pi_family, institution, amount,
  currency, scheme, start_date_raw, end_date_raw, description, landing_page_url

  funder_award_id  = "telethon-<wp_id>"   (native, stable WP post id)
  amount           = integer euros as string; currency "EUR" only where amount present
  start_date_raw   = "YYYY-01-01"; end_date_raw = "YYYY-12-31" when an end year is known
  description      = disease/area + status + abstract + EuropePMC paper links (packed)
  landing_page_url = the public www detail URL

Completeness guard: harvested vs advertised X-WP-Total; HARD-FAIL (no upload) if materially short.
Shrink guard (§1.4): if uploading would materially shrink an existing S3 parquet, abort unless --allow-shrink.
"""

import argparse
import io
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ----------------------------------------------------------------------------- constants
FUNDER_ID        = "4320321179"
PROVENANCE       = "telethon"
CURRENCY         = "EUR"

REST_BASE        = "https://back.fondazionetelethon.it/wp-json/wp/v2/projects"
DETAIL_BASE      = "https://www.fondazionetelethon.it/cosa-facciamo/ricerca/progetti-finanziati/{slug}/"
PER_PAGE         = 100

S3_BUCKET        = "openalex-ingest"
S3_KEY           = f"awards/{PROVENANCE}/{PROVENANCE}_grants.parquet"
S3_URI           = f"s3://{S3_BUCKET}/{S3_KEY}"
OUTPUT_FILENAME  = f"{PROVENANCE}_grants.parquet"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "Accept-Language": "it-IT,it;q=0.9,en;q=0.8"}

COLUMNS = [
    "funder_award_id", "title", "pi_full", "pi_given", "pi_family",
    "institution", "amount", "currency", "scheme",
    "start_date_raw", "end_date_raw", "description", "landing_page_url",
]

# Completeness guard: require at least this fraction of advertised projects to have
# been enumerated, and this fraction of enumerated rows to survive into the parquet.
MIN_ENUM_FRACTION    = 0.97   # harvested-from-REST vs X-WP-Total
MIN_ROW_FRACTION     = 0.97   # rows in parquet vs enumerated count

# Honorific tokens stripped before PI name splitting.
TITLE_TOKENS = {
    "dr", "dr.", "dott", "dott.", "dottor", "dottoressa", "dssa", "d.ssa",
    "prof", "prof.", "professor", "professore", "professoressa",
    "mr", "mr.", "mrs", "mrs.", "ms", "ms.", "phd", "ph.d", "md", "m.d",
    "sig", "sig.", "sig.ra", "ing", "ing.",
}

_thread_local = threading.local()


# ----------------------------------------------------------------------------- helpers
def get_session() -> requests.Session:
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update(HEADERS)
        _thread_local.session = s
    return s


def clean(val):
    """Normalize a scraped string -> stripped value or None (treat dashes/empties as absent)."""
    if val is None:
        return None
    v = " ".join(str(val).split()).strip()
    if v in ("", "-", "–", "—", "/", "N/A", "n/a", "—/—"):
        return None
    return v


def fetch(url, *, want_json, retries=5, base_delay=0.6, timeout=40):
    """GET with retry/backoff. Returns parsed JSON (+ response) or text, or None on failure."""
    sess = get_session()
    last = None
    for attempt in range(retries):
        try:
            r = sess.get(url, timeout=timeout)
            if r.status_code == 200:
                return (r.json(), r) if want_json else r.text
            if r.status_code in (429, 500, 502, 503, 504):
                last = f"HTTP {r.status_code}"
            elif r.status_code == 404:
                return None  # missing page -> caller handles
            else:
                last = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last = repr(e)
        time.sleep(base_delay * (2 ** attempt))
    sys.stderr.write(f"[warn] giving up on {url}: {last}\n")
    return None


# ----------------------------------------------------------------------------- §1 enumerate
def enumerate_projects(limit=None):
    """
    Walk the WP-REST projects endpoint. Returns (records, advertised_total).
    records: list of dicts with wp_id, slug, title_it, title_en, abstract_it.
    """
    # First call also yields the advertised totals from response headers.
    sess = get_session()
    first = sess.get(f"{REST_BASE}?per_page={PER_PAGE}&page=1", timeout=40)
    first.raise_for_status()
    advertised = int(first.headers.get("X-WP-Total", "0"))
    total_pages = int(first.headers.get("X-WP-TotalPages", "0"))
    print(f"[enum] advertised X-WP-Total={advertised}  X-WP-TotalPages={total_pages}")

    records = []
    seen = set()

    def ingest(page_json):
        for rec in page_json:
            wp_id = rec.get("id")
            slug = rec.get("slug")
            if wp_id is None or not slug or wp_id in seen:
                continue
            seen.add(wp_id)
            title_it = clean((rec.get("title") or {}).get("rendered"))
            # EN title via WPML translations, if present
            title_en = None
            for tr in (rec.get("wpml_translations") or []):
                if str(tr.get("locale", "")).startswith("en"):
                    title_en = clean(tr.get("post_title"))
                    break
            abstract_it = (rec.get("content") or {}).get("rendered")
            records.append({
                "wp_id": wp_id,
                "slug": slug,
                "title_it": title_it,
                "title_en": title_en,
                "abstract_it": abstract_it,
            })

    ingest(first.json())
    page = 2
    while page <= total_pages:
        if limit is not None and len(records) >= limit:
            break
        data = fetch(f"{REST_BASE}?per_page={PER_PAGE}&page={page}", want_json=True)
        if data is None:
            sys.stderr.write(f"[warn] enum page {page} failed; stopping enumeration early\n")
            break
        page_json, _ = data
        if not page_json:
            break
        ingest(page_json)
        if page % 8 == 0 or page == total_pages:
            print(f"[enum] page {page}/{total_pages}  cumulative={len(records)}")
        page += 1

    if limit is not None:
        records = records[:limit]
    print(f"[enum] enumerated {len(records)} unique projects")
    return records, advertised


# ----------------------------------------------------------------------------- §2 detail parse
_AMOUNT_RE = re.compile(r"([\d][\d. \s]*)\s*€")
_YEAR_RE   = re.compile(r"(19|20)\d{2}")


def parse_amount(strong_text):
    """'135.900€' (Italian) -> '135900'. Returns int-as-str or None."""
    if not strong_text:
        return None
    m = _AMOUNT_RE.search(strong_text)
    if not m:
        return None
    digits = re.sub(r"[^\d]", "", m.group(1))  # drop '.', spaces, nbsp
    if not digits:
        return None
    try:
        return str(int(digits))
    except ValueError:
        return None


def parse_years(anni_label):
    """'Anni 2008/2010' -> ('2008','2010'); 'Anni 2009' -> ('2009', None)."""
    if not anni_label:
        return None, None
    yrs = re.findall(r"\b(?:19|20)\d{2}\b", anni_label)
    start = yrs[0] if yrs else None
    end = yrs[-1] if len(yrs) >= 2 else None
    return start, end


def split_pi(name):
    """Strip honorifics; last token = family, rest = given. Returns (full, given, family)."""
    name = clean(name)
    if not name:
        return None, None, None
    toks = [t for t in name.split() if t]
    # drop honorifics (case-insensitive, with/without trailing dot)
    kept = [t for t in toks if t.lower().strip(".") not in {h.strip(".") for h in TITLE_TOKENS}]
    if not kept:
        kept = toks
    full = " ".join(kept)
    if len(kept) == 1:
        return full, None, kept[0]
    family = kept[-1]
    given = " ".join(kept[:-1])
    return full, given, family


def parse_detail_html(html):
    """Extract the fields that only exist in the SSR detail page."""
    soup = BeautifulSoup(html, "html.parser")
    out = {
        "pi_name": None, "institution": None, "disease": None, "status": None,
        "amount": None, "start_year": None, "end_year": None,
        "scheme": None, "papers": [], "title_h1": None,
    }

    h1 = soup.find("h1")
    if h1:
        out["title_h1"] = clean(h1.get_text(strip=True))

    # <dl class="project-details__list"> dt/dd pairs.
    # IMPORTANT: bind each <dt> to the <dd> that actually FOLLOWS it in document order.
    # Some pages omit a <dd> (e.g. an empty "Partner"), so a naive zip(dts, dds) would
    # shift every later value onto the wrong label. Walk siblings instead.
    dl = soup.select_one("dl.project-details__list")
    if dl:
        pairs = {}
        for dt in dl.find_all("dt"):
            key = clean(dt.get_text(" ", strip=True)) or ""
            # find the next element sibling; only treat it as this dt's value if it's a <dd>
            sib = dt.find_next_sibling()
            while sib is not None and getattr(sib, "name", None) is None:
                sib = sib.find_next_sibling()
            val = None
            if sib is not None and sib.name == "dd":
                val = clean(sib.get_text(" ", strip=True))
            pairs[key] = val
        for k, v in pairs.items():
            kl = k.lower()
            if "ricercatore" in kl or "coordinatore" in kl:
                out["pi_name"] = v
            elif "istituto ospitante" in kl:
                out["institution"] = v
            elif "malatti" in kl:
                out["disease"] = v
            elif "stato progetto" in kl:
                out["status"] = v
            elif "bando" in kl or "programma" in kl or "call" in kl:
                out["scheme"] = v

    # <ul class="projects-figures"> figures: amount + years
    ul = soup.select_one("ul.projects-figures")
    if ul:
        for li in ul.find_all("li"):
            span = li.find("span")
            strong = li.find("strong")
            label = clean(span.get_text(strip=True)) if span else ""
            value = strong.get_text(strip=True) if strong else ""
            if label and "totale fondi" in label.lower():
                out["amount"] = parse_amount(value)
            elif label and label.lower().startswith("anni"):
                s, e = parse_years(label)
                out["start_year"], out["end_year"] = s, e

    # Publications (EuropePMC / DOI / PubMed) under the publications heading
    papers = []
    seen_links = set()
    for h2 in soup.select("h2.publications__heading"):
        cont = h2.find_parent()
        if not cont:
            continue
        for a in cont.find_all("a", href=True):
            href = a["href"].strip()
            if any(k in href.lower() for k in ("europepmc", "doi.org", "ncbi.nlm", "pubmed")):
                if href not in seen_links:
                    seen_links.add(href)
                    papers.append(href)
    out["papers"] = papers
    return out


def build_description(abstract_it, disease, status, papers):
    """Pack disease/area + status + abstract + paper links into one description string."""
    parts = []
    if disease:
        parts.append(f"Malattie: {disease}")
    if status:
        parts.append(f"Stato progetto: {status}")
    if abstract_it:
        ab = clean(BeautifulSoup(abstract_it, "html.parser").get_text(" ", strip=True))
        if ab:
            parts.append(ab)
    if papers:
        parts.append("Pubblicazioni: " + " ; ".join(papers))
    desc = "\n\n".join(p for p in parts if p)
    return desc or None


def process_record(rec):
    """Fetch + parse one project's detail page; return a contract row dict (or None on hard failure)."""
    slug = rec["slug"]
    wp_id = rec["wp_id"]
    url = DETAIL_BASE.format(slug=slug)
    html = fetch(url, want_json=False)

    detail = {}
    if html:
        try:
            detail = parse_detail_html(html)
        except Exception as e:  # noqa: BLE001
            sys.stderr.write(f"[warn] parse failed for {wp_id} ({slug}): {e!r}\n")
            detail = {}

    # polite jitter so 8 workers don't hammer in lockstep
    time.sleep(0.15)

    # Title: prefer EN (from REST WPML) -> IT REST title -> H1
    title = rec.get("title_en") or rec.get("title_it") or detail.get("title_h1")

    pi_full, pi_given, pi_family = split_pi(detail.get("pi_name"))

    amount = detail.get("amount")
    currency = CURRENCY if amount else None

    start_year = detail.get("start_year")
    end_year = detail.get("end_year")
    start_date_raw = f"{start_year}-01-01" if start_year else None
    end_date_raw = f"{end_year}-12-31" if end_year else None

    description = build_description(
        rec.get("abstract_it"), detail.get("disease"),
        detail.get("status"), detail.get("papers"),
    )

    return {
        "funder_award_id": f"{PROVENANCE}-{wp_id}",
        "title": clean(title),
        "pi_full": pi_full,
        "pi_given": pi_given,
        "pi_family": pi_family,
        "institution": clean(detail.get("institution")),
        "amount": amount,
        "currency": currency,
        "scheme": clean(detail.get("scheme")),
        "start_date_raw": start_date_raw,
        "end_date_raw": end_date_raw,
        "description": description,
        "landing_page_url": url,
    }


# ----------------------------------------------------------------------------- §3 harvest
def harvest(records, workers=8):
    rows = []
    total = len(records)
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(process_record, rec): rec for rec in records}
        for fut in as_completed(futures):
            row = fut.result()
            if row is not None:
                rows.append(row)
            done += 1
            if done % 200 == 0 or done == total:
                print(f"[detail] {done}/{total} processed")
    return rows


# ----------------------------------------------------------------------------- §4 frame / report
def to_frame(rows):
    df = pd.DataFrame(rows, columns=COLUMNS)
    # dedup by funder_award_id (keep first)
    before = len(df)
    df = df.drop_duplicates(subset=["funder_award_id"], keep="first").reset_index(drop=True)
    if len(df) != before:
        print(f"[dedup] {before} -> {len(df)} rows after dropping {before - len(df)} dup ids")
    # enforce all-string dtype, NaN/empty -> None
    for c in COLUMNS:
        df[c] = df[c].apply(lambda v: None if (v is None or (isinstance(v, float) and pd.isna(v)) or v == "") else str(v))
    df = df.astype("string")
    return df


def fill_report(df):
    print("\n=== field fill rates ===")
    n = len(df)
    for c in ["title", "pi_full", "institution", "amount", "start_date_raw",
              "end_date_raw", "description", "scheme", "landing_page_url"]:
        filled = int(df[c].notna().sum())
        pct = (100.0 * filled / n) if n else 0.0
        print(f"  {c:16s} {filled:5d}/{n:<5d}  {pct:5.1f}%")
    print(f"  rows total       {n}")


# ----------------------------------------------------------------------------- §1.4 shrink check
def s3_existing_rowcount(output_dir):
    """Return row count of the existing S3 parquet (or None if absent/unreadable)."""
    try:
        import boto3  # noqa: F401
    except Exception:
        boto3 = None
    tmp = os.path.join(output_dir, f"_existing_{OUTPUT_FILENAME}")
    rc = os.system(f"aws s3 cp {S3_URI} {tmp} >/dev/null 2>&1")
    if rc != 0 or not os.path.exists(tmp):
        return None
    try:
        existing = pd.read_parquet(tmp)
        return len(existing)
    except Exception:
        return None
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


def shrink_check(df, output_dir, allow_shrink):
    """§1.4 — abort upload if the new parquet would materially shrink the existing one."""
    prev = s3_existing_rowcount(output_dir)
    if prev is None:
        print("[shrink] no readable existing S3 parquet — first upload, OK")
        return True
    new = len(df)
    print(f"[shrink] existing rows={prev}  new rows={new}")
    if new < prev * 0.97:
        if allow_shrink:
            print(f"[shrink] WARNING new < 97% of existing but --allow-shrink set; proceeding")
            return True
        print(f"[shrink] ABORT new ({new}) materially smaller than existing ({prev}); "
              f"re-run with --allow-shrink to override")
        return False
    return True


# ----------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(
        description="Build Fondazione Telethon (IT) awards parquet and upload to S3.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--limit", type=int, default=None,
                    help="Cap number of projects enumerated/harvested (debug).")
    ap.add_argument("--skip-upload", action="store_true",
                    help="Build the parquet locally but do not upload to S3.")
    ap.add_argument("--skip-download", action="store_true",
                    help="Reuse an existing local parquet (skip scrape); only re-run guards/upload.")
    ap.add_argument("--output-dir", default="/tmp",
                    help="Directory to write the parquet into.")
    ap.add_argument("--workers", type=int, default=8,
                    help="ThreadPool workers for detail fetches.")
    ap.add_argument("--allow-shrink", action="store_true",
                    help="Permit upload even if it materially shrinks the existing S3 parquet.")
    args = ap.parse_args()

    # SKIP_UPLOAD env var also honored
    skip_upload = args.skip_upload or os.environ.get("SKIP_UPLOAD", "").strip().lower() in ("1", "true", "yes")

    os.makedirs(args.output_dir, exist_ok=True)
    out_path = os.path.join(args.output_dir, OUTPUT_FILENAME)

    if args.skip_download:
        if not os.path.exists(out_path):
            sys.exit(f"[fatal] --skip-download set but {out_path} does not exist")
        df = pd.read_parquet(out_path).astype("string")
        print(f"[reuse] loaded {len(df)} rows from {out_path}")
        advertised = None
    else:
        # §1 enumerate
        records, advertised = enumerate_projects(limit=args.limit)
        if not records:
            sys.exit("[fatal] enumeration returned 0 records")

        # completeness guard A: enumerated vs advertised (skip when --limit constrains)
        if args.limit is None and advertised:
            if len(records) < advertised * MIN_ENUM_FRACTION:
                sys.exit(f"[fatal] completeness guard: enumerated {len(records)} < "
                         f"{MIN_ENUM_FRACTION:.0%} of advertised {advertised}; refusing to continue")

        # §3 harvest details
        rows = harvest(records, workers=args.workers)

        # §4 frame
        df = to_frame(rows)

        # completeness guard B: rows vs enumerated
        if args.limit is None and len(df) < len(records) * MIN_ROW_FRACTION:
            sys.exit(f"[fatal] completeness guard: parquet rows {len(df)} < "
                     f"{MIN_ROW_FRACTION:.0%} of enumerated {len(records)}; refusing to upload")

        df.to_parquet(out_path, index=False)
        print(f"[write] {len(df)} rows -> {out_path}")

    # report
    fill_report(df)
    if advertised is not None:
        harvested = len(df)
        print(f"\n[completeness] advertised X-WP-Total={advertised}  harvested={harvested}  "
              f"({100.0 * harvested / advertised:.1f}%)")

    # upload
    if skip_upload:
        print("\n[upload] SKIPPED (skip-upload / SKIP_UPLOAD)")
        return

    if args.limit is not None:
        print("\n[upload] SKIPPED (--limit set; partial harvest must not overwrite S3)")
        return

    # §1.4 shrink guard
    if not shrink_check(df, args.output_dir, args.allow_shrink):
        sys.exit(1)

    print(f"\n[upload] {out_path} -> {S3_URI}")
    rc = os.system(f"aws s3 cp {out_path} {S3_URI}")
    if rc != 0:
        sys.exit(f"[fatal] aws s3 cp failed with code {rc}")
    print("[upload] done")


if __name__ == "__main__":
    main()
