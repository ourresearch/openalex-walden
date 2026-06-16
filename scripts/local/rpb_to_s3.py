#!/usr/bin/env python3
"""
Research to Prevent Blindness (RPB) -> S3 Data Pipeline
=======================================================

Scrapes the RPB grantee archive and uploads a parquet to S3 for Databricks
ingestion. RPB is OpenAlex funder F4320306811 (ROR 04drjs621, DOI
10.13039/100001818, ~20.9K works, awards_count ~611 → large net-new gap).

Source: https://www.rpbusa.org/grantees/ (WordPress).
  PAGINATION: the static /grantees/page/N/ URLs are a TRAP (return the same first
  9 cards for every N). Real pagination is a WordPress admin-ajax action (plain
  GET, NO nonce required):
      GET /wp-admin/admin-ajax.php?action=get_filtered_posts&post_type=individual-grantees&paged=N&pagination_id=0
      GET /wp-admin/admin-ajax.php?action=get_dept_grantees&post_type=dept-grantees&paged=N&pagination_id=1
  Each page returns raw .grantee-card / .dept-grantee-card HTML (not JSON). The
  genuine end is HTTP 200 with 0 cards / the "No posts found." sentinel. Walk both
  endpoints to their sentinel (individuals ~184 pages, dept ~4 pages) -> 1,657
  unique grants. (A prior pass truncated at 54 = a client-side early-stop bug, not
  a server limit.)

Build decisions:
- NO native grant id anywhere -> funder_award_id SYNTHESIZED as
  'rpb-syn-' + sha1(year|grantee|award|institution)[:16]. (Weakens net-new work
  linkage and is unstable if RPB re-words a record — documented limitation.)
- No distinct project title on cards -> title = the award name (= scheme).
- 1,625 person-grantees (PI parsed, degrees stripped) + 32 departmental/org grants
  (pi_* null, org -> institution). Year-only -> start_date_raw=YYYY-01-01 (the
  notebook keeps start_year only; start_date NULL). 28 source-level dupes collapsed.

Output: s3://openalex-ingest/awards/rpb/rpb_grants.parquet
"""

import argparse
import hashlib
import os
import re
import subprocess
import sys
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

BASE = "https://www.rpbusa.org/grantees/"
AJAX = "https://www.rpbusa.org/wp-admin/admin-ajax.php"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "X-Requested-With": "XMLHttpRequest",
           "Referer": BASE, "Accept": "*/*"}
THROTTLE = 1.0
MAX_PAGES = 400
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/rpb/rpb_grants.parquet"

COLUMNS = ["funder_award_id", "title", "pi_full", "pi_given", "pi_family",
           "institution", "amount", "currency", "scheme", "start_date_raw",
           "end_date_raw", "description", "landing_page_url"]

DEGREE_TOKENS = {"md", "phd", "do", "ms", "msc", "mph", "od", "dvm", "facs",
                 "frcs", "mbbs", "mba", "ma", "ba", "bs", "bsc", "sc.d", "scd",
                 "dphil", "drph", "mba.", "pharmd", "mse", "meng", "bm", "bch",
                 "frcophth", "facp"}

session = requests.Session()
session.headers.update(HEADERS)


def clean(s):
    if s is None:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def synth_id(year, grantee, award, inst):
    raw = "|".join((year or "", grantee or "", award or "", inst or "")).lower()
    raw = re.sub(r"\s+", " ", raw).strip()
    return "rpb-syn-" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def parse_amount(text):
    if not text:
        return None
    m = re.search(r"[\d,]+(?:\.\d+)?", text)
    if not m:
        return None
    val = m.group(0).replace(",", "")
    try:
        return str(int(round(float(val))))
    except ValueError:
        return None


def _strip_trailing_degree_tokens(text):
    if not text:
        return text
    toks = text.split()
    while toks and toks[-1].lower().replace(".", "").rstrip(",") in DEGREE_TOKENS:
        toks.pop()
    return " ".join(toks).strip(" ,")


def split_name(grantee_raw):
    """RPB format: 'Family, Given / DEGREE, DEGREE' (degrees optional)."""
    if not grantee_raw:
        return None, None, None
    g = clean(grantee_raw)
    name_part = g.split("/", 1)[0].strip()
    parts = [p.strip() for p in name_part.split(",")]
    while len(parts) > 1 and parts[-1].lower().replace(".", "") in DEGREE_TOKENS:
        parts.pop()
    if len(parts) >= 2:
        family = parts[0]
        given = _strip_trailing_degree_tokens(parts[1])
        pi_full = f"{given} {family}".strip() if given else family
    else:
        family = None
        given = None
        pi_full = _strip_trailing_degree_tokens(parts[0]) if parts else None
    return clean(pi_full), clean(given), clean(family)


def parse_individual_card(card):
    for vh in card.select(".visually-hidden"):
        vh.extract()

    def txt(sel, root=card):
        el = root.select_one(sel)
        return clean(el.get_text(" ", strip=True)) if el else None

    grantee_raw = txt(".grantee")
    award_name = txt(".grant-name")
    dialog = card.select_one("dialog")
    institution = inst_url = grant_type = amount = year = None
    research_area = description = award_name_dlg = None
    if dialog:
        inst_a = dialog.select_one("a.institution-link")
        if inst_a:
            for vh in inst_a.select(".visually-hidden"):
                vh.extract()
            institution = clean(inst_a.get_text(" ", strip=True))
            inst_url = inst_a.get("href")
        gt = dialog.select_one(".grant-type .result")
        grant_type = clean(gt.get_text(" ", strip=True)) if gt else None
        an = dialog.select_one(".award-name .result")
        award_name_dlg = clean(an.get_text(" ", strip=True)) if an else None
        gy = dialog.select_one(".grant-year .result")
        year = clean(gy.get_text(" ", strip=True)) if gy else None
        ga = dialog.select_one(".grant-amount .result")
        amount = parse_amount(ga.get_text(" ", strip=True)) if ga else None
        areas = [clean(a.get_text(" ", strip=True))
                 for a in dialog.select(".research-area .area-item")]
        areas = [a for a in areas if a]
        research_area = "; ".join(areas) if areas else None
        desc_block = dialog.select_one(".description-research")
        if desc_block:
            ps = [clean(p.get_text(" ", strip=True)) for p in desc_block.find_all("p")]
            ps = [p for p in ps if p]
            description = " ".join(ps) if ps else None
    if not year:
        hy = card.select_one(".grant-awarded")
        if hy:
            m = re.search(r"(\d{4})", hy.get_text())
            year = m.group(1) if m else None

    award = award_name_dlg or award_name
    start_date_raw = f"{year}-01-01" if year and re.fullmatch(r"\d{4}", year) else None
    pi_full, pi_given, pi_family = split_name(grantee_raw)

    is_org = False
    gt_l = (grant_type or "").lower()
    if any(k in gt_l for k in ("department", "institution", "unrestricted")):
        is_org = True
    if pi_full is None and grantee_raw:
        is_org = True
    if is_org:
        if not institution:
            institution = clean(grantee_raw)
        pi_full = pi_given = pi_family = None

    desc_final = description
    if research_area and desc_final:
        desc_final = f"[Research Area: {research_area}] {desc_final}"
    elif research_area and not desc_final:
        desc_final = f"[Research Area: {research_area}]"

    return {
        "funder_award_id": synth_id(year, grantee_raw, award, institution),
        "title": award, "pi_full": pi_full, "pi_given": pi_given,
        "pi_family": pi_family, "institution": institution, "amount": amount,
        "currency": "USD", "scheme": award, "start_date_raw": start_date_raw,
        "end_date_raw": None, "description": desc_final, "landing_page_url": inst_url,
    }


def parse_dept_card(card):
    for vh in card.select(".visually-hidden"):
        vh.extract()

    def txt(sel):
        el = card.select_one(sel)
        return clean(el.get_text(" ", strip=True)) if el else None

    org = txt(".grantee")
    award = txt(".grant-name")
    state = txt(".grant-state")
    link = card.select_one("a.dept-grantee-card__external-link")
    return {
        "funder_award_id": synth_id(None, org, award, org),
        "title": award, "pi_full": None, "pi_given": None, "pi_family": None,
        "institution": org, "amount": None, "currency": "USD", "scheme": award,
        "start_date_raw": None, "end_date_raw": None,
        "description": f"[Departmental grant; State: {state}]" if state else "[Departmental grant]",
        "landing_page_url": link.get("href") if link else None,
    }


NO_POSTS_RE = re.compile(r"no\s+post\s+cards\s+to\s+show|no\s+posts\s+found", re.I)


def ajax_get(action, post_type, paged, pagination_id):
    params = {"action": action, "post_type": post_type,
              "paged": str(paged), "pagination_id": str(pagination_id)}
    last_exc = None
    for attempt in range(4):
        try:
            r = session.get(AJAX, params=params, timeout=45)
            if r.status_code >= 500:
                raise requests.RequestException(f"HTTP {r.status_code}")
            return r
        except requests.RequestException as e:
            last_exc = e
            if attempt == 3:
                raise
            time.sleep(2 * (attempt + 1))
    if last_exc:
        raise last_exc
    return None


def paginate(action, post_type, pagination_id, card_selector, parse_fn, label):
    """Walk until the genuine end: HTTP 200 with 0 cards / 'No posts found.'."""
    out = []
    page = 1
    ended_clean = False
    while page <= MAX_PAGES:
        r = ajax_get(action, post_type, page, pagination_id)
        if r.status_code != 200:
            print(f"[{label}] page {page} -> HTTP {r.status_code} (ERROR); stopping.", file=sys.stderr)
            break
        body = r.text or ""
        cards = BeautifulSoup(body, "lxml").select(card_selector)
        if not cards:
            tag = "sentinel" if NO_POSTS_RE.search(body) else "0 cards"
            print(f"[{label}] page {page} -> {tag} -> END.", file=sys.stderr)
            ended_clean = True
            break
        for c in cards:
            out.append(parse_fn(c))
        page += 1
        time.sleep(THROTTLE)
    print(f"[{label}] rows={len(out)} ended_clean={ended_clean}", file=sys.stderr)
    return out, ended_clean


def upload_to_s3(local_path):
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"\nUploading to {s3_uri}...")
    try:
        subprocess.run(["aws", "s3", "cp", str(local_path), s3_uri],
                       capture_output=True, text=True, check=True)
        print(f"Upload complete: {s3_uri}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Upload failed: {e.stderr}")
        return False


def main():
    ap = argparse.ArgumentParser(description="RPB grantees to S3")
    ap.add_argument("--output-dir", default="/tmp/rpb_data")
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    print("=" * 60)
    print("Research to Prevent Blindness (RPB) -> S3")
    print("=" * 60)

    rows, ind_clean = paginate("get_filtered_posts", "individual-grantees", 0,
                               ".grantee-card", parse_individual_card, "individual")
    dept_rows, dept_clean = paginate("get_dept_grantees", "dept-grantees", 1,
                                     ".dept-grantee-card", parse_dept_card, "dept")
    all_rows = rows + dept_rows

    seen, deduped, n_dupe = set(), [], 0
    for r in all_rows:
        if r["funder_award_id"] in seen:
            n_dupe += 1
            continue
        seen.add(r["funder_award_id"])
        deduped.append(r)
    all_rows = deduped

    df = pd.DataFrame(all_rows, columns=COLUMNS).astype("string")
    df = df.where(pd.notna(df), None)
    print(f"\nDataFrame: {len(df)} rows (dupes removed {n_dupe}; "
          f"ind_clean={ind_clean} dept_clean={dept_clean})")
    for c in ("title", "pi_family", "institution", "amount", "start_date_raw",
              "scheme", "description"):
        nn = int(df[c].notna().sum())
        print(f"  {c:16}: {nn}/{len(df)} ({round(100*nn/max(len(df),1))}%)")

    if len(df) < 1200 or not ind_clean:
        print(f"[ERROR] {len(df)} rows / ind_clean={ind_clean} — pagination truncated?", file=sys.stderr)
        sys.exit(1)

    out = os.path.join(args.output_dir, "rpb_grants.parquet")
    df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({os.path.getsize(out)/1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
