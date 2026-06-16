#!/usr/bin/env python3
"""
FULL VEGA (Slovakia) EXTRACT — concurrent version of /tmp/scout_vega.py
OpenAlex F4320323641 | provenance vega_sk | currency EUR

SOURCE: SKCRIS — Slovak national CRIS (https://www.skcris.sk).
Public REST backend (no auth/cookies needed):
  base = https://www.skcris.sk/projapi/api/v1
  LIST   GET /projects?offset=&limit=&filter=<F>   -> {count,limit,pages,page,data:[{id,name,code}]}
  DETAIL GET /projects/{id}                         -> {code,name,...,funds,department,duration,abstrs}
  ORGS   GET /projects/{id}/organizations           -> {data:[{id,name,relation}]}
  PERS   GET /projects/{id}/persons                  -> {data:[{id,name,relation}]}

CRITICAL GOTCHA (kept from scout): do NOT send langCode. With ?langCode=sk the
localized string fields come back EMPTY. Omitting langCode returns full Slovak data.

CONCURRENCY: per-project enrichment is 3 calls (detail+orgs+persons). We fan these
projects out across a ThreadPoolExecutor; each worker has its OWN requests.Session.
List enumeration stays sequential (offset/limit). 4x retry/backoff kept.
"""
import sys, time, json, urllib.parse, re, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd

BASE = "https://www.skcris.sk/projapi/api/v1"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
VEGA_UNI = "c41e6008-af50-40d2-857b-bc5b95fe5f47"
VEGA_SAV = "95daecdd-c774-4552-8710-c18e38d0a1e9"
FILTER = f"fund_class({VEGA_UNI},{VEGA_SAV})"
LP_BASE = "https://www.skcris.sk/sk/register-projektov/"

WORKERS = int(__import__("os").environ.get("VEGA_WORKERS", "8"))
REQ_DELAY = float(__import__("os").environ.get("VEGA_REQ_DELAY", "0"))  # optional per-request throttle

# per-thread requests.Session (one Session per worker thread, reused across its projects)
_tls = threading.local()


def _session():
    s = getattr(_tls, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": UA, "Accept": "application/json"})
        _tls.session = s
    return s


def _get(url, tries=4, pause=0.4, timeout=45):
    """GET json with 4x retry/backoff (kept from scout). Uses the calling thread's Session."""
    last = None
    sess = _session()
    for a in range(tries):
        try:
            if REQ_DELAY:
                time.sleep(REQ_DELAY)
            r = sess.get(url, timeout=timeout)
            r.raise_for_status()
            return json.loads(r.content.decode("utf-8"))
        except Exception as e:
            last = e
            time.sleep(pause * (a + 1))
    raise last


def list_ids(limit_total=None, page=1000):
    """Enumerate VEGA project ids via offset/limit paging. Returns (list[(id,code)], total)."""
    out, offset = [], 0
    qf = urllib.parse.quote(FILTER)
    total = None
    while True:
        # LIST queries have a fixed ~14s server cost and occasionally run 60-90s
        # (the filtered count/join is expensive; page size is server-capped at 1000).
        # Give them a long timeout + extra retries so a slow page completes rather
        # than timing out and re-running the same expensive query.
        d = _get(f"{BASE}/projects?offset={offset}&limit={page}&filter={qf}", tries=6, pause=2.0, timeout=120)
        total = d.get("count")
        rows = d.get("data", [])
        if not rows:
            break
        out += [(r["id"], r.get("code") or "") for r in rows]
        offset += page
        if limit_total and len(out) >= limit_total:
            out = out[:limit_total]
            break
        if offset >= (total or 0):
            break
        time.sleep(0.2)
    return out, total


def split_person(full):
    full = (full or "").strip()
    if not full:
        return None, None, None
    parts = full.split()
    if len(parts) == 1:
        return full, None, full
    given = " ".join(parts[:-1])
    family = parts[-1]
    return full, given, family


def parse_dates(duration):
    # duration like '01.01.2022 - 31.12.2024' -> ('01.01.2022','31.12.2024')
    if not duration:
        return None, None
    m = re.match(r"\s*([\d.]+)\s*-\s*([\d.]+)", duration)
    if m:
        return m.group(1), m.group(2)
    return None, None


def fetch_project(pid):
    det = _get(f"{BASE}/projects/{pid}")
    code = det.get("code") or None
    title = det.get("name") or None
    desc = det.get("abstrs") or None
    funds = (det.get("funds") or "").strip()  # e.g. '40378.00 EUR'
    amount, currency = None, "EUR"
    if funds:
        mm = re.match(r"([\d.,]+)\s*([A-Z]{3})?", funds)
        if mm:
            amount = mm.group(1)
            if mm.group(2):
                currency = mm.group(2)
    scheme = det.get("department") or None  # scientific discipline path; closest to 'scheme'
    sd, ed = parse_dates(det.get("duration"))

    # institution(s)
    inst = None
    try:
        orgs = _get(f"{BASE}/projects/{pid}/organizations").get("data", [])
        names = [o.get("name") for o in orgs if o.get("name")]
        if names:
            inst = "; ".join(dict.fromkeys(names))
    except Exception:
        pass

    # PI = person with relation 'zodpovedny riesitel'
    pi_full = pi_given = pi_family = None
    try:
        pers = _get(f"{BASE}/projects/{pid}/persons").get("data", [])
        pi = None
        for p in pers:
            rel = (p.get("relation") or "").lower()
            if "zodpovedn" in rel:  # zodpovedny riesitel (responsible investigator)
                pi = p.get("name")
                break
        if pi:
            pi_full, pi_given, pi_family = split_person(pi)
    except Exception:
        pass

    return {
        "funder_award_id": code,
        "title": title,
        "pi_full": pi_full,
        "pi_given": pi_given,
        "pi_family": pi_family,
        "institution": inst,
        "amount": amount,
        "currency": currency if amount else None,
        "scheme": scheme,
        "start_date_raw": sd,
        "end_date_raw": ed,
        "description": desc,
        "landing_page_url": LP_BASE + pid,
    }


def main():
    t0 = time.time()
    ids, total = list_ids(limit_total=None)
    print(f"[list] VEGA total count = {total}; enumerated ids = {len(ids)}", file=sys.stderr, flush=True)
    print(f"[fetch] fetching {len(ids)} projects with {WORKERS} workers (req_delay={REQ_DELAY})", file=sys.stderr, flush=True)

    rows = []
    fails = []
    done = 0
    n = len(ids)
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_project, pid): (pid, code) for pid, code in ids}
        for fut in as_completed(futs):
            pid, code = futs[fut]
            try:
                rows.append(fut.result())
            except Exception as e:
                fails.append((pid, code, str(e)))
                print(f"  ! {pid} ({code}) failed: {e}", file=sys.stderr, flush=True)
            done += 1
            if done % 200 == 0:
                el = time.time() - t0
                rate = done / el if el else 0
                eta = (n - done) / rate if rate else 0
                print(f"  ..{done}/{n}  ok={len(rows)} fail={len(fails)}  {rate:.1f} proj/s  eta {eta/60:.1f}m", file=sys.stderr, flush=True)

    df = pd.DataFrame(rows, columns=[
        "funder_award_id", "title", "pi_full", "pi_given", "pi_family", "institution",
        "amount", "currency", "scheme", "start_date_raw", "end_date_raw", "description",
        "landing_page_url"])
    # ensure every column is string dtype (null where absent), per contract
    for c in df.columns:
        df[c] = df[c].astype("string")
    # Completeness guard (Codex review): refuse to overwrite S3 with a shrunken /
    # heavily-failed pull.
    if len(df) < 11000 or len(fails) > 100:
        print(f"[ERROR] VEGA pull incomplete — rows={len(df)} (expected ~11,900), "
              f"{len(fails)} enrichment failures: refusing to write/upload",
              file=sys.stderr, flush=True)
        sys.exit(1)
    out = "/tmp/vega_sk_grants.parquet"
    df.to_parquet(out, index=False)
    import os as _os, subprocess as _sp
    if not _os.environ.get('SKIP_UPLOAD'):
        _sp.run(['aws','s3','cp',out,'s3://openalex-ingest/awards/vega_sk/vega_sk_grants.parquet'], check=True)
        print('uploaded s3://openalex-ingest/awards/vega_sk/vega_sk_grants.parquet')
    el = time.time() - t0
    print(f"[done] wrote {len(df)} rows -> {out}  (total {total}, fails {len(fails)}, {el/60:.1f}m)", file=sys.stderr, flush=True)

    # §1.4 coverage
    n2 = len(df)
    print(f"\n=== §1.4 SUMMARY  rows={n2} ===", flush=True)
    for c in df.columns:
        nn = df[c].notna().sum()
        print(f"  {c:18} {nn:5}/{n2}  {100*nn/n2:5.1f}%", flush=True)

    if fails:
        print(f"\n[fails] {len(fails)} project(s) failed after retries:", flush=True)
        for pid, code, e in fails[:50]:
            print(f"   {pid} {code} :: {e}", flush=True)


if __name__ == "__main__":
    main()
