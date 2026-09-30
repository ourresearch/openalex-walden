#!/usr/bin/env python3
"""
Defra (Department for Environment, Food and Rural Affairs) to S3 Data Pipeline
==============================================================================

Defra publishes every research project it has sponsored in "Science Search"
(https://randd.defra.gov.uk/), ~13,600 projects. The site is a Blazor Server
app: pages render over a SignalR circuit, so plain HTTP returns an empty shell
and there is no export/API (checked 2026-09-30; data.gov.uk has no dataset).
Method 6 (Playwright) on the runbook ladder.

1. List crawl: run the empty search (status = All, sorted by project code) and
   page through all results (10 per page). Each result gives the project code,
   title, start/end month, cost (GBP) and the ProjectDetails?ProjectId=N link.
2. Detail crawl (N concurrent pages, cached per ProjectId): description,
   objective, contractor / funded organisations, keywords, fields of study.
   Defra does not publish investigator names; the contractor list is the
   grantee.

funder_award_id = the Defra project code (e.g. "SE3287", "AC0114"): this is
the form researchers cite (OpenAlex grants.funder:F4320319995 award ids are
overwhelmingly Defra project codes).

Output: s3://openalex-ingest/awards/defra/defra_projects.parquet
"""

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (sys.stdout.reconfigure + utf-8 file I/O defaults, under the _sys_utf8 alias)
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). This
# crashes scrapers writing laureate names with non-ASCII chars (Polish ł,
# Turkish ğ, Greek μ, combining accents, zero-width spaces). Production
# runs on Linux/Databricks where UTF-8 is the default, but this fixes
# local validation on Windows without requiring contractors to set
# PYTHONUTF8=1 in their environment. See runbook §1.2.
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

import argparse
import asyncio
import calendar
import html
import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

BASE = "https://randd.defra.gov.uk"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/defra/defra_projects.parquet"
DEFAULT_CHROME = r"C:\Users\kyled\.agent-browser\browsers\chrome-148.0.7778.97\chrome.exe"
UA = "Mozilla/5.0 (compatible; openalex-walden/1.0; +https://openalex.org)"

MAX_CONSECUTIVE_EMPTY = 3


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<!--!-->", "", fragment)
    t = re.sub(r"<br\s*/?>|</p>|</li>", "\n", t)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t).replace("\u200b", "").replace("\ufeff", "")
    t = re.sub(r"[ \t\r\f\v]+", " ", t)
    t = re.sub(r"\s*\n\s*", "\n", t).strip()
    return t or None


# ---------------------------------------------------------------- list crawl
ITEM_RE = re.compile(
    r'<p class="govuk-body-l[^"]*">(?P<head>.*?)</p>\s*(?:<!--!-->)?\s*'
    r'<p class="govuk-body-s[^"]*">(?P<meta>.*?)</p>\s*'
    r'<a href="/ProjectDetails\?ProjectId=(?P<pid>\d+)"',
    re.S,
)


def month(s: str | None) -> str | None:
    """'04/2006' -> '2006-04'"""
    m = re.fullmatch(r"(\d{1,2})/(\d{4})", (s or "").strip())
    if not m or not 1 <= int(m.group(1)) <= 12:
        return None
    return f"{m.group(2)}-{int(m.group(1)):02d}"


def parse_list(page_html: str) -> list[dict]:
    rows = []
    for m in ITEM_RE.finditer(page_html):
        head = text(m.group("head")) or ""
        meta = text(m.group("meta")) or ""
        code, _, title = head.partition(" - ")
        sd = re.search(r"Start Date - ([\d/]+)", meta)
        ed = re.search(r"End Date - ([\d/]+)", meta)
        cost = re.search(r"Project Cost\s*(.*)$", meta)
        rows.append({
            "project_id": m.group("pid"),
            "project_code": code.strip(),
            "list_title": title.strip() or None,
            "start_month": month(sd.group(1)) if sd else None,
            "end_month": month(ed.group(1)) if ed else None,
            "list_cost_text": cost.group(1).strip() if cost else None,
        })
    return rows


async def crawl_list(browser, cache_dir: Path, max_pages: int | None) -> list[dict]:
    ckpt = cache_dir / "list.json"
    if ckpt.exists():
        data = json.loads(ckpt.read_text())
        if data.get("complete"):
            log(f"List: using cached complete list ({len(data['rows'])} rows)")
            return data["rows"]
    ctx = await browser.new_context(user_agent=UA)
    page = await ctx.new_page()
    await page.route("**/*.{png,svg,woff2,jpg,ico}", lambda r: r.abort())
    await page.goto(BASE + "/", wait_until="networkidle", timeout=90000)
    await page.get_by_text("Search Projects").last.click()
    await page.wait_for_selector("text=/Showing Page 1 of/", timeout=60000)
    body = await page.inner_text("body")
    m = re.search(r"Results: (\d+) Found - Showing Page 1 of (\d+)", body)
    if not m:
        raise RuntimeError("could not read total results / pages from search page")
    total_results, total_pages = int(m.group(1)), int(m.group(2))
    log(f"List: {total_results} projects over {total_pages} pages")
    if max_pages:
        total_pages = min(total_pages, max_pages)
    rows, empty = [], 0
    pg = 1
    while True:
        items = parse_list(await page.content())
        if not items:
            empty += 1
            log(f"  page {pg}: 0 items ({empty}/{MAX_CONSECUTIVE_EMPTY}); continuing")
            if empty >= MAX_CONSECUTIVE_EMPTY:
                raise RuntimeError(f"{empty} consecutive empty list pages at page {pg}; not truncating silently")
        else:
            empty = 0
            rows += items
        if pg % 50 == 0:
            log(f"  list page {pg}/{total_pages}: {len(rows)} rows")
            ckpt.write_text(json.dumps({"complete": False, "rows": rows}))
        if pg >= total_pages:
            break
        for attempt in range(3):
            try:
                await page.get_by_label("Next page").first.click()
                await page.wait_for_selector(f"text=/Showing Page {pg + 1} of/", timeout=30000)
                break
            except Exception as e:  # noqa: BLE001
                log(f"  next-page click to {pg + 1} failed ({e.__class__.__name__}); retry {attempt + 1}")
                if attempt == 2:
                    raise
        pg += 1
    await ctx.close()
    uniq = {r["project_id"]: r for r in rows}
    log(f"List: {len(rows)} rows parsed, {len(uniq)} unique project ids (site reports {total_results})")
    if not max_pages and len(uniq) < total_results * 0.99:
        raise RuntimeError(f"list crawl short: {len(uniq)} of {total_results}")
    out = list(uniq.values())
    ckpt.write_text(json.dumps({"complete": not max_pages, "rows": out}))
    return out


# -------------------------------------------------------------- detail crawl
def parse_detail(page_html: str) -> dict:
    t = re.sub(r"<script.*?</script>", "", page_html, flags=re.S)
    main = re.search(r"<main.*?</main>", t, re.S)
    t = main.group(0) if main else t
    title = re.search(r"<title>(.*?)</title>", page_html, re.S)
    sections = {}
    parts = re.split(r'<h2 class="govuk-heading-l[^"]*">(.*?)</h2>', t)
    for i in range(1, len(parts) - 1, 2):
        sections[text(parts[i])] = parts[i + 1]

    def lis(sec):
        return [x for x in (text(li) for li in re.findall(r"<li[^>]*>(.*?)</li>", sections.get(sec) or "", re.S)) if x]

    tsc = sections.get("Time-Scale and Cost") or ""
    frm = re.search(r"From:</span>\s*<span[^>]*>(.*?)</span>", tsc, re.S)
    to = re.search(r"To:</span>\s*<span[^>]*>(.*?)</span>", tsc, re.S)
    cost = re.search(r"Cost:</span>\s*<span[^>]*>(.*?)</span>", tsc, re.S)
    docs = re.findall(r'title="([^"]+)"', sections.get("Project Documents") or "")
    return {
        "page_title": text(title.group(1)) if title else None,
        "description": text(sections.get("Description")),
        "objective": text(sections.get("Objective")),
        "from_year": text(frm.group(1)) if frm else None,
        "to_year": text(to.group(1)) if to else None,
        "detail_cost_text": text(cost.group(1)) if cost else None,
        "contractors": lis("Contractor / Funded Organisations"),
        "keywords": lis("Keywords"),
        "fields_of_study": lis("Fields of Study"),
        "n_documents": len(docs),
        "sections": sorted(k for k in sections if k),
    }


async def detail_worker(wid, browser, queue, cache: Path, stats):
    ctx = await browser.new_context(user_agent=UA)
    page = await ctx.new_page()
    await page.route("**/*.{png,svg,woff2,jpg,ico,css}", lambda r: r.abort())
    while True:
        try:
            pid = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        f = cache / f"{pid}.json"
        ok = False
        for attempt in range(3):
            try:
                await page.goto(f"{BASE}/ProjectDetails?ProjectId={pid}", wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_selector("h2:has-text('Time-Scale and Cost')", timeout=25000)
                rec = parse_detail(await page.content())
                f.write_text(json.dumps(rec, ensure_ascii=False))
                ok = True
                break
            except Exception as e:  # noqa: BLE001
                if attempt == 2:
                    log(f"  [w{wid}] {pid}: failed after 3 attempts ({e.__class__.__name__})")
                await asyncio.sleep(2 * (attempt + 1))
        stats["done"] += 1
        stats["failed" if not ok else "ok"] += 1
        if stats["done"] % 250 == 0:
            el = (datetime.now() - stats["t0"]).total_seconds()
            rate = stats["done"] / el
            eta = (stats["total"] - stats["done"]) / rate / 60 if rate else 0
            log(f"  detail {stats['done']}/{stats['total']} ({stats['failed']} failed) - {rate:.1f}/s, ETA {eta:.0f} min")
    await ctx.close()


async def crawl_details(browser, ids: list[str], cache: Path, workers: int) -> None:
    cache.mkdir(parents=True, exist_ok=True)
    todo = [i for i in ids if not (cache / f"{i}.json").exists()]
    log(f"Details: {len(ids) - len(todo)} cached, {len(todo)} to fetch with {workers} workers")
    q = asyncio.Queue()
    for i in todo:
        q.put_nowait(i)
    stats = {"done": 0, "ok": 0, "failed": 0, "total": len(todo), "t0": datetime.now()}
    await asyncio.gather(*(detail_worker(w, browser, q, cache, stats) for w in range(workers)))
    log(f"Details: fetched {stats['ok']}, failed {stats['failed']}")


# --------------------------------------------------------------------- build
def gbp(s: str | None) -> float | None:
    digits = re.sub(r"[^\d.]", "", s or "")
    try:
        v = float(digits) if digits else None
    except ValueError:
        return None
    return v if v else None  # "£0" = cost not recorded -> NULL


def month_start(m: str | None) -> str | None:
    return f"{m}-01" if m else None


def month_end(m: str | None) -> str | None:
    if not m:
        return None
    y, mo = int(m[:4]), int(m[5:7])
    return f"{m}-{calendar.monthrange(y, mo)[1]:02d}"


async def run(args) -> list[dict]:
    from playwright.async_api import async_playwright
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as pw:
        kw = {"headless": True}
        if args.chrome_path:
            kw["executable_path"] = args.chrome_path
        browser = await pw.chromium.launch(**kw)
        rows = await crawl_list(browser, args.cache_dir, args.max_list_pages)
        if args.limit:
            rows = rows[: args.limit]
        if not args.skip_details:
            await crawl_details(browser, [r["project_id"] for r in rows], args.cache_dir / "detail", args.workers)
        await browser.close()
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description="Defra Science Search -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only fetch details for / emit the first N projects")
    p.add_argument("--max-list-pages", type=int, default=None, help="smoke: crawl only N list pages")
    p.add_argument("--workers", type=int, default=6)
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=Path("defra_cache"))
    p.add_argument("--chrome-path", default=DEFAULT_CHROME, help="empty string = Playwright bundled Chromium")
    p.add_argument("--skip-details", action="store_true")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    rows = asyncio.run(run(args))
    dcache = args.cache_dir / "detail"
    out, missing = [], 0
    for r in rows:
        f = dcache / f"{r['project_id']}.json"
        d = json.loads(f.read_text()) if f.exists() else {}
        if not d:
            missing += 1
        contractors = d.get("contractors") or []
        desc = d.get("description")
        obj = d.get("objective")
        out.append({
            "project_code": r["project_code"],
            "project_id": r["project_id"],
            "title": r["list_title"],
            "description": desc or obj,
            "objective": obj,
            "start_month": r["start_month"],
            "end_month": r["end_month"],
            "start_date": month_start(r["start_month"]),
            "end_date": month_end(r["end_month"]),
            "from_year": d.get("from_year"),
            "to_year": d.get("to_year"),
            "cost_text": r["list_cost_text"],
            "amount": gbp(r["list_cost_text"]),
            "currency": "GBP",
            "lead_contractor": contractors[0] if contractors else None,
            "contractors": json.dumps(contractors, ensure_ascii=False),
            "n_contractors": len(contractors),
            "keywords": json.dumps(d.get("keywords") or [], ensure_ascii=False),
            "fields_of_study": json.dumps(d.get("fields_of_study") or [], ensure_ascii=False),
            "n_documents": d.get("n_documents"),
            "has_detail": bool(d),
            "landing_page_url": f"{BASE}/ProjectDetails?ProjectId={r['project_id']}",
        })
    df = pd.DataFrame(out)
    log(f"Built {len(df)} rows ({missing} without detail page)")
    blank = df["project_code"].fillna("").str.strip() == ""
    if blank.any():
        log(f"  dropping {blank.sum()} rows with blank project code")
        df = df[~blank]
    dup = df["project_code"].str.strip().str.lower().duplicated(keep=False)
    if dup.any():
        # Same code listed under two ProjectIds. Keep the one with a detail page
        # and the most populated fields; log the rest.
        log(f"  {dup.sum()} rows share a project code: {sorted(df.loc[dup, 'project_code'].unique())[:30]}")
        df["_score"] = df.notna().sum(axis=1) + df["has_detail"].astype(int) * 10
        df = df.sort_values(["_score", "project_id"], ascending=[False, True])
        df["_k"] = df["project_code"].str.strip().str.lower()
        df = df.drop_duplicates(subset=["_k"], keep="first").drop(columns=["_score", "_k"])
    df["_k"] = df["project_code"].str.strip().str.lower()
    if df["_k"].duplicated().any():
        raise SystemExit("duplicate funder_award_id after dedup")
    df = df.drop(columns="_k").sort_values("project_code").reset_index(drop=True)
    for c in ["title", "description", "start_date", "end_date", "amount", "lead_contractor"]:
        log(f"  {c:16s} {df[c].notna().mean():6.1%}")
    log(f"  total amount GBP {df['amount'].sum():,.0f}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "defra_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_defra_projects.parquet"
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
