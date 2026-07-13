#!/usr/bin/env python3
"""Generic static-HTML listing engine for the CN provincial framework.

The W2 pilot provinces (Shandong, Jiangsu) run Hanweb "大汉版通" CMSes whose
notice lists are enumerated via the dataproxy.jsp POST proxy (common.py).
The W2-A scale-up provinces (Guangdong, Shanghai, Hunan, Beijing, Hebei,
Hubei) all run OTHER CMSes (TRS, wdit, gov.cn static publishers) whose notice
columns are plain paginated static HTML:

    page 1:  {col}/index.html            (or a curated single topic page)
    page N:  {col}/index_{N}.html        (TRS / gov.cn convention)
             {col}/{token}-{N}.html      (Hebei convention)

This module extends the framework with an `HtmlListing` column type and a
`run_province_html()` entry point that mirrors `common.run_province()` --
same CLI contract (--limit / --skip-upload / --output-dir / --allow-shrink /
--skip-listing), same checkpointing, and it reuses ALL of common.py's
machinery (article fetch, attachment download, xls/xlsx/pdf/docx header-driven
parsing, §1.4 shrink-checked upload). Only the listing enumeration differs.

It deliberately does NOT modify common.py or the pilot configs -- append-only
extension so parallel agents' changes merge cleanly.

Termination rules (runbook §1 "empty page ≠ end of corpus"):
- `max_pages` is the authoritative loop terminator. It is read from the
  column's own pagination widget (e.g. "1/114", createPageHTML(...,25,...))
  and stored in the config after manual verification -- the analogue of
  Hanweb's <totalrecord>.
- Each page is retried up to 3 times on non-200 / exception before being
  logged and skipped; >=5 consecutive failed pages raises (never silently
  truncates).
- Pages that parse to 0 articles are logged and the loop continues to
  max_pages (a mid-archive empty page is a flake, not EOF).
"""

from __future__ import annotations

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
import sys
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass
# (common.py installs the win32 open()/Path UTF-8 monkey-patch on import.)

import argparse
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd
import requests

from . import common
from .common import (
    ProvinceConfig,
    article_url,
    make_session,
    parse_attachment,
    process_article,
    rows_to_records,
    select_award_articles,
    write_and_upload,
)


@dataclass
class HtmlListing:
    """One paginated static-HTML notice column.

    `page_url(n)` maps a 1-based page number to the page URL. `max_pages` is
    the column's page count as shown by its own pagination widget (verify by
    loading page 1 in a browser / curl before writing the config; it is the
    loop terminator, so a too-small value silently truncates the archive --
    prefer rounding UP, trailing 404/empty pages are just logged).
    `article_href` filters anchors to real article links (each CMS has a
    distinctive article path shape).
    """
    page_url: Callable[[int], str]
    max_pages: int
    article_href: str = r"\.s?html?"
    encoding: str = "utf-8"
    referer: Optional[str] = None

    def extract(self, html: str, base_url: str) -> list[dict]:
        from urllib.parse import urljoin
        pat = re.compile(self.article_href)
        out, seen = [], set()
        body = re.sub(r"<script.*?</script>", "", html, flags=re.S)
        for m in re.finditer(r'<a([^>]+)>(.*?)</a>', body, re.S):
            attrs, inner = m.group(1), m.group(2)
            hm = re.search(r'href="([^"]+)"', attrs)
            if not hm:
                continue
            href = hm.group(1).strip()
            if not pat.search(href):
                continue
            tm = re.search(r'title="([^"]+)"', attrs)
            title = (tm.group(1) if tm else re.sub(r"<[^>]+>", "", inner)).strip()
            title = re.sub(r"\s+", " ", title)
            if not title:
                continue
            full = urljoin(base_url, href)
            if full in seen:
                continue
            seen.add(full)
            # date: look right after the anchor for yyyy-mm-dd, else in the URL.
            tail = body[m.end():m.end() + 160]
            dm = (re.search(r"(\d{4}-\d{2}-\d{2})", tail)
                  or re.search(r"[/_t](20\d{2})(\d{2})(\d{2})", href))
            if dm and len(dm.groups()) == 3:
                date = "-".join(dm.groups())
            else:
                date = dm.group(1) if dm else None
            out.append({"url": full, "title": title, "date": date})
        return out


@dataclass
class HtmlProvinceConfig(ProvinceConfig):
    """ProvinceConfig for static-HTML portals: adds html_listings.

    `listing_columns` (the Hanweb field) stays empty for these provinces.

    `attachment_resolver`, when set, replaces common.find_attachment_links for
    finding roster files -- signature (article_html, article_url, base_url) ->
    [(file_url, display_name)]. Used by Guangdong, whose live server drops the
    roster PDF after the publicity window, so the resolver recovers it from the
    Internet Archive (extras.resolve_via_wayback).
    """
    html_listings: list[HtmlListing] = field(default_factory=list)
    attachment_resolver: Optional[Callable[[str, str, str], list[tuple[str, str]]]] = None


def _process_article_resolved(cfg: HtmlProvinceConfig, art: dict,
                              sess: requests.Session, work_dir: Path) -> list[dict]:
    """Like common.process_article but uses cfg.attachment_resolver.

    (Only invoked when cfg.attachment_resolver is set; otherwise
    common.process_article is used unchanged.)
    """
    url = article_url(cfg, art["url"])
    try:
        r = sess.get(url, timeout=45)
        r.raise_for_status()
    except Exception as exc:
        print(f"  [WARN] article fetch failed {url}: {exc}")
        return []
    html = r.content.decode("utf-8", "replace")
    links = cfg.attachment_resolver(html, url, cfg.base_url)
    records: list[dict] = []
    adir = work_dir / "attachments"
    adir.mkdir(parents=True, exist_ok=True)
    for i, (full, text) in enumerate(links):
        m = re.search(r"\.(xlsx?|pdf|docx?|et|wps)", (text + " " + full), re.I)
        ext = "." + m.group(1).lower() if m else ""
        stem = re.sub(r"[^\w.]", "_", full.rsplit("/", 1)[-1])[:50]
        local = adir / f"{art['url'].rsplit('/', 1)[-1]}__{i}_{stem}{ext}"
        if not local.exists():
            try:
                fr = sess.get(full, timeout=120)
                local.write_bytes(fr.content)
                time.sleep(cfg.request_pause)
            except Exception as exc:
                print(f"    [WARN] attachment download failed {full}: {exc}")
                continue
        # NB: call via the module attribute (not the import-time binding) so an
        # extras.enable_legacy_doc_parsing() monkey-patch of common.parse_attachment
        # is honored here too.
        for t in common.parse_attachment(local):
            records += rows_to_records(t, cfg, source_file=text or local.name,
                                       landing_page=url, article_title=art["title"])
    print(f"  {art.get('date', '?')} | {art['title'][:44]} | {len(links)} attach -> {len(records)} rows")
    return records


def enumerate_html_listing(cfg: ProvinceConfig, lst: HtmlListing,
                           sess: requests.Session,
                           max_pages: Optional[int] = None) -> list[dict]:
    """Fetch pages 1..max_pages of one column; return [{url,title,date}]."""
    headers = {"Referer": lst.referer} if lst.referer else {}
    records: list[dict] = []
    seen: set[str] = set()
    n_pages = max_pages or lst.max_pages
    consec_fail = 0
    for page in range(1, n_pages + 1):
        url = lst.page_url(page)
        html = None
        for attempt in range(3):
            try:
                r = sess.get(url, headers=headers, timeout=30)
            except Exception as exc:
                print(f"  page {page}: EXC {exc} (attempt {attempt + 1}/3)")
                time.sleep(4)
                continue
            if r.status_code == 200:
                html = r.content.decode(lst.encoding, "replace")
                break
            print(f"  page {page}: HTTP {r.status_code} (attempt {attempt + 1}/3)")
            time.sleep(4)
        if html is None:
            consec_fail += 1
            print(f"  page {page}: FAILED after retries ({consec_fail}/5 consecutive)")
            if consec_fail >= 5:
                raise RuntimeError(f"{consec_fail} consecutive failed listing pages at {url}")
            continue
        consec_fail = 0
        items = lst.extract(html, cfg.base_url)
        fresh = [it for it in items if it["url"] not in seen]
        for it in fresh:
            seen.add(it["url"])
        records += fresh
        print(f"  page {page}/{n_pages}: {len(fresh)} articles (cum {len(records)})")
        if not fresh:
            # Mid-archive empty page = probably a soft-404 / flake; max_pages
            # terminates the loop, so just log and continue (runbook §1).
            print(f"    (page {page} yielded 0 new articles; continuing to max_pages)")
        time.sleep(cfg.request_pause)
    return records


def run_province_html(cfg: HtmlProvinceConfig, argv: Optional[list[str]] = None) -> None:
    """Mirror of common.run_province() for static-HTML portals."""
    parser = argparse.ArgumentParser(description=f"{cfg.funder_display_name} awards -> S3")
    parser.add_argument("--output-dir", type=Path, default=Path(f"/tmp/{cfg.provenance}"))
    parser.add_argument("--limit", type=int, default=None,
                        help="Max award-articles to process (smoke testing)")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    parser.add_argument("--skip-listing", action="store_true",
                        help="Reuse cached listing.json / articles from output-dir")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Override every listing's max_pages (smoke testing)")
    args = parser.parse_args(argv)

    wd = args.output_dir
    wd.mkdir(parents=True, exist_ok=True)
    print("=" * 64)
    print(f"{cfg.funder_display_name}  (F{cfg.funder_id})")
    print(f"  provenance: {cfg.provenance}")
    print(f"  base:       {cfg.base_url}")
    print(f"  started:    {datetime.now(timezone.utc).isoformat()}")
    print("=" * 64)

    sess = make_session(cfg)

    # 1. Listing
    listing_cache = wd / "listing.json"
    if args.skip_listing and listing_cache.exists():
        all_records = json.loads(listing_cache.read_text())
        print(f"[cache] {len(all_records)} listing records")
    else:
        all_records = []
        for lst in cfg.html_listings:
            all_records += enumerate_html_listing(cfg, lst, sess, args.max_pages)
        seen, dedup = set(), []
        for r in all_records:
            if r["url"] not in seen:
                seen.add(r["url"])
                dedup.append(r)
        all_records = dedup
        listing_cache.write_text(json.dumps(all_records, ensure_ascii=False, indent=1))
        print(f"[OK] {len(all_records)} listing records -> {listing_cache}")

    # 2. Filter to award lists
    awards = select_award_articles(cfg, all_records)
    print(f"[OK] {len(awards)} award-list articles match config pattern")
    if args.limit:
        awards = awards[:args.limit]
        print(f"    (--limit {args.limit})")

    # 3. Process with checkpointing (same format as common.run_province)
    ckpt = wd / "records.checkpoint.jsonl"
    done_urls: set[str] = set()
    if ckpt.exists():
        for line in ckpt.read_text().splitlines():
            if line.strip():
                done_urls.add(json.loads(line)["_article_url"])
        print(f"[resume] {len(done_urls)} articles already checkpointed")
    _proc = (_process_article_resolved if getattr(cfg, "attachment_resolver", None)
             else process_article)
    with open(ckpt, "a", encoding="utf-8") as fh:
        for art in awards:
            if art["url"] in done_urls:
                continue
            recs = _proc(cfg, art, sess, wd)
            for rec in recs:
                rec["_article_url"] = art["url"]
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            if not recs:
                fh.write(json.dumps({"_article_url": art["url"], "_empty": True},
                                    ensure_ascii=False) + "\n")
            fh.flush()

    # 4. Assemble dataframe
    rows = []
    for line in ckpt.read_text().splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        if obj.get("_empty"):
            continue
        obj.pop("_article_url", None)
        rows.append(obj)
    if not rows:
        print("[ERROR] no records extracted; check config pattern / parsing.")
        raise SystemExit(1)
    df = pd.DataFrame(rows).drop_duplicates(
        subset=["display_name", "lead_family_name", "institution", "provenance"])
    print(f"[OK] {len(df):,} unique project rows after dedup")

    write_and_upload(cfg, df, args.output_dir, args.skip_upload, args.allow_shrink)
