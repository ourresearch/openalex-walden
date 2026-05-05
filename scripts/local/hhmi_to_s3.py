#!/usr/bin/env python3
"""
HHMI to S3 Data Pipeline (agent-browser scrape)
================================================

HHMI publishes per-investigator profile pages at
https://www.hhmi.org/scientists/{slug}, but their site uses Shadow DOM —
plain HTTP requests return an empty shell. agent-browser is required.

Pipeline:
  Phase 1: read /sitemap.xml to enumerate all scientist URLs (~1,739 of
           them as of 2026-05, including emeriti).
  Phase 2: for each URL, drive agent-browser:
             open URL → eval shadow-DOM-aware extraction JS → parse JSON.
           Page title is "Name | Role | Term", e.g.
             "David Haussler, PhD | Investigator Emeriti | 2000-2022".
           That gives us name, role, and start/end year. Inside the page
           we also harvest Institution, Scientific Disciplines, and any
           bio paragraph(s).
  Phase 3: write parquet, upload to S3.

HHMI does NOT disclose per-investigator funding amounts. Currency is set
to "USD" but amount is NULL — flagged in the Step 6.7 verification block
of the notebook with an explicit waiver.

Prerequisites:
    npm i -g agent-browser && agent-browser install
    pip install pandas pyarrow requests boto3

Output: s3://openalex-ingest/awards/hhmi/hhmi_investigators.parquet
"""

import argparse
import json
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen

import pandas as pd
import requests

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/hhmi/hhmi_investigators.parquet"

# Shadow-DOM-aware extraction. Returns a JSON string.
EXTRACT_JS = r"""
(function() {
  function shadowAll(sel, root) {
    root = root || document;
    const out = [];
    function walk(node) {
      if (node.querySelectorAll) for (const el of node.querySelectorAll(sel)) out.push(el);
      const all = node.querySelectorAll ? node.querySelectorAll('*') : [];
      for (const el of all) if (el.shadowRoot) walk(el.shadowRoot);
    }
    walk(root);
    return out;
  }
  function listAfter(h) {
    const parent = h.parentElement;
    if (!parent) return [];
    return Array.from(parent.querySelectorAll('a, li')).map(a => a.textContent.trim()).filter(Boolean);
  }

  const r = {};
  // Title parsing — "Name | Role | YYYY-YYYY"
  r.title = document.title;
  r.url = location.href;
  const titleParts = (document.title || '').split('|').map(s => s.trim());
  r.name_short = titleParts[0] || null;
  r.role = titleParts[1] || null;
  r.term = titleParts[2] || null;

  // Term parsing
  const termMatch = (r.term || '').match(/^(\d{4})\s*-\s*(\d{4}|present)?/i);
  if (termMatch) {
    r.start_year = parseInt(termMatch[1], 10);
    const e = termMatch[2];
    r.end_year = (!e || /present/i.test(e)) ? null : parseInt(e, 10);
  } else {
    r.start_year = null;
    r.end_year = null;
  }

  // h1 (name with degree, e.g. "David Haussler, PhD")
  const h1s = shadowAll('h1');
  r.name_full = h1s[0] ? h1s[0].textContent.trim() : null;

  // Sections under h6 headings
  const h6s = shadowAll('h6');
  const sections = {};
  for (const h of h6s) sections[h.textContent.trim()] = Array.from(new Set(listAfter(h)));
  r.institution = (sections['Institution'] || []).find(t => t && t !== 'Institution') || null;
  r.disciplines = (sections['Scientific Disciplines'] || []).filter(t => t && t !== 'Scientific Disciplines');

  // Bio paragraphs (filter chrome)
  const ps = shadowAll('p').map(p => p.textContent.trim())
    .filter(t => t.length > 80 && !/cookie|Privacy Policy/i.test(t));
  r.bio = ps.slice(0, 5).join(' ').replace(/\s+/g, ' ').slice(0, 3000);

  return JSON.stringify(r);
})();
"""


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------- agent-browser drivers ------------------------------------------

def ab(args: list[str], stdin_text: str | None = None, timeout: int = 60) -> str:
    """Run `npx agent-browser <args>` and return stdout.

    Uses shell=True on Windows so the shim `.cmd` resolution works without
    hardcoding a path. On POSIX shell=True still works fine.
    """
    cmd = "npx agent-browser " + " ".join(shlex.quote(a) for a in args)
    p = subprocess.run(
        cmd,
        input=stdin_text,
        capture_output=True,
        text=True,
        timeout=timeout,
        encoding="utf-8",
        errors="replace",
        shell=True,
    )
    if p.returncode != 0:
        raise RuntimeError(f"agent-browser failed ({p.returncode}): {p.stderr.strip() or p.stdout.strip()}")
    return p.stdout.strip()


def ab_open(url: str) -> None:
    ab(["open", url], timeout=90)


def ab_get_title() -> str:
    return ab(["get", "title"])


def ab_eval_extract() -> dict:
    """Run extraction JS and parse the JSON output.

    `agent-browser eval` prints the result with surrounding quotes; the JSON
    payload is double-encoded as a string because we return JSON.stringify(...).
    """
    raw = ab(["eval", "--stdin"], stdin_text=EXTRACT_JS)
    # The CLI prints the JS return value: '"...escaped JSON..."'
    # Strip outer quotes and unescape
    if raw.startswith('"') and raw.endswith('"'):
        try:
            inner = json.loads(raw)  # decode the outer string
            if isinstance(inner, str):
                return json.loads(inner)
            return inner
        except Exception as e:
            raise RuntimeError(f"failed to decode eval output: {e!r}; raw={raw[:200]!r}")
    return json.loads(raw)


# ---------- sitemap ----------------------------------------------------------

def fetch_scientist_urls() -> list[str]:
    """Pull all /scientists/* URLs from HHMI's sitemap (paginated)."""
    log("Fetching HHMI sitemap")
    headers = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
    r = requests.get("https://www.hhmi.org/sitemap.xml", headers=headers, timeout=30, verify=False)
    r.raise_for_status()
    child_sitemaps = re.findall(r"<loc>([^<]+)</loc>", r.text)
    all_urls: list[str] = []
    for sm in child_sitemaps:
        rr = requests.get(sm, headers=headers, timeout=30, verify=False)
        urls = re.findall(r"<loc>([^<]+)</loc>", rr.text)
        scientist_urls = [u for u in urls if "/scientists/" in u]
        log(f"  {sm.rsplit('/', 1)[-1]}: {len(scientist_urls)} scientist URLs")
        all_urls.extend(scientist_urls)
    # Dedup, preserve order
    seen = set()
    out = []
    for u in all_urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    log(f"Total unique scientist URLs: {len(out):,}")
    return out


# ---------- main loop --------------------------------------------------------

def scrape_one(url: str, retries: int = 2) -> dict | None:
    last_err = None
    for attempt in range(retries + 1):
        try:
            ab_open(url)
            # Give Shadow DOM time to render
            time.sleep(1.5)
            data = ab_eval_extract()
            data["url"] = url
            data["downloaded_at"] = datetime.utcnow().isoformat()
            return data
        except Exception as e:
            last_err = e
            time.sleep(2 ** attempt)
    log(f"  ✗ FAILED {url}: {last_err}")
    return None


def main() -> None:
    p = argparse.ArgumentParser(description="HHMI scientists -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None,
                   help="For smoke-test: only process first N URLs")
    p.add_argument("--checkpoint-every", type=int, default=50,
                   help="Save partial parquet every N records (recoverable interruption)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--skip-upload", action="store_true")
    args = p.parse_args()

    # Suppress urllib3 warnings (sitemap fetch)
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    log("=" * 60)
    log("HHMI -> S3 pipeline starting (agent-browser)")

    urls = fetch_scientist_urls()
    if args.limit:
        urls = urls[: args.limit]
        log(f"Smoke-test mode: limited to first {len(urls)} URLs")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "hhmi_investigators.parquet"

    rows: list[dict] = []
    for i, url in enumerate(urls, 1):
        rec = scrape_one(url)
        if rec:
            rows.append(rec)
        if i == 1 or i % 25 == 0 or i == len(urls):
            log(f"[{i}/{len(urls)}] cumulative ok: {len(rows)} (failed: {i - len(rows)})")
        if rows and len(rows) % args.checkpoint_every == 0:
            pd.DataFrame(rows).to_parquet(parquet_path, index=False)
            log(f"  checkpoint: {len(rows)} rows written")

    df = pd.DataFrame(rows)
    log(f"DataFrame shape: {df.shape}")
    if not df.empty:
        log(f"Coverage:")
        log(f"  name_full: {df.name_full.notna().sum()}")
        log(f"  institution: {df.institution.notna().sum()}")
        log(f"  start_year: {df.start_year.notna().sum()}")
        log(f"  bio non-empty: {(df.bio.fillna('').str.len() > 0).sum() if 'bio' in df else 0}")

    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {parquet_path}")

    # Tidy up the browser session
    try:
        ab(["close"])
    except Exception:
        pass

    if args.skip_upload:
        log("--skip-upload set; done.")
        return

    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    import boto3
    s3 = boto3.client("s3")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


if __name__ == "__main__":
    main()
