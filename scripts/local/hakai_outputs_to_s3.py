"""hakai_outputs_to_s3.py — OUTPUT-LIST pattern scraper (how-to-add-a-funder-v2 §11).

Scrapes the Hakai Institute's own published list of funded publications
(https://hakai.org/publications) and writes one row per funder-asserted DOI to a
parquet that the CreateHakaiWorkFunders notebook resolves into work.funders.

This is NOT the Tula analysis corpus (that is search-discovered + LLM-verified, for
impact studies). Per §11's eligibility gate, the OUTPUT-LIST source must be the funder's
OWN published assertion — exactly what hakai.org/publications is.

DOI cleaning is done HERE in Python (strip trailing punctuation, lowercase, canonical
https://doi.org/ form) so the notebook can do a plain join — no SQL regex, no
escapedStringLiterals backslash hazard. Mirrors the scrape/extract logic proven in
plans/awards/examples/tula-hakai-funder-impact/13_precision_recall.py::scrape_hakai_dois.

S3 target: s3://openalex-ingest/awards/hakai/hakai_outputs.parquet
"""

# --- Windows UTF-8 compatibility shim (no-op on Linux/Databricks) -----------------
import sys
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass
if sys.platform == "win32":
    import builtins
    from pathlib import Path as _Path
    _orig_open = builtins.open

    def _utf8_open(file, mode="r", *a, **k):
        if "b" not in mode and k.get("encoding") is None:
            k["encoding"] = "utf-8"
        return _orig_open(file, mode, *a, **k)

    builtins.open = _utf8_open
    _orig_wt, _orig_rt = _Path.write_text, _Path.read_text
    _Path.write_text = lambda self, data, encoding="utf-8", *a, **k: _orig_wt(self, data, encoding=encoding, *a, **k)
    _Path.read_text = lambda self, encoding="utf-8", *a, **k: _orig_rt(self, encoding=encoding, *a, **k)
# ----------------------------------------------------------------------------------

import argparse
import re
import time
from pathlib import Path

import pandas as pd
import requests

# Hakai Institute (funder) = F4320334031. Tula Foundation (parent) = F4320315065.
# The publications list is published by Hakai, so the assertion attaches to Hakai.
# (If we later decide the parent should also be asserted, add a Tula row per DOI.)
FUNDER_ID = "4320334031"
PROVENANCE = "hakai_publications"
BASE_URL = "https://hakai.org/publications"

DOI_RE = re.compile(r'10\.\d{4,9}/[^\s"\'<>)]+', re.I)
HREF_RE = re.compile(r'href="https://doi\.org/(10\.\d{4,9}/[^"]+)"', re.I)


def clean_doi(raw: str) -> str:
    """Normalize to a canonical lowercase https://doi.org/<doi> form.

    Strips trailing punctuation the text-citation form sometimes glues on. Garbled
    DOIs that survive cleaning simply fail to match openalex_works.doi in the notebook
    (harmless — they drop out), the same way the Tula recall script handles them.
    """
    d = re.sub(r'[.,;:]+$', '', raw.strip()).lower()
    return f"https://doi.org/{d}"


def scrape(max_pages: int, mailto: str) -> set:
    ua = {"User-Agent": f"openalex-awards (mailto:{mailto})"}
    dois, consecutive_empty = set(), 0
    for page in range(1, max_pages + 1):
        try:
            r = requests.get(BASE_URL, params={"page": page}, headers=ua, timeout=30)
        except requests.RequestException as e:
            print(f"[page {page}] request error: {e} — continuing")
            consecutive_empty += 1
            if consecutive_empty >= 3:
                print(f"[page {page}] 3 consecutive failures — assuming end of list")
                break
            continue
        found = {clean_doi(d) for d in DOI_RE.findall(r.text)}
        found |= {clean_doi(d) for d in HREF_RE.findall(r.text)}
        if not found:
            consecutive_empty += 1
            print(f"[page {page}] HTTP {r.status_code}, 0 DOIs ({consecutive_empty}/3)")
            if consecutive_empty >= 3:
                print(f"[page {page}] 3 consecutive empty pages — end of list")
                break
            time.sleep(1)
            continue
        consecutive_empty = 0
        before = len(dois)
        dois |= found
        print(f"[page {page}] HTTP {r.status_code}, +{len(dois) - before} new DOIs "
              f"(running total {len(dois)})")
        time.sleep(1)  # politeness
    return dois


def main() -> None:
    ap = argparse.ArgumentParser(description="Scrape Hakai's funded-publications list to parquet.")
    ap.add_argument("--output-dir", default=".", help="Directory to write hakai_outputs.parquet")
    ap.add_argument("--max-pages", type=int, default=80, help="Page-loop upper bound (~66 expected)")
    ap.add_argument("--limit", type=int, default=None, help="Truncate to N DOIs (smoke test)")
    ap.add_argument("--mailto", default="kyle@ourresearch.org", help="Polite-pool contact")
    ap.add_argument("--skip-upload", action="store_true", help="Write local parquet only")
    args = ap.parse_args()

    dois = sorted(scrape(args.max_pages, args.mailto))
    if args.limit:
        dois = dois[: args.limit]
    if not dois:
        raise RuntimeError("Scrape produced 0 DOIs — page structure may have changed; aborting.")
    print(f"Scraped {len(dois)} distinct DOIs from Hakai's publications list.")

    df = pd.DataFrame({
        "doi": dois,
        "funder_id": FUNDER_ID,
        "provenance": PROVENANCE,
    }).astype("string")

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "hakai_outputs.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Wrote {len(df)} rows -> {out_path}")

    s3_uri = "s3://openalex-ingest/awards/hakai/hakai_outputs.parquet"
    if args.skip_upload:
        print(f"--skip-upload set. Upload manually: aws s3 cp {out_path} {s3_uri}")
        return
    try:
        import boto3
        boto3.client("s3").upload_file(str(out_path), "openalex-ingest",
                                       "awards/hakai/hakai_outputs.parquet")
        print(f"Uploaded -> {s3_uri}")
    except Exception as e:  # noqa: BLE001 - degrade gracefully when creds/boto3 absent
        print(f"Upload skipped ({e}). Upload manually: aws s3 cp {out_path} {s3_uri}")


if __name__ == "__main__":
    main()
