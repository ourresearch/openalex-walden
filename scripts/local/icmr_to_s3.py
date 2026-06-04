#!/usr/bin/env python3
"""
Indian Council of Medical Research (ICMR) — HMSC Approved Projects -> S3
=======================================================================

Downloads ICMR's published "List of Approved Projects" PDFs and writes a
project-level parquet for the OpenAlex awards pipeline.

Source authority
----------------
ICMR has NO JSON/REST API and publishes no Excel/CSV grant lists. Its
machine-reachable structured grant data lives in the monthly PDFs linked from

    https://www.icmr.gov.in/list-of-approved-projects

These are the "List of International Collaborative Research Projects Approved by
HMSC" (the Health Ministry's Screening Committee, administered by ICMR). Each
monthly meeting PDF lists, per project: title, Principal Investigator (name +
designation + Indian host institution), the funding/collaborating agency,
date of approval, total budget, duration and subject area.

IMPORTANT — what ICMR is in this dataset
----------------------------------------
ICMR/HMSC is the **approving body** for these projects, not (usually) the
monetary funder — the "Funding/Collaborating Agency" is frequently a foreign or
third-party agency (NIH, DBT, a foreign university) or "Self". Under OpenAlex's
broad funder model (anything that would appear in a paper's acknowledgements,
even if no money was provided), HMSC/ICMR approval is acknowledgement-worthy, so
these rows map to ICMR funder F4320320720. We deliberately do **not** ship the
"Total budget" as ICMR's `amount` (that money belongs to the collaborating
agency), so `amount`/`currency` are left NULL and the Step 6.7 amount check is
waived in the notebook. The raw budget + agency are preserved for provenance.

Scope
-----
Parses the per-month HMSC PDFs (2020–2026) plus the consolidated
"August 2017 – December 2019" PDF — all share the same per-project label
layout (two small variants: "Funding/Collaborating Agency" vs "Funded by",
labels with or without a colon), both handled here.

The large consolidated multi-year volumes (Vol_I 2000–2007 … Vol_IV 2015–2017)
are **deferred** — they are 4–5 MB / 200+ page scans on a slow host and stall
pdfplumber; tracked as a follow-up PDF tail. They are skipped by URL pattern.

Output
------
s3://openalex-ingest/awards/icmr/icmr_projects.parquet

Usage
-----
    python scripts/local/icmr_to_s3.py --limit 30 --skip-upload
    python scripts/local/icmr_to_s3.py --skip-upload
    python scripts/local/icmr_to_s3.py
    python scripts/local/icmr_to_s3.py --allow-shrink
"""

from __future__ import annotations

import argparse
import hashlib
import io
import re
import ssl
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin

import pandas as pd
import pdfplumber

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
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

    def _open_utf8(
        file, mode="r", buffering=-1, encoding=None, errors=None,
        newline=None, closefd=True, opener=None,
    ):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)

    _builtins_utf8.open = _open_utf8
# --- end shim ---


LIST_URL = "https://www.icmr.gov.in/list-of-approved-projects"
FUNDER_ID = 4320320720
PROVENANCE = "icmr_approved_projects"

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/icmr/icmr_projects.parquet"

USER_AGENT = "openalex-walden-icmr-ingest/1.0 (+https://openalex.org)"
REQUEST_TIMEOUT = 90
MAX_RETRIES = 4

DEFAULT_OUTPUT_DIR = Path("data/icmr")

# Consolidated multi-year volumes we defer (large/slow; layout drift). Skipped
# by URL substring. Tracked as a follow-up PDF tail.
DEFERRED_VOLUME_PATTERNS = ("/2000-2007/", "/2008-2012/", "/2013-2015/", "/2015-2017/", "vol_i", "vol_ii", "vol_iii", "vol_iv")

# A monthly/consolidated HMSC list anchor must look like a date or month-range.
_MONTH = r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
ANCHOR_DATE_RE = re.compile(rf"\b(?:\d{{1,2}}(?:st|nd|rd|th)?\s+)?{_MONTH}[a-z]*\.?\s*,?\s*20\d{{2}}", re.I)
# Hrefs that are clearly NOT approved-project lists.
EXCLUDE_HREF_RE = re.compile(r"eoi|call|brochure|manual|result|hindi|notice|tender|advert", re.I)

_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE


# ---------------------------------------------------------------------------
# PI name splitting (canonical helper, ported from wolf_to_s3.py per runbook
# §2.4.1 — strips trailing degree/suffix tokens; last token = family).
# ---------------------------------------------------------------------------
_HONORIFIC_RE = re.compile(
    r"^(?:Dr\.?|Prof\.?|Professor|Mr\.?|Mrs\.?|Ms\.?|Miss|Shri|Smt\.?|Col\.?|Maj\.?|Lt\.?|Brig\.?|Surg\.?|Sqn\.?|Wg\.?|Capt\.?|Sir|Dame)\s+",
    re.I,
)
_NAME_SUFFIXES = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr", "facp", "frcp"}


def split_name(name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not name:
        return None, None
    cleaned = _HONORIFIC_RE.sub("", name.strip())
    # Strip again in case of stacked honorifics ("Dr. Prof.")
    cleaned = _HONORIFIC_RE.sub("", cleaned)
    tokens = cleaned.split()
    while tokens and tokens[-1].lower().strip(",.") in _NAME_SUFFIXES:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def fetch(url: str) -> bytes:
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            return urllib.request.urlopen(req, context=_ssl_ctx, timeout=REQUEST_TIMEOUT).read()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            sleep = min(2 ** attempt, 20)
            log(f"  retry {attempt}/{MAX_RETRIES} for {url}: {exc} (sleep {sleep}s)")
            time.sleep(sleep)
    raise RuntimeError(f"GET failed after {MAX_RETRIES} attempts: {url}") from last_exc


# ---------------------------------------------------------------------------
# Source discovery
# ---------------------------------------------------------------------------
def discover_pdfs() -> list[dict[str, str]]:
    html = fetch(LIST_URL).decode("utf-8", "replace")
    pairs = re.findall(r'<a[^>]*href=["\']([^"\']+\.pdf)["\'][^>]*>(.*?)</a>', html, re.I | re.S)
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for href, raw_text in pairs:
        label = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", raw_text)).strip()
        url = urljoin(LIST_URL, href)
        url = re.sub(r"^https?://icmr\.gov\.in", "https://www.icmr.gov.in", url)
        key = url.lower()
        if key in seen:
            continue
        if EXCLUDE_HREF_RE.search(href):
            continue
        if any(p in key for p in DEFERRED_VOLUME_PATTERNS):
            continue
        # Keep anchors that name a date/month, OR sit on a known list path but
        # whose visible text is just "Download"/"View" — pair those by URL with
        # a dated sibling (already deduped by URL, so accept list-path hrefs).
        is_dated = bool(ANCHOR_DATE_RE.search(label))
        on_list_path = bool(re.search(r"list-ap-pr|approved-projects|/Static/|/Reports/", href, re.I))
        if not (is_dated or on_list_path):
            continue
        seen.add(key)
        out.append({"url": url, "label": label})
    return out


# ---------------------------------------------------------------------------
# PDF parsing
# ---------------------------------------------------------------------------
PI_HEADER_RE = re.compile(r"Principal\s+Investigator\s+(Funding/Collaborating Agency|Funded by|Funding Agency|Collaborating Agency)", re.I)
RIGHT_HEADER_TOKEN_RE = re.compile(r"^(Funding|Funded|Collaborating)", re.I)
SERIAL_RE = re.compile(r"^(\d{1,3})\.$")

DATE_RE = re.compile(
    r"Date of approval\s*:?\s*"
    r"([A-Z][a-z]+\s+\d{1,2},?\s+\d{4}"          # January 29, 2026
    r"|\d{1,2}(?:st|nd|rd|th)?\s+[A-Z][a-z]+,?\s+\d{4})",  # 29th January 2026
    re.I,
)
BUDGET_RE = re.compile(r"Total budget\s*:?\s*(.+?)(?:\s*Duration\b|\s*Subject area\b|$)", re.I)
DURATION_RE = re.compile(r"Duration\s*:?\s*(.+?)(?:\s*Subject area\b|\s*Approved\b|$)", re.I)
SUBJECT_RE = re.compile(r"Subject area\s*:?\s*(.+?)(?:\s*Approved\b|$)", re.I | re.S)
MEETING_DATE_RE = re.compile(r"meeting held on\s+(.+?)(?:\s*\(|$)", re.I)
YEAR_RE = re.compile(r"(19|20)\d{2}")

PAGE_NOISE_RE = re.compile(
    r"^(List of International"
    r"|Research Projects Approved by HMSC"          # continuation-page running header
    r"|Indian Council of Medical Research(\s+\d+)?$"  # running footer ("... Research 1")
    r"|during\b|held on\b|S\.?\s*No\.?\s*Details|S\.?\s*No\.?$|Annexure|Page\s+\d|Contd|continued|\d+$)",
    re.I,
)
LEFT_STOP_RE = re.compile(r"^(Duration|Subject area|Approved|Total budget|Date of approval)\b", re.I)
PIN_RE = re.compile(r"\b\d{6}\b")


def _group_lines(page) -> list[dict[str, Any]]:
    """Return visual lines: words grouped by y, each with per-word x positions."""
    words = page.extract_words(use_text_flow=False, keep_blank_chars=False)
    words.sort(key=lambda w: (round(w["top"]), w["x0"]))
    lines: list[dict[str, Any]] = []
    cur: list[dict] = []
    cur_top: Optional[float] = None
    for w in words:
        if cur_top is None or abs(w["top"] - cur_top) <= 3.5:
            cur.append(w)
            cur_top = w["top"] if cur_top is None else cur_top
        else:
            lines.append(_mk_line(cur))
            cur = [w]
            cur_top = w["top"]
    if cur:
        lines.append(_mk_line(cur))
    return lines


def _mk_line(ws: list[dict]) -> dict[str, Any]:
    ws = sorted(ws, key=lambda w: w["x0"])
    return {
        "words": ws,
        "text": " ".join(w["text"] for w in ws),
        "top": min(w["top"] for w in ws),
    }


def _left_right(line: dict, boundary_x: float) -> tuple[str, str]:
    left = " ".join(w["text"] for w in line["words"] if w["x0"] < boundary_x - 6)
    right = " ".join(w["text"] for w in line["words"] if w["x0"] >= boundary_x - 6)
    return left.strip(), right.strip()


def _clean(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def parse_pdf(raw: bytes, source: dict[str, str]) -> list[dict[str, Any]]:
    try:
        pdf = pdfplumber.open(io.BytesIO(raw))
    except Exception as exc:  # noqa: BLE001
        log(f"  OPEN ERROR {source['url']}: {exc}")
        return []

    # Linearize lines across all pages, dropping page-noise lines.
    lines: list[dict[str, Any]] = []
    meeting_date_raw: Optional[str] = None
    boundary_xs: list[float] = []
    for pi, page in enumerate(pdf.pages):
        page_lines = _group_lines(page)
        if pi == 0:
            joined = " ".join(pl["text"] for pl in page_lines[:6])
            m = MEETING_DATE_RE.search(joined)
            if m:
                meeting_date_raw = _clean(m.group(1))
        for pl in page_lines:
            if PI_HEADER_RE.search(pl["text"]):
                for w in pl["words"]:
                    if RIGHT_HEADER_TOKEN_RE.match(w["text"]):
                        boundary_xs.append(w["x0"])
                        break
            if PAGE_NOISE_RE.search(pl["text"]):
                continue
            lines.append(pl)
    pdf.close()

    if not boundary_xs:
        log(f"  no PI headers found in {source['url']} (unexpected layout) — 0 rows")
        return []
    boundary_x = sorted(boundary_xs)[len(boundary_xs) // 2]  # median

    header_idx = [i for i, ln in enumerate(lines) if PI_HEADER_RE.search(ln["text"])]
    if not header_idx:
        return []

    # For each project (anchored on its PI-header), locate the title block in the
    # window above the header. The serial number "N." starts the title; in the
    # 2021–2026 layout the serial sits on its own line with the title text
    # spanning the line *before* and *after* it, while in the 2017–2020 layout
    # the serial is inline with the first title line. Handle both.
    title_start_abs: list[int] = []
    title_blocks: list[list[dict]] = []
    for k, h_i in enumerate(header_idx):
        win_start = header_idx[k - 1] + 1 if k > 0 else 0
        window = lines[win_start:h_i]
        si = None
        standalone = False
        for j, ln in enumerate(window):
            if not ln["words"]:
                continue
            first = ln["words"][0]["text"]
            if SERIAL_RE.match(first):
                si = j
                standalone = (len(ln["words"]) == 1)
                break
            if re.match(r"^\d{1,3}\.\S?", first) or re.match(r"^\d{1,3}\.$", first):
                si = j
                standalone = False
                break
        if si is None:
            tb = window
            tstart_abs = win_start
        elif standalone and si - 1 >= 0:
            tb = window[si - 1:]
            tstart_abs = win_start + si - 1
        else:
            tb = window[si:]
            tstart_abs = win_start + si
        title_blocks.append(tb)
        title_start_abs.append(tstart_abs)

    records: list[dict[str, Any]] = []
    for k, h_i in enumerate(header_idx):
        field_end = title_start_abs[k + 1] if k + 1 < len(header_idx) else len(lines)
        field_lines = lines[h_i + 1:field_end]
        rec = _build_record(title_blocks[k], field_lines, boundary_x, source, meeting_date_raw)
        if rec:
            records.append(rec)
    log(f"  parsed {len(records)} projects from {source['label'][:40] or source['url']}")
    return records


def _build_record(title_lines, field_lines, boundary_x, source, meeting_date_raw) -> Optional[dict[str, Any]]:
    # Title: strip the leading serial token, join full-width text.
    title_parts: list[str] = []
    for ln in title_lines:
        txt = re.sub(r"^\d{1,3}\.\s*", "", ln["text"])  # strip a leading serial token
        if txt.strip():
            title_parts.append(txt)
    display_name = _clean(" ".join(title_parts))
    if not display_name:
        return None

    field_text = " ".join(ln["text"] for ln in field_lines)

    # Right column (agency + date + budget), left column (PI/designation/institution)
    left_lines: list[str] = []
    right_text_parts: list[str] = []
    for ln in field_lines:
        lft, rgt = _left_right(ln, boundary_x)
        if lft:
            left_lines.append(lft)
        if rgt:
            right_text_parts.append(rgt)
    right_text = _clean(" ".join(right_text_parts)) or ""

    # PI name = first left line; designation = second (best effort)
    pi_name_raw = _clean(left_lines[0]) if left_lines else None
    pi_designation = _clean(left_lines[1]) if len(left_lines) > 1 else None

    # Institution: left lines after name/designation, until a stop label (or a
    # short cap so a missing terminator can't absorb the next project).
    inst_parts: list[str] = []
    for lft in left_lines[2:]:
        if LEFT_STOP_RE.search(lft):
            break
        inst_parts.append(lft)
        if len(inst_parts) >= 4:
            break
    institution = _clean(" ".join(inst_parts))
    # If designation actually looks like an institution (has a PIN/keywords) and
    # we found no institution, fold it in.
    if not institution and pi_designation and (PIN_RE.search(pi_designation) or re.search(r"Institute|University|Hospital|Centre|Center|College|AIIMS|ICMR", pi_designation, re.I)):
        institution = pi_designation
        pi_designation = None

    # Agency = right text up to "Date of approval"
    agency_raw = _clean(re.split(r"Date of approval", right_text, flags=re.I)[0]) if right_text else None

    m_date = DATE_RE.search(field_text)
    date_of_approval_raw = _clean(m_date.group(1)) if m_date else None
    start_year = None
    if date_of_approval_raw:
        ym = YEAR_RE.search(date_of_approval_raw)
        if ym:
            start_year = ym.group(0)

    m_budget = BUDGET_RE.search(field_text)
    total_budget_raw = _clean(m_budget.group(1)) if m_budget else None
    m_dur = DURATION_RE.search(field_text)
    duration_raw = _clean(m_dur.group(1)) if m_dur else None
    m_subj = SUBJECT_RE.search(field_text)
    subject_area = _clean(m_subj.group(1)) if m_subj else None

    given, family = split_name(pi_name_raw)

    key = "|".join([
        (display_name or "").casefold(),
        (pi_name_raw or "").casefold(),
        date_of_approval_raw or "",
        (agency_raw or "").casefold(),
    ])
    award_hash = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]

    return {
        "funder_award_id": f"icmr-hmsc-{award_hash}",
        "display_name": display_name,
        "pi_name_raw": pi_name_raw,
        "lead_given_name": given,
        "lead_family_name": family,
        "pi_designation": pi_designation,
        "institution": institution,
        "collaborating_agency": agency_raw,
        "date_of_approval_raw": date_of_approval_raw,
        "start_year": str(start_year) if start_year else None,
        "total_budget_raw": total_budget_raw,
        "duration_raw": duration_raw,
        "subject_area": subject_area,
        "meeting_label": source.get("label"),
        "meeting_date_raw": meeting_date_raw,
        "source_pdf_url": source["url"],
        "provenance": PROVENANCE,
        "funder_id": str(FUNDER_ID),
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Assemble, validate, write, upload
# ---------------------------------------------------------------------------
STRING_COLS = [
    "funder_award_id", "display_name", "pi_name_raw", "lead_given_name",
    "lead_family_name", "pi_designation", "institution", "collaborating_agency",
    "date_of_approval_raw", "start_year", "total_budget_raw", "duration_raw",
    "subject_area", "meeting_label", "meeting_date_raw", "source_pdf_url",
    "provenance", "funder_id", "downloaded_at",
]


def build_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    for col in STRING_COLS:
        if col not in df.columns:
            df[col] = None
    # Dedupe by synthetic funder_award_id (same project re-published across PDFs).
    df = df.drop_duplicates(subset=["funder_award_id"], keep="first").reset_index(drop=True)
    df = df[STRING_COLS].astype("string")
    return df


def validate(df: pd.DataFrame) -> None:
    n = len(df)
    log(f"Local validation: {n} rows")
    if n == 0:
        raise RuntimeError("No rows parsed — aborting.")

    def pct(col: str) -> None:
        c = int(df[col].notna().sum())
        log(f"  {col:<22}{c:>7}/{n}  ({100.0 * c / n:5.1f}%)")

    for col in ["display_name", "pi_name_raw", "lead_family_name", "institution",
                "collaborating_agency", "date_of_approval_raw", "start_year", "subject_area"]:
        pct(col)
    uniq = df["funder_award_id"].nunique()
    log(f"  unique funder_award_id {uniq}/{n}")
    yrs = pd.to_numeric(df["start_year"], errors="coerce").dropna()
    if len(yrs):
        log(f"  year range {int(yrs.min())}-{int(yrs.max())}")
    # Sanity: title should not be dominated by one value; PI not an institution.
    top_pi = df["lead_family_name"].value_counts().head(3).to_dict()
    log(f"  top PI family names: {top_pi}")


def upload_to_s3(local_path: Path, allow_shrink: bool) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for upload; use --skip-upload locally.") from exc

    client = boto3.client("s3")
    new_rows = len(pd.read_parquet(local_path))
    log(f"Runbook §1.4 shrink-check vs s3://{S3_BUCKET}/{S3_KEY}")
    try:
        client.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        prev_path = local_path.with_suffix(".prev.parquet")
        client.download_file(S3_BUCKET, S3_KEY, str(prev_path))
        prev_rows = len(pd.read_parquet(prev_path))
        log(f"  previous S3 rows={prev_rows:,}, new rows={new_rows:,}")
        if new_rows < prev_rows and not allow_shrink:
            raise RuntimeError(
                f"Refusing to shrink: new {new_rows:,} < previous {prev_rows:,}. "
                f"Use --allow-shrink to override."
            )
    except client.exceptions.ClientError:
        log("  No existing S3 parquet found; treating this as first ingest.")

    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    client.upload_file(str(local_path), S3_BUCKET, S3_KEY)
    log("Upload complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="ICMR HMSC Approved Projects -> parquet -> S3")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit", type=int, default=None, help="Smoke-test: stop after N parsed projects")
    parser.add_argument("--max-pdfs", type=int, default=None, help="Smoke-test: only fetch the first N PDFs")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--allow-shrink", action="store_true")
    args = parser.parse_args()

    log("=" * 72)
    log("ICMR HMSC Approved Projects ingest starting")
    log(f"source={LIST_URL}")
    log(f"funder_id={FUNDER_ID}  provenance={PROVENANCE}")

    sources = discover_pdfs()
    log(f"Discovered {len(sources)} HMSC list PDFs (Vol_I–IV deferred)")
    if args.max_pdfs:
        sources = sources[:args.max_pdfs]
        log(f"--max-pdfs: limiting to first {len(sources)} PDFs")
    if not sources:
        log("No source PDFs discovered; aborting.")
        sys.exit(1)

    records: list[dict[str, Any]] = []
    for idx, src in enumerate(sources, 1):
        log(f"[{idx}/{len(sources)}] {src['url']}")
        try:
            raw = fetch(src["url"])
        except Exception as exc:  # noqa: BLE001
            log(f"  fetch failed, skipping: {exc}")
            continue
        records.extend(parse_pdf(raw, src))
        if args.limit and len(records) >= args.limit:
            records = records[:args.limit]
            log(f"--limit reached ({args.limit}); stopping")
            break

    df = build_dataframe(records)
    validate(df)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "icmr_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"Wrote {out} ({out.stat().st_size:,} bytes)")

    if args.skip_upload:
        log("--skip-upload set; not uploading to S3.")
        return
    upload_to_s3(out, args.allow_shrink)


if __name__ == "__main__":
    main()
