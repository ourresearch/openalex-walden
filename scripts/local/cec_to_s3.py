#!/usr/bin/env python3
"""
California Energy Commission to S3 Data Pipeline
================================================

The California Energy Commission (CEC; OpenAlex F4320308020) funds energy R&D
through its Energy Research and Development Division (EPIC, the Electric Program
Investment Charge; Natural Gas R&D, formerly PIER) alongside large deployment,
loan and incentive programmes (Clean Transportation Program chargers and
vehicles, ECAA efficiency loans, FPIP, DEBA, building decarbonisation...).

Every CEC grant, loan and contract is approved at a public Business Meeting,
and the meeting agenda (docketed as a PDF in the CEC e-filing system, docket
``YY-BUSMTG-01``) describes each new agreement in a fixed form:

    f. National Community Renaissance. Proposed resolution conditionally approving
    agreement EPC-23-035 with National Community Renaissance for a $8,000,000
    grant, and adopting staff's determination that this action is exempt from
    CEQA. This project will build a 288-unit all-electric affordable housing ...
    (EPIC Funding) Contact: ...

This script reads the docket logs (https://efiling.energy.ca.gov/Lists/DocketLog.aspx
?docketnumber=YY-BUSMTG-01, 2022 onward -- earlier agendas are not in the docket
system; the CEC's awards page likewise points to Public Records Act requests for
pre-2022 awards), takes the latest revision of each meeting's agenda, extracts the
text (pypdf) and parses every NEW agreement approved in it (amendments are
skipped): agreement number, recipient, amount, instrument (grant / loan /
contract), funding source tag, project description, meeting date.

The Energy Innovation Showcase (energizeinnovation.fund, CEC's EPIC project
database) disallows ClaudeBot / anthropic-ai / Claude-User in robots.txt and is
NOT used. energy.ca.gov and efiling.energy.ca.gov have no such rule.

Scope (2026-10-01 batch rule: keep research programmes, drop money that is
clearly not research). Kept: grant agreements funded by EPIC, PIER / Gas R&D or
CRISP (Carbon Removal Innovation Support Program), i.e. numbers EPC-, PIR-,
CRI-. Kept and flagged (demonstration / resource programmes, not clearly
research): Long-Duration Energy Storage (LDS-), INDIGO industrial
decarbonization (IND-) and Geothermal Resources Development Account (GEO-)
grants. Dropped (written to the parquet with ``in_scope = false`` and a reason;
the notebook filters them): Clean Transportation Program / NEVI / ZVI / EVC
vehicle and charger infrastructure, ECAA loans, FPIP, DEBA, DSGS, CERRI,
building-decarbonisation incentives, offshore-wind port planning, IRA workforce
grants, and the CEC's own support contracts, memberships and MOUs (numeric
prefixes 100- to 800-, MOU-) even when paid from EPIC / Gas R&D money.

funder_award_id (runbook §2.1.1): the agreement number ("EPC-23-035",
"PIR-22-003", "500-09-020"), which is exactly the form citing papers carry.

Output: s3://openalex-ingest/awards/cec/cec_projects.parquet
"""

import argparse
import html
import io
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (TWCF-style shim; it renames sys, so for the §4.0 grep: sys.stdout.reconfigure)
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

DOCKET_LOG = "https://efiling.energy.ca.gov/Lists/DocketLog.aspx?docketnumber={yy:02d}-BUSMTG-01"
DOC_URL = "https://efiling.energy.ca.gov/GetDocument.aspx?tn={tn}"
FIRST_YEAR = 2022
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/cec/cec_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 1.0
RETRIES = 4

MONTHS = {m: i for i, m in enumerate(["january", "february", "march", "april", "may", "june", "july",
                                       "august", "september", "october", "november", "december"], 1)}

# PDF line breaks split numbers ("EPC- 23-012", "EPC-23- 003"): allow a space after each hyphen
AGREEMENT = r"(?:[A-Z]{2,4}|\d{3})-\s?\d{2}-\s?\d{3}(?:-[A-Z]{2,4})?"
APPROVE_RE = re.compile(
    rf"approving\s+(?:an?\s+|the\s+)?(?:new\s+)?(?:grant\s+|loan\s+|contract\s+|interagency\s+)?agreement\s+({AGREEMENT})",
    re.I)
AMOUNT_RE = re.compile(r"(up to\s+)?\$\s?([\d,]+(?:\.\d+)?)\s*(million)?\s*(grant|loan|contract|interagency agreement|agreement)?", re.I)
FUNDING_RE = re.compile(r"\(((?:[^()]|\([^()]*\)){2,200}?)\s+Fund(?:ing|s)?\)", re.I)

RESEARCH_TAG_RE = re.compile(r"\bEPIC\b|\bPIER\b|Electric Program Investment Charge|Public Interest Energy Re|"
                             r"Gas R&D|Gas Research|Natural Gas R ?& ?D|\bNG R&D\b|\bCRISP\b|Carbon Removal Innovation", re.I)
RESEARCH_PREFIX_RE = re.compile(r"^(EPC|PIR|CRI)-")
FLAG_TAG_RE = re.compile(r"\bLDES\b|Long[- ]Duration|INDIGO|Geothermal Resources Development", re.I)
FLAG_PREFIX_RE = re.compile(r"^(LDS|IND|GEO)-")


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, binary: bool = False):
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=120)
            if r.status_code == 200:
                time.sleep(REQUEST_DELAY)
                return r.content if binary else r.text
            last = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(4 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last}")


def clean_text(fragment: str) -> str:
    t = html.unescape(re.sub(r"<[^>]+>", " ", fragment)).replace("\xa0", " ")
    return re.sub(r"\s+", " ", t).strip()


def list_agendas(year: int, cache_dir: Path) -> list[dict]:
    """Latest agenda revision (highest TN) per meeting date in docket YY-BUSMTG-01."""
    cache = cache_dir / f"docket_{year}.html"
    if cache.exists():
        page = cache.read_text()
    else:
        page = get(DOCKET_LOG.format(yy=year % 100))
        cache.write_text(page)
    meetings: dict[str, dict] = {}
    for tn, label in re.findall(r"GetDocument\.aspx\?tn=(\d+)[^>]*>(.*?)</a>", page, re.S):
        title = clean_text(label)
        if not re.search(r"\bagenda\b", title, re.I) or not re.search(r"business meeting", title, re.I):
            continue
        if re.search(r"comment|support|item|presentation|transcript|minutes", title, re.I):
            continue
        m = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s*(\d{4})?", title, re.I)
        short = re.match(r"^(\d{2})-(\d{2})(\d{2})\b", title)  # "22-0524 Business Meeting Agenda"
        if m:
            yr = int(m.group(3)) if m.group(3) else year
            date = f"{yr:04d}-{MONTHS[m.group(1).lower()]:02d}-{int(m.group(2)):02d}"
        elif short:
            date = f"20{short.group(1)}-{short.group(2)}-{short.group(3)}"
        else:
            log(f"  {year}: cannot date agenda TN {tn}: {title}")
            continue
        if date not in meetings or int(tn) > int(meetings[date]["tn"]):
            meetings[date] = {"tn": tn, "title": title, "meeting_date": date}
    return sorted(meetings.values(), key=lambda x: x["meeting_date"])


def agenda_text(tn: str, cache_dir: Path) -> str:
    cache = cache_dir / f"agenda_{tn}.txt"
    if cache.exists():
        return cache.read_text()
    from pypdf import PdfReader
    pdf = get(DOC_URL.format(tn=tn), binary=True)
    text = "\n".join((p.extract_text() or "") for p in PdfReader(io.BytesIO(pdf)).pages)
    cache.write_text(text)
    return text


def normalise(text: str) -> str:
    # drop the e-filing cover block and the page furniture
    text = re.sub(r"California Energy Commission\s*•\s*715 P Street.*?\n", "\n", text)
    text = re.sub(r"Sacramento,? California 95814\s*•\s*916-\d{3}-\d{4}\s*\n", "\n", text)
    text = re.sub(r"^\s*Page\s*-?\s*\d+\s*$", "", text, flags=re.M)
    text = text.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    text = text.replace("‐", "-").replace("‑", "-").replace("–", "-").replace("—", "-")
    lines = [ln.rstrip() for ln in text.split("\n")]
    return "\n".join(ln for ln in lines if ln.strip())


ITEM_START = re.compile(r"^\s*(\d{1,2}|[a-z]{1,2}|[ivx]{1,4})\.\s+(?=[A-Z0-9\"'(])", re.M)


def split_items(text: str) -> list[dict]:
    """Return items with their label and the enclosing top-level item's text."""
    starts = [m for m in ITEM_START.finditer(text)]
    items = []
    parent = None
    group_tag = None  # funding tag of the latest group-header (non-approval) item inside this top-level item
    for i, m in enumerate(starts):
        end = starts[i + 1].start() if i + 1 < len(starts) else len(text)
        body = re.sub(r"\s+", " ", text[m.end():end]).strip()
        label = m.group(1)
        it = {"label": label, "body": body, "parent": None, "group_tag": None}
        if label.isdigit():
            parent = it
            group_tag = None
        else:
            it["parent"] = parent
            it["group_tag"] = group_tag or (FUNDING_RE.findall(parent["body"]) if parent else None)
        own = FUNDING_RE.findall(body)
        if own and not APPROVE_RE.search(body):
            group_tag = own
        items.append(it)
    return items


def to_amount(m: re.Match) -> float | None:
    try:
        v = float(m.group(2).replace(",", ""))
    except ValueError:
        return None
    if m.group(3):
        v *= 1_000_000
    return v


def parse_agreements(text: str, meeting: dict) -> list[dict]:
    out = []
    for it in split_items(normalise(text)):
        body = it["body"]
        hits = list(APPROVE_RE.finditer(body))
        if not hits:
            continue
        header = re.match(r"^(.{2,250}?)\.\s+Proposed\s+(?:resolution|order)", body)
        fund = FUNDING_RE.findall(body) or it["group_tag"] or []
        for k, h in enumerate(hits):
            seg_end = hits[k + 1].start() if k + 1 < len(hits) else len(body)
            seg = body[h.start():seg_end]
            number = re.sub(r"\s", "", h.group(1)).upper()
            wm = re.search(rf"{re.escape(h.group(1))},?\s+with\s+(.+?),?\s+(?:for|to)\s+(?:a|an|up to|\$)", seg, re.I)
            am = AMOUNT_RE.search(seg)
            amount = to_amount(am) if am else None
            instrument = (am.group(4) or "").lower() if am else ""
            if not instrument:
                im = re.search(r"\b(grant|loan|contract|interagency agreement)\b", seg, re.I)
                instrument = im.group(1).lower() if im else None
            purpose = None  # "for a $600,000 grant to fund follow-on work associated with ..., and adopting"
            pm = re.search(r"\$[\d,.]+\s*(?:million\s*)?(?:grant|loan|contract|agreement)?,?\s+(?:to|for)\s+(.+?)"
                           r"(?:,?\s+and\s+adopting|,?\s+and\s+(?:a\s+)?(?:finding|determin)|;|$)", seg, re.I)
            if pm and len(pm.group(1)) > 15:
                purpose = pm.group(1).strip()
            after = None  # the project description that follows the CEQA clause
            dm = re.search(r"CEQA[^.]*\.+\s*(.+)", seg)
            if dm:
                after = re.split(r"\s*(?:\((?:[^()]|\([^()]*\)){2,200}?\s+Fund(?:ing|s)?\)|Contact:|Staff recommends)",
                                 dm.group(1))[0].strip(" .") or None
            desc = " ".join(x.rstrip(".") + "." for x in [purpose and (purpose[:1].upper() + purpose[1:]), after] if x) or None
            recipient = wm.group(1).strip() if wm else None
            if header and len(hits) == 1 and not re.match(r"^(Proposed|Consideration|Possible)", header.group(1)):
                recipient = header.group(1).strip()
            out.append({
                "agreement_number": number,
                "recipient": recipient,
                "amount": amount,
                "amount_is_up_to": bool(am and am.group(1)),
                "instrument": instrument,
                "funding_source": "; ".join(dict.fromkeys(f.strip() for f in fund)) or None,
                "description": desc,
                "purpose_clause": purpose,
                # "adopting CEQA findings for PSGM3, LLC's (PSG) Electrified Steel Mill Long Duration
                # Energy Storage Demonstration, and approving agreement ..." names the project
                "project_name": (lambda cm: cm.group(1).strip() if cm else None)(re.search(
                    r"CEQA\)?\s+findings\s+for\s+.{2,200}?['’]s\s+(?:\([A-Za-z&]{2,10}\)\s+)?(.{4,250}?)\s*,?\s+and\s+(?:conditionally\s+)?approving",
                    body)),
                "post_ceqa_text": after,
                "conditional": bool(re.search(r"conditionally approving", body[max(0, h.start() - 40):h.end()], re.I)),
                "item_label": (it["parent"]["label"] + "." if it["parent"] else "") + it["label"],
                "item_text": body[:4000],
                "meeting_date": meeting["meeting_date"],
                "agenda_tn": meeting["tn"],
                "agenda_title": meeting["title"],
                "landing_page_url": DOC_URL.format(tn=meeting["tn"]),
            })
    return out


def classify(row: dict) -> tuple[bool, str]:
    num, tag = row["agreement_number"], row["funding_source"] or ""
    research = RESEARCH_TAG_RE.search(tag) or RESEARCH_PREFIX_RE.match(num)
    flag = FLAG_TAG_RE.search(tag) or FLAG_PREFIX_RE.match(num)
    if (research or flag) and not (RESEARCH_PREFIX_RE.match(num) or FLAG_PREFIX_RE.match(num)):
        # numeric-prefix (300-/800-...) and MOU agreements paid from R&D money are the CEC's own
        # support contracts, memberships and MOUs, not awards
        return False, f"R&D-funded support contract / membership / MOU ({num.split('-')[0]})"
    if research:
        return True, "research (EPIC / PIER-Gas R&D / CRISP)"
    if flag:
        return True, "flag: demonstration / resource programme (LDES / INDIGO / GRDA)"
    return False, f"not research: {tag or 'no funding tag'} ({num.split('-')[0]})"


def title_from(desc: str | None) -> str | None:
    if not desc:
        return None
    first = re.split(r"(?<=[a-z0-9)])\.\s+(?=[A-Z])", desc)[0].strip().rstrip(".")
    first = re.sub(r"^(?:This|The)\s+(?:proposed\s+)?(?:project|grant|agreement|loan|contract|research|study)\s+(?:will|would|is to|aims to)\s+", "", first, flags=re.I)
    first = re.sub(r"^take place (?:in|at) [^,]{2,80}? and (?:will )?", "", first, flags=re.I)
    if len(first) > 300:
        first = first[:300].rsplit(" ", 1)[0] + "..."
    return (first[:1].upper() + first[1:]) if first else None


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--output-dir", type=Path, default=Path("./cec_out"))
    ap.add_argument("--cache-dir", type=Path, default=Path("./cec_cache"))
    ap.add_argument("--limit", type=int, default=None, help="only parse the first N agendas (smoke test)")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--allow-shrink", action="store_true")
    args = ap.parse_args()
    args.cache_dir.mkdir(parents=True, exist_ok=True)

    meetings = []
    for y in range(FIRST_YEAR, datetime.now().year + 1):
        ag = list_agendas(y, args.cache_dir)
        log(f"{y}: {len(ag)} meeting agendas")
        meetings += ag
    if args.limit:
        meetings = meetings[-args.limit:]
    rows = []
    for i, m in enumerate(meetings, 1):
        found = parse_agreements(agenda_text(m["tn"], args.cache_dir), m)
        rows += found
        log(f"  [{i}/{len(meetings)}] {m['meeting_date']} TN {m['tn']}: {len(found)} new agreements")

    df = pd.DataFrame(rows)
    # an agreement approved twice (re-approval after a conditional approval, or a revised item)
    # keeps its latest approval
    df = df.sort_values(["meeting_date", "agenda_tn"]).drop_duplicates("agreement_number", keep="last")
    cls = df.apply(lambda r: classify(r), axis=1)
    df["in_scope"] = [c[0] for c in cls]
    df["scope_reason"] = [c[1] for c in cls]
    # display title: the project name the agenda gives (CEQA-findings items), else the first sentence
    # of the project description, else the purpose clause ("...grant to <purpose>")
    df["title"] = [(n[:1].upper() + n[1:]) if n else (title_from(a) or title_from(p))
                   for n, a, p in zip(df["project_name"], df["post_ceqa_text"], df["purpose_clause"])]
    df["funder_award_id"] = df["agreement_number"]
    if df["funder_award_id"].str.lower().duplicated().any():
        raise SystemExit("duplicate agreement numbers after dedup")
    log(f"{len(df)} distinct new agreements; in scope {int(df['in_scope'].sum())}")
    log("scope reasons: " + json.dumps(df["scope_reason"].value_counts().head(25).to_dict(), ensure_ascii=False))
    ins = df[df["in_scope"]]
    for c in ["recipient", "amount", "instrument", "funding_source", "description", "title"]:
        log(f"  in-scope {c:15s} {ins[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "cec_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")
    if args.skip_upload or args.limit:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_cec_projects.parquet"
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
