#!/usr/bin/env python3
"""
Generic IATI publisher -> parquet -> S3 pipeline
================================================

One ingester for any organisation that publishes IATI activity XML (aid agencies
such as Sida, Norad/Norway MFA, FCDO, BMZ, Danida, Netherlands MFA, AFD ...).
It generalises `idrc_to_s3.py`: `parse_activity()` and `get_narratives()` keep
the same names and the same IDRC columns, and add everything the IDRC notebook
threw away (every recipient country with its percentage, regions, all
participating orgs with roles, sector vocabularies, transactions with
provider/receiver orgs, related activities, default currency, hierarchy).

Discovery (no API key, no sign-up):
  --source bulk      (default) https://bulk-data.iatistandard.org  — IATI's own
                     Bulk Data Service: a daily cached copy of every registered
                     dataset, indexed by publisher short name.
  --source registry  https://iatiregistry.org CKAN API -> the publisher's own
                     file URLs (fallback if the bulk service is down).

Publishers are named by their IATI Registry short name (`sida`, `norad`,
`fcdo`) or by their IATI organisation identifier (`SE-0`, `NO-BRC-971277882`,
`GB-GOV-1`). `iati_publishers.csv` (next to this file) maps each publisher to
its OpenAlex funder and names its research filter.

RESEARCH FILTER (required): IATI is all aid, not just research. Only activities
admitted by a rule in `iati_research_rules.csv` may go toward the public
`/awards` corpus. The filter is data, not code: DAC research purpose codes plus
per-publisher programme rules. Every activity records which rule(s) admitted
it (`research_rule_ids`). ONLY research-admitted awards that route to an
OpenAlex funder are written to the S3 parquet; the full parse stays local.

FROM ACTIVITIES TO AWARDS (`derive_awards`): publishers split a grant across
hierarchy levels differently, so the grain is a per-publisher setting
(`award_level`: leaf or top), as are the citable award number
(`award_id_regex`), the funder routing for files that carry several bodies
(`funder_org_refs`), the landing page and the amount basis. Award-level
columns: is_award, is_research, ship, funder_award_id, openalex_funder_id,
award_amount / _currency / _basis, award_start_date / award_end_date,
lead_org_name / _ref / _country, recipient_country_codes, parent_iati_identifier,
parent_title, children_total_*.

Outputs per publisher (slug = short name with non-alphanumerics -> "_"):
  <output-dir>/iati_<slug>_activities.parquet   every activity (local only)
  <output-dir>/iati_<slug>_projects.parquet     rows with ship = true only
  <output-dir>/iati_<slug>_summary.json         counts
  s3://openalex-ingest/awards/iati_<slug>/iati_<slug>_projects.parquet

Usage:
    python iati_to_s3.py --publisher sida norad fcdo        # parse + upload
    python iati_to_s3.py --all                              # every row of iati_publishers.csv
    python iati_to_s3.py --publisher fcdo --limit 500       # smoke test, never uploads
    python iati_to_s3.py --publisher bmz --skip-upload      # local parse only

Import use (no AWS/Databricks/pandas needed at import time):
    from iati_to_s3 import parse_activity, iter_activities, load_rules, apply_research_rules

Requirements (only for the CLI run): pandas, pyarrow, requests; boto3 for upload.

Deliberately not captured: `contact-info` (staff phone/e-mail), `result`
(indicator trees, large), `conditions`, `crs-add`, `fss`, `country-budget-items`
and `document-link` bodies beyond url/title/category. Their element counts are
recorded per activity in `uncaptured_elements_json` so nothing is dropped
silently. Elements in a foreign XML namespace (e.g. Sida's `strategies`) are
kept generically in `extensions_json`.
"""

import argparse
import csv
import hashlib
import json
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree as ET

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# Windows Python defaults to cp1252 for BOTH stdout-when-piped AND default
# file I/O (Path.write_text / open() without explicit encoding=). Production
# runs on Linux/Databricks where UTF-8 is the default, but this fixes local
# validation on Windows. See runbook §1.2. (sys.stdout.reconfigure is done
# through the `_sys_utf8` alias below.)
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

HERE = Path(__file__).resolve().parent
DEFAULT_PUBLISHERS_CSV = HERE / "iati_publishers.csv"
DEFAULT_RULES_CSV = HERE / "iati_research_rules.csv"

BULK_BASE = "https://bulk-data.iatistandard.org"
BULK_DATASETS_INDEX = f"{BULK_BASE}/datasets-full"
BULK_REPORTING_ORGS = f"{BULK_BASE}/reporting-orgs"
REGISTRY_API = "https://iatiregistry.org/api/3/action"

S3_BUCKET = "openalex-ingest"

HTTP_HEADERS = {
    "User-Agent": "openalex-walden/1.0 (+https://openalex.org; contact: team@openalex.org)",
}

XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"

# IATI 1.x used words where 2.x uses numeric codes. Map them so one notebook
# can read both.
V1_DATE_TYPES = {"start-planned": "1", "start-actual": "2", "end-planned": "3", "end-actual": "4"}
V1_ORG_ROLES = {"funding": "1", "accountable": "2", "extending": "3", "implementing": "4"}
V1_TRANSACTION_TYPES = {
    "IF": "1", "C": "2", "D": "3", "E": "4", "IR": "5", "LR": "6", "R": "7",
    "QP": "8", "QS": "9", "CG": "10",
}

# Child elements of <iati-activity> that this parser reads.
CAPTURED_TAGS = {
    "iati-identifier", "reporting-org", "title", "description", "participating-org",
    "other-identifier", "activity-status", "activity-date", "activity-scope",
    "recipient-country", "recipient-region", "location", "sector", "tag",
    "policy-marker", "collaboration-type", "default-flow-type", "default-finance-type",
    "default-aid-type", "default-tied-status", "budget", "planned-disbursement",
    "transaction", "related-activity", "document-link", "humanitarian-scope",
    "capital-spend",
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def slugify(short_name: str) -> str:
    """`gac-amc` -> `gac_amc`; used in file names, S3 keys and provenance."""
    return re.sub(r"[^a-z0-9]+", "_", short_name.lower()).strip("_")


# ---------------------------------------------------------------------------
# Parsing (publisher-agnostic; stdlib only)
# ---------------------------------------------------------------------------

def get_narratives(elem, default_lang: str = "und") -> dict:
    """Collect language -> narrative text for an element with `narrative` children.

    A narrative with no xml:lang inherits `default_lang` (the activity's
    xml:lang). IATI 1.x has no `narrative` children: the text sits directly on
    the element, so fall back to that.
    """
    out: dict = {}
    if elem is None:
        return out
    for n in elem.findall("narrative"):
        lang = (n.attrib.get(XML_LANG) or default_lang or "und").lower()
        if n.text and n.text.strip() and lang not in out:
            out[lang] = n.text.strip()
    if not out and elem.text and elem.text.strip():
        lang = (elem.attrib.get(XML_LANG) or default_lang or "und").lower()
        out[lang] = elem.text.strip()
    return out


def en_or_first(narratives: dict):
    if not narratives:
        return None
    return narratives.get("en") or next(iter(narratives.values()))


def _attr(elem, name):
    if elem is None:
        return None
    v = elem.attrib.get(name)
    return v.strip() if isinstance(v, str) and v.strip() else None


def _code(parent, tag):
    return _attr(parent.find(tag), "code")


def _text(elem):
    return elem.text.strip() if elem is not None and elem.text and elem.text.strip() else None


def _to_float(s):
    try:
        return float(str(s).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _org(elem, default_lang, activity_id_attr=None):
    """Serialise a provider-org / receiver-org / participating-org element."""
    if elem is None:
        return None
    names = get_narratives(elem, default_lang)
    out = {
        "ref": _attr(elem, "ref"),
        "type": _attr(elem, "type"),
        "name": en_or_first(names),
    }
    if activity_id_attr:
        out["activity_id"] = _attr(elem, activity_id_attr)
    return out


def _extension_to_obj(elem):
    """Generic JSON for an element in a non-IATI namespace (publisher extension)."""
    obj = {"tag": elem.tag}
    if elem.attrib:
        obj["attrs"] = dict(elem.attrib)
    if elem.text and elem.text.strip():
        obj["text"] = elem.text.strip()
    kids = [_extension_to_obj(c) for c in list(elem)]
    if kids:
        obj["children"] = kids
    return obj


def _sum_by_currency(pairs):
    """[(value, currency)] -> (total_in_dominant_currency, dominant_currency, {cur: total})."""
    by = {}
    n = Counter()
    for value, cur in pairs:
        v = _to_float(value)
        if v is None:
            continue
        cur = cur or "UNKNOWN"
        by[cur] = by.get(cur, 0.0) + v
        n[cur] += 1
    if not by:
        return None, None, {}
    dominant = n.most_common(1)[0][0]
    return by[dominant], (None if dominant == "UNKNOWN" else dominant), by


def parse_activity(act, source_url: str, iati_version: str = None,
                   max_related_siblings: int = 0) -> dict:
    """Map one iati-activity element to a flat dict of strings.

    Superset of `idrc_to_s3.parse_activity`: the IDRC columns keep their names
    and meaning. Multi-row sub-elements are JSON strings so the notebook (or
    workstream E) decides how to flatten them.

    `max_related_siblings`: related-activity type 3 (sibling) refs kept per
    activity. Parent/child/co-funded/third-party refs are always kept in full.
    Sida lists every sibling on every component (~227 refs per activity), which
    is derivable from the shared parent, so the default keeps none and records
    the count.
    """
    default_lang = (act.attrib.get(XML_LANG) or "und").lower()
    default_currency = _attr(act, "default-currency")

    iati_id = _text(act.find("iati-identifier"))

    rep = act.find("reporting-org")
    rep_names = get_narratives(rep, default_lang)

    title_n = get_narratives(act.find("title"), default_lang)

    # Description type 1 = general, 2 = objectives, 3 = target groups, 4 = other
    descs: dict = {}
    for d in act.findall("description"):
        t = d.attrib.get("type", "1")
        narr = get_narratives(d, default_lang)
        if t in descs:
            for k, v in narr.items():
                descs[t].setdefault(k, v)
        else:
            descs[t] = narr
    desc_general = descs.get("1", {})

    # Activity dates: 1 planned-start, 2 actual-start, 3 planned-end, 4 actual-end
    dates = {"1": None, "2": None, "3": None, "4": None}
    for d in act.findall("activity-date"):
        t = d.attrib.get("type")
        t = V1_DATE_TYPES.get(t, t)
        iso = d.attrib.get("iso-date") or (_text(d) if iati_version and iati_version.startswith("1") else None)
        if t in dates and iso and dates[t] is None:
            dates[t] = iso[:10]

    def value_parts(v):
        if v is None:
            return None, None, None
        return _text(v), (_attr(v, "currency") or default_currency), _attr(v, "value-date")

    # Budgets (type 1 = original, 2 = revised; status 1 = indicative, 2 = committed)
    budgets = []
    for b in act.findall("budget"):
        val, cur, vdate = value_parts(b.find("value"))
        budgets.append({
            "status": b.attrib.get("status"),
            "type": b.attrib.get("type"),
            "period_start": _attr(b.find("period-start"), "iso-date"),
            "period_end": _attr(b.find("period-end"), "iso-date"),
            "value": val,
            "currency": cur,
            "value_date": vdate,
        })

    planned_disbursements = []
    for pd_ in act.findall("planned-disbursement"):
        val, cur, vdate = value_parts(pd_.find("value"))
        planned_disbursements.append({
            "type": pd_.attrib.get("type"),
            "period_start": _attr(pd_.find("period-start"), "iso-date"),
            "period_end": _attr(pd_.find("period-end"), "iso-date"),
            "value": val,
            "currency": cur,
            "value_date": vdate,
            "provider_org": _org(pd_.find("provider-org"), default_lang, "provider-activity-id"),
            "receiver_org": _org(pd_.find("receiver-org"), default_lang, "receiver-activity-id"),
        })

    # Transactions, with provider/receiver orgs (the sub-award signal)
    transactions = []
    for t in act.findall("transaction"):
        val, cur, vdate = value_parts(t.find("value"))
        ttype = _code(t, "transaction-type")
        ttype = V1_TRANSACTION_TYPES.get(ttype, ttype)
        aid_types = [
            {"code": a.attrib.get("code"), "vocabulary": a.attrib.get("vocabulary")}
            for a in t.findall("aid-type")
        ]
        transactions.append({
            "ref": _attr(t, "ref"),
            "type": ttype,
            "date": _attr(t.find("transaction-date"), "iso-date"),
            "value": val,
            "currency": cur,
            "value_date": vdate,
            "description": en_or_first(get_narratives(t.find("description"), default_lang)),
            "provider_org": _org(t.find("provider-org"), default_lang, "provider-activity-id"),
            "receiver_org": _org(t.find("receiver-org"), default_lang, "receiver-activity-id"),
            "sectors": [
                {"code": s.attrib.get("code"), "vocabulary": s.attrib.get("vocabulary")}
                for s in t.findall("sector")
            ] or None,
            "recipient_country": _code(t, "recipient-country"),
            "recipient_region": _code(t, "recipient-region"),
            "flow_type": _code(t, "flow-type"),
            "finance_type": _code(t, "finance-type"),
            "aid_types": aid_types or None,
            "tied_status": _code(t, "tied-status"),
            "disbursement_channel": _code(t, "disbursement-channel"),
            "humanitarian": _attr(t, "humanitarian"),
        })

    # Participating organisations (role 1=funding, 2=accountable, 3=extending, 4=implementing)
    participating = []
    for po in act.findall("participating-org"):
        narr = get_narratives(po, default_lang)
        role = po.attrib.get("role")
        role = V1_ORG_ROLES.get((role or "").lower(), role)
        participating.append({
            "role": role,
            "type": po.attrib.get("type"),
            "ref": po.attrib.get("ref"),
            "name": en_or_first(narr),
            "name_en": narr.get("en"),
            "name_fr": narr.get("fr"),
            "name_es": narr.get("es"),
            "names": narr or None,
            "activity_id": po.attrib.get("activity-id"),
            "crs_channel_code": po.attrib.get("crs-channel-code"),
        })

    # Recipient countries / regions — ALL of them, with percentages
    recipient_countries = [
        {"code": (rc.attrib.get("code") or "").upper() or None,
         "percentage": rc.attrib.get("percentage"),
         "name": en_or_first(get_narratives(rc, default_lang))}
        for rc in act.findall("recipient-country")
    ]
    recipient_regions = [
        {"code": rr.attrib.get("code"), "percentage": rr.attrib.get("percentage"),
         "vocabulary": rr.attrib.get("vocabulary"),
         "vocabulary_uri": rr.attrib.get("vocabulary-uri"),
         "name": en_or_first(get_narratives(rr, default_lang))}
        for rr in act.findall("recipient-region")
    ]

    # Sectors. vocabulary absent = "1" (OECD DAC 5-digit purpose code).
    sectors = [
        {"code": s.attrib.get("code"), "vocabulary": s.attrib.get("vocabulary"),
         "percentage": s.attrib.get("percentage"),
         "vocabulary_uri": s.attrib.get("vocabulary-uri"),
         "name": en_or_first(get_narratives(s, default_lang))}
        for s in act.findall("sector")
    ]

    tags = [
        {"code": t.attrib.get("code"), "vocabulary": t.attrib.get("vocabulary"),
         "vocabulary_uri": t.attrib.get("vocabulary-uri"),
         "name": en_or_first(get_narratives(t, default_lang))}
        for t in act.findall("tag")
    ]

    policy_markers = [
        {"code": p.attrib.get("code"), "vocabulary": p.attrib.get("vocabulary"),
         "significance": p.attrib.get("significance")}
        for p in act.findall("policy-marker")
    ]

    # Related activities: 1 parent, 2 child, 3 sibling, 4 co-funded, 5 third party
    related = []
    sibling_count = 0
    for ra in act.findall("related-activity"):
        rtype = ra.attrib.get("type")
        if rtype == "3":
            sibling_count += 1
            if sibling_count > max_related_siblings:
                continue
        related.append({"ref": ra.attrib.get("ref"), "type": rtype})

    other_identifiers = []
    for oi in act.findall("other-identifier"):
        owner = oi.find("owner-org")
        other_identifiers.append({
            "ref": oi.attrib.get("ref") or _text(oi),
            "type": oi.attrib.get("type"),
            "owner_ref": _attr(owner, "ref"),
            "owner_name": en_or_first(get_narratives(owner, default_lang)),
        })

    locations = []
    for loc in act.findall("location"):
        locations.append({
            "ref": loc.attrib.get("ref"),
            "name": en_or_first(get_narratives(loc.find("name"), default_lang)),
            "reach": _code(loc, "location-reach"),
            "pos": _text(loc.find("point/pos")),
            "location_ids": [
                {"code": li.attrib.get("code"), "vocabulary": li.attrib.get("vocabulary")}
                for li in loc.findall("location-id")
            ] or None,
        })

    document_links = [
        {"url": dl.attrib.get("url"), "format": dl.attrib.get("format"),
         "title": en_or_first(get_narratives(dl.find("title"), default_lang)),
         "categories": [c.attrib.get("code") for c in dl.findall("category")] or None}
        for dl in act.findall("document-link")
    ]

    default_aid_types = [
        {"code": a.attrib.get("code"), "vocabulary": a.attrib.get("vocabulary")}
        for a in act.findall("default-aid-type")
    ]

    humanitarian_scopes = [
        {"code": h.attrib.get("code"), "type": h.attrib.get("type"),
         "vocabulary": h.attrib.get("vocabulary")}
        for h in act.findall("humanitarian-scope")
    ]

    # Publisher extensions (foreign namespace) and anything not captured
    extensions = []
    uncaptured = Counter()
    for child in list(act):
        tag = child.tag
        if isinstance(tag, str) and tag.startswith("{"):
            extensions.append(_extension_to_obj(child))
        elif tag not in CAPTURED_TAGS:
            uncaptured[tag] += 1

    # Convenience totals (strings). Budgets: a revised budget (type 2) replaces
    # the original (type 1) for the same period, so they are not double counted.
    periods = {}
    for b in budgets:
        key = (b["period_start"], b["period_end"])
        cur = periods.get(key)
        if cur is None or (b.get("type") == "2" and cur.get("type") != "2"):
            periods[key] = b
        elif b.get("type") == cur.get("type"):
            # same period, same type listed twice: keep both by widening the key
            periods[(key, len(periods))] = b
    budget_total, budget_currency, _ = _sum_by_currency(
        [(b["value"], b["currency"]) for b in periods.values()])

    def tx_total(types):
        return _sum_by_currency([(t["value"], t["currency"]) for t in transactions if t["type"] in types])

    commit_total, commit_cur, _ = tx_total({"2"})
    disb_total, disb_cur, _ = tx_total({"3"})
    exp_total, exp_cur, _ = tx_total({"4"})
    incoming_total, incoming_cur, _ = tx_total({"1", "11"})

    def num(x):
        return None if x is None else repr(round(x, 2))

    def dumps(x):
        return json.dumps(x, ensure_ascii=False)

    return {
        # Identifiers
        "iati_identifier": iati_id,
        "reporting_org_ref": _attr(rep, "ref"),
        "reporting_org_type": _attr(rep, "type"),
        "reporting_org_name": en_or_first(rep_names),
        "reporting_org_secondary_reporter": _attr(rep, "secondary-reporter"),
        "hierarchy": _attr(act, "hierarchy"),
        "other_identifiers_json": dumps(other_identifiers),
        # Titles / descriptions: best single value, IDRC-compatible per-language
        # columns, and every language as JSON
        "title": en_or_first(title_n),
        "title_en": title_n.get("en"),
        "title_fr": title_n.get("fr"),
        "title_es": title_n.get("es"),
        "titles_json": dumps(title_n),
        "description": en_or_first(desc_general),
        "description_en": desc_general.get("en"),
        "description_fr": desc_general.get("fr"),
        "description_es": desc_general.get("es"),
        "descriptions_json": dumps(descs),
        # Dates kept as ISO strings so Spark parses them with TRY_TO_DATE
        "planned_start": dates["1"],
        "actual_start": dates["2"],
        "planned_end": dates["3"],
        "actual_end": dates["4"],
        # Status / scope / classifications
        "activity_status_code": _code(act, "activity-status"),
        "activity_scope_code": _code(act, "activity-scope"),
        "collaboration_type_code": _code(act, "collaboration-type"),
        "default_flow_type_code": _code(act, "default-flow-type"),
        "default_finance_type_code": _code(act, "default-finance-type"),
        "default_aid_types_json": dumps(default_aid_types),
        "default_tied_status_code": _code(act, "default-tied-status"),
        "humanitarian": _attr(act, "humanitarian"),
        "default_currency": default_currency,
        "default_lang": default_lang,
        # Money (JSON arrays keep every row; totals are a convenience)
        "budgets_json": dumps(budgets),
        "planned_disbursements_json": dumps(planned_disbursements),
        "transactions_json": dumps(transactions),
        "total_budget": num(budget_total),
        "total_budget_currency": budget_currency,
        "total_commitment": num(commit_total),
        "total_commitment_currency": commit_cur,
        "total_disbursement": num(disb_total),
        "total_disbursement_currency": disb_cur,
        "total_expenditure": num(exp_total),
        "total_expenditure_currency": exp_cur,
        "total_incoming": num(incoming_total),
        "total_incoming_currency": incoming_cur,
        "n_transactions": str(len(transactions)),
        # Orgs / geography / sectors
        "participating_orgs_json": dumps(participating),
        "recipient_countries_json": dumps(recipient_countries),
        "recipient_regions_json": dumps(recipient_regions),
        "sectors_json": dumps(sectors),
        "tags_json": dumps(tags),
        "policy_markers_json": dumps(policy_markers),
        "locations_json": dumps(locations),
        "humanitarian_scopes_json": dumps(humanitarian_scopes),
        # Hierarchy / links
        "related_activities_json": dumps(related),
        "n_related_siblings": str(sibling_count),
        "document_links_json": dumps(document_links),
        # Publisher extensions and what was not read
        "extensions_json": dumps(extensions),
        "uncaptured_elements_json": dumps(dict(uncaptured)),
        # Provenance
        "iati_version": iati_version,
        "last_updated_datetime": _attr(act, "last-updated-datetime"),
        "source_xml_url": source_url,
        "downloaded_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }


def iter_activities(xml_path, source_url: str, max_related_siblings: int = 0):
    """Stream-parse one IATI file, yielding a `parse_activity` dict per activity.

    Organisation files (`<iati-organisations>`) yield nothing. Streaming keeps
    memory flat on Sida's 1.3 GB of XML.
    """
    version = None
    root = None
    depth = 0
    for event, el in ET.iterparse(str(xml_path), events=("start", "end")):
        if event == "start":
            if root is None:
                root = el
                if root.tag != "iati-activities":
                    return
                version = root.attrib.get("version")
            depth += 1
            continue
        depth -= 1
        if depth == 1 and el.tag == "iati-activity":
            yield parse_activity(el, source_url, iati_version=version,
                                 max_related_siblings=max_related_siblings)
            root.clear()


def parse_xml_to_rows(xml_bytes: bytes, source_url: str) -> list:
    """In-memory variant, same signature as `idrc_to_s3.parse_xml_to_rows`."""
    root = ET.fromstring(xml_bytes)
    if root.tag != "iati-activities":
        return []
    version = root.attrib.get("version")
    return [parse_activity(a, source_url, iati_version=version) for a in root.findall("iati-activity")]


# ---------------------------------------------------------------------------
# Research filter (rules are data: iati_research_rules.csv)
# ---------------------------------------------------------------------------

def load_rules(path=DEFAULT_RULES_CSV) -> list:
    """Read the rules CSV. Columns: rule_set, rule_id, field, qualifier, match, value, note."""
    rules = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if not row.get("rule_id") or row["rule_id"].startswith("#"):
                continue
            row = {k: (v or "").strip() for k, v in row.items()}
            if row["match"] == "regex":
                row["_re"] = re.compile(row["value"])
            rules.append(row)
    return rules


def _match(rule, candidate) -> bool:
    if candidate is None:
        return False
    m = rule["match"]
    if m == "exact":
        return str(candidate) == rule["value"]
    if m == "prefix":
        return str(candidate).startswith(rule["value"])
    if m == "regex":
        return bool(rule["_re"].search(str(candidate)))
    raise ValueError(f"rule {rule['rule_id']}: unknown match type {m!r}")


def apply_research_rules(row: dict, rules: list, rule_sets) -> dict:
    """Evaluate `rules` (restricted to `rule_sets`) against one parsed activity.

    Returns {"research_rule_ids": [...], "research_sector_share": float|None}.
    Supported `field` values:
      sector            qualifier = sector vocabulary ("1" = DAC 5-digit; an
                        absent vocabulary counts as "1"). Activity-level
                        sectors, else the sectors on its transactions.
      tag               qualifier = tag vocabulary
      participating_org qualifier = role ("1".."4", blank = any); value tested
                        against the org ref and the org name
      title, description, extension, iati_identifier, aid_type   (qualifier unused)
    """
    wanted = set(rule_sets)
    active = [r for r in rules if r["rule_set"] in wanted]
    if not active:
        return {"research_rule_ids": [], "research_sector_share": None}

    sectors = json.loads(row["sectors_json"])
    if not sectors:
        seen = set()
        for t in json.loads(row["transactions_json"]):
            for s in t.get("sectors") or []:
                key = (s.get("code"), s.get("vocabulary"))
                if key not in seen:
                    seen.add(key)
                    sectors.append({"code": s.get("code"), "vocabulary": s.get("vocabulary"),
                                    "percentage": None})
    tags = orgs = aid_types = None
    hits = []
    share = None
    for r in active:
        field = r["field"]
        ok = False
        if field == "sector":
            vocab = r["qualifier"] or "1"
            same_vocab = [s for s in sectors if (s.get("vocabulary") or "1") == vocab]
            matched = [s for s in same_vocab if _match(r, s.get("code"))]
            if matched:
                ok = True
                for s in matched:
                    pct = _to_float(s.get("percentage"))
                    if pct is None:
                        pct = 100.0 / len(same_vocab)
                    share = (share or 0.0) + pct
        elif field == "tag":
            if tags is None:
                tags = json.loads(row["tags_json"])
            ok = any((not r["qualifier"] or (t.get("vocabulary") or "") == r["qualifier"])
                     and _match(r, t.get("code")) for t in tags)
        elif field == "participating_org":
            if orgs is None:
                orgs = json.loads(row["participating_orgs_json"])
            ok = any((not r["qualifier"] or o.get("role") == r["qualifier"])
                     and (_match(r, o.get("ref")) or _match(r, o.get("name"))) for o in orgs)
        elif field == "aid_type":
            if aid_types is None:
                aid_types = json.loads(row["default_aid_types_json"])
            ok = any(_match(r, a.get("code")) for a in aid_types)
        elif field == "title":
            ok = _match(r, row.get("titles_json"))
        elif field == "description":
            ok = _match(r, row.get("descriptions_json"))
        elif field == "extension":
            ok = _match(r, row.get("extensions_json"))
        elif field == "iati_identifier":
            ok = _match(r, row.get("iati_identifier"))
        else:
            raise ValueError(f"rule {r['rule_id']}: unknown field {field!r}")
        if ok:
            hits.append(r["rule_id"])
    return {"research_rule_ids": hits,
            "research_sector_share": None if share is None else round(min(share, 100.0), 2)}


def load_publishers(path=DEFAULT_PUBLISHERS_CSV) -> list:
    """Read iati_publishers.csv, skipping blank lines and `#` comments."""
    with open(path, newline="", encoding="utf-8") as f:
        lines = [ln for ln in f if ln.strip() and not ln.lstrip().startswith("#")]
    return [{k: (v or "").strip() for k, v in row.items()} for row in csv.DictReader(lines)]


# ---------------------------------------------------------------------------
# From activities to awards (grain, roll-up, funder routing, ids, amounts)
# ---------------------------------------------------------------------------

# IATI org identifiers start with a registration-agency prefix. When the prefix
# starts with an ISO 3166-1 alpha-2 code (GB-COH-..., KE-NCB-...), that is the
# country the organisation is registered in. These prefixes are not countries:
NON_COUNTRY_ORG_PREFIXES = {"XM", "XI", "XR", "XE", "XN", "XX", "ZZ", "EU"}

# Strings publishers put in an organisation's name when they do not name it:
# the OECD DAC channel *categories* (CRSChannelCode codelist, codes ending 000)
# and plain placeholders. Sida's older activities name the implementer only as
# "University, college or other teaching institution, research institute or
# think-tank". These must never become an award's recipient organisation.
PLACEHOLDER_ORG_NAMES = frozenset(x.lower() for x in [
    "Public sector institutions", "Donor government", "Recipient government",
    "Third country government (delegated co-operation)",
    "Non-governmental organisations (NGOs) and civil society", "International NGO",
    "Donor country-based NGO", "Developing country-based NGO",
    "Public-private partnerships (PPPs) and networks", "Public-private partnership (PPP)",
    "Network", "Multilateral organisations", "United Nations agency, fund or commission (UN)",
    "European Union institution (EU)", "International Monetary Fund (IMF)",
    "World Bank Group (WB)", "World Trade Organisation (WTO)", "Regional development bank",
    "Other multilateral institution", "Others",
    "University, college or other teaching institution, research institute or think-tank",
    "Other", "Private sector institution", "Private sector in provider country",
    "Private sector in recipient country", "Private sector in third country",
    "Central government", "Local government", "Other public entities in donor country",
    "Other public entities in recipient country",
    "United Nations agency, fund or commission", "European Union institution",
    "Donor country NGO", "Dev. country based NGOs", "University College Research Other",
    "Regering i givarlandet", "Mottagarlandets regering", "Odefinierat",
    "Internal decision Letter of Co",
    "Sida administrative activity implemented by procured partner",
    "Misc", "Miscellaneous", "Not applicable", "N/A", "NA", "None", "Undefined", "Unknown",
    "Unspecified", "Various", "Multiple", "Correction", "Redacted", "Withheld", "TBC", "TBD",
    "Supplier Name Redacted", "Name Redacted", "Name withheld", "Not available", "Excluded",
])


def is_placeholder_org_name(name, also=()) -> bool:
    """True when `name` is empty, a channel category or a placeholder.

    `also`: extra names to reject for this activity (the publisher itself and
    its funding/extending organisations: a funder is not its own recipient).
    Sida writes "ACRONYM/Full name", so the part after the first "/" is tested too.
    """
    if not name or not str(name).strip():
        return True
    n = re.sub(r"[\u2010-\u2015\u2212]", "-", str(name)).strip().lower()
    candidates = {n}
    if "/" in n:
        candidates.add(n.split("/", 1)[1].strip())
    reject = PLACEHOLDER_ORG_NAMES | {str(a).strip().lower() for a in also if a}
    return bool(candidates & reject)


DEFAULT_AMOUNT_BASIS = ["commitment", "budget", "disbursement"]
DEFAULT_MIN_RESEARCH_SHARE = 50.0
DEFAULT_LANDING_PAGE = "https://d-portal.org/q.html?aid={iati_identifier}"


def _rule_sets(cfgs: list) -> list:
    out = []
    for c in cfgs:
        for s in (c.get("research_filter") or "").split(";"):
            if s.strip() and s.strip() not in out:
                out.append(s.strip())
    return out


def org_country_from_ref(ref):
    """`GB-COH-03259922` -> `GB`; `XM-DAC-41114` -> None."""
    if not ref or len(ref) < 3 or ref[2] != "-":
        return None
    cc = ref[:2].upper()
    if not cc.isalpha() or cc in NON_COUNTRY_ORG_PREFIXES:
        return None
    return cc


def _f(row, key):
    return _to_float(row.get(key))


def _own_amounts(row) -> dict:
    """{basis: (value, currency)} for one activity's own money."""
    out = {}
    for basis, key in (("commitment", "total_commitment"), ("budget", "total_budget")):
        v = _f(row, key)
        if v is not None:
            out[basis] = (v, row.get(f"{key}_currency"))
    d, e = _f(row, "total_disbursement"), _f(row, "total_expenditure")
    if d is not None or e is not None:
        out["disbursement"] = ((d or 0.0) + (e or 0.0),
                               row.get("total_disbursement_currency") or row.get("total_expenditure_currency"))
    return out


def derive_awards(rows: list, cfgs: list, rules: list, publisher: dict) -> dict:
    """Decide which activities are awards and fill the award-level columns in place.

    `cfgs` = this publisher's row(s) of iati_publishers.csv. One row is the
    usual case. Several rows route activities to different OpenAlex funders by
    `funder_org_refs` (a shared-reporting publisher such as Norway's file, which
    carries Norad, the MFA and other ministries: runbook §2.3.2).

    award_level (first cfg row):
      leaf  every activity that carries money is an award; a parent that only
            groups children is not (its title becomes `parent_title`). FCDO:
            programme (hierarchy 1) -> components (hierarchy 2) hold the money.
      top   every top-level activity is an award and its children roll up into
            it (money, organisations, countries). Sida: one contribution
            (hierarchy 1) is cut into hundreds of country/year slices.
    """
    main = cfgs[0]
    level = (main.get("award_level") or "leaf").lower()
    if level not in ("leaf", "top"):
        raise ValueError(f"award_level must be leaf or top, got {level!r}")
    basis_order = [b.strip() for b in (main.get("amount_basis") or "").split("|") if b.strip()] \
        or DEFAULT_AMOUNT_BASIS
    min_share = _to_float(main.get("min_research_share"))
    if min_share is None:
        min_share = DEFAULT_MIN_RESEARCH_SHARE
    id_re = re.compile(main["award_id_regex"]) if main.get("award_id_regex") else None
    landing = main.get("landing_page_template") or DEFAULT_LANDING_PAGE
    rule_sets = _rule_sets(cfgs)
    slug = publisher["slug"]

    by_id = {r["iati_identifier"]: r for r in rows}

    # --- hierarchy: parent/child links stated from either side ---------------
    parent_of: dict = {}
    for r in rows:
        iid = r["iati_identifier"]
        for rel in json.loads(r["related_activities_json"]):
            ref = rel.get("ref")
            if ref not in by_id or ref == iid:
                continue
            if rel.get("type") == "1":
                parent_of.setdefault(iid, ref)
            elif rel.get("type") == "2":
                parent_of.setdefault(ref, iid)
    children_of: dict = {}
    for child, parent in parent_of.items():
        children_of.setdefault(parent, []).append(child)

    # --- per-activity research rules (own evidence only) ----------------------
    own_hits = {}
    for r in rows:
        res = apply_research_rules(r, rules, rule_sets)
        own_hits[r["iati_identifier"]] = res

    stats = {"award_level": level, "awards": 0, "research_awards": 0,
             "research_awards_unrouted": 0, "shipped": 0, "rule_hits": Counter(),
             "shipped_by_funder": Counter(), "amount_basis": Counter()}
    seen_award_keys: dict = {}

    for r in rows:
        iid = r["iati_identifier"]
        kids = [by_id[c] for c in children_of.get(iid, [])]
        parent = by_id.get(parent_of.get(iid))
        own = _own_amounts(r)
        has_money = bool(own) or r.get("budgets_json") not in (None, "[]")

        r["parent_iati_identifier"] = parent["iati_identifier"] if parent else None
        r["parent_title"] = parent.get("title") if parent else None
        r["n_children"] = str(len(kids))

        # --- children roll-up --------------------------------------------------
        child_sums = {}
        if kids:
            for basis in ("commitment", "budget", "disbursement"):
                pairs = []
                for k in kids:
                    a = _own_amounts(k).get(basis)
                    if a:
                        pairs.append(a)
                total, cur, _ = _sum_by_currency(pairs)
                if total is not None:
                    child_sums[basis] = (total, cur)
        for basis in ("commitment", "budget", "disbursement"):
            v = child_sums.get(basis)
            r[f"children_total_{basis}"] = None if v is None else repr(round(v[0], 2))
            r[f"children_total_{basis}_currency"] = None if v is None else v[1]

        # --- is this activity an award? ----------------------------------------
        if level == "top":
            is_award = parent is None
        else:
            is_award = has_money or not kids
        r["is_award"] = "true" if is_award else "false"
        r["award_level"] = level

        # --- organisations: own, plus children's when rolling up ---------------
        orgs = json.loads(r["participating_orgs_json"])
        txs = json.loads(r["transactions_json"])
        if level == "top" and kids:
            seen = {(o.get("role"), o.get("ref"), o.get("name")) for o in orgs}
            child_orgs = []
            for k in kids:
                for o in json.loads(k["participating_orgs_json"]):
                    key = (o.get("role"), o.get("ref"), o.get("name"))
                    if key not in seen:
                        seen.add(key)
                        child_orgs.append({kk: o.get(kk) for kk in
                                           ("role", "type", "ref", "name", "crs_channel_code")})
                txs = txs + json.loads(k["transactions_json"])
            r["children_participating_orgs_json"] = json.dumps(child_orgs, ensure_ascii=False)
            orgs = orgs + child_orgs
        else:
            r["children_participating_orgs_json"] = None

        # Lead (recipient) organisation: who receives the money.
        #  1. the receiver-org with the largest outgoing commitments + disbursements
        #  2. else the first implementing org (role 4)
        #  3. else the first accountable org (role 2) that is not the reporter
        lead = None
        lead_basis = None
        own_names = {publisher["short_name"], r.get("reporting_org_name")} | {
            o.get("name") for o in orgs if o.get("role") in ("1", "3")}
        received = {}
        order = []
        for t in txs:
            ro = t.get("receiver_org")
            if t.get("type") in ("2", "3") and ro and not is_placeholder_org_name(ro.get("name"), own_names):
                key = (ro.get("ref"), ro.get("name"))
                if key not in received:
                    order.append(key)
                    received[key] = [0.0, ro]
                received[key][0] += abs(_to_float(t.get("value")) or 0.0)
        if received:
            best = max(order, key=lambda k: received[k][0])
            lead, lead_basis = received[best][1], "transaction_receiver"
        if lead is None:
            for o in orgs:
                if o.get("role") == "4" and not is_placeholder_org_name(o.get("name"), own_names):
                    lead, lead_basis = o, "implementing"
                    break
        if lead is None:
            for o in orgs:
                if o.get("role") == "2" and not is_placeholder_org_name(o.get("name"), own_names) \
                        and o.get("ref") != r.get("reporting_org_ref"):
                    lead, lead_basis = o, "accountable"
                    break
        r["lead_org_name"] = lead.get("name") if lead else None
        r["lead_org_ref"] = lead.get("ref") if lead else None
        r["lead_org_type"] = lead.get("type") if lead else None
        r["lead_org_country"] = org_country_from_ref(lead.get("ref")) if lead else None
        r["lead_org_basis"] = lead_basis

        # --- recipient countries / regions (own, transaction-level, children) --
        countries, regions = [], []
        def add_geo(row_, txs_):
            for c in json.loads(row_["recipient_countries_json"]):
                if c.get("code") and c["code"] not in countries:
                    countries.append(c["code"])
            for g in json.loads(row_["recipient_regions_json"]):
                if g.get("code") and g["code"] not in regions:
                    regions.append(g["code"])
            for t in txs_:
                c = (t.get("recipient_country") or "").upper()
                if c and c not in countries:
                    countries.append(c)
                g = t.get("recipient_region")
                if g and g not in regions:
                    regions.append(g)
        add_geo(r, json.loads(r["transactions_json"]))
        if level == "top" and kids and not countries and not regions:
            for k in kids:
                add_geo(k, json.loads(k["transactions_json"]))
        r["recipient_country_codes"] = "|".join(countries) or None
        r["recipient_region_codes"] = "|".join(regions) or None

        # --- research admission --------------------------------------------------
        res = own_hits[iid]
        ids = list(res["research_rule_ids"])
        share = res["research_sector_share"]
        sector_rule_ids = {x["rule_id"] for x in rules if x["field"] == "sector"}
        non_sector_hit = any(i not in sector_rule_ids for i in ids)
        child_share = child_programme_share = None
        if level == "top" and kids:
            # Money-weighted share of the children that are research on their
            # own evidence: a programme (non-sector) rule counts the whole
            # child, a sector rule counts the child's research sector share.
            tot = adm = adm_programme = 0.0
            for k in kids:
                ka = _own_amounts(k)
                w = abs(next((ka[b][0] for b in basis_order if b in ka), 0.0))
                kres = own_hits[k["iati_identifier"]]
                kshare = kres["research_sector_share"]
                k_non_sector = any(i not in sector_rule_ids for i in kres["research_rule_ids"])
                tot += w
                adm += w * (1.0 if k_non_sector else (kshare or 0.0) / 100.0)
                adm_programme += w if k_non_sector else 0.0
                for i in kres["research_rule_ids"]:
                    if f"child:{i}" not in ids and i not in ids:
                        ids.append(f"child:{i}")
            if tot > 0:
                child_share = round(100.0 * adm / tot, 2)
                child_programme_share = round(100.0 * adm_programme / tot, 2)
        # Admitted when: a programme rule matches the activity itself; or its
        # research sector share reaches the threshold; or (rolled-up parents)
        # children under a programme rule hold that share of the money; or the
        # parent has no sectors of its own and its children reach the threshold.
        admitted = non_sector_hit or (share is not None and share >= min_share)
        if not admitted and child_programme_share is not None and child_programme_share >= min_share:
            admitted = True
        if not admitted and share is None and child_share is not None and child_share >= min_share:
            admitted = True
        r["research_rule_ids"] = json.dumps(ids) if ids else None
        r["research_sector_share"] = None if share is None else repr(share)
        r["children_research_share"] = None if child_share is None else repr(child_share)
        r["is_research"] = "true" if admitted else "false"

        # --- funder routing -------------------------------------------------------
        funder_cfg = None
        refs_123 = {o.get("ref") for o in orgs if o.get("role") in ("1", "2", "3") and o.get("ref")}
        default_cfg = None
        for c in cfgs:
            wanted = [x.strip() for x in (c.get("funder_org_refs") or "").split("|") if x.strip()]
            if not wanted:
                default_cfg = default_cfg or c
            elif refs_123 & set(wanted):
                funder_cfg = c
                break
        funder_cfg = funder_cfg or default_cfg
        r["openalex_funder_id"] = (funder_cfg or {}).get("openalex_funder_id") or None
        r["publisher_short_name"] = publisher["short_name"]
        r["publisher_slug"] = slug
        r["publisher_org_id"] = publisher["org_id"]
        r["provenance"] = f"iati_{slug}"

        # --- award id, landing page, amount --------------------------------------
        award_id = iid
        if id_re is not None:
            m = id_re.search(iid)
            if m and m.groups() and m.group(1):
                award_id = m.group(1)
        r["funder_award_id"] = award_id
        # Landing page: the publisher's own page. `document_link` = the activity's
        # own "activity web page" link (IATI document category A12).
        page = None
        if landing == "document_link":
            for dl in json.loads(r["document_links_json"]):
                if dl.get("url") and "A12" in (dl.get("categories") or []):
                    page = dl["url"]
                    break
        else:
            page = landing.format(iati_identifier=iid, funder_award_id=award_id,
                                  parent_or_self=r["parent_iati_identifier"] or iid)
        r["landing_page_url"] = page or DEFAULT_LANDING_PAGE.format(iati_identifier=iid)

        amount = currency = basis_used = None
        for b in basis_order:
            if b in own and own[b][0] > 0:
                amount, currency, basis_used = own[b][0], own[b][1], b
                break
        if amount is None and level == "top":
            for b in basis_order:
                if b in child_sums and child_sums[b][0] > 0:
                    amount, currency, basis_used = child_sums[b][0], child_sums[b][1], f"children_{b}"
                    break
        r["award_amount"] = None if amount is None else repr(round(amount, 2))
        r["award_currency"] = currency if amount is not None else None
        r["award_amount_basis"] = basis_used

        # Award dates: own; a rolled-up parent with none takes its children's span
        start = r.get("actual_start") or r.get("planned_start")
        end = r.get("actual_end") or r.get("planned_end")
        if level == "top" and kids and (not start or not end):
            ks = [k.get("actual_start") or k.get("planned_start") for k in kids]
            ke = [k.get("actual_end") or k.get("planned_end") for k in kids]
            start = start or min([x for x in ks if x], default=None)
            end = end or max([x for x in ke if x], default=None)
        r["award_start_date"] = start
        r["award_end_date"] = end

        ship = is_award and admitted and bool(r["openalex_funder_id"])
        r["ship"] = "true" if ship else "false"
        if is_award:
            stats["awards"] += 1
            if admitted:
                stats["research_awards"] += 1
                stats["rule_hits"].update(ids)
                if not r["openalex_funder_id"]:
                    stats["research_awards_unrouted"] += 1
        if ship:
            stats["shipped"] += 1
            stats["shipped_by_funder"][r["openalex_funder_id"]] += 1
            stats["amount_basis"][basis_used or "none"] += 1
            key = (r["openalex_funder_id"], award_id.lower())
            if key in seen_award_keys:
                # A duplicate funder_award_id silently merges two awards downstream.
                raise RuntimeError(
                    f"{slug}: funder_award_id collision {award_id!r} from {iid!r} and "
                    f"{seen_award_keys[key]!r}. Fix award_id_regex in iati_publishers.csv.")
            seen_award_keys[key] = iid

    stats["shipped_by_funder"] = dict(stats["shipped_by_funder"])
    stats["amount_basis"] = dict(stats["amount_basis"])
    return stats


# ---------------------------------------------------------------------------
# Discovery and download
# ---------------------------------------------------------------------------

def http_get(url: str, retries: int = 4, stream: bool = False, timeout: int = 180):
    import requests  # lazy: importing this module must not need requests
    last_err = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=timeout, headers=HTTP_HEADERS, stream=stream)
            log(f"  GET {url} -> {resp.status_code}")
            resp.raise_for_status()
            return resp
        except Exception as e:  # noqa: BLE001 - retried, then raised
            last_err = e
            log(f"  retry {attempt + 1}/{retries} after error: {e}")
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


def _cached_json(url: str, cache_dir: Path, name: str, max_age_hours: float = 20.0):
    cache_dir.mkdir(parents=True, exist_ok=True)
    p = cache_dir / name
    if p.exists() and (time.time() - p.stat().st_mtime) < max_age_hours * 3600:
        return json.loads(p.read_text(encoding="utf-8"))
    resp = http_get(url)
    p.write_bytes(resp.content)
    return resp.json()


def resolve_publisher(ref: str, cache_dir: Path) -> dict:
    """Accept a registry short name (`sida`) or an IATI org id (`SE-0`).

    Returns {"short_name", "org_id", "name"} from the Bulk Data Service's
    reporting-orgs index.
    """
    idx = _cached_json(BULK_REPORTING_ORGS, cache_dir / "_index", "reporting-orgs.json")
    orgs = idx["reporting_orgs"]
    want = ref.strip().lower()
    for o in orgs:
        if (o.get("short_name") or "").lower() == want:
            break
    else:
        hits = [o for o in orgs if (o.get("organisation_identifier") or "").lower() == want
                or (o.get("iati_identifier") or "").lower() == want]
        if len(hits) != 1:
            raise RuntimeError(
                f"Publisher {ref!r}: {len(hits)} matches in the reporting-orgs index. "
                "Use the IATI Registry short name (e.g. sida, norad, fcdo).")
        o = hits[0]
    return {"short_name": o["short_name"], "org_id": o.get("organisation_identifier"),
            "name": o.get("human_readable_name")}


def discover_xml_urls(publisher: dict, source: str, cache_dir: Path) -> list:
    """Enumerate one publisher's IATI files. Returns [{name, url, source_url, hash, size}].

    Organisation files are not filtered here (names are unreliable); the parser
    skips any file whose root element is not `iati-activities`.
    """
    short = publisher["short_name"]
    out = []
    if source == "bulk":
        idx = _cached_json(BULK_DATASETS_INDEX, cache_dir / "_index", "datasets-full.json")
        for d in idx["datasets"]:
            if d.get("reporting_org_short_name") != short:
                continue
            good = d.get("last_known_good_dataset") or {}
            url = good.get("cached_dataset_xml_url") or good.get("cached_dataset_url_xml")
            if not url:
                log(f"  WARNING: {d.get('short_name')} has no cached copy in the bulk service "
                    f"(publisher URL {d.get('source_url')}); skipped")
                continue
            out.append({"name": d["short_name"], "url": url, "source_url": d.get("source_url"),
                        "hash": good.get("hash"), "size": good.get("content_length")})
    elif source == "registry":
        start, total = 0, None
        while total is None or start < total:
            url = f"{REGISTRY_API}/package_search?fq=organization:{short}&rows=200&start={start}"
            body = http_get(url).json()
            if not body.get("success"):
                raise RuntimeError(f"IATI Registry error: {body.get('error')}")
            total = body["result"]["count"]
            results = body["result"]["results"]
            for pkg in results:
                for res in pkg.get("resources", []):
                    out.append({"name": pkg["name"], "url": res.get("url"),
                                "source_url": res.get("url"), "hash": None,
                                "size": res.get("size")})
            start += 200
            if not results and start < total:
                log(f"  empty registry page at start={start - 200}; continuing to {total}")
    else:
        raise ValueError(f"unknown source {source!r}")
    out.sort(key=lambda d: d["name"])
    log(f"{short}: found {len(out)} IATI files via {source}")
    return out


def fetch_to_cache(ent: dict, cache_dir: Path, short: str) -> Path:
    """Download one file into the cache unless the cached copy has the same hash."""
    d = cache_dir / short
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{ent['name']}.xml"
    stamp = d / f"{ent['name']}.xml.sha1"
    if path.exists() and ent.get("hash") and stamp.exists() \
            and stamp.read_text(encoding="utf-8").strip() == ent["hash"]:
        log(f"  cache hit {path.name}")
        return path
    resp = http_get(ent["url"], stream=True)
    tmp = path.with_suffix(".part")
    sha = hashlib.sha1()
    n = 0
    with open(tmp, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            f.write(chunk)
            sha.update(chunk)
            n += len(chunk)
    tmp.replace(path)
    if ent.get("hash") and sha.hexdigest() != ent["hash"]:
        log(f"  WARNING: {path.name} sha1 differs from the index (file changed since the index was built)")
    stamp.write_text(ent.get("hash") or sha.hexdigest(), encoding="utf-8")
    log(f"  downloaded {path.name}: {n:,} bytes")
    return path


# ---------------------------------------------------------------------------
# Per-publisher run
# ---------------------------------------------------------------------------

def process_publisher(ref: str, cfg_by_ref: dict, rules: list, args) -> dict:
    publisher = resolve_publisher(ref, args.cache_dir)
    short = publisher["short_name"]
    slug = slugify(short)
    publisher["slug"] = slug
    # The CSV may name the publisher by short name or by IATI organisation id.
    cfgs = (cfg_by_ref.get(short.lower()) or cfg_by_ref.get((publisher["org_id"] or "").lower())
            or cfg_by_ref.get(ref.lower()))
    if cfgs is None:
        log(f"WARNING: {ref} is not in the publishers CSV; parsing with NO research filter "
            "and NO funder (nothing admitted, nothing uploaded)")
        cfgs = [{"publisher_ref": ref, "research_filter": ""}]
    rule_sets = _rule_sets(cfgs)
    log("=" * 60)
    log(f"{short} ({publisher['org_id']}; {publisher['name']}) -> provenance iati_{slug}")
    if not rule_sets:
        log("  WARNING: no research_filter for this publisher; nothing will be admitted")

    files = discover_xml_urls(publisher, args.source, args.cache_dir)
    if args.limit_files:
        files = files[: args.limit_files]
        log(f"Smoke-test mode: limited to first {len(files)} files")

    by_id: dict = {}
    n_parsed = n_dupes = n_no_id = 0
    failed = []
    t0 = time.time()
    for i, ent in enumerate(files, 1):
        log(f"[{i}/{len(files)}] {ent['name']} ({(ent.get('size') or 0) / 1e6:.1f} MB)")
        try:
            path = fetch_to_cache(ent, args.cache_dir, short)
            n_file = 0
            for row in iter_activities(path, ent.get("source_url") or ent["url"],
                                       max_related_siblings=args.max_related_siblings):
                n_file += 1
                n_parsed += 1
                iid = row["iati_identifier"]
                if not iid:
                    n_no_id += 1
                    continue
                row["dataset_name"] = ent["name"]
                prev = by_id.get(iid)
                if prev is not None:
                    n_dupes += 1
                    # keep the most recently updated copy
                    if (row.get("last_updated_datetime") or "") < (prev.get("last_updated_datetime") or ""):
                        continue
                by_id[iid] = row
                if args.limit and len(by_id) >= args.limit:
                    break
        except Exception as e:  # noqa: BLE001 - recorded, run fails closed below
            failed.append({"name": ent["name"], "error": f"{type(e).__name__}: {e}"})
            log(f"  ERROR in {ent['name']}: {type(e).__name__}: {e}")
            continue
        elapsed = time.time() - t0
        eta = elapsed / i * (len(files) - i)
        log(f"  parsed {n_file:,} activities (unique so far: {len(by_id):,}; "
            f"elapsed {elapsed:.0f}s, ETA {eta:.0f}s)")
        if args.limit and len(by_id) >= args.limit:
            log(f"Smoke-test row limit hit ({args.limit}); stopping")
            break

    if failed and not args.allow_partial:
        raise RuntimeError(
            f"{short}: {len(failed)} of {len(files)} files failed ({failed[:3]}). "
            "Refusing to write a partial corpus; re-run, or pass --allow-partial.")

    rows = list(by_id.values())
    stats = derive_awards(rows, cfgs, rules, publisher)
    rule_counts = stats["rule_hits"]
    n_research = stats["research_awards"]

    summary = {
        "publisher": short, "slug": slug, "org_id": publisher["org_id"],
        "source": args.source, "files": len(files), "files_failed": failed,
        "activities_parsed": n_parsed, "duplicate_identifiers": n_dupes,
        "missing_identifier": n_no_id, "activities_unique": len(rows),
        "awards": stats["awards"], "research_awards": n_research,
        "research_awards_unrouted": stats["research_awards_unrouted"],
        "shipped": stats["shipped"], "award_level": stats["award_level"],
        "rule_sets": rule_sets, "rule_hits": dict(rule_counts.most_common()),
        "shipped_by_funder": stats["shipped_by_funder"],
        "amount_basis": stats["amount_basis"],
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "limit": args.limit, "limit_files": args.limit_files,
    }
    log(f"{short}: {n_parsed:,} parsed, {len(rows):,} unique, {n_dupes:,} duplicate ids; "
        f"{stats['awards']:,} awards at level '{stats['award_level']}', {n_research:,} research-admitted, "
        f"{stats['shipped']:,} shipped ({dict(rule_counts.most_common(5))})")

    import pandas as pd  # lazy: the parser itself needs only the stdlib

    args.output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    # All source columns are strings (IATI text + JSON-serialised substructures).
    # Force string dtype so pyarrow does not infer null-heavy columns as int.
    df = df.astype("string")
    all_path = args.output_dir / f"iati_{slug}_activities.parquet"
    df.to_parquet(all_path, index=False)
    log(f"Wrote {all_path} ({len(df):,} rows, every activity; local only)")

    research_df = df[df["ship"] == "true"] if len(df) else df
    research_path = args.output_dir / f"iati_{slug}_projects.parquet"
    research_df.to_parquet(research_path, index=False)
    log(f"Wrote {research_path} ({len(research_df):,} rows: research-admitted awards with a funder)")

    if args.jsonl:
        import gzip
        jl = args.output_dir / f"iati_{slug}_activities.jsonl.gz"
        with gzip.open(jl, "wt", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        log(f"Wrote {jl}")

    (args.output_dir / f"iati_{slug}_summary.json").write_text(
        json.dumps(summary, indent=1, ensure_ascii=False), encoding="utf-8")

    if args.skip_upload:
        log("--skip-upload set; not uploading.")
        return summary
    if args.limit or args.limit_files:
        log("Smoke-test limits set; refusing to upload a truncated corpus.")
        return summary
    if len(research_df) == 0:
        log("No research-admitted activities; nothing to upload.")
        return summary

    s3_key = f"awards/iati_{slug}/iati_{slug}_projects.parquet"
    upload_to_s3(research_path, len(research_df), s3_key, args.allow_shrink)
    summary["s3_path"] = f"s3://{S3_BUCKET}/{s3_key}"
    return summary


def upload_to_s3(parquet_path: Path, new_count: int, s3_key: str, allow_shrink: bool) -> None:
    """Upload, failing closed if the new corpus is smaller (runbook §1.4)."""
    import boto3  # imported here so parsing and smoke-tests don't require boto3
    import pyarrow.parquet as pq
    from botocore.exceptions import ClientError

    s3 = boto3.client("s3")
    prev_count = None
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        tmp = parquet_path.with_suffix(".previous.parquet")
        s3.download_file(S3_BUCKET, s3_key, str(tmp))
        prev_count = pq.ParquetFile(str(tmp)).metadata.num_rows
        tmp.unlink()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in ("404", "NoSuchKey", "NotFound"):
            raise
        log("  no existing parquet in S3; first ingest")
    if prev_count is not None:
        log(f"  existing S3 parquet has {prev_count:,} rows; new has {new_count:,}")
        if new_count < prev_count and not allow_shrink:
            raise RuntimeError(
                f"Refusing to overwrite s3://{S3_BUCKET}/{s3_key}: new corpus ({new_count:,}) is smaller "
                f"than the existing one ({prev_count:,}). Investigate, or pass --allow-shrink.")
    log(f"Uploading to s3://{S3_BUCKET}/{s3_key}")
    s3.upload_file(str(parquet_path), S3_BUCKET, s3_key)
    log("Upload complete.")


def main() -> None:
    p = argparse.ArgumentParser(description="Generic IATI publisher XML -> parquet -> S3")
    p.add_argument("--publisher", nargs="+", default=None,
                   help="IATI Registry short name(s) or org id(s), e.g. sida norad fcdo")
    p.add_argument("--all", action="store_true",
                   help="Process every publisher listed in --publishers-csv")
    p.add_argument("--publishers-csv", type=Path, default=DEFAULT_PUBLISHERS_CSV,
                   help="publisher_ref, openalex_funder_id, funder_name, research_filter, ...")
    p.add_argument("--rules-csv", type=Path, default=DEFAULT_RULES_CSV,
                   help="Research filter rules (data, not code)")
    p.add_argument("--source", choices=["bulk", "registry"], default="bulk",
                   help="bulk = bulk-data.iatistandard.org cache; registry = publisher's own URLs")
    p.add_argument("--cache-dir", type=Path, default=Path("/tmp/iati_cache"),
                   help="Where downloaded XML is kept between runs (resumable)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"),
                   help="Local output directory")
    p.add_argument("--limit", type=int, default=None,
                   help="Smoke-test: stop after N activities per publisher (never uploads)")
    p.add_argument("--limit-files", type=int, default=None,
                   help="Smoke-test: only the first N XML files per publisher (never uploads)")
    p.add_argument("--max-related-siblings", type=int, default=0,
                   help="Sibling related-activity refs kept per activity (parents/children always kept)")
    p.add_argument("--jsonl", action="store_true", help="Also write a gzipped JSONL of every activity")
    p.add_argument("--skip-upload", action="store_true", help="Parse and write locally only")
    p.add_argument("--allow-shrink", action="store_true",
                   help="Override the never-shrink check (runbook §1.4)")
    p.add_argument("--allow-partial", action="store_true",
                   help="Write output even if some files failed to download or parse")
    args = p.parse_args()

    publishers = load_publishers(args.publishers_csv)
    cfg_by_ref: dict = {}
    for r in publishers:
        cfg_by_ref.setdefault(r["publisher_ref"].lower(), []).append(r)
    if args.all:
        refs = list(dict.fromkeys(r["publisher_ref"] for r in publishers))
    elif args.publisher:
        refs = args.publisher
    else:
        p.error("give --publisher REF [REF ...] or --all")

    rules = load_rules(args.rules_csv)
    log(f"IATI -> S3 pipeline starting: {len(refs)} publisher(s), {len(rules)} research rules")

    summaries = []
    for ref in refs:
        summaries.append(process_publisher(ref, cfg_by_ref, rules, args))

    log("=" * 60)
    for s in summaries:
        log(f"{s['publisher']}: {s['activities_unique']:,} activities, {s['awards']:,} awards, "
            f"{s['research_awards']:,} research-admitted, {s['shipped']:,} shipped")


if __name__ == "__main__":
    main()
