"""Shared config for the AHA gold-set precision/recall analysis.

The gold set is AHA's manually QC'd verification of Dimensions' candidate
publications for calendar 2024 (one Excel tab per month). We use it to measure
how well OpenAlex links works to AHA as a funder (and to specific AHA grants),
and to diagnose the misses.
"""
import os
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"
TABLES = HERE / "tables"
CACHE = HERE / ".cache"
for d in (DATA, TABLES, CACHE):
    d.mkdir(exist_ok=True)

# Raw partner workbook — kept OUT of the repo (PII + partner data). Override with
# AHA_XLSX env var; defaults to Kyle's desktop copy.
RAW_XLSX = Path(os.environ.get(
    "AHA_XLSX",
    r"C:\Users\kyled\OneDrive\Desktop\PMCID_Calendar year 2025_Kyle copy.xlsx",
))

# OpenAlex
API_KEY = os.environ.get("OPENALEX_API_KEY", "XXX")
AHA_FUNDER_ID = "F4320306230"          # American Heart Association
API_BASE = "https://api.openalex.org"

# --- Workbook layout (1-based column indices; headers in row 1) ---------------
COL = {
    "doi": 3, "pmid": 4, "pmcid": 5, "title": 7,
    "funder": 36,                       # "Funder" (semicolon list, all funders)
    "uid_grants": 39,                   # Dimensions grant UIDs (grant.NNNN)
    "grants": 40,                       # "Supporting Grants" (codes, all funders)
    "pmcid_status": 57,                 # "PMCID status"
    "notes": 59,                        # "Recommendation / Notes:"
    "status": 64,                       # "Status"  <- the verification/compliance signal
    "pi_last": 65, "pi_first": 66,      # PII — never checked in
}
MONTH_SHEETS = [f"{m} 2024" for m in (
    "January February March April May June July August September October "
    "November December".split())]

# --- Status -> gold class -----------------------------------------------------
# Conditional formatting colours are driven by the Status column:
#   Compliant -> green, "Out of compliance" -> red, Pending -> amber,
#   Non-responsive -> magenta, N/A -> grey.
POSITIVE_STATUS = {"Compliant", "Out of compliance"}   # verified AHA-funded
PENDING_STATUS = {"Pending", "Non-responsive"}          # verification in progress
# "N/A" (grey) is mixed: false positives (disclosure/editorial/unlink) vs
# "theirs but no grant ID" — disambiguated by note (see na_note_category).

# AHA grant-ID format: 2-digit year + program code (POST, CDA, SDG, SFRN, TPA,
# IPA, EIA, AIREA, ...) + serial, e.g. 23POST1022457, 20CDA35350059, 17SDG33670349.
AHA_GRANT_RE = re.compile(r"^\d{2}[A-Z]{2,6}\d{3,}$")

# N/A note -> category. Order matters (first match wins).
NA_NOTE_RULES = [
    ("remove_erroneous", re.compile(
        r"unlink|no aha award|erroneous|\bremove\b|not us|initials aha|"
        r"aha is a procedure|not sure how this got selected|not cited anywhere", re.I)),
    ("doc_type_excluded", re.compile(
        r"preprint|letter to (the )?editor|\bcase study\b|\bcorrection\b|"
        r"conference|proceedings|aha sessions", re.I)),
    ("aha_authored", re.compile(
        r"editorial|position statement|guideline|aha publication|"
        r"presented at aha|aha meeting", re.I)),
    ("theirs_no_grant_id", re.compile(
        r"endowed chair|did not cite the award|needs to link|grant id not included", re.I)),
    ("not_funded_disclosure", re.compile(
        r"disclosure|no funding|not funded|acknowledg|no.*grant id|"
        r"not a grant id|no.*award", re.I)),
]


def na_note_category(note: str) -> str:
    if not note or not str(note).strip():
        return "na_blank_note"
    for name, rx in NA_NOTE_RULES:
        if rx.search(str(note)):
            return name
    return "na_other"
