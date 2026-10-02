"""Award country lookup: country/award_country_lookup.csv as a SQL relation (the file is versioned with the code).

The CSV maps the free-text `affiliation.country` of an award to ISO 3166-1 alpha-2 and says, per source, what that field
means: the organisation's country (most sources), a US state (RWJF's 'MA' is Massachusetts, not Morocco), the country the
project takes place in (IDRC), or a constant the ingest notebook wrote (Humboldt: 'Germany'). Columns: value (lower-case,
trimmed; '*' = the row describing the source), provenance_scope ('*' or one provenance), iso2, confidence (high / medium
usable, low not), meaning, note. sql/api_payload.sql applies it; country/build_country_lookup.py documents the columns.
"""
import csv
from pathlib import Path

COLUMNS = ("value", "provenance_scope", "iso2", "confidence")
CONFIDENCE = {"high", "medium", "low"}
MEANING = {"organisation", "assumed_domestic", "us_state", "project_country", "assumed", "mixed", "not_a_country"}


def read_lookup(package_root):
    """Rows of the lookup, validated: unique (value, scope), normalised value, 2-letter upper-case iso2 or empty."""
    with open(Path(package_root) / "country" / "award_country_lookup.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    seen = set()
    for row in rows:
        key = (row["value"], row["provenance_scope"])
        iso2, usable, source_row = row["iso2"], row["confidence"] in ("high", "medium"), row["value"] == "*"
        ok = (row["value"] and row["value"] == row["value"].strip().lower() and row["provenance_scope"]
              and "|" not in row["value"] + row["provenance_scope"]
              and row["confidence"] in CONFIDENCE and row["meaning"] in MEANING and key not in seen
              and (iso2 == "" or (len(iso2) == 2 and iso2.isalpha() and iso2.isupper()))
              and (source_row or not usable or iso2)                     # a usable value row names a country
              and (not source_row or (row["provenance_scope"] != "*" and iso2 == "")))   # a source row describes one provenance
        if not ok:
            raise RuntimeError("COUNTRY_LOOKUP_ROW_INVALID: " + repr(row))
        seen.add(key)
    if not rows:
        raise RuntimeError("COUNTRY_LOOKUP_EMPTY")
    return rows


def sql_string(value):
    """A SQL string literal. Backslash escapes: in Spark SQL 'a''b' is two adjacent literals, read as ab."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def lookup_relation_sql(package_root):
    """SELECT over an inline VALUES list: value, provenance_scope, iso2, confidence."""
    values = ",".join("(" + ",".join(sql_string(row[c]) for c in COLUMNS) + ")" for row in read_lookup(package_root))
    return f"SELECT * FROM VALUES {values} AS award_country_lookup({','.join(COLUMNS)})"


def country_code(rows, provenance, country):
    """The lookup's answer for one (provenance, country) in Python: the reference for the map lookup in api_payload.sql."""
    value = (country or "").strip().lower()
    usable = lambda r: r["confidence"] in ("high", "medium")
    table = {(r["provenance_scope"], r["value"]): r for r in rows}
    if value == "*" or not value:
        return None
    if (provenance, value) in table:                                     # a row for this provenance and value decides
        row = table[(provenance, value)]
        return row["iso2"] if usable(row) else None
    source = table.get((provenance, "*"))
    if source is None or not usable(source) or ("*", value) not in table:
        return None
    row = table[("*", value)]
    return row["iso2"] if usable(row) else None
