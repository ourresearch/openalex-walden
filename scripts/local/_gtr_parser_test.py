"""
Ad-hoc parser test for gtr_to_s3.py — verifies the patched parsers without
hitting the live GtR API (which is currently down).

Tests:
  1. parse_funds_xml extracts (project_id, amount, currency) from a /funds payload
  2. parse_single_project preserves ALL existing fields after the patch
  3. process_projects merge logic correctly populates amount via /funds map

Run: python _gtr_parser_test.py
"""

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

# Import parsers from the script under test
sys.path.insert(0, str(Path(__file__).parent))
from gtr_to_s3 import parse_funds_xml, parse_single_project, NS  # noqa: E402


# -----------------------------------------------------------------------------
# Test 1: /funds parser
# -----------------------------------------------------------------------------
# Schema mirrors live /funds response: valuePounds is self-closing with
# ns1:amount/ns1:currencyCode attributes, and the project backlink uses
# rel="FUNDED" (not "PROJECT").
FUNDS_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<ns2:funds xmlns:ns1="http://gtr.rcuk.ac.uk/gtr/api"
           xmlns:ns2="http://gtr.rcuk.ac.uk/gtr/api/fund"
           ns1:totalPages="17440" ns1:totalSize="174397" ns1:size="2" ns1:page="1">
  <ns2:fund ns1:id="fund-uuid-A">
    <ns1:links>
      <ns1:link ns1:rel="FUNDER" ns1:href="http://gtr.ukri.org/gtr/api/organisations/EPSRC"/>
      <ns1:link ns1:rel="FUNDED" ns1:href="http://gtr.ukri.org/gtr/api/projects/PROJ-UUID-A"
                ns1:start="2020-01-01" ns1:end="2023-12-31"/>
    </ns1:links>
    <ns2:start>2020-01-01Z</ns2:start>
    <ns2:end>2023-12-31Z</ns2:end>
    <ns2:valuePounds ns1:amount="123456" ns1:currencyCode="GBP"/>
    <ns2:category>INCOME_ACTUAL</ns2:category>
  </ns2:fund>
  <ns2:fund ns1:id="fund-uuid-B-extension">
    <ns1:links>
      <ns1:link ns1:rel="FUNDED" ns1:href="http://gtr.ukri.org/gtr/api/projects/PROJ-UUID-A"/>
    </ns1:links>
    <ns2:valuePounds ns1:amount="50000" ns1:currencyCode="GBP"/>
    <ns2:category>INCOME_ACTUAL</ns2:category>
  </ns2:fund>
  <ns2:fund ns1:id="fund-uuid-A-expenditure">
    <ns1:links>
      <ns1:link ns1:rel="FUNDED" ns1:href="http://gtr.ukri.org/gtr/api/projects/PROJ-UUID-A"/>
    </ns1:links>
    <ns2:valuePounds ns1:amount="999999" ns1:currencyCode="GBP"/>
    <ns2:category>EXPENDITURE_ACTUAL</ns2:category>
  </ns2:fund>
  <ns2:fund ns1:id="fund-uuid-no-value">
    <ns1:links>
      <ns1:link ns1:rel="FUNDED" ns1:href="http://gtr.ukri.org/gtr/api/projects/PROJ-UUID-NOVAL"/>
    </ns1:links>
    <ns2:category>INCOME_ACTUAL</ns2:category>
  </ns2:fund>
  <ns2:fund ns1:id="fund-uuid-no-project">
    <ns1:links>
      <ns1:link ns1:rel="FUNDER" ns1:href="http://gtr.ukri.org/gtr/api/organisations/MRC"/>
    </ns1:links>
    <ns2:valuePounds ns1:amount="77777" ns1:currencyCode="GBP"/>
    <ns2:category>INCOME_ACTUAL</ns2:category>
  </ns2:fund>
</ns2:funds>
"""


def test_funds_parser():
    funds = parse_funds_xml(FUNDS_XML)
    pids = [f["project_id"] for f in funds]

    # Both INCOME_ACTUAL records for project A and the EXPENDITURE_ACTUAL one
    # should all be parsed; download_all_funds applies the category filter.
    assert pids.count("PROJ-UUID-A") == 3, f"expected 3 funds for PROJ-UUID-A, got {pids.count('PROJ-UUID-A')}"

    # Sanity: amounts are read from ns1:amount attribute, currency from ns1:currencyCode
    a_funds = [f for f in funds if f["project_id"] == "PROJ-UUID-A"]
    a_income = [f for f in a_funds if f["category"] == "INCOME_ACTUAL"]
    assert len(a_income) == 2, f"want 2 INCOME_ACTUAL for A, got {len(a_income)}"
    assert sum(f["amount"] for f in a_income) == 173456.0
    assert all(f["currency"] == "GBP" for f in a_income)

    # No-value fund must be skipped (amount=None filtered out before append)
    assert "PROJ-UUID-NOVAL" not in pids

    # No-project fund must be skipped
    assert all(f["project_id"] for f in funds)
    print(f"  PASS: parse_funds_xml extracted {len(funds)} valid funds (skipped malformed)")


# -----------------------------------------------------------------------------
# Test 2: /projects parser preserves existing fields
# -----------------------------------------------------------------------------
PROJECT_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<ns1:projects xmlns:ns1="http://gtr.rcuk.ac.uk/gtr/api"
              xmlns:ns2="http://gtr.rcuk.ac.uk/gtr/api/project"
              ns1:totalPages="1714" ns1:size="1" ns1:page="1">
  <ns2:project ns1:id="PROJ-UUID-A" ns1:created="2020-01-01" ns1:updated="2024-01-01">
    <ns2:title>Test Grant on Quantum Widgets</ns2:title>
    <ns2:abstractText>This project investigates widgets in the quantum regime.</ns2:abstractText>
    <ns2:techAbstractText>Tech-level abstract.</ns2:techAbstractText>
    <ns2:status>Active</ns2:status>
    <ns2:grantCategory>Research Grant</ns2:grantCategory>
    <ns2:leadFunder>EPSRC</ns2:leadFunder>
    <ns2:identifiers>
      <ns2:identifier ns2:type="RCUK">EP/X012345/1</ns2:identifier>
      <ns2:identifier ns2:type="OTHER">whatever</ns2:identifier>
    </ns2:identifiers>
    <ns2:participantValues>
      <ns2:participant>
        <ns2:role>LEAD_PARTICIPANT</ns2:role>
        <ns2:organisationName>University of Test</ns2:organisationName>
      </ns2:participant>
      <ns2:participant>
        <ns2:role>COLLABORATION</ns2:role>
        <ns2:organisationName>Other Org</ns2:organisationName>
      </ns2:participant>
    </ns2:participantValues>
    <ns1:links>
      <ns1:link ns1:rel="FUND" ns1:start="2020-09-01" ns1:end="2024-08-31"
                ns1:href="https://gtr.ukri.org/gtr/api/funds/fund-uuid-A"/>
      <ns1:link ns1:rel="LEAD_ORG" ns1:href="https://gtr.ukri.org/gtr/api/organisations/ORG-UUID"/>
      <ns1:link ns1:rel="PI_PER" ns1:href="https://gtr.ukri.org/gtr/api/persons/PERSON-UUID"/>
    </ns1:links>
  </ns2:project>
</ns1:projects>
"""


def test_project_parser():
    root = ET.fromstring(PROJECT_XML)
    proj_elem = root.find(".//ns2:project", NS)
    assert proj_elem is not None, "test fixture has no project element"

    p = parse_single_project(proj_elem)
    assert p is not None, "parser returned None"

    expected = {
        "project_id": "PROJ-UUID-A",
        "grant_reference": "EP/X012345/1",
        "title": "Test Grant on Quantum Widgets",
        "abstract": "This project investigates widgets in the quantum regime.",
        "status": "Active",
        "grant_category": "Research Grant",
        "lead_funder": "EPSRC",
        "amount": None,  # <-- intentionally None; comes from /funds pass
        "start_date": "2020-09-01",
        "end_date": "2024-08-31",
        "lead_org_id": "ORG-UUID",
        "lead_org_name": "University of Test",
        "pi_id": "PERSON-UUID",
        "pi_given_name": None,
        "pi_family_name": None,
    }
    for k, want in expected.items():
        got = p.get(k)
        assert got == want, f"field {k!r} mismatch: want={want!r} got={got!r}"
    # No extra keys leaked or dropped
    assert set(p.keys()) == set(expected.keys()), \
        f"key set drift: extra={set(p.keys()) - set(expected.keys())} missing={set(expected.keys()) - set(p.keys())}"
    print(f"  PASS: parse_single_project preserved all {len(expected)} fields")


# -----------------------------------------------------------------------------
# Test 3: end-to-end merge logic in process_projects
# -----------------------------------------------------------------------------
def test_merge_logic():
    """Simulate process_projects merge step."""
    import pandas as pd

    projects = [
        {"project_id": "PROJ-UUID-A", "title": "A", "amount": None, "lead_funder": "EPSRC", "grant_reference": "EP/X1/1"},
        {"project_id": "PROJ-UUID-B", "title": "B", "amount": None, "lead_funder": "MRC", "grant_reference": "MR/X1/1"},
        {"project_id": "PROJ-UUID-Z", "title": "Z", "amount": None, "lead_funder": "STFC", "grant_reference": "ST/X1/1"},
    ]
    project_funds = {
        "PROJ-UUID-A": {"amount": 123456.78, "currency": "GBP"},
        "PROJ-UUID-B": {"amount": 50000.0, "currency": "GBP"},
        # PROJ-UUID-Z deliberately missing
    }

    df = pd.DataFrame(projects)
    funds_df = (
        pd.DataFrame.from_dict(project_funds, orient="index")
        .reset_index()
        .rename(columns={"index": "project_id", "amount": "fund_amount"})
    )
    df = df.merge(funds_df[["project_id", "fund_amount"]], on="project_id", how="left")
    df["amount"] = df["fund_amount"].combine_first(df.get("amount"))
    df = df.drop(columns=["fund_amount"])

    by_pid = df.set_index("project_id")["amount"].to_dict()
    assert by_pid["PROJ-UUID-A"] == 123456.78
    assert by_pid["PROJ-UUID-B"] == 50000.0
    assert pd.isna(by_pid["PROJ-UUID-Z"]), "missing fund must remain NaN, not crash"

    # Original cols preserved
    assert set(df.columns) == {"project_id", "title", "amount", "lead_funder", "grant_reference"}, \
        f"unexpected columns: {df.columns.tolist()}"
    print(f"  PASS: merge logic populates amount where /funds had it ({df['amount'].notna().sum()}/{len(df)})")


if __name__ == "__main__":
    print("=" * 60)
    print("gtr_to_s3.py parser tests")
    print("=" * 60)
    print("\n[1/3] /funds parser")
    test_funds_parser()
    print("\n[2/3] /projects parser (regression)")
    test_project_parser()
    print("\n[3/3] process_projects merge logic")
    test_merge_logic()
    print("\nAll tests passed.")
