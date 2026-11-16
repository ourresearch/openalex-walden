"""oxjob #126: the one-time fix table's builder must apply DataCite.py's DATACITE_PY_AGREE_SQL verbatim.

BuildDataCitePublicationYearFix.sql re-dates DataCite locations parsed before the rule; if the two copies drift, records
parsed before and after the rule get different dates. Plain Python (no Spark): cut the constants block out of the
notebook, render the expression, and look for it, whitespace-normalised, in the builder.
"""
import re
from pathlib import Path

NOTEBOOKS = Path(__file__).resolve().parents[2]
DATACITE = (NOTEBOOKS / "ingest" / "DataCite.py").read_text()
BUILDER = (NOTEBOOKS / "maintenance" / "BuildDataCitePublicationYearFix.sql").read_text()


def _rendered():
    block = DATACITE[DATACITE.index("_DC_NOT_FUTURE ="):DATACITE.index("def _not_heritage_client")]
    ns = {}
    exec(block, ns)
    return ns["DATACITE_PY_AGREE_SQL"]


def _norm(s):
    return re.sub(r"\s+", " ", s).strip()


def test_builder_applies_the_notebook_rule_verbatim():
    assert _norm(_rendered()) in _norm(BUILDER)


def test_rule_runs_in_the_parse_chain():
    assert 'F.expr(DATACITE_PY_AGREE_SQL)' in DATACITE
