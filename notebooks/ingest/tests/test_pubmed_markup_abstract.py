"""PubMed abstracts with inline markup (oxjob #1052).

`pubmed_markup_abstract_sql` in notebooks/ingest/PubMed.py builds a record's abstract from <Abstract>'s raw inner XML
(pubmed_abstract_markup), so the text before an <i>, <sub>, <sup>, <b>, <u> or MathML child is no longer lost. It is
pure SQL, so the same text backfills pubmed_works; these cases run it on a Databricks SQL warehouse (ANSI on, like the
pipeline), and check PUBMED_ABSTRACT_CHILD_RE, which picks the records that take this path. They need the `databricks`
CLI logged in; elsewhere they skip. CI does not run pytest.

    python3 notebooks/ingest/tests/test_pubmed_markup_abstract.py     # or: pytest -q this file
"""
import ast
import json
import os
import re
import shutil
import subprocess
import time

PUBMED_PY = os.path.join(os.path.dirname(__file__), "..", "PubMed.py")
WAREHOUSE = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "ab72acf2b2d1ef92")


def load():
    """Exec only the builder and the child regex out of PubMed.py (the notebook imports dlt)."""
    src = open(PUBMED_PY, encoding="utf-8").read()
    keep = [n for n in ast.parse(src).body
            if (isinstance(n, ast.Assign) and any(getattr(t, "id", "") == "PUBMED_ABSTRACT_CHILD_RE" for t in n.targets))
            or (isinstance(n, ast.FunctionDef) and n.name == "pubmed_markup_abstract_sql")]
    assert len(keep) == 2, "PubMed.py must define PUBMED_ABSTRACT_CHILD_RE and pubmed_markup_abstract_sql"
    ns = {}
    exec(compile(ast.Module(body=keep, type_ignores=[]), PUBMED_PY, "exec"), ns)
    return ns["pubmed_markup_abstract_sql"], ns["PUBMED_ABSTRACT_CHILD_RE"]


# (<Abstract> inner XML as the XML reader returns it, expected abstract before clean_abstract_text)
CASES = [
    ('<AbstractText Label="BACKGROUND" NlmCategory="BACKGROUND">Before <i>it</i> middle Mn<sub>3</sub>O<sub>4</sub> end.'
     '</AbstractText><AbstractText Label="METHODS">Plain text.</AbstractText>\n   <CopyrightInformation>(c) X'
     '</CopyrightInformation>',
     'BACKGROUND: Before <i>it</i> middle Mn<sub>3</sub>O<sub>4</sub> end. METHODS: Plain text.'),
    ('<AbstractText Label="" NlmCategory="UNASSIGNED">x <i>y</i></AbstractText>', 'x <i>y</i>'),
    ('<AbstractText NlmCategory="RESULTS">r<sup>2</sup></AbstractText>', 'RESULTS: r<sup>2</sup>'),
    ('<AbstractText Label="A"></AbstractText><AbstractText>t<b>b</b></AbstractText>', 't<b>b</b>'),  # empty body skipped
    ('<CopyrightInformation>(c) X</CopyrightInformation>', None),
    ('<AbstractText>p<0.001 and <i>x</i></AbstractText>', 'p<0.001 and <i>x</i>'),  # decoded '<' in the text stays
    ('<AbstractText Label="AIMS">line one\n<i>two</i>\nthree</AbstractText>', 'AIMS: line one\n<i>two</i>\nthree'),
    ('<AbstractText>  <i>Whole abstract in italics.</i>  </AbstractText>', '<i>Whole abstract in italics.</i>'),
]

# (<Abstract> inner XML, takes the markup path?)
CHILD_CASES = [
    ('<AbstractText>Mn<sub>3</sub>O<sub>4</sub></AbstractText>', True),
    ('<AbstractText>x <mml:math display="inline"><mml:mi>x</mml:mi></mml:math></AbstractText>', True),
    ('<AbstractText Label="A">plain</AbstractText><CopyrightInformation>(c)</CopyrightInformation>', False),
    ('<AbstractText>p<0.001, n < 5</AbstractText>', False),
]


def _sql_lit(v):
    return "NULL" if v is None else "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"


def run_sql(q):
    body = {"warehouse_id": WAREHOUSE, "statement": q, "wait_timeout": "30s", "format": "JSON_ARRAY"}
    out = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--json", json.dumps(body)],
                         capture_output=True, text=True, check=True)
    st = json.loads(out.stdout)
    while st["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(2)
        st = json.loads(subprocess.run(["databricks", "api", "get", f"/api/2.0/sql/statements/{st['statement_id']}"],
                                       capture_output=True, text=True, check=True).stdout)
    assert st["status"]["state"] == "SUCCEEDED", st["status"]
    return {int(i): t for i, t in st["result"]["data_array"]}


def test_child_regex():
    _, child_re = load()
    bad = [(m, want) for m, want in CHILD_CASES if bool(re.search(child_re, m)) != want]
    assert not bad, bad


def test_markup_abstract():
    if not shutil.which("databricks"):
        import pytest
        pytest.skip("databricks CLI not available")
    builder, _ = load()
    rows = [f"SELECT {i} AS i, CAST({_sql_lit(m)} AS STRING) AS m" for i, (m, _) in enumerate(CASES)]
    got = run_sql(f"SELECT i, {builder('m')} AS t FROM ({' UNION ALL '.join(rows)}) ORDER BY i")
    bad = [(m, exp, got.get(i)) for i, (m, exp) in enumerate(CASES) if got.get(i) != exp]
    assert not bad, "\n".join(f"{m!r}: expected {exp!r}, got {g!r}" for m, exp, g in bad)


if __name__ == "__main__":
    test_child_regex()
    test_markup_abstract()
    print(f"ok: {len(CASES)} + {len(CHILD_CASES)} cases")
