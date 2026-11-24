"""PubMed display titles: NLM's [bracketed] English translations and its placeholders (oxjob #2590).

`nlm_title_sql` in notebooks/ingest/PubMed.py builds the SQL that turns a MEDLINE record's ArticleTitle and
VernacularTitle into the title works show. It is pure SQL, so the same text backfills pubmed_works; these cases run it
on a Databricks SQL warehouse (ANSI on, like the pipeline). They need the `databricks` CLI logged in; elsewhere they
skip. CI does not run pytest.

    python3 notebooks/ingest/tests/test_pubmed_nlm_title.py     # or: pytest -q this file
"""
import ast
import json
import os
import shutil
import subprocess
import time

PUBMED_PY = os.path.join(os.path.dirname(__file__), "..", "PubMed.py")
WAREHOUSE = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "ab72acf2b2d1ef92")
NAMES = ("_NLM_TRANSLATED_TITLE", "_NLM_TITLE_PLACEHOLDERS")


def load_builder():
    """Exec only the builder and its constants out of PubMed.py (the notebook imports dlt)."""
    src = open(PUBMED_PY, encoding="utf-8").read()
    keep = [n for n in ast.parse(src).body
            if (isinstance(n, ast.Assign) and any(getattr(t, "id", "") in NAMES for t in n.targets))
            or (isinstance(n, ast.FunctionDef) and n.name == "nlm_title_sql")]
    assert len(keep) == len(NAMES) + 1, f"PubMed.py must define {', '.join(NAMES)} and nlm_title_sql"
    ns = {}
    exec(compile(ast.Module(body=keep, type_ignores=[]), PUBMED_PY, "exec"), ns)
    return ns["nlm_title_sql"]


# (ArticleTitle, VernacularTitle, expected display title)
CASES = [
    ("[Pneumothorax in AIDS].", "Pneumotorace in AIDS", "Pneumothorax in AIDS"),        # W2410441348 / PMID 1589130
    ("[New antiepileptic drugs].", "Nuevos fármacos antiepilépticos", "New antiepileptic drugs"),  # PMID 9147761
    ("[GONADOTROPINS].", None, "GONADOTROPINS"),                    # no VernacularTitle: only the brackets go
    ("[In process citation]", None, "[In process citation]"),      # a placeholder without a title stays as stored
    ("[Title without the closing period]", None, "Title without the closing period"),
    ("[Scientific activity of Prof. A.I. Kuznetsov; 60th anniversary of his birth].", None,
     "Scientific activity of Prof. A.I. Kuznetsov; 60th anniversary of his birth"),
    ("[What is your diagnosis?].", None, "What is your diagnosis?"),
    ("[Binding of [3H]imipramine in human platelets].", None, "Binding of [3H]imipramine in human platelets"),
    ("[3H]Spiperone binding in rat striatum [proceedings].", None,
     "[3H]Spiperone binding in rat striatum [proceedings]."),       # outer brackets are not a pair: unchanged
    ("[A [[nested]] title].", None, "[A [[nested]] title]."),       # nesting deeper than one: unchanged
    ("[Not Available].", "Hautkrebs bei Landwirten.", "Hautkrebs bei Landwirten."),  # the placeholder gives way
    ("[Not available].", "  Hautkrebs bei Landwirten  ", "Hautkrebs bei Landwirten"),
    ("[In Process Citation].", "Un titre.", "Un titre."),
    ("[Not Available].", None, "[Not Available]."),                 # nothing better: the placeholder stays
    ("[Not Available].", "   ", "[Not Available]."),
    ("[Not Available.]", "Ein Titel.", "Ein Titel."),               # the period inside the brackets
    ("[In process.]", None, "[In process.]"),
    ("  [Pneumothorax in AIDS].  ", None, "Pneumothorax in AIDS"),   # whitespace around the raw title
    ("[Not available, cope with it: an unacceptable scandal].", None,
     "Not available, cope with it: an unacceptable scandal"),       # a real title that starts like a placeholder
    ("[].", None, "[]."),                                           # nothing inside: unchanged
    ("Scoliosis.", "Scoliose.", "Scoliosis."),                      # English title, no brackets: unchanged
    ("Treatment of X [in Russian].", "Lechenie X.", "Treatment of X [in Russian]."),
    (None, "Titulo.", None),
]


def _sql_lit(v):
    return "NULL" if v is None else "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"


def build_query(builder):
    rows = [f"SELECT {i} AS i, CAST({_sql_lit(a)} AS STRING) AS a, CAST({_sql_lit(v)} AS STRING) AS v"
            for i, (a, v, _) in enumerate(CASES)]
    return f"SELECT i, {builder('a', 'v')} AS t FROM ({' UNION ALL '.join(rows)}) ORDER BY i"


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


def test_nlm_title():
    if not shutil.which("databricks"):
        import pytest
        pytest.skip("databricks CLI not available")
    got = run_sql(build_query(load_builder()))
    bad = [(a, v, exp, got.get(i)) for i, (a, v, exp) in enumerate(CASES) if got.get(i) != exp]
    assert not bad, "\n".join(f"{a!r} / {v!r}: expected {exp!r}, got {g!r}" for a, v, exp, g in bad)


if __name__ == "__main__":
    test_nlm_title()
    print(f"ok: {len(CASES)} cases")
