"""Page strings read the way NCBI reads them (oxjob #2240).

`page_range_sql` in notebooks/ingest/PubMed.py (and its copy in Crossref.py) builds the SQL that turns a page
string ("358-60", "27, 31-2, 34-7", "17-1-17-28") into first_page / last_page. It is pure SQL, so the same text
backfills pubmed_works and crossref_works. These cases run it on a Databricks SQL warehouse (ANSI on, like the
pipeline); expected values for MEDLINE shapes are NCBI efetch's StartPage / EndPage for real records. They need the
`databricks` CLI logged in; elsewhere they skip (the copy check always runs). CI does not run pytest.

    python3 notebooks/ingest/tests/test_page_ranges.py     # or: pytest -q this file
"""
import ast
import json
import os
import shutil
import subprocess
import time

HERE = os.path.dirname(__file__)
NAMES = ("_PG_ITEM", "_PG_RANGE", "_PG_COMPOUND", "page_range_sql")
WAREHOUSE = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "ab72acf2b2d1ef92")


def builder_nodes(notebook):
    src = open(os.path.join(HERE, "..", notebook), encoding="utf-8").read()
    keep = [n for n in ast.parse(src).body
            if (isinstance(n, ast.Assign) and any(getattr(t, "id", "") in NAMES for t in n.targets))
            or (isinstance(n, ast.FunctionDef) and n.name in NAMES)]
    assert len(keep) == len(NAMES), f"{notebook} must define {NAMES} at top level"
    return keep


def load_builder(notebook="PubMed.py"):
    """Exec only the builder and its patterns out of the notebook (the notebooks import dlt)."""
    ns = {}
    exec(compile(ast.Module(body=builder_nodes(notebook), type_ignores=[]), notebook, "exec"), ns)
    return ns["page_range_sql"]


# (page string, expected first_page, expected last_page)
CASES = [
    ("358-60", "358", "360"),                        # W2419605274 / PMID 6624168 (served last_page "60")
    ("520-8", "520", "528"),                         # W2409388172 / PMID 11092140
    ("146-52", "146", "152"),                        # W2243887979 / PMID 26417534
    ("401-6", "401", "406"),                         # W2399984779 / PMID 20999542
    ("27, 31-2, 34-7", "27", "37"),                  # W4301725007 / PMID 10118768 (NCBI 27-37)
    ("220, 222", "220", "222"),
    ("26, 28, 30-1", "26", "31"),
    ("48-50, 52", "48", "52"),
    ("557-60, 576", "557", "576"),
    ("096-7, 100-1", "096", "101"),
    ("e156-7; author reply e156-7", "e156", "e157"),
    ("C845-53", "C845", "C853"),
    ("E156-7", "E156", "E157"),
    ("S12-5", "S12", "S15"),
    ("1001-6; discussion 1006-7", "1001", "1007"),
    ("8-20; discussion 20-2", "8", "22"),
    ("811-5; discussion 815", "811", "815"),
    ("175-6; author reply 176-7", "175", "177"),
    ("318; author reply 318-9", "318", "319"),
    ("1-100; quiz 101-4", "1", "104"),
    ("Suppl 1:234-7", "234", "237"),
    ("Suppl:822-4", "822", "824"),
    # NCBI gives no EndPage (or a roman numeral) for these: the end of the last range they give
    ("43-6, 48, 50 passim", "43", "50"),
    ("283-90, ix", "283", "290"),
    ("300-1; passim", "300", "301"),
    ("2266-71 contd", "2266", "2271"),
    ("999-1024 passim", "999", "1024"),
    ("33 passim", "33", "33"),
    # unchanged: full ranges, single pages
    ("358-360", "358", "360"),
    ("358", "358", "358"),
    ("S12-S15", "S12", "S15"),
    (" 358-60 ", "358", "360"),
    # Crossref chapter-page and abbreviated ranges
    ("17-1-17-28", "17-1", "17-28"),                 # W4253803375 / 10.1201/9780367341558-8
    ("C5-171-C5-174", "C5-171", "C5-174"),           # W2053400059 / 10.1051/jphyscol:1983526
    ("V9-163-V9-165", "V9-163", "V9-165"),           # W2011603468 / 10.1109/iccasm.2010.5623061
    ("41-5848-41-5848", "41-5848", "41-5848"),       # Choice review number, read whole
    ("253-62", "253", "262"),
    # shapes it can't read keep the old split at the first hyphen
    ("126-130.e1", "126", "130.e1"),
    ("359.e1-6", "359.e1", "6"),                      # NCBI itself gives EndPage 359.e3596
    ("358-12", "358", "12"),                          # would run backwards
    ("99-1", "99", "1"),
    ("A 1-170", "A 1", "170"),
    ("vii 374-6", "vii 374", "6"),
    ("xii-2-11", "xii", "2"),
    ("iii-xii", "iii", "xii"),
    ("e2022GH000730", "e2022GH000730", "e2022GH000730"),
    ("2 pp.", "2 pp.", "2 pp."),                      # IEEE page count (10.1109/qels.2003.238053), not page 2
    ("9 p", "9 p", "9 p"),
    ("1 Suppl", "1 Suppl", "1 Suppl"),
    # Crossref lists, read like NCBI's
    ("412, 414", "412", "414"),
    ("204-209,319", "204", "319"),
    ("1-10 CONTD", "1", "10"),
    ("", "", ""),
    (None, None, None),
]


def _sql_lit(v):
    return "NULL" if v is None else "'" + v.replace("'", "''") + "'"


def build_query(builder):
    rows = " UNION ALL ".join(f"SELECT {i} AS i, CAST({_sql_lit(p)} AS STRING) AS pg" for i, (p, _, _) in enumerate(CASES))
    expr = builder("pg")
    return f"SELECT i, r.first_page, r.last_page FROM (SELECT i, {expr} AS r FROM ({rows})) ORDER BY i"


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
    return {int(i): (f, l) for i, f, l in st["result"]["data_array"]}


def test_copies_match():
    dump = lambda nb: [ast.dump(n) for n in builder_nodes(nb)]  # noqa: E731
    assert dump("PubMed.py") == dump("Crossref.py"), "page_range_sql differs between PubMed.py and Crossref.py"


def test_page_ranges():
    if not shutil.which("databricks"):
        import pytest
        pytest.skip("databricks CLI not available")
    got = run_sql(build_query(load_builder()))
    bad = [(p, (f, l), got.get(i)) for i, (p, f, l) in enumerate(CASES) if got.get(i) != (f, l)]
    assert not bad, "\n".join(f"{p!r}: expected {exp}, got {g}" for p, exp, g in bad)


if __name__ == "__main__":
    test_copies_match()
    test_page_ranges()
    print(f"ok: {len(CASES)} cases")
