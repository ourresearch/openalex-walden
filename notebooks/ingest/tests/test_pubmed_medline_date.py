"""PubMed issue dates when PubDate has no Year (oxjob #1863).

`medline_date_fallback_sql` in notebooks/ingest/PubMed.py builds the SQL that dates a record from
PubDate.MedlineDate ("1950 Oct-Dec"), else its PubStatus="pubmed" history date. It is pure SQL, so the
same text backfills pubmed_works; these cases run it on a Databricks SQL warehouse (ANSI on, like the
pipeline). They need the `databricks` CLI logged in; elsewhere they skip. CI does not run pytest.

    python3 notebooks/ingest/tests/test_pubmed_medline_date.py     # or: pytest -q this file
"""
import ast
import json
import os
import shutil
import subprocess
import time

PUBMED_PY = os.path.join(os.path.dirname(__file__), "..", "PubMed.py")
WAREHOUSE = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "ab72acf2b2d1ef92")


def load_builder():
    """Exec only the builder and its month table out of PubMed.py (the notebook imports dlt)."""
    src = open(PUBMED_PY, encoding="utf-8").read()
    keep = [n for n in ast.parse(src).body
            if (isinstance(n, ast.Assign) and any(getattr(t, "id", "") == "_MEDLINE_MONTHS" for t in n.targets))
            or (isinstance(n, ast.FunctionDef) and n.name == "medline_date_fallback_sql")]
    assert len(keep) == 2, "PubMed.py must define _MEDLINE_MONTHS and medline_date_fallback_sql at top level"
    ns = {}
    exec(compile(ast.Module(body=keep, type_ignores=[]), PUBMED_PY, "exec"), ns)
    return ns["medline_date_fallback_sql"]


# (MedlineDate, 'pubmed' history date as (y, m, d) or None, expected date or None)
CASES = [
    ("1950 Oct-Dec", None, "1950-10-01"),          # W2415494337 / PMID 14816886 (OpenAlex had 2004-02-15)
    ("1996 Mar-Apr", None, "1996-03-01"),          # W2397129584 / PMID 9174395
    ("2012 Nov-Dec", None, "2012-11-01"),          # W2993912274 / PMID 23367755
    ("1951", None, "1951-01-01"),                  # W4291260256 / PMID 14865314
    ("2011-2012 Winter", None, "2011-01-01"),
    ("1945 Dec 22-29", None, "1945-12-22"),
    ("1946 Aug 29-Sep 5", None, "1946-08-29"),
    ("1945 Nov-1946 Jan", None, "1945-11-01"),
    ("1978 Dec 26-1979 Jan 2", None, "1978-12-26"),
    ("1970 Jan. - Feb.", None, "1970-01-01"),
    ("1989 Jan/Feb", None, "1989-01-01"),
    ("1959 Jul-Aug 20", None, "1959-07-01"),
    ("1962 Apr Jun", None, "1962-04-01"),
    ("2020 September December", None, "2020-09-01"),
    ("1998 Oct. 31", None, "1998-10-31"),
    ("2026 Sept 20", None, "2026-09-20"),
    ("2016 JUNE 5", None, "2016-06-05"),
    ("2000 Aug, 7", None, "2000-08-07"),
    ("1980 Feb 30", None, "1980-02-01"),           # no such day: keep the month
    ("1971 Oct 70-Jan", None, "1971-10-01"),
    ("1954 Nov 24-1954 Dec1", None, "1954-11-24"),
    ("2005 Nov10-23", None, "2005-11-01"),
    ("1946 Spring-Summer", None, "1946-01-01"),    # seasons and quarters keep the year only
    ("Autumn 2014", None, "2014-01-01"),
    ("1966 winter", None, "1966-01-01"),
    ("2016 Second Quarter", None, "2016-01-01"),
    ("1965 2d Quart", None, "1965-01-01"),
    ("2023 Oct Autumn (Fall)", None, "2023-10-01"),
    ("2019 Sep - Dec - Fourth Quarter", None, "2019-09-01"),
    ("2015 Abr", None, "2015-04-01"),
    ("2015 Ago", None, "2015-08-01"),
    ("2015 Dic", None, "2015-12-01"),
    ("2016 Oktober", None, "2016-10-01"),
    ("2021 Avril", None, "2021-04-01"),
    ("2021 Décembre", None, "2021-12-01"),
    ("2022 juillet", None, "2022-07-01"),
    ("2021 Aout", None, "2021-08-01"),
    ("2016 Mayo-Agosto", None, "2016-05-01"),
    ("2017 SJan", None, "2017-01-01"),             # no word boundary before Jan: year only
    ("1782 Apr-Jun", None, "1782-04-01"),
    ("2021 Special Issue on COVID-19", None, "2021-01-01"),
    ("1977, reçu 1980", None, "1977-01-01"),
    ("1948-49", None, "1948-01-01"),
    ("1950 Oct-Dec", (2004, 2, 15), "1950-10-01"),  # MedlineDate beats the history date
    ("Spring", (2004, 2, 15), "2004-02-15"),        # no year in MedlineDate: the 'pubmed' history date
    (None, (2001, 6, 30), "2001-06-30"),
    (None, None, None),                              # nothing: NULL, never DateCompleted
]


def _sql_lit(v):
    return "NULL" if v is None else "'" + v.replace("'", "''") + "'"


def build_query(builder):
    rows = []
    for i, (md, hist, _) in enumerate(CASES):
        h = ("CAST(NULL AS ARRAY<STRUCT<Day:BIGINT,Hour:BIGINT,Minute:BIGINT,Month:BIGINT,Year:BIGINT,_PubStatus:STRING>>)"
             if hist is None else
             "array(named_struct('Day', CAST({2} AS BIGINT), 'Hour', CAST(0 AS BIGINT), 'Minute', CAST(0 AS BIGINT), "
             "'Month', CAST({1} AS BIGINT), 'Year', CAST({0} AS BIGINT), '_PubStatus', 'pubmed'), "
             "named_struct('Day', CAST(1 AS BIGINT), 'Hour', CAST(0 AS BIGINT), 'Minute', CAST(0 AS BIGINT), "
             "'Month', CAST(1 AS BIGINT), 'Year', CAST(1999 AS BIGINT), '_PubStatus', 'entrez'))".format(*hist))
        rows.append(f"SELECT {i} AS i, CAST({_sql_lit(md)} AS STRING) AS md, {h} AS h")
    expr = builder("md", "h")
    return f"SELECT i, CAST({expr} AS STRING) AS d FROM ({' UNION ALL '.join(rows)}) ORDER BY i"


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
    return {int(i): d for i, d in st["result"]["data_array"]}


def test_medline_date_fallback():
    if not shutil.which("databricks"):
        import pytest
        pytest.skip("databricks CLI not available")
    got = run_sql(build_query(load_builder()))
    bad = [(md, hist, exp, got.get(i)) for i, (md, hist, exp) in enumerate(CASES) if got.get(i) != exp]
    assert not bad, "\n".join(f"{md!r} hist={hist}: expected {exp}, got {g}" for md, hist, exp, g in bad)


if __name__ == "__main__":
    test_medline_date_fallback()
    print(f"ok: {len(CASES)} cases")
