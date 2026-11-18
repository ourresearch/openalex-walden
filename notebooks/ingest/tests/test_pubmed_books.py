"""PubMed book records parse (oxjob #1104).

PubMed.py parses PubmedBookArticle records with pure SQL over an explicit schema (PUBMED_BOOK_SCHEMA,
PUBMED_BOOK_COLUMNS). `book_parse_sql(path)` builds the same SELECT over read_files(), so a dry run on a SQL warehouse
reads exactly the text the pipeline runs. The cases below run it on the fixture files in the oxjobs job's volume copy
(18 StatPearls chapters fetched by E-utilities, oxjob #1104) and need the `databricks` CLI logged in; elsewhere they
skip. CI does not run pytest.

    python3 notebooks/ingest/tests/test_pubmed_books.py            # run the checks
    python3 notebooks/ingest/tests/test_pubmed_books.py --sql PATH # print the dry-run SQL for files under PATH
"""
import ast
import json
import os
import shutil
import subprocess
import sys
import time

PUBMED_PY = os.path.join(os.path.dirname(__file__), "..", "PubMed.py")
WAREHOUSE = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "ab72acf2b2d1ef92")
FIXTURES = "/Volumes/openalex/oxjobs/data/missing-works-ingest-rollup/scratch/pubmed_books_trial/"
KEEP = {"MAX_TITLE_LENGTH", "MAX_ABSTRACT_LENGTH", "MAX_AUTHOR_NAME_LENGTH", "MAX_AFFILIATION_STRING_LENGTH",
        "PUBMED_BOOK_SCHEMA", "_BD", "_BOOK_DOI", "_BOOK_PMID", "_BOOK_NBK", "_BOOK_HISTORY_PUBMED",
        "PUBMED_BOOK_COLUMNS", "_book_date_sql", "_strip_tags_sql"}


def load():
    """Exec only the book parse constants and helpers out of PubMed.py (the notebook imports dlt)."""
    src = open(PUBMED_PY, encoding="utf-8").read()
    keep = [n for n in ast.parse(src).body
            if (isinstance(n, ast.Assign) and any(getattr(t, "id", "") in KEEP for t in n.targets))
            or (isinstance(n, ast.FunctionDef) and n.name in KEEP)]
    ns = {}
    exec(compile(ast.Module(body=keep, type_ignores=[]), PUBMED_PY, "exec"), ns)
    missing = KEEP - set(ns)
    assert not missing, f"PubMed.py no longer defines {sorted(missing)} at top level"
    return ns


def book_parse_sql(path):
    ns = load()
    schema = ns["PUBMED_BOOK_SCHEMA"].replace("'", "\\'")
    cols = ",\n  ".join(f"{v} AS {k}" for k, v in ns["PUBMED_BOOK_COLUMNS"].items())
    return (f"SELECT\n  {cols}\nFROM read_files('{path}', format => 'xml', rowTag => 'PubmedBookArticle', "
            f"schema => '{schema}')")


def run_sql(sql):
    cli = shutil.which("databricks")
    if not cli:
        return None
    body = json.dumps({"statement": sql, "warehouse_id": WAREHOUSE, "wait_timeout": "50s"})
    r = json.loads(subprocess.run([cli, "api", "post", "/api/2.0/sql/statements", "--json", body],
                                  capture_output=True, text=True, check=True).stdout)
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = json.loads(subprocess.run([cli, "api", "get", f"/api/2.0/sql/statements/{r['statement_id']}"],
                                      capture_output=True, text=True, check=True).stdout)
    assert r["status"]["state"] == "SUCCEEDED", r["status"]
    names = [c["name"] for c in r["manifest"]["schema"]["columns"]]
    return [dict(zip(names, row)) for row in r.get("result", {}).get("data_array", [])]


def test_fixture_chapters():
    sql = ("WITH b AS (" + book_parse_sql(FIXTURES + "*.xml.gz") + ")\n"
           "SELECT native_id, title, raw_type, size(authors) n_authors, "
           "published_date, updated_date, length(abstract) abstract_len, abstract RLIKE '<' abstract_has_tag, "
           "source_name, publisher, size(references) n_refs, size(urls) n_urls, "
           "exists(ids, x -> x.namespace = 'pmid' AND x.id = native_id) has_pmid FROM b ORDER BY native_id")
    rows = run_sql(sql)
    if rows is None:
        print("skip: no databricks CLI")
        return
    assert len(rows) == 18, len(rows)
    by = {r["native_id"]: r for r in rows}
    gcs = by["30020670"]
    assert gcs["title"] == "Glasgow Coma Scale", gcs
    assert gcs["raw_type"] == "book-chapter" and gcs["source_name"] == "StatPearls", gcs
    assert gcs["publisher"] == "StatPearls Publishing" and gcs["published_date"] == "2026-06-17", gcs
    for r in rows:
        assert int(r["n_authors"]) > 0, r
        assert r["has_pmid"] == "true" and r["n_urls"] == "2", r
        assert int(r["abstract_len"]) > 100 and r["abstract_has_tag"] == "false", r
        assert int(r["n_refs"]) > 0, r
    print(f"ok: {len(rows)} chapters parse")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--sql":
        print(book_parse_sql(sys.argv[2]))
    else:
        test_fixture_chapters()
