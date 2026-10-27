#!/usr/bin/env python3
"""Guard test for placeholder author names (oxjob #1881).

1. Builds openalex.authors.placeholder_author_name from UpdateWorkAuthors' own cell under a scratch name and checks it on
   fixtures: placeholders ('Anonymous', 'et al.', '(:unav)', 'GBIF.org User', '12292787', Hangul filler) and the real names
   a careless rule would catch ('S. N.', 'Una S.', 'Unal', 'A. D. Min', 'E. Tall', 'Moshe Talpaz', 'The Editors').
2. EXPLAINs every statement the change touched (UpdateWorkAuthors' MERGE, MatchAuthors' batch / mint seats / decisions,
   Vector_Cards), read straight from the files with the scratch function name, so a typo or a wrong column fails here,
   not in the End 2 End.

    python3 qa/oxjob1881_placeholder_names_test.py
Needs the `databricks` CLI authenticated; writes only the scratch function openalex.transient.qa_oxjob1881_placeholder_name.
"""
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
E2E = os.path.join(HERE, "..", "notebooks", "end2end")
WAREHOUSE = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "69a583ace3bdc8d0")
FN = "openalex.authors.placeholder_author_name"
SCRATCH = "openalex.transient.qa_oxjob1881_placeholder_name"


def run(sql):
    body = {"warehouse_id": WAREHOUSE, "statement": sql, "wait_timeout": "50s", "format": "JSON_ARRAY"}
    r = json.loads(subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--json", json.dumps(body)],
                                  capture_output=True, text=True).stdout)
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = json.loads(subprocess.run(["databricks", "api", "get", f"/api/2.0/sql/statements/{r['statement_id']}"],
                                      capture_output=True, text=True).stdout)
    if r["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(json.dumps(r["status"])[:1500])
    return (r.get("result") or {}).get("data_array") or []


def cell_with(nb_name, marker):
    nb = json.load(open(os.path.join(E2E, nb_name)))
    for c in nb["cells"]:
        s = c["source"] if isinstance(c["source"], str) else "".join(c["source"])
        if marker in s:
            return s
    raise KeyError(marker)


def scratch(sql):
    return sql.replace(FN, SCRATCH).replace("max_updated_date", "to_timestamp('2026-10-26')")


PLACEHOLDERS = {
    "Anonymous": "anonymous", "anonymous, anonymous": "anonymous", "Anónimo": "anonymous", "佚名": "anonymous",
    "Unknown": "unknown", "Author, Unknown": "unknown", "作成者不明": "unknown",
    "et al.": "et_al", "ET AL. ET AL.": "et_al", "et all": "et_al", "ほか": "et_al", "等": "et_al", "и др.": "et_al",
    ":unav": "datacite_code", "(:unkn) unknown": "datacite_code", "(:tba)": "datacite_code",
    "No author provided": "no_author", "N/A": "no_author", "[s.n.]": "no_author", "N.N.": "no_author", ":null": "no_author",
    "GBIF.org User": "gbif_user", "Admin": "test_admin", "test test": "test_admin",
    "12292787": "no_letters", "?????????": "no_letters", "-": "no_letters", "ㅤ": "no_letters", "  ": "no_letters",
}
NAMES = ["S. N.", "N. A.", "NA", "Una S.", "Unal", "A. D. Min", "E. Tall", "T Ba", "Moshe Talpaz", "Anon Sukprasert",
         "Wang Wei", "李明", "The Editors", "Editorial Board", "IUCN", "Testa, Marianna", "Paganoni, Marco", "jsmith@mit.edu"]


def main():
    fails = 0
    body = cell_with("UpdateWorkAuthors.ipynb", f"CREATE OR REPLACE FUNCTION {FN}(")
    run(body.replace(FN, SCRATCH))
    cases = list(PLACEHOLDERS.items()) + [(n, None) for n in NAMES]
    values = ", ".join("(" + json.dumps(n).replace("'", "''").replace('"', "'") + ")" for n, _ in cases)
    got = {row[0]: row[1] for row in run(f"SELECT x, {SCRATCH}(x) FROM VALUES {values} AS v(x)")}
    got_null = run(f"SELECT {SCRATCH}(CAST(NULL AS STRING))")[0][0]
    for name, want in cases:
        ok = got.get(name) == want
        fails += not ok
        print(("ok  " if ok else "FAIL"), repr(name), "->", got.get(name), "" if ok else f"(want {want})")
    print(("ok  " if got_null == "no_letters" else "FAIL"), "NULL ->", got_null)
    fails += got_null != "no_letters"

    stmts = {
        "UpdateWorkAuthors MERGE": cell_with("UpdateWorkAuthors.ipynb", "MERGE INTO openalex.works.work_authors AS target"),
        "MatchAuthors batch": cell_with("MatchAuthors.ipynb", "CREATE OR REPLACE TABLE openalex.authors.author_matching_batch AS")
            .split("CREATE OR REPLACE TABLE openalex.authors.author_matching_batch AS", 1)[1],
        "MatchAuthors mint seats": cell_with("MatchAuthors.ipynb", "CREATE OR REPLACE TABLE openalex.authors.author_matching_mint_seats AS")
            .split("CREATE OR REPLACE TABLE openalex.authors.author_matching_mint_seats AS", 1)[1].split("-- C. Cluster and Mint")[0].strip().rstrip(";"),
        "MatchAuthors decisions": cell_with("MatchAuthors.ipynb", "CREATE OR REPLACE TEMPORARY VIEW batch_author_decisions AS")
            .split("CREATE OR REPLACE TEMPORARY VIEW batch_author_decisions AS", 1)[1],
        "Vector_Cards": open(os.path.join(E2E, "vector_match", "vector_cards.sql")).read()
            .split("CREATE OR REPLACE TABLE openalex.authors.vector_match_cards AS", 1)[1].split(";")[0],
    }
    for label, sql in stmts.items():
        assert SCRATCH in scratch(sql), f"{label}: the #1881 filter is missing"
        try:
            plan = run("EXPLAIN " + scratch(sql))
            text = plan[0][0] if plan else ""
            bad = "Error" in text[:300] or "AnalysisException" in text
            print(("FAIL" if bad else "ok  "), "EXPLAIN", label, text[:200].replace("\n", " ") if bad else "")
            fails += bad
        except RuntimeError as e:
            print("FAIL EXPLAIN", label, str(e)[:500])
            fails += 1
    print("PASS" if not fails else f"{fails} FAILED")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
