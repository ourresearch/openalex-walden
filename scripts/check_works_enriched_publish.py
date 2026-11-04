#!/usr/bin/env python3
"""Fail when CreateWorksEnriched writes openalex.works.openalex_works anywhere but its publish cell (oxjob #2004).

CreateWorksEnriched builds tonight's works in openalex_works_build and its last cell publishes the finished build as
openalex_works in one commit (CREATE OR REPLACE ... DEEP CLONE), so readers outside the End 2 End never see a
half-built table. A new cell that writes openalex.works.openalex_works BEFORE the publish has its change thrown away
by the publish, silently; one AFTER it changes the published table in place. New enrichments go into
openalex.works.openalex_works_build, before the publish cell.

usage: check_works_enriched_publish.py [NOTEBOOK]   (default: notebooks/end2end/CreateWorksEnriched.ipynb)
       check_works_enriched_publish.py --self-test
"""
import json
import re
import sys

LIVE = r"openalex\.works\.openalex_works(?![A-Za-z0-9_])"
WRITE = re.compile(r"\b(?:MERGE\s+INTO|INSERT\s+(?:INTO|OVERWRITE)(?:\s+TABLE)?|UPDATE|DELETE\s+FROM|TRUNCATE\s+TABLE|"
                   r"ALTER\s+TABLE|OPTIMIZE|RESTORE(?:\s+TABLE)?|CREATE\s+(?:OR\s+REPLACE\s+)?TABLE(?:\s+IF\s+NOT\s+EXISTS)?)"
                   r"\s+" + LIVE, re.I)
PUBLISH = re.compile(r"CREATE\s+OR\s+REPLACE\s+TABLE\s+" + LIVE + r"\s+DEEP\s+CLONE\s+openalex\.works\.openalex_works_build\b", re.I)


def strip_comments(sql):
    return re.sub(r"--[^\n]*", "", sql)


def problems(cells):
    """cells: list of SQL code-cell texts in order. Returns a list of messages (empty = OK)."""
    code = [strip_comments(c) for c in cells]
    pubs = [i for i, s in enumerate(code) if PUBLISH.search(s)]
    if len(pubs) != 1 or pubs[0] != len(code) - 1:
        return [f"expected exactly one publish cell (CREATE OR REPLACE TABLE openalex.works.openalex_works DEEP CLONE "
                f"openalex.works.openalex_works_build) as the last code cell; found it in code cells {pubs} of {len(code)}"]
    out = []
    for i, s in enumerate(code[:-1]):
        for m in WRITE.finditer(s):
            out.append(f"code cell {i} writes openalex.works.openalex_works before the publish ({m.group(0)!r}): "
                       f"that write is lost when the publish replaces the table; write openalex.works.openalex_works_build")
    return out


def code_cells(path):
    nb = json.load(open(path, encoding="utf-8"))
    return ["".join(c["source"]) if isinstance(c["source"], list) else c["source"]
            for c in nb["cells"] if c["cell_type"] == "code"]


def self_test():
    clone = "CREATE OR REPLACE TABLE openalex.works.openalex_works_build DEEP CLONE openalex.works.openalex_works_base"
    pub = "CREATE OR REPLACE TABLE openalex.works.openalex_works\nDEEP CLONE openalex.works.openalex_works_build"
    good = [clone, "MERGE INTO openalex.works.openalex_works_build AS t USING x ON t.id = x.id",
            "-- MERGE INTO openalex.works.openalex_works (comment only)\nSELECT 1", pub]
    assert problems(good) == [], problems(good)
    bad = [clone, "MERGE INTO openalex.works.openalex_works AS target USING x ON target.id = x.id", pub]
    assert len(problems(bad)) == 1, problems(bad)
    assert problems([clone, "UPDATE openalex.works.openalex_works SET x = 1", pub])
    assert problems([clone, pub, "SELECT 1"]), "publish not last"
    assert problems([clone]), "no publish"
    assert problems([clone, "SELECT * FROM openalex.works.openalex_works_hash", pub]) == []
    print("self-test OK")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        sys.exit(0)
    path = sys.argv[1] if len(sys.argv) > 1 else "notebooks/end2end/CreateWorksEnriched.ipynb"
    found = problems(code_cells(path))
    for p in found:
        print(f"{path}: {p}")
    print(f"{path}: {'FAIL' if found else 'OK'} (oxjob #2004: only the last cell writes openalex.works.openalex_works)")
    sys.exit(1 if found else 0)
