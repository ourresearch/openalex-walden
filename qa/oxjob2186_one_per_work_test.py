#!/usr/bin/env python3
"""Guard test for MatchAuthors Step 2b, "one profile per paper" (oxjob #2186 step C).

Reads the Step 2b cell straight from the notebook, takes its seat-judging SELECT, swaps tonight's tables for literal fixture
seats, runs it on a SQL warehouse, and checks which seats it blocks. The name parses and profile names come from the live
openalex.authors.author_names / openalex_authors (read only), as the nightly reads them; the test first checks that the names
of every blocking case are parsed, so a block can't pass by accident of a missing parse.

Cases: the dry run's examples (CHEN Baozhang onto Jing Chen's profile; Do Hoon Lim onto Il Han Kim's) and the forms of one
person that must pass (initials, accents, katakana vs kanji, Cyrillic vs Latin, identical names), the ORCID exemption, a tie
between two seats picked tonight, and a holder that is itself in tonight's batch.

    python3 qa/oxjob2186_one_per_work_test.py     # warehouse: $DATABRICKS_SQL_WAREHOUSE_ID or the serverless default
"""
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from oxjob1861_one_seat_test import lit, run  # noqa: E402

NOTEBOOK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "notebooks", "end2end", "MatchAuthors.ipynb")
JING_CHEN = 5067037840   # A5067037840 'Jing Chen' (W4214559618's shared profile)

# pending (tonight): (work, seq, name, pick, method); served (already bound): (work, seq, author_id, name)
PENDING = [
    (1, 2, "CHEN Baozhang", JING_CHEN, "name"),          # profile already holds CHEN Jing at seat 3: block
    (2, 1, "Guillén-Gosálbez, G", 9000000002, "name"),   # same person (dry-run false unassign): pass
    (3, 4, "A Ronne", 9000000003, "name"),               # same person (dry-run false unassign): pass
    (4, 1, "Wei Wang", 9000000004, "name"),              # identical printed name: never blocked
    (5, 2, "CHEN Baozhang", JING_CHEN, "orcid"),         # ORCID pick: logged as orcid_conflict, not blocked
    (6, 2, "Zhe Zhang", 9000000006, "name"),             # tonight tie: lower seat wins ...
    (6, 5, "Zhenyang Zhang", 9000000006, "vector"),      # ... the other is blocked
    (7, 1, "イサオ ハヤシ", 9000000007, "name"),            # katakana reading vs kanji: one person, pass
    (8, 3, "Кулик, Ірина Володимирівна", 9000000008, "name"),  # Cyrillic vs Latin copy: pass
    (9, 2, "J. Chen", 9000000009, "name"),               # initials form of the holder: pass
    (10, 14, "Do Hoon Lim", 9000000010, "name"),         # profile holds Il Han Kim at seat 13: block
    (11, 0, "Il Han Kim", 9000000011, "name"),           # its only other same-pick seat is tonight's seat 1 below,
    (11, 1, "Il Han Kim", 9000000011, "name"),           #   identical name: neither blocked
    (12, 3, "Junhong Li", 9000000012, "name"),           # #1893's classic: Jun Li holds the profile; full given names conflict
    (13, 2, "Chris Smith", 9000000013, "name"),          # nickname of the holder's given name (4+ letters shared): pass
    (14, 1, "Wendy Prin-Conti", 9000000014, "name"),     # the profile's other seat is an address, not a person: pass
]
SERVED = [
    (1, 3, JING_CHEN, "CHEN Jing"), (2, 0, 9000000002, "Guillen-Gosalbez, Gonzalo"), (3, 0, 9000000003, "Anita RøNne"),
    (4, 3, 9000000004, "Wei Wang"), (5, 3, JING_CHEN, "CHEN Jing"), (7, 0, 9000000007, "勲男 林"),
    (8, 0, 9000000008, "Kulyk, Iryna V."), (9, 0, 9000000009, "Jing Chen"), (10, 13, 9000000010, "Il Han Kim"),
    (6, 2, None, "Zhe Zhang"),   # a batch seat as served (unbound): never a holder
    (12, 1, 9000000012, "Jun Li"), (13, 0, 9000000013, "Christopher Smith"),
    (14, 0, 9000000014, "Université Sorbonne Nouvelle - Paris 3 (Francia)"),
]
EXPECT = {(1, 2): "blocked", (5, 2): "orcid_conflict", (6, 5): "blocked", (10, 14): "blocked", (12, 3): "blocked"}   # every other seat: no row
MUST_PARSE = ["CHEN Baozhang", "CHEN Jing", "Zhe Zhang", "Zhenyang Zhang", "Do Hoon Lim", "Il Han Kim", "Junhong Li", "Jun Li"]


def step_select():
    cells = ["".join(c["source"]) for c in json.load(open(NOTEBOOK))["cells"]]
    cell = next(c for c in cells if c.startswith("-- oxjob #2186 step C"))
    m = re.search(r"SELECT c\.\*, one_per_work_mode AS mode FROM \((.*)\) c\nWHERE one_per_work_mode", cell, re.S)
    sel = m.group(1)
    assert "openalex.authors.pending_author_assignments" in sel and "openalex.works.work_authors_curated" in sel
    return sel.replace("openalex.authors.pending_author_assignments", "fx_pending").replace(
        "openalex.works.work_authors_curated", "fx_served")


def num(v):
    return "CAST(NULL AS BIGINT)" if v is None else f"CAST({v} AS BIGINT)"


def build_sql():
    pend = " UNION ALL ".join(
        f"SELECT {num(w)} AS work_id, {s} AS author_sequence, {lit(n)} AS raw_author_name, 'MATCHED' AS match_outcome, "
        f"{num(a)} AS existing_author_id, {lit(m)} AS match_method, CAST(NULL AS STRING) AS name_match_tier"
        for w, s, n, a, m in PENDING)
    served = " UNION ALL ".join(
        f"SELECT {num(w)} AS work_id, {s} AS author_sequence, {num(a)} AS author_id, {lit(n)} AS raw_author_name"
        for w, s, a, n in SERVED + [(w, s, a, n) for w, s, n, a, _ in PENDING])   # tonight's seats are served too (as bound)
    return (f"WITH fx_pending AS ({pend}), fx_served AS ({served}) "
            f"SELECT work_id, author_sequence, action FROM ({step_select()}) ORDER BY 1, 2")


def main():
    names = ", ".join(lit(n) for n in MUST_PARSE)
    parsed = {r[0] for r in run(f"SELECT raw_author_name FROM openalex.authors.author_names WHERE raw_author_name IN ({names}) "
                               f"AND match_last IS NOT NULL")}
    missing = [n for n in MUST_PARSE if n not in parsed]
    assert not missing, f"author_names lacks parses for {missing}: the blocking cases would pass vacuously"
    got = {(int(w), int(s)): a for w, s, a in run(build_sql())}
    ok = True
    for w, s, n, a, m in PENDING:
        want, have = EXPECT.get((w, s)), got.get((w, s))
        mark = "PASS" if want == have else "FAIL"
        ok &= want == have
        print(f"{mark}  work {w} seat {s} {n!r} ({m}): expected {want or 'no row'}, got {have or 'no row'}")
    print(f"{sum(1 for k in [(w, s) for w, s, *_ in PENDING] if EXPECT.get(k) == got.get(k))}/{len(PENDING)} seats as expected")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
