#!/usr/bin/env python3
"""Guard test for CreateWorkAuthorships step 5b-F, "language copies" (oxjob #2186).

Creates the Language_Copies task's UDF (read from notebooks/end2end/CreateLanguageCopyGroups, renamed into
openalex.transient), computes each fixture's targets the way the task does (on the names the task saw), applies the
main statement's printed-name guard (read from the notebook), runs step 5b (A-F, then 5b-E) straight from the notebook,
and checks which seats survive and what the kept seat carries. Fixtures: the shapes from oxjob #2186's samples (Kulyk x4,
blocks uk | ru | en, uneven per-person groups, a Latin copy without patronymic), the kept-id handover, ORCID / affiliation /
corresponding merges, and the cases the step must leave alone (two ORCIDs in one person, relatives, a byline that changed
since the task ran, Latin-only works that A-D handle as before).

    python3 qa/oxjob2186_language_copy_test.py     # warehouse: $DATABRICKS_SQL_WAREHOUSE_ID or the serverless default
Needs the `databricks` CLI authenticated. Run it from $HOME if the shared checkout's databricks.yml is stale.
"""
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from oxjob1861_one_seat_test import (CONFIRM_SRC_TYPE, NOTEBOOK, SEAT_KEYS_TYPE, authorship_sql, lit, run, seat,  # noqa: E402
                                     step_sql)

TASK_NOTEBOOK = os.path.join(os.path.dirname(NOTEBOOK), "CreateLanguageCopyGroups.ipynb")
TEST_UDF = "openalex.transient.oxjob2186_language_copy_targets_test"
# the task's output for the fixtures, written first as the task writes work_byline_language_copies: a query may call a
# Python UDF at most 5 times, and the optimizer would inline one call into every read of lc_t
TEST_TASK = "openalex.transient.oxjob2186_language_copy_task_test"
TEST_FIX = "openalex.transient.oxjob2186_language_copy_fixtures_test"  # one table scan, so the UDF is called in one place
A = "https://openalex.org/A"

# (label, seats, names the task saw (None = the same), expected surviving names, extra checks)
CASES = [
    ("Kulyk x4: uk full, uk initials, ru full, Latin (W7199223071): one seat, affiliations merged",
     [seat("Кулик, Ірина Володимирівна", A + "1", ras=["ОНУ ім. І. І. Мечникова"]), seat("Кулик, І. В.", A + "2"),
      seat("Кулик, Ирина Владимировна", A + "3", ras=["ОНУ им. И. И. Мечникова"]), seat("Kulyk, Iryna V.", A + "4")],
     None, ["Кулик, Ірина Володимирівна"],
     {"id": [A + "1"], "ras": ["ОНУ ім. І. І. Мечникова", "ОНУ им. И. И. Мечникова"], "sole_corresponding": True}),
    ("kept seat has no profile: takes the first copy's (Nick, 13 Nov)",
     [seat("Грищенко, Ольга Валентинівна"), seat("Грищенко, Ольга Валентиновна", A + "7"), seat("Gryshchenko, Olga", A + "8")],
     None, ["Грищенко, Ольга Валентинівна"], {"id": [A + "7"]}),
    ("blocks uk | ru | en, two people: order kept",
     [seat("Іваненко Олег Петрович", A + "1"), seat("Сидоренко Тетяна Юріївна", A + "2"), seat("Иваненко Олег Петрович", A + "3"),
      seat("Сидоренко Татьяна Юрьевна", A + "4", corr=True), seat("Ivanenko Oleh", A + "5"), seat("Sydorenko Tetiana", A + "6")],
     None, ["Іваненко Олег Петрович", "Сидоренко Тетяна Юріївна"], {"id": [A + "1", A + "2"], "corresponding": [False, True]}),
    ("uneven per-person groups (W3168541993): Сіренко x3, Нікітіна x2",
     [seat("Наталя Миколаївна Сіренко", A + "1"), seat("Наталья Николаевна Сиренко", A + "2"), seat("Natalya Sirenko", A + "3"),
      seat("Регіна Євгенівна Нікітіна", A + "4"), seat("Регина Евгеньевна Никитина", A + "4")],
     None, ["Наталя Миколаївна Сіренко", "Регіна Євгенівна Нікітіна"], {"id": [A + "1", A + "4"]}),
    ("a copy's ORCID moves to the kept seat",
     [seat("Петрук, Б. О.", A + "1"), seat("Petruk, B. O.", A + "2", "https://orcid.org/0000-0002-1825-0097")],
     None, ["Петрук, Б. О."], {"raw_orcid": ["https://orcid.org/0000-0002-1825-0097"]}),
    ("same id on two copies (rule C would also fold the Latin one): F decides, one seat",
     [seat("Ковальчук, Олена Іванівна", A + "1"), seat("Ковальчук, Елена Ивановна", A + "2"), seat("Kovalchuk, Olena I.", A + "1")],
     None, ["Ковальчук, Олена Іванівна"], {"id": [A + "1"]}),
    ("two ORCIDs inside one person: left alone", [seat("Кулик Ірина", A + "1", "https://orcid.org/0000-0001-0000-0001"),
      seat("Кулик Ирина", A + "2", "https://orcid.org/0000-0001-0000-0002")], None, ["Кулик Ірина", "Кулик Ирина"], {"unchanged": True}),
    ("relatives with different given names: left alone",
     [seat("Грищенко Ольга Валентинівна", A + "1"), seat("Грищенко Микола Григорович", A + "2")],
     None, ["Грищенко Ольга Валентинівна", "Грищенко Микола Григорович"], {"unchanged": True}),
    ("byline changed since the task ran (printed-name guard): left alone",
     [seat("Кулик, Ірина Володимирівна", A + "1"), seat("Петренко, Олег", A + "2"), seat("Кулик, Ирина Владимировна", A + "3")],
     ["Кулик, Ірина Володимирівна", "Кулик, Ирина Владимировна"],
     ["Кулик, Ірина Володимирівна", "Петренко, Олег", "Кулик, Ирина Владимировна"], {"unchanged": True}),
    ("one copy missing (A B A'): left alone",
     [seat("Кулик Ірина", A + "1"), seat("Петренко Олег", A + "2"), seat("Кулик Ирина", A + "3")],
     None, ["Кулик Ірина", "Петренко Олег", "Кулик Ирина"], {"unchanged": True}),
    ("Latin whole list repeated (rule A, CIPEEX): A still folds it",
     [seat("Rios, Ana", A + "1"), seat("Sousa, Mickael", A + "2"), seat("Rios, Ana", A + "1"), seat("Sousa, Mickael", A + "2")],
     None, ["Rios, Ana", "Sousa, Mickael"], {}),
    ("no repeats, no Cyrillic: unchanged", [seat("A. Smith", A + "1"), seat("B. Jones", A + "2")], None, ["A. Smith", "B. Jones"],
     {"unchanged": True}),
]


def cells(path):
    return ["".join(c["source"]) for c in json.load(open(path))["cells"]]


def create_udf():
    udf = next(c for c in cells(TASK_NOTEBOOK) if "CREATE OR REPLACE FUNCTION openalex.works.language_copy_targets" in c)
    run(udf.replace("openalex.works.language_copy_targets", TEST_UDF))


def guard_sql():
    """the main statement's step-5 guard, as written in the notebook (ba = the byline, lc = the task's row)"""
    main = next(c for c in cells(NOTEBOOK) if "dedupe_gate AS (" in c)
    m = re.search(r"(CASE WHEN lc\.work_id IS NOT NULL.*?END) AS lc_t", main, re.S)
    assert m, "guard not found in the main statement"
    return m.group(1)


def arr_sql(xs):
    return "ARRAY(" + ", ".join(lit(x) for x in xs) + ")"


def fixtures_sql():
    rows = []
    for k, (_, seats, seen, _, _) in enumerate(CASES):
        auths = ", ".join(authorship_sql(s, i, len(seats)) for i, s in enumerate(seats))
        seen = seen if seen is not None else [s["name"] for s in seats]
        seen_orc = [s["orcid"] for s in seats] if len(seen) == len(seats) else [None] * len(seen)
        rows.append(f"SELECT {k} AS work_id, CAST(NULL AS TIMESTAMP) AS updated_datetime, FALSE AS deposit_byline, "
                    f"ARRAY({auths}) AS authorships, {arr_sql(seen)} AS seen, {arr_sql(seen_orc)} AS seen_orcids")
    return "\nUNION ALL\n".join(rows)


def task_sql():
    """what the Language_Copies task writes for each fixture (on the names it saw)"""
    return (f"CREATE OR REPLACE TABLE {TEST_TASK} AS WITH "
            f"r AS (SELECT work_id, XXHASH64(seen) AS h, {TEST_UDF}(seen, seen_orcids) AS t FROM {TEST_FIX}) "
            "SELECT work_id, h, t FROM r WHERE t IS NOT NULL")


def build_sql():
    guard = guard_sql().replace("ba.authorships", "f.authorships")
    return ("WITH fixtures AS (\n" + fixtures_sql() + "\n),\n"
            "enriched_authorships AS (SELECT f.work_id, f.updated_datetime, f.deposit_byline, "
            "TRANSFORM(f.authorships, a -> a.author.id) AS author_ids, f.authorships, "
            f"CAST(NULL AS {CONFIRM_SRC_TYPE}) AS confirm_src, CAST(NULL AS {SEAT_KEYS_TYPE}) AS seat_keys, "
            f"{guard} AS lc_t FROM fixtures f LEFT JOIN {TEST_TASK} lc ON lc.work_id = f.work_id),\n"
            + step_sql() + "\n"
            "SELECT o.work_id, TO_JSON(o.authorships) AS out, TO_JSON(f.authorships) AS input FROM one_seat_confirmed o "
            "JOIN fixtures f USING (work_id) ORDER BY o.work_id")


def main():
    create_udf()
    run(f"CREATE OR REPLACE TABLE {TEST_FIX} AS {fixtures_sql()}")
    run(task_sql())
    rows = run(build_sql())
    failures = 0
    for wid, out, inp in rows:
        label, _, _, expected, checks = CASES[int(wid)]
        got = json.loads(out)
        names = [a["raw_author_name"] for a in got]
        ok = names == expected
        if ok and checks.get("unchanged"):
            ok = got == json.loads(inp)
        if ok and "id" in checks:
            ok = [a["author"]["id"] for a in got] == checks["id"]
        if ok and "ras" in checks:
            ok = sorted(got[0]["raw_affiliation_strings"]) == sorted(checks["ras"])
        if ok and "raw_orcid" in checks:
            ok = [a.get("raw_orcid") for a in got] == checks["raw_orcid"]
        if ok and "corresponding" in checks:
            ok = [a["is_corresponding"] for a in got] == checks["corresponding"]
        if ok and checks.get("sole_corresponding"):
            ok = len(got) == 1 and got[0]["is_corresponding"] is True
        if ok and len(got) < len(json.loads(inp)):
            ok = [a["author_order_number"] for a in got] == list(range(len(got))) and got[0]["author_position"] == "first" \
                and got[-1]["author_position"] == ("last" if len(got) > 1 else "first")
        failures += not ok
        print(("PASS " if ok else "FAIL ") + label + ("" if ok else f"\n     got {names} {[a['author']['id'] for a in got]}\n     want {expected}"))
    print(f"{len(rows) - failures}/{len(rows)} passed")
    run(f"DROP FUNCTION IF EXISTS {TEST_UDF}")
    run(f"DROP TABLE IF EXISTS {TEST_TASK}")
    run(f"DROP TABLE IF EXISTS {TEST_FIX}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
