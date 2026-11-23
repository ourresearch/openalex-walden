#!/usr/bin/env python3
"""Guard test for CreateWorkAuthorships step 5b-E, "a repeat another source confirms" (oxjob #2186).

Reads step 5b (A-E) straight from the notebook, runs it on literal fixture works on a SQL warehouse, and checks which
seats survive. Each fixture carries the confirming records the cell before the main statement would find (folded with
the notebook's own expression). The fixtures are judged works from oxjob #2186's samples (Opus, blind): drops judged
right, and the drops judged wrong, which the rule must not make (initials-only namesakes, a father and son, a shared
surname, an id slid onto a neighbour, names that share no word), plus synthetic guards (ORCID conflict, a source that names the person twice).

    python3 qa/oxjob2186_confirmed_repeat_test.py     # warehouse: $DATABRICKS_SQL_WAREHOUSE_ID or the serverless default
Needs the `databricks` CLI authenticated. Run it from $HOME if the shared checkout's databricks.yml is stale.
"""
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from oxjob1861_one_seat_test import (NOTEBOOK, CONFIRM_SRC_TYPE, SEAT_KEYS_TYPE, authorship_sql, lit, run, seat,  # noqa: E402
                                     step_sql)

A = "https://openalex.org/A"
I9 = "https://openalex.org/I9"


def cells():
    return ["".join(c["source"]) for c in json.load(open(NOTEBOOK))["cells"]]


def fold_sql():
    """The fold expression of 5b-E (main statement), for one name `x`; checked identical in the confirming-sources cell."""
    main = next(c for c in cells() if "dedupe_gate AS (" in c)
    # oxjob #2342 wrapped the column in IF(SPARK_PARTITION_ID() >= 0, ..., NULL)
    m = re.search(r"TRANSFORM\(kept_keys, x -> (.*)\) END(?:, NULL\))? AS ck", main)
    expr = m.group(1)
    assert "coalesce(x.name, '')" in expr
    side = next(c for c in cells() if "CREATE OR REPLACE TABLE openalex.works.work_byline_confirming_sources" in c)
    for arg in ("a.raw_author_name", "COALESCE(a.name, CONCAT_WS(' ', a.given, a.family))"):
        assert expr.replace("coalesce(x.name, '')", f"coalesce({arg}, '')") in side, f"fold differs in the side cell for {arg}"
    return expr.replace("coalesce(x.name, '')", "coalesce(x, '')")


# (label, deposit_byline, seats, confirming records [(provenance, names)], expected surviving names, extra checks)
CASES = [
    ("deposit repeats a co-author, PubMed lists him once (W4376209463): drop, merge affiliation / institution / corresponding", True,
     [seat("Christian Che-Castaldo", A + "1", "https://orcid.org/0000-0002-7670-2178"), seat("Grant Humphries", A + "2"),
      seat("Heather Lynch", A + "3", "https://orcid.org/0000-0002-9026-1612"),
      seat("Grant Humphries", A + "2", ras=["Black Bawks Data Science"], corr=True, inst=[I9])],
     [("pubmed", ["Christian Che-Castaldo", "Grant Humphries", "Heather Lynch"])],
     ["Christian Che-Castaldo", "Grant Humphries", "Heather Lynch"],
     {"merged": ("Grant Humphries", "Black Bawks Data Science", I9, True)}),
    ("two people each repeated, landing page lists them once (W4234090252)", True,
     [seat(n, A + str(k)) for n, k in [("Friedemann Kreuder", 1), ("Jens Roselt", 2), ("Hans-Christian von Herrmann", 3),
                                        ("Erika Fischer-Lichte", 4), ("Sonja Galle", 5), ("Clemens Risi", 6), ("Doris Kolesch", 7),
                                        ("Helmar Schramm", 8), ("Nikolaus Müller-Schöll", 9), ("Doris Kolesch", 7),
                                        ("Christopher Balme", 10), ("Christopher Balme", 10)]],
     [("landing_page", ["Friedemann Kreuder", "Jens Roselt", "Hans-Christian von Herrmann", "Erika Fischer-Lichte", "Sonja Galle",
                        "Clemens Risi", "Doris Kolesch", "Helmar Schramm", "Nikolaus Müller-Schöll", "Christopher Balme"])],
     ["Friedemann Kreuder", "Jens Roselt", "Hans-Christian von Herrmann", "Erika Fischer-Lichte", "Sonja Galle", "Clemens Risi",
      "Doris Kolesch", "Helmar Schramm", "Nikolaus Müller-Schöll", "Christopher Balme"], {}),
    ("with and without the accent, PubMed initials list her once (W4299688320)", True,
     [seat("Douglas W. Key", A + "1"), seat("Brenda C. Gough", A + "2"), seat("J. Brian Derbyshire", A + "3"),
      seat("Éva Nagy", A + "4"), seat("Eva Nagy", A + "4")],
     [("pubmed", ["D W Key", "B C Gough", "J B Derbyshire", "E Nagy"])],
     ["Douglas W. Key", "Brenda C. Gough", "J. Brian Derbyshire", "Éva Nagy"], {}),
    ("catalogue role text in one copy (BnF, W2760532378)", False,
     [seat("René. Auteur du texte Ngatsako", A + "1"), seat("René Ngatsako", A + "1")],
     [("repo", ["Ngatsako, René"])],
     ["René. Auteur du texte Ngatsako"], {"sole_author_corresponding": True}),
    ("after rule C drops a Cyrillic copy, a Latin repeat further on is still found (ids carried past C's drop)", False,
     [seat("Grant Humphries", A + "2"), seat("Heather Lynch", A + "3"), seat("Ivanov Ivan", A + "4"), seat("Иванов Иван", A + "4"),
      seat("Grant H. Humphries", A + "2")],
     [("crossref", ["Grant Humphries", "Heather Lynch", "Ivan Ivanov"])],
     ["Grant Humphries", "Heather Lynch", "Ivanov Ivan"], {}),
    # must not drop
    ("initials-only names (S.-H. Kim twice, W2134729936; judged two people): left alone", True,
     [seat("S.‐H. Kim", A + "1"), seat("Y.‐M. Kang", A + "2"), seat("S.‐H. Kim", A + "1"), seat("B.‐Y. Cho", A + "3"),
      seat("Y.‐M. Ye", A + "4"), seat("G.‐Y. Hur", A + "5"), seat("H.‐S. Park", A + "6")],
     [("repo", ["Kim, SH", "Kang, YM", "Cho, BY", "Ye, YM", "Hur, GY", "Park, HS"])],
     ["S.‐H. Kim", "Y.‐M. Kang", "S.‐H. Kim", "B.‐Y. Cho", "Y.‐M. Ye", "G.‐Y. Hur", "H.‐S. Park"], {}),
    ("father and son, A Plastino / A R Plastino (W2066636156): left alone", True,
     [seat("G B Roston", A + "1"), seat("M Casas", A + "2"), seat("A Plastino", A + "3"), seat("A R Plastino", A + "3")],
     [("repo", ["Roston, G.B.", "Casas, Montserrat", "Plastino, Angel Ricardo (Angelo)"])],
     ["G B Roston", "M Casas", "A Plastino", "A R Plastino"], {}),
    ("two Y. Wangs (W2129274639; judged two people): left alone", True,
     [seat("D.X. Wang", A + "1"), seat("Y. Zhang", A + "2"), seat("Y. Wang", A + "3"), seat("P. Lu"), seat("Y. Wang", A + "3")],
     [("landing_page", ["D.X. Wang", "Y. Zhang", "Y. Wang", "P. Lu"])],
     ["D.X. Wang", "Y. Zhang", "Y. Wang", "P. Lu", "Y. Wang"], {}),
    ("shared surname only, Gong-Yue Wang / Bei Wang (W4408685211): left alone", True,
     [seat("Zong-Bo Wu", A + "1"), seat("Gong-Yue Wang", A + "2"), seat("Bei Wang", A + "2"), seat("Xi Peng", A + "3")],
     [("crossref", ["Zong-Bo Wu", "Bei Wang", "Xi Peng"])],
     ["Zong-Bo Wu", "Gong-Yue Wang", "Bei Wang", "Xi Peng"], {}),
    ("id slid onto an unrelated name, Jian-Ping Li / Hong-Sheng Lin (W3159098433): left alone", True,
     [seat("Zhi-Sheng Ji"), seat("Jian-Ping Li", A + "1"), seat("Hua Yang", A + "2"), seat("Hong-Sheng Lin", A + "1")],
     [("landing_page", ["Zhi-Sheng Ji", "Jian-Ping Li", "Hua Yang"])],
     ["Zhi-Sheng Ji", "Jian-Ping Li", "Hua Yang", "Hong-Sheng Lin"], {}),
    ("names share no word, Romson Seah / Sunisa Siripongvutikorn (W7139956477; initials pass, 'S' inside 'R S'): left alone", True,
     [seat("Romson Seah", A + "1"), seat("Sunisa Siripongvutikorn", A + "1"), seat("Santad Wichienchot", A + "2"),
      seat("Worapong Usawakesmanee", A + "3")],
     [("crossref", ["Romson Seah", "Santad Wichienchot", "Worapong Usawakesmanee"])],
     ["Romson Seah", "Sunisa Siripongvutikorn", "Santad Wichienchot", "Worapong Usawakesmanee"], {}),
    ("two different ORCIDs on the repeated id: left alone", True,
     [seat("Maria Silva", A + "1", "https://orcid.org/0000-0001-0000-0001"), seat("Joao Costa", A + "2"),
      seat("Maria A. Silva", A + "1", "https://orcid.org/0000-0001-0000-0002")],
     [("crossref", ["Maria Silva", "Joao Costa"])],
     ["Maria Silva", "Joao Costa", "Maria A. Silva"], {}),
    ("the source names the repeated person twice (shared surname with a co-author): left alone", True,
     [seat("Ana Lima", A + "1"), seat("Rui Lima", A + "2"), seat("Ana Lima", A + "1")],
     [("crossref", ["Ana Lima", "Rui Lima"])],
     ["Ana Lima", "Rui Lima", "Ana Lima"], {}),
    ("the source lists someone else fewer: left alone", True,
     [seat("Ana Lima", A + "1"), seat("Rui Dias", A + "2"), seat("Ana Lima", A + "1"), seat("Eva Mota", A + "3")],
     [("crossref", ["Ana Lima", "Rui Dias"])],
     ["Ana Lima", "Rui Dias", "Ana Lima", "Eva Mota"], {}),
    ("the source lists them in another order: left alone", True,
     [seat("Ana Lima", A + "1"), seat("Rui Dias", A + "2"), seat("Ana Lima", A + "1")],
     [("crossref", ["Rui Dias", "Ana Lima"])],
     ["Ana Lima", "Rui Dias", "Ana Lima"], {}),
    ("no confirming record: unchanged", True,
     [seat("Grant Humphries", A + "2"), seat("Heather Lynch", A + "3"), seat("Grant Humphries", A + "2")],
     None, ["Grant Humphries", "Heather Lynch", "Grant Humphries"], {"unchanged": True}),
]


def confirm_sql(srcs, fold):
    if srcs is None:
        return f"CAST(NULL AS {CONFIRM_SRC_TYPE})"
    recs = ", ".join(f"NAMED_STRUCT('provenance', {lit(p)}, 'native_id', 'fixture', 'nk', "
                     f"TRANSFORM(ARRAY({', '.join(lit(n) for n in names)}), x -> {fold}))" for p, names in srcs)
    return f"ARRAY({recs})"


def build_sql():
    fold = fold_sql()
    rows = []
    for k, (_, pub, seats, srcs, _, _) in enumerate(CASES):
        auths = ", ".join(authorship_sql(s, i, len(seats)) for i, s in enumerate(seats))
        rows.append(f"SELECT {k} AS work_id, CAST(NULL AS TIMESTAMP) AS updated_datetime, "
                    f"{'TRUE' if pub else 'FALSE'} AS deposit_byline, ARRAY({auths}) AS authorships, "
                    f"{confirm_sql(srcs, fold)} AS confirm_src")
    return ("WITH fixtures AS (\n" + "\nUNION ALL\n".join(rows) + "\n),\n"
            "enriched_authorships AS (SELECT work_id, updated_datetime, deposit_byline, "
            "TRANSFORM(authorships, a -> a.author.id) AS author_ids, authorships, confirm_src, "
            "CASE WHEN confirm_src IS NOT NULL THEN TRANSFORM(authorships, a -> NAMED_STRUCT('name', a.raw_author_name, "
            f"'orcid', a.raw_orcid)) ELSE CAST(NULL AS {SEAT_KEYS_TYPE}) END AS seat_keys, "
            "CAST(NULL AS ARRAY<INT>) AS lc_t FROM fixtures),\n"
            + step_sql() + "\n"
            "SELECT o.work_id, TO_JSON(o.authorships) AS out, TO_JSON(f.authorships) AS input FROM one_seat_confirmed o "
            "JOIN fixtures f USING (work_id) ORDER BY o.work_id")


def main():
    rows = run(build_sql())
    failures = 0
    for wid, out, inp in rows:
        label, _, _, _, expected, checks = CASES[int(wid)]
        got = json.loads(out)
        names = [a["raw_author_name"] for a in got]
        ok = names == expected
        if ok and checks.get("unchanged"):
            ok = got == json.loads(inp)
        if ok and checks.get("sole_author_corresponding"):
            ok = len(got) == 1 and got[0]["is_corresponding"] is True
        if ok and checks.get("merged"):
            name, ras, inst, corr = checks["merged"]
            a = next(x for x in got if x["raw_author_name"] == name)
            ok = (ras in a["raw_affiliation_strings"] and any(x["raw_affiliation_string"] == ras for x in a["affiliations"])
                  and any(i["id"] == inst for i in a["institutions"]) and a["is_corresponding"] is corr)
        if ok and len(got) < len(json.loads(inp)):
            ok = [a["author_order_number"] for a in got] == list(range(len(got))) and got[-1]["author_position"] == (
                "last" if len(got) > 1 else "first") and got[0]["author_position"] == "first"
        failures += not ok
        print(("PASS " if ok else "FAIL ") + label + ("" if ok else f"\n     got {names}\n     want {expected}"))
    print(f"{len(rows) - failures}/{len(rows)} passed")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
