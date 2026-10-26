#!/usr/bin/env python3
"""Guard test for CreateWorkAuthorships step 5b, "one seat per person" (oxjob #1861).

Reads the step's SQL straight from the notebook, runs it on literal fixture works on a SQL warehouse, and checks
which seats survive. The fixtures are the shapes from the tickets (repeated OJS/repository creator lists, PNNL
creators listed twice with one ORCID, kanji + romaji pairs) and the cases the step must leave alone (namesakes on a
publisher byline, initials-only names, one id on two different same-script names, different affiliations).

    python3 qa/oxjob1861_one_seat_test.py            # warehouse: $DATABRICKS_SQL_WAREHOUSE_ID or the serverless default
Needs the `databricks` CLI authenticated. Run it from $HOME if the shared checkout's databricks.yml is stale.
"""
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
NOTEBOOK = os.path.join(HERE, "..", "notebooks", "end2end", "CreateWorkAuthorships.ipynb")
WAREHOUSE = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "69a583ace3bdc8d0")


def step_sql():
    nb = json.load(open(NOTEBOOK))
    src = "".join(nb["cells"][4]["source"])
    start = src.index("dedupe_gate AS (")
    end = src.index("-- 6. Compute corresponding ids")
    return src[start:end].rstrip().rstrip(",")


def seat(name, aid=None, orcid=None, ras=(), corr=False, inst=()):
    return dict(name=name, aid=aid, orcid=orcid, ras=list(ras), corr=corr, inst=list(inst))


A = "https://openalex.org/A"
# (label, deposit_byline, seats, expected surviving names, extra checks)
CASES = [
    ("whole list repeated (CIPEEX / OJS per-language copies)", False,
     [seat("Rios, Ana", A + "1"), seat("Sousa, Mickael", A + "2"), seat("Rios, Ana", A + "1"), seat("Sousa, Mickael", A + "2")],
     ["Rios, Ana", "Sousa, Mickael"], {}),
    ("whole list repeated on a publisher deposit (AIAA)", True,
     [seat("J. Liu", A + "1"), seat("H. Shang", A + "2"), seat("J. Liu", A + "1"), seat("H. Shang", A + "2")],
     ["J. Liu", "H. Shang"], {}),
    ("same name + id, repository byline (Global Age W7110913065)", False,
     [seat("Lanzillo, Maria Laura", A + "1", inst=["https://openalex.org/I9360294"]), seat("Lanzillo, Maria Laura", A + "1")],
     ["Lanzillo, Maria Laura"], {"sole_author_corresponding": True, "institutions_kept": True}),
    ("same name + id + ORCID, publisher byline (PNNL / W7111114766)", True,
     [seat("Baker, Scott", A + "1"), seat("Adkins, Joshua", A + "2", "https://orcid.org/0000-0003-0399-0700"),
      seat("Adkins, Joshua", A + "2", "https://orcid.org/0000-0003-0399-0700", corr=True), seat("Law, Terry", A + "3")],
     ["Baker, Scott", "Adkins, Joshua", "Law, Terry"], {"merged_corresponding": "Adkins, Joshua"}),
    ("same name + id, publisher byline, no ORCID: may be namesakes, left alone", True,
     [seat("Yu Chen", A + "1", ras=["Wuxi Hospital"]), seat("Li Wang", A + "2"), seat("Yu Chen", A + "1", ras=["Wuxi Hospital"])],
     ["Yu Chen", "Li Wang", "Yu Chen"], {}),
    ("same initials-only name + id, repository byline: may be namesakes, left alone", False,
     [seat("Inyang, E. P.", A + "1"), seat("Ikot, A. N.", A + "2"), seat("Inyang, E. P.", A + "1")],
     ["Inyang, E. P.", "Ikot, A. N.", "Inyang, E. P."], {}),
    ("same name + id, different affiliations: may be two people, left alone", False,
     [seat("Maria Silva", A + "1", ras=["Univ A"]), seat("Maria Silva", A + "1", ras=["Univ B"])],
     ["Maria Silva", "Maria Silva"], {}),
    ("same name + id, different ORCIDs: two people, left alone", False,
     [seat("Maria Silva", A + "1", "https://orcid.org/0000-0001-0000-0001"),
      seat("Maria Silva", A + "1", "https://orcid.org/0000-0001-0000-0002")],
     ["Maria Silva", "Maria Silva"], {}),
    ("one id on two different same-script names: matcher error, left alone", True,
     [seat("Giorgina Mieli-Vergani", A + "1"), seat("Diego Vergani", A + "1")],
     ["Giorgina Mieli-Vergani", "Diego Vergani"], {}),
    ("kanji + romaji of one person (IRDB)", False,
     [seat("片岡, 孝夫", A + "1"), seat("KATAOKA, TAKAO", A + "1")],
     ["片岡, 孝夫"], {}),
    ("Latin, Cyrillic, Latin blocks (inscience.uz W7215682133); one Cyrillic seat has its own id", False,
     [seat("Sabirov, Obidjon", A + "1", ras=["Andijon"]), seat("Sobirov, Odil", A + "2", ras=["Andijon"]),
      seat("Сабиров, Обиджон", A + "9"), seat("Собиров, Одил", A + "2"),
      seat("Sabirov, Obidjon", A + "1", ras=["Andijon"]), seat("Sobirov, Odil", A + "2", ras=["Andijon"])],
     ["Sabirov, Obidjon", "Sobirov, Odil", "Сабиров, Обиджон"], {}),
    ("Cyrillic + Latin halves, one id each (W2914014112)", False,
     [seat("Д. С. Лундышев", A + "1"), seat("Igor Orlov", A + "2", "https://orcid.org/0000-0002-4717-7167"),
      seat("Д. С. Лундышев", A + "1"), seat("Igor Orlov", A + "2", "https://orcid.org/0000-0002-4717-7167")],
     ["Д. С. Лундышев", "Igor Orlov"], {}),
    ("no repeats: unchanged", True,
     [seat("A. Smith", A + "1"), seat("B. Jones", A + "2")], ["A. Smith", "B. Jones"], {"unchanged": True}),
    ("single author: unchanged", False, [seat("A. Smith", A + "1")], ["A. Smith"], {"unchanged": True}),
]


def lit(v):
    if v is None:
        return "CAST(NULL AS STRING)"
    return "'" + str(v).replace("'", "''") + "'"


def arr(xs):
    return "ARRAY(" + ", ".join(lit(x) for x in xs) + ")" if xs else "CAST(ARRAY() AS ARRAY<STRING>)"


def authorship_sql(s, i, n):
    pos = "first" if i == 0 else ("last" if i == n - 1 else "middle")
    insts = ", ".join(f"NAMED_STRUCT('country_code', 'XX', 'display_name', 'x', 'id', {lit(x)}, 'lineage', ARRAY({lit(x)}), "
                      f"'ror', CAST(NULL AS STRING), 'type', 'education')" for x in s["inst"])
    inst_sql = f"ARRAY({insts})" if s["inst"] else ("CAST(ARRAY() AS ARRAY<STRUCT<country_code: STRING, display_name: STRING, "
                                                    "id: STRING, lineage: ARRAY<STRING>, ror: STRING, type: STRING>>)")
    affs = ", ".join(f"NAMED_STRUCT('institution_ids', CAST(ARRAY() AS ARRAY<STRING>), 'raw_affiliation_string', {lit(r)})" for r in s["ras"])
    aff_sql = f"ARRAY({affs})" if s["ras"] else "CAST(ARRAY() AS ARRAY<STRUCT<institution_ids: ARRAY<STRING>, raw_affiliation_string: STRING>>)"
    return (f"NAMED_STRUCT('affiliations', {aff_sql}, "
            f"'author', NAMED_STRUCT('id', {lit(s['aid'])}, 'display_name', {lit(s['name'])}, 'orcid', {lit(s['orcid'])}, "
            f"'observed_orcids', CAST(ARRAY() AS ARRAY<STRING>)), "
            f"'raw_orcid', {lit(s['orcid'])}, 'author_position', '{pos}', 'author_order_number', {i}, "
            f"'countries', {arr(['XX'] if s['inst'] else [])}, 'institutions', {inst_sql}, "
            f"'is_corresponding', {'TRUE' if s['corr'] else 'FALSE'}, 'raw_affiliation_strings', {arr(s['ras'])}, "
            f"'raw_author_name', {lit(s['name'])})")


def build_sql():
    rows = []
    for k, (_, pub, seats, _, _) in enumerate(CASES):
        auths = ", ".join(authorship_sql(s, i, len(seats)) for i, s in enumerate(seats))
        rows.append(f"SELECT {k} AS work_id, CAST(NULL AS TIMESTAMP) AS updated_datetime, "
                    f"{'TRUE' if pub else 'FALSE'} AS deposit_byline, ARRAY({auths}) AS authorships")
    return ("WITH fixtures AS (\n" + "\nUNION ALL\n".join(rows) + "\n),\n"
            "enriched_authorships AS (SELECT work_id, updated_datetime, deposit_byline, "
            "TRANSFORM(authorships, a -> a.author.id) AS author_ids, authorships FROM fixtures),\n"
            + step_sql() + "\n"
            "SELECT o.work_id, TO_JSON(o.authorships) AS out, TO_JSON(f.authorships) AS input FROM one_seat_per_person o "
            "JOIN fixtures f USING (work_id) ORDER BY o.work_id")


def run(sql):
    body = json.dumps({"warehouse_id": WAREHOUSE, "statement": sql, "wait_timeout": "50s"})
    d = json.loads(subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--json", body],
                                  capture_output=True, text=True).stdout)
    sid = d["statement_id"]
    while d["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        d = json.loads(subprocess.run(["databricks", "api", "get", f"/api/2.0/sql/statements/{sid}"],
                                      capture_output=True, text=True).stdout)
    if d["status"]["state"] != "SUCCEEDED":
        sys.exit(f"SQL failed: {d['status']}")
    return d["result"]["data_array"]


def main():
    rows = run(build_sql())
    failures = 0
    for wid, out, inp in rows:
        label, _, _, expected, checks = CASES[int(wid)]
        got = json.loads(out)
        names = [a["raw_author_name"] for a in got]
        ok = names == expected
        if ok and checks.get("unchanged"):
            ok = got == json.loads(inp)
        if ok and checks.get("sole_author_corresponding"):
            ok = len(got) == 1 and got[0]["is_corresponding"] is True
        if ok and checks.get("institutions_kept"):
            ok = bool(got[0]["institutions"])
        if ok and checks.get("merged_corresponding"):
            ok = any(a["raw_author_name"] == checks["merged_corresponding"] and a["is_corresponding"] for a in got)
        if ok and len(got) < len(json.loads(inp)):
            ok = [a["author_order_number"] for a in got] == list(range(len(got))) and got[-1]["author_position"] == (
                "last" if len(got) > 1 else "first")
        failures += not ok
        print(("PASS " if ok else "FAIL ") + label + ("" if ok else f"\n     got {names}\n     want {expected}"))
    print(f"{len(rows) - failures}/{len(rows)} passed")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
