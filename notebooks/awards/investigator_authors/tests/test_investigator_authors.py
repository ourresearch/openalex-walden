"""Offline tests for the investigator -> author job (oxjob #1847). The job's SQL files run as they are on small fixtures: Spark SQL
is transpiled with sqlglot and executed in DuckDB, as nightly_grants' tests do (skipped when either is missing). Covered: the name
rule (same-person rule 2a-2e) on rows of Casey's author_names, decide.sql's layers, the tripwires, the MERGE, determinism, and the
listwise features against the training code's definition.
    uv run --with pytest --with sqlglot --with duckdb python -m pytest -q notebooks/awards/investigator_authors/tests
"""
import json
import pathlib
import re
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import iaa  # noqa: E402

SQL = {p.name: p.read_text() for p in (ROOT / "sql").glob("*.sql")}
PINS = iaa.pins({"prefix": "ia_", "run_date": "2026-11-03", "max_change": "0.05", "model": "m", "alias": "a"})
# Spark functions sqlglot leaves as they are, as DuckDB macros (Spark's get() and [] index from 0, DuckDB's lists from 1)
COMPAT = ["CREATE MACRO get(a, i) AS a[i + 1]", "CREATE MACRO arrays_overlap(a, b) AS list_has_any(a, b)",
          "CREATE MACRO raise_error(m) AS error(m)"]
INPUTS = {
    "openalex.authors.authors_api": "id VARCHAR, display_name VARCHAR, display_name_alternatives VARCHAR[], works_count INT, orcid VARCHAR, "
                                    "affiliations STRUCT(institution STRUCT(id VARCHAR), years INT[])[]",
    "openalex.authors.author_names": "raw_author_name VARCHAR, match_last VARCHAR, match_first VARCHAR, "
                                     "alt_parses STRUCT(key VARCHAR, first VARCHAR, last VARCHAR)[]",
    "openalex.awards.awards_api": "id BIGINT, funder_id BIGINT, start_year INT, end_year INT",
    "openalex.funders.funders": "funder_id BIGINT, country_code VARCHAR",
    "openalex.awards.award_id_redirects": "old_id BIGINT, canonical_id BIGINT",
    "openalex.awards.investigator_jev_links": "award_id BIGINT, slot VARCHAR, position INT, given_name VARCHAR, family_name VARCHAR, author_id BIGINT, p DOUBLE",
    "openalex.authors.orcid_majority_lookup": "author_id BIGINT, majority_orcid VARCHAR, majority_count BIGINT, second_count BIGINT, "
                                              "distinct_orcids BIGINT",
}


def spark_json_types(sql):
    """from_json(x, 'array<struct<a:string,...>>') -> DuckDB's from_json(x, '[{"a":"VARCHAR",...}]'): the one Spark form sqlglot keeps."""
    def conv(m):
        fields = re.findall(r"(\w+):string", m.group(1))
        return "'[{" + ",".join(f'"{f}":"VARCHAR"' for f in fields) + "}]'"
    return re.sub(r"'array<struct<([^>]*)>>'", conv, sql)


class Duck:
    """A DuckDB with empty copies of the job's openalex.* inputs; run() executes one of the job's SQL files."""
    def __init__(self):
        duckdb = pytest.importorskip("duckdb")
        self.sqlglot = pytest.importorskip("sqlglot")
        self.con = duckdb.connect()
        self.con.execute("ATTACH ':memory:' AS openalex")
        for schema in ("authors", "awards", "funders", "works", "institutions"):
            self.con.execute(f"CREATE SCHEMA openalex.{schema}")
        for table, cols in INPUTS.items():
            self.con.execute(f"CREATE TABLE {table} ({cols})")
        for macro in COMPAT:
            self.con.execute(macro)

    def run(self, name, **pins):
        for stmt in iaa.statements(iaa.fill(SQL[name], {**PINS, **pins})):
            self.con.execute(spark_json_types(self.sqlglot.transpile(stmt, read="spark", write="duckdb")[0]))

    def rows(self, sql, *args):
        return self.con.execute(sql, list(args)).fetchall()


# ---------- the name module: same-person rule 2a-2e ----------
# Casey's parser output for each profile name, exactly as openalex.authors.author_names held it on 2026-11-03 (read-only lookup)
PARSED = {
    "Hans Mueller": ("mueller", "hans", [("mueller", "hans"), ("hans", "muler")]),
    "Hans Müller": ("muller", "hans", [("muller", "hans"), ("hans", "muler")]),
    "Maria Garcia Lopez": ("garcialopez", "maria", [("maria", "garcia"), ("maria", "lopez"), ("maria", "garcia-lopez"),
                                                    ("maria", "garcialopez"), ("garcia lopez", "maria")]),
    "María García-López": ("garcialopez", "maria", [("maria", "garcia"), ("maria", "lopez"), ("maria", "garcia lopez"),
                                                    ("maria", "garcialopez"), ("garcia-lopez", "maria")]),
    "Sean OBrien": ("obrien", "sean", [("obrien", "sean")]),
    "John Smith": ("smith", "john", [("smith", "john")]),
    "J. Smith": ("smith", "j", []),
    "James Smith": ("smith", "james", [("smith", "james")]),
    "John B. Smith": ("smith", "john", [("smith", "john")]),
    "John Smith Jr.": ("smith", "john", [("smith", "john")]),
    "Kenji Sato": ("sato", "kenji", [("sato", "kenji")]),
    "Wang Wei": ("wei", "wang", [("wei", "wang")]),
    "WANG, Wei": ("wang", "wei", [("wang", "wei")]),
    "Wei Wang": ("wang", "wei", [("wang", "wei")]),
    "王伟": ("wang", "wei", [("wei", "wong"), ("wei", "yu")]),
    "佐藤 健": ("sato", "ken", [("sato", "ken"), ("ken", "saito"), ("ken", "satou")]),
    "Иван Иванов": ("ivanov", "ivan", [("ivan", "ivanov"), ("yvan", "yvanov")]),
    "Anna Becker": ("becker", "anna", [("becker", "anna")]),
    "Jan Berg": ("berg", "jan", [("berg", "jan")]),
    "Ana Oliveira": ("oliveira", "ana", [("oliveira", "ana")]),
}
# (grant given name, grant family name, profile display name, same name?) -- same-person rule 2
CASES = [
    ("Hans", "Müller", "Hans Mueller", True),                   # 2a: Müller = Mueller = Muller
    ("Hans", "Mueller", "Hans Müller", True),
    ("Hans", "Muller", "Hans Müller", True),
    ("María", "García-López", "Maria Garcia Lopez", True),      # 2a: García-López = Garcia Lopez
    ("Maria", "Garcia Lopez", "María García-López", True),
    ("Sean", "O'Brien", "Sean OBrien", True),                   # 2a: apostrophes
    ("J.", "Smith", "John Smith", True),                        # 2b: an initial
    ("John", "Smith", "J. Smith", True),
    ("Ken", "Sato", "Kenji Sato", False),                       # 2b: Ken is not Kenji
    ("John", "Smith", "James Smith", False),                    # 2b: John is not James
    ("John A.", "Smith", "John B. Smith", True),                # 2b: a different middle initial is a warning, not a veto
    ("Wei", "Wang", "Wang Wei", True),                          # 2c: order does not matter
    ("Wei", "Wang", "WANG, Wei", True),
    ("Wang", "Wei", "Wei Wang", True),                          # 2c: the grant's fields swapped
    ("Wei", "Wang", "王伟", True),                               # 2d: romanisations, through Casey's parser
    ("Ken", "Sato", "佐藤 健", True),
    ("Ivan", "Ivanov", "Иван Иванов", True),
    ("Anna", "Schmidt", "Anna Becker", False),                  # 2e: another family name
    ("Jan", "van der Berg", "Jan Berg", True),                  # particles
    ("Ana", "de Oliveira", "Ana Oliveira", True),
    ("John", "Smith Jr.", "John Smith", True),                  # suffixes and titles
    ("John", "Smith", "John Smith Jr.", True),
    ("Prof. Dr. Hans", "Müller", "Hans Müller", True),
    ("A/Prof Jane", "Doe", "John Smith", False),
]


def names_db():
    d = Duck()
    for i, (raw, (last, first, alts)) in enumerate(PARSED.items(), start=1):
        d.con.execute("INSERT INTO openalex.authors.authors_api VALUES (?, ?, [], 1, NULL, NULL)", [f"https://openalex.org/A{i}", raw])
        d.con.execute("INSERT INTO openalex.authors.author_names VALUES (?, ?, ?, ?)",
                      [raw, last, first, [{"key": "", "first": f, "last": l} for f, l in alts]])
    d.run("names.sql")
    return d, {raw: i for i, raw in enumerate(PARSED, start=1)}


def test_the_name_rule_on_same_person_rule_2a_to_2e():
    d, ids = names_db()
    for given, family, profile, want in CASES:
        got = d.rows("""SELECT coalesce(bool_or(ia_names_agree(ia_family_keys(?), get(ia_name_tokens(?), 0), [last_key], first_key)), false)
                        FROM ia_profile_names WHERE author_id = ?""", family, given, ids[profile])[0][0]
        assert got == want, (given, family, profile)


def test_name_tokens_and_family_keys():
    d, _ = names_db()
    assert d.rows("SELECT ia_name_tokens('Prof. Dr. Hans-Jürgen Müller Jr.')")[0][0] == ["hans", "jurgen", "muller"]
    assert d.rows("SELECT ia_name_tokens('A/Prof Jane')")[0][0] == ["jane"]
    assert d.rows("SELECT ia_name_tokens(?)", "Müller­")[0][0] == ["muller"]      # decomposed umlaut, soft hyphen
    assert sorted(d.rows("SELECT ia_family_keys('van der Berg')")[0][0]) == ["berg", "vanderberg"]
    assert sorted(d.rows("SELECT ia_family_keys('García-López')")[0][0]) == ["garcia", "garcialopez", "lopez"]


def test_romaji_fold_meets_hepburn_kunrei_and_long_vowels():
    d, _ = names_db()
    fold = lambda t: d.rows("SELECT ia_romaji_fold(?)", t)[0][0]
    for a, b in [("kohji", "koji"), ("kouji", "koji"), ("shimpei", "shinpei"), ("tsutomu", "tutomu"), ("ryuuichi", "ryuichi"),
                 ("shunsuke", "syunsuke"), ("junko", "zyunko")]:
        assert fold(a) == fold(b), (a, b)
    assert fold("yohei") == "yohei" and fold("koji") != fold("keishi")    # oh before a vowel stays


def test_names_compatible_is_looser_than_rule_2():
    """Why names_agree exists: Casey's openalex.authors.names_compatible (UpdateWorkAuthors.ipynb cell 3, copied) on the same keys."""
    d = Duck()
    d.con.execute("""CREATE MACRO names_compatible(al, af, bl, bf) AS coalesce(CASE
        WHEN al = bl AND (left(af, 1) = left(bf, 1) OR af IS NULL OR bf IS NULL) THEN true
        WHEN al = bf AND af = bl THEN true
        WHEN (starts_with(al, bl) OR ends_with(al, bl) OR starts_with(bl, al) OR ends_with(bl, al)) AND least(length(al), length(bl)) >= 4
             AND (left(af, 1) = left(bf, 1) OR af IS NULL OR bf IS NULL) THEN true ELSE false END, false)""")
    nc = lambda *a: d.rows("SELECT names_compatible(?, ?, ?, ?)", *a)[0][0]
    assert nc("sato", "ken", "sato", "kenji") and nc("smith", "john", "smith", "james")      # rule 2b says no
    assert nc("smith", "john", "smithson", "john")                                           # containment
    assert not nc("muller", "hans", "mueller", "hans")                                        # rule 2a says yes


# ---------- decide.sql: one named layer per slot ----------
O1, O2 = "0000-0001-0000-0001", "0000-0002-0000-0002"
# (award, slot, position, person_key, grant ORCID): one award per scenario unless the scenario is about two people on one award
SLOTS = [(1, "lead_investigator", 0, "a|x", None), (2, "lead_investigator", 0, "b|x", None), (3, "lead_investigator", 0, "c|x", None),
         (4, "lead_investigator", 0, "d|x", None), (5, "lead_investigator", 0, "e|x", None), (6, "lead_investigator", 0, "f|x", None),
         (7, "lead_investigator", 0, "g|x", None), (8, "lead_investigator", 0, "h|x", None), (9, "lead_investigator", 0, "i|x", None),
         (10, "lead_investigator", 0, "j|x", None), (11, "lead_investigator", 0, "k|x", None), (12, "lead_investigator", 0, "l|x", None),
         (13, "lead_investigator", 0, "m|x", O1), (14, "lead_investigator", 0, "n|x", O1),
         (15, "lead_investigator", 0, "smith|john", None), (15, "investigators", 0, "smith|jane", None),
         (16, "lead_investigator", 0, "lee|ann", None), (16, "investigators", 0, "lee|ann", None),
         (17, "lead_investigator", 0, "park|so", None), (17, "investigators", 0, "kim|bo", None),
         (20, "lead_investigator", 0, "t|x", None), (21, "lead_investigator", 0, "u|x", None), (22, "lead_investigator", 0, "|", None),
         (23, "lead_investigator", 0, "w|x", "0000-0003-0000-0003"), (25, "lead_investigator", 0, "y|x", "0000-0004-0000-0004")]
ROUTES = [(1, "lead_investigator", 0, 10, "orcid"), (2, "lead_investigator", 0, 20, "orcid"), (2, "lead_investigator", 0, 21, "nih_core_papers"),
          (3, "lead_investigator", 0, 30, "funder_person_id"), (4, "lead_investigator", 0, 40, "funder_person_id"),
          (5, "lead_investigator", 0, 50, "funder_person_id"), (14, "lead_investigator", 0, 140, "linked_paper"),
          (17, "lead_investigator", 0, 170, "orcid"), (17, "lead_investigator", 0, 170, "linked_paper"),
          (23, "lead_investigator", 0, 231, "linked_paper"), (25, "lead_investigator", 0, 250, "orcid")]
# entry -> [(author, score)], the scored 1-6 lists (ScoreCandidates's output); thresholds 0.9649769 / 0.995 / 0.5
SCORES = {"3|c|x": [(31, 0.99), (32, 0.1)], "4|d|x": [(40, 0.99)], "5|e|x": [(51, 0.5), (52, 0.1)], "6|f|x": [(60, 0.97), (61, 0.2)],
          "7|g|x": [(70, 0.99), (71, 0.6)], "8|h|x": [(80, 0.99), (81, 0.1), (82, 0.1), (83, 0.1), (84, 0.1)],
          "9|i|x": [(90, 0.96), (91, 0.1)], "13|m|x": [(130, 0.99)], "15|smith|john": [(150, 0.99)], "15|smith|jane": [(150, 0.98)],
          "16|lee|ann": [(160, 0.99)], "17|kim|bo": [(170, 0.99)], "20|t|x": [(201, 0.99), (200, 0.99)],
          "21|u|x": [(210, 0.996), (211, 0.1), (212, 0.1), (213, 0.1)]}
WANT = {(1, "lead_investigator", 0): "route_bind", (2, "lead_investigator", 0): "route_conflict",
        (3, "lead_investigator", 0): "route_conflict", (4, "lead_investigator", 0): "route_bind", (5, "lead_investigator", 0): "route_bind",
        (6, "lead_investigator", 0): "model_bind", (7, "lead_investigator", 0): "two_plausible",
        (8, "lead_investigator", 0): "model_relaxed", (9, "lead_investigator", 0): "below_threshold",   # 8: a list of 5 at 0.99
        (10, "lead_investigator", 0): "too_many_candidates", (11, "lead_investigator", 0): "too_many_candidates",
        (12, "lead_investigator", 0): "no_candidates", (13, "lead_investigator", 0): "guard_orcid", (14, "lead_investigator", 0): "guard_orcid",
        (15, "lead_investigator", 0): "guard_shared_author", (15, "investigators", 0): "guard_shared_author",
        (16, "lead_investigator", 0): "model_bind", (16, "investigators", 0): "model_bind",
        (17, "lead_investigator", 0): "route_bind", (17, "investigators", 0): "guard_shared_author",
        (20, "lead_investigator", 0): "two_plausible", (21, "lead_investigator", 0): "model_bind",
        (22, "lead_investigator", 0): "no_name_key",                     # 佐藤 健 printed in kanji: no a-z key yet (phase 2)
        (23, "lead_investigator", 0): "guard_orcid",                     # the grant's ORCID is on profile 230; the paper route names 231
        (25, "lead_investigator", 0): "route_bind"}                      # the grant's ORCID is on 250, and 250 is bound
TH = (0.9649769067764282, 0.995, 0.5)   # the registered version's tags, as RegisterMatcher writes them from gr1102_model.json

# model_relaxed: 80 a list of 1 at 0.955 (band A); 81 a list of 5 at 0.985 with the second at 0.1 (band B); 82 the same but the
# second at 0.3; 83 a list of 1 at 0.94; 84 a list of 1 at 0.955 where a route names someone else; 85 two people on one award, both
# relaxed to 850; 86 a list of 1 at 0.955 whose grant ORCID is on another single profile (861); 87 a list of 5 at 0.97 (band C: dropped)
O5 = "0000-0005-0000-0005"
SLOTS5 = [(80, "lead_investigator", 0, "r80|x", None), (81, "lead_investigator", 0, "r81|x", None), (82, "lead_investigator", 0, "r82|x", None),
          (83, "lead_investigator", 0, "r83|x", None), (84, "lead_investigator", 0, "r84|x", None),
          (85, "lead_investigator", 0, "r85a|x", None), (85, "investigators", 0, "r85b|x", None), (86, "lead_investigator", 0, "r86|x", O5),
          (87, "lead_investigator", 0, "r87|x", None)]
ROUTES5 = [(84, "lead_investigator", 0, 841, "linked_paper")]
SCORES5 = {"80|r80|x": [(800, 0.955)], "81|r81|x": [(810, 0.985), (811, 0.1), (812, 0.05), (813, 0.05), (814, 0.01)],
           "87|r87|x": [(870, 0.97), (871, 0.1), (872, 0.05), (873, 0.05), (874, 0.01)],
           "82|r82|x": [(820, 0.97), (821, 0.3), (822, 0.05), (823, 0.05), (824, 0.01)], "83|r83|x": [(830, 0.94)],
           "84|r84|x": [(840, 0.955)], "85|r85a|x": [(850, 0.955)], "85|r85b|x": [(850, 0.956)], "86|r86|x": [(860, 0.955)]}


def decide_db(reverse=False, th=TH, unscored=(), kaken=False, relaxed=False, prep=None, jev=False):
    d = Duck()
    d.con.execute("""CREATE TABLE ia_people (award_id BIGINT, slot VARCHAR, position INT, given_name VARCHAR, family_name VARCHAR,
                     orcid VARCHAR, person_key VARCHAR, fam_keys VARCHAR[], funder_id BIGINT, provenance VARCHAR, funder_award_id VARCHAR,
                     gt VARCHAR[])""")
    d.con.execute("CREATE TABLE ia_routes (award_id BIGINT, slot VARCHAR, position INT, author_id BIGINT, method VARCHAR, evidence VARCHAR)")
    d.con.execute("CREATE TABLE ia_shortlist (entry_id VARCHAR, author_id BIGINT, n_short BIGINT, capped BOOLEAN)")
    d.con.execute("""CREATE TABLE ia_scores (entry_id VARCHAR, author_id BIGINT, s DOUBLE, model_version INT, th99 DOUBLE, th_4_6 DOUBLE,
                     guard_second DOUBLE)""")
    d.con.execute("CREATE TABLE ia_profile_orcids (author_id BIGINT, orcid VARCHAR, n_profiles BIGINT)")
    order = (lambda xs: list(reversed(xs))) if reverse else list
    slots, routes, scores = SLOTS, ROUTES, SCORES
    if kaken:
        slots, routes, scores = slots + SLOTS3, routes + ROUTES3, {**scores, **SCORES3}
    if relaxed:
        slots, routes, scores = slots + SLOTS5, routes + ROUTES5, {**scores, **SCORES5}
    if jev:
        slots, routes = slots + SLOTS8, routes + ROUTES8
    for a, slot, pos, key, orcid in order(slots):
        fam_keys = [key.split("|")[0]] if key[0] != "|" else []
        d.con.execute("INSERT INTO ia_people VALUES (?, ?, ?, 'G', 'F', ?, ?, ?, NULL, NULL, NULL, NULL)", [a, slot, pos, orcid, key, fam_keys])
    for a, slot, pos, author, method in order(routes):
        d.con.execute("INSERT INTO ia_routes VALUES (?, ?, ?, ?, ?, '{}')", [a, slot, pos, author, method])
    for e, cands in order(sorted(scores.items())):
        for author, s in order(cands):
            d.con.execute("INSERT INTO ia_shortlist VALUES (?, ?, ?, false)", [e, author, len(cands)])
            if e not in unscored:
                d.con.execute("INSERT INTO ia_scores VALUES (?, ?, ?, 1, ?, ?, ?)", [e, author, s, *th])
    for i in range(8):
        d.con.execute("INSERT INTO ia_shortlist VALUES ('10|j|x', ?, 8, false)", [100 + i])
    d.con.execute("INSERT INTO ia_shortlist VALUES ('11|k|x', NULL, NULL, true)")
    d.con.execute(f"""INSERT INTO ia_profile_orcids VALUES (130, '{O2}', 1), (140, '{O2}', 1), (10, '{O1}', 1),
                      (230, '0000-0003-0000-0003', 1), (250, '0000-0004-0000-0004', 1)""")
    if prep:
        prep(d)
    d.run("decide.sql")
    return d


def test_every_slot_gets_its_named_layer():
    d = decide_db()
    got = {(a, s, p): layer for a, s, p, layer in d.rows("SELECT award_id, slot, position, layer FROM ia_decisions")}
    assert got == WANT
    served = d.rows("SELECT award_id, slot, author_id, method, layer FROM to_serve ORDER BY 1, 2")
    assert [(a, s) for a, s, *_ in served] == [(1, "lead_investigator"), (4, "lead_investigator"), (5, "lead_investigator"),
                                               (6, "lead_investigator"), (8, "lead_investigator"), (16, "investigators"),
                                               (16, "lead_investigator"), (17, "lead_investigator"), (21, "lead_investigator"),
                                               (25, "lead_investigator")]
    by = {(a, s): (author, method) for a, s, author, method, _ in served}
    assert by[(4, "lead_investigator")] == (40, "funder_person_id") and by[(6, "lead_investigator")] == (60, "student")
    assert by[(17, "lead_investigator")] == (170, "linked_paper+orcid")        # two routes, one author: one row, methods joined


def test_an_id_route_against_the_student_is_held_not_served():
    d = decide_db()
    row = d.rows("SELECT author_id, model_author, methods FROM ia_decisions WHERE award_id = 3")[0]
    assert row == (None, 31, ["funder_person_id"])
    assert json.loads(d.rows("SELECT evidence FROM ia_decisions WHERE award_id = 3")[0][0])["student"]["author_id"] == 31


def test_an_exact_tie_goes_to_the_lower_author_id():
    d = decide_db()
    assert d.rows("SELECT model_author FROM ia_decisions WHERE award_id = 20")[0][0] == 200


def test_a_list_without_a_score_or_a_model_with_other_thresholds_fails_the_run():
    with pytest.raises(Exception, match="model_missing"):
        decide_db(unscored=("6|f|x",))
    with pytest.raises(Exception, match="model_missing"):
        decide_db(th=(0.9, 0.995, 0.5))
    def drop_one(d):   # one candidate of a 2+ list loses its score: a smaller list would get a lower bar
        d.con.execute("DELETE FROM ia_scores WHERE rowid = (SELECT min(s.rowid) FROM ia_scores s JOIN "
                      "(SELECT entry_id FROM ia_scores GROUP BY 1 HAVING count(*) >= 2 ORDER BY 1 LIMIT 1) e USING (entry_id))")
    with pytest.raises(Exception, match="model_missing"):
        decide_db(prep=drop_one)
    def null_one(d):   # a NULL score is no score either
        d.con.execute("UPDATE ia_scores SET s = NULL WHERE rowid = (SELECT min(s.rowid) FROM ia_scores s JOIN "
                      "(SELECT entry_id FROM ia_scores GROUP BY 1 HAVING count(*) >= 2 ORDER BY 1 LIMIT 1) e USING (entry_id))")
    with pytest.raises(Exception, match="model_missing"):
        decide_db(prep=null_one)


def test_one_entry_two_people_is_held_not_served_to_both():
    def prep(d):   # Ann A. Lee and Ann B. Lee on award 116: one person_key, so one entry and one pick
        for slot, gt in (("lead_investigator", ["ann", "a"]), ("co_lead_investigator", ["ann", "b"])):
            d.con.execute("INSERT INTO ia_people VALUES (116, ?, 0, 'G', 'F', NULL, 'lee|ann', ['lee'], NULL, NULL, NULL, ?)", [slot, gt])
        d.con.execute("INSERT INTO ia_shortlist VALUES ('116|lee|ann', 1160, 1, false)")
        d.con.execute("INSERT INTO ia_scores VALUES ('116|lee|ann', 1160, 0.99, 1, ?, ?, ?)", list(TH))
    got = {s: layer for a, s, layer in decide_db(prep=prep).rows("SELECT award_id, slot, layer FROM ia_decisions WHERE award_id = 116")}
    assert got == {"lead_investigator": "guard_shared_author", "co_lead_investigator": "guard_shared_author"}
    def same(d):   # the same person printed twice keeps the pick on both slots
        prep(d)
        d.con.execute("UPDATE ia_people SET gt = ['ann', 'a'] WHERE award_id = 116")
    got = {s: layer for a, s, layer in decide_db(prep=same).rows("SELECT award_id, slot, layer FROM ia_decisions WHERE award_id = 116")}
    assert got == {"lead_investigator": "model_bind", "co_lead_investigator": "model_bind"}


def test_decide_is_deterministic_and_reruns_replace_the_run():
    a = decide_db().rows("SELECT * FROM ia_decisions ORDER BY award_id, slot, position")
    d = decide_db(reverse=True)
    assert d.rows("SELECT * FROM ia_decisions ORDER BY award_id, slot, position") == a
    d.run("decide.sql")                                        # the same run again replaces the table
    assert d.rows("SELECT * FROM ia_decisions ORDER BY award_id, slot, position") == a


def test_no_clock_random_or_limit_in_any_step():
    for name, text in SQL.items():
        code = "\n".join(l.split("--")[0] for l in text.splitlines()).lower()
        assert not re.search(r"current_(timestamp|date)|\bnow\(|\brand(n)?\(|\buuid\(|\blimit\b|tablesample", code), name


# ---------- tripwires.sql, then publish.sql's MERGE ----------
def publish_db():
    return decide_db()


def tripped(d, **pins):
    try:
        d.run("tripwires.sql", **pins)
    except Exception as e:
        return {k: v for k, v in json.loads(re.search(r"(\{.*\})", str(e)).group(1)).items() if v and k in FAULTS}
    return {}


FAULTS = {
    "shrink": "INSERT INTO ia_authors SELECT * FROM to_serve; INSERT INTO ia_authors SELECT award_id + 100, slot, position, given_name, "
              "family_name, orcid, author_id, method, layer, evidence, model_version, run_date FROM to_serve",   # served now 20, new set 10
    "churn": "INSERT INTO ia_authors SELECT award_id, slot, position, given_name, family_name, orcid, CASE WHEN award_id = 6 THEN 61 ELSE "
             "author_id END, method, layer, evidence, model_version, run_date FROM to_serve",
}


def test_tripwires_pass_a_clean_set_and_name_each_fault():
    assert tripped(publish_db()) == {}
    for name, fault in FAULTS.items():
        d = publish_db()
        d.con.execute(fault)
        got = tripped(d)
        assert name in got, (name, got)
        assert set(got) <= {name, "shrink", "churn"}, (name, got)     # removing rows also moves the counts against what is served


def test_the_go_live_run_may_change_everything_and_later_runs_only_a_little():
    d = publish_db()
    d.con.execute(FAULTS["churn"])                                    # 1 of 10 served rows re-pointed: 10%
    assert tripped(d) == {"churn": 1}
    assert tripped(d, MAXCHG="1.0") == {}


def test_publish_merges_once_and_a_rerun_changes_nothing():
    d = publish_db()
    d.run("tripwires.sql")
    d.run("publish.sql")
    first = d.rows("SELECT * FROM ia_authors ORDER BY award_id, slot, position")
    assert first == d.rows("SELECT * FROM to_serve ORDER BY award_id, slot, position") and len(first) == 10
    d.run("publish.sql")
    assert d.rows("SELECT * FROM ia_authors ORDER BY award_id, slot, position") == first
    d.con.execute("UPDATE ia_decisions SET layer = 'below_threshold' WHERE award_id = 21")   # no longer served -> deleted
    d.con.execute("UPDATE ia_decisions SET author_id = 61 WHERE award_id = 6")               # re-pointed -> updated
    d.con.execute("UPDATE ia_decisions SET orcid = '0000-0009-0000-0009' WHERE award_id = 1")  # the grant's ORCID edited -> updated
    d.run("publish.sql")
    got = {a: (author, orcid) for a, author, orcid in d.rows("SELECT award_id, author_id, orcid FROM ia_authors")}
    assert 21 not in got and got[6][0] == 61 and got[1][1] == "0000-0009-0000-0009" and len(got) == 8   # award 16 holds two slots


# ---------- features: the listwise columns equal the training code's definition ----------
def test_listwise_columns_equal_the_training_sorted_array_definition():
    d = Duck()
    rows = [("e1", 1, 3, 0.9), ("e1", 2, 3, 0.4), ("e1", 3, 3, None), ("e2", 4, 2, 0.7), ("e2", 5, 2, 0.7), ("e3", 6, 1, 0.2)]
    d.con.execute("CREATE TABLE f (entry_id VARCHAR, author_id BIGINT, n_short BIGINT, v DOUBLE)")
    d.con.executemany("INSERT INTO f VALUES (?, ?, ?, ?)", rows)
    sql = f"SELECT entry_id, author_id, {iaa.listwise(['v'])['LISTWISE']} FROM f ORDER BY author_id"
    got = d.rows(d.sqlglot.transpile(sql, read="spark", write="duckdb")[0])
    for (e, a, n, v), (_, _, dv, rv) in zip(rows, got):
        arr = sorted([x for ee, _, _, x in rows if ee == e and x is not None], reverse=True)   # training code: sort_array(collect_list(v), false)
        best_other = arr[0] if v is None or v != arr[0] else (arr[1] if len(arr) > 1 else None)
        want = (v - best_other if v is not None and best_other is not None else None) if n > 1 else 0.0
        assert dv == pytest.approx(want) if want is not None else dv is None, (e, a)
        assert rv == (None if v is None else 1 + sum(x > v for x in arr)), (e, a)


# ---------- the files themselves ----------
def test_every_step_fills_its_pins_parses_and_keeps_lines_short():
    sqlglot = pytest.importorskip("sqlglot")
    for name, text in SQL.items():
        for stmt in iaa.statements(iaa.fill(text, PINS)):
            sqlglot.parse_one(stmt, read="spark")
        assert max(len(l) for l in text.splitlines()) <= 150, name
    for path in [*ROOT.glob("*.py"), ROOT.parents[2] / "jobs/award_investigator_authors.yaml"]:
        assert max(len(l) for l in path.read_text().splitlines()) <= 150, path


def test_the_job_runs_every_file_once_in_order():
    yml = (ROOT.parents[2] / "jobs/award_investigator_authors.yaml").read_text()
    notebooks = re.findall(r"notebook_path: notebooks/awards/investigator_authors/(\w+)", yml)
    assert notebooks == ["Prepare", "RunSql", "RunSql", "ScoreCandidates", "RunSql"]
    assert all((ROOT / f"{n}.py").exists() for n in notebooks)
    run = ["names.sql", "people.sql"] + [f for line in re.findall(r"sql: (\S+)", yml) for f in line.split(",")]
    assert run == ["names.sql", "people.sql", "routes.sql", "shortlist.sql", "features.sql", "decide.sql", "tripwires.sql", "publish.sql"]
    assert sorted(run) == sorted(SQL)
    assert tuple(re.findall(r"- name: (\w+)", yml)) == iaa.NAMES         # the job's parameters, defined once (in the YAML)
    assert re.findall(r"views: (\S+)", yml) == ["nsfc_pinyin"] and set(iaa.VIEWS) == {"nsfc_pinyin"}   # built in the routes task
    assert "pypinyin==0.55.0" in yml


def test_excluded_funders_file_is_valid_and_holds_the_sweden_rule():
    rows = iaa.read_excluded()
    assert (None, "SE") in [(f, c) for f, c, _, _ in rows]
    assert {4320320940, 4320327257} <= {f for f, _, _, _ in rows}


def test_every_step_is_safe_under_ansi_mode():
    """Serverless jobs run with ANSI on: an out-of-range array index or a failed cast stops the run (rehearsal 11-03: element_at(ft, -1)
    on a Han-script family name, whose a-z tokens are empty). Spark-only failures DuckDB does not reproduce, so the rule is checked here."""
    for name, text in SQL.items():
        code = "\n".join(l.split("--")[0] for l in text.splitlines())
        assert not re.search(r"(?<!try_)element_at\(", code), name          # try_element_at: NULL when out of range
        assert not re.search(r"\w\[\d+\]", code), name                      # get(a, i): NULL when out of range
        assert not re.search(r"(?<!try_)\bCAST\(regexp_extract", code, re.I), name   # try_cast: NULL when the id has no number


def test_features_survive_a_person_without_a_latin_family_name():
    d = Duck()
    d.con.execute("CREATE TABLE ia_people (award_id BIGINT, ft VARCHAR[], given_key VARCHAR)")
    d.con.execute("INSERT INTO ia_people VALUES (1, [], 'ken'), (1, ['sato'], 'ken')")
    op = re.search(r"\(SELECT award_id, (try_element_at\(ft, -1\)) AS kf", SQL["features.sql"]).group(1)
    sql = d.sqlglot.transpile(f"SELECT {op} FROM ia_people ORDER BY award_id, size(ft)", read="spark", write="duckdb")[0]
    assert [r[0] for r in d.rows(sql)] == [None, "sato"]



def test_an_entry_takes_the_institutions_of_all_its_slots():
    """John Smith leads award 50 with no institution and is in its investigators[] with one (an affiliation ROR): one entry, asked with
    that institution. Jo Ng (award 51) has none on any slot: not asked."""
    d = Duck()
    d.con.execute("CREATE TABLE ia_routes (award_id BIGINT, slot VARCHAR, position INT, author_id BIGINT, method VARCHAR, evidence VARCHAR)")
    d.con.execute("""CREATE TABLE ia_people (award_id BIGINT, funder_id BIGINT, slot VARCHAR, position INT, given_name VARCHAR,
                     family_name VARCHAR, gt VARCHAR[], ft VARCHAR[], fam_keys VARCHAR[], given_key VARCHAR, person_key VARCHAR,
                     multi_name BOOLEAN, insts BIGINT[], start_year INT, end_year INT, lo INT, hi INT)""")
    for a, slot, given, family, insts in [(50, "lead_investigator", "John", "Smith", []), (50, "investigators", "John", "Smith", [100]),
                                          (51, "lead_investigator", "Jo", "Ng", [])]:
        g, f = given.lower(), family.lower()
        d.con.execute("INSERT INTO ia_people VALUES (?, 1, ?, 0, ?, ?, [?], [?], [?], ?, ?, false, ?, 2020, 2022, 2017, 2025)",
                      [a, slot, given, family, g, f, f, g, f"{f}|{g}", insts])
    entries = next(iter(iaa.statements(iaa.fill(SQL["shortlist.sql"], PINS))))
    d.con.execute(d.sqlglot.transpile(entries, read="spark", write="duckdb")[0])
    assert d.rows("SELECT entry_id, insts FROM entries") == [("50|smith|john", [100])]

def test_an_orcid_bind_is_held_when_the_profile_mostly_carries_another_orcid():
    def prep(majority):
        def go(d):
            d.con.execute(f"""UPDATE ia_routes SET evidence = '{{"orcid": "{O1}"}}' WHERE method = 'orcid'""")
            d.con.execute("INSERT INTO openalex.authors.orcid_majority_lookup VALUES (10, ?, 9, 2, 2), (170, ?, 9, 2, 2)",
                          [f"https://orcid.org/{majority}"] * 2)
        return go
    layer = lambda d, a: d.rows("SELECT layer FROM ia_decisions WHERE award_id = ? AND slot = 'lead_investigator'", a)[0][0]
    d = decide_db(prep=prep("0000-0009-0000-0009"))     # award 1: ORCID route alone, the profile's main ORCID is someone else's
    assert layer(d, 1) == "guard_orcid" and layer(d, 17) == "guard_orcid"  # 17: held too, though a linked paper agrees (Kyle, 11-05: all 99)
    assert d.rows("SELECT author_id FROM ia_decisions WHERE award_id = 1")[0][0] == 10
    d = decide_db(prep=prep(O1))                        # the profile's main ORCID is the grant's: served as before
    assert layer(d, 1) == "route_bind"
    assert {k: v for k, v in decide_db().rows("SELECT award_id, layer FROM ia_decisions")}[1] == "route_bind"   # no lookup row: unchanged


def test_model_relaxed_serves_never_against_a_route_and_keeps_the_guards():
    prep = lambda d: d.con.execute("INSERT INTO ia_profile_orcids VALUES (861, ?, 1)", [O5])
    d = decide_db(relaxed=True, prep=prep)
    on = {(a, s): (layer, au) for a, s, layer, au in d.rows("SELECT award_id, slot, layer, author_id FROM ia_decisions "
                                                          "WHERE award_id BETWEEN 80 AND 87")}
    assert on[(80, "lead_investigator")] == ("model_relaxed", 800) and on[(81, "lead_investigator")] == ("model_relaxed", 810)
    assert on[(82, "lead_investigator")][0] == on[(83, "lead_investigator")][0] == "below_threshold"     # second 0.3; s 0.94
    assert on[(84, "lead_investigator")] == ("route_bind", 841)            # a route claims the slot: the relaxed pick never conflicts
    assert on[(85, "lead_investigator")][0] == on[(85, "investigators")][0] == "below_threshold"   # two people, one profile
    assert on[(86, "lead_investigator")][0] == "below_threshold"           # the grant's ORCID is on another profile
    assert on[(87, "lead_investigator")][0] == "below_threshold"           # band C (4-6 at 0.965-0.98) is not served
    assert d.rows("SELECT award_id, author_id, method, model_version FROM to_serve WHERE layer = 'model_relaxed' ORDER BY 1") == \
        [(8, 80, "student_relaxed", 1), (80, 800, "student_relaxed", 1), (81, 810, "student_relaxed", 1)]   # 8: a list of 5 at 0.99 (B)


# jev_link: 110 alone; 111 with the ORCID route on the same author; 112 against a linked paper; 113 one profile for two people
SLOTS8 = [(110, "lead_investigator", 0, "j110|x", None), (111, "lead_investigator", 0, "j111|x", None),
          (112, "lead_investigator", 0, "j112|x", None), (113, "lead_investigator", 0, "ja|x", None), (113, "investigators", 0, "jb|y", None)]
ROUTES8 = [(110, "lead_investigator", 0, 1100, "jev_link"), (111, "lead_investigator", 0, 1110, "jev_link"),
           (111, "lead_investigator", 0, 1110, "orcid"), (112, "lead_investigator", 0, 1120, "jev_link"),
           (112, "lead_investigator", 0, 1121, "linked_paper"), (113, "lead_investigator", 0, 1130, "jev_link"),
           (113, "investigators", 0, 1130, "jev_link")]


def test_decide_gives_jev_its_own_layer_and_holds_it_like_a_name_route():
    d = decide_db(jev=True)
    got = {(a, s): layer for a, s, layer in d.rows("SELECT award_id, slot, layer FROM ia_decisions WHERE award_id BETWEEN 110 AND 113")}
    assert got == {(110, "lead_investigator"): "jev_link", (111, "lead_investigator"): "route_bind",
                   (112, "lead_investigator"): "route_conflict", (113, "lead_investigator"): "guard_shared_author",
                   (113, "investigators"): "guard_shared_author"}
    assert d.rows("SELECT award_id, author_id, method, layer FROM to_serve WHERE award_id >= 110 ORDER BY 1") == [
        (110, 1100, "jev_link", "jev_link"), (111, 1110, "jev_link+orcid", "route_bind")]


def add_slot(c, award, slot, position, given, family):
    c.execute("""INSERT INTO ia_people VALUES (?, 1, 'ukri', ?, NULL, ?, ?, ?, ?, NULL, ia_name_tokens(?), ia_name_tokens(?), ia_family_keys(?),
                 ia_name_tokens(?)[1], ?, [])""", [award, f"X{award}", slot, position, given, family, given, family, family, given,
                                                   f"{family.lower()}|{given.lower()}"])


def jev_db():
    """routes_db plus Jev's picks: Ada Judge (award 90, a new link); Bo Fold (award 9200, folded into 92); a slot that now prints another
    name the profile would still fit (94: Cy -> Cyril, profile "C. Judged"); a profile no longer live (95); a profile whose family name
    still matches but whose first name no longer agrees (97: Ed vs Fay Name)."""
    d = routes_db()
    c = d.con
    for a, given, family in [(90, "Ada", "Judge"), (92, "Bo", "Fold"), (94, "Cyril", "Judged"), (95, "Di", "Gone"), (97, "Ed", "Name")]:
        add_person(c, a, 1, "ukri", f"X{a}", given, family, None, [])
    c.execute("INSERT INTO openalex.awards.award_id_redirects VALUES (9200, 92)")
    c.execute("INSERT INTO ia_profile_names VALUES (91, 'judge', 'ada'), (93, 'fold', 'bo'), (941, 'judged', 'c'), (98, 'name', 'fay')")
    lead = "lead_investigator"
    for row in [(90, lead, 0, "Ada", "Judge", 91, 0.93), (9200, lead, 0, "Bo", "Fold", 93, 0.71), (94, lead, 0, "Cy", "Judged", 941, 0.88),
                (95, lead, 0, "Di", "Gone", 96, 0.97), (97, lead, 0, "Ed", "Name", 98, 0.66)]:
        c.execute("INSERT INTO openalex.awards.investigator_jev_links VALUES (?, ?, ?, ?, ?, ?, ?)", list(row))
    return d


def test_jev_claims_only_a_slot_that_still_prints_the_judged_name_on_a_live_agreeing_profile():
    d = jev_db()
    d.run("routes.sql")
    assert [x for x in claims(d) if x[1] == "jev_link"] == [(90, "jev_link", 91), (92, "jev_link", 93)]
    assert json.loads(d.rows("SELECT evidence FROM ia_routes WHERE award_id = 90 AND method = 'jev_link'")[0][0]) == {"p": 0.93}


# ---------- routes.sql ----------
PERSON = 'STRUCT(given VARCHAR, family VARCHAR, "ORCID" VARCHAR)[]'


AUTHORSHIPS = "STRUCT(author STRUCT(id VARCHAR), raw_author_name VARCHAR, institutions STRUCT(id VARCHAR, lineage VARCHAR[])[])[]"


def routes_db():
    """Empty copies of every input routes.sql reads, plus three people: Mark Williams (the grant prints the ORCID of the profile
    "J. Mark G. Williams"), Ken Sato (the ORCID of "Kenji Sato") and an NIH lead whose grant funded one paper."""
    d = Duck()
    c = d.con
    c.execute("INSERT INTO openalex.authors.authors_api VALUES ('https://openalex.org/A5', 'J. Mark G. Williams', [], 9, NULL, NULL), "
              "('https://openalex.org/A6', 'Kenji Sato', [], 9, NULL, NULL), ('https://openalex.org/A7', 'K. N. Subramanian', [], 9, NULL, NULL), "
              "('https://openalex.org/A8', '王伟', [], 9, NULL, NULL)")
    d.run("names.sql")
    c.execute("INSERT INTO ia_profile_names VALUES (7, 'subramanian', 'k'), (8, 'wang', 'wei')")   # 8: 王伟, romanised by the parser
    c.execute("""CREATE TABLE ia_people (award_id BIGINT, funder_id BIGINT, provenance VARCHAR, funder_award_id VARCHAR, award_doi VARCHAR,
                 slot VARCHAR, position INT, given_name VARCHAR, family_name VARCHAR, orcid VARCHAR, gt VARCHAR[], ft VARCHAR[],
                 fam_keys VARCHAR[], given_key VARCHAR, person_key VARCHAR, insts BIGINT[])""")
    for a, prov, num, given, family, orcid in [(1, "ukri", "X1", "Mark", "Williams", "0000-0002-9884-2614"),
                                               (2, "kaken", "X2", "Ken", "Sato", "0000-0001-1111-1111"),
                                               (3, "nih_exporter", "5r01gm035872-03", "Kiranur", "Subramanian", None),
                                               (4, "nsfc", "X4", "Wei", "Wang", "0000-0008-8888-8888")]:
        add_person(c, a, 1, prov, num, given, family, orcid, [])
    c.execute("CREATE TABLE ia_paper_authors (award_id BIGINT, work_id BIGINT, author_id BIGINT)")
    c.execute("CREATE TABLE ia_profile_orcids (author_id BIGINT, orcid VARCHAR, n_profiles BIGINT)")
    c.execute("INSERT INTO ia_profile_orcids VALUES (5, '0000-0002-9884-2614', 1), (6, '0000-0001-1111-1111', 1), (8, '0000-0008-8888-8888', 1)")
    c.execute("INSERT INTO ia_paper_authors VALUES (3, 900, 7)")
    c.execute("CREATE TABLE openalex.works.work_authors (work_id BIGINT, author_id BIGINT)")
    c.execute("INSERT INTO openalex.works.work_authors VALUES (900, 7)")
    c.execute("CREATE TABLE openalex.awards.nih_grants_raw (core_project_num VARCHAR, pi_ids VARCHAR, pi_names VARCHAR)")
    c.execute("CREATE TABLE openalex.awards.nsf_grants_raw (awd_id VARCHAR, pi_all VARCHAR)")
    c.execute("CREATE TABLE openalex.awards.kaken_projects_raw (project_id VARCHAR, pi_nrid VARCHAR, pi_given_name VARCHAR, pi_family_name VARCHAR)")
    c.execute(f"CREATE TABLE openalex.works.work_authorships (work_id BIGINT, authorships {AUTHORSHIPS})")
    c.execute("CREATE TABLE openalex.works.openalex_works (id BIGINT, publication_year INT)")
    c.execute("CREATE TABLE openalex.institutions.institution_ancestors (institution_id BIGINT, lineage_ids BIGINT[])")
    c.execute("CREATE TABLE nsfc_pinyin (award_id BIGINT, slot VARCHAR, position INT, f VARCHAR, g VARCHAR)")   # a temp view in the job
    c.execute("ALTER TABLE openalex.funders.funders ADD COLUMN doi VARCHAR")
    c.execute("ALTER TABLE openalex.funders.funders ADD COLUMN merge_into_id BIGINT")
    c.execute(f"""CREATE TABLE openalex.awards.crossref_grants_deduplicated ("DOI" VARCHAR, award VARCHAR, publisher VARCHAR, project STRUCT(
                 funding STRUCT(funder STRUCT(id STRUCT(id VARCHAR, "id-type" VARCHAR)[]))[], "lead-investigator" {PERSON},
                 "co-lead-investigator" {PERSON}, investigator {PERSON})[])""")
    return d


def add_person(c, award, funder, prov, num, given, family, orcid, insts):
    c.execute("""INSERT INTO ia_people VALUES (?, ?, ?, ?, NULL, 'lead_investigator', 0, ?, ?, ?, ia_name_tokens(?), ia_name_tokens(?),
                 ia_family_keys(?), ia_name_tokens(?)[1], ?, ?)""", [award, funder, prov, num, given, family, orcid, given, family, family,
                                                                     given, f"{family.lower()}|{given.lower()}", insts])


def claims(d):
    return sorted((a, m, author) for a, m, author in d.rows("SELECT award_id, method, author_id FROM ia_routes"))


def test_the_orcid_route_takes_any_given_token_and_still_refuses_another_full_name():
    d = routes_db()
    d.run("routes.sql")
    got = claims(d)
    assert (1, "orcid", 5) in got                     # Mark Williams = J. Mark G. Williams (dev v0's rule, lost by first-given-only)
    assert not [c for c in got if c[0] == 2]          # Ken Sato is still not Kenji Sato, ORCID or not
    assert (4, "orcid", 8) in got                     # the name rule's romanised readings still count (王伟 = Wang Wei)


def test_the_core_papers_route_reads_the_papers_the_core_funded():
    d = routes_db()
    d.run("routes.sql")
    assert (3, "nih_core_papers", 7) in claims(d)


def test_a_contradicted_anchor_does_not_spread_its_person_id():
    """John Smith, NIH PI 777, on awards 20, 24 (another year of 20's core project) and 21. Award 20's own paper names a James Smith
    (linked_paper -> 90) while the core's other paper names John (nih_core_papers -> 91): a conflict, so 20 anchors nothing. Award 24's
    paper names John alone, so PI 777 -> 91, and award 21 gets 91, never 90. Jane Hart (PI 888, awards 22 and 23) has only a
    core-paper claim (her core's paper is on award 25): no anchor, so no person-id claim."""
    d = routes_db()
    c = d.con
    c.execute("INSERT INTO ia_profile_names VALUES (90, 'smith', 'james'), (91, 'smith', 'john'), (92, 'hart', 'jane')")
    for a, num, given, family in [(20, "5u2cgm000020-01", "John", "Smith"), (24, "5u2cgm000020-02", "John", "Smith"),
                                  (21, "5u2cgm000021-01", "John", "Smith"), (22, "5r01gm000022-01", "Jane", "Hart"),
                                  (23, "5r01gm000023-01", "Jane", "Hart"), (25, "5r01gm000022-02", "Kim", "Lee")]:
        add_person(c, a, 1, "nih_exporter", num, given, family, None, [])
    c.execute("INSERT INTO openalex.awards.nih_grants_raw VALUES ('U2CGM000020', '777', 'SMITH, JOHN'), ('U2CGM000021', '777', 'SMITH, JOHN'), "
              "('R01GM000022', '888', 'HART, JANE'), ('R01GM000023', '888', 'HART, JANE')")
    c.execute("INSERT INTO ia_paper_authors VALUES (20, 2000, 90), (24, 2400, 91), (25, 2500, 92)")
    c.execute("INSERT INTO openalex.works.work_authors VALUES (2000, 90), (2400, 91), (2500, 92)")
    d.run("routes.sql")
    got = claims(d)
    assert (20, "linked_paper", 90) in got and (20, "nih_core_papers", 91) in got   # a U2C core number is read
    assert (21, "funder_person_id", 91) in got and (21, "funder_person_id", 90) not in got
    assert [x for x in got if x[0] in (22, 23)] == [(22, "nih_core_papers", 92)]



def test_an_anchor_the_orcid_guards_refuse_does_not_spread():
    """decide.sql's ORCID guards hold an author on its own slot, so the anchor must not spread it either. NIH PI 555 Ana Costa: award 30
    prints ORCID ...3333, whose one profile (95) mostly carries ...9999 on its works, and nothing else backs it: awards 30 and 31 get no
    person-id claim. PI 556 Rui Lima: award 32's paper names profile 96, but 32 prints the ORCID of profile 97 (Paulo Reis): award 33
    gets nothing. PI 557 Eva Mota: award 34's ORCID is the majority one of its profile 98, so 98 spreads to award 35. PI 558 Ivo Neto:
    award 38's ORCID is a minority one of profile 99; 38's paper names 99 too, but decide.sql holds a minority ORCID whatever agrees, so
    award 39 gets nothing. KAKEN awards 36 and 37 (one researcher number) print Ana Costa, 36 with ORCID ...3333: no NRID claim either.
    The anchor sees every claim decide.sql sees but the two built from anchors: PI 559 Leo Dias's minority ORCID on award 43 is backed by
    Jev's pick of the same profile 81, still held, so award 44 gets nothing; PI 560 Rita Melo's ORCID on award 45 names 82 while Jev
    picks 83 (a conflict), so award 46 gets nothing."""
    d = routes_db()
    c = d.con
    c.execute("INSERT INTO ia_profile_names VALUES (95, 'costa', 'ana'), (96, 'lima', 'rui'), (97, 'reis', 'paulo'), (98, 'mota', 'eva'), "
              "(99, 'neto', 'ivo'), (81, 'dias', 'leo'), (82, 'melo', 'rita'), (83, 'melo', 'rita')")
    c.execute("INSERT INTO ia_profile_orcids VALUES (95, '0000-0003-3333-3333', 1), (97, '0000-0003-4444-4444', 1), "
              "(98, '0000-0003-5555-5555', 1), (99, '0000-0003-6666-6666', 1), (81, '0000-0003-7777-7777', 1), (82, '0000-0003-8888-8888', 1)")
    c.execute("INSERT INTO openalex.authors.orcid_majority_lookup VALUES (95, 'https://orcid.org/0000-0009-9999-9999', 5, 1, 2), "
              "(98, 'https://orcid.org/0000-0003-5555-5555', 5, 0, 1), (99, 'https://orcid.org/0000-0003-7777-7777', 5, 1, 2), "
              "(81, 'https://orcid.org/0000-0009-0000-0081', 5, 1, 2), (82, 'https://orcid.org/0000-0003-8888-8888', 5, 0, 1)")
    for a, pi, given, family, orcid in [(30, 555, "Ana", "Costa", "0000-0003-3333-3333"), (31, 555, "Ana", "Costa", None),
                                        (32, 556, "Rui", "Lima", "0000-0003-4444-4444"), (33, 556, "Rui", "Lima", None),
                                        (34, 557, "Eva", "Mota", "0000-0003-5555-5555"), (35, 557, "Eva", "Mota", None),
                                        (38, 558, "Ivo", "Neto", "0000-0003-6666-6666"), (39, 558, "Ivo", "Neto", None),
                                        (43, 559, "Leo", "Dias", "0000-0003-7777-7777"), (44, 559, "Leo", "Dias", None),
                                        (45, 560, "Rita", "Melo", "0000-0003-8888-8888"), (46, 560, "Rita", "Melo", None)]:
        add_person(c, a, 1, "nih_exporter", f"5r01gm0000{a}-01", given, family, orcid, [])
        c.execute("INSERT INTO openalex.awards.nih_grants_raw VALUES (?, ?, ?)", [f"R01GM0000{a}", str(pi), f"{family.upper()}, {given.upper()}"])
    for a, orcid in [(36, "0000-0003-3333-3333"), (37, None)]:
        add_person(c, a, 4320334764, "kaken", f"K{a}", "Ana", "Costa", orcid, [])
        c.execute("INSERT INTO openalex.awards.kaken_projects_raw VALUES (?, '1000055555555', 'Ana', 'Costa')", [f"K{a}"])
    c.execute("INSERT INTO openalex.awards.investigator_jev_links VALUES (43, 'lead_investigator', 0, 'Leo', 'Dias', 81, 0.9), "
              "(45, 'lead_investigator', 0, 'Rita', 'Melo', 83, 0.9)")
    c.execute("INSERT INTO ia_paper_authors VALUES (32, 3200, 96), (38, 3800, 99)")
    c.execute("INSERT INTO openalex.works.work_authors VALUES (3200, 96), (3800, 99)")
    d.run("routes.sql")
    got = [x for x in claims(d) if x[0] >= 30]
    assert (30, "orcid", 95) in got and (32, "linked_paper", 96) in got and (36, "orcid", 95) in got
    assert (43, "jev_link", 81) in got and (45, "orcid", 82) in got and (45, "jev_link", 83) in got
    assert [x for x in got if x[1] in ("funder_person_id", "kaken_nrid")] == [(34, "funder_person_id", 98), (35, "funder_person_id", 98)]


def test_crossref_claims_a_slot_only_when_one_person_on_the_award_fits():
    """Crossref deposits Ann A. Lee's ORCID (profile 41) on two grants. Award 40 lists Ann A. Lee and Ann B. Lee (one person_key, two
    people): no claim. Award 42 prints Ann A. Lee twice (lead and investigators[]): one person, so both slots."""
    d = routes_db()
    c = d.con
    c.execute("INSERT INTO ia_profile_names VALUES (41, 'lee', 'ann')")
    c.execute("INSERT INTO ia_profile_orcids VALUES (41, '0000-0004-0000-0001', 1)")
    for a, slot, pos, given in [(40, "lead_investigator", 0, "Ann A."), (40, "investigators", 0, "Ann B."),
                                (42, "lead_investigator", 0, "Ann A."), (42, "investigators", 0, "Ann A.")]:
        c.execute("""INSERT INTO ia_people VALUES (?, 1, 'ukri', ?, ?, ?, ?, ?, 'Lee', NULL, ia_name_tokens(?), ['lee'], ['lee'],
                     ia_name_tokens(?)[1], 'lee|ann', [])""", [a, f"X{a}", f"10.1234/g{a}", slot, pos, given, given, given])
        if slot == "lead_investigator":
            c.execute("""INSERT INTO openalex.awards.crossref_grants_deduplicated VALUES (?, ?, 'p', [{'funding': [],
                         'lead-investigator': [{'given': 'Ann A.', 'family': 'Lee', 'ORCID': 'https://orcid.org/0000-0004-0000-0001'}],
                         'co-lead-investigator': [], 'investigator': []}])""", [f"10.1234/g{a}", f"X{a}"])
    d.run("routes.sql")
    got = d.rows("SELECT award_id, slot, author_id FROM ia_routes WHERE method = 'crossref_orcid' ORDER BY 1, 2")
    assert got == [(42, "investigators", 41), (42, "lead_investigator", 41)]

# ---------- kaken_nrid: KAKEN's researcher number ----------


def kaken_db(numbers=True):
    """routes_db plus KAKEN leads: Taro Yamada (NRID ...111) is bound by his grant's papers on award 60 and printed in kanji on
    award 61 (no name key); award 62's number no grant binds; number ...333 is bound to two authors (Ken Ito 41 on award 63, Kenji Ito
    42 on award 64), so award 65 (伊藤 健, the same number) gets nothing. numbers=False: the same people, KAKEN's file empty."""
    d = routes_db()
    c = d.con
    c.execute("INSERT INTO ia_profile_names VALUES (40, 'yamada', 'taro'), (41, 'ito', 'ken'), (42, 'ito', 'kenji')")
    for a, given, family in [(60, "Taro", "Yamada"), (61, "太郎", "山田"), (62, "Hanako", "Suzuki"), (63, "Ken", "Ito"),
                             (64, "Kenji", "Ito"), (65, "健", "伊藤")]:
        add_person(c, a, 4320334764, "kaken", f"K{a}", given, family, None, [])
    c.execute("INSERT INTO ia_paper_authors VALUES (60, 6000, 40), (63, 6300, 41), (64, 6400, 42)")
    if numbers:
        c.execute("INSERT INTO openalex.awards.kaken_projects_raw VALUES ('K60', '1000011111111', 'Taro', 'Yamada'), "
                  "('K61', '1000011111111', '太郎', '山田'), ('K62', '1000022222222', 'Hanako', 'Suzuki'), "
                  "('K63', '1000033333333', 'Ken', 'Ito'), ('K64', '1000033333333', 'Kenji', 'Ito'), ('K65', '1000033333333', '健', '伊藤')")
    return d


def test_kaken_nrid_carries_one_bound_author_by_number_not_name():
    d = kaken_db()
    d.run("routes.sql")
    got = [x for x in claims(d) if x[1] == "kaken_nrid"]
    assert got == [(60, "kaken_nrid", 40), (61, "kaken_nrid", 40)]   # the kanji lead too; the anchor claims itself, as NIH/NSF ids do
    ev = json.loads(d.rows("SELECT evidence FROM ia_routes WHERE award_id = 61 AND method = 'kaken_nrid'")[0][0])
    assert ev == {"nrid": "1000011111111", "ties": 1}


def test_a_kaken_lead_needs_the_given_name_or_its_initial_on_the_profile_and_a_dropped_anchor_spreads_nothing():
    """KAKEN leads matched on family key + initial: Abe Koji (award 66) against the profile "Keishi Abe" is no claim, and its number
    (also on award 67, printed in kanji, and award 73, whose own paper names a "Koji Abe") then spreads nothing. Kohji Sato = "Koji Sato"
    (68), Shinpei Ito = "Shimpei Ito" (69), "K. Abe" (70), a kanji-only profile name (71) and Shinichiro Ito = "Shin Ito" (74, a
    shared prefix) pass; a UKRI lead (72) is not checked."""
    d = kaken_db()
    c = d.con
    c.execute("INSERT INTO openalex.authors.authors_api VALUES ('https://openalex.org/A43', 'Keishi Abe', [], 9, NULL, NULL), "
              "('https://openalex.org/A44', 'Koji Sato', [], 9, NULL, NULL), ('https://openalex.org/A45', 'Shimpei Ito', [], 9, NULL, NULL), "
              "('https://openalex.org/A46', 'K. Abe', [], 9, NULL, NULL), ('https://openalex.org/A47', '阿部康二', [], 9, NULL, NULL), "
              "('https://openalex.org/A48', 'Koji Abe', [], 9, NULL, NULL), ('https://openalex.org/A49', 'Shin Ito', [], 9, NULL, NULL)")
    c.execute("INSERT INTO ia_profile_names VALUES (43, 'abe', 'keishi'), (44, 'sato', 'koji'), (45, 'ito', 'shimpei'), (46, 'abe', 'k'), "
              "(47, 'abe', 'koji'), (48, 'abe', 'koji'), (49, 'ito', 'shin')")
    for a, prov, given, family, author in [(66, "kaken", "Koji", "Abe", 43), (67, "kaken", "康二", "阿部", None),
                                           (73, "kaken", "Koji", "Abe", 48), (68, "kaken", "Kohji", "Sato", 44),
                                           (69, "kaken", "Shinpei", "Ito", 45), (70, "kaken", "Koji", "Abe", 46),
                                           (71, "kaken", "Koji", "Abe", 47), (72, "ukri", "Koji", "Abe", 43),
                                           (74, "kaken", "Shinichiro", "Ito", 49)]:
        add_person(c, a, 4320334764 if prov == "kaken" else 1, prov, f"K{a}", given, family, None, [])
        if author:
            c.execute("INSERT INTO ia_paper_authors VALUES (?, ?, ?)", [a, a * 100, author])
    c.execute("INSERT INTO openalex.awards.kaken_projects_raw VALUES ('K66', '1000044444444', 'Koji', 'Abe'), "
              "('K67', '1000044444444', '康二', '阿部'), ('K73', '1000044444444', 'Koji', 'Abe')")
    d.run("routes.sql")
    got = [x for x in claims(d) if x[0] >= 66]
    assert [x for x in got if x[1] == "linked_paper"] == [(68, "linked_paper", 44), (69, "linked_paper", 45), (70, "linked_paper", 46),
                                                          (71, "linked_paper", 47), (72, "linked_paper", 43), (73, "linked_paper", 48),
                                                          (74, "linked_paper", 49)]
    assert [x for x in got if x[1] == "kaken_nrid"] == []    # 73 alone would bind the number to 48; the dropped anchor 66 stops it


# decide.sql with kaken_nrid claims: one award per scenario, the claims, the scored lists, and the layer each must get
SLOTS3 = [(a, "lead_investigator", 0, f"k{a}|x", None) for a in range(60, 66)]
ROUTES3 = [(60, "lead_investigator", 0, 600, "kaken_nrid"), (61, "lead_investigator", 0, 610, "kaken_nrid"),
           (61, "lead_investigator", 0, 610, "linked_paper"), (62, "lead_investigator", 0, 620, "kaken_nrid"),
           (62, "lead_investigator", 0, 621, "linked_paper"), (63, "lead_investigator", 0, 630, "kaken_nrid"),
           (64, "lead_investigator", 0, 640, "kaken_nrid"), (65, "lead_investigator", 0, 650, "kaken_nrid"),
           (65, "lead_investigator", 0, 650, "jev_link")]
SCORES3 = {"63|k63|x": [(631, 0.99)], "64|k64|x": [(640, 0.99), (641, 0.6)]}
WANT3 = {(60, "lead_investigator", 0): "kaken_nrid",      # the number alone
         (61, "lead_investigator", 0): "route_bind",      # with an identifier route on the same author
         (62, "lead_investigator", 0): "route_conflict",  # against an identifier route
         (63, "lead_investigator", 0): "route_conflict",  # the student's servable pick is another author
         (64, "lead_investigator", 0): "kaken_nrid",      # a common name (two plausible): an identifier is not held for it
         (65, "lead_investigator", 0): "kaken_nrid"}      # with Jev's pick on the same author


def test_decide_gives_kaken_nrid_its_own_layer_and_conflicts_like_any_route():
    d = decide_db(kaken=True)
    got = {(a, s, p): layer for a, s, p, layer in d.rows("SELECT award_id, slot, position, layer FROM ia_decisions")}
    assert got == {**WANT, **WANT3}
    served = d.rows("SELECT award_id, author_id, method, layer FROM to_serve WHERE award_id >= 60 ORDER BY 1")
    assert served == [(60, 600, "kaken_nrid", "kaken_nrid"), (61, 610, "kaken_nrid+linked_paper", "route_bind"),
                      (64, 640, "kaken_nrid", "kaken_nrid"), (65, 650, "jev_link+kaken_nrid", "kaken_nrid")]


# ---------- nsfc_name: NSFC's Chinese-only PIs, by pinyin on the grant's own papers ----------


def nsfc_db():
    """routes_db plus NSFC leads (2015-2018). 张伟 (80, institution 100): paper 8000 prints "Wei Zhang" (author 50) at 100, and 2 of 50's
    3 other works in 2012-2023 are at 100. 李明 (81, 101): "Ming Li" (52) at 101 on the paper, but 1 of 52's 4 other works (a merged
    profile). 王芳 (82, 102): "Fang Wang" (53) at 103, a child of 102 (by institution_ancestors), also on her other work. 王磊 (83): two authors print Lei Wang on
    the paper. 张伟 (84): the paper prints a Hua Zhang only."""
    d = routes_db()
    c = d.con
    def au(author, name, *insts):
        return {"author": {"id": f"https://openalex.org/A{author}"}, "raw_author_name": name,
                "institutions": [{"id": f"https://openalex.org/I{i}", "lineage": [f"https://openalex.org/I{x}" for x in lin]} for i, lin in insts]}
    for a, f, g, inst in [(80, "zhang", "wei", 100), (81, "li", "ming", 101), (82, "wang", "fang", 102), (83, "wang", "lei", 104),
                          (84, "zhang", "wei", 105)]:
        add_person(c, a, 4320321001, "nsfc", f"N{a}", "x", "x", None, [inst])
        c.execute("INSERT INTO nsfc_pinyin VALUES (?, 'lead_investigator', 0, ?, ?)", [a, f, g])
        c.execute("INSERT INTO openalex.awards.awards_api VALUES (?, 4320321001, 2015, 2018)", [a])
    works = {8000: (2016, [au(50, "Wei Zhang", (100, [100])), au(59, "Na Li", (100, [100]))]),
             8001: (2014, [au(50, "W. Zhang", (100, [100]))]), 8002: (2017, [au(50, "Zhang Wei", (100, [100]))]),
             8003: (2019, [au(50, "Wei Zhang", (999, [999]))]), 8004: (2030, [au(50, "Wei Zhang", (999, [999]))]),
             8100: (2016, [au(52, "Ming Li", (101, [101]))]), 8101: (2015, [au(52, "M. Li", (101, [101]))]),
             8102: (2016, [au(52, "Ming Li", (998, [998]))]), 8103: (2017, [au(52, "Ming Li", (998, [998]))]),
             8104: (2018, [au(52, "Ming Li", (998, [998]))]),
             8200: (2016, [au(53, "Fang Wang", (103, []))]), 8201: (2013, [au(53, "F. Wang", (103, [103, 102]))]),
             8300: (2016, [au(54, "Lei Wang", (104, [104])), au(55, "Wang Lei", (104, [104]))]),
             8400: (2016, [au(56, "Hua Zhang", (105, [105]))])}
    for w, (y, aus) in works.items():
        c.execute("INSERT INTO openalex.works.work_authorships VALUES (?, ?)", [w, aus])
        c.execute("INSERT INTO openalex.works.openalex_works VALUES (?, ?)", [w, y])
        for x in aus:
            author = int(x["author"]["id"].rsplit("A", 1)[1])
            c.execute("INSERT INTO openalex.works.work_authors VALUES (?, ?)", [w, author])
            if w // 100 in (80, 81, 82, 83, 84) and w % 100 == 0:
                c.execute("INSERT INTO ia_paper_authors VALUES (?, ?, ?)", [w // 100, w, author])
    c.execute("INSERT INTO openalex.institutions.institution_ancestors VALUES (103, [103, 102])")   # 8200's authorship lacks lineage
    return d


def test_nsfc_name_takes_the_one_full_pinyin_author_at_the_institution_unless_the_profile_is_mostly_elsewhere():
    d = nsfc_db()
    d.run("routes.sql")
    assert [x for x in claims(d) if x[1] == "nsfc_name"] == [(80, "nsfc_name", 50), (82, "nsfc_name", 53)]
    ev = json.loads(d.rows("SELECT evidence FROM ia_routes WHERE award_id = 80 AND method = 'nsfc_name'")[0][0])
    assert ev == {"pinyin": "zhang wei", "works": 1, "inst_works": 2, "window_works": 3}   # 8004 (2030) is outside the window


def test_pinyin_keys_read_heteronyms_and_drop_latin_names():
    pytest.importorskip("pypinyin")
    assert iaa.pinyin_keys("张", "伟") == [("zhang", "wei")]
    assert {("zeng", "xiaoming"), ("ceng", "xiaoming")} <= set(iaa.pinyin_keys("曾", "小明"))
    assert ("lu", "jianguo") in iaa.pinyin_keys("吕", "建国") and ("lyu", "jianguo") in iaa.pinyin_keys("吕", "建国")
    assert iaa.pinyin_keys("Smith", "伟") == [] and iaa.pinyin_keys("张", "") == []
