"""The author on each award investigator (lib/award_investigators.py, sql/api_payload.sql;
oxjob #1847, grants on author profiles).

The loader and wiring tests need nothing but pytest. The behaviour tests run sql/api_payload.sql itself on small DuckDB
fixtures (the test_award_country.py fixture, with named people).
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "tests"))

import award_country
import award_investigators
from test_award_country import PAYLOAD, FakeC, Fixture


# ---------- wiring ----------
def test_without_the_input_no_author_is_attached_and_nothing_is_checked():
    c = FakeC()
    assert award_investigators.links_relation(c) == award_investigators.EMPTY_LINKS and not c.checks


def test_the_links_need_the_profiles_input():
    with pytest.raises(RuntimeError, match="INVESTIGATOR_PROFILES_MISSING"):
        award_investigators.links_relation(FakeC(extra={"investigator_authors"}))


def test_every_served_row_attaches_by_slot_position_and_name():   # which layers are served is the job's call
    c = FakeC(extra={"investigator_authors", "investigator_profiles"})
    sql = award_investigators.links_relation(c)
    assert "l.position" in sql and "FROM investigator_authors_v l JOIN investigator_profiles_v p" in sql and "layer" not in sql
    assert "INVESTIGATOR_AUTHORS_SLOT" in c.checks


# ---------- behaviour (DuckDB) ----------
def q(v):
    return "NULL" if v is None else "'" + v.replace("'", "''") + "'"


def named(given, family, affiliation=None, orcid=None, country=None):
    return (f"{{'given_name': {q(given)}, 'family_name': {q(family)}, 'orcid': {q(orcid)}, 'role_start': NULL, "
            f"'affiliation': {{'name': {q(affiliation)}, 'country': {q(country)}, 'ids': NULL}}}}")


class People(Fixture):
    def grant(self, lead=None, investigators=(), funder_id=1, end_date="2026-12-31"):
        self.next_id += 1
        inv = "[" + ",".join(named(*p) for p in investigators) + "]" if investigators else "NULL"
        self.con.execute(f"""INSERT INTO awards (id, funder_id, end_date, provenance, funder_award_id, lead_investigator, investigators)
            VALUES ({self.next_id}, {funder_id}, DATE '{end_date}', 'test', 'x{self.next_id}', {named(*lead) if lead else 'NULL'}, {inv})""")
        return self.next_id

    def payload(self, links=()):
        """links: (award_id, slot, position, given, family, author number[, the grant's ORCID as the job cleans it]), positions as the
        job writes them (Spark, from 0). DuckDB's lambda index counts from 1, so an investigators[] position is shifted by one here to
        meet the payload's transform."""
        rel = award_investigators.EMPTY_LINKS
        if links:
            rows = ",".join(f"({a},{q(s)},{p + (s == 'investigators')},{q(g)},{q(fam)},'https://openalex.org/A{n}','Person {n}',"
                            f"'https://orcid.org/0000-0000-0000-000{n}',"
                            f"{q(o[0] if o else None)})" for a, s, p, g, fam, n, *o in links)
            rel = (f"SELECT * FROM VALUES {rows} AS t(award_id,slot,position,given_name,family_name,author_id,display_name,"
                   "author_orcid,grant_orcid)")
        sql = (PAYLOAD.replace("{AWARDS}", "awards").replace("{ANSWERS}", "answers")
               .replace("{COUNTRY_LOOKUP}", award_country.lookup_relation_sql(ROOT))
               .replace("{INVESTIGATOR_LINKS}", rel))
        return self.sqlglot.transpile(sql, read="spark", write="duckdb")[0]

    def people(self, links=()):
        rows = self.con.execute(f"SELECT id, lead_investigator, investigators FROM ({self.payload(links)})").fetchall()
        return {i: dict(lead=lead, investigators=inv) for i, lead, inv in rows}


def author(p):
    return p and p["author"] and p["author"]["id"]


def test_each_person_gets_the_author_linked_to_their_slot_position_and_name():
    f = People()
    g = f.grant(lead=("Ana", "Silva"), investigators=[("Bruno", "Costa"), ("Ana", "Silva"), ("Carla", "Dias")])
    got = f.people(links=[(g, "lead_investigator", 0, "Ana", "Silva", 1), (g, "investigators", 1, "ana", " SILVA ", 1),
                          (g, "investigators", 0, "Bruno", "Costa", 2)])[g]
    assert author(got["lead"]) == "https://openalex.org/A1" and got["lead"]["author"]["display_name"] == "Person 1"
    # {id, display_name, orcid} as works' authorships[].author (Rohan + Casey 11-09); the person's own printed orcid is untouched
    assert got["lead"]["author"] == {"id": "https://openalex.org/A1", "display_name": "Person 1", "orcid": "https://orcid.org/0000-0000-0000-0001"}
    assert [author(p) for p in got["investigators"]] == ["https://openalex.org/A2", "https://openalex.org/A1", None]


def test_a_reordered_list_an_edited_name_or_another_slot_attaches_nothing():
    f = People()
    moved = f.grant(lead=("Ana", "Silva"), investigators=[("Carla", "Dias"), ("Bruno", "Costa")])   # Bruno was judged at 0
    edited = f.grant(lead=("Ana", "Silva-Costa"), investigators=[("Bruno", "Costa")])
    got = f.people(links=[(moved, "investigators", 0, "Bruno", "Costa", 2), (edited, "lead_investigator", 0, "Ana", "Silva", 1),
                          (edited, "lead_investigator", 0, "Bruno", "Costa", 2)])
    assert [author(p) for p in got[moved]["investigators"]] == [None, None]
    assert author(got[edited]["lead"]) is None and author(got[edited]["investigators"][0]) is None


def test_two_people_with_one_name_each_get_their_own_link():
    f = People()
    j1, j2 = ("John", "Smith", None, "https://orcid.org/0000-0001-0000-0001"), ("John", "Smith", None, "0000-0002-0000-000x")
    g = f.grant(lead=("Ana", "Silva"), investigators=[j1, j2])
    links = [(g, "investigators", 0, "John", "Smith", 1, "0000-0001-0000-0001"), (g, "investigators", 1, "John", "Smith", 2, "0000-0002-0000-000X")]
    got = f.people(links=links)[g]
    assert [author(p) for p in got["investigators"]] == ["https://openalex.org/A1", "https://openalex.org/A2"]
    got = f.people(links=links[:1])[g]
    assert [author(p) for p in got["investigators"]] == ["https://openalex.org/A1", None]
    swapped = f.grant(lead=("Ana", "Silva"), investigators=[j2, j1])      # the two John Smiths swapped before the links are refreshed
    got = f.people(links=[(swapped, *x[1:]) for x in links])[swapped]
    assert [author(p) for p in got["investigators"]] == [None, None]


def test_without_links_people_are_served_as_before_with_a_null_author():
    f = People()
    g = f.grant(lead=("Ana", "Silva", "Universidade de Lisboa"), investigators=[("Bruno", "Costa")])
    got = f.people()[g]
    assert got["lead"] == {"given_name": "Ana", "family_name": "Silva", "orcid": None, "role_start": None,
                           "affiliation": {"name": "Universidade de Lisboa", "country": None, "ids": None}, "author": None}
    assert f.people()[g]["investigators"][0]["author"] is None


def test_investigators_full_lists_everyone_lead_first():   # links-on: the filter investigators.author.id reads it
    f = People()
    g = f.grant(lead=("Ana", "Silva"), investigators=[("Bruno", "Costa"), ("Ana", "Silva"), ("Carla", "Dias")])
    full = dict(f.con.execute(f"SELECT id, investigators_full FROM ({f.payload()})").fetchall())[g]
    assert [p["family_name"] for p in full] == ["Silva", "Costa", "Silva", "Dias"]      # no co-lead (NULL) entry


def test_the_author_orcid_is_the_profiles_and_a_profile_missing_attaches_nothing():
    """links_relation on DuckDB: the author's orcid is the profile's (openalex_authors.orcid, as works read it), NULL when it has none;
    a served author with no profile row attaches nothing."""
    f = People()
    c = FakeC(extra={"investigator_authors", "investigator_profiles"})
    f.con.execute("""CREATE VIEW investigator_authors_v AS SELECT * FROM (VALUES (1, 'lead_investigator', 0, 'Ana', 'Silva', 11, 'X'),
        (2, 'lead_investigator', 0, 'Bo', 'Li', 12, NULL), (4, 'lead_investigator', 0, 'Di', 'Ho', 99, NULL))
        t(award_id, slot, position, given_name, family_name, author_id, orcid)""")
    f.con.execute("""CREATE VIEW investigator_profiles_v AS SELECT * FROM (VALUES (11, 'Ana Silva', 'https://orcid.org/A'),
        (12, 'Bo Li', NULL)) t(id, display_name, orcid)""")
    sql = f.sqlglot.transpile(award_investigators.links_relation(c), read="spark", write="duckdb")[0]
    got = {r[0]: r[1:] for r in f.con.execute(f"SELECT award_id, author_id, author_orcid, grant_orcid FROM ({sql})").fetchall()}
    assert got == {1: ("https://openalex.org/A11", "https://orcid.org/A", "X"), 2: ("https://openalex.org/A12", None, None)}
