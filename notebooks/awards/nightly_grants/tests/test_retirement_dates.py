"""The date guard on the shell rule's retirements (build_awards.date_guard), run offline in DuckDB on the build's own SQL.

A paper-only record (shell) whose number keys to a funder's record retires into it, permanently. When the number is a typo or a
recycled number on old papers (MOP192552 on papers from 2012 keys to CIHR's 2024 grant 192552_1), that fold is wrong. The guard
holds the retirement when more than half of the dated papers citing that number came out DATE_HOLD_MIN_YEARS or more before the
record's earliest start year. Decided per (target record, funder, normalized number). Held = not retired tonight, proposed again
next night; no fuse.

    uv run --with pytest --with sqlglot --with duckdb python -m pytest -q notebooks/awards/nightly_grants/tests/test_retirement_dates.py
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "tests"))

import build_awards
import work_awards
from stable_award_ids import generic_sql
from test_nih_fold import render

CIHR, NSERC, MERGED_INTO_CIHR = 4320334506, 4320334593, 999
RUN_TS = "2026-11-24 00:00:00"          # run year 2026: plausible years are 1900..2027
RECORD, SHELL = 4735743864, 1474434405  # CIHR 192552_1 (2024) and the paper-only "MOP192552"


def q(v):
    return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"


class DB:
    """The runtime surface date_guard uses (artifact, count, r, nih), on DuckDB; Spark SQL is transpiled by sqlglot."""
    def __init__(self):
        duckdb = pytest.importorskip("duckdb")
        self.sqlglot = pytest.importorskip("sqlglot")
        self.con = duckdb.connect()
        self.r, self.nih, self.counts = "", False, {}
        for ddl in ("funder_canon (funder_id BIGINT, canonical_funder_id BIGINT)",
                    "retirements_rule_proposed (old_id BIGINT, canonical_id BIGINT)",
                    "resolved_observations (observation_key VARCHAR, stable_id BIGINT, staging_id BIGINT, funder_id BIGINT, "
                    "award_key VARCHAR, nk VARCHAR, sk VARCHAR, is_direct BOOLEAN)",
                    "bindings (observation_key VARCHAR, stable_id BIGINT, is_direct BOOLEAN, payload STRUCT(start_year INTEGER))",
                    "work_observations_in_v (source VARCHAR, work_id BIGINT, source_funder_id BIGINT, award_key VARCHAR)",
                    "works_v (id BIGINT, publication_year INTEGER)"):
            self.con.execute("CREATE TABLE " + ddl)
        for f, canon in ((CIHR, CIHR), (NSERC, NSERC), (MERGED_INTO_CIHR, CIHR)):
            self.con.execute(f"INSERT INTO funder_canon VALUES ({f},{canon})")
        self.obs = 0

    def spark(self, sql):
        return self.sqlglot.transpile(sql.replace(":ts", q(RUN_TS)), read="spark", write="duckdb")[0]

    def artifact(self, name, query, params=None):
        self.con.execute(f"CREATE OR REPLACE TABLE {name} AS {self.spark(query)}"); return name

    def count(self, name, relation):
        n = self.con.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]; self.counts[name] = n; return n

    def rows(self, sql):
        return self.con.execute(sql).fetchall()

    # ---- fixture rows ----
    def observation(self, stable_id, key, direct, funder=CIHR, start_year=None):
        self.obs += 1
        self.con.execute(f"""INSERT INTO resolved_observations SELECT 'o{self.obs}',{stable_id},{self.obs},{funder},{q(key)},
            {self.spark(generic_sql(q(key)))},NULL,{str(direct).lower()}""")
        self.con.execute(f"INSERT INTO bindings VALUES ('o{self.obs}',{stable_id},{str(direct).lower()},{{'start_year': {q(start_year)}}})")

    def record(self, start_years=(2024,), number="192552_1", stable_id=RECORD, funder=CIHR):
        for y in start_years:
            self.observation(stable_id, number, True, funder, y)

    def shell(self, keys=("mop192552",), stable_id=SHELL, target=RECORD, funder=CIHR):
        for k in keys:
            self.observation(stable_id, k, False, funder)
        self.con.execute(f"INSERT INTO retirements_rule_proposed VALUES ({stable_id},{target})")

    def paper(self, work_id, year, number="MOP192552", funder=CIHR, source="crossref_work_funders", served=True):
        self.con.execute(f"INSERT INTO work_observations_in_v VALUES ({q(source)},{work_id},{funder},lower({q(number)}))")
        if served and not self.rows(f"SELECT 1 FROM works_v WHERE id={work_id}"):
            self.con.execute(f"INSERT INTO works_v VALUES ({work_id},{q(year)})")

    def papers(self, years, first=1, **kw):
        for i, y in enumerate(years):
            self.paper(first + i, y, **kw)

    def guard(self):
        """Runs the guard; returns (held shell ids, shells retiring tonight)."""
        out = build_awards.date_guard(self, "retirements_rule_proposed")
        assert out == "retirements_rule"
        held = {r[0] for r in self.rows("SELECT old_id FROM retirement_date_held")}
        retiring = {r[0] for r in self.rows("SELECT old_id FROM retirements_rule")}
        proposed = {r[0] for r in self.rows("SELECT old_id FROM retirements_rule_proposed")}
        assert held | retiring == proposed and not held & retiring
        return held, retiring


def held_row(db, shell=SHELL):
    (row,) = db.rows(f"SELECT reason,start_year,dated_papers,earlier_papers FROM retirement_date_held WHERE old_id={shell}")
    return row


# ---------- the rule ----------
def test_mop192552_on_2012_papers_is_held_from_cihrs_2024_grant():
    db = DB(); db.record((2024,)); db.shell(); db.papers((2012, 2013, 2014))
    assert db.guard() == ({SHELL}, set())
    assert held_row(db) == ("PREDATES_GRANT", 2024, 3, 3)
    assert db.counts == {"retirements_rule_proposed": 1, "retirement_held_predates_grant": 1}


def test_papers_after_the_start_retire_as_before():
    db = DB(); db.record((2014,)); db.shell(); db.papers((2015, 2016, 2019))
    assert db.guard() == (set(), {SHELL})
    assert db.counts["retirement_held_predates_grant"] == 0


@pytest.mark.parametrize("years,held", [((2012, 2013, 2025, 2026), False),        # exactly half early: retires
                                        ((2012, 2013, 2014, 2025, 2026), True)])  # more than half: held
def test_more_than_half_of_the_dated_papers(years, held):
    db = DB(); db.record((2024,)); db.shell(); db.papers(years)
    assert db.guard()[0] == ({SHELL} if held else set())


@pytest.mark.parametrize("year,held", [(2019, True), (2020, False)])   # start 2024: 5 years before counts, 4 does not
def test_five_years_before_the_start_is_the_line(year, held):
    assert build_awards.DATE_HOLD_MIN_YEARS == 5
    db = DB(); db.record((2024,)); db.shell(); db.papers((year,))
    assert db.guard()[0] == ({SHELL} if held else set())


def test_no_dated_paper_retires_as_before():
    db = DB(); db.record((2024,)); db.shell()
    db.paper(1, None)                       # served, no year
    db.paper(2, 2012, served=False)         # not a served work
    assert db.guard() == (set(), {SHELL})
    db2 = DB(); db2.record((2024,)); db2.shell()   # no paper at all
    assert db2.guard() == (set(), {SHELL})


def test_implausible_publication_years_are_undated():
    db = DB(); db.record((2024,)); db.shell(); db.papers((1066, 1899, 2025))   # 1900..run year + 1 only
    assert db.guard() == (set(), {SHELL})
    assert db.rows("SELECT count(*) FROM retirement_date_papers WHERE publication_year IS NOT NULL") == [(1,)]


def test_a_record_without_a_start_year_retires_as_before():
    db = DB(); db.record((None,)); db.shell(); db.papers((1990, 1991, 1992))
    assert db.guard() == (set(), {SHELL})


@pytest.mark.parametrize("starts,held", [((1850, 2020), True),    # 1850 is not a start year: 2020 dates it, 2014 is 6 years early
                                         ((2035,), False),        # only a future year: no start, retires
                                         ((2016, 2030), False),   # earliest plausible start 2016: 2014 is 2 years early
                                         ((2016, 2022), False)])  # the earliest, not the latest (2022 would hold)
def test_the_start_is_the_earliest_plausible_year_on_the_records_own_rows(starts, held):
    db = DB(); db.record(starts); db.shell(); db.papers((2014,))
    assert db.guard()[0] == ({SHELL} if held else set())


def test_only_the_records_direct_rows_date_it():
    db = DB(); db.record((2024,)); db.shell(); db.papers((2012, 2013))
    db.observation(RECORD, "mop 192552", False, start_year=2000)   # a paper-side row bound to the record: not its start
    assert db.guard()[0] == {SHELL}


def test_a_late_start_year_within_five_years_lets_a_right_fold_through():
    # NSERC serves the latest fiscal year of an application as its start (a grant paid 2012-2016 shows 2016): papers 2013-2015
    db = DB(); db.record((2016,), number="386404-2012", funder=NSERC)
    db.shell(("rgpin-386404-2012",), funder=NSERC); db.papers((2013, 2014, 2015), number="RGPIN-386404-2012", funder=NSERC)
    assert db.guard() == (set(), {SHELL})


# ---------- what one decision covers ----------
def test_spellings_of_one_number_share_one_decision():
    db = DB(); db.record((2024,))
    db.shell(("mop192552",), stable_id=11); db.shell(("mop-192552",), stable_id=12)
    db.paper(1, 2012, number="MOP 192552"); db.paper(2, 2013, number="MOP-192552"); db.paper(3, 2025, number="MOP192552")
    assert db.guard() == ({11, 12}, set())
    assert held_row(db, 11)[2:] == held_row(db, 12)[2:] == (3, 2)


def test_two_numbers_into_one_record_are_decided_apart():
    db = DB(); db.record((2024,))
    db.shell(("mop192552",), stable_id=11); db.shell(("pjt192552",), stable_id=12)
    db.papers((2012, 2013), number="MOP192552"); db.papers((2024, 2025), first=10, number="PJT192552")
    assert db.guard() == ({11}, {12})


def test_a_shell_in_two_numbers_is_held_if_either_is():
    db = DB(); db.record((2024,)); db.shell(("mop192552", "cihr192552"))
    db.papers((2012, 2013), number="MOP192552"); db.papers((2025, 2026), first=10, number="CIHR192552")
    assert db.guard() == ({SHELL}, set())
    assert db.rows("SELECT count(*) FROM retirement_date_held") == [(1,)]


def test_only_papers_of_the_same_funder_count():
    db = DB(); db.record((2024,)); db.shell(); db.paper(1, 2025)
    db.record((2026,), number="mop192552-1", stable_id=21, funder=NSERC)   # NSERC has a proposal of the same spelling tonight
    db.shell(stable_id=22, target=21, funder=NSERC); db.paper(30, 2026, funder=NSERC)
    db.papers((2010, 2011, 2012), first=10, funder=NSERC)                  # same spelling, another funder
    assert db.guard() == ({22}, {SHELL})
    db2 = DB(); db2.record((2024,)); db2.shell(); db2.paper(1, 2025)
    db2.papers((2010, 2011, 2012), first=10, funder=MERGED_INTO_CIHR)      # a funder merged into CIHR counts as CIHR
    assert db2.guard() == ({SHELL}, set())


def test_papers_count_by_the_numbers_link_key_and_once_each():
    db = DB(); db.record((2024,)); db.shell()
    db.paper(1, 2012, number="MOP192552"); db.paper(1, 2012, number="mop-192552", source="europepmc_work_funders")
    db.papers((2025, 2026), first=10, number="192552")   # the bare number links by the record's own key, not the shell's
    assert db.guard() == ({SHELL}, set())
    assert held_row(db)[2:] == (1, 1)


def test_the_reason_columns_and_one_row_per_held_shell():
    db = DB(); db.record((2024,)); db.shell(("mop192552", "mop 192552")); db.papers((2012,))
    db.guard()
    assert db.rows("SELECT old_id,reason,start_year,dated_papers,earlier_papers FROM retirement_date_held") == [(SHELL, "PREDATES_GRANT", 2024, 1, 1)]


# ---------- no fuse: a held shell is proposed again every night and every night runs ----------
def test_a_wave_night_with_150_held_runs_and_so_do_the_next_two():
    db = DB(); db.record((2024,))
    for night, n in ((1, 20000), (2, 150), (3, 150)):
        for t in ("retirements_rule_proposed", "resolved_observations", "work_observations_in_v", "works_v"):
            db.con.execute(f"DELETE FROM {t} WHERE {'NOT is_direct' if t == 'resolved_observations' else 'true'}")
        db.con.execute(f"""INSERT INTO resolved_observations SELECT 's'||i,i,i,{CIHR},'mop'||i,'mop'||i,NULL,false FROM range(1,{n}+1) t(i)""")
        db.con.execute(f"INSERT INTO retirements_rule_proposed SELECT i,{RECORD} FROM range(1,{n}+1) t(i)")
        db.con.execute(f"INSERT INTO work_observations_in_v SELECT 'crossref_work_funders',i,{CIHR},'mop'||i FROM range(1,{n}+1) t(i)")
        db.con.execute("INSERT INTO works_v SELECT i,CASE WHEN i<=150 THEN 2012 ELSE 2025 END FROM range(1,{}+1) t(i)".format(n))
        held, retiring = db.guard()
        assert len(held) == 150 and len(retiring) == n - 150, night


# ---------- where it sits in the night ----------
@pytest.mark.parametrize("extra", [(), ("migration_manifest",)])
def test_the_guard_sits_between_the_rule_and_every_reader(extra):
    c = render(ROOT / "lib" / "build_awards.py", extra=extra)
    names = [n for n, _ in c.statements]; s = dict(c.statements)
    order = ["shell_targets", "retirements_rule_proposed", "retirement_date_units", "retirement_date_papers", "retirement_date_held",
             "retirements_rule", "retirement_sources", "retirements"]
    assert [names.index(n) for n in order] == sorted(names.index(n) for n in order)
    assert "FROM dev.lab.ngr_s_shell_targets s" in s["retirements_rule_proposed"]
    assert s["retirements_rule"] == ("SELECT p.old_id,p.canonical_id FROM dev.lab.ngr_s_retirements_rule_proposed p "
                                     "LEFT ANTI JOIN dev.lab.ngr_s_retirement_date_held h ON h.old_id=p.old_id")
    assert "'RULE' source FROM dev.lab.ngr_s_retirements_rule " in s["retirement_sources"] + " "
    # the scan of paper legs covers only the funders with a proposal tonight
    assert "f.canonical_funder_id IN (SELECT funder_id FROM dev.lab.ngr_s_retirement_date_units)" in s["retirement_date_papers"]
    if extra:
        # a manifest pair whose loser the rule proposes elsewhere still stops the run, held or not, as before
        assert "JOIN dev.lab.ngr_s_retirements_rule_proposed t ON t.old_id=m.loser_id" in s["MIGRATION_CONFLICT_WITH_RULE"]
        assert "JOIN dev.lab.ngr_s_retirements_rule t ON t.old_id=m.target_id" in s["MIGRATION_SHELL_TARGET_RETIRED"]
        # a public stub-twin pair whose target the rule proposes is held, as before the guard (held target or not)
        assert "JOIN dev.lab.ngr_s_retirements_rule_proposed t ON t.old_id=m.target_id" in s["public_shell_held"]


def test_with_nih_pairs_the_guard_reads_each_observations_own_funder():
    c = render(ROOT / "lib" / "build_awards.py", extra=("migration_manifest",), nih=True)
    assert "coalesce(nof.funder_id,o.funder_id) funder_id FROM dev.lab.ngr_s_resolved_observations o" in dict(c.statements)["retirement_date_units"]


def test_an_eu_pair_whose_loser_the_rule_proposes_stays_held():
    # the EU fold holds a pair whose loser the rule retires elsewhere, so the conflict gate never sees it; it must read the
    # proposals too, or a date-held loser would leave its EU pair to that gate and stop the night
    from test_eu_fold import eu_build
    held = dict(eu_build().statements)["eu_held"]
    assert "'LOSER_RULE_RETIRING' FROM dev.lab.ngr_s_eu_pairs p JOIN dev.lab.ngr_s_retirements_rule_proposed t ON t.old_id=p.loser_id" in held


# ---------- what a hold means for the papers (the link election's own SQL) ----------
class _Captured(Exception):
    pass


class Capture:
    """work_awards.run up to the link election; records the SQL of each artifact by name (scratch prefix '')."""
    def __init__(self):
        self.r, self.p, self.nih, self.sql_of, self.config, self.counts = "", "", False, {}, {}, {}
    def guard(self): pass
    def zero(self, name, query): pass
    def artifact(self, name, query, params=None):
        self.sql_of[name] = query
        if name == "work_matches":
            raise _Captured
        return name


def election_sql():
    c = Capture()
    with pytest.raises(_Captured):
        work_awards.run(c)
    return [(n, c.sql_of[n]) for n in ("collapsed_observations", "linking_sharp_input", "linking_sharp", "work_key_candidates",
                                       "work_key_map", "work_matches")]


def linked_award(retired):
    """Paper 1 cites MOP192552, paper 2 the bare 192552. Returns {work: elected award} with the shell held or retired."""
    db = DB(); con = db.con
    for ddl in ("legacy_aliases (old_id BIGINT, funder_id BIGINT, award_key VARCHAR, stable_id BIGINT)",
                "work_aliases (target_id BIGINT, funder_id BIGINT, old_funder_award_id VARCHAR)",
                "resolve_candidate (owner_id BIGINT, stable_id BIGINT, status VARCHAR)",
                "state_candidate (stable_id BIGINT, entity_kind VARCHAR)",
                "awards_candidate (id BIGINT, provenance VARCHAR, display_name VARCHAR, end_year INTEGER)",
                "normalization_lookup (funder_id BIGINT, award_key VARCHAR, is_direct BOOLEAN, sharp_key VARCHAR, sharp_eligible BOOLEAN, is_weak BOOLEAN)",
                "work_normalized (work_id BIGINT, funder_id BIGINT, award_key VARCHAR, nk VARCHAR, sk VARCHAR, priority INTEGER)"):
        con.execute("CREATE TABLE " + ddl)
    con.execute(f"""INSERT INTO resolved_observations VALUES ('r',{RECORD},1,{CIHR},'192552_1','1925521','192552',true),
        ('s',{SHELL},2,{CIHR},'mop192552','mop192552','192552',false)""")
    con.execute(f"""INSERT INTO normalization_lookup VALUES ({CIHR},'192552_1',true,'192552',true,false),
        ({CIHR},'mop192552',false,'192552',true,false),({CIHR},'192552',false,'192552',true,false)""")
    con.execute(f"INSERT INTO awards_candidate VALUES ({RECORD},'cihr','PET/MR imaging',2029)")
    con.execute(f"INSERT INTO state_candidate VALUES ({RECORD},'DIRECT')")
    if retired:
        con.execute(f"INSERT INTO resolve_candidate VALUES ({RECORD},{RECORD},'ACTIVE'),({SHELL},{RECORD},'ACTIVE')")
    else:
        con.execute(f"INSERT INTO resolve_candidate VALUES ({RECORD},{RECORD},'ACTIVE'),({SHELL},{SHELL},'ACTIVE')")
        con.execute(f"INSERT INTO awards_candidate VALUES ({SHELL},'crossref_work_funders',NULL,NULL)")
        con.execute(f"INSERT INTO state_candidate VALUES ({SHELL},'SHELL')")
    con.execute(f"INSERT INTO work_normalized VALUES (1,{CIHR},'mop192552','mop192552','192552',4),(2,{CIHR},'192552','192552','192552',4)")
    for name, sql in election_sql():
        db.artifact(name, sql)
    return dict(db.rows("SELECT work_id,stable_id FROM work_matches ORDER BY work_id"))


def test_a_held_shell_keeps_its_papers_and_a_retired_one_hands_them_to_the_record():
    assert linked_award(retired=False) == {1: SHELL, 2: RECORD}
    assert linked_award(retired=True) == {1: RECORD, 2: RECORD}
