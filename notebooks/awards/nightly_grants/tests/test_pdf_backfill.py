"""Offline tests for the nightly PDF backfill step (lib/pdf_backfill.py, 10-01)."""
import json
import pathlib
import re
import sys
import types

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "tests"))

import pdf_backfill
from nightly_runtime import Nightly
from test_nightly_grants import Result, FakeSpark, config, calls_in_order

TABLES = dict(fulltext_work_funders="openalex.works.fulltext_work_funders", locations_mapped="openalex.works.locations_mapped",
              grobid_processing_results="openalex.pdf.grobid_processing_results", funder_names_keep="openalex.common.funder_names_keep",
              funders_api="openalex.funders.funders_api", ror_relationships="openalex.institutions.ror_relationships",
              awards="openalex.awards.openalex_awards", award_id_guard="openalex.awards.award_id_guard",
              grobid="openalex.pdf.grobid_award_matches")
LAST_RUN = json.dumps(dict(versions=dict(raw=dict(relation="a.b.raw", version=7))))


class BackfillSpark(FakeSpark):
    def __init__(self, ledger_rows=0, due=2, fail_on=None, last_run=LAST_RUN):
        super().__init__()
        self.ledger_rows, self.due, self.fail_on, self.last_run = ledger_rows, due, fail_on, last_run

    def sql(self, statement, args=None):
        s = " ".join(statement.split())
        if self.fail_on and self.fail_on in s:
            self.statements.append((statement, dict(args or {})))
            raise Exception("BOOM")
        if "award_nightly_runs WHERE status LIKE 'SUCCEEDED%'" in s:
            self.statements.append((statement, dict(args or {})))
            return Result([types.SimpleNamespace(details_json=self.last_run)] if self.last_run else [])
        if s.startswith("SELECT 1 FROM dev.lab.ngr_award_nightly_pdf_backfill"):
            self.statements.append((statement, dict(args or {})))
            return Result([1] if self.ledger_rows else [])
        if s.startswith("SELECT count(*) n FROM dev.lab.ngr_s_pdfbf_due"):
            self.statements.append((statement, dict(args or {})))
            return Result([types.SimpleNamespace(n=self.due)])
        if s.startswith("SELECT count(*) n FROM"):
            self.statements.append((statement, dict(args or {})))
            return Result([types.SimpleNamespace(n=0)])
        if s.startswith("SELECT count_if(status='DONE')"):
            self.statements.append((statement, dict(args or {})))
            return Result([types.SimpleNamespace(done=self.due, held=0, written=5)])
        return super().sql(statement, args)


def cfg(**pb):
    return config(write_fence=["dev.lab.ngr_", "dev.lab.grobid"],
                  pdf_backfill=dict(tables=dict(TABLES, grobid="dev.lab.grobid"), **pb))


def statements(spark):
    return [" ".join(s.split()) for s, _ in spark.statements]


NAME_CUT = "LEFT SEMI JOIN R.pdfbf_names keep ON keep.id=fnk.id AND keep.name=fnk.name"


def test_matching_is_the_manual_notebook_verbatim():
    """Token-for-token equal to BackfillPdfAwardMatches.sql step 3 (the staged match) and step 3b (the insert). The only
    differences: the trailing ';', the batch_time column, the scratch table names, and the one added line that cuts the
    funder-name list to the names that can matter."""
    kyle = (ROOT.parent / "BackfillPdfAwardMatches.sql").read_text()
    stage = "CREATE OR REPLACE TABLE openalex.pdf.backfill_award_matches AS"
    i = kyle.index(stage)
    k3 = kyle[i:kyle.index("-- COMMAND", i)].replace(stage, "", 1)
    i = kyle.index("INSERT INTO openalex.pdf.grobid_award_matches")
    k3b = kyle[i:kyle.index("-- COMMAND", i)].replace("INSERT INTO openalex.pdf.grobid_award_matches", "", 1)
    made = {}
    c = types.SimpleNamespace(r="R.", artifact=lambda name, query: made.setdefault(name, query))
    mine = pdf_backfill.match_sql(c, "backfill_funders", TABLES)
    assert made["pdfbf_matches"].count(NAME_CUT) == 1 and NAME_CUT not in mine
    # the cut keeps every name that contains a due funder's name (case-insensitive), from the same name list
    names = " ".join(made["pdfbf_names"].split())
    assert "ON contains(lower(a.name),lower(d.name))" in names and names.count("openalex.common.funder_names_keep") == 2

    def tokens(s):
        s = re.sub(r"--[^\n]*", "", s)
        s = re.sub(r"/\*\+.*?\*/", "", s)
        s = s.replace(NAME_CUT, "")
        s = s.replace("R.pdfbf_sections", "openalex.pdf.backfill_funder_sections").replace("R.pdfbf_target_works", "backfill_target_works")
        s = s.replace("R.pdfbf_matches", "openalex.pdf.backfill_award_matches")
        s = re.sub(r",\s*now\(\) AS batch_time", "", s).replace("DISTINCT pfs.work_id", "pfs.work_id")
        return re.findall(r"'(?:[^'\\]|\\.)*'|\w+|[^\s\w]", s.rstrip().rstrip(";"))
    assert tokens(k3) == tokens(made["pdfbf_matches"])          # the costly match, built once
    assert tokens(k3b) == tokens(mine)                          # the insert that reads it


def test_dormant_without_config():
    spark = FakeSpark()
    c = Nightly(spark, config())
    pdf_backfill.run(c)
    assert spark.statements == []


def test_first_night_seeds_the_ledger_except_seed_as_due():
    spark = BackfillSpark(ledger_rows=0, due=0)
    c = Nightly(spark, cfg(seed_as_due=[11, 22]))
    pdf_backfill.run(c)
    seed = next(s for s in statements(spark) if s.startswith("INSERT INTO dev.lab.ngr_award_nightly_pdf_backfill"))
    assert "'SEEDED'" in seed and "WHERE funder_id NOT IN (11,22)" in seed
    assert "FROM a.b.raw VERSION AS OF 7 WHERE priority>=3" in " ".join(statements(spark))


def test_due_funders_are_matched_written_under_the_cap_and_recorded():
    spark = BackfillSpark(ledger_rows=1, due=2)
    c = Nightly(spark, cfg(max_pairs_per_funder=500, max_checks_per_funder=9000, max_checks_per_night=20000))
    pdf_backfill.run(c)
    st = statements(spark)
    made = lambda name: next(s for s in st if f"ngr_s_{name} " in s and s.startswith("CREATE OR REPLACE TABLE"))
    # tonight = every too-big funder (to be held) + the cheapest others up to the night's budget; the rest stay due
    tonight = made("pdfbf_tonight")
    assert "sum(CASE WHEN checks<=9000 THEN checks ELSE 0 END) OVER (ORDER BY checks,funder_id ROWS UNBOUNDED PRECEDING) running" in tonight
    assert "WHERE checks>9000 OR running<=20000" in tonight
    # too-big funders never reach the match; only DONE funders are written
    assert "FROM dev.lab.ngr_s_pdfbf_tonight WHERE checks<=9000" in made("pdfbf_funders")
    per = made("pdfbf_per_funder")
    assert "CASE WHEN z.checks>9000 OR coalesce(p.pairs,0)>500 THEN 'HELD' ELSE 'DONE' END status" in per
    assert "JOIN dev.lab.ngr_s_pdfbf_tonight z USING(funder_id)" in per          # funders over the budget get no ledger row
    assert c.counts["pdf_backfill_waiting"] == 0
    ins = next(s for s in st if s.startswith("INSERT INTO dev.lab.grobid"))
    assert "p.status='DONE'" in ins
    # held for a person: the sources are recorded with the HELD status, so the funder is not retried every night
    merge = next(s for s in st if s.startswith("MERGE INTO dev.lab.ngr_award_nightly_pdf_backfill"))
    assert "UPDATE SET sources=u.sources," in merge and "status=u.status" in merge
    assert c.counts["pdf_backfill_done"] == 2 and c.counts["pdf_backfill_pairs_written"] == 5


def test_a_failing_match_holds_the_funders_and_does_not_stop_the_night():
    spark = BackfillSpark(ledger_rows=1, due=3, fail_on="ngr_s_pdfbf_new")
    c = Nightly(spark, cfg())
    pdf_backfill.run(c)                                   # must not raise
    hold = [s for s in statements(spark) if s.startswith("MERGE INTO dev.lab.ngr_award_nightly_pdf_backfill")][-1]
    assert "status='HELD'" in hold and "sources=" not in hold.split("WHEN NOT MATCHED")[0]   # sources untouched: retried
    assert c.counts["pdf_backfill_held"] == 3 and c.counts["pdf_backfill_error"].startswith("step failed")
    assert not any(s.startswith("INSERT INTO dev.lab.grobid") for s in statements(spark))


def test_no_successful_run_yet_skips():
    spark = BackfillSpark(last_run=None)
    c = Nightly(spark, cfg())
    pdf_backfill.run(c)
    assert c.counts["pdf_backfill"].startswith("skipped")


def test_an_unreadable_run_log_is_reported_not_raised():
    spark = BackfillSpark(fail_on="award_nightly_runs WHERE status LIKE 'SUCCEEDED%'")
    c = Nightly(spark, cfg())
    pdf_backfill.run(c)                                   # must not raise
    assert c.counts["pdf_backfill"].startswith("error: BOOM")


def test_step_runs_in_its_own_cell_after_the_night_is_finished_and_public():
    text = (ROOT / "NightlyGrants.py").read_text()
    order = calls_in_order(ROOT / "NightlyGrants.py")
    last = lambda name: max(i for i, n in enumerate(order) if n == name)
    assert order.count("pdf_backfill.run") == 1
    assert order.index("pdf_backfill.run") > max(last("c.finish"), last("build_awards.mark_published"), last("c.swap"))
    night, after = text.split("except BaseException as exc:")
    assert "pdf_backfill.run" not in night                # outside the night's try: it can't turn a finished night into FAILED
    cell = next(x for x in after.split("# COMMAND ----------") if "pdf_backfill.run" in x)
    assert 'if not config.get("stop_before_apply"):' in cell


def test_a_night_budget_below_the_funder_cap_cannot_starve_a_funder():
    spark = BackfillSpark(ledger_rows=1, due=1)
    c = Nightly(spark, cfg(max_checks_per_funder=9000, max_checks_per_night=10))
    pdf_backfill.run(c)
    assert any("WHERE checks>9000 OR running<=9000" in s for s in statements(spark))


def test_prod_config_fences_the_match_table_and_uses_the_measured_budget():
    prod = json.loads((ROOT / "config.prod.json").read_text())
    pb = prod["pdf_backfill"]
    assert pb["tables"]["grobid"] in prod["write_fence"]
    assert (pb["max_checks_per_funder"], pb["max_checks_per_night"], pb["max_pairs_per_funder"]) == (4_000_000_000, 4_000_000_000, 100000)
    assert pb["seed_as_due"] and all(isinstance(x, int) for x in pb["seed_as_due"])
    assert set(pb) - {"tables", "seed_as_due"} == set(pdf_backfill.DEFAULTS)          # no stray or misspelled knobs


# --- longest number wins (cut_short_awards) ---------------------------------------------------------------------------
def _sql_literal(sql, before):
    """The SQL string literal that follows `before`, with SQL's backslash escapes undone (what the regex engine sees)."""
    i = sql.index(before) + len(before)
    lit = re.match(r"\s*'((?:[^'\\]|\\.)*)'", sql[i:]).group(1)
    return re.sub(r"\\(.)", r"\1", lit)


def _cut(text, awards, history=()):
    """cut_short_awards for one paper text, run with the regexes taken from the SQL itself. `awards` maps each of the
    funder's usable numbers to True when it comes from the funder's own feed. `history` is the funder's rows already in
    the match table, as (paper, number, text). Returns the numbers the builders drop."""
    made = {}
    c = types.SimpleNamespace(r="R.", artifact=lambda name, query: made.setdefault(name, query))
    sql = " ".join(pdf_backfill.match_sql(c, "backfill_funders", TABLES).split())
    staged = " ".join(made["pdfbf_matches"].split())                  # award_matches, built once
    nested = sql[sql.index("nested_awards AS ("):sql.index("standalone_awards AS (")]
    alone_sql = sql[sql.index("standalone_awards AS ("):sql.index("cut_short_awards AS (")]
    # the shape the regexes sit in: same paper, funder and text, the longer number starts with the short one, every
    # occurrence must be explained, and a number from a funder's feed is never dropped
    assert "JOIN R.pdfbf_matches l ON l.work_id=s.work_id AND l.funder_id=s.funder_id AND l.all_sections=s.all_sections" in nested
    assert "AND startswith(l.funder_award_id,s.funder_award_id)" in nested
    assert "HAVING NOT bool_or(s.from_funder_feed) AND NOT s.all_sections RLIKE CONCAT(s.award_match_pattern," in nested
    # paper metadata is the Nightly's own DEPOSITED list, and the notebooks spell out the same list
    import work_awards
    feed = f"bool_or(NOT COALESCE(oa.provenance IN {work_awards.DEPOSITED},false)) OVER (PARTITION BY oa.funder_id,oa.funder_award_id) AS from_funder_feed"
    assert feed in staged and "pfs.all_sections RLIKE ua.award_match_pattern" in staged
    for notebook in ("TagPdfAwardsIncremental.sql", "BackfillPdfAwardMatches.sql"):
        assert f"oa.provenance IN {work_awards.DEPOSITED}" in (ROOT.parent / notebook).read_text()
    # stands alone: counted per paper over the match table, the number's own rows and its longer forms, same funder
    assert "JOIN openalex.pdf.grobid_award_matches g ON g.funder_id=n.funder_id AND startswith(g.funder_award_id,n.funder_award_id) GROUP BY" in alone_sql
    assert "GROUP BY n.funder_id,n.funder_award_id,g.paper_id) GROUP BY funder_id,funder_award_id HAVING count_if(alone)>=count_if(NOT alone))" in alone_sql
    assert "FROM nested_awards n LEFT ANTI JOIN standalone_awards k ON n.funder_id=k.funder_id AND n.funder_award_id=k.funder_award_id)" in sql
    tail_ok = _sql_literal(nested, "substr(l.funder_award_id,LENGTH(s.funder_award_id)+1) RLIKE")
    meta = _sql_literal(nested, "substr(l.funder_award_id,LENGTH(s.funder_award_id)+1),")
    open_, close = _sql_literal(nested, "CONCAT(s.award_match_pattern,"), _sql_literal(nested, "'|'),")
    not_continued = _sql_literal(alone_sql, "CONCAT(n.award_match_pattern,")
    esc = lambda x: re.sub(meta, r"\\\1", x)
    pattern = lambda a: r"\b" + esc(a) + r"\b"                       # award_match_pattern in candidate_awards
    matched = [a for a in awards if re.search(pattern(a), text)]     # the staged match
    dropped = set()
    for s in matched:
        tails = sorted({l[len(s):] for l in matched if l.startswith(s) and re.search(tail_ok, l[len(s):])})
        if not tails or awards[s] or re.search(pattern(s) + open_ + "|".join(esc(t) for t in tails) + close, text):
            continue                                                 # not in nested_awards
        papers = {}                                                  # standalone_awards: per paper, does it stand alone there
        for paper, number, old_text in history:
            if number.startswith(s):
                alone = number == s and bool(re.search(pattern(s) + not_continued, old_text))
                papers[paper] = papers.get(paper, False) or alone
        if not papers or sum(papers.values()) < sum(not v for v in papers.values()):
            dropped.add(s)
    return dropped


def test_cut_short_number_goes_when_every_occurrence_starts_a_longer_matched_number():
    held = {"016.Veni": False, "016.Veni.173.076": False, "DE-AC02": False, "DE-AC02-": False, "DE-AC02-05CH11231": False}
    assert _cut("NWO grant 016.Veni.173.076 and DOE contract DE-AC02-05CH11231.", held) == {"016.Veni", "DE-AC02", "DE-AC02-"}
    # the full number is never dropped, and with no longer number matched nothing is dropped
    assert _cut("NWO grant 016.Veni.999.001.", held) == set()


def test_a_number_that_stands_alone_once_is_kept():
    held = {"AB12345": False, "AB12345-999": False}
    assert _cut("Grants AB12345 and AB12345-999.", held) == set()
    assert _cut("This AB12345-funded work also used AB12345-999.", held) == set()      # not inside the longer one
    assert _cut("Grant AB12345-999.", held) == {"AB12345"}


def test_a_longer_number_that_only_shares_the_prefix_does_not_explain_the_short_one():
    # neither longer number sits where the short one occurs
    assert _cut("NIH R01CA12-01 and R01CA123.", {"R01CA12": False, "R01CA123": False}) == set()
    most = {"MOST 107-2221-E-006-08": False, "MOST 107-2221-E-006-089": False}
    assert _cut("MOST 107-2221-E-006-08-MY3 and MOST 107-2221-E-006-089.", most) == set()


def test_a_unicode_dash_is_not_a_separator_so_the_number_is_kept():
    # the matcher texts carry no Unicode dashes today; if one appears, that occurrence is unexplained
    assert _cut("Grant AB12345–999 and AB12345-999.", {"AB12345": False, "AB12345-999": False}) == set()


def test_a_number_from_a_funders_own_feed_is_never_dropped():
    text = "NWO 480-15-001/674 and NIH R01CA123456-05."
    assert _cut(text, {"480-15-001": True, "480-15-001/674": False, "R01CA123456": True, "R01CA123456-05": False}) == set()
    # the same parent and sub-number pair from paper metadata: the longer one wins
    assert _cut(text, {"R01CA123456": False, "R01CA123456-05": False}) == {"R01CA123456"}


def test_a_label_or_funder_doi_after_the_number_does_not_make_a_longer_number():
    pid = "PID2019-104576GB-I00"
    held = {pid: False, pid + "/AEI/10.13039/501100011033": False, pid + "/AEI/10": False}
    assert _cut(f"Funded by {pid}/AEI/10.13039/501100011033.", held) == {pid + "/AEI/10"}      # the real number stays
    assert _cut(f"Funded by {pid}/10.13039/501100011033.", {pid: False, pid + "/10.13039/501100011033": False}) == set()
    assert _cut("CNPq 3381/2013/CAPES.", {"3381/2013": False, "3381/2013/CAPES": False}) == set()
    assert _cut("EU grant 101052200-EUROfusion.", {"101052200": False, "101052200-EUROfusion": False}) == set()
    # a longer number that carries on after a space is kept apart too
    assert _cut("DFG EXC 2145 SyNergy.", {"EXC 2145": False, "EXC 2145 SyNergy": False}) == set()
    # a real continuation after "/" or "-" still wins
    assert _cut("FCT 2020.03107.CEECIND/CP1596/CT0005 and EPSRC EP/T016337/1.", {"2020.03107.CEECIND": False,
                "2020.03107.CEECIND/CP1596/CT0005": False, "EP/T016337": False, "EP/T016337/1": False}) == {"2020.03107.CEECIND", "EP/T016337"}


def test_a_code_or_word_after_the_number_does_not_make_a_longer_number():
    # a real number that papers cite with an administrative code or a project acronym after it
    assert _cut("NSFC grant 81171037/H0903.", {"81171037": False, "81171037/H0903": False}) == set()
    assert _cut("IMI2 grant 807015-Im2Pact.", {"807015": False, "807015-Im2Pact": False}) == set()
    assert _cut("H2020 grant 101000000-5GSMART.", {"101000000": False, "101000000-5GSMART": False}) == set()
    # a continuation in capitals and digits is still a longer number
    assert _cut("DOE DE-AC02-05CH11231 and ANR-10-LABX-0001.", {"DE-AC02": False, "DE-AC02-05CH11231": False,
                "ANR-10": False, "ANR-10-LABX-0001": False}) == {"DE-AC02", "ANR-10"}


def test_a_label_then_a_dash_does_not_make_a_longer_number():
    assert _cut("ERC grant 639088/ERC-2014-STG.", {"639088": False, "639088/ERC-2014-STG": False}) == set()


def test_a_number_papers_mostly_cite_on_its_own_is_kept():
    held = {"2022YFD2400600": False, "2022YFD2400600-2": False}
    text = "National Key R&D Program 2022YFD2400600-2."
    alone = [(1, "2022YFD2400600", "grant 2022YFD2400600."), (2, "2022YFD2400600", "grant 2022YFD2400600, and others")]
    nested = [(3, "2022YFD2400600-2", "grant 2022YFD2400600-2"), (3, "2022YFD2400600", "grant 2022YFD2400600-2")]
    assert _cut(text, held) == {"2022YFD2400600"}                                  # no history: the longer number wins
    assert _cut(text, held, alone + nested) == set()                               # alone on 2 papers, nested on 1: kept
    assert _cut(text, held, alone[:1] + nested + [(4, "2022YFD2400600-2", "x")]) == {"2022YFD2400600"}     # 1 against 2: cut
    # a stub stays a stub: DE-AC02 written alone on one paper, inside a full contract number on three
    doe = {"DE-AC02": False, "DE-AC02-05CH11231": False}
    rows = [(1, "DE-AC02", "contract DE-AC02 with DOE")] + [(n, "DE-AC02-05CH11231", "DE-AC02-05CH11231") for n in (2, 3, 4)]
    assert _cut("Contract DE-AC02-05CH11231.", doe, rows) == {"DE-AC02"}


def test_dropping_nested_rows_does_not_change_which_numbers_stand_alone():
    # a chain of three: the middle number explains the short one, the longest explains the middle one, and the longest
    # is not a longer form of the short one by the tail test. The count must not need the middle rows.
    held = {"AB12345": False, "AB12345-a1": False, "AB12345-a1-b2": False}
    text = "grant AB12345-a1-b2."
    before = [(1, "AB12345", "grant AB12345.")] + [(p, n, text) for p in (2, 3) for n in held]
    after = [(1, "AB12345", "grant AB12345.")] + [(p, "AB12345-a1-b2", text) for p in (2, 3)]     # what the cleanup leaves
    assert _cut(text, held, before) == _cut(text, held, after) == {"AB12345", "AB12345-a1"}
