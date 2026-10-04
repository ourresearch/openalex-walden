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
    """Token-for-token equal to BackfillPdfAwardMatches.sql step 3. The only differences: the trailing ';', the batch_time
    column, and the one added line that cuts the funder-name list to the names that can matter."""
    kyle = (ROOT.parent / "BackfillPdfAwardMatches.sql").read_text()
    i = kyle.index("INSERT INTO openalex.pdf.grobid_award_matches")
    k3 = kyle[i:kyle.index("-- COMMAND", i)].replace("INSERT INTO openalex.pdf.grobid_award_matches", "", 1)
    made = {}
    c = types.SimpleNamespace(r="R.", artifact=lambda name, query: made.setdefault(name, query))
    mine = pdf_backfill.match_sql(c, "backfill_funders", TABLES)
    assert mine.count(NAME_CUT) == 1
    # the cut keeps every name that contains a due funder's name (case-insensitive), from the same name list
    names = " ".join(made["pdfbf_names"].split())
    assert "ON contains(lower(a.name),lower(d.name))" in names and names.count("openalex.common.funder_names_keep") == 2

    def tokens(s):
        s = re.sub(r"--[^\n]*", "", s)
        s = re.sub(r"/\*\+.*?\*/", "", s)
        s = s.replace(NAME_CUT, "")
        s = s.replace("R.pdfbf_sections", "openalex.pdf.backfill_funder_sections").replace("R.pdfbf_target_works", "backfill_target_works")
        s = re.sub(r",\s*now\(\) AS batch_time", "", s).replace("DISTINCT pfs.work_id", "pfs.work_id")
        return re.findall(r"'(?:[^'\\]|\\.)*'|\w+|[^\s\w]", s.rstrip().rstrip(";"))
    assert tokens(k3) == tokens(mine)


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
