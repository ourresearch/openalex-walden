"""The four-tier rank table for grant sources (rank 1 = highest), on a local SQL engine. No warehouse, no network."""
import pathlib
import re
import sys
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import award_source_rank as R
import registry_twin

duckdb = pytest.importorskip("duckdb")

# provenance, old number, funder, expected rank (old numbers are the real ones on main, 19 Nov 2026)
CASES = [
    ("nih_exporter", 3, 1, 1), ("nwopen", 11, 20, 1), ("fwf_open_api", 51, 30, 1), ("ucop_grants", 106, 40, 1),
    ("bhf_europepmc_grist", 575, 50, 1),                        # not decided: stays rank 1
    ("crossref_work", 1, 30, 2), ("datacite", 1, 40, 2),
    ("usaspending", 26, 1, 3), ("usaspending_subaward", 703, 1, 3), ("usaspending_cdc", 55, 1, 3), ("iati_fcdo", 600, 1, 3),
    ("idrc_iati", 37, 1, 3), ("hhs_taggs", 417, 1, 3), ("openaire_fwf", 29, 30, 3),
    ("crossref_work_funders", 0, 1, 4), ("crossref_work.grants", 0, 1, 4), ("europepmc_work_funders", 0, 1, 4),
    ("datacite_work_funders", 0, 1, 4), ("crossref_work.grants", 2, 1, 4),
]


def con(rows):
    c = duckdb.connect()
    c.execute("CREATE TABLE raw(id BIGINT,provenance VARCHAR,priority INT,funder_id BIGINT,funder_award_id VARCHAR,doi VARCHAR,updated_date VARCHAR)")
    c.executemany("INSERT INTO raw VALUES (?,?,?,?,?,?,?)", rows)
    return c


def test_names_say_rank_one_is_highest():
    assert R.HIGHEST == 1 and R.LOWEST == 4 and list(R.NAMES) == [1, 2, 3, 4]
    assert R.NAMES[1] == "funder's own data" and R.NAMES[4] == "mention-only grant"


def test_sql_and_python_agree_on_every_case():
    c = con([(i, p, n, f, "x", None, None) for i, (p, n, f, _) in enumerate(CASES)])
    got = dict(c.execute(f"SELECT id,{R.rank_sql('x')} FROM raw x").fetchall())
    for i, (p, n, f, want) in enumerate(CASES):
        assert got[i] == want == R.rank(p, n, f), (p, n)


def elect(c, not_own=None, joins=""):
    """The builder's election shape: one winner per funder + number."""
    return c.execute(f"""WITH o AS (SELECT x.*,{R.rank_sql('x', not_own)} source_rank FROM raw x{joins})
        SELECT funder_id,lower(funder_award_id) k,provenance FROM o
        QUALIFY row_number() OVER(PARTITION BY funder_id,lower(funder_award_id) ORDER BY {R.order_sql()},updated_date DESC NULLS LAST,id)=1
        ORDER BY 1,2""").fetchall()


def test_higher_rank_overwrites_lower_whatever_the_old_number():
    c = con([
        (1, "nih_exporter", 3, 1, "R01", None, None), (2, "usaspending_subaward", 703, 1, "R01", None, None),   # old rule: 703 won
        (3, "crossref_work", 1, 2, "G1", "10.1/g1", None), (4, "usaspending", 26, 2, "G1", None, None),        # old rule: 26 beat the grant DOI
        (5, "crossref_work_funders", 0, 3, "M1", None, None), (6, "iati_fcdo", 600, 3, "M1", None, None),
        (7, "crossref_work_funders", 0, 4, "ONLY", None, None),
    ])
    assert elect(c) == [(1, "r01", "nih_exporter"), (2, "g1", "crossref_work"), (3, "m1", "iati_fcdo"), (4, "only", "crossref_work_funders")]
    old = c.execute("""SELECT provenance FROM raw WHERE funder_id IN (1,2)
        QUALIFY row_number() OVER(PARTITION BY funder_id ORDER BY (priority>=3 OR priority=1) DESC,priority DESC)=1 ORDER BY funder_id""").fetchall()
    assert old == [("usaspending_subaward",), ("usaspending",)]   # what main does today


def test_inside_one_rank_the_old_order_is_kept():
    c = con([(1, "gateway_to_research", 3, 1, "EP/1", None, None), (2, "gateway_to_research", 30, 1, "EP/1", None, None)])
    assert c.execute(f"""SELECT priority FROM (SELECT x.*,{R.rank_sql('x')} source_rank FROM raw x)
        QUALIFY row_number() OVER(PARTITION BY funder_id ORDER BY {R.order_sql()})=1""").fetchall() == [(30,)]


def test_a_funders_own_data_wins_only_for_that_funder():
    """Kyle's example: NWO's source must not outrank FWF's on an FWF grant. NWO = funder 20, FWF = funder 30.
    Made-up case: the NWO loader writes a row for an FWF grant and its old number is 99 (really 11; FWF's is 51).
    On main the higher number wins and nothing checks whose grant it is. With NWO's real 11, FWF's row already wins."""
    rows = [(1, "fwf_open_api", 51, 30, "P 12345", None, "2026-01-01"), (2, "nwopen", 99, 30, "P 12345", None, "2026-11-01"),
            (3, "nwopen", 99, 20, "VI.1", None, None)]
    c = con(rows)
    assert elect(c) == [(20, "vi.1", "nwopen"), (30, "p 12345", "nwopen")]            # without the owner list: old number decides
    c.execute("CREATE TABLE source_owners_v AS SELECT * FROM (VALUES ('nwopen',20),('fwf_open_api',30)) t(provenance,funder_id)")
    joins, not_own = R.owner_join_sql("x")
    assert elect(c, not_own, joins) == [(20, "vi.1", "nwopen"), (30, "p 12345", "fwf_open_api")]
    owners = {"nwopen": {20}, "fwf_open_api": {30}}
    assert R.rank("nwopen", 99, 30, owners) == 3 and R.rank("nwopen", 99, 20, owners) == 1 and R.rank("nih_exporter", 3, 7, owners) == 1


def test_every_old_number_test_lives_in_the_rank_table():
    """Kyle's 19 Nov decision 4: one place decides precedence. No file of the grant builder but award_source_rank.py may compare
    the old priority number (`priority>=3` = a loader, `priority=1` = a grant DOI); they ask the rank table instead."""
    bad = []
    for path in list((ROOT / "lib").glob("*.py")) + list((ROOT / "sql").rglob("*.sql")) + [ROOT / "NightlyGrants.py"]:
        if path.name == "award_source_rank.py":
            continue
        for n, line in enumerate(path.read_text().splitlines(), 1):
            if re.search(r"priority\s*(>=|<=|<>|!=|=|<|>)\s*[0-9]", line.split("#")[0]):
                bad.append(f"{path.name}:{n}: {line.strip()[:80]}")
    assert not bad, bad


def test_helpers_agree_with_the_rank():
    c = con([(i, p, n, f, "x", None, None) for i, (p, n, f, _) in enumerate(CASES)])
    got = c.execute(f"""SELECT id,{R.rank_sql('x')},{R.is_direct_sql('x')},{R.is_grant_doi_sql('x')},{R.is_loader_sql('x')},
        {R.namespace_sql('x')} FROM raw x ORDER BY id""").fetchall()
    for (i, rank, direct, doi, loader, ns), (p, n, f, want) in zip(got, CASES):
        assert rank == want and direct == (want < 4) and doi == (want == 2) and loader == (want in (1, 3)), (p, n)
        assert ns == (R.GRANT_DOI[p] if want == 2 else None), (p, n)


def test_a_row_without_a_number_has_no_rank():
    """As on main: such an observation fails INVALID_OBSERVATION (is_direct NULL) instead of passing as a mention."""
    c = con([(1, "crossref_work", None, 1, "x", None, None), (2, "nih_exporter", None, 1, "x", None, None)])
    assert c.execute(f"SELECT {R.rank_sql('x')},{R.is_direct_sql('x')} FROM raw x").fetchall() == [(None, None), (None, None)]
    assert R.rank("nih_exporter", None) is None


def test_undecided_sources_are_rank_one_and_named():
    for prov in ("bhf_europepmc_grist", "guys_st_thomas_360giving", "agaur_raisc_socrata", "gateway_to_research"):
        assert R.rank(prov, 100) == 1 and any(R._is(prov, k, x) for k, x, _ in R.UNDECIDED), prov
    assert not any(R._is("innovate_uk", k, x) for k, x, _ in R.UNDECIDED)


class Row(dict):
    __getattr__ = dict.__getitem__

    def asDict(self):
        return dict(self)


class DuckC:
    """Runs a build step on a local DuckDB: artifacts become tables named r+name. Spark-only syntax is translated."""
    def __init__(self, config=None, skip=()):
        self.db, self.r, self.p = duckdb.connect(), "ngr_", "reg_"
        self.counts, self.checks, self.config, self.skip, self.skipped = {}, {}, dict(config or {}), set(skip), {}
        payload = types.SimpleNamespace(dataType=types.SimpleNamespace(fieldNames=lambda: ["id", "doi"]))
        self.spark = types.SimpleNamespace(table=lambda name: types.SimpleNamespace(schema={"payload": payload}))

    def has_extra(self, name):
        return name in self.config.get("extra_inputs", {})

    @staticmethod
    def t(q):
        return (q.replace("LEFT ANTI JOIN", "ANTI JOIN").replace("LEFT SEMI JOIN", "SEMI JOIN")
                .replace(" RLIKE '^10[.][0-9]+/.+'", " SIMILAR TO '10[.][0-9]+/.+'"))

    def sql(self, q):
        cur = self.db.execute(self.t(q))
        cols = [d[0] for d in cur.description]
        rows = [Row(zip(cols, r)) for r in cur.fetchall()]
        return types.SimpleNamespace(collect=lambda: rows)

    def artifact(self, name, q):
        if name in self.skip:
            self.skipped[name] = q
        else:
            self.db.execute(f"CREATE OR REPLACE TABLE {self.r}{name} AS {self.t(q)}")
        return self.r + name

    def zero(self, name, q):
        rows = self.sql(f"SELECT * FROM ({q}) x LIMIT 20").collect()
        self.checks[name] = not rows
        if rows:
            raise RuntimeError(name)

    def count(self, name, relation):
        self.counts[name] = n = self.sql(f"SELECT count(*) n FROM {relation}").collect()[0]["n"]
        return n

    def table(self, name, cols, rows):
        self.db.execute(f"CREATE TABLE {name} ({cols})")
        if rows:
            self.db.executemany(f"INSERT INTO {name} VALUES ({','.join('?' * len(rows[0]))})", rows)


def twins_case():
    """UCOP's shape and every reason a pair is held. Funder 40 = UCOP. Grant ids: 1xx feed (rank 1), 2xx grant DOI (rank 2)."""
    c = DuckC()
    D = "https://doi.org/10.17920/"
    c.table("ngr_bindings", "stable_id BIGINT,source_rank INT,source_record_id VARCHAR,payload STRUCT(doi VARCHAR)", [
        (100, 1, None, {"doi": D + "G9159N"}), (200, 2, "10.17920/g9159n", {"doi": None}),     # the pair: feed keeps its id
        (101, 1, None, {"doi": None}), (201, 2, "10.17920/g9zzzz", {"doi": None}),            # feed without a DOI: no pair
        (102, 1, None, {"doi": D + "DUP"}), (103, 1, None, {"doi": D + "dup"}), (202, 2, "10.17920/dup", {"doi": None}),  # two feeds
        (104, 1, None, {"doi": D + "RET"}), (204, 2, "10.17920/ret", {"doi": None}),          # target retires tonight (manifest)
        (105, 1, None, {"doi": D + "OWN"}), (205, 2, "10.17920/own", {"doi": None}), (205, 1, None, {"doi": None}),   # 205 is own data too
        (106, 3, None, {"doi": D + "AGG"}), (206, 2, "10.17920/agg", {"doi": None}),          # an aggregator is never the target
        (107, 1, None, {"doi": D + "ANC"}), (207, 2, "10.17920/anc", {"doi": None}),          # a manifest pair points at the loser
        (108, 1, None, {"doi": D + "FUN"}), (208, 2, "10.17920/fun", {"doi": None}),          # other funder: no pair
        (109, 1, None, {"doi": D + "INTO"}), (209, 2, "10.17920/into", {"doi": None}),        # a shell retires into the loser tonight
        (110, 1, None, {"doi": D + "SHL"}), (210, 2, "10.17920/shl", {"doi": None}),          # a shell retired into it LAST night: no hold
    ])
    c.table("ngr_ownership_plan", "stable_id BIGINT,funder_id BIGINT",
            [(i, 41 if i == 208 else 40) for i in list(range(100, 111)) + [200, 201, 202, 204, 205, 206, 207, 208, 209, 210]])
    c.table("ngr_retirements_rule", "old_id BIGINT,canonical_id BIGINT", [(300, 209)])               # the shell rule only retires shells
    c.table("ngr_retirements_manifest", "old_id BIGINT,canonical_id BIGINT,evidence_class VARCHAR", [(104, 999, "X")])
    c.table("migration_manifest_v", "loser_id BIGINT,target_id BIGINT,evidence_class VARCHAR", [(104, 999, "X"), (400, 207, "X")])
    c.table("ngr_entities_allocated", "stable_id BIGINT,status VARCHAR,redirect_to BIGINT", [(400, "REDIRECTED", 207), (301, "REDIRECTED", 210)])
    return c


def test_doi_twin_ucop_shape_feed_keeps_its_id():
    """Kyle's UCOP example: the DataCite grant-DOI grant (number = DOI suffix) retires into the feed grant with that DOI."""
    c = twins_case()
    pairs = registry_twin.doi_twins(c, True)
    assert c.sql(f"SELECT old_id,canonical_id FROM {pairs} ORDER BY 1").collect() == [
        {"old_id": 200, "canonical_id": 100}, {"old_id": 210, "canonical_id": 110}]
    held = {r["loser_id"]: r["reason"] for r in c.sql("SELECT * FROM ngr_dt_held").collect()}
    assert held == {202: "NOT_ONE_TO_ONE", 204: "TARGET_RETIRING", 207: "MANIFEST_TARGET", 209: "LOSER_IS_TARGET"}
    assert c.counts["doi_twin_pairs"] == 2 and c.counts["doi_twin_held"] == 4 and c.checks["DOI_TWIN_FUSE"]


def test_doi_twin_fuse_stops_a_runaway_night():
    c = twins_case()
    c.config["doi_twin_fuse"] = 1
    with pytest.raises(RuntimeError, match="DOI_TWIN_FUSE"):
        registry_twin.doi_twins(c, True)


def test_doi_twin_holds_a_loser_the_manifest_retires():
    c = twins_case()
    c.db.execute("INSERT INTO ngr_retirements_manifest VALUES (200, 555, 'PUBLIC_DOI_EQUALS_TARGET_DOI')")
    registry_twin.doi_twins(c, True)
    assert c.counts["doi_twin_pairs"] == 1 and c.counts["doi_twin_held_loser_retiring"] == 1


def links_case(produced_w2):
    """Loser G200 retires into G100. Yesterday: w1 and w2 linked G200, w3 linked G100. Tonight w2's acknowledgement is gone
    upstream unless produced_w2 (then the build lost it: a real failure)."""
    c = DuckC()
    c.outputs = {"work_awards": "live_work_awards"}
    c.table("ngr_dt_applied", "loser_id BIGINT,target_id BIGINT", [(200, 100)])
    G = "https://openalex.org/G"
    c.table("ngr_twin_links_before", "work_id BIGINT,award_id VARCHAR", [(1, G + "200"), (2, G + "200"), (3, G + "100")])
    c.table("ngr_twin_candidate_edges", "work_id BIGINT,award_id VARCHAR", [(1, G + "100"), (3, G + "100")])
    c.table("ngr_work_matches", "work_id BIGINT,stable_id BIGINT", [(1, 100), (3, 100)] + ([(2, 100)] if produced_w2 else []))
    c.table("ngr_work_links_moved", "work_id BIGINT,original_work_id BIGINT,stable_id BIGINT", [])
    c.table("ngr_list_suppress_keyed", "served_work_id BIGINT,work_id BIGINT,award_url VARCHAR", [])
    c.table("ngr_list_adds", "work_id BIGINT,served_work_id BIGINT,stable_id BIGINT", [])
    return c


def test_doi_twin_link_gate_explains_a_paper_gone_upstream():
    c = links_case(produced_w2=False)
    registry_twin._conserve(c, "dt", "DOI_TWIN", strict=False)
    assert c.checks == {"DOI_TWIN_NO_LOSER_LINKS": True, "DOI_TWIN_MAPPED_LINKS_PRESERVED": True, "DOI_TWIN_TARGET_LINKS_PRESERVED": True}
    assert c.counts["dt_explained_removals"] == 1 and c.counts["dt_applied_pairs"] == 1 and c.counts["dt_applied_moved"] == 2
    with pytest.raises(RuntimeError, match="DOI_TWIN_MAPPED_LINKS_PRESERVED"):   # the manifest's strict contract would stop here
        registry_twin._conserve(links_case(produced_w2=False), "dt", "DOI_TWIN", strict=True)


def test_doi_twin_link_gate_stops_when_the_build_loses_a_link():
    with pytest.raises(RuntimeError, match="DOI_TWIN_MAPPED_LINKS_PRESERVED"):
        registry_twin._conserve(links_case(produced_w2=True), "dt", "DOI_TWIN", strict=False)


def test_no_link_gates_without_pairs_or_manifest():
    c = DuckC()
    c.outputs = {"work_awards": "live_work_awards"}
    registry_twin.link_conservation(c)           # no table exists: anything it built would fail here
    assert c.checks == {}


def test_retirement_sources_carry_the_doi_twins_with_and_without_a_manifest():
    import test_nih_fold as T
    for extra in ((), ("migration_manifest",)):
        c = T.render(ROOT / "lib" / "build_awards.py", extra=extra)
        src = [q for name, q in c.statements if name == "retirement_sources"][0]
        assert "'DOI_TWIN' FROM dev.lab.ngr_s_dt_pairs" in src and ("'MANIFEST'" in src) == bool(extra), src


def test_served_grant_keeps_its_registered_doi():
    """#2486 (FCT, ANR, AHA): the feed wins the election and carries no DOI; the grant keeps the DOI the funder registered.
    Guard: no two grants serve one DOI."""
    c = DuckC(skip={"award_metadata_doi_kept"})
    n = {"doi": None, "updated_date": None}
    c.table("ngr_bindings_final", "stable_id BIGINT,source_rank INT,priority INT,source_record_id VARCHAR,observation_key VARCHAR,"
            "payload STRUCT(doi VARCHAR,updated_date VARCHAR)", [
                (1, 1, 385, None, "o1", n), (1, 2, 1, "10.54499/a", "o2", {"doi": "https://doi.org/10.54499/A", "updated_date": None}),
                (2, 1, 385, None, "o3a", n), (2, 2, 1, "10.54499/b", "o3", {"doi": "https://doi.org/10.54499/b", "updated_date": None}),
                (3, 1, 385, None, "o3b", n),                                                        # 3: another funder's-own grant serves 10.54499/b
                (4, 2, 1, "10.54499/c", "o4", {"doi": "10.54499/c", "updated_date": None}), (5, 2, 1, "10.54499/c", "o5", {"doi": "10.54499/c", "updated_date": None}),
                (6, 2, 1, "10.54499/d", "o6", {"doi": "10.54499/d", "updated_date": None}),          # 6 serves its own DOI already
                (7, 1, 397, None, "o7a", n), (7, 2, 1, "10.54499/e", "o7", {"doi": "10.54499/e", "updated_date": None}),
                (8, 4, 0, None, "o8", n)])                                                         # 8: a mention-only grant that carries 10.54499/e
    c.table("meta", "stable_id BIGINT,payload STRUCT(id BIGINT,doi VARCHAR)",
            [(1, {"id": 1, "doi": None}), (2, {"id": 2, "doi": None}), (3, {"id": 3, "doi": "https://doi.org/10.54499/B"}),
             (4, {"id": 4, "doi": None}), (5, {"id": 5, "doi": None}), (6, {"id": 6, "doi": "10.54499/d"}),
             (7, {"id": 7, "doi": None}), (8, {"id": 8, "doi": "https://doi.org/10.54499/E"})])
    out = registry_twin.keep_grant_doi(c, "meta")
    got = {r["stable_id"]: (r["doi"], r["skipped"]) for r in c.sql("SELECT * FROM ngr_doi_keep").collect()}
    assert got == {1: ("https://doi.org/10.54499/A", None), 2: ("https://doi.org/10.54499/b", "SERVED_ELSEWHERE"),
                   4: ("10.54499/c", "SHARED"), 5: ("10.54499/c", "SHARED"), 7: ("10.54499/e", None)}   # rank decides: 8 doesn't block 7
    assert c.counts == {"doi_kept": 2, "doi_kept_with_lower_holder": 1, "doi_kept_served_elsewhere": 1, "doi_kept_shared": 2}
    assert out == "ngr_award_metadata_doi_kept" and "coalesce(m.payload.doi,k.doi) AS doi" in c.skipped["award_metadata_doi_kept"]


def test_builder_uses_the_rank_and_the_one_rule():
    text = (ROOT / "lib" / "build_awards.py").read_text()
    assert "is_direct DESC,priority DESC" not in text and "is_direct DESC,b.priority DESC" not in text
    assert "true is_direct,-1 priority,3 source_rank," in text   # legacy GtR rows stay below grant DOIs, as on main
    assert text.count("award_source_rank.order_sql(") == 3 and "award_source_rank.rank_sql('x',not_own)" in text
    assert "source_rank<{award_source_rank.LOWEST} is_direct" in text and "award_source_rank.namespace_sql('x')" in text
    assert "registry_twin.doi_twins(c," in text and "registry_twin.DOI_TWIN_SOURCE" in text
    assert text.index("registry_twin.keep_grant_doi(c,merge_doi_metadata(c))") < text.index("registry_twin.fill(c,metadata)")
    assert "source_rank,o.priority DESC" in (ROOT / "lib" / "nih_fold.py").read_text()
    assert (ROOT / "lib" / "registry_twin.py").read_text().count("award_source_rank.order_sql('b')") == 2
    assert "award_source_rank.is_direct_sql('x')" in (ROOT / "lib" / "nightly_runtime.py").read_text()
    assert "award_source_rank.is_loader_sql('x')" in (ROOT / "lib" / "pdf_backfill.py").read_text()
