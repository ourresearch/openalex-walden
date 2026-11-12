"""Offline tests for the award country lookup, the affiliation-matcher input and the country guard on institution matching.

The lookup and wiring tests need nothing but pytest. The behaviour tests run sql/api_payload.sql itself on small fixtures:
the Spark SQL is transpiled with sqlglot and executed in DuckDB (no warehouse); they are skipped when either is missing.
    uv run --with pytest --with sqlglot --with duckdb python -m pytest -q
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import award_country
import award_investigators
import create_api

PAYLOAD = (ROOT / "sql/api_payload.sql").read_text()


# ---------- the lookup file ----------
def code(provenance, country):
    return award_country.country_code(award_country.read_lookup(ROOT), provenance, country)


def test_lookup_file_is_valid_and_keys_are_unique():
    rows = award_country.read_lookup(ROOT)
    assert len(rows) > 1000
    assert len({(r["value"], r["provenance_scope"]) for r in rows}) == len(rows)


def test_spellings_of_one_country_resolve_to_one_code():
    for value in ("UNITED STATES", "United States", "US", "USA", " usa ", "United States of America", "États-Unis d'Amérique"):
        assert code("nih_exporter", value) == "US", value
    for value in ("South Africa", "SOUTH AFRICA", "ZA", "ZAF", "Afrique du Sud"):
        assert code("anr_opendata", value) == "ZA", value
    assert code("cordis", "UK") == "GB" and code("cordis", "EL") == "GR"
    assert code("nih_exporter", "TANZANIA U REP") == "TZ" and code("nih_exporter", "CONGO DEM REP") == "CD"
    assert code("nih_exporter", "COTE D'IVOIRE") == "CI"


def test_us_state_codes_do_not_become_african_countries():
    # RWJF stores the grantee's state: MA, GA, TN, SC, NE, SD are Massachusetts ... South Dakota, not Morocco ... Sudan
    for state in ("MA", "GA", "TN", "SC", "NE", "SD", "DC", "CA"):
        assert code("rwjf_grants_explorer", state) == "US", state
    assert code("rwjf_grants_explorer", "United Kingdom") == "GB"      # RWJF's few foreign grantees keep their country
    assert code("erasmus_plus", "MA") == "MA" and code("idrc_iati", "MA") is None


def test_sources_whose_field_is_not_the_organisations_country_give_no_code():
    assert code("idrc_iati", "UG") is None            # recipient country of the project
    assert code("humboldt", "Germany") is None        # constant written by the ingest notebook, fellows' institutions abroad
    assert code("cihr_opendata", "Canada") is None and code("nwopen", "Netherlands") is None
    assert code("nihr", "Award does not have an ODA Downstream Partner") is None
    assert code("fct", "Italy") is None and code("fct", "Portugal") is None
    assert code("snsf", "Germany") is None and code("snsf", "Switzerland") == "CH"


def test_place_of_performance_is_not_the_organisations_country():
    # NSF records where the work is performed: Emory with Tanzania, University of Washington with Antarctica
    for place in ("Tanzania", "Kenya", "United Kingdom", "Antarctica", "Puerto Rico"):
        assert code("nsf_award_search", place) is None, place
    assert code("nsf_award_search", "United States") == "US"       # performed in the US: kept, as a stated assumption
    rows = {(r["provenance_scope"], r["value"]): r for r in award_country.read_lookup(ROOT)}
    assert rows[("nsf_award_search", "*")]["meaning"] == "project_country" and rows[("nsf_award_search", "*")]["confidence"] == "low"
    assert rows[("nsf_award_search", "united states")]["confidence"] == "medium"


def test_a_persons_country_is_not_the_organisations_country():
    assert code("kavli_nextdata", "The Netherlands") is None and code("kavli_nextdata", "USA") is None    # the laureate's country
    assert code("cifar_wp_rest", "Canada") is None and code("bbrf_narsad", "US") is None
    assert code("villum_veluxfonden", "United States") is None and code("villum_veluxfonden", "Denmark") == "DK"
    rows = {(r["provenance_scope"], r["value"]): r for r in award_country.read_lookup(ROOT)}
    assert rows[("kavli_nextdata", "*")]["meaning"] == "person_country" and rows[("cifar_wp_rest", "*")]["meaning"] == "person_country"


def test_a_constant_is_used_only_where_the_funder_is_known_to_fund_at_home():
    assert code("kaken", "Japan") == "JP" and code("fapesp_bv", "Brazil") == "BR"
    rows = {(r["provenance_scope"], r["value"]): r for r in award_country.read_lookup(ROOT)}
    assert rows[("kaken", "*")]["meaning"] == "assumed_domestic" and rows[("humboldt", "*")]["meaning"] == "assumed"
    assert rows[("nih_exporter", "*")]["meaning"] == "organisation" and rows[("idrc_iati", "*")]["meaning"] == "project_country"


def test_a_source_the_file_does_not_describe_gives_no_code():
    assert code("a_source_added_tomorrow", "Kenya") is None and code("nih_exporter", "*") is None


def test_values_that_are_not_one_country_give_no_code():
    for value in ("RI REQUIRED", "Unknown", "USA/Finland", "Yugoslavia", "Harvard Medical School", "", None):
        assert code("nsf_award_search", value) is None, value


def test_invalid_rows_are_refused(tmp_path):
    (tmp_path / "country").mkdir()
    header = "value,provenance_scope,iso2,confidence,meaning,note\n"
    for bad in ("Kenya,*,KE,high,organisation,\n",                      # value not lower-case
                "kenya,*,KEN,high,organisation,\n",                     # not alpha-2
                "kenya,*,,high,organisation,\n",                        # usable row without a code
                "kenya,*,KE,sure,organisation,\n",                      # unknown confidence
                "*,idrc_iati,KE,low,project_country,\n",                # a source row carries no code
                "*,*,,high,organisation,\n",                            # a source row describes one provenance
                "kenya,*,KE,high,organisation,\nkenya,*,KE,high,organisation,\n"):
        (tmp_path / "country" / "award_country_lookup.csv").write_text(header + bad)
        with pytest.raises(RuntimeError, match="COUNTRY_LOOKUP_ROW_INVALID"):
            award_country.read_lookup(tmp_path)
    (tmp_path / "country" / "award_country_lookup.csv").write_text(header)
    with pytest.raises(RuntimeError, match="COUNTRY_LOOKUP_EMPTY"):
        award_country.read_lookup(tmp_path)


def test_apostrophes_are_backslash_escaped_not_doubled():
    # In Spark SQL 'a''b' is two adjacent literals and reads as ab: "côte d'ivoire" would silently become "côte divoire".
    assert award_country.sql_string("côte d'ivoire") == "'côte d\\'ivoire'"
    relation = award_country.lookup_relation_sql(ROOT)
    assert "'côte d\\'ivoire'" in relation and "d''ivoire" not in relation
    assert relation.startswith("SELECT * FROM VALUES (") and relation.endswith("AS award_country_lookup(value,provenance_scope,iso2,confidence)")


# ---------- wiring ----------
class Frame:
    def __init__(self, cols=()):
        self.columns = list(cols)

    def collect(self):
        return []


class FakeC:
    def __init__(self, extra=(), config=None):
        self.r, self.p, self.extra = "dev.lab.ngr_s_", "dev.lab.ngr_", set(extra)
        import json
        boot = json.loads((ROOT / "config.prod.json").read_text())["bootstrap_counts"]
        self.config = dict(package_root=str(ROOT), bootstrap_counts={k: 0 for k in boot}, **(config or {}))
        self.artifacts, self.checks, self.counts = [], {}, {}

    def guard(self): pass
    def require(self, cond, name):
        if not cond: raise RuntimeError(name)
    def has_extra(self, name): return name in self.extra
    def sql(self, statement, params=None): return Frame()
    def artifact(self, name, query, params=None):
        self.artifacts.append((name, query)); return self.r + name
    def zero(self, name, query): self.checks[name] = query
    def count(self, name, relation): self.counts[name] = 0; return 0


def payload_query(**kw):
    c = FakeC(**kw)
    create_api.run(c)
    return c, next(q for n, q in c.artifacts if n == "api_payload")      # the hydration query; research_scope is added to it afterwards


def test_payload_has_no_placeholder_left_and_carries_the_lookup():
    c, q = payload_query()
    for placeholder in ("{AWARDS}", "{ANSWERS}", "{COUNTRY_LOOKUP}", "{INVESTIGATOR_LINKS}"):
        assert placeholder in PAYLOAD and placeholder not in q
    assert "FROM dev.lab.ngr_s_awards_candidate oa" in q and "('uk','*','GB','high')" in q


def test_without_the_matcher_input_every_string_stays_on_the_legacy_lookup():
    c, q = payload_query()
    assert ("affiliation_answers_empty", create_api.EMPTY_ANSWERS) in c.artifacts
    assert "LEFT JOIN dev.lab.ngr_s_affiliation_answers_empty ans" in q and "MATCHER_ANSWERS_UNIQUE" not in c.checks


def test_with_the_matcher_input_its_answers_are_read_first_and_checked_unique():
    c, q = payload_query(extra=["affiliation_answers"])
    assert "LEFT JOIN affiliation_answers_v ans" in q and "affiliation_answers_empty" not in dict(c.artifacts)
    assert "GROUP BY raw_affiliation_string HAVING count(*)<>1" in c.checks["MATCHER_ANSWERS_UNIQUE"]
    case = PAYLOAD[PAYLOAD.index("AS from_matcher,"):PAYLOAD.index("END AS ids")]
    assert case.index("ans.raw_affiliation_string IS NOT NULL THEN") < case.index("asl.institution_ids_override") < case.index("asl.model_response")


def test_matcher_input_is_an_allowed_extra_input():
    text = (ROOT / "lib/nightly_runtime.py").read_text()
    allowed = text[text.index("self.require(view in ("):text.index('"UNKNOWN_EXTRA_INPUT')]
    assert '"affiliation_answers"' in allowed


def test_a_collapse_of_institution_matches_stops_the_night():
    c, _ = payload_query(config=dict(institution_drop_fuse=0.25))
    q = c.checks["INSTITUTION_AWARDED_DROP_FUSE"]
    assert "FROM dev.lab.ngr_s_api_payload WHERE size(institution_awarded)>0" in q and "FROM previous_api_v WHERE size(institution_awarded)>0" in q
    assert "now_n<(1-0.25)*prev_n" in q
    assert "now_n<(1-0.1)*prev_n" in payload_query()[0].checks["INSTITUTION_AWARDED_DROP_FUSE"]


def test_guard_shape():
    assert "deduped AS (SELECT DISTINCT award_id, institution_id FROM guarded WHERE country_ok)" in PAYLOAD
    assert "x.score >= s.thresh AND NOT ISNAN(x.score)" in PAYLOAD
    assert "COALESCE(si.state, i.country_code) = COALESCE(sr.state, e.record_country)" in PAYLOAD
    assert "e.record_country IS NULL OR i.country_code IS NULL" in PAYLOAD                    # unknown never rejects


# ---------- behaviour: the SQL itself, on fixtures ----------
AFF = "STRUCT(name VARCHAR, country VARCHAR, ids STRUCT(id VARCHAR, type VARCHAR, asserted_by VARCHAR)[])"
INV = f"STRUCT(given_name VARCHAR, family_name VARCHAR, orcid VARCHAR, role_start DATE, affiliation {AFF})"
INSTITUTIONS = {            # id: (display_name, country_code)
    10: ("Fred Hutch Cancer Center", "US"), 11: ("Hutchinson Centre Research Institute of South Africa", "ZA"),
    20: ("Universidade de São Paulo", "BR"), 21: ("Universidade Politecnica", "MZ"),
    30: ("Eunice Kennedy Shriver National Institute of Child Health and Human Development", "US"),
    31: ("Health and Human Development (2HD) Research Network", "CM"),
    40: ("Makerere University", "UG"), 41: ("University of Puerto Rico", "PR"), 42: ("University of Reunion Island", "RE"),
    50: ("University of Waterloo", "CA"), 51: ("University of Cape Town", "ZA"), 52: ("Harvard University", "US"),
    60: ("Institut National de la Recherche Agronomique de Tunisie", "TN"), 61: ("Somewhere without a country", None),
    70: ("Queens University", "BD"), 71: ("Queen's University", "CA"),
    80: ("Emory University", "US"), 81: ("Lincoln University", "NZ"), 82: ("Lincoln University", "US"),
    83: ("Universidad Técnica Federico Santa María", "CL"), 84: ("Universidad Técnica Federico Santa María", "EC"),
    90: ("National Sun Yat-sen University", "TW"), 91: ("Sun Yat-sen University", "CN"), 92: ("Tunghai University", "TW"),
    93: ("Tokai University", "JP"), 94: ("University of Sydney", "AU"), 95: ("Universidade de Lisboa", "PT"),
    96: ("University of Hong Kong", "HK"), 97: ("National Central University", "TW"), 98: ("Chuo University", "JP"),
    85: ("Lincoln University", "US"), 99: ("Washington University in St. Louis", "US"),
    100: ("The University of Texas Health Science Center at Houston", "US"),
}
RORS = {94, 95, 90}          # institutions with a ROR in the fixture: https://ror.org/0<id, 6 digits>00
FUNDERS = {1: "TW", 2: "AU", 3: None, 4: "US", 5: "JP"}   # 1 Taiwan MOST, 2 NHMRC, 3 an international funder, 4 NIH, 5 Japan MHLW


def person(name, country, ids=()):
    quote = lambda v: "NULL" if v is None else "'" + v.replace("'", "''") + "'"
    ids = ("[" + ",".join(f"{{'id': {quote(i)}, 'type': {quote(t)}, 'asserted_by': NULL}}" for i, t in ids) + "]") if ids else "NULL"
    return (f"{{'given_name': NULL, 'family_name': NULL, 'orcid': NULL, 'role_start': NULL, "
            f"'affiliation': {{'name': {quote(name)}, 'country': {quote(country)}, 'ids': {ids}}}}}")


class Fixture:
    """Tiny copies of the relations api_payload.sql reads; run() returns {award id: sorted institution ids}."""
    def __init__(self):
        duckdb = pytest.importorskip("duckdb")
        self.sqlglot = pytest.importorskip("sqlglot")
        self.con = con = duckdb.connect()
        con.execute(f"""CREATE TABLE awards (id BIGINT, display_name VARCHAR, description VARCHAR, funder_id BIGINT, funder_award_id VARCHAR,
            amount DOUBLE, currency VARCHAR, funder STRUCT(id VARCHAR, display_name VARCHAR), funding_type VARCHAR, funder_scheme VARCHAR,
            provenance VARCHAR, start_date DATE, end_date DATE, start_year BIGINT, end_year BIGINT, lead_investigator {INV},
            co_lead_investigator {INV}, investigators {INV}[], landing_page_url VARCHAR, doi VARCHAR, works_api_url VARCHAR,
            created_date TIMESTAMP, funded_outputs VARCHAR[], funded_outputs_count BIGINT, primary_topic VARCHAR, topics VARCHAR[],
            parent_awards VARCHAR[], sub_awards VARCHAR[], sub_awards_count BIGINT, parent_awards_full VARCHAR[], sub_awards_full VARCHAR[],
            amount_usd DOUBLE, amount_usd_basis VARCHAR, recipient_countries VARCHAR[], recipient_regions VARCHAR[], recipient_countries_full VARCHAR[],
            recipient_regions_full VARCHAR[], recipients VARCHAR[], recipients_full VARCHAR[], keywords VARCHAR[],
            sustainable_development_goals VARCHAR[])""")  # funding-flows columns (oxjob #1570) the payload selects
        con.execute("CREATE TABLE kaken_projects_v (project_id VARCHAR, institution VARCHAR)")
        con.execute("CREATE TABLE affiliation_lookup_v (raw_affiliation_string VARCHAR, institution_ids_override BIGINT[], model_response STRUCT(id VARCHAR, score DOUBLE)[])")
        con.execute("CREATE TABLE answers (raw_affiliation_string VARCHAR, institution_ids BIGINT[])")
        con.execute("CREATE TABLE institutions_api_v (id BIGINT, display_name VARCHAR, ror VARCHAR, country_code VARCHAR, type VARCHAR, lineage VARCHAR[])")
        for i, (name, country) in INSTITUTIONS.items():
            ror = f"https://ror.org/0{i:06d}00" if i in RORS else None
            con.execute("INSERT INTO institutions_api_v VALUES (?, ?, ?, ?, 'education', [])", [i, name, ror, country])
        con.execute("CREATE TABLE funders_v (funder_id BIGINT, country_code VARCHAR)")
        con.executemany("INSERT INTO funders_v VALUES (?, ?)", list(FUNDERS.items()))
        self.next_id = 0

    def award(self, provenance, lead=None, co_lead=None, investigators=(), funder_award_id="x1", funder_id="NULL"):
        self.next_id += 1
        investigators = "[" + ",".join(person(*p) for p in investigators) + "]" if investigators else "NULL"
        self.con.execute(f"""INSERT INTO awards (id, provenance, funder_id, funder_award_id, lead_investigator, co_lead_investigator, investigators)
            VALUES ({self.next_id}, '{provenance}', {funder_id}, '{funder_award_id}', {person(*lead) if lead else 'NULL'},
                    {person(*co_lead) if co_lead else 'NULL'}, {investigators})""")
        return self.next_id

    def legacy(self, name, scored=(), override=()):
        score = lambda s: "CAST('NaN' AS DOUBLE)" if s != s else str(s)
        model = "[" + ",".join(f"{{'id': '{i}', 'score': {score(s)}}}" for i, s in scored) + "]"
        self.con.execute(f"INSERT INTO affiliation_lookup_v VALUES (?, ?, {model})", [name, list(override)])

    def answer(self, name, ids):
        self.con.execute("INSERT INTO answers VALUES (?, ?)", [name, list(ids)])

    def run(self):
        sql = (PAYLOAD.replace("{AWARDS}", "awards").replace("{ANSWERS}", "answers")
               .replace("{COUNTRY_LOOKUP}", award_country.lookup_relation_sql(ROOT))
               .replace("{INVESTIGATOR_LINKS}", award_investigators.EMPTY_LINKS))
        sql = self.sqlglot.transpile(sql, read="spark", write="duckdb")[0]
        rows = self.con.execute(f"SELECT id, institution_awarded FROM ({sql})").fetchall()
        return {i: sorted(int(x["id"].rsplit("I", 1)[1]) for x in awarded) for i, awarded in rows}


def test_the_three_headline_wrong_matches_are_rejected_and_the_right_ones_kept():
    f = Fixture()
    f.legacy("FRED HUTCHINSON CANCER CENTER", scored=[(10, 0.9), (11, 0.4)])
    f.legacy("Universidade de São Paulo (USP). Escola Politécnica (EP)", scored=[(21, 0.8), (20, 0.5)])
    f.legacy("CHILD HEALTH AND HUMAN DEVELOPMENT", scored=[(31, 0.7)])
    f.legacy("MAKERERE UNIVERSITY COLLEGE OF HEALTH SCIENCES", scored=[(40, 0.9)])
    hutch = f.award("nih_exporter", lead=("FRED HUTCHINSON CANCER CENTER", "UNITED STATES"))
    usp = f.award("fapesp_bv", lead=("Universidade de São Paulo (USP). Escola Politécnica (EP)", "Brazil"))
    nichd = f.award("nih_exporter", lead=("CHILD HEALTH AND HUMAN DEVELOPMENT", "UNITED STATES"))
    makerere = f.award("nih_exporter", lead=("MAKERERE UNIVERSITY COLLEGE OF HEALTH SCIENCES", "UGANDA"))
    got = f.run()
    assert got[hutch] == [10]          # the Cape Town laboratory (ZA, child of 10) is dropped although its parent is in the US
    assert got[usp] == [20]            # Universidade Politecnica, Mozambique is dropped
    assert got[nichd] == []            # the Cameroon network is dropped; nothing right was offered
    assert got[makerere] == [40]


def test_nih_intramural_award_without_a_country_counts_as_us():
    f = Fixture()
    f.legacy("CHILD HEALTH AND HUMAN DEVELOPMENT", scored=[(31, 0.7), (30, 0.6)])
    intramural = f.award("nih_exporter", lead=("CHILD HEALTH AND HUMAN DEVELOPMENT", None), funder_award_id="1z01hd000001-05")
    intramural_zia = f.award("nih_exporter", lead=("CHILD HEALTH AND HUMAN DEVELOPMENT", ""), funder_award_id="ziahd008751")
    extramural = f.award("nih_exporter", lead=("CHILD HEALTH AND HUMAN DEVELOPMENT", None), funder_award_id="5r01hd000001-05")
    got = f.run()
    assert got[intramural] == [30] and got[intramural_zia] == [30]
    assert got[extramural] == [30, 31]                                   # no country, not intramural: nothing to contradict


def test_unknown_or_unreliable_country_never_rejects():
    f = Fixture()
    f.legacy("University of Waterloo", scored=[(50, 0.9)])
    f.legacy("University of Cape Town", scored=[(51, 0.9)])
    f.legacy("Nowhere Institute", scored=[(61, 0.9)])
    idrc = f.award("idrc_iati", lead=("University of Waterloo", "UG"))          # project country, not the university's
    humboldt = f.award("humboldt", lead=("University of Cape Town", "Germany"))  # constant from the ingest notebook
    junk = f.award("nsf_award_search", lead=("University of Cape Town", "RI REQUIRED"))
    empty = f.award("nsf_award_search", lead=("University of Cape Town", ""))
    no_inst_country = f.award("nih_exporter", lead=("Nowhere Institute", "UNITED STATES"))
    got = f.run()
    assert got[idrc] == [50] and got[humboldt] == [51] and got[junk] == [51] and got[empty] == [51] and got[no_inst_country] == [61]


def test_nsf_place_of_performance_abroad_does_not_remove_the_us_awardee():
    f = Fixture()
    f.answer("Emory University", [80])
    f.answer("Lincoln University", [82, 81])                      # the matcher returns both same-name institutions
    f.answer("Universidad Tecnica Federico Santa Maria", [83, 84])
    fieldwork = f.award("nsf_award_search", lead=("Emory University", "Tanzania"))          # US awardee, field site in Tanzania
    at_home = f.award("nsf_award_search", lead=("Lincoln University", "United States"))     # performed in the US: the NZ twin goes
    abroad = f.award("nsf_award_search", lead=("Universidad Tecnica Federico Santa Maria", "Chile"))
    got = f.run()
    assert got[fieldwork] == [80]
    assert got[at_home] == [82]
    assert got[abroad] == [83, 84]     # a place abroad checks nothing, so the Ecuador twin stays (the price of not trusting it)


def test_territories_count_as_their_sovereign_state():
    f = Fixture()
    f.legacy("UNIVERSITY OF PUERTO RICO MED SCIENCES", scored=[(41, 0.9)])
    f.legacy("UNIVERSITE DE LA REUNION", scored=[(42, 0.9), (51, 0.5)])
    pr = f.award("nih_exporter", lead=("UNIVERSITY OF PUERTO RICO MED SCIENCES", "UNITED STATES"))
    reunion = f.award("erasmus_plus", lead=("UNIVERSITE DE LA REUNION", "FR"))
    got = f.run()
    assert got[pr] == [41] and got[reunion] == [42]


def test_each_role_is_checked_against_its_own_country_and_any_agreeing_name_keeps_the_institution():
    f = Fixture()
    f.legacy("Harvard University", scored=[(52, 0.9)])
    f.legacy("University of Cape Town", scored=[(51, 0.9)])
    f.legacy("UCT / Harvard", scored=[(51, 0.9), (52, 0.9)])
    a = f.award("gates_foundation", lead=("Harvard University", "United States"), co_lead=("University of Cape Town", "South Africa"),
                investigators=[("UCT / Harvard", "Kenya"), ("University of Cape Town", "ZA")])
    b = f.award("gates_foundation", lead=("UCT / Harvard", "Kenya"))
    got = f.run()
    assert got[a] == [51, 52]          # each kept by the role whose country agrees
    assert got[b] == []                # the same name under a Kenyan record keeps neither


def test_nan_scores_do_not_pass_the_threshold():
    f = Fixture()
    f.legacy("東京大学 分子細胞生物学研究所", scored=[(60, float("nan")), (10, float("nan"))])
    f.legacy("Makerere", scored=[(40, 0.31), (51, 0.29)])
    cjk = f.award("naito_foundation_grants", lead=("東京大学 分子細胞生物学研究所", None))
    plain = f.award("gates_foundation", lead=("Makerere", None))
    got = f.run()
    assert got[cjk] == [] and got[plain] == [40]


def test_matcher_answer_wins_over_the_legacy_lookup_and_an_empty_answer_is_final():
    f = Fixture()
    f.legacy("FRED HUTCHINSON CANCER CENTER", scored=[(11, 0.9)])
    f.answer("FRED HUTCHINSON CANCER CENTER", [10])
    f.legacy("Department of Biology", scored=[(52, 0.9)])
    f.answer("Department of Biology", [])
    f.answer("Queen's University", [70])                # the matcher's own wrong twin: the guard still applies
    f.answer("Queen's University at Kingston", [71])
    f.legacy("Only legacy knows", scored=[(40, 0.9)])
    hutch = f.award("nih_exporter", lead=("FRED HUTCHINSON CANCER CENTER", "UNITED STATES"))
    dept = f.award("gates_foundation", lead=("Department of Biology", "United States"))
    twin = f.award("nserc_open_data", lead=("Queen's University", "CANADA"))
    right = f.award("nserc_open_data", lead=("Queen's University at Kingston", "CANADA"))
    legacy_only = f.award("gates_foundation", lead=("Only legacy knows", "Uganda"))
    unknown = f.award("gates_foundation", lead=("Nobody has seen this", "Uganda"))
    got = f.run()
    assert got[hutch] == [10] and got[dept] == [] and got[twin] == [] and got[right] == [71]
    assert got[legacy_only] == [40] and got[unknown] == []


def test_a_match_in_the_wrong_country_moves_to_the_one_institution_of_that_name_in_the_recorded_country():
    f = Fixture()
    f.answer("UNIVERSIDAD TECNICA FEDERICO SANTA MARIA", [84])     # the matcher picked the Ecuador record of a Chilean university
    f.answer("Depto. de Física, Universidad Técnica Federico Santa María - Valparaíso", [84])
    f.answer("UNIVERSIDAD TECNICA DEL NORTE", [84])                # a different university the matcher took for it
    f.answer("Lincoln University", [81])                         # New Zealand, for awards recorded in the US
    f.legacy("Universidade de São Paulo (USP). Escola Politécnica (EP)", scored=[(21, 0.8)])
    chile = f.award("anid_github", lead=("UNIVERSIDAD TECNICA FEDERICO SANTA MARIA", "CHILE"))
    ecuador = f.award("gates_foundation", lead=("UNIVERSIDAD TECNICA FEDERICO SANTA MARIA", "Ecuador"))
    unknown = f.award("gates_foundation", lead=("UNIVERSIDAD TECNICA FEDERICO SANTA MARIA", None))
    unit = f.award("anid_github", lead=("Depto. de Física, Universidad Técnica Federico Santa María - Valparaíso", "CHILE"))
    other = f.award("anid_github", lead=("UNIVERSIDAD TECNICA DEL NORTE", "CHILE"))
    two_in_us = f.award("nih_exporter", lead=("Lincoln University", "UNITED STATES"))
    usp = f.award("fapesp_bv", lead=("Universidade de São Paulo (USP). Escola Politécnica (EP)", "Brazil"))
    got = f.run()
    assert got[chile] == [83] and got[unit] == [83]               # moved to the Chilean record
    assert got[other] == []            # the text does not contain the name: not that university in any country
    assert got[ecuador] == [84] and got[unknown] == [84]          # right country already, or nothing to check
    assert got[two_in_us] == []        # two Lincoln Universities in the US (81's twins 82 and 85): which one is not known
    assert got[usp] == []              # no 'Universidade Politecnica' in Brazil: dropped as before


def spelled(*names):
    """grant_addresses.spelled for each name, as an NIH lead's affiliation."""
    f = Fixture()
    for n in names:
        f.award("nih_exporter", lead=(n, None))
    cte = PAYLOAD[PAYLOAD.index("grant_addresses AS ("):PAYLOAD.index("\n-- The country code recorded next to each grant address")]
    sql = f"WITH {cte.rstrip().rstrip(',')} SELECT name, spelled FROM grant_addresses".replace("{AWARDS}", "awards")
    return dict(f.con.execute(f.sqlglot.transpile(sql, read="spark", write="duckdb")[0]).fetchall())


def test_a_name_in_capitals_is_spelled_as_papers_write_it():
    got = spelled("WASHINGTON UNIVERSITY", "UNIVERSITY OF TEXAS HLTH SCI CTR HOUSTON", "THE UNIVERSITY OF THE WEST INDIES",
                  "UNIVERSIDAD DEL BIO BIO", "QUEEN'S MEDICAL CENTER", "Washington University", "UNISYS", "N/A")
    assert got["WASHINGTON UNIVERSITY"] == "Washington University"
    assert got["UNIVERSITY OF TEXAS HLTH SCI CTR HOUSTON"] == "University of Texas Health Science Center Houston"
    assert got["THE UNIVERSITY OF THE WEST INDIES"] == "The University of the West Indies"     # a small word first keeps its capital
    assert got["UNIVERSIDAD DEL BIO BIO"] == "Universidad del Bio Bio"
    assert got["QUEEN'S MEDICAL CENTER"] == "Queen's Medical Center"
    assert got["Washington University"] is None and got["UNISYS"] is None                    # not in capitals; one word
    assert got[None] is None                                                                  # a placeholder is no name


def test_a_name_in_capitals_answered_empty_takes_the_answer_for_its_spelling():
    f = Fixture()
    f.answer("WASHINGTON UNIVERSITY", [])
    f.answer("Washington University", [99])
    f.answer("UNIVERSITY OF TEXAS HLTH SCI CTR HOUSTON", [])
    f.answer("University of Texas Health Science Center Houston", [100])
    f.answer("HARVARD UNIVERSITY", [52])
    f.answer("Harvard University", [80])
    f.answer("Washington university", [])
    f.answer("QUEENS COLLEGE", [])
    f.answer("Queens College", [70])
    f.answer("THE UNIVERSITY OF THE WEST", [])
    f.legacy("THE UNIVERSITY OF THE WEST", scored=[(52, 0.9)])
    washu = f.award("nih_exporter", lead=("WASHINGTON UNIVERSITY", "UNITED STATES"))
    uth = f.award("nih_exporter", lead=("UNIVERSITY OF TEXAS HLTH SCI CTR HOUSTON", "UNITED STATES"))
    harvard = f.award("nih_exporter", lead=("HARVARD UNIVERSITY", "UNITED STATES"))
    mixed = f.award("nih_exporter", lead=("Washington university", "UNITED STATES"))
    queens = f.award("nih_exporter", lead=("QUEENS COLLEGE", "UNITED STATES"))
    west = f.award("nih_exporter", lead=("THE UNIVERSITY OF THE WEST", "UNITED STATES"))
    no_country = f.award("gates_foundation", lead=("WASHINGTON UNIVERSITY", None))
    got = f.run()
    assert got[washu] == [99] and got[uth] == [100]
    assert got[harvard] == [52]        # the name's own answer wins; its spelling is read only after an empty answer
    assert got[mixed] == []            # not in capitals: the empty answer is final
    assert got[queens] == []           # the spelling's answer is in Bangladesh: the country guard still applies
    assert got[west] == []             # the spelling has no answer yet: still empty, and the legacy lookup is not read
    assert got[no_country] == []       # no recorded country to check the spelling's answer against: not read


def test_excluded_sources_and_the_kaken_route():
    f = Fixture()
    f.legacy("Harvard University", scored=[(52, 0.9)])
    f.legacy("Kyoto somewhere", scored=[(52, 0.2), (10, 0.05)])
    f.con.execute("INSERT INTO kaken_projects_v VALUES ('k1', 'Kyoto somewhere'), ('k2', 'Kyoto somewhere'), ('k3', 'Harvard University')")
    rwjf = f.award("rwjf_grants_explorer", lead=("Harvard University", "MA"))
    kaken = f.award("kaken", lead=("Kyoto somewhere", "Japan"), funder_award_id="k1")      # the lead's name is the host institution
    kaken_no_pi = f.award("kaken", lead=None, funder_award_id="k2")                         # no PI: the scrape's institution
    kaken_coinv = f.award("kaken", investigators=[("Harvard University", None)], funder_award_id="k9")
    kaken_nameless_lead = f.award("kaken", lead=(None, "Japan"), funder_award_id="k3")       # the scrape's name, the lead's country
    got = f.run()
    assert got[rwjf] == []                              # still excluded from matching
    assert got[kaken] == []                             # threshold 0.1 lets 52 through; Japan contradicts US
    assert got[kaken_no_pi] == [52]
    assert got[kaken_coinv] == []                       # KAKEN co-investigators' strings are not read
    assert got[kaken_nameless_lead] == []               # Harvard (US) under a lead recorded in Japan


# ---------- the funder's own ids, and Han names with no recorded country (oxjob #2032) ----------
def test_a_ror_the_funder_gives_on_the_persons_affiliation_links_the_institution():
    f = Fixture()
    url = f.award("nhmrc", lead=("Some Faculty Name We Cannot Match", "Australia", [("https://ror.org/000009400", "ROR")]), funder_id=2)
    bare = f.award("fct", lead=("x", None, [(" 000009500 ", "ror")]))                          # a bare id, lower-case type
    no_scheme = f.award("fct", lead=("x", None, [("ror.org/000009500", "ROR")]))
    http = f.award("fct", lead=("x", None, [("http://ror.org/000009500", "ROR")]))
    on_investigator = f.award("datacite", investigators=[("Lab A", None), ("Lab B", None, [("https://ror.org/000009500", "ROR")])])
    got = f.run()
    assert got[url] == [94] and got[bare] == [95] and got[no_scheme] == [95] and got[http] == [95] and got[on_investigator] == [95]


def test_a_ror_counts_on_a_person_with_no_usable_name():
    f = Fixture()
    f.answer("n/a", [52])
    no_name = f.award("sfi_open_data", lead=(None, None, [("https://ror.org/000009400", "ROR")]))
    placeholder = f.award("sfi_open_data", lead=("n/a", None, [("https://ror.org/000009500", "ROR")]))
    got = f.run()
    assert got[no_name] == [94] and got[placeholder] == [95]          # the placeholder's own answer (52) is not read


def test_only_a_ror_id_counts():
    f = Fixture()
    other_host = f.award("nhmrc", lead=("x", None, [("https://example.org/000009400", "ROR")]))   # ends like a ROR, is not one
    longer = f.award("nhmrc", lead=("x", None, [("https://ror.org/000009400/x", "ROR")]))
    prefixed = f.award("nhmrc", lead=("x", None, [("x000009400", "ROR")]))
    other_type = f.award("nhmrc", lead=("x", None, [("grid.1013.3", "GRID")]))
    unknown_ror = f.award("nhmrc", lead=("x", None, [("https://ror.org/0abcdef12", "ROR")]))          # no institution has it
    got = f.run()
    assert got[other_host] == [] and got[longer] == [] and got[prefixed] == [] and got[other_type] == [] and got[unknown_ror] == []


def test_a_funders_ror_is_added_to_a_matched_name_and_checked_by_the_country_guard():
    f = Fixture()
    f.answer("University of Sydney", [94])
    both = f.award("nhmrc", lead=("University of Sydney", None, [("https://ror.org/000009500", "ROR")]))
    wrong_country = f.award("nhmrc", lead=("University of Sydney", "Australia", [("https://ror.org/000009500", "ROR")]))
    got = f.run()
    assert got[both] == [94, 95]
    assert got[wrong_country] == [94]                   # a Portuguese ROR on a record that says Australia: dropped like any match


def test_skipped_feeds_stay_skipped_for_rors_too():
    f = Fixture()
    skipped = f.award("crossref_work.grants", lead=("x", None, [("https://ror.org/000009400", "ROR")]))
    assert f.run()[skipped] == []


def test_a_han_name_from_a_taiwanese_funder_loses_its_china_and_japan_twins_only():
    f = Fixture()
    f.answer("國立中山大學海洋生物科技暨資源學系(所)", [91, 90])     # the 11-03 sheet's twins: Taiwan's NSYSU and China's SYSU
    f.answer("國立中山大學材料與光電科學學系", [91])
    f.answer("東海大學行政管理暨政策學系", [93])                       # Tunghai matched to Tokai (JP)
    f.answer("雪梨大學", [94])                                         # the University of Sydney, written in Taiwan
    f.answer("香港大學", [96])
    f.answer("Sun Yat-sen University", [91])
    sysu = f.award("most_taiwan", lead=("國立中山大學海洋生物科技暨資源學系(所)", None), funder_id=1)
    sysu_only = f.award("nstc_taiwan", lead=("國立中山大學材料與光電科學學系", None), funder_id=1)
    tokai = f.award("most_taiwan", lead=("東海大學行政管理暨政策學系", None), funder_id=1)
    sydney = f.award("most_taiwan", lead=("雪梨大學", None), funder_id=1)
    hong_kong = f.award("most_taiwan", lead=("香港大學", None), funder_id=1)                  # Hong Kong counts as China
    latin = f.award("most_taiwan", lead=("Sun Yat-sen University", None), funder_id=1)        # Latin script: the rule does not apply
    got = f.run()
    assert got[sysu] == [90] and got[sysu_only] == [] and got[tokai] == [] and got[hong_kong] == []
    assert got[sydney] == [94] and got[latin] == [91]


def test_the_han_check_needs_a_funder_in_china_japan_or_taiwan_and_no_recorded_country():
    f = Fixture()
    f.answer("中央大学", [98, 97])                                     # Chuo (JP) and Taiwan's National Central University
    f.answer("國立中山大學", [91, 90])
    japan = f.award("mhlw_japan", lead=("中央大学", None), funder_id=5)
    recorded = f.award("nih_exporter", lead=("國立中山大學", "CHINA"), funder_id=4)              # a recorded country decides
    us_funder = f.award("nih_exporter", lead=("國立中山大學", None), funder_id=4)
    no_funder_country = f.award("erc", lead=("國立中山大學", None), funder_id=3)
    no_funder = f.award("most_taiwan", lead=("國立中山大學", None))
    got = f.run()
    assert got[japan] == [98] and got[recorded] == [91]
    assert got[us_funder] == [90, 91] and got[no_funder_country] == [90, 91] and got[no_funder] == [90, 91]


def test_grant_addresses_are_the_names_read():
    f = Fixture()
    for name in ("Harvard University", "n/a", "  ", "Institution abroad - Germany", "No institution available"):
        f.answer(name, [52])
    read = f.award("gates_foundation", lead=("Harvard University", None))
    junk = f.award("gates_foundation", lead=("n/a", None), co_lead=("  ", None),
                   investigators=[("Institution abroad - Germany", None), ("No institution available", None)])
    gtr = f.award("gateway_to_research", lead=("Harvard University", None))
    got = f.run()
    assert got[read] == [52] and got[junk] == [] and got[gtr] == []


def test_the_ten_skipped_feeds_are_listed_once():
    assert PAYLOAD.count("'argentina_mincyt', 'openaire_fwf', 'researchfi', 'nobelprize_api'") == 1
