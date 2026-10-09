"""Offline tests for the funding-flows release (oxjob #1570): amount_usd, recipient countries / regions, research scope, the shared
key resolver, hash compatibility, payload and job wiring (no warehouse)."""
import pathlib
import re
import sys
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import award_keys
import award_recipients
import award_relations
import award_research_scope
import award_usd
import create_api


class Frame:
    def __init__(self, rows=(), cols=()):
        self.rows, self.columns = list(rows), list(cols)

    def collect(self):
        return self.rows


import json
BOOTSTRAP = json.loads((ROOT / "config.prod.json").read_text())["bootstrap_counts"]


class FakeC:
    """Yesterday predates the columns unless prev_cols says otherwise; a zero baseline for every bootstrap key (the fake counts
    everything as 0) unless the test passes its own."""
    def __init__(self, extra=(), prev_cols=(), config=None):
        self.r, self.p, self.extra, self.prev_cols = "dev.lab.ngr_s_", "dev.lab.ngr_", set(extra), list(prev_cols)
        self.config = dict(package_root=str(ROOT), bootstrap_counts={k: 0 for k in BOOTSTRAP})
        self.config.update(config or {})
        self.artifacts, self.checks, self.counts, self.statements = [], {}, {}, []
        self.outputs = {}

    def guard(self): pass
    def has_extra(self, name): return name in self.extra
    def require(self, cond, name):
        if not cond: raise RuntimeError(name)

    def sql(self, statement, params=None):
        self.statements.append(statement)
        if "LIMIT 0" in statement:
            return Frame(cols=self.prev_cols)
        if statement.startswith("SELECT count(*) n"):
            return Frame([types.SimpleNamespace(n=0)])
        return Frame()

    def artifact(self, name, query, params=None):
        self.artifacts.append((name, query)); return self.r + name

    def zero(self, name, query):
        self.checks[name] = True; self.statements.append(query)

    def count(self, name, relation):
        self.counts[name] = 0; return 0


# ---------- shared key resolver ----------
def test_relations_use_the_shared_resolver_with_their_own_gate_names():
    c = FakeC(extra=["award_relations_raw"])
    award_relations.run(c)
    for gate in ("RELATION_BRIDGE_UNIQUE", "RELATION_LOOKUP_UNIQUE", "RELATION_NATIVE_STAGING_DISAGREE"):
        assert gate in c.checks
    q = dict(c.artifacts)["relation_endpoint_lookup"]
    assert "kn.key_type='NATIVE'" in q and "ks.key_type='STAGING'" in q and "resolve_candidate" in q


def test_recipients_resolve_through_the_same_lookup():
    c = FakeC(extra=["award_recipients_raw"])
    award_recipients.run(c)
    q = dict(c.artifacts)["recipient_endpoint_lookup"]
    assert "kn.key_type='NATIVE'" in q and "ks.key_type='STAGING'" in q
    for gate in ("RECIPIENT_RAW_SHAPE", "RECIPIENT_RAW_UNIQUE", "RECIPIENT_BRIDGE_UNIQUE", "RECIPIENT_LOOKUP_UNIQUE",
                 "RECIPIENT_NATIVE_STAGING_DISAGREE", "RECIPIENT_NO_LOSS", "RECIPIENT_DROP_FUSE", "RECIPIENT_SELECTED_UNIQUE",
                 "RECIPIENT_AWARDS_SAME_ROWS"):
        assert gate in c.checks, gate


# ---------- recipients ----------
def test_recipients_no_input_before_first_rows_runs_on_empty_relation():
    c = FakeC(prev_cols=["id", "display_name"])
    award_recipients.run(c)
    names = [n for n, _ in c.artifacts]
    assert names[0] == "recipients_raw_empty" and "RECIPIENT_INPUT_REQUIRED" not in c.checks
    assert names[-1] == "awards_candidate"


def test_recipients_no_input_after_rows_exist_is_checked():
    c = FakeC(prev_cols=["id", "recipient_countries"])
    award_recipients.run(c)
    assert "RECIPIENT_INPUT_REQUIRED" in c.checks and "RECIPIENT_COUNT_DROP" in c.checks


def test_recipients_best_basis_wins_and_shares_split_equally_when_missing():
    c = FakeC(extra=["award_recipients_raw"])
    award_recipients.run(c)
    q = dict(c.artifacts)["recipients_selected"]
    assert "WHEN 'funder_reported' THEN 0 WHEN 'place_of_performance' THEN 1 WHEN 'organisation_location' THEN 2" in q
    assert "min(basis_rank) best_rank" in q and "CASE WHEN any_missing THEN 1.0/n_rows ELSE share END" in q


def test_recipients_projection_adds_four_columns_only_and_uses_amount_usd():
    c = FakeC(extra=["award_recipients_raw"])
    award_recipients.run(c)
    q = dict(c.artifacts)["awards_candidate"]
    for col in ("recipient_countries", "recipient_regions", "recipient_countries_full", "recipient_regions_full"):
        assert f"coalesce(p.{col.replace('_full', '')},array()) {col}" in q
    assert "FROM dev.lab.ngr_s_awards_candidate_before_recipients a" in q
    arrays = dict(c.artifacts)["recipient_arrays"]
    assert "round(a.amount_usd*s.share,2)" in arrays and "'oecd_dac_region'" in arrays
    assert not any("work_awards" in n or "institution_awarded" in q for n, q in c.artifacts)


def test_dac_region_file_is_valid():
    rows = award_recipients.region_names(str(ROOT))
    codes = {r["code"]: r["name"] for r in rows}
    assert codes["289"].startswith("South of Sahara") and codes["298"].startswith("Africa")
    assert "VALUES ('88'," in award_recipients.region_relation_sql(str(ROOT))


# ---------- amount_usd ----------
def test_usd_no_input_runs_on_empty_rates_and_is_checked_once_published():
    c = FakeC(prev_cols=["id"])
    award_usd.run(c)
    names = [n for n, _ in c.artifacts]
    assert "rates_daily_empty" in names and "rates_annual_empty" in names and "USD_INPUT_REQUIRED" not in c.checks
    c = FakeC(prev_cols=["id", "amount_usd"])
    award_usd.run(c)
    assert "USD_INPUT_REQUIRED" in c.checks and "USD_COVERAGE_DROP" in c.checks


def test_usd_rules_start_date_rate_then_annual_then_null_and_amount_untouched():
    c = FakeC(extra=["exchange_rates", "exchange_rates_annual"])
    award_usd.run(c)
    q = dict(c.artifacts)["awards_candidate"]
    assert "WHEN a.currency='USD' THEN CAST(a.amount AS DOUBLE)" in q
    assert "WHEN d.usd_per_unit IS NOT NULL THEN round(a.amount*d.usd_per_unit,2)" in q
    assert "WHEN y.usd_per_unit IS NOT NULL THEN round(a.amount*y.usd_per_unit,2)" in q
    assert "WHEN false OR a.amount IS NULL OR a.currency IS NULL THEN CAST(NULL AS DOUBLE)" in q
    assert "d.rate_date=CAST(a.start_date AS DATE)" in q and "y.rate_year=CAST(a.start_year AS INT)" in q
    assert "SELECT a.*," in q and "amount AS" not in q.replace("CAST(a.amount AS DOUBLE)", "")   # the native amount is passed through
    filled = dict(c.artifacts)["rates_daily_filled"]
    assert "last_value(usd_per_unit,true)" in filled and f"<= {award_usd.MAX_CARRY_DAYS}" in filled
    for gate in ("RATES_DAILY_SHAPE", "RATES_DAILY_UNIQUE", "RATES_ANNUAL_SHAPE", "RATES_ANNUAL_UNIQUE", "USD_AWARDS_SAME_ROWS", "USD_BASIS_SHAPE"):
        assert gate in c.checks, gate


# ---------- research scope ----------
def test_research_reference_files_are_valid_and_cover_the_funder_feeds():
    defaults = award_research_scope.read_defaults(str(ROOT))
    rules = award_research_scope.read_rules(str(ROOT))
    by = {r["provenance"]: r for r in defaults}
    for stub in ("crossref_work_funders", "europepmc_work_funders", "crossref_work.grants", "datacite_work_funders", "gtr_legacy"):
        assert stub not in by                                            # shell awards get no research_scope and do not move
    assert by["nih_exporter"]["is_research"] == "true" and by["usaspending_usda"]["is_research"] == "mixed"
    assert by["iati_sida"]["basis"] == "sector_code"
    assert rules and all(r["is_research"] in ("true", "false", "unknown") for r in rules)


@pytest.mark.parametrize("provenance,values,expected", [
    ("nih_exporter", {"funder_scheme": "R01"}, (True, "funder_default", 1)),
    ("usaspending_usda", {"funder_scheme": "NATIONAL SCHOOL LUNCH PROGRAM"}, (False, "programme_rule", None)),
    ("usaspending_usda", {"funder_scheme": "ANIMAL HEALTH AND DISEASE RESEARCH"}, (True, "programme_rule", 2)),
    ("usaspending", {"funder_scheme": "NATIONAL GUARD MILITARY OPERATIONS AND MAINTENANCE (O&M) PROJECTS"}, (False, "programme_rule", None)),
    ("usaspending_usaid", {"funder_scheme": "USAID FOREIGN ASSISTANCE FOR PROGRAMS OVERSEAS"}, (None, "unknown", None)),
    ("gates_foundation", {"funder_scheme": "Global Development|Global Health"}, (True, "programme_rule", 2)),
    ("gates_foundation", {"funder_scheme": "U.S. Program"}, (None, "unknown", None)),
    ("hrsa_data_warehouse", {"funder_scheme": "Health Center Program (H80)"}, (False, "funder_default", None)),
    ("neh_open_data", {"funder_scheme": "Institutes for K-12 Educators"}, (False, "programme_rule", None)),
    ("iati_fcdo", {}, (True, "sector_code", 2)),
    ("nih_exporter", {"funder_scheme": "B09", "funder_name": "Substance Abuse and Mental Health Services Administration"}, (False, "programme_rule", None)),
    ("nih_exporter", {"funder_scheme": "A11", "funder_name": "Health Resources and Services Administration"}, (False, "programme_rule", None)),
    ("nih_exporter", {"funder_scheme": "U2G", "funder_name": "Centers for Disease Control and Prevention"}, (None, "unknown", None)),
    ("nih_exporter", {"funder_scheme": "U01", "funder_name": "National Institutes of Health"}, (True, "funder_default", 1)),
    ("nih_exporter", {"funder_scheme": "R01", "funder_name": "Centers for Disease Control and Prevention"}, (True, "programme_rule", 2)),
    ("nih_exporter", {"funder_scheme": "I01", "funder_name": "U.S. Department of Veterans Affairs"}, (True, "programme_rule", 2)),
    ("novo_nordisk_fonden_grants", {"funder_scheme": "Humanitære og sociale formål"}, (False, "programme_rule", None)),
    ("novo_nordisk_fonden_grants", {"funder_scheme": "Pioneer Innovator"}, (True, "programme_rule", 2)),
    ("novo_nordisk_fonden_grants", {"funder_scheme": "Enkeltsatsninger", "display_name": "Emergency drought relief in Ethiopia"}, (False, "programme_rule", None)),
    ("novo_nordisk_fonden_grants", {"funder_scheme": "Enkeltsatsninger", "display_name": "A centre"}, (None, "unknown", None)),
    ("nih_exporter", {"funder_scheme": "VF1", "funder_name": "Centers for Disease Control and Prevention"}, (False, "programme_rule", None)),
    ("nih_exporter", {"funder_scheme": "E10", "funder_name": "Health Resources and Services Administration"}, (False, "programme_rule", None)),
    ("nih_exporter", {"funder_scheme": "KD1", "funder_name": "Substance Abuse and Mental Health Services Administration"}, (False, "programme_rule", None)),
    ("nih_exporter", {"funder_scheme": None, "funder_name": "Administration for Children and Families"}, (None, "unknown", None)),
    ("nih_exporter", {"funder_scheme": "F19", "funder_name": "Indian Health Service"}, (True, "programme_rule", 2)),
    ("nih_exporter", {"funder_scheme": "R01", "funder_name": "Agency for Healthcare Research and Quality"}, (True, "funder_default", 1)),
])
def test_research_reference_answers(provenance, values, expected):
    d, r = award_research_scope.read_defaults(str(ROOT)), award_research_scope.read_rules(str(ROOT))
    got = award_research_scope.scope_in_python(d, r, provenance, values)
    assert (got["is_research"], got["basis"], got["tier"]) == expected


@pytest.mark.parametrize("provenance,values,types,works,code,expected", [
    ("gates_foundation", {"funder_scheme": "Global Development"}, ["government"], [3], False, (None, "unknown", None)),   # mixed feed: rules first
    ("wellcome_trust", {}, ["government"], [3], False, (None, "recipient_type", None)),       # research funder, grant to a city: Kyle's park
    ("wellcome_trust", {}, ["nonprofit"], [19705], False, (True, "funder_default", 1)),     # Wellcome Sanger Institute: a publisher of research
    ("wellcome_trust", {}, ["education"], [0], False, (True, "funder_default", 1)),         # to a university
    ("wellcome_trust", {}, ["healthcare", "education"], [10, 0], False, (True, "funder_default", 1)),
    ("wellcome_trust", {}, [], [], False, (True, "funder_default", 1)),                     # no matched recipient: cannot be tested
    ("nih_exporter", {"funder_scheme": "R01", "funder_name": "National Institutes of Health"}, ["company"], [0], False, (True, "funder_default", 1)),   # agency exempt
    ("idrc_iati", {}, ["nonprofit"], [2], True, (True, "sector_code", 2)),                  # IDRC research purpose code: about research
    ("idrc_iati", {}, ["nonprofit"], [2], False, (None, "recipient_type", None)),
    ("novo_nordisk_fonden_grants", {"funder_scheme": "Pioneer Innovator"}, ["company"], [0], False, (True, "programme_rule", 2)),   # programme says research
    ("wellcome_trust", {"display_name": "Preclinical development of a malaria drug candidate"}, ["company"], [9], False, (True, "programme_rule", 4)),   # title says research
    ("wellcome_trust", {"display_name": "A festival of music and the brain"}, ["nonprofit"], [10], False, (None, "recipient_type", None)),
])
def test_recipient_test_and_sector_codes(provenance, values, types, works, code, expected):
    d, r = award_research_scope.read_defaults(str(ROOT)), award_research_scope.read_rules(str(ROOT))
    got = award_research_scope.scope_in_python(d, r, provenance, values, recipient_types=types, research_code=code, recipient_works=works)
    assert (got["is_research"], got["basis"], got["tier"]) == expected


def test_feed_defaults_exempt_research_agencies_and_test_foundations():
    by = {r["provenance"]: r for r in award_research_scope.read_defaults(str(ROOT))}
    for p in ("nih_exporter", "nsf_award_search", "kaken", "gepris", "erc", "cihr_opendata", "foerderkatalog", "nsfc_kd", "anr_opendata", "nhmrc"):
        assert by[p]["recipient_test"] == "no", p
    for p in ("gates_foundation", "wellcome_trust", "idrc_iati", "iati_fcdo", "hewlett_facetwp", "templeton_wp", "sloan_foundation", "vivensa"):
        assert by[p]["recipient_test"] == "yes", p
    assert by["edctp_grants_portal"]["recipient_test"] == "no"                       # Codex 10-09: EDCTP funds research only
    assert all(i.startswith("I") for i in award_research_scope.read_research_organisations(str(ROOT)))
    assert award_research_scope.read_research_codes(str(ROOT))[:2] == ["11182", "12182"]


def test_research_stub_feeds_get_no_scope():
    d, r = award_research_scope.read_defaults(str(ROOT)), award_research_scope.read_rules(str(ROOT))
    assert award_research_scope.scope_in_python(d, r, "crossref_work_funders", {}) is None


def test_research_sql_applies_rules_first_then_defaults_and_leaves_stubs_null():
    c = FakeC()
    award_research_scope.run(c)
    q = dict(c.artifacts)["awards_candidate"]
    assert (q.index("WHEN h.award_id IS NOT NULL") < q.index("WHEN s.award_id IS NOT NULL") < q.index("WHEN d.is_research='mixed'")
            < q.index("d.recipient_test='yes'") < q.index("WHEN d.provenance IS NOT NULL"))
    assert "rr.award_id IS NULL" in q and "'basis','recipient_type'" in q
    assert "x.short_id IN (" in dict(c.artifacts)["research_recipient_awards"]
    for gate in ("CLASSIFICATION_NO_LOSS", "CLASSIFICATION_DROP_FUSE"):
        assert gate in c.checks, gate
    rr = dict(c.artifacts)["research_recipient_awards"]
    assert "x.type IN ('education','facility') OR coalesce(i.works_count,0) >= 50" in rr and "institutions_api_v" in rr
    for gate in ("CLASSIFICATION_RAW_SHAPE", "CLASSIFICATION_RAW_UNIQUE", "CLASSIFICATION_BRIDGE_UNIQUE"):
        assert gate in c.checks, gate
    assert "'basis','unknown'" in q and "LEFT JOIN dev.lab.ngr_s_research_feed_defaults d ON d.provenance=a.provenance" in q
    hits = dict(c.artifacts)["research_rule_hits"]
    assert "min_by(" in hits and "concat('(?i)',p.pattern)" in hits and "a.funder.display_name RLIKE concat('(?i)',p.funder_scope)" in hits
    assert "RESEARCH_AWARDS_SAME_ROWS" in c.checks


def test_research_invalid_rows_are_refused(tmp_path):
    (tmp_path / "research").mkdir()
    (tmp_path / "research" / "feed_defaults.csv").write_text("provenance,is_research,basis,tier,recipient_test,note\nx,maybe,funder_default,1,no,\n")
    (tmp_path / "research" / "programme_rules.csv").write_text("rule_id,provenance_scope,funder_scope,field,pattern,is_research,tier,note\nr,*,*,funder_scheme,(,true,2,\n")
    with pytest.raises(RuntimeError, match="RESEARCH_DEFAULT_ROW_INVALID"):
        award_research_scope.read_defaults(str(tmp_path))
    with pytest.raises(RuntimeError, match="RESEARCH_RULE_PATTERN_INVALID"):
        award_research_scope.read_rules(str(tmp_path))


# ---------- hash compatibility ----------
BASE = (ROOT / "sql/api_hash_expression.sql").read_text().strip()
LINKS = f"xxhash64(concat_ws('|',CAST({BASE} AS STRING),to_json(parent_awards_full),to_json(sub_awards_full)))"
NO_LINKS = "coalesce(size(parent_awards_full),0)=0 AND coalesce(size(sub_awards_full),0)=0"


def test_hash_without_any_optional_column_is_the_base():
    assert create_api.hash_expression(BASE, {"id", "display_name"}) == BASE


def test_hash_with_links_only_is_unchanged_from_the_sub_awards_release():
    assert create_api.hash_expression(BASE, {"id", "parent_awards_full", "sub_awards_full"}) == f"CASE WHEN {NO_LINKS} THEN {BASE} ELSE {LINKS} END"


def test_hash_with_flow_columns_keeps_both_older_forms_for_awards_without_flow_data():
    cols = {"id", "parent_awards_full", "sub_awards_full", *create_api.FLOW_COLUMNS}
    e = create_api.hash_expression(BASE, cols)
    present = "amount_usd IS NOT NULL OR size(recipient_countries_full)>0 OR size(recipient_regions_full)>0 OR research_scope IS NOT NULL"
    assert e.startswith(f"CASE WHEN {NO_LINKS} AND NOT ({present}) THEN {BASE} WHEN NOT ({present}) THEN {LINKS} ELSE xxhash64(concat_ws('|',")
    assert "CAST(amount_usd AS STRING),amount_usd_basis,to_json(recipient_countries_full),to_json(recipient_regions_full),to_json(research_scope)))" in e


def test_hash_with_flow_columns_but_no_links_in_yesterdays_table():
    e = create_api.hash_expression(BASE, {"id", "amount_usd", "amount_usd_basis"})
    assert e.startswith(f"CASE WHEN true AND NOT (amount_usd IS NOT NULL) THEN {BASE} WHEN NOT (amount_usd IS NOT NULL) THEN {BASE} ELSE")
    assert "'',''," in e and ",'','','')" in e                            # absent columns contribute empty strings


def test_create_api_hashes_yesterday_with_its_own_columns_and_tonight_with_all():
    c = FakeC(prev_cols=["id", "parent_awards_full", "sub_awards_full"])
    c.sql = lambda st, params=None: Frame(cols=c.prev_cols) if "LIMIT 0" in st else Frame()
    create_api.run(c)
    a = dict(c.artifacts)
    assert a["previous_api_hash"] == f"SELECT id,updated_date,CASE WHEN {NO_LINKS} THEN {BASE} ELSE {LINKS} END content_hash FROM previous_api_v"
    assert "to_json(research_scope)" in a["new_api_hash"] and a["new_api_hash"].startswith("SELECT id,CASE WHEN")


# ---------- wiring ----------
def test_job_runs_usd_and_recipients_between_relations_and_api_and_research_inside_api():
    text = (ROOT / "NightlyGrants.py").read_text()
    assert (text.index("award_relations.run(c)") < text.index("award_usd.run(c)") < text.index("award_recipients.run(c)")
            < text.index("create_api.run(c)") < text.index('config.get("stop_before_apply")'))
    assert "award_research_scope" not in text
    api = (ROOT / "lib/create_api.py").read_text()
    assert api.index("c.artifact('api_payload'") < api.index("award_research_scope.run(c,'api_payload')") < api.index("api_hash_expression.sql")


def test_payload_passes_flow_columns():
    sql = (ROOT / "sql/api_payload.sql").read_text()
    for col in ("amount_usd", "amount_usd_basis", "recipient_countries", "recipient_regions", "recipient_countries_full",
                "recipient_regions_full"):
        assert re.search(rf"\boa\.{col}\b", sql), col
    assert "oa.research_scope" not in sql                                       # added after the payload, from the matched institutions
    assert "oa.amount," in sql and "oa.currency," in sql                       # native amount still served


def test_config_binds_the_three_inputs_and_runtime_allows_them():
    import json
    cfg = json.loads((ROOT / "config.prod.json").read_text())
    assert cfg["extra_inputs"]["award_recipients_raw"] == "openalex.awards.award_recipients_raw"
    assert cfg["extra_inputs"]["exchange_rates"] == "openalex.common.exchange_rates"
    assert cfg["extra_inputs"]["exchange_rates_annual"] == "openalex.common.exchange_rates_annual"
    assert cfg["extra_inputs"]["award_classifications_raw"] == "openalex.awards.award_classifications_raw"
    runtime = (ROOT / "lib/nightly_runtime.py").read_text()
    for name in ("award_recipients_raw", "award_classifications_raw", "exchange_rates", "exchange_rates_annual"):
        assert f'"{name}"' in runtime[runtime.index("UNKNOWN_EXTRA_INPUT") - 400:runtime.index("UNKNOWN_EXTRA_INPUT")]


# ---------- loss gates and rollback (Codex review 10-07, findings 1 and 2) ----------
def test_configured_but_empty_inputs_are_refused():
    c = FakeC(extra=["exchange_rates", "exchange_rates_annual", "award_recipients_raw"])
    award_usd.run(c); award_recipients.run(c)
    for gate in ("RATES_DAILY_NONEMPTY", "RATES_ANNUAL_NONEMPTY", "RECIPIENT_RAW_NONEMPTY"):
        assert gate in c.checks, gate
        assert any("count(*) FROM" in q and ")=0" in q for q in c.statements)


def test_usd_loss_gates_cover_basis_and_currency_not_only_the_total():
    class C(FakeC):
        def sql(self, st, params=None):
            self.statements.append(st)
            if "LIMIT 0" in st: return Frame(cols=self.prev_cols)
            if st.startswith("SELECT count(*) n"): return Frame([types.SimpleNamespace(n=0)])
            if "amount_usd_basis basis" in st and "previous_api_v" in st:
                return Frame([types.SimpleNamespace(basis="daily_rate", n=1000)])
            return Frame()
    c = C(extra=["exchange_rates", "exchange_rates_annual"], prev_cols=["id", "amount_usd", "amount_usd_basis"])
    with pytest.raises(RuntimeError, match="USD_BASIS_DROP_daily_rate: 1000 -> 0"):   # tonight converted nothing by daily rate
        award_usd.run(c)
    c = FakeC(extra=["exchange_rates", "exchange_rates_annual"], prev_cols=["id", "amount_usd", "amount_usd_basis"])
    award_usd.run(c)
    assert any(k.startswith("USD_CURRENCY_DROP_") for k in c.checks) and "USD_COVERAGE_DROP" in c.checks


def test_recipient_loss_gates_cover_rows_per_source_regions_and_memberships():
    class C(FakeC):
        def sql(self, st, params=None):
            self.statements.append(st)
            if "LIMIT 0" in st: return Frame(cols=self.prev_cols)
            if st.startswith("SELECT count(*) n"): return Frame([types.SimpleNamespace(n=0)])
            if "award_nightly_runs" in st:
                import json
                return Frame([types.SimpleNamespace(details_json=json.dumps({"counts": {"recipients_rows_idrc_iati_region": 4031}}))])
            if "count_if(size(recipient_countries)>0) countries" in st:
                return Frame([types.SimpleNamespace(countries=0, regions=0, members=0)])
            return Frame()
    c = C(extra=["award_recipients_raw"], prev_cols=["id", "recipient_countries", "recipient_regions"], config=dict(bootstrap_counts={})); c.p = "dev.lab.ngr_"
    with pytest.raises(RuntimeError, match="RECIPIENT_ROWS_DROP_idrc_iati_region: 4031 -> 0"):      # last night's recorded count, no bootstrap needed
        award_recipients.run(c)
    c = FakeC(extra=["award_recipients_raw"], prev_cols=["id", "recipient_countries", "recipient_regions"])
    award_recipients.run(c)
    for gate in ("RECIPIENT_COUNT_DROP", "RECIPIENT_REGIONS_DROP", "RECIPIENT_MEMBERSHIP_DROP"):
        assert gate in c.checks, gate


def test_research_loss_gate_covers_the_true_count():
    c = FakeC(prev_cols=["id", "research_scope"])
    award_research_scope.run(c)
    assert "RESEARCH_SCOPE_DROP" in c.checks and "RESEARCH_TRUE_DROP" in c.checks
    c = FakeC(extra=["award_classifications_raw"], prev_cols=["id", "research_scope"])
    award_research_scope.run(c)
    assert "CLASSIFICATION_RAW_NONEMPTY" in c.checks and any("award_classifications_raw_v" in q for _, q in c.artifacts)


def test_clear_funding_flows_empties_every_field_skips_loss_gates_and_restamps():
    cfg = dict(clear_funding_flows=True)
    c = FakeC(extra=["exchange_rates", "exchange_rates_annual", "award_recipients_raw"], prev_cols=["id", "amount_usd", "recipient_countries", "research_scope"], config=cfg)
    award_usd.run(c); award_recipients.run(c); award_research_scope.run(c)
    names = [n for n, _ in c.artifacts]
    assert "rates_daily_empty" in names and "recipients_raw_empty" in names
    assert "WHERE false" in dict(c.artifacts)["research_feed_defaults"]
    assert "CASE WHEN true OR a.amount IS NULL" in dict(c.artifacts)["awards_candidate"] or any("CASE WHEN true OR a.amount IS NULL" in q for _, q in c.artifacts)
    for gate in ("USD_INPUT_REQUIRED", "USD_COVERAGE_DROP", "RECIPIENT_INPUT_REQUIRED", "RECIPIENT_COUNT_DROP", "RESEARCH_SCOPE_DROP"):
        assert gate not in c.checks, gate
    # the hash of a cleared award is the old expression, which differs from yesterday's flow-aware hash: it is restamped
    cols = {"id", "parent_awards_full", "sub_awards_full", *create_api.FLOW_COLUMNS}
    e = create_api.hash_expression(BASE, cols)
    assert e.startswith(f"CASE WHEN {NO_LINKS} AND NOT (amount_usd IS NOT NULL")



# ---------- first-night baselines (Codex re-check 10-07, finding 1) ----------
def test_gates_fail_closed_without_any_baseline():
    none = dict(bootstrap_counts={})
    c = FakeC(extra=["award_recipients_raw"], prev_cols=["id"], config=none)   # yesterday predates the columns, no run has recorded counts
    with pytest.raises(RuntimeError, match="RECIPIENT_ROWS_BASELINE_REQUIRED"):
        award_recipients.run(c)
    c = FakeC(extra=["exchange_rates", "exchange_rates_annual"], prev_cols=["id"], config=none)
    with pytest.raises(RuntimeError, match="USD_COVERAGE_DROP_BASELINE_REQUIRED"):
        award_usd.run(c)
    c = FakeC(prev_cols=["id"], config=none)
    with pytest.raises(RuntimeError, match="RESEARCH_SCOPE_DROP_BASELINE_REQUIRED"):
        award_research_scope.run(c)


def test_bootstrap_counts_carry_the_first_night_and_catch_a_loss():
    import json
    boot = json.loads((ROOT / "config.prod.json").read_text())["bootstrap_counts"]
    for key in ("awards_with_amount_usd", "amount_usd_daily_rate", "recipients_rows_idrc_iati_region", "awards_with_recipient_regions",
                "recipient_country_memberships", "awards_with_research_scope", "research_true"):
        assert boot[key] > 0, key
    c = FakeC(extra=["award_recipients_raw"], prev_cols=["id"], config=dict(bootstrap_counts=dict(boot)))
    with pytest.raises(RuntimeError, match="RECIPIENT_ROWS_DROP_idrc_iati_country: 5649 -> 0"):   # tonight's side table is empty in the fake
        award_recipients.run(c)
    c = FakeC(extra=["exchange_rates", "exchange_rates_annual"], prev_cols=["id"], config=dict(bootstrap_counts=dict(boot)))
    with pytest.raises(RuntimeError, match="USD_COVERAGE_DROP: 6380945 -> 0"):
        award_usd.run(c)


def test_clearing_skips_baselines_too():
    c = FakeC(extra=["award_recipients_raw"], prev_cols=["id"], config=dict(clear_funding_flows=True))
    award_recipients.run(c); award_usd.run(c); award_research_scope.run(c)
    assert not any(k.endswith("BASELINE_REQUIRED") for k in c.checks)



# ---------- per-currency gate on the first night (Codex round 3, finding 2) ----------
def test_a_whole_currency_cannot_vanish_on_the_first_night():
    import json
    boot = json.loads((ROOT / "config.prod.json").read_text())["bootstrap_counts"]
    assert boot["amount_usd_currency_DKK"] == 25948

    class C(FakeC):
        def sql(self, st, params=None):
            self.statements.append(st)
            if "LIMIT 0" in st: return Frame(cols=self.prev_cols)
            if st.startswith("SELECT count(*) n"): return Frame([types.SimpleNamespace(n=0)])
            if "amount_usd_basis basis,count(*) n" in st:
                return Frame([types.SimpleNamespace(basis=b, n=boot["amount_usd_" + b]) for b in award_usd.BASES])
            if "SELECT currency,count(*) n FROM" in st:          # tonight: every currency but DKK converted as before
                return Frame([types.SimpleNamespace(currency=k[len("amount_usd_currency_"):], n=v) for k, v in boot.items()
                              if k.startswith("amount_usd_currency_") and not k.endswith("_DKK")])
            return Frame()
    c = C(extra=["exchange_rates", "exchange_rates_annual"], prev_cols=["id"], config=dict(bootstrap_counts=dict(boot)))
    c.count = lambda name, relation: c.counts.setdefault(name, boot.get(name, 0)) or boot.get(name, 0)
    with pytest.raises(RuntimeError, match="USD_CURRENCY_DROP_DKK: 25948 -> 0"):
        award_usd.run(c)
