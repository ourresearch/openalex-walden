"""Every array the API hashes has one order whatever order its input rows arrive in (pa1014-det, Rohan + Casey 10-15: no run-to-run
variation). The content hash (create_api.py) covers institution_awarded and the recipient arrays, so an order that follows the
shuffle re-stamps updated_date for nothing. Pure SQL in DuckDB via sqlglot, as test_award_country.py; skipped without them.
The negative controls run the pre-fix SQL on the same two input orders and show it does differ."""
import pathlib
import random
import re
import sys

import pytest

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(HERE))

import award_country
import award_recipients
import test_award_country as tac
import test_award_flows as taf

PAYLOAD = (ROOT / "sql/api_payload.sql").read_text()
# the pre-fix form of the one array the API built unsorted (sql/api_payload.sql before pa1014-det)
UNSORTED_PAYLOAD = re.sub(r"ARRAY_SORT\(COLLECT_LIST\(STRUCT\(\s*CONCAT\('https://openalex.org/I'(.*?)\)\)\) AS institution_awarded",
                          r"COLLECT_LIST(STRUCT(CONCAT('https://openalex.org/I'\1)) AS institution_awarded", PAYLOAD, flags=re.S)
FLOW_COLUMNS = ("amount_usd DOUBLE", "amount_usd_basis VARCHAR", "recipient_countries VARCHAR[]", "recipient_regions VARCHAR[]",
                "recipient_countries_full VARCHAR[]", "recipient_regions_full VARCHAR[]", "recipients VARCHAR[]", "recipients_full VARCHAR[]")


def payload_fixture(order_seed):
    """Six awards, each matched to several institutions through every route (matcher answer, override, model scores, several
    names on one award); every input relation is inserted in an order drawn from order_seed."""
    f = tac.Fixture()
    f.con.execute("SET threads=1")
    cols = {r[0] for r in f.con.execute("DESCRIBE awards").fetchall()}
    for col in FLOW_COLUMNS:                    # older fixture (before 2bf0cf47) lacks the funding-flows columns
        if col.split()[0] not in cols:
            f.con.execute(f"ALTER TABLE awards ADD COLUMN {col}")
    rng = random.Random(order_seed)
    insts = list(f.con.execute("SELECT * FROM institutions_api_v").fetchall())
    rng.shuffle(insts)
    f.con.execute("DELETE FROM institutions_api_v")
    for row in insts:
        f.con.execute("INSERT INTO institutions_api_v VALUES (?, ?, ?, ?, ?, ?)", list(row))
    us = [52, 80, 82, 10]
    legacy = [("LEGACY A", [(i, 0.9) for i in us]), ("LEGACY B", [(i, 0.8) for i in (50, 71, 51)]),
              ("LEGACY C", [(i, 0.7) for i in (40, 41, 42, 60, 61)])]
    answers = [("ANSWERED D", [83, 84, 81, 70]), ("ANSWERED E", [52, 10, 80])]
    rng.shuffle(legacy); rng.shuffle(answers)
    for name, scored in legacy:
        scored = list(scored); rng.shuffle(scored)
        f.legacy(name, scored=scored)
    for name, ids in answers:
        ids = list(ids); rng.shuffle(ids)
        f.answer(name, ids)
    awards = [("nih_exporter", ("LEGACY A", None), None, [("ANSWERED E", None)]),
              ("cordis", ("LEGACY B", None), ("LEGACY C", None), []),
              ("cordis", ("LEGACY C", None), None, [("ANSWERED D", None), ("LEGACY A", None)]),
              ("cordis", ("ANSWERED D", None), None, []),
              ("cordis", ("ANSWERED E", None), ("LEGACY A", None), [("LEGACY B", None), ("LEGACY C", None)]),
              ("cordis", ("LEGACY A", None), None, [])]
    idx = list(range(len(awards))); rng.shuffle(idx)
    ids = {}
    for k in idx:                               # award ids are fixed by k, the insert order is not
        prov, lead, co, inv = awards[k]
        f.next_id = k
        ids[k] = f.award(prov, lead=lead, co_lead=co, investigators=inv)
    return f


def institution_arrays(f, payload):
    sql = (payload.replace("{AWARDS}", "awards").replace("{ANSWERS}", "answers")
           .replace("{COUNTRY_LOOKUP}", award_country.lookup_relation_sql(ROOT)))
    sql = f.sqlglot.transpile(sql, read="spark", write="duckdb")[0]
    rows = f.con.execute(f"SELECT id, institution_awarded, institution_awarded_full FROM ({sql}) ORDER BY id").fetchall()
    return [(i, a, b) for i, a, b in rows]                     # whole elements, every field in order


def test_institution_awarded_is_identical_whatever_the_input_order():
    runs = [institution_arrays(payload_fixture(seed), PAYLOAD) for seed in range(8)]
    assert all(len(ids) >= 2 for _, ids, _ in runs[0][:5])          # the fixture really has multi-institution awards
    assert all(r == runs[0] for r in runs)
    for _, a, b in runs[0]:
        assert [x["id"] for x in a] == sorted(x["id"] for x in a) and a == b
        assert all(list(x) == ["id", "display_name", "ror", "country_code", "type", "lineage"] for x in a)   # schema unchanged


def test_negative_control_the_unsorted_collect_list_follows_the_input_order():
    assert UNSORTED_PAYLOAD != PAYLOAD and "ARRAY_SORT(COLLECT_LIST(STRUCT(" not in UNSORTED_PAYLOAD.split("institution_awarded_per_award")[1]
    runs = [institution_arrays(payload_fixture(seed), UNSORTED_PAYLOAD) for seed in range(8)]
    assert any(r != runs[0] for r in runs)
    ids = lambda run: sorted(sorted(x["id"] for x in a) for _, a, _ in run)
    assert ids(runs[0]) == ids(runs[1])                                                         # same sets, other order


# ---------- recipient countries / regions (lib/award_recipients.py) ----------
def recipient_arrays_sql():
    c = taf.FakeC(extra=["award_recipients_raw"])      # the flows suite's fake runtime: the generated SQL, nothing executed
    award_recipients.run(c)
    return dict(c.artifacts)["recipient_arrays"].replace("dev.lab.ngr_s_", "s_")


def recipient_db(order_seed, sql):
    duckdb = pytest.importorskip("duckdb"); sqlglot = pytest.importorskip("sqlglot")
    con = duckdb.connect(); con.execute("SET threads=1")
    con.execute("CREATE TABLE s_recipients_selected (award_id BIGINT, kind VARCHAR, country_code VARCHAR, region_code VARCHAR, "
                "region_vocabulary VARCHAR, basis VARCHAR, share DOUBLE, equal_split BOOLEAN)")
    con.execute("CREATE TABLE s_awards_candidate_before_recipients (id BIGINT, amount_usd DOUBLE)")
    rows = [(1, "country", "KE", None, None, "funder_reported", 0.5), (1, "country", "UG", None, None, "funder_reported", 0.25),
            (1, "country", "TZ", None, None, "funder_reported", 0.25), (1, "region", None, "289", "1", "funder_reported", 0.5),
            (1, "region", None, "289", "2", "funder_reported", 0.5),                 # one code under two vocabularies: the old tie
            (2, "country", "ZA", None, None, "funder_reported", 1 / 3), (2, "country", "NA", None, None, "funder_reported", 1 / 3),
            (2, "country", "BW", None, None, "funder_reported", 1 / 3), (2, "region", None, "298", "1", "funder_reported", 1.0)]
    rng = random.Random(order_seed); rng.shuffle(rows)
    for r in rows:
        con.execute("INSERT INTO s_recipients_selected VALUES (?, ?, ?, ?, ?, ?, ?, false)", list(r))
    con.execute("INSERT INTO s_awards_candidate_before_recipients VALUES (1, 1000.0), (2, 999.0)")
    q = sqlglot.transpile(sql, read="spark", write="duckdb")[0]
    return con.execute(f"SELECT award_id, recipient_countries, recipient_regions FROM ({q}) ORDER BY award_id").fetchall()


def test_recipient_arrays_are_identical_whatever_the_input_order_and_keep_their_fields():
    sql = recipient_arrays_sql()
    assert "neg_share" in sql and "array_sort(" not in sql
    runs = [recipient_db(seed, sql) for seed in range(8)]
    assert all(r == runs[0] for r in runs)
    (a1, c1, g1), (a2, c2, g2) = runs[0]
    assert [x["country_code"] for x in c1] == ["KE", "TZ", "UG"]      # largest share first, then the code (unchanged rule)
    assert [x["country_code"] for x in c2] == ["BW", "NA", "ZA"]
    assert list(c1[0]) == ["country_code", "share", "amount_usd", "basis"] and list(g1[0]) == ["code", "vocabulary", "display_name", "share"]
    assert [x["code"] for x in g1] == ["289", "289"] and len({x["vocabulary"] for x in g1}) == 2


def test_negative_control_recipients_unsorted_follow_the_input_order_and_the_old_comparator_tied():
    sql = recipient_arrays_sql()
    unsorted = re.sub(r"sort_array\((collect_list\(named_struct\(.*?\)\))\) items", r"\1 items", sql, flags=re.S)
    assert unsorted.count("sort_array(") == 0
    runs = [recipient_db(seed, unsorted) for seed in range(8)]
    assert any(r != runs[0] for r in runs)
    # the pre-fix comparator, (share desc, code), on award 1's two regions: 0 = a tie, left to collect_list order
    old = lambda l, r: -1 if l["share"] > r["share"] else 1 if l["share"] < r["share"] else -1 if l["k"] < r["k"] else 1 if l["k"] > r["k"] else 0
    a = {"share": 0.5, "k": "289", "vocabulary": "oecd_dac_region"}; b = {"share": 0.5, "k": "289", "vocabulary": "publisher"}
    assert old(a, b) == 0 and a != b


def test_yesterday_is_hashed_with_its_institution_arrays_sorted_so_order_alone_never_restamps():
    import create_api
    cols = {"id", "updated_date", "institution_awarded", "institution_awarded_full"}
    rel = create_api.previous_sorted("previous_api_v", cols)
    assert rel == ("(SELECT * EXCEPT(institution_awarded,institution_awarded_full),array_sort(institution_awarded) institution_awarded,"
                   "array_sort(institution_awarded_full) institution_awarded_full FROM previous_api_v) p")
    assert create_api.previous_sorted("previous_api_v", {"id", "updated_date"}) == "previous_api_v"   # an older table: unchanged
    # executed: yesterday stored [I9, I10] unsorted, tonight builds the sorted array; the normalised yesterday equals tonight
    duckdb = pytest.importorskip("duckdb"); sqlglot = pytest.importorskip("sqlglot")
    con = duckdb.connect()
    con.execute("CREATE TABLE previous_api_v AS SELECT 1 id, TIMESTAMP '2026-10-01' updated_date, "
                "[{'id': 'https://openalex.org/I9'}, {'id': 'https://openalex.org/I10'}] institution_awarded, "
                "[{'id': 'https://openalex.org/I9'}, {'id': 'https://openalex.org/I10'}] institution_awarded_full")
    q = sqlglot.transpile(f"SELECT to_json(institution_awarded) j, to_json(institution_awarded_full) f FROM {rel}", read="spark", write="duckdb")[0]
    j, f = con.execute(q).fetchone()
    tonight = con.execute("SELECT to_json(list_sort([{'id': 'https://openalex.org/I10'}, {'id': 'https://openalex.org/I9'}]))").fetchone()[0]
    assert j == f == tonight
