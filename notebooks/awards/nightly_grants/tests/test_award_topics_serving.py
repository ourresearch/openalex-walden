"""Topics, keywords and SDGs on grants (oxjob #2125): Nightly Grants serves AwardTopics' three tables in the works shape.

Wiring and hash tests need nothing but pytest; the payload test runs sql/api_payload.sql in DuckDB (skipped without it)."""
import json

from test_award_country import Fixture, PAYLOAD, ROOT
from test_award_flows import BASE
import award_country
import award_investigators
import create_api
import nightly_runtime
import sync_awards

CONFIG = json.loads((ROOT / "config.prod.json").read_text())


def test_nightly_reads_award_topics_v2_keywords_and_sdgs():
    assert CONFIG["inputs"]["topics"] == "openalex.awards.award_topics_v2"
    assert CONFIG["inputs"]["keywords"] == "openalex.awards.award_keywords"
    assert CONFIG["inputs"]["sdgs"] == "openalex.awards.award_sdgs"
    assert {"topics", "keywords", "sdgs"} <= nightly_runtime.REQUIRED_INPUTS
    src = (ROOT / "lib/build_awards.py").read_text()
    assert "t.topics,t.topics[0] primary_topic,k.keywords,s.sdgs sustainable_development_goals" in src
    assert "LEFT JOIN keywords_v k ON k.award_id=m.stable_id" in src and "LEFT JOIN sdgs_v s ON s.award_id=m.stable_id" in src
    assert "for v in ('topics_v','keywords_v','sdgs_v'):" in src   # TOPICS_UNIQUE, KEYWORDS_UNIQUE, SDGS_UNIQUE


def test_payload_serves_keywords_and_sdgs_as_given_and_empty_when_none():
    f = Fixture()
    tagged, bare = f.award("nih_exporter"), f.award("crossref_work")
    f.con.execute("UPDATE awards SET keywords = ['kw'], sustainable_development_goals = ['sdg3'] WHERE id = ?", [tagged])
    sql = f.sqlglot.transpile(PAYLOAD.replace("{AWARDS}", "awards").replace("{ANSWERS}", "answers")
                              .replace("{COUNTRY_LOOKUP}", award_country.lookup_relation_sql(ROOT))
                              .replace("{INVESTIGATOR_LINKS}", award_investigators.EMPTY_LINKS), read="spark", write="duckdb")[0]
    rows = {i: (k, s) for i, k, s in f.con.execute(f"SELECT id, keywords, sustainable_development_goals FROM ({sql})").fetchall()}
    assert rows[tagged] == (["kw"], ["sdg3"])
    assert rows[bare] == ([], [])


def test_awards_without_keywords_or_sdgs_keep_their_hash_and_the_others_wrap_it():
    cols = set(create_api.FLOW_COLUMNS) | set(create_api.LINK_COLUMNS) | {"recipients_full"}
    old = create_api.hash_expression(BASE, cols)
    new = create_api.hash_expression(BASE, cols | set(create_api.ENRICH_COLUMNS))
    assert new == (f"CASE WHEN coalesce(size(keywords),0)=0 AND coalesce(size(sustainable_development_goals),0)=0 THEN {old} "
                   f"ELSE xxhash64(concat_ws('|',CAST(({old}) AS STRING),to_json(keywords),to_json(sustainable_development_goals))) END")
    # yesterday's table without the columns hashes exactly as before; today's always carries them
    assert create_api.hash_expression(BASE, cols | {"keywords"}) == old
    assert "|set(ENRICH_COLUMNS))" in (ROOT / "lib/create_api.py").read_text()


def test_sync_refuses_until_keyword_and_sdg_ids_are_mapped_as_keyword():
    assert sync_awards.AWARD_FIELD_MAPPINGS["keywords.id"] == {"type": "keyword"}
    assert sync_awards.AWARD_FIELD_MAPPINGS["sustainable_development_goals.id"] == {"type": "keyword"}
    assert sync_awards.AWARD_FIELD_MAPPINGS["sustainable_development_goals.id.lower"] == {"type": "keyword"}   # elastic-api's SDG term


def test_an_unprovisioned_keyword_or_sdg_id_stops_the_sync():
    from test_nightly_grants import _field_mapping
    resp = _field_mapping({"keywords.id": {"type": "text"}})                   # dynamic mapping's guess
    del resp["awards-v4"]["mappings"]["sustainable_development_goals.id.lower"]  # the PUT never ran
    assert sync_awards.mapping_mismatches(resp, "awards-v4") == ["keywords.id", "sustainable_development_goals.id.lower"]
    assert sync_awards.mapping_mismatches(_field_mapping(), "awards-v4") == []
