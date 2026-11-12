"""Offline tests for the awards enrich job (no Spark, no network): the usable-text rule, the Modal request checks against Jason's app
sources, the SDG head, and the keyword normaliser's swap of the two works tables.
    python3 -m pytest -q notebooks/awards/enrich/tests
"""
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "lib"))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "..", "..")))
import award_text as T  # noqa: E402
import modal_call as M  # noqa: E402


@pytest.mark.parametrize("title,desc,ok", [
    ("A four word title", None, True), ("Too short", None, False), (None, " ".join(["w"] * 20), True), (None, " ".join(["w"] * 19), False),
    ("具有液体发光层的柔性有机电致发光器件发光特性研究", None, True), ("丝状微生物", None, False),          # 5 Han characters
    ("No title - Aucun titre", None, False), ("", "近年、大学教育では学生同士の討論等を基盤としたアクティブ・ラーニングによる様々な教育的効果が報告されている", True),
])
def test_usable(title, desc, ok):   # NSERC's title placeholder is empty text (text_rules.csv)
    assert T.python_usable(title, desc) is ok


def test_sql_rule_names_the_same_thresholds():
    assert f"tc >= {T.MIN_TITLE_CJK}" in T.USABLE and f"dc >= {T.MIN_DESC_CJK}" in T.USABLE and f"tw >= {T.MIN_TITLE_WORDS}" in T.USABLE
    assert "\n\nAbstract: " in T.EMBED_TEXT.replace("\\n", "\n")


def test_requests_against_the_app_sources():
    for app in ("topics", "keywords"):
        r = M.app_rules(app)
        assert r["table"] and r["out"] and r["warehouse"] and r["default_warehouse"] == "3996dc0a9b183ce3"
        good = M.start_body(app, "openalex.transient.awt_queue", "/Volumes/openalex/works/data/topics_nightly/awards/awt-1", "awt-1", "0123456789abcdef", [0])
        assert M.check_request(app, good) == []
        dev = dict(good, table_in="openalex_dev.rohan_lab.awt_queue")
        assert any("TABLE_RX" in b for b in M.check_request(app, dev))
        assert any("never-use" in b for b in M.check_request(app, dict(good, warehouse="3996dc0a9b183ce3")))
        assert any("16-hex" in b for b in M.check_request(app, dict(good, warehouse="")))
    assert M.APPS["keywords"]["model"] in M.app_rules("keywords")["models"]


def test_estimates_are_jasons():
    assert round(M.estimate_usd("topics", 10000, 1), 3) == 0.505
    assert round(M.estimate_usd("keywords", 10000, 1), 3) == 0.65
    assert round(M.estimate_usd("topics", 6849747, 274), 1) == 41.3


def test_sdg_head_matches_the_works_file():
    np = pytest.importorskip("numpy")
    import sdg_head as H
    h = H.load()
    assert h["version"] == "sdg-jev-head-v2" and h["path"].endswith("notebooks/sdg/sdg_jev_head_v2.json") and set(h["thr"]) == {0.4}
    p = H.probs(h, np.zeros((2, h["input_dim"]), dtype=np.float32))
    assert p.shape == (2, 17) and ((p >= 0) & (p <= 1)).all()
    row = np.full(17, 0.1, dtype=np.float32); row[2], row[6], row[12] = 0.9, 0.4, 0.9
    assert [g["id"][-2:].strip("/") for g in H.sdgs(h, row)] == ["3", "13", "7"]   # score desc, ties in goal order


def test_keyword_templates_swap_exactly_the_two_works_tables():
    from utils import keywords_nightly as kn
    vals = {k: f"<{k}>" for k in kn.TOKENS}
    swapped = 0
    for step, sql in kn.normalise_statements(vals):
        swapped += sql.count("openalex.works.openalex_works") + sql.count("openalex.works.works_study_design")
        out = sql.replace("openalex.works.openalex_works", "awt_award_docs").replace("openalex.works.works_study_design", "awt_no_study_design")
        assert "openalex.works." not in out, step
    assert swapped >= 6


class _FS:   # a dbutils.fs stand-in
    def __init__(self): self.files = {}
    def mkdirs(self, d): pass
    def ls(self, d): return [type("F", (), {"name": k.rsplit("/", 1)[1]}) for k in self.files if k.startswith(d + "/")]
    def head(self, p): return self.files[p]
    def put(self, p, v, overwrite=False): self.files[p] = v
    def rm(self, p): self.files.pop(p, None)


def test_modal_call_starts_once_and_a_retry_polls_the_same_call(monkeypatch):
    monkeypatch.setattr(M.time, "sleep", lambda s: None)
    calls = []

    def fake_post(self, path, body, tries=5):
        calls.append(path)
        return {"call_id": "fc-abcdefghij12"} if path == "/start" else {"state": "done", "result": {"works": 3}}
    monkeypatch.setattr(M.ModalCall, "post", fake_post)
    fs = _FS()
    body = M.start_body("topics", "openalex.transient.awt_queue", "/Volumes/openalex/works/data/topics_nightly/awards/awt-1", "awt-1", "0123456789abcdef", [0])
    assert M.ModalCall("topics", "https://x", "t", fs, log=lambda m: None).run(body, 1) == {"works": 3}
    assert M.ModalCall("topics", "https://x", "t", fs, log=lambda m: None).run(body, 1) == {"works": 3}   # the retry
    assert calls == ["/start", "/status", "/status"]
    assert fs.files["/Volumes/openalex/works/data/topics_nightly/awards/awt-1/_call_id.txt"] == "fc-abcdefghij12"


def test_award_topics_job_runs_its_notebooks_with_safe_defaults():
    yaml = pytest.importorskip("yaml")
    import re
    repo = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
    raw = open(os.path.join(repo, "jobs", "award_topics.yaml")).read()
    job = yaml.safe_load(raw)["resources"]["jobs"]["AwardTopics"]
    tasks = {t["task_key"]: t for t in job["tasks"]}
    assert list(tasks) == ["build_queue", "topics", "keywords", "sdgs", "papers"] and "job_clusters" not in job and "schedule" not in job
    assert [d["task_key"] for d in tasks["papers"]["depends_on"]] == ["topics"]   # both write award_topics_v2: text first
    params = {p["name"]: p["default"] for p in job["parameters"]}
    assert params["dry_run"] == "true" and params["warehouse_id"] == "" and params["state_prefix"].startswith("openalex_dev.")
    assert not any(w in raw for w in M.FORBIDDEN_WAREHOUSES)
    for t in tasks.values():
        nb = os.path.join(repo, t["notebook_task"]["notebook_path"] + ".py")
        widgets = set(re.findall(r'\("([a-z_]+)", ', open(nb).read()))
        assert set(t["notebook_task"]["base_parameters"]) <= widgets, (t["task_key"], set(t["notebook_task"]["base_parameters"]) - widgets)
    assert "jobs/award_topics.yaml" in open(os.path.join(repo, "databricks.yml")).read()
    assert not os.path.exists(os.path.join(repo, "jobs", "award_enrich.yaml"))


def test_an_approved_never_use_warehouse_passes_only_by_its_exact_id():
    body = M.start_body("topics", "openalex.transient.awt_queue", "/Volumes/openalex/works/data/topics_nightly/awards/awt-1", "awt-1",
                        "3996dc0a9b183ce3", [])
    assert any("never-use" in b for b in M.check_request("topics", body))
    assert M.check_request("topics", body, approved_warehouse="3996dc0a9b183ce3") == []
    assert any("never-use" in b for b in M.check_request("topics", dict(body, warehouse="4d37bf76372a2901"), "3996dc0a9b183ce3"))


def test_go_uploads_every_file_the_notebooks_read():
    sys.path.insert(0, os.path.join(HERE, "..", "sample"))
    import sample_run as S
    up = S.UPLOADS
    covered = lambda rel: any(rel == u or rel.startswith(u + "/") for u in up)
    for rel in (M.APPS["topics"]["source"], M.APPS["keywords"]["source"], "utils/keywords_nightly.py",
                "notebooks/sdg/sdg_jev_head_v2.json", "notebooks/awards/enrich/lib/award_text.py", "notebooks/awards/enrich/sample/awt_tiny_ids.csv", "notebooks/awards/enrich/text_rules.csv"):
        assert covered(rel), rel
        assert os.path.exists(os.path.join(S.REPO, rel)), rel


# ---------- queue fixes 2026-11-09: placeholders and person feeds (text_rules.csv) ----------
@pytest.mark.parametrize("text,empty", [
    ("No description available", True), ("  no DESCRIPTION available. ", True), ("N/A", True), ("none", True), ("-", True), ("TBD", True),
    ("...", True), ("No Summary - Aucun sommaire", True), ("Keine Zusammenfassung vorhanden", True), ("", True),
    ("No description of the method is available yet, so we measure it", False), ("Nonelinear optics", False), ("A", False),
])
def test_placeholders_are_empty_text(text, empty):
    assert (T.python_clean(text) is None) is empty


def test_placeholder_description_is_never_the_abstract():
    assert T.python_usable("A study of four words", "No description available")            # the title still qualifies
    assert not T.python_usable("Too short", "No description available")                      # the placeholder does not
    sql = T.usable_awards_sql()
    assert "'no description available'" in sql and sql.count("THEN NULL ELSE a.description END") == 1


@pytest.mark.parametrize("prov,title,desc,ok", [
    ("guggenheim_fellowship", "Guggenheim Fellowship - Jessica Eaton (2019.0)", None, False),
    ("conahcyt_snii_ckan", "SNII Investigador Nivel 2 — Pablo Fernando Lazo Briones", "SNII appointment in Humanidades at UNIVERSIDAD IBEROAMERICANA.", False),
    ("humboldt", "Humboldt Research Fellowship (March 2015)",
     "Area of expertise: Bioorganic and Biological Chemistry | Keywords: Nucleotidylylation, AMPylation, Fic-domain, MS, Proteomics, "
     "protein modification, enzyme mechanisms, bacterial effectors, mass spectrometry", True),   # the description qualifies alone
    ("nih_exporter", "Guggenheim Fellowship - Jessica Eaton (2019.0)", None, True),                 # same text, not a person feed
])
def test_person_feed_titles_never_qualify(prov, title, desc, ok):
    assert T.python_usable(title, desc, prov) is ok


def test_person_feed_titles_are_not_sent():
    sql = T.usable_awards_sql()
    assert "CASE WHEN a.provenance IN ('aos_kessel_laccr_research', 'ash_award_recipients'" in sql and "'wolf_prize_wp') THEN NULL ELSE CASE WHEN" in sql


def test_rules_csv_is_reviewed_data():
    import csv
    rows = list(csv.DictReader(open(T.RULES_FILE, encoding="utf-8")))
    assert {r["kind"] for r in rows} == {"placeholder", "person_feed"}
    vals = [(r["kind"], T.norm_text(r["value"]) if r["kind"] == "placeholder" else r["value"]) for r in rows]
    assert len(vals) == len(set(vals)), "duplicate rule"
    for r in rows:
        assert r["note"].strip(), r
        if r["kind"] == "placeholder":
            assert r["value"] == T.norm_text(r["value"]), f"store placeholders normalised: {r['value']!r}"
            assert len(r["value"].split()) <= 8, r
        else:
            assert r["value"].replace("_", "").isalnum() and r["count_1109"].isdigit(), r
    feeds = T.load_rules()["person_feed"]
    assert {"conahcyt_snii_ckan", "guggenheim_fellowship", "humboldt", "bbrf_narsad", "fnp_laureates"} <= feeds and len(feeds) == 56


def test_sql_literals_are_escaped(tmp_path):
    p = tmp_path / "rules.csv"
    p.write_text("kind,value,count_1109,note\nplaceholder,it's none,,quote\nperson_feed,x_feed,1,n\n", encoding="utf-8")
    sql = T.usable_awards_sql(rules=T.load_rules(str(p)))
    assert "'it\\'s none'" in sql and "IN ('x_feed')" in sql


@pytest.mark.parametrize("title,desc,ok", [
    ("No title - Aucun titre", None, False),          # NBSP inside a placeholder: still a placeholder
    (" No title - Aucun titre ", None, False),
    ("Four words in title", None, True),     # NBSP separates words, as Python's split() says
    ("　 ", None, False),                          # ideographic space + NBSP only: empty
])
def test_unicode_whitespace(title, desc, ok):
    assert T.python_usable(title, desc) is ok


def test_every_sql_whitespace_class_is_unicode():
    """Java's \\s is ASCII unless (?U); Python's is Unicode. Every regex in the queue SQL that uses \\s must carry (?U)."""
    import re
    sql = T.usable_awards_sql()
    for lit in re.findall(r"'((?:[^'\\]|\\.)*)'", sql):
        if "\\\\s" in lit:
            assert lit.startswith("(?U)"), lit
    assert " trim(" not in T.text_columns("c") and "split(trim(" not in sql


def test_sdg_passes_never_read_the_stage_they_append_to():
    src = open(os.path.join(HERE, "..", "ScoreAwardSdgs.py")).read()
    body = src[src.index("def score_chunk(part):"):src.index("import concurrent.futures as cf")]
    assert "FROM {PENDING}" in body and "{STAGE}" not in body.split(".saveAsTable(STAGE)")[0]
    assert "keep_vectors" not in src


@pytest.mark.parametrize("title,ok", [
    ("+ + + +", False),        # ASCII punctuation only: empty in both twins
    ("$ % & *", False),
    ("！ ！ ！ ！", True),       # fullwidth marks are not ASCII punctuation in either twin: four "words"
])
def test_punctuation_twins_agree(title, ok):
    assert T.python_usable(title, None) is ok
    clean = T.clean_sql("x", T.load_rules())
    assert "RLIKE '^[\\\\p{Punct} –—…]*$'" in clean and "(?U)^[" not in clean   # ASCII \p{Punct}, as string.punctuation


@pytest.mark.parametrize("title,ok", [
    ("No\u001ctitle - Aucun titre", True),       # U+001C is not (?U)\s in Java: one token "No\x1ctitle" + 4 more = 5 words, not the placeholder
    ("Four\u001cwords\u001cin\u001ctitle", False),  # likewise one word
    ("・・・・・・・・", False),                        # katakana middle dot is Common script, not \p{IsKatakana}
    ("ｶﾀｶﾅｶﾀｶﾅ", True),                           # halfwidth katakana is \p{IsKatakana}: 8 CJK characters
    ("\U00030000" * 8, True),                      # CJK Ext G is \p{IsHan}
])
def test_python_twin_matches_java_classes(title, ok):
    assert T.python_usable(title, None) is ok


def test_topics_merge_has_no_map_cross_join_and_skips_a_loaded_build():
    """A CROSS JOIN of the 4,516-topic map onto 6.68M awards stalled the backfill. The served rows come from posexplode of the same
    top-3 slice + a broadcast join to the vocabulary, re-gathered in position order; a loaded build is never scored or inserted twice;
    NULL flags or arrays in the shard files and a vocabulary that repeats a topic id stop the run before anything is written."""
    src = open(os.path.join(HERE, "..", "ScoreAwardTopics.py")).read()
    merge = src[src.index('spark.sql(f"""MERGE INTO {TARGET} v USING ('):]
    assert "CROSS JOIN" not in merge and "map_from_entries" not in merge and "QUALIFY" not in merge
    assert "posexplode(slice(filter(arrays_zip(sc.topic_ids, sc.probs), x -> x.topic_ids <> -1), 1, 3))" in merge
    assert "/*+ BROADCAST(m) */" in merge and "array_sort(collect_list(named_struct('rn', k.rn," in merge
    assert "CAST(ROUND(k.p, 4) AS FLOAT)" in merge and "WHEN sc.not_classifiable THEN CAST(ARRAY() AS {TOPICS_TYPE})" in merge
    assert "WITH meta AS ({VOCAB})" in merge
    load = src[src.index("L = one("):src.index('spark.sql(f"DELETE FROM {RAW}')]
    assert "if loaded:" in load and "skipping Modal and the raw INSERT" in load
    assert "NOT (size(topic_ids) <=> 10 AND size(probs) <=> 10 AND not_classifiable <=> (topic_ids[0] = -1))" in load
    gate = src[src.index("g = one("):src.index('spark.sql(f"""MERGE INTO')]
    assert "count(DISTINCT tid) FROM ({VOCAB})) AS vocab_dupes" in gate and "if g.vocab_dupes:" in gate
