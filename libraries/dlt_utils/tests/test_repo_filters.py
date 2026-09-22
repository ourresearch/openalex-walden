"""Tests for the repo ingest policy filters (oxjob #881).

The filter itself needs a real SparkSession, so what is asserted here is the contract that
drifted: the constant, and that the rules are sourced from one vocabulary rather than a
hand-copied list.
"""

from openalex.dlt import repo_filters as rf
from openalex.dlt import repo_types as rt


def test_min_title_length_matches_the_rule_it_replaced():
    # Repo.py used the literal 5 inline; RepoBackfill.py had no title rule at all
    assert rf.MIN_TITLE_LENGTH == 5


def test_policy_filter_is_exported():
    # the point of the extraction: one callable both notebooks import
    assert callable(rf.apply_repo_policy_filters)


def test_filter_reads_the_shared_vocabulary():
    # not a hand-copied list -- if these ever stop being the same object, the drift is back
    assert rf.TYPES_TO_DELETE is rt.TYPES_TO_DELETE


def test_filter_signature_allows_column_overrides():
    # RepoBackfill and Repo name these columns identically today, but the filter should not
    # assume it
    import inspect
    params = inspect.signature(rf.apply_repo_policy_filters).parameters
    assert {"title_col", "type_col", "native_id_col"} <= set(params)


def test_endpoint_denylist_shape():
    # oxjob #881 round 2: 67 tier-A endpoints + pt.cision.com; oxjob #880 round 3: +10 item-level
    # digitization / periodical-issue endpoints. Every id is a non-empty string; the count is
    # asserted so an accidental paste-truncation fails loudly.
    assert len(rf.ENDPOINTS_TO_DELETE) == 77
    assert all(isinstance(e, str) and e for e in rf.ENDPOINTS_TO_DELETE)
    # the four individually-adjudicated verdicts (ENDPOINT_VERDICTS.md) are present
    assert {
        "2b57dfbd43207095dbc",  # pt.cision.com -- EDP media monitoring
        "05b312337ed8746c780",  # UBC ContentDM
        "db7d86c1ad782e15c55",  # server15795 ContentDM
        "215bbea87561a3ab940",  # CDAEA
    } <= rf.ENDPOINTS_TO_DELETE


def test_gallica_is_carved_not_denylisted():
    # the carve keeps monographies/manuscrits/periodiques:titres -- whole-endpoint removal
    # of Gallica would be wrong, so it must never appear in both structures
    assert "b6f3a90f96528af2baa" in rf.ENDPOINT_SETSPEC_DELETE
    assert "b6f3a90f96528af2baa" not in rf.ENDPOINTS_TO_DELETE
    prefixes = rf.ENDPOINT_SETSPEC_DELETE["b6f3a90f96528af2baa"]
    assert "gallica:typedoc:periodiques:fascicules" in prefixes
    # keep-classes must not be carved, even by prefix overlap
    for keep in ("gallica:typedoc:monographies", "gallica:typedoc:manuscrits",
                 "gallica:typedoc:periodiques:titres"):
        assert not any(keep.startswith(p) for p in prefixes)


def test_no_endpoint_in_both_structures():
    assert not set(rf.ENDPOINT_SETSPEC_DELETE) & rf.ENDPOINTS_TO_DELETE


def test_endpoint_filter_is_exported():
    import inspect
    assert callable(rf.apply_endpoint_filters)
    params = inspect.signature(rf.apply_endpoint_filters).parameters
    assert {"endpoint_col", "set_spec_col", "keep_when"} <= set(params)


def test_hispana_is_carved_not_denylisted():
    # oxjob #880/#881: heritage collections go, university-IR sets and the no-setSpec bucket stay
    import openalex.dlt.repo_filters as rf
    assert "0ef9aa4cd18142685bb" in rf.ENDPOINT_SETSPEC_DELETE
    assert "0ef9aa4cd18142685bb" not in rf.ENDPOINTS_TO_DELETE
    prefixes = rf.ENDPOINT_SETSPEC_DELETE["0ef9aa4cd18142685bb"]
    for heritage in ("eseceres", "bdmmadrid", "gal2", "hdmurcia", "historico_valencia"):
        assert any(heritage.startswith(p) for p in prefixes)
    for keep in ("gredos", "riunet", "idus", "uji", "bvandalucia", "historico_castellon"):
        assert not any(keep.startswith(p) for p in prefixes)


def test_round3_fanout_carves():
    # oxjob #880 round 3 (KEY_LEDGER_PLAN.md s 9): the ten whole endpoints and the set carves on
    # endpoints that stay harvested. Galiciana's 'duplicados' (digitized books) must survive.
    for whole in ("3e821e5524e99c846c7", "424791a2d217cbac04a",
                  "33c353ac55f2fe86ca4", "2464b7588f6c599ab7c", "e11d60e7cd398991490"):
        assert whole in rf.ENDPOINTS_TO_DELETE
        assert whole not in rf.ENDPOINT_SETSPEC_DELETE
    gal = rf.ENDPOINT_SETSPEC_DELETE["82fda151e563b16f718"]
    assert any("hemeroteca_1".startswith(p) for p in gal)
    assert not any("duplicados".startswith(p) for p in gal)
    assert "partner:CAH" in rf.ENDPOINT_SETSPEC_DELETE["4c3c0d4c422b7f8c3fc"]


def test_unc_is_a_keep_list_not_a_whole_endpoint_denial():
    # oxjob #880 round 3b: the 2026-09-04 whole-endpoint denial deleted the Journal of the North
    # Carolina Academy of Science (71 cited works, 208 citations) along with 1.38M finding-aid scans.
    unc = "7ccc21dda876bd4e680"
    assert unc not in rf.ENDPOINTS_TO_DELETE
    assert unc not in rf.ENDPOINT_SETSPEC_DELETE
    assert unc in rf.ENDPOINT_SETSPEC_KEEP
    kept = rf.ENDPOINT_SETSPEC_KEEP[unc]
    for scholarly in ("jncas", "nchh", "dmisc"):        # every set that produced a cited work
        assert scholarly in kept
    for item_level in ("01819", "03ddd", "00ddd", "sfc", "uars", "sohp", "dig_nccpa", "ead_tail"):
        assert not any(item_level.startswith(p) for p in kept)


def test_keep_and_delete_structures_are_disjoint():
    # an endpoint must be governed by exactly one rule, or the filters contradict each other
    assert not set(rf.ENDPOINT_SETSPEC_KEEP) & set(rf.ENDPOINT_SETSPEC_DELETE)
    assert not set(rf.ENDPOINT_SETSPEC_KEEP) & rf.ENDPOINTS_TO_DELETE


def test_component_carve_is_scoped_to_figshare():
    # oxjob #1000: the suffix alone matches 25,612 real works (Technometrics' 18K-cite
    # 10.1198/tech.2005.s303, OJS galley DOIs). Only the figshare endpoint is carved.
    assert rf.COMPONENT_CARVE_NATIVE_ID_PREFIXES == ("oai:figshare.com",)
    import inspect
    params = inspect.signature(rf.apply_endpoint_filters).parameters
    assert {"native_id_col", "ids_col"} <= set(params)


def test_component_doi_suffix_regex():
    # The regex is interpolated into a Spark SQL string literal, so it must carry no backslash
    # escapes (a '\.' would be unescaped to a bare '.'); it must match the three component
    # classes and never a figshare-native DOI.
    import re
    assert "\\" not in rf.COMPONENT_DOI_SUFFIX
    rx = re.compile(rf.COMPONENT_DOI_SUFFIX)
    for component in ("10.1371/journal.pone.0274801.s001",
                      "10.1371/journal.ppat.1001173.t003",
                      "10.1371/journal.pgen.1011542.g001",
                      "10.1021/acsabm.0c01427.s001"):
        assert rx.search(component), component
    for real_work in ("10.6084/m9.figshare.12345678",
                      "10.6084/m9.figshare.12345678.v2",
                      "10.1371/journal.pone.0274801",
                      "10.4067/s0718-07642015000400001",
                      "10.1080/00131881.2024.2347977"):
        assert not rx.search(real_work), real_work


# ---- oxjob #1311: Cairn.info paratext headings ---------------------------------------------

def _fixture():
    import json, os
    path = os.path.join(os.path.dirname(__file__), "fixtures", "oxjob1311_cairn_titles.json")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def test_cairn_paratext_rule_is_scoped_to_the_two_cairn_endpoints():
    # both endpoints map to source S4406923042 (Cairn.info); the rule must never widen to the
    # generic endpoint structures, and the same headings on Crossref (1.76M) are out of scope
    assert rf.ENDPOINT_PARATEXT_TITLE_DELETE == {"asswjsx35xuxkrfsyfwn", "saf6vuotbas9qzkypz6j"}
    assert not rf.ENDPOINT_PARATEXT_TITLE_DELETE & rf.ENDPOINTS_TO_DELETE
    assert not rf.ENDPOINT_PARATEXT_TITLE_DELETE & set(rf.ENDPOINT_SETSPEC_DELETE)
    assert not rf.ENDPOINT_PARATEXT_TITLE_DELETE & set(rf.ENDPOINT_SETSPEC_KEEP)
    import inspect
    assert "title_col" in inspect.signature(rf.apply_endpoint_filters).parameters


def test_fold_title_matches_spark_translate_semantics():
    # same-length translate pairs, so str.translate and Spark translate() agree letter for letter
    assert len(rf.PARATEXT_FOLD_FROM) == len(rf.PARATEXT_FOLD_TO)
    assert rf.fold_title("  Pages  de\tDébut ") == "pages de debut"
    assert rf.fold_title("PRÉFACE") == "preface"
    assert rf.fold_title("Avant-Propos") == "avant-propos"
    assert rf.fold_title(None) is None
    assert rf.fold_title("   ") == ""
    # every exact heading is already in folded form (or the Spark side can never match it)
    for t in rf.PARATEXT_TITLES:
        assert rf.fold_title(t) == t, t


def test_paratext_patterns_are_plain_java_regexes():
    # no backslash escapes (the #1000 rule; these go through Column.rlike as literals, but the
    # habit keeps them safe if anyone ever moves them into an F.expr string)
    for p in rf.PARATEXT_TITLE_PATTERNS:
        assert "\\" not in p, p


def test_authored_headings_are_not_paratext():
    # the README's warning: "Introduction" can be a real chapter. These stay.
    for heading in ("Introduction", "Conclusion", "Présentation", "Éditorial", "Editorial",
                    "Prologue", "Épilogue", "Préambule", "Ouverture", "Conclusion générale",
                    "Introduction générale", "Chapitre 4. Modélisation de la crise ontologique (2)",
                    "Annexe 2. Grille d’évaluation des compétences sociales",
                    "Bibliographie des travaux scientifiques de jean lafond et de ses collaborateurs (1950-2020)",
                    "La psychomotricité", "Petit lexique de l’administration française"):
        assert not rf.is_paratext_title(heading), heading


def test_paratext_headings_are_matched_in_every_observed_form():
    for heading in ("Pages de début", "Pages de debut", "pages de fin", "Pages de Fin", "Páginas iniciales",
                    "Bibliographie", "BIBLIOGRAPHIE", "Bibliographie sélective", "Références bibliographiques",
                    "Préface", "Preface", "Préfacé", "Préface à l’édition française", "Avant-Propos", "Postface",
                    "Index", "Index des noms de personnes", "Index des noms cités", "Index nominum",
                    "Les auteurs", "Liste des auteurs", "Présentation des auteurs", "Remerciements",
                    "Glossaire", "Lexique", "Liste des abréviations", "Sigles et acronymes",
                    "Annexe", "Annexes", "Annexe 3", "Annexe II", "Notes", "Chronologie", "Avertissement",
                    "Front matter", "Back Matter", "Table des matières", "Sommaire", "Erratum", "Foreword"):
        assert rf.is_paratext_title(heading), heading


def test_rule_against_the_30_night_cairn_sample():
    # tests/fixtures/oxjob1311_cairn_titles.json: every distinct title the two Cairn endpoints
    # admitted in the 30 nights before 2026-09-22. 'paratext' is the reviewed set the rule must
    # remove, 'content' the headings it must keep. A pattern edit that widens or narrows the
    # rule fails here, with the title.
    fx = _fixture()
    assert fx["sample"]["paratext_distinct_titles"] == len(fx["paratext"])
    missed = [t for t in fx["paratext"] if not rf.is_paratext_title(t)]
    assert missed == [], missed[:20]
    claimed = [t for t in fx["content"] if rf.is_paratext_title(t)]
    assert claimed == [], claimed[:20]
    # the rule's footprint on the sample: 4.9% of records. A rewrite that claims a materially
    # different share of the same sample must update the fixture on purpose.
    assert 0.04 < fx["sample"]["paratext_records"] / fx["sample"]["records"] < 0.06
