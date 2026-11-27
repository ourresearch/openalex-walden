"""oxjob #881: the split index builder must equal the clean-then-invert public path.

transform.py cannot be imported under the conftest pyspark stub (its normalize import touches
real StructType machinery), so the three pure functions are extracted by AST -- the same
functions, same source, no Spark.
"""
import ast
import os
import re

from openalex.dlt.text_clean import clean_text, drop_wrap_whitespace, space_sentence_joins, strip_abstract_heading

_SRC = open(os.path.join(os.path.dirname(__file__), "..", "openalex", "dlt", "transform.py")).read()
_ns = {"clean_text": clean_text, "drop_wrap_whitespace": drop_wrap_whitespace, "space_sentence_joins": space_sentence_joins,
       "strip_abstract_heading": strip_abstract_heading, "re": re}
for _node in ast.parse(_SRC).body:
    if isinstance(_node, ast.FunctionDef) and _node.name in (
            "clean_abstract_text", "_inverted_index_from_cleaned", "f_generate_inverted_index"):
        exec(compile(ast.get_source_segment(_SRC, _node), "transform.py", "exec"), _ns)

clean_abstract_text = _ns["clean_abstract_text"]
_inverted_index_from_cleaned = _ns["_inverted_index_from_cleaned"]
f_generate_inverted_index = _ns["f_generate_inverted_index"]

CASES = [
    None,
    "",
    "Plain ASCII abstract with several words repeated words words.",
    "Vuln&eacute;rabilit&eacute; <jats:p>des</jats:p> syst&egrave;mes",
    "UniversitÃ© de MontrÃ©al studies\nacross\tlines",
    "<p>alpha</p><p>beta</p> gamma",
]


def test_udf_path_equals_public_path():
    for raw in CASES:
        assert f_generate_inverted_index(raw) == _inverted_index_from_cleaned(clean_abstract_text(raw)), raw


def test_index_positions_track_cleaned_tokens():
    assert f_generate_inverted_index("alpha beta alpha") == '{"alpha": [0, 2], "beta": [1]}'


# oxjob #2059: raw Crossref JATS as deposited (crossref_parsed.abstract, 19 Nov 2026).
NATURE = ("<jats:title>Abstract</jats:title>\n                  <jats:p>\n                    Celtic languages are today "
          "restricted to Europe’s Atlantic fringes")
WILEY = ("<jats:title>ABSTRACT</jats:title>\n                  <jats:p>\n                    Coastal wetlands are "
         "critical tropical biodiversity hotspots")
SMALL_CAPS = "<jats:title>\n  A\n  <jats:sc>bstract</jats:sc>\n</jats:title>\n<jats:p>Coastal wetlands are critical</jats:p>"
COPERNICUS = "<jats:p>Abstract. Forestation is seen as a possible option to counter climate change</jats:p>"
CHEM = "<jats:title>Abstract</jats:title><jats:p>β‐Lactoglobulin in 50 m<jats:sc>M</jats:sc> Tris/HCl</jats:p>"


def test_heading_dropped_from_raw_jats():
    assert clean_abstract_text(NATURE) == "Celtic languages are today restricted to Europe’s Atlantic fringes"
    assert clean_abstract_text(WILEY) == "Coastal wetlands are critical tropical biodiversity hotspots"
    assert clean_abstract_text(SMALL_CAPS) == "Coastal wetlands are critical"
    assert clean_abstract_text(COPERNICUS) == "Forestation is seen as a possible option to counter climate change"
    assert clean_abstract_text(CHEM).startswith("β‐Lactoglobulin")


def test_heading_only_abstract_is_none():
    assert clean_abstract_text("<jats:title>Abstract</jats:title>") is None
    assert clean_abstract_text("ABSTRACT:") is None


def test_text_and_index_agree_after_heading_strip():
    for raw in (NATURE, WILEY, SMALL_CAPS, COPERNICUS, CHEM):
        cleaned = clean_abstract_text(raw)
        assert clean_abstract_text(cleaned) == cleaned, raw
        assert f_generate_inverted_index(raw) == _inverted_index_from_cleaned(cleaned), raw
        assert '"Abstract"' not in f_generate_inverted_index(raw) and '"ABSTRACT"' not in f_generate_inverted_index(raw)


# oxjob #2570: raw Crossref JATS, pretty-printed after an inline tag (W7214765603, 10.1177/10781552241281936).
AAS = ("<jats:p>We present the discovery and first characterization of\n                    <jats:italic>"
       "Nāpōwawā‘enakaulua</jats:italic>\n                    , the most compact known quasar pair</jats:p>")
SAGE = ("<jats:p>a panel for <jats:italic>DPYD</jats:italic>\n , <jats:italic>TPMT</jats:italic>\n , "
        "<jats:italic>NUDT15</jats:italic>\n , and <jats:italic>UGT1A1</jats:italic>\n variants (\n"
        "<jats:italic>n</jats:italic> = 12\n ).</jats:p>")


def test_line_break_before_punctuation_dropped_in_abstracts():
    assert clean_abstract_text(AAS) == ("We present the discovery and first characterization of Nāpōwawā‘enakaulua, "
                                        "the most compact known quasar pair")
    assert clean_abstract_text(SAGE) == "a panel for DPYD, TPMT, NUDT15, and UGT1A1 variants (n = 12)."


def test_text_and_index_agree_after_line_break_drop():
    for raw in (AAS, SAGE):
        cleaned = clean_abstract_text(raw)
        assert clean_abstract_text(cleaned) == cleaned, raw
        assert f_generate_inverted_index(raw) == _inverted_index_from_cleaned(cleaned), raw
        assert '"Nāpōwawā‘enakaulua,"' in f_generate_inverted_index(AAS)


# oxjob #2689: GROBID's <s> units and legacy parses joined sentences with no space
GLUED_JATS = "<jats:p>Fast anodes need a quick response.Starting from oxides we built cells.</jats:p>"


def test_glued_sentence_join_spaced_in_text_and_index():
    cleaned = clean_abstract_text(GLUED_JATS)
    assert cleaned == "Fast anodes need a quick response. Starting from oxides we built cells."
    assert clean_abstract_text(cleaned) == cleaned
    index = f_generate_inverted_index(GLUED_JATS)
    assert index == _inverted_index_from_cleaned(cleaned)
    assert '"Starting"' in index and "response.Starting" not in index


def test_heading_strip_and_join_rule_compose_idempotently():
    for raw in ("Abstract.The study found a fast response.Starting from oxides we built cells and more.",
                "ABSTRACT:Background:The aim was to measure it.Methods were standard and well known here.",
                "Abstract algebra.The rings we study are commutative and the results.Hold in general here.",
                "Abstract BackgroundThe cohort had 300 people.MethodsWe followed them up for ten years."):
        once = clean_abstract_text(raw)
        assert clean_abstract_text(once) == once, raw
        assert f_generate_inverted_index(raw) == _inverted_index_from_cleaned(once), raw
    assert clean_abstract_text("Abstract.The study found a fast response.Starting from oxides here.") == \
        "The study found a fast response. Starting from oxides here."
