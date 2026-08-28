"""oxjob #881: the split index builder must equal the clean-then-invert public path.

transform.py cannot be imported under the conftest pyspark stub (its normalize import touches
real StructType machinery), so the three pure functions are extracted by AST -- the same
functions, same source, no Spark.
"""
import ast
import os
import re

from openalex.dlt.text_clean import clean_text

_SRC = open(os.path.join(os.path.dirname(__file__), "..", "openalex", "dlt", "transform.py")).read()
_ns = {"clean_text": clean_text, "re": re}
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
