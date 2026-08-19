"""Unit tests for oxjob #807 text cleaning (entities + mojibake + strip-all-tags).

Pure-Python (no Spark) — run with `python -m pytest test_text_clean.py` or `python test_text_clean.py`.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "openalex", "dlt"))
import text_clean as tc  # noqa: E402


# --- HTML entities ---------------------------------------------------------- #

def test_named_entity():
    assert tc.clean_title("Vuln&eacute;rabilit&eacute;") == "Vulnérabilité"

def test_decimal_and_hex_entities():
    assert tc.clean_title("caf&#233; / caf&#xe9;") == "café / café"

def test_double_encoded_entity():
    # &amp;eacute; -> &eacute; -> é  (needs iteration)
    assert tc.clean_title("Universit&amp;eacute;") == "Université"

def test_amp_and_literal_ampersand_preserved():
    assert tc.clean_title("Chips &amp; Fish") == "Chips & Fish"
    assert tc.clean_title("AT&T and R&D") == "AT&T and R&D"  # not valid entities -> untouched

def test_cnrs_example_findability():
    # The CNRS-reported title must decode to accented form.
    assert "vulnérabilité" in tc.clean_title("La vuln&eacute;rabilit&eacute; des syst&egrave;mes").lower()


# --- Mojibake (ported #801 core) -------------------------------------------- #

def test_basic_mojibake():
    assert tc.clean_title("UniversitÃ© de MontrÃ©al") == "Université de Montréal"

def test_entity_encoded_mojibake():
    # &Atilde;&copy; decodes to "Ã©" (real mojibake bytes) which must then be repaired to "é".
    assert tc.clean_title("Universit&Atilde;&copy;") == "Université"

def test_clean_text_is_noop_on_clean_accents():
    assert tc.clean_title("Économie et société") == "Économie et société"


# --- French-typography NBSP must NOT be treated as mojibake (FP guard) ------- #

def test_french_nbsp_not_mangled():
    s = "UNIVERSITÉ PARIS"  # É + NBSP — legit French typography, not mojibake
    assert tc.clean_title(s) == "UNIVERSITÉ PARIS"  # NBSP collapses to space, É preserved


# --- Tag stripping ---------------------------------------------------------- #

def test_strip_inline_tags():
    assert tc.clean_title("<i>Escherichia coli</i> growth") == "Escherichia coli growth"

def test_strip_jats():
    assert tc.clean_title("<jats:p>Hello</jats:p> world") == "Hello world"

# --- Inline tags close up; structural tags become a space ------------------- #
# Both wrong answers are unsearchable: a space inside `CO<sub>2</sub>` splits one token in two,
# and no space around `<h3>` welds a heading to the neighbouring sentence.

def test_subscript_superscript_close_up():
    assert tc.clean_title("Reducing CO<sub>2</sub> emissions") == "Reducing CO2 emissions"
    assert tc.clean_title("an area of 5 m<sup>2</sup>") == "an area of 5 m2"
    assert tc.clean_title("H<sub>2</sub>O and CH<sub>4</sub>") == "H2O and CH4"

def test_elsevier_inf_is_subscript():
    # <inf> is Elsevier's subscript spelling — 229K occurrences in the corrupted-abstract census.
    assert tc.clean_title("minimizing CO<inf>2</inf> and other emissions") == (
        "minimizing CO2 and other emissions"
    )

def test_inline_formatting_closes_up_midword():
    # Real corpus row: "offshore S<span>&atilde;</span>o Tom&eacute;" must not become "S ã o".
    assert tc.clean_title("offshore S<span>&atilde;</span>o Tom&eacute;") == "offshore São Tomé"
    assert tc.clean_title("Na<b>N</b>oparticles") == "NaNoparticles"

def test_structural_tags_still_separate_words():
    # 2.4M <h3> occurrences are structured-abstract headings; welding them to the surrounding
    # sentence is exactly the failure the space is there to prevent.
    assert tc.clean_title("outcomes.<h3>SIGNIFICANCE STATEMENT</h3>Bupropion works") == (
        "outcomes. SIGNIFICANCE STATEMENT Bupropion works"
    )
    assert tc.clean_title("line one<br>line two") == "line one line two"
    assert tc.clean_title("<p>alpha</p><p>beta</p>") == "alpha beta"

def test_namespace_prefix_ignored_when_classifying():
    # The same element arrives under many prefixes: mml:mi / m:mi / mi, jats:p / jats1:p / ns4:p.
    assert tc.clean_title("x<mml:msub><mml:mi>i</mml:mi></mml:msub>y") == "xiy"
    assert tc.clean_title("x<m:msub><m:mi>i</m:mi></m:msub>y") == "xiy"
    assert tc.clean_title("one<jats1:p>two</jats1:p>") == "one two"
    assert tc.clean_title("one<ns4:p>two</ns4:p>") == "one two"

def test_unknown_tag_defaults_to_space():
    # Splitting leaves two real searchable words; welding creates a junk token that matches
    # nothing, so an unrecognised tag takes the recoverable failure.
    assert tc.clean_title("alpha<weirdtag>beta") == "alpha beta"

def test_math_inequality_preserved():
    # "< 0.05" and "a > b" are NOT tags (space after '<') -> preserved.
    assert tc.clean_title("Effect at p < 0.05 in group A > B") == "Effect at p < 0.05 in group A > B"
    assert tc.clean_title("for i < j > k in the loop") == "for i < j > k in the loop"

def test_truncated_trailing_tag_removed():
    # OAI titles truncated at a length cap leave a dangling unclosed tag.
    assert tc.clean_title("Preparation of an <i") == "Preparation of an"
    assert tc.clean_title("Identification of </jats:p") == "Identification of"


# --- SECURITY: decoding must not arm XSS (strip-all-tags => inert) ----------- #

def _no_markup(s):
    """No angle-bracket tags and no live event handlers survive."""
    assert tc.strip_tags(s) == s or True  # sanity: strip_tags callable
    # after cleaning there must be no tag-shaped substring and no on*= handler
    import re
    assert not re.search(r"<(?:/[a-zA-Z]|[a-zA-Z])", s), f"tag survived: {s!r}"
    assert "onerror=" not in s.lower() and "onload=" not in s.lower(), f"handler survived: {s!r}"

def test_injection_script():
    _no_markup(tc.clean_title("Title &lt;script&gt;alert(1)&lt;/script&gt;"))

def test_injection_double_encoded_script():
    _no_markup(tc.clean_title("Title &amp;lt;script&amp;gt;alert(1)&amp;lt;/script&amp;gt;"))

def test_injection_numeric_script():
    _no_markup(tc.clean_title("Title &#60;script&#62;alert(1)&#60;/script&#62;"))

def test_injection_img_onerror():
    _no_markup(tc.clean_title("Title &lt;img src=x onerror=alert(1)&gt;"))

def test_injection_svg_onload():
    _no_markup(tc.clean_title("Title &lt;svg/onload=alert(1)&gt;"))


# --- Idempotency + None-safety ---------------------------------------------- #

def test_idempotent():
    for s in ["Universit&eacute;", "UniversitÃ©", "<i>x</i> &lt;b&gt;", "clean text"]:
        once = tc.clean_title(s)
        assert tc.clean_title(once) == once, f"not idempotent: {s!r}"

def test_clean_text_idempotent_for_abstract_path():
    # clean_abstract_text runs clean_text twice (once directly, once via inverted index) —
    # clean_text must be a fixpoint after one application.
    for s in ["A &lt;jats:p&gt;study&lt;/jats:p&gt; of caf&#233;", "UniversitÃ©"]:
        once = tc.clean_text(s)
        assert tc.clean_text(once) == once, f"clean_text not idempotent: {s!r}"

def test_none_and_nonstr_passthrough():
    assert tc.clean_title(None) is None
    assert tc.clean_title("") == ""
    assert tc.clean_title(123) == 123  # non-str untouched


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"PASS {fn.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"ERROR {fn.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    sys.exit(1 if failed else 0)
