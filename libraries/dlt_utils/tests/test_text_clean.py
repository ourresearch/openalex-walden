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
