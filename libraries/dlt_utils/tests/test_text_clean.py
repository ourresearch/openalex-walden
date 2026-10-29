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

def test_mathml_leaves_close_up_but_wrappers_separate():
    # Leaf/grouping math closes up: <mi>x</mi><mo>+</mo><mi>y</mi> is the expression "x+y".
    assert tc.clean_title("<mml:mrow><mml:mi>x</mml:mi><mml:mo>+</mml:mo><mml:mi>y</mml:mi></mml:mrow>") == "x+y"

def test_mathml_annotation_does_not_weld_duplicate_representations():
    # MathML carries the same expression twice — presentation markup AND a TeX <annotation>.
    # Treating the wrapper as inline welds them into one junk token ("x+yx+y"). ~4% of
    # annotation-bearing corpus rows have no whitespace between the elements to save us.
    s = ("<mml:math><mml:semantics><mml:mrow><mml:mi>x</mml:mi><mml:mo>+</mml:mo>"
         "<mml:mi>y</mml:mi></mml:mrow>"
         "<mml:annotation encoding=\"TeX\">x+y</mml:annotation></mml:semantics></mml:math>")
    assert tc.clean_title(s) == "x+y x+y"

def test_formula_wrapper_separates_from_prose():
    # An unspaced formula wrapper must not weld the maths onto the neighbouring word.
    assert tc.clean_title("shown<inline-formula><tex-math>$L_X$</tex-math></inline-formula>here") == (
        "shown $L_X$ here"
    )

def test_unknown_tag_defaults_to_space():
    # Splitting leaves two real searchable words; welding creates a junk token that matches
    # nothing, so an unrecognised tag takes the recoverable failure. (An unknown name counts as a
    # tag when it is closed later or carries an attribute value; oxjob #1928.)
    assert tc.clean_title("alpha<weirdtag>beta</weirdtag>") == "alpha beta"
    assert tc.clean_title('alpha<weirdtag class="x">beta') == "alpha beta"

def test_math_inequality_preserved():
    # "< 0.05" and "a > b" are NOT tags (space after '<') -> preserved.
    assert tc.clean_title("Effect at p < 0.05 in group A > B") == "Effect at p < 0.05 in group A > B"
    assert tc.clean_title("for i < j > k in the loop") == "for i < j > k in the loop"

def test_truncated_trailing_tag_removed():
    # OAI titles truncated at a length cap leave a dangling unclosed tag.
    assert tc.clean_title("Preparation of an <i") == "Preparation of an"
    assert tc.clean_title("Identification of </jats:p") == "Identification of"
    assert tc.clean_title("u ( 0 ) = 0 , D T</mml:") == "u ( 0 ) = 0 , D T"


# --- SECURITY: decoding must not arm XSS (strip-all-tags => inert) ----------- #

def _no_markup(s):
    """No markup-named tag and no live event handler or script URL survives. A `<` before a word
    that is not markup is text and may stay (oxjob #1928), so the check is on names, not on `<`."""
    import re
    for m in re.finditer(r"</?([A-Za-z][A-Za-z0-9._-]*)", s):
        assert m.group(1).lower() not in tc._MARKUP_NAMES - tc._WORD_NAMES, f"tag survived: {s!r}"
    assert not re.search(r"(?i)\bon[a-z]+\s*=|javascript:", s), f"handler survived: {s!r}"

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


def test_injection_unknown_element_with_handler():
    # Browsers run handlers on unknown elements too; a tag-shaped span carrying one always goes.
    _no_markup(tc.clean_title("Title &lt;x onclick=alert(1)&gt;"))
    _no_markup(tc.clean_title("Title &lt;x onclick=alert(1) (y&gt;"))
    _no_markup(tc.clean_title('Title &lt;a href="javascript:alert(1)"&gt;x&lt;/a&gt;'))

def test_injection_script_with_bare_attribute_and_dangerous_bare_elements():
    _no_markup(tc.clean_title("Title &lt;script x&gt;alert(1)"))
    for name in ("script", "svg", "math", "iframe", "style", "noscript", "template", "textarea"):
        out = tc.clean_title(f"Title &lt;{name}&gt;payload")
        assert f"<{name}" not in out.lower(), out

def test_injection_handler_in_a_stretch_that_is_not_a_clean_tag():
    # A browser reads "<i onclick=... x ... >" as one tag even with text in between; it must not survive.
    _no_markup(tc.clean_title("a &lt;i onclick=alert(1) x&lt;b&gt;y and p&gt;0.05"))
    _no_markup(tc.clean_title("a &lt;img src=x onerror=alert(1) &lt;3&gt;"))
    assert tc.clean_title("onset (&lt;age 25 years) one = 5 and p&gt;0.05") == "onset (<age 25 years) one = 5 and p>0.05"

def test_cut_off_tags_at_the_end():
    for s, want in [("of the algebra <mml:math xmlns:", "of the algebra"), ("P ( Y n = k <mml:mo st", "P ( Y n = k"),
                    ("in an economy<span xml:lang", "in an economy"), ('see <a href="http://x.org/a', "see"),
                    ("comment </p --> more", "comment more"),
                    ("D(P/spl par/Q)</spl infin/. We ask when p>0", "D(P/spl par/Q)</spl infin/. We ask when p>0"),
                    ("Δt <p for a buildup test", "Δt <p for a buildup test"), ("values at <LOD in", "values at <LOD in")]:
        assert tc.clean_title(s) == want, (s, tc.clean_title(s))


def test_injection_truncated_handler_at_end():
    _no_markup(tc.clean_title("Title &lt;x onmouseover=alert(1)"))


# --- A less-than that is not markup is text (oxjob #1928) ------------------- #
# The old rule removed every '<'+letter up to the next '>' (or the end of the text), cutting a median
# 17 words from ~24K Crossref abstracts. Cases below are real deposits (DOIs in comments).

def test_inequality_before_a_word_keeps_the_rest_of_the_abstract():
    # 10.1071/mf9941007 kept 21 of 277 words; 10.1046/j.1365-2796.1997.90111000.x 12 of 102.
    s = "otoliths of juveniles (&lt;I00 days old) were read. Ages agreed in 90% of fish &gt;1 year old."
    assert tc.clean_title(s) == "otoliths of juveniles (<I00 days old) were read. Ages agreed in 90% of fish >1 year old."
    s = "developed severe, systemic, early onset (&lt;age 25 years) disease. The proband had p&gt;0.05."
    assert tc.clean_title(s) == "developed severe, systemic, early onset (<age 25 years) disease. The proband had p>0.05."

def test_inequality_chains_and_detection_limits():
    assert tc.clean_title("in the range 30&lt;Rλ&lt;130 we find") == "in the range 30<Rλ<130 we find"     # 10.1063/1.868219
    assert tc.clean_title("salinity CK&lt;RSA&lt;RS&lt;RSB was") == "salinity CK<RSA<RS<RSB was"     # 10.1002/ldr.5442
    assert tc.clean_title("patients who had &lt;VGPR (P = 0.05) and") == "patients who had <VGPR (P = 0.05) and"
    assert tc.clean_title("losartan &lt;telmisartan &lt;irbesartan via CYP2C9") == "losartan <telmisartan <irbesartan via CYP2C9"
    assert tc.clean_title("values were &lt;LOD") == "values were <LOD"              # trailing, not a cut-off tag
    assert tc.clean_title("for all 0&lt;x&lt;1 and p&lt;a.") == "for all 0<x<1 and p<a."

def test_angle_quoted_titles_and_names_stay():
    assert tc.clean_title("Study on Close-Up Shots in Film &lt;The Throne&gt;(2015)") == (
        "Study on Close-Up Shots in Film <The Throne>(2015)")
    assert tc.clean_title("&lt;Hygrophila phlomoides&gt; Nees (Acanthaceae), a new record") == (
        "<Hygrophila phlomoides> Nees (Acanthaceae), a new record")
    assert tc.clean_title("life was &lt;the only Sun in the sky&gt;, and") == "life was <the only Sun in the sky>, and"
    assert tc.clean_title("&lt;uo&gt; and &lt;uu&gt; for /wu/") == "<uo> and <uu> for /wu/"     # graphemes
    assert tc.clean_title("Film &lt;Comment&gt; review") == "Film <Comment> review"           # a word, no markup around
    assert tc.clean_title("write to &lt;fimte@fimte.org&gt;") == "write to <fimte@fimte.org>"
    assert tc.clean_title("see &lt;http://www.adaa.org/&gt;") == "see <http://www.adaa.org/>"

def test_word_named_tags_are_markup_inside_marked_up_text():
    assert tc.clean_title("<sec><title>Background</title><p>We study</p></sec>") == "Background We study"
    assert tc.clean_title("<jats:title>Background") == "Background"

def test_markup_still_removed_next_to_text_less_thans():
    s = "&lt;jats:p&gt;Doses &lt;5 mg and &lt;LOD in &lt;i&gt;E. coli&lt;/i&gt;&lt;/jats:p&gt;"
    assert tc.clean_title(s) == "Doses <5 mg and <LOD in E. coli"
    assert tc.clean_title('a <span style="font-size: 12pt;>x</span> b') == "a x b"   # malformed but markup
    assert tc.clean_title("grade <I (n=5) vs grade >II") == "grade <I (n=5) vs grade >II"  # not markup

def test_reversed_tags_are_repaired():
    # USP theses and some IEEE titles arrive with every tag's brackets swapped.
    assert tc.clean_title("em &gt;i&lt;Pinus&gt;/i&lt; tropicais") == "em Pinus tropicais"
    assert tc.clean_title("&gt;b&lt;Consumo de peixe&gt;/b&lt;: análise") == "Consumo de peixe: análise"
    assert tc.clean_title("Cr&gt;sup&lt;4+&gt;/sup&lt; ions") == "Cr4+ ions"
    assert tc.clean_title("for j&gt;i&lt;n and k") == "for j>i<n and k"   # no swapped closing tag: prose
    # Real tags side by side are not swapped ones: ">/p<" here ends </sup> and starts <sup>.
    assert tc.clean_title("n&lt;sup&gt;+&lt;/sup&gt;/p&lt;sup&gt;+&lt;/sup&gt; poly-Si") == "n+/p+ poly-Si"
    assert tc.clean_title("(Al<sub>2</sub>O<sub>3</sub>/B<sub>4</sub>C)") == "(Al2O3/B4C)"
    assert tc.clean_title("HER2<sup>+</sup>/HR<sup>−</sup> Trial") == "HER2+/HR− Trial"

def test_attribute_check_stays_linear():
    # An unquoted value that could stop at any `/` made the attribute pattern backtrack
    # exponentially ("x=a/b=a/b=..."): 30 segments took minutes, so one deposit could stall a task.
    import time
    t = time.perf_counter()
    tc.strip_tags('<a href=http://x.org/q?a=1' + '/b=2' * 60 + '">x')
    tc.strip_tags('<jats:p x=a' + '/b=a' * 60 + '">x')
    assert time.perf_counter() - t < 1.0
    assert tc.clean_title('<a href=http://x.org/q?a=1>link</a> text') == "link text"


def test_emergent_tag_is_stripped_in_the_same_call():
    # Removing inner markup can leave a new tag-shaped span; the result must already be final,
    # because the ES sync cleans the stored abstract again (#191.1).
    s = "velocity &lt;<jats:italic>u</jats:italic>&gt; across"
    once = tc.clean_text(s)
    assert tc.clean_text(once) == once


# --- Idempotency + None-safety ---------------------------------------------- #

def test_idempotent():
    for s in ["Universit&eacute;", "UniversitÃ©", "<i>x</i> &lt;b&gt;", "clean text",
              "(&lt;I00 days old) &gt; x", "Film &lt;The Throne&gt;", "&gt;i&lt;Pinus&gt;/i&lt;",
              "a &lt;<i>u</i>&gt; b", "values &lt;LOD", "<sec><title>T</title></sec> &lt;Comment&gt;"]:
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
