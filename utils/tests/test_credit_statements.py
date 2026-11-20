"""Tests for the full-text CRediT statement parser (oxjob #2450). Statements are real GROBID text, trimmed; a few are
adapted so that each test isolates one rule."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from utils.credit_statements import CREDIT_ROLES, parse  # noqa: E402
from utils.pmc_jats import CREDIT_ROLES as PMC_ROLES  # noqa: E402


def roles(text, byline):
    r = parse(text, byline)
    return r.status, r.roles


def test_term_first_initials_mdpi():
    text = ("Funding: This study was supported by ... Conflicts of Interest:The authors declare no conflict of interest. "
            "Author Contributions: Conceptualization, L.Y. and J.L.; data curation, G.C. and J.L.; formal analysis, "
            "L.Y., L.Z., G.C. and S.W.; funding acquisition, J.L.; methodology, L.Y.; project administration, J.L.; "
            "resources, L.Y., L.Z., S.W. and L.W.; supervision, J.L.; validation, L.Y.; writing-original draft, L.Y.; "
            "writing-review and editing, J.L.All authors have read and agreed to the published version of the "
            "manuscript. Supplementary Materials:The following are available online")
    st, r = roles(text, ["Lu Yang", "Ling Zhang", "Gong Chen", "Shan Wang", "Lei Wang", "Jianhua Li"])
    assert st == "ok"
    assert r[0] == ["conceptualization", "formal-analysis", "methodology", "resources", "validation",
                    "writing-original-draft"]
    assert r[5] == ["conceptualization", "data-curation", "funding-acquisition", "project-administration",
                    "supervision", "writing-review-editing"]
    assert r[4] == ["resources"]


def test_name_first_full_names_with_degrees():
    text = ("Conflict of InterestThe authors have no conflicts to disclose. Author ContributionsDawei Li: Data curation "
            "(equal); Formal analysis (equal); Investigation (equal); Writingoriginal draft (equal).Hongbo Liu: Funding "
            "acquisition (equal); Methodology (equal); Supervision (equal); Writingoriginal draft (lead); Writingreview "
            "& editing (lead).Luqiu Chen: Methodology (equal). DATA AVAILABILITY The data ...")
    st, r = roles(text, ["Dawei Li", "Hongbo Liu", "Luqiu Chen"])
    assert st == "ok"
    assert r[0] == ["data-curation", "formal-analysis", "investigation", "writing-original-draft"]
    assert r[1] == ["funding-acquisition", "methodology", "supervision", "writing-original-draft",
                    "writing-review-editing"]
    assert r[2] == ["methodology"]


def test_term_first_colon_full_names_then_references():
    text = ("Author ContributionsConceptualization: Yao Wang, Jiahui Ling. Data curation: Yao Wang. Formal analysis: "
            "Jiahui Ling.Investigation: Yao Wang. Writing -original draft: Jiahui Ling.Writing -review & editing: "
            "Jiahui Ling. Land use policy in China: Introduction HLong Land Use Policy 40 2014 Long H.")
    st, r = roles(text, ["Yao Wang", "Jiahui Ling"])
    assert st == "ok"
    assert r[0] == ["conceptualization", "data-curation", "investigation"]
    assert r[1] == ["conceptualization", "formal-analysis", "writing-original-draft", "writing-review-editing"]


def test_name_first_without_colon_ends_the_entry():
    # "MS Data Collection, Methodology." belongs to MS (no colon), not to AA
    text = ("Author contributions FB: Data curation, Supervision, Writing -original draft.AA: Investigation, Project "
            "administration, Writing -original draft.MS Data Collection, Methodology.MC: Conceptualization, Formal "
            "analysis, Methodology, Supervision, Writing -review & editing. Funding The author(s) declare ...")
    st, r = roles(text, ["Fatemeh Babadi", "Ahmad Ahmadi", "Mohsen Sarkarian", "Maria Cheraghi"])
    assert st == "ok"
    assert r[1] == ["investigation", "project-administration", "writing-original-draft"]
    assert 2 not in r
    assert r[3] == ["conceptualization", "formal-analysis", "methodology", "supervision", "writing-review-editing"]


def test_ambiguous_initials_are_skipped():
    text = ("Author Contributions: Conceptualization, J.Z.; software, Y.H.; writing-original draft preparation, J.Z. "
            "and Y.H.; writing-review and editing, J.Z. All authors have read and agreed")
    st, r = roles(text, ["Jun Zhang", "Jie Zhao", "Yu He"])
    assert st == "ok"
    assert set(r) == {2}           # J.Z. fits two authors: nobody gets J.Z.'s roles
    assert r[2] == ["software", "writing-original-draft"]


def test_statement_naming_other_people_is_dropped():
    # stored full text of another paper (#2281): the names don't fit this byline
    text = ("Author Contributions: Conceptualization, A.B. and C.D.; methodology, E.F.; writing-original draft, A.B.; "
            "writing-review and editing, C.D. and E.F. All authors have read")
    st, _ = roles(text, ["Maria Lopez", "Tom Smith"])
    assert st != "ok"


def test_all_authors_and_hyphenated_initials():
    text = ("Authors' contributions Conceptualization: JC-I and RS-H. Investigation: MI-L, RS-H, and KW. Writing - "
            "original draft: MI-L. Writing - review & editing: all authors. Funding This work ...")
    by = ["María Isabel Iñiguez-Luna", "Jorge Cadena-Iñiguez", "Ramón Marcos Soto-Hernández", "Kazuo N. Watanabe"]
    st, r = roles(text, by)
    assert st == "ok"
    assert r[0] == ["investigation", "writing-original-draft", "writing-review-editing"]
    assert r[1] == ["conceptualization", "writing-review-editing"]
    assert r[2] == ["conceptualization", "investigation", "writing-review-editing"]
    assert r[3] == ["investigation", "writing-review-editing"]


def test_free_form_prose_is_not_read():
    text = ("Author contributionsThis study was conceptualized by L.C., F.S. and G.P. The original draft was written "
            "by L.C. while L.C. and F.S. revised the final manuscript. G.P. supervised the study.")
    st, _ = roles(text, ["Lorenzo Colombini", "Francesco Santoro", "Gianni Pozzi"])
    assert st != "ok"


def test_table_row_with_lost_names_is_not_shared():
    # "Methodology: Visualization: X" is a GROBID-flattened table: Methodology's names are lost, not X's
    text = ("Author Contributions Conceptualization: Vanessa Ferreira Methodology: Visualization: Vanessa Ferreira "
            "Writing -original draft: Vanessa Ferreira Conflict of Interest")
    st, r = roles(text, ["Vanessa Ferreira"])
    assert st == "ok"
    assert "methodology" not in r[0]
    assert r[0] == ["conceptualization", "visualization", "writing-original-draft"]


def test_parenthetical_full_name_settles_shared_initials():
    text = ("Author Contributions: Conceptualization, M.B.; methodology, G.C. (Giuliano Costa) and M.F.; validation, "
            "G.C. (Giuseppe Castania); writing-original draft preparation, M.F. All authors have read")
    by = ["Marco Frazzetto", "Giuliano Costa", "Giuseppe Castania", "Marco Barbanti"]
    st, r = roles(text, by)
    assert st == "ok"
    assert r[1] == ["methodology"]
    assert r[2] == ["validation"]
    assert r[3] == ["conceptualization"]


def test_initials_that_fit_two_ways_are_skipped():
    # "JS" fits John Smith (every word) and James Alan Smith (middle name left out)
    text = ("Author Contributions: Conceptualization, JS and M.K.; writing-original draft, JS; writing-review and "
            "editing, M.K. All authors have read")
    st, r = roles(text, ["John Smith", "James Alan Smith", "Maria Klein"])
    assert st == "ok"
    assert set(r) == {2}


def test_same_name_twice_on_the_byline_is_ambiguous():
    text = ("Author Contributions: Conceptualization, W.W. and Y.L.; writing-original draft, W.W.; writing-review and "
            "editing, Y.L. All authors have read")
    st, r = roles(text, ["Wei Wang", "Yan Li", "Wei Wang"])
    assert st == "ok"
    assert set(r) == {1}


def test_all_authors_alone_does_not_prove_the_text_is_this_papers():
    text = ("Author Contributions: Conceptualization, all authors; writing-original draft, all authors. "
            "Funding: none")
    st, _ = roles(text, ["Maria Lopez", "Tom Smith"])
    assert st != "ok"


def test_unmatched_full_names_count_against_the_byline():
    text = ("Author Contributions: Conceptualization, Alice Smith; methodology, Robert Brown; writing-original draft, "
            "Robert Brown. Funding: none")
    st, _ = roles(text, ["Alice Smith", "Tom Jones"])
    assert st != "ok"


def test_names_before_terms_without_colons_are_not_read_term_first():
    # reading this term first would give J.S.'s roles to J.D. and J.D.'s to B.L.
    text = ("Author contributions J.S. Conceptualization, Investigation. J.D. Methodology, Writing - original draft. "
            "B.L. Supervision. Funding none")
    st, r = roles(text, ["Jane Smith", "John Doe", "Bo Lee"])
    assert st != "ok" or (r.get(1, []) != ["conceptualization", "investigation"])


def test_term_first_with_dashes_is_not_read_name_first():
    text = ("Author Contributions Conceptualization: Jane Smith, John Doe - Methodology, Validation: Jane Smith. "
            "Writing - original draft: John Doe. Funding none")
    st, r = roles(text, ["Jane Smith", "John Doe"])
    assert st == "ok"
    assert "methodology" not in r.get(1, [])
    assert r[0] == ["conceptualization", "methodology", "validation"]


def test_prose_after_a_term_is_not_a_surname():
    text = ("Author Contributions: Conceptualization, A.M. and J.D.; writing-original draft, A.M.; writing-review and "
            "editing, J.D.; resources: may be requested from the corresponding author. Funding: none")
    st, r = roles(text, ["Anna May", "John Doe"])
    assert st == "ok"
    assert "resources" not in r.get(0, [])


def test_same_role_list_as_the_structured_readers():
    assert CREDIT_ROLES == PMC_ROLES
