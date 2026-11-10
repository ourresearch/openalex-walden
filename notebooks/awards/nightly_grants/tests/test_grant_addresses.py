"""Grant addresses, one definition (oxjob #2032): the award names Nightly Grants matches (sql/api_payload.sql's grant_addresses) are the
names PrepareAffiliationStrings sends to the matcher's lookup and the Affiliation Matcher Backlog takes as pending. Both carry a copy of
the CTE over openalex.awards.awards_api, because PrepareAffiliationStrings runs on a SQL warehouse and cannot read this file. These tests
fail when a copy differs from api_payload.sql (whitespace aside); fix it by pasting the CTE again."""
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
REPO = ROOT.parents[2]


def cte(text):
    """The SQL inside 'grant_addresses AS ( ... )': -- comments dropped, whitespace collapsed outside string literals, literals kept
    as written; parentheses inside literals and comments don't count. Block comments and backslash escapes are refused, not parsed."""
    k, depth, out = text.index("grant_addresses AS (") + len("grant_addresses AS ("), 1, []
    while True:
        ch = text[k]
        if ch == "\\" or text.startswith("/*", k):
            raise ValueError("grant_addresses: write it without block comments or backslash escapes")
        if ch == "'":
            end = text.index("'", k + 1)
            while text.startswith("''", end):
                end = text.index("'", end + 2)
            if "\\" in text[k:end]:
                raise ValueError("grant_addresses: write it without block comments or backslash escapes")
            out.append(text[k:end + 1])
            k = end + 1
            continue
        if text.startswith("--", k):
            k = text.index("\n", k)
            continue
        depth += {"(": 1, ")": -1}.get(ch, 0)
        if depth == 0:
            return "".join(out).strip()
        if not ch.isspace():
            out.append(ch)
        elif out and not out[-1].endswith(" "):
            out.append(" ")
        k += 1


PAYLOAD = (ROOT / "sql/api_payload.sql").read_text()
EXPECTED = cte(PAYLOAD.replace("{AWARDS}", "openalex.awards.awards_api"))


def test_api_payload_defines_grant_addresses_once_and_reads_people_from_it():
    assert PAYLOAD.count("grant_addresses AS (") == 1
    assert "FROM grant_addresses g" in PAYLOAD


def test_the_comparison_keeps_literals_exact_and_skips_comments():
    a = "grant_addresses AS (\n  x LIKE 'Institution abroad%'  -- a comment with ) and (\n  AND y IN ('a (b)', 'it''s')\n)"
    assert cte(a) == "x LIKE 'Institution abroad%' AND y IN ('a (b)', 'it''s')"
    assert cte(a.replace("Institution abroad%", "Institution  abroad%")) != cte(a)
    assert cte(a.replace("  AND", "\n      AND")) == cte(a)
    for refused in ("x /* ) */ y", "x IN ('O\\'Brien', 'D\\'Arcy')"):
        try:
            cte("grant_addresses AS (" + refused + ")")
        except ValueError:
            continue
        raise AssertionError("cte() accepted " + refused)


def test_prepare_affiliation_strings_carries_the_same_cte_before_the_works_merge():
    cells = json.loads((REPO / "notebooks/end2end/PrepareAffiliationStrings.ipynb").read_text())["cells"]
    code = [c["source"] if isinstance(c["source"], str) else "".join(c["source"]) for c in cells if c["cell_type"] == "code"]
    grants = [k for k, s in enumerate(code) if "grant_addresses AS (" in s]
    works = [k for k, s in enumerate(code) if "openalex.works.openalex_works_base" in s]
    assert len(grants) == 1 and cte(code[grants[0]]) == EXPECTED
    assert grants[0] < works[0]          # the nightly matcher takes the newest strings first: tonight's paper strings stay ahead


def test_the_backlog_carries_the_same_cte():
    text = (REPO / "notebooks/institutions/AffiliationMatcherBacklog.py").read_text()
    assert text.count("grant_addresses AS (") == 1 and cte(text) == EXPECTED
