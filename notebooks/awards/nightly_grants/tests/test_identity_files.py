"""The award-key files match their generator, and FWF's part-project rule (oxjob #1765 c).

FWF lists a Special Research Area (F) or National Research Network (S) as one grant; papers cite its parts as the main
number plus two digits (F6804 = part 04 of F68). The deposited key drops the part and the registry key takes the two-digit
main number, so the two meet. The key cases run FWF's own patterns from scripts/award_translation_rules.py through Python's
re (the patterns use nothing Java and Python read differently); the same cases were run on Spark against every live FWF
number on 13 Nov 2026.
"""
import pathlib
import re
import sys

import pytest

if sys.version_info < (3, 12):   # the rules file nests backslashes in f-strings
    pytest.skip("scripts/award_translation_rules.py needs Python 3.12", allow_module_level=True)

ROOT = pathlib.Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "scripts"))

import gen_award_norm_key
from award_translation_rules import FUNDERS


def test_generated_key_files_equal_a_fresh_regeneration():
    sql = gen_award_norm_key.gen("openalex.awards")
    assert (ROOT / "notebooks/awards/AwardNormKey.sql").read_text() == sql, "run scripts/gen_award_norm_key.py"
    # the identity file is the function body as DESCRIBE FUNCTION EXTENDED prints it: the same query in one pair of parentheses
    body = re.search(r"FUNCTION openalex\.awards\.award_norm_key\(.*?\nRETURN\n(.*?);\n", sql, re.S).group(1)
    identity = ROOT / "notebooks/awards/nightly_grants/identity/udf_award_norm_key.body.sql"
    assert identity.read_text() == f"({body})\n", "udf_award_norm_key.body.sql does not match AwardNormKey.sql"


def fwf_key(number, side):
    """FWF's sharp key for a number, or None where the function falls back to the generic key."""
    expr = FUNDERS["FWF"]["rkey" if side == "registry" else "xkey"]
    patterns = re.findall(r"'(\^[^']*\$)', 1\)", expr)
    assert len(patterns) == expr.count("regexp_extract(regexp_replace(norm,' ',''),")   # every arm reads the space-stripped number
    text = number.upper().strip().replace(" ", "")
    for pattern in patterns:                                   # COALESCE order
        found = re.search(pattern.replace("\\\\", "\\"), text)
        if found and found.group(1):
            return found.group(1)
    return None


@pytest.mark.parametrize("cited,main", [
    ("F6804", "F68"), ("F 6804-N36", "F68"), ("f6811-n36", "F68"), ("10.55776/F6804", "F68"),
    ("https://doi.org/10.55776/s11407", "S114"), ("S11407-N23", "S114"), ("S 11405-N23", "S114"), ("S9607", "S96"),
    ("F8512", "F85"), ("F0504", "F05"),
])
def test_a_part_project_number_takes_the_main_grants_key(cited, main):
    assert fwf_key(cited, "deposited") == main
    assert fwf_key(main, "registry") == main                   # the grant FWF lists: 'F68', 'F 68'
    assert fwf_key(main[0] + " " + main[1:], "registry") == main


@pytest.mark.parametrize("number", [
    "F8100", "F8300",              # the grant itself: a part of 00
    "F1002", "F1003", "F100500",   # four-digit SFB numbers (2024 on) and their own 00 form
    "S114", "S100", "P31234", "PAT1041324", "W1234", "Z211", "I2096", "J4321", "ESP197",
])
def test_a_full_grant_number_keeps_its_key(number):
    assert fwf_key(number, "deposited") == number
    assert fwf_key(number, "registry") == number


@pytest.mark.parametrize("number,key", [
    ("I2096-B16", "I2096"),        # I20 is a different grant
    ("P3192321", "P3192321"), ("T579-N19", "T579"), ("E1243-N19", "E1243"), ("PUB1360", "PUB1360"),
    ("CM1105", "CM1105"), ("FG2700", "FG2700"), ("V1234", "V1234"), ("ESP1234567", "ESP1234567"),
    ("F100201", "F100201"), ("F68041", "F68041"), ("S1160821", "S1160821"),
])
def test_other_programmes_and_other_lengths_do_not_drop_digits(number, key):
    assert fwf_key(number, "deposited") == key


def test_only_two_digit_f_and_s_numbers_join_the_registry_key():
    for number in ("F8", "COE1", "FG31", "Z49", "T7", "CM11", "F6", "SFB68"):
        assert fwf_key(number, "registry") is None, number
    assert fwf_key("F68", "deposited") is None                 # a cited main number still matches by its spelling
