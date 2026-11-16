"""CIHR's award key: a program code written straight onto the number keeps the whole number.

CIHR serves each grant under its funding reference number (FRN), e.g. 134179_1; papers cite the first number, usually after
the program code: MOP-130465, PJT 153212, HBF134179. The deposited key strips the code. A code may end in a digit only when a
separator follows it (OV3-170344), or when it is two letters and a digit written onto exactly six digits (OV3170344). Before,
any code written onto the number took the number's first digit: HBF134179 keyed 34179.

How the cases are read: CIHR's patterns are taken from scripts/award_translation_rules.py, unescaped as Spark's SQL parser
unescapes string literals, and run with Python's re after the same normalisation the generator puts in front of every rule
(NORM: trim spaces, upper case, Unicode dashes to '-', Unicode spaces to ' '). re.ASCII keeps \\d to 0-9, as in Java. On
the 125 inputs of the 15 Nov review this reading equals java.util.regex for the old rule and this one. Not covered here:
upper case of the few letters where Spark and Python differ (e.g. 'ß'), and the generic a-z0-9 key the function falls back
to where CIHR's arm reads nothing (None below).
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
from award_translation_rules import FUNDERS, NORM


def test_generated_key_files_equal_a_fresh_regeneration():
    sql = gen_award_norm_key.gen("openalex.awards")
    assert (ROOT / "notebooks/awards/AwardNormKey.sql").read_text() == sql, "run scripts/gen_award_norm_key.py"
    # the identity file is the function body as DESCRIBE FUNCTION EXTENDED prints it: the same query in one pair of parentheses
    body = re.search(r"FUNCTION openalex\.awards\.award_norm_key\(.*?\nRETURN\n(.*?);\n", sql, re.S).group(1)
    identity = ROOT / "notebooks/awards/nightly_grants/identity/udf_award_norm_key.body.sql"
    assert identity.read_text() == f"({body})\n", "udf_award_norm_key.body.sql does not match AwardNormKey.sql"


def sql_literals(expr):
    """The string literals of a SQL expression, unescaped as Spark's parser does (a doubled backslash is one)."""
    return [literal.replace("\\\\", "\\") for literal in re.findall(r"'((?:[^'\\]|\\.)*)'", expr)]


NORM_STEPS = list(zip(*[iter(sql_literals(NORM.format(c="award_id")))] * 2))   # (pattern, replacement), in order


def normalise(number):
    text = number.strip(" ").upper()                           # Spark's TRIM strips spaces only
    for pattern, replacement in NORM_STEPS:
        text = re.sub(pattern, replacement, text, flags=re.ASCII)
    return text


def cihr_key(number, side):
    """CIHR's sharp key for a number, or None where the function falls back to the generic key."""
    patterns = [p for p in sql_literals(FUNDERS["CIHR"]["rkey" if side == "registry" else "xkey"]) if p.startswith("^")]
    text = normalise(number)
    if side == "registry":
        (key,) = patterns
    else:                                                      # strip '#', then the code, then read the number
        decoration, code, key = patterns
        text = re.sub(code, "", re.sub(decoration, "", text, flags=re.ASCII), flags=re.ASCII)
    found = re.search(key, text, flags=re.ASCII)
    return str(int(found.group(1))) if found else None


def test_the_normalisation_is_the_generators():
    assert len(NORM_STEPS) == 4
    assert [normalise(n) for n in ("mop–130465", "MOP 130465", "MOP  130465", " mop－130465 ")] == \
        ["MOP-130465", "MOP 130465", "MOP 130465", "MOP-130465"]


@pytest.mark.parametrize("cited,frn", [
    ("HBF134179", "134179_1"), ("MOP130465", "130465_1"), ("PJT153212", "153212_1"), ("CIHR153282", "153282_1"),
    ("# CGV127503", "127503_1"), ("#FDN143233", "143233_1"), ("FRN111242", "111242_1"), ("MOP89855", "89855_1"),
])
def test_a_code_written_onto_the_number_keeps_the_whole_number(cited, frn):
    assert cihr_key(cited, "deposited") == cihr_key(frn, "registry") == frn.split("_")[0]


@pytest.mark.parametrize("cited,key", [
    ("OV3170344", "170344"), ("VR1172711", "172711"), ("EH4201128", "201128"), ("VR1172711-1", "172711"),
    ("CA7170130", "170130"),
])
def test_two_letters_and_a_digit_written_onto_six_digits_are_a_code(cited, key):
    assert cihr_key(cited, "deposited") == key


@pytest.mark.parametrize("cited", [
    "CA128978", "GM123456", "NS104513", "DK62267", "HL12345", "AI1234", "R01CA128978", "5R01GM123456-03",
    "K08CA123456", "P30CA016672", "U54GM104940", "T32HL007081",
])
def test_nih_shaped_numbers_get_no_cihr_key(cited):
    assert cihr_key(cited, "deposited") is None                # two letters written straight onto the number are not read


@pytest.mark.parametrize("cited,key", [("PS175257", None), ("FS154313", None), ("PS 175257", "175257"),
                                       ("FS-154313", "154313"), ("CA 128978", "128978")])
def test_two_letters_need_a_separator(cited, key):
    # glued CIHR shorthands (PS, FS, RN, IC: 269 links on 15 Nov) stay unread, as the old rule left them unmatched;
    # a separated two-letter number reads as before, an NIH one included (3 links folded that way on 15 Nov)
    assert cihr_key(cited, "deposited") == key


@pytest.mark.parametrize("cited,key", [
    ("MOP-130465", "130465"), ("MOP 286719", "286719"), ("PJT 153212", "153212"), ("FDN-143265", "143265"),
    ("FRN 14058", "14058"), ("OV3-170344", "170344"), ("VR1 172711", "172711"), ("EH4-201128", "201128"),
    ("950-200986", "200986"), ("MOP 130465-1", "130465"), ("102709-1", "102709"), ("286719", "286719"),
    # the reviewer's inputs a reading without the normalisation or re.ASCII got wrong
    ("MOP–130465", "130465"), ("MOP 130465", "130465"), ("MOP  130465", "130465"), ("#  130465", "130465"),
    ("MOP－130465", "130465"),
])
def test_a_separated_code_keys_as_before(cited, key):
    assert cihr_key(cited, "deposited") == key


@pytest.mark.parametrize("cited,key", [
    ("PJT: 178385", "178385"), ("CBR#104072", "104072"), ("MOP #81213", "81213"), ("FRN# 114506", "114506"),
    ("MOP - 133712", "133712"), ("ACD- 187254", "187254"), ("MOP -74575", "74575"), ("VR5 #172669", "172669"),
    ("OV3:170344", "170344"), ("#MOP#130465", "130465"), ("PS: 175257", "175257"),
])
def test_a_code_followed_by_punctuation_keys_its_number(cited, key):
    assert cihr_key(cited, "deposited") == key


@pytest.mark.parametrize("cited,key", [("NO 130465", "130465"), ("NO: 130465", "130465"), ("APP: 568789", "568789"),
                                       ("REF# 12345", "12345"), ("ID: 1234", "1234")])
def test_a_label_word_reads_like_a_code(cited, key):
    # known: on 15 Nov this retired 4 paper-only CIHR records that the rule before did not (ID 1, APP 2, REF 1)
    assert cihr_key(cited, "deposited") == key


@pytest.mark.parametrize("cited", ["APP1043567", "DMS1234567", "RES0064610", "GNT1110773", "MOP0123456"])
def test_a_longer_code_ending_in_a_digit_written_onto_the_number_is_not_read(cited):
    assert cihr_key(cited, "deposited") is None                # the old rule keyed the last six digits


@pytest.mark.parametrize("cited", ["AB12345678", "11R06913", "#102 545", "GRANT: 130465", "NO. 130465", "MOP-١٣٠٤٦٥"])
def test_a_number_the_rule_cannot_read_has_no_cihr_key(cited):
    assert cihr_key(cited, "deposited") is None                # the function falls back to the generic key


def test_the_registry_key_is_the_frns_first_number():
    assert [cihr_key(n, "registry") for n in ("134179_1", "65929_2", "9855_1", "200597_1")] == ["134179", "65929", "9855", "200597"]
    assert cihr_key("MOP-130465", "registry") is None          # only CIHR's own NNNNNN_n form is a registry key
