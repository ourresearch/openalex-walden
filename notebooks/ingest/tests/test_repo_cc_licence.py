"""oxjob #2516: Creative Commons licences a repository record states in a code or in words.

Repo.py and RepoBackfill.py each hold the same pure-Python block (cc_licence_from_rights and its regexes). Plain Python,
no Spark: cut the block out of each notebook, check the two are identical, and run it on dc:rights values seen in
repo_items (22 Nov 2026).
"""
import re
import unicodedata
from pathlib import Path

import pytest

INGEST = Path(__file__).resolve().parents[1]
START = "# --- cc licence block (oxjob #2516): start."
END = "# --- cc licence block (oxjob #2516): end ---"


def _block(name):
    text = (INGEST / name).read_text()
    body = text[text.index(START):text.index(END)]
    return body.split("\n", 1)[1]  # without the start marker line, which names the other notebook


def _ns():
    repo = (INGEST / "Repo.py").read_text()
    m = re.search(r'^RIGHTS_RESTRICTED_PATTERN = (r".*")$', repo, re.M)
    ns = {"re": re, "unicodedata": unicodedata, "RIGHTS_RESTRICTED_PATTERN": eval(m.group(1))}
    exec(_block("Repo.py"), ns)
    return ns


NS = _ns()


def test_both_notebooks_hold_the_same_block():
    assert _block("Repo.py") == _block("RepoBackfill.py")


def test_both_notebooks_apply_it():
    repo = (INGEST / "Repo.py").read_text()
    backfill = (INGEST / "RepoBackfill.py").read_text()
    assert "cc_licence_from_rights_udf(F.when(" in repo
    assert 'F.lower(F.col("_license_prior")).startswith("cc")' in repo  # is_oa reads the pick, not the new licence
    assert '"_cc_licence"' in backfill and 'c not in ("_abstract_desc", "_cc_licence")' in backfill


CASES = [
    # EPrints licence codes
    ("cc_by_4", "cc-by"), ("cc_by", "cc-by"), ("cc_by_nc_nd", "cc-by-nc-nd"), ("cc_by_nc_nd_4", "cc-by-nc-nd"),
    ("cc_by_nc_sa_4_0", "cc-by-nc-sa"), ("cc_by_nd", "cc-by-nd"), ("cc_by_sa_4", "cc-by-sa"), ("cc_by_nc_4", "cc-by-nc"),
    ("cc_by_nc_nd4", "cc-by-nc-nd"), ("cc_by_4.0", "cc-by"), ("cc_by_4_0_ipl", "cc-by"),
    ("cc_public_domain", "public-domain"), ("cc_zero", "public-domain"),
    ("cc_by_og", None),  # DERA: the Open Government Licence
    ("cc_utas", None),
    # codes and abbreviations
    ("CC BY 4.0", "cc-by"), ("CC BY V4.0", "cc-by"), ("CC-BY-SA", "cc-by-sa"), ("cc by-nc-nd 4.0", "cc-by-nc-nd"),
    ("CC::CC BY 4.0", "cc-by"), ("CC BY + CC0", "cc-by"),
    ("http://spdx.org/licenses/CC-BY-NC-SA-4.0", "cc-by-nc-sa"),
    ("license:Licenza per Accesso Aperto. Creative Commons Attribuzione - Non commerciale - Non opere derivate (CCBYNCND)",
     "cc-by-nc-nd"),
    ("Licencia Creative Commons Atribución - No Comercial - Sin Derivadas (CC - By-NC-ND 4.0)", "cc-by-nc-nd"),
    ("Atribución-NoComercial-SinDerivadas 4.0 Internacional (CC BY-NC-ND 4.0)", "cc-by-nc-nd"),
    ("Creative Commons BY-NC 4.0", "cc-by-nc"), ("Creative Commons BY Attribution 4.0 International", "cc-by"),
    ("Licence Creative Commons « by/nc/nd »", "cc-by-nc-nd"),
    ("by-nc-nd", "cc-by-nc-nd"),
    # in words
    ("Creative Commons Attribution 4.0 International", "cc-by"),
    ("Creative Commons Attribution Non Commercial Share Alike 4.0 International", "cc-by-nc-sa"),
    ("creative_commons_attribution_noncommercial_noderivatives_4_0_international_license", "cc-by-nc-nd"),
    ("Attribution-NonCommercial-NoDerivatives 4.0 International", "cc-by-nc-nd"),
    ("Attribution Non-commercial No Derivatives", "cc-by-nc-nd"),
    ("Attribution 3.0 United States", "cc-by"),
    ("Attribution", "cc-by"), ("Reconocimiento", "cc-by"), ("Atribución", "cc-by"),
    ("Atribución-NoComercial-SinDerivadas 3.0 España", "cc-by-nc-nd"),
    ("Reconocimiento – NoComercial – SinObraDerivada", "cc-by-nc-nd"),
    ("Atribución – No comercial – Compartir igual", "cc-by-nc-sa"),
    ("Atribución – Sin Derivar", "cc-by-nd"),
    ("Licencja Creative Commons Uznanie autorstwa 4.0", "cc-by"),
    ("Udzielam licencji. Uznanie autorstwa - Użycie niekomercyjne - Na tych samych warunkach 4.0 Międzynarodowa",
     "cc-by-nc-sa"),
    ("Creative Commons - Namensnennung, Nicht kommerz., Keine Bearbeitung 4.0", "cc-by-nc-nd"),
    ("Namensnennung - Weitergabe unter gleichen Bedingungen 4.0 International", "cc-by-sa"),
    ("Creative Commons Namensnennung - Keine kommerzielle Nutzung-Keine Bearbeitung 3.0 Deutschland", "cc-by-nc-nd"),
    ("S'autoritza la difusió de l'obra mitjançant la llicència Creative Commons o similar "
     "'Reconeixement-NoComercial- SenseObraDerivada'", "cc-by-nc-nd"),
    ("Licença pública Creative Commons Licença 4.0 Atribuição-NãoComercial-SemDerivações 4.0 Internacional", "cc-by-nc-nd"),
    ("Estudos de Literatura Brasileira Contemporânea é licenciada sob uma Licença Creative Commons Atribuição-Uso "
     "não-comercial-Vedada a criação de obras derivadas 3.0 Unported.", "cc-by-nc-nd"),
    ("クリエイティブ・コモンズ 表示 - 非営利 - 改変禁止 4.0 国際", "cc-by-nc-nd"),
    ("Authors who publish with this journal agree to the following terms:Authors retain copyright and grant the journal "
     "right of first publication with the work simultaneously licensed under a Creative Commons Attribution License that "
     "allows others to share the work", "cc-by"),
    # prose after the licence name adds nothing
    ("This is an open access article distributed under the terms of the Creative Commons Attribution License, which "
     "permits unrestricted use for commercial and non-commercial purposes", "cc-by"),
    # names no licence
    ("KOGL BY-NC-ND", None), ("Creative Commons", None), ("license:Creative commons", None), ("CC0", None),
    ("This item is likely protected under Title 17 of the U.S. Copyright Law. Unless on a Creative Commons license, for "
     "uses protected by Copyright Law, contact the copyright holder or the author.", None),
    ("All items in Research Commons are provided for private study and research purposes and are protected by copyright "
     "with all rights reserved unless otherwise indicated.", None),
    ("No Creative Commons licence (All rights reserved)", None),
    ("© Saint Mary’s University. All rights reserved. With attribution, users are permitted to access, display and copy "
     "portions of this title for individual and educational purposes only.", None),
    ("Attribution required for any reuse", None),
    ("Copyright © 2014 the author(s). Theses may be used for non-commercial research, educational, or related academic "
     "purposes only.", None),
    ("Open Data Commons Attribution License (ODC-By)", None), ("http://opendatacommons.org/licenses/by/1.0/", None),
    ("info:eu-repo/semantics/openAccess", None), ("Copyright (c) 2022 Universidad Pedagógica y Tecnológica", None),
    ("Creative Commons Attribution-NoDerivs-ShareAlike", None),  # ND with SA is no licence
    ("", None), (None, None),
]


@pytest.mark.parametrize("value,expected", CASES)
def test_cc_licence_from_text(value, expected):
    assert NS["cc_licence_from_text"](value) == expected


def test_first_value_that_names_a_licence_wins():
    f = NS["cc_licence_from_rights"]
    # W7133408444 (UPTC): a copyright line first, the licence second
    assert f(["Copyright (c) 2022 Universidad Pedagógica y Tecnológica de Colombia",
              "Atribución-NoComercial-SinDerivadas 4.0 Internacional (CC BY-NC-ND 4.0)",
              "info:eu-repo/semantics/openAccess"]) == "cc-by-nc-nd"
    assert f(["cc_by", "info:eu-repo/semantics/openAccess"]) == "cc-by"  # W3201697337 (HELDA)
    assert f(["cc_by_nc_nd", "cc_by_4"]) == "cc-by-nc-nd"
    assert f(["info:eu-repo/semantics/openAccess", None]) is None
    assert f(None) is None and f([]) is None


@pytest.mark.parametrize("value,blocks", [
    ("info:eu-repo/semantics/restrictedAccess", True), ("http://purl.org/coar/access_right/c_14cb", True),
    ("http://purl.org/coar/access_right/c_f1cf", True), ("Metadata only", True),
    ("http://rightsstatements.org/vocab/InC/1.0/", True),
    ("info:eu-repo/semantics/openAccess", False), ("http://purl.org/coar/access_right/c_abf2", False), ("cc_by", False),
])
def test_block_pattern(value, blocks):
    norm = re.sub(r"[ -]", "", value.lower())
    assert (re.search(NS["CC_RIGHTS_BLOCK_PATTERN"], norm) is not None) == blocks
