"""CRediT roles read from PMC JATS <role> elements (oxjob #2075). Fixtures follow the three shapes seen in PMC:
NISO vocab attributes (MDPI, Frontiers), a credit.niso.org URL in content-type (PLOS), and plain text."""
import os
import sys
import xml.etree.ElementTree as ET

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from utils import pmc_jats  # noqa: E402

FRONT = b"""<?xml version="1.0"?>
<!DOCTYPE article PUBLIC "-//NLM//DTD JATS (Z39.96) Journal Archiving and Interchange DTD v1.3 20210610//EN" "JATS-archivearticle1-3.dtd">
<article article-type="research-article"><front><article-meta>
<contrib-group>
 <contrib contrib-type="author"><name><surname>Rivera</surname><given-names>Ana</given-names></name>
  <role vocab="credit" vocab-identifier="https://credit.niso.org/" vocab-term="Conceptualization"
        vocab-term-identifier="https://credit.niso.org/contributor-roles/conceptualization/">Conceptualization</role>
  <role vocab="credit" vocab-identifier="https://credit.niso.org/" vocab-term="Writing &#8211; original draft"
        vocab-term-identifier="https://credit.niso.org/contributor-roles/writing-original-draft/">Writing &#8211; original draft</role>
 </contrib>
 <contrib contrib-type="author"><name><surname>Maity</surname><given-names>B.</given-names></name>
  <role content-type="http://credit.niso.org/contributor-roles/writing-review-editing/">Writing &#8211; review &amp; editing</role>
  <role content-type="http://credit.niso.org/contributor-roles/software/">Software</role>
 </contrib>
 <contrib contrib-type="author"><name><surname>Lee</surname><given-names>C.</given-names></name>
  <role>Conceptualisation, Data curation; Writing&#8212;original draft preparation, Corresponding author</role>
  <role>Visualisation</role>
 </contrib>
 <contrib contrib-type="author"><name><surname>Nobody</surname><given-names>N.</given-names></name></contrib>
</contrib-group>
<contrib-group><contrib contrib-type="editor"><name><surname>Ed</surname></name><role>Academic Editor</role></contrib></contrib-group>
</article-meta></front><body/></article>"""


def test_roles_per_author():
    authors = pmc_jats.parse_front(FRONT)["authors"]
    assert [a["family"] for a in authors] == ["Rivera", "Maity", "Lee", "Nobody"]
    assert authors[0]["roles"] == ["conceptualization", "writing-original-draft"]
    assert authors[1]["roles"] == ["writing-review-editing", "software"]
    # plain text: split on commas/semicolons, British spellings, dashes; non-CRediT terms dropped
    assert authors[2]["roles"] == ["conceptualization", "data-curation", "writing-original-draft", "visualization"]
    assert authors[3]["roles"] == []


def test_non_credit_roles_dropped():
    c = ET.fromstring("<contrib><role>Editor</role><role>Author</role><role>Writing</role></contrib>")
    assert pmc_jats.credit_roles(c) == []


def test_slugs_are_the_fourteen():
    assert len(pmc_jats.CREDIT_ROLES) == 14
    assert set(pmc_jats._CREDIT_TERMS.values()) == set(pmc_jats.CREDIT_ROLES)
