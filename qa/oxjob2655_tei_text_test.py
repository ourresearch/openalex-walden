#!/usr/bin/env python3
"""Guard test for notebooks/ingest/PDF.py tei_text(): GROBID text units are separate words (oxjob #2655).

GROBID writes adjacent sentences, paragraphs and headings with nothing between them (<s>One.</s><s>Two.</s>,
<head>INTRODUCTION</head><p>Over...). Until #2655 the full text and the abstract were ''.join(itertext()), which glued
'pupil.' + 'Lower' into the one search token 'pupil.Lower', so the first word of almost every sentence could not be
found by full-text search. The fixtures are trimmed from real GROBID output (works W2060833070, W2023915892).

Runs the functions straight from the notebook source (ast), no Spark needed:

    python3 qa/oxjob2655_tei_text_test.py              # fixtures
    python3 qa/oxjob2655_tei_text_test.py <dir>        # also every <dir>/*.xml TEI: same characters, spaces added only
"""
import ast
import glob
import os
import re
import sys
import xml.etree.ElementTree as ET

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
PDF_PY = os.path.join(HERE, "..", "notebooks", "ingest", "PDF.py")
WANTED = {"TEI_INLINE", "tei_text", "clean_xml", "_safe_extract", "extract_fields",
          "MAX_TITLE_LENGTH", "MAX_ABSTRACT_LENGTH", "MAX_FULLTEXT_LENGTH"}


def load_notebook_functions():
    tree = ast.parse(open(PDF_PY).read())
    keep = []
    for node in tree.body:
        names = set()
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            names = {node.name}
        elif isinstance(node, ast.Assign):
            names = {t.id for t in node.targets if isinstance(t, ast.Name)}
        if names & WANTED:
            keep.append(node)
    found = {n.name if hasattr(n, "name") else n.targets[0].id for n in keep}
    assert found == WANTED, f"PDF.py lost {WANTED - found}"
    ns = {"re": re, "ET": ET, "pd": pd}
    exec(compile(ast.Module(body=keep, type_ignores=[]), PDF_PY, "exec"), ns)
    return ns


def tei(body, back="", abstract=""):
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0"><teiHeader xml:lang="en"><fileDesc><titleStmt>'
        '<title level="a" type="main">A title</title></titleStmt></fileDesc><profileDesc>'
        f'<abstract>\n{abstract}\n</abstract></profileDesc></teiHeader><text xml:lang="en"><body>\n{body}\n</body>'
        f'<back>\n{back}\n</back></text></TEI>'
    )


DIV = '<div xmlns="http://www.tei-c.org/ns/1.0">'
FIXTURE = tei(
    body=(
        f'{DIV}<head n="1.">INTRODUCTION</head><p><s>Over the past decade, more than one hundred extrasolar planets have '
        'been detected indirectly.</s><s>One of the next significant steps may be direct detection.</s></p><p><s>'
        'ExAO systems will be needed to control phase errors between ~3 cycles per pupil and ~30 cycles per pupil.</s>'
        '<s>Lower frequency errors will primarily rearrange light under the coronagraph occulting spot.</s></p></div>\n'
        f'{DIV}<head>Introduction</head><p><s>Flows of inertial particles have been extensively studied '
        '<ref type="bibr" target="#b2">(Boelle, Balzer &amp; Simonin 1995;</ref><ref type="bibr" target="#b8">Tsao '
        '&amp; Koch 1995)</ref>.</s><s>A transition was observed in dilute flows<ref type="bibr">1</ref>.</s></p></div>\n'
        '<figure xmlns="http://www.tei-c.org/ns/1.0" xml:id="fig_0"><head>FIGURE 1 .</head><label>1</label><figDesc>'
        '<div><p><s>Particle agitation as a function of the volume fraction.</s></p></div></figDesc></figure>\n'
        '<figure xmlns="http://www.tei-c.org/ns/1.0" type="table" xml:id="tab_0"><head>Table 1</head><table><row>'
        '<cell>Stokes</cell><cell>Agitation</cell></row></table></figure>'
    ),
    back=(
        '<div type="references"><listBibl><biblStruct xml:id="b0"><analytic><title level="a" type="main">Flow of '
        'particles</title><author><persName><forename type="first">M</forename><surname>Abbas</surname></persName>'
        '</author></analytic><monogr><title level="j">AIChE J</title><imprint><biblScope unit="volume">56</biblScope>'
        '<date type="published" when="2010">2010</date></imprint></monogr></biblStruct></listBibl></div>'
    ),
    abstract=(
        f'{DIV}<p><s>A review of existing theories is presented first.</s><s>A comparison between theoretical '
        'predictions and numerical results is made.</s></p></div>'
    ),
)


def old_join(xml, path):
    """What PDF.py stored before #2655, for comparison."""
    el = ET.fromstring(xml).find(path, {"tei": "http://www.tei-c.org/ns/1.0"})
    return " ".join("".join(el.itertext()).split())


def check_same_characters(fn, xml, label):
    """tei_text only ever adds spaces: no character is lost, moved or added."""
    for path in (".//tei:text", ".//tei:profileDesc/tei:abstract"):
        el = ET.fromstring(xml).find(path, {"tei": "http://www.tei-c.org/ns/1.0"})
        if el is None:
            continue
        new, old = fn["tei_text"](el), old_join(xml, path)
        assert new.replace(" ", "") == old.replace(" ", ""), f"{label} {path}: characters differ"
        assert len(new) >= len(old), f"{label} {path}: shorter"


def main():
    fn = load_notebook_functions()
    fields = fn["extract_fields"](FIXTURE)
    text, abstract = fields["fulltext"], fields["abstract"]

    must = [
        "INTRODUCTION Over the past decade",       # heading then paragraph
        "detected indirectly. One of the next",    # sentence then sentence
        "per pupil. Lower frequency errors",       # paragraph then paragraph (#2655's example, W2060833070)
        "Introduction Flows of inertial",
        "FIGURE 1 . 1 Particle agitation",         # caption head, label, description
        "Table 1 Stokes Agitation",                # table cells
        "M Abbas",                                 # reference fields
        "AIChE J 56 2010",
    ]
    for m in must:
        assert m in text, f"missing {m!r}"
    keep = [
        "studied (Boelle, Balzer & Simonin 1995;Tsao & Koch 1995).",  # inline citations read as before
        "dilute flows1.",                                             # superscript citation stays on its word
    ]
    for k in keep:
        assert k in text, f"inline element changed: {k!r}"
    for glued in ("pupil.Lower", "INTRODUCTIONOver", "indirectly.One"):
        assert glued not in text, f"still glued: {glued!r}"
    # the abstract is a served field: #2655 leaves its derivation as it was (a separate change, with its own stamps)
    assert abstract == old_join(FIXTURE, ".//tei:profileDesc/tei:abstract").replace("first. A", "first.A"), abstract
    assert "  " not in text and text == text.strip()
    check_same_characters(fn, FIXTURE, "fixture")
    # a parser that keeps comments: their text is never read, as itertext() never reads it
    root = ET.fromstring("<p xmlns='http://www.tei-c.org/ns/1.0'><s>One.</s><s>Two</s> words.</p>",
                         parser=ET.XMLParser(target=ET.TreeBuilder(insert_comments=True)))
    root.insert(1, ET.Comment("not text"))
    root[1].tail = "Tail"
    assert fn["tei_text"](root) == "One. Tail Two words.", fn["tei_text"](root)
    print(f"fixtures OK ({len(must) + len(keep) + 2} checks)")

    if len(sys.argv) > 1:
        files = sorted(glob.glob(os.path.join(sys.argv[1], "*.xml")))
        n = 0
        for f in files:
            xml = fn["clean_xml"](open(f, encoding="utf-8").read())
            try:
                ET.fromstring(xml)
            except ET.ParseError:
                continue
            check_same_characters(fn, xml, os.path.basename(f))
            n += 1
        print(f"{n} TEI files OK: same characters, spaces added only")


if __name__ == "__main__":
    main()
