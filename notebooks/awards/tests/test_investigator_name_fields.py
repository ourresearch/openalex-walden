"""Investigator names in the right field (16 Nov): parsers that printed a person's whole name, or a swapped name, in one field.

Each test pins the parser's name expressions in its SQL, and what they give on sample rows (real names from each raw object, 16 Nov)
is mirrored in Python: Spark's regexes are Java regex, and these patterns use nothing Python reads differently (ASCII classes only;
Spark gave the same output on every sample, 16 Nov). The shared guard holds every sample to the two rules a misplaced name breaks.
"""
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
AWARDS = ROOT / 'notebooks/awards'


def source(name):
    return (AWARDS / f'sources/{name}.sql').read_text()


def squash(s):
    return ' '.join(s.split())


def sql_regex(literal):
    """A Spark SQL string literal's text -> the regex Spark compiles ('\\\\s' in the SQL is \\s)."""
    return literal.replace('\\\\', '\\')


def trim(s):
    """Spark's TRIM: spaces only."""
    return None if s is None else s.strip(' ')


def nullif_empty(s):
    return None if s is None or s == '' else s


# ---------------------------------------------------------------- SSHRC: 'Family, Given'

COMMA_GIVEN = "CASE WHEN {c} LIKE '%,%' THEN NULLIF(TRIM(SUBSTRING({c}, INSTR({c}, ',') + 1)), '') END as given_name"
COMMA_FAMILY = "CASE WHEN {c} LIKE '%,%' THEN TRIM(SUBSTRING_INDEX({c}, ',', 1)) ELSE TRIM({c}) END as family_name"


def comma_split(name):
    if ',' in name:
        return nullif_empty(trim(name[name.index(',') + 1:])), trim(name.split(',', 1)[0])
    return None, trim(name)


# ---------------------------------------------------------------- CFI: 'Given Family'

CFI_WORDS = "filter(split(TRIM(SPLIT(team_leaders, '[|,]')[0]), ' '), t -> t <> '') AS leader_words"
CFI_GIVEN = "CASE WHEN size(g.leader_words) > 1 THEN array_join(slice(g.leader_words, 1, size(g.leader_words) - 1), ' ') END as given_name"
CFI_FAMILY = "try_element_at(g.leader_words, -1) as family_name"


def cfi_cell():
    cells = [c['source'] if isinstance(c['source'], str) else ''.join(c['source'])
             for c in json.loads((AWARDS / 'CreateCFIAwards.ipynb').read_text())['cells']]
    found = [c for c in cells if 'CREATE OR REPLACE TABLE openalex.awards.cfi_awards' in c]
    assert len(found) == 1
    return found[0]


def cfi_split(team_leaders):
    words = [t for t in trim(re.split('[|,]', team_leaders)[0]).split(' ') if t != '']
    return (' '.join(words[:-1]) if len(words) > 1 else None), (words[-1] if words else None)


# ---------------------------------------------------------------- ZonMw: '[titles] initials [prefix] surname [degrees]'

ZONMW_TITLES = r"^(([Pp]rof|[Dd]rs?|[Ii]r|[Ii]ng|[Mm]r|[Ee]m|[Dd]hr|[Mm]evr|[Mm]w|[Mm]\\.?[Ss]c|[Bb]\\.?[Ss]c|md|[Pp]h[Dd]|[Dd]ipl)\\.?\\s+)+"
ZONMW_DEGREES = r"(\\s+(PhD|PHD|MD|MSc|BSc|MBA|MA|MPH|BA|MHA|LLM|MPA|MPhil|em\\.))+$"
ZONMW_INITIALS = r"^((?:[A-Z](?:[a-z]{1,2}\\.|\\.)?)+(?:\\s+(?:[A-Z](?:[a-z]{1,2}\\.|\\.)?){1,3}(?=\\s))*)\\s+(\\S.*)$"


def zonmw_split(name):
    clean = trim(re.sub(sql_regex(ZONMW_DEGREES), '', re.sub(sql_regex(ZONMW_TITLES), '', trim(name))))
    m = re.search(sql_regex(ZONMW_INITIALS), clean)
    return (nullif_empty(m.group(1)) if m else None), (nullif_empty(m.group(2)) if m else None) or nullif_empty(clean)


# ---------------------------------------------------------------- samples: raw name -> (given, family)

PARSE = {'sshrc_opendata': comma_split, 'cfi': cfi_split, 'zonmw_projects_jsonapi': zonmw_split}
SAMPLES = {
    'sshrc_opendata': {
        'Marin, Alexandra': ('Alexandra', 'Marin'),                                     # G5541173956
        'Pereira de Grandmont, Pascal-Olivier': ('Pascal-Olivier', 'Pereira de Grandmont'),
        'Willis, Alette J.': ('Alette J.', 'Willis'),
        'Brown, Virginia / Reynolds, Roger E.': ('Virginia / Reynolds, Roger E.', 'Brown'),  # two people: the first keeps both names
        'Cégep de Sherbrooke': (None, 'Cégep de Sherbrooke'),                           # an institution stays whole
    },
    'cfi': {
        'Pierre Thibault': ('Pierre', 'Thibault'),                                      # G6444361795
        'C Bruce Verchere': ('C Bruce', 'Verchere'),
        'Philip  Acott': ('Philip', 'Acott'),
        'Michael Jacobson, Jr.': ('Michael', 'Jacobson'),
        'Jerome Claverie, Nathalie Faucheux': ('Jerome', 'Claverie'),                   # leaders separated by ','
        'Sam': (None, 'Sam'),
    },
    'zonmw_projects_jsonapi': {
        'dr. R.K.E. Poortvliet MD PhD': ('R.K.E.', 'Poortvliet'),
        'dr. M.M. van der Krogt PhD': ('M.M.', 'van der Krogt'),
        'Dr. ir. M.A. van der Marck BSc MSc PhD': ('M.A.', 'van der Marck'),
        'dr. P. H. J. Kouwer PhD': ('P. H. J.', 'Kouwer'),
        'prof. CG. Schalkwijk PhD': ('CG.', 'Schalkwijk'),
        'dr. K.J.B Notten': ('K.J.B', 'Notten'),
        'mr. EE Nollet': ('EE', 'Nollet'),
        'E J W van Someren': ('E J W', 'van Someren'),
        'prof. dr. DSM Spencer Moore MPH PhD': ('DSM', 'Spencer Moore'),
        'dr. L. Van Rijn - van Gelderen': ('L.', 'Van Rijn - van Gelderen'),
        'B.L.A.M. Weusten': ('B.L.A.M.', 'Weusten'),
        'Boumans': (None, 'Boumans'),                                                   # no initials: whole name stays family
        'Mohammadi Jouabadi': (None, 'Mohammadi Jouabadi'),
    },
}


def test_sshrc_reads_family_comma_given():
    sql = source('sshrc_opendata')
    assert COMMA_GIVEN.format(c='g.applicant_name') in sql and COMMA_FAMILY.format(c='g.applicant_name') in sql
    assert 'Store full name in family_name' not in sql
    for raw, want in SAMPLES['sshrc_opendata'].items():
        assert comma_split(raw) == want, raw


def test_cfi_reads_the_first_leader_as_given_then_family():
    cell = cfi_cell()
    assert CFI_WORDS in cell and CFI_GIVEN in cell and CFI_FAMILY in cell
    assert 'FROM cfi_named g' in cell and 'Store full name in family_name' not in cell
    for raw, want in SAMPLES['cfi'].items():
        assert cfi_split(raw) == want, raw


def test_nhmrc_cells_strip_one_title_list_with_an_escaped_space_class():
    """nhmrc: the primary and historical cells strip the same titles, written '\\\\s' (Spark SQL reads a lone '\\s' as 's', which
    left 'Prof' as the given name on 17,149 historical grants)."""
    cells = [c['source'] if isinstance(c['source'], str) else ''.join(c['source'])
             for c in json.loads((AWARDS / 'CreateNHMRCAwards.ipynb').read_text())['cells']]
    titles = [t for c in cells for t in re.findall(r"REGEXP_REPLACE\((?:cia_name|pi_name), '([^']*)'", c)]
    assert len(titles) == 4 and len(set(titles)) == 1 and '\\\\s+' in titles[0] and 'A/Pr' in titles[0]


def test_zonmw_reads_initials_as_given_and_strips_titles_and_degrees():
    sql = squash(source('zonmw_projects_jsonapi'))
    assert f"REGEXP_REPLACE(REGEXP_REPLACE(TRIM(project_leader_name), '{ZONMW_TITLES}', ''), '{ZONMW_DEGREES}', '')" in sql
    assert f"NULLIF(REGEXP_EXTRACT(leader_clean_name, '{ZONMW_INITIALS}', 1), '') AS leader_given_name" in sql
    assert (f"COALESCE(NULLIF(REGEXP_EXTRACT(leader_clean_name, '{ZONMW_INITIALS}', 2), ''), NULLIF(leader_clean_name, '')) "
            "AS leader_family_name") in sql
    assert 'rp.leader_given_name AS given_name, rp.leader_family_name AS family_name' in sql and 'FROM raw_named rp' in sql
    for raw, want in SAMPLES['zonmw_projects_jsonapi'].items():
        assert zonmw_split(raw) == want, raw


GIVEN_IN_FAMILY = re.compile(r',\s+[^\W\d_]')   # 'Marin, Alexandra' in family_name


def misplaced(given, family):
    return bool(GIVEN_IN_FAMILY.search(family or '')) or (given or '').rstrip().endswith(',')


@pytest.mark.parametrize('src', sorted(SAMPLES))
def test_guard_no_name_left_in_the_wrong_field(src):
    """Shared guard over every sample: no family name holding ', ' and a given name, no given name ending in ','."""
    assert misplaced(None, 'Marin, Alexandra') and misplaced('Kellerhals,', 'Andreas')   # what SSHRC and SNSF printed before
    for raw in SAMPLES[src]:
        given, family = PARSE[src](raw)
        assert family and not misplaced(given, family), (src, raw, given, family)
