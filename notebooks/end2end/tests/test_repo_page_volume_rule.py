#!/usr/bin/env python3
"""Offline checks for oxjob #1098 step 2c: a work no record gives a volume takes the one the landing pages of its
repository records state in their citation_volume / citation_issue meta tags (CreateWorksBase cell
openalex.works.repo_page_volumes), fill-only in the main statement's biblio. No warehouse, no network.

The SQL regexes are read from the notebook itself and run here with Python's re (same syntax for these patterns), so
the cell and these checks can't drift apart.

Run from any directory: python3 notebooks/end2end/tests/test_repo_page_volume_rule.py   (also collected by pytest)
"""
import json
import re
from pathlib import Path

NB = Path(__file__).resolve().parents[1] / 'CreateWorksBase.ipynb'
TABLE = 'openalex.works.repo_page_volumes'


def cells():
    return [''.join(c['source']) for c in json.loads(NB.read_text())['cells']]


def table_cell():
    return next(c for c in cells() if 'CREATE OR REPLACE TABLE ' + TABLE in c)


def sql_regexes(cell):
    """Every regex literal in the cell's REGEXP_EXTRACT / REGEXP_REPLACE / RLIKE calls, SQL escapes undone."""
    lits = re.findall(r"(?:REGEXP_EXTRACT|REGEXP_REPLACE)\([^']*'((?:[^'\\]|\\.)*)'", cell)
    lits += re.findall(r"RLIKE '((?:[^'\\]|\\.)*)'", cell)
    return [l.replace('\\\\', '\\') for l in lits]


CELL = table_cell()
RX = sql_regexes(CELL)
NAME_RX = next(r for r in RX if 'name|property' in r)
CONTENT_RX = next(r for r in RX if 'content' in r)
VOL_RX = next(r for r in RX if r.startswith('^(0*[1-9][0-9]{0,3})'))
ISS_RX = next(r for r in RX if r.startswith('^(0*[1-9][0-9]{0,2}(?:'))
ISS_ZEROS_RX = next(r for r in RX if r.startswith('(?<![0-9])0+'))


def extract(rx, s):
    m = re.search(rx, s)
    return m.group(1) if m else ''


def tag(line):
    """The cell's (name, content) for one raw <meta> line."""
    return extract(NAME_RX, line).strip().lower(), ' '.join(extract(CONTENT_RX, line).split())


def volume(v):
    """The cell's volume rule: 1-4 digits (leading zeros dropped), not a year 19xx/20xx. An empty content never gets
    here (the cell keeps only tags with t.c <> '')."""
    if not v or extract(VOL_RX, v) != v:
        return None
    v = re.sub('^0+', '', v)
    return None if len(v) == 4 and re.match('^(19|20)', v) else v


def issue(i):
    """The cell's issue rule: 1-3 digits or a combined issue (3-4, 1/2), leading zeros dropped."""
    if not i or extract(ISS_RX, i) != i:
        return None
    return re.sub(ISS_ZEROS_RX, '', i)


# Raw meta lines as openalex.landing_page.meta_tags stores them (22 Nov 2026 samples: OJS, CERN CDS, IRIS)
LINES = [
    ('<meta name="citation_volume" content="73" />', ('citation_volume', '73')),
    ('<meta content="46" name="citation_volume" />', ('citation_volume', '46')),   # cds.cern.ch: content first
    ("<meta name='citation_issue' content='3-4'>", ('citation_issue', '3-4')),
    ('<meta property="citation_volume" content=" 12 ">', ('citation_volume', '12')),
    ('<meta name="citation_title" content="Water-stable coatings">', ('citation_title', 'Water-stable coatings')),
]
VOLUMES = [('12', '12'), ('05', '5'), ('146', '146'), ('1', '1'), ('1899', '1899'),
           ('0', None), ('2019', None), ('2003', None), ('12345', None), ('vol. 15, no. 2', None),
           ('XXXIV', None), ('8(6)', None), ('54a', None), ('N° 45', None), ('', None)]
ISSUES = [('3', '3'), ('03', '3'), ('3-4', '3-4'), ('1/2', '1/2'), ('01-02', '1-2'), ('100', '100'),
          ('0', None), ('2010', None), ('5636', None), ('Suppl 7', None), ('1 Pt 2', None), ('S7', None), ('', None)]


def test_meta_lines():
    assert [tag(line) for line, _ in LINES] == [want for _, want in LINES]


def test_volume_rule():
    assert [(v, volume(v)) for v, _ in VOLUMES] == VOLUMES


def test_issue_rule():
    assert [(i, issue(i)) for i, _ in ISSUES] == ISSUES


def test_main_statement_fills_only():
    main = next(c for c in cells() if 'CREATE OR REPLACE TABLE openalex.works.openalex_works_base' in c
                or 'LEFT JOIN openalex.works.crossref_article_numbers crn' in c)
    assert 'LEFT JOIN ' + TABLE + ' rpv ON rpv.work_id = s.id' in main
    assert ('COALESCE(s.volume, IF(s.issue IS NULL OR rpv.issue IS NULL OR s.issue = rpv.issue, rpv.volume, NULL)) AS volume'
            in main)
    assert 'COALESCE(s.issue, IF(s.volume IS NULL, rpv.issue, NULL)) AS issue' in main
    # the table is built before the statement that reads it
    order = cells()
    assert order.index(CELL) < order.index(main)


def test_empty_content_is_no_value():
    assert "t.n = 'citation_volume' AND t.c <> ''" in CELL and "t.n = 'citation_issue' AND t.c <> ''" in CELL


def test_curations_win():
    assert "ARRAY_CONTAINS(curated_fields, 'biblio.volume') OR ARRAY_CONTAINS(curated_fields, 'biblio.issue')" in CELL
    assert 'LEFT ANTI JOIN' in CELL


HOST_RX = next(r for r in RX if r.startswith('^https?://'))
SHARED_ID_RX = next(r for r in RX if r.startswith('^oai:('))


def host(url):
    """The cell's host of a URL: lower case, without www. and port."""
    return extract(HOST_RX, url or '').lower()


def title_key(t):
    r"""The cell's title key: HTML entities dropped, then only letters and digits, lower case. Python's re has no
    \p{L}/\p{N}, so the same classes are spelt with unicodedata here."""
    import unicodedata
    if not t:
        return ''
    t = re.sub('&#?[a-zA-Z0-9]+;', '', t).lower()
    return ''.join(ch for ch in t if unicodedata.category(ch)[0] in 'LN')


def lev(a, b):
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def same_title(p, r):
    """The cell's title test for a page under a shared id (p, r are title keys)."""
    if not p or not r:
        return False
    return (p == r or (min(len(p), len(r)) >= 15 and (p.startswith(r) or r.startswith(p)))
            or lev(p[:60], r[:60]) <= max(2, min(len(p), len(r), 60) // 5))


def pick(pmh_id, pages, rec_title, rec_urls):
    """The page (vol, iss) the cell takes for one record: pages are (fetched_at, vol, iss, citation_title, [own urls])."""
    keys = {title_key(t)[:40] for _, _, _, t, _ in pages if title_key(t)}
    shared = bool(re.match(SHARED_ID_RX, pmh_id)) or len(keys) > 1
    newest_first = sorted(pages, key=lambda pg: pg[0], reverse=True)
    if not shared:
        return newest_first[0][1:3]
    rec_hosts = {host(u) for u in rec_urls} - {''}
    for _, vol, iss, t, urls in newest_first:
        if {host(u) for u in urls} & rec_hosts or same_title(title_key(t), title_key(rec_title)):
            return vol, iss
    return None


# W7224060605 (23 Nov 2026): its record oai::article/1057 is the IJSSHR article; a July page of a Brazilian journal's
# article 1057 sits under the same id.
IJSSHR = 'https://www.ijsshr.com/journal/index.php/IJSSHR/article/view/1057'
PAGES_1057 = [
    ('2026-07-13', '8', '1', 'OS INSTITUTOS FEDERAIS DE EDUCAÇÃO, CIÊNCIA E TECNOLOGIA NO CAMPO ACADÊMICO BRASILEIRO',
     ['https://ojs.ifes.edu.br/index.php/dect/article/view/1057', 'https://ojs.ifes.edu.br/index.php/dect/article/download/1057/651']),
    ('2026-11-23', '13', '2', 'INFLUENCE OF FINANCIAL LEVERAGE RISK ON FIRM VALUE IN NIGERIA',
     [IJSSHR, 'https://www.ijsshr.com/journal/index.php/IJSSHR/article/download/1057/897']),
]


def test_host_rule():
    assert host('https://www.ijsshr.com/journal/index.php/IJSSHR/article/view/1057') == 'ijsshr.com'
    assert host('http://journals.tmkarpinski.com:80/index.php/ejbr/article/view/5') == 'journals.tmkarpinski.com'
    assert host('https://Revistas.UCR.ac.cr?x=1') == 'revistas.ucr.ac.cr'
    assert host('doi:10.1/x') == ''


def test_default_ids_are_shared():
    for i in ['oai::article/1057', 'oai:ojs.pkp.sfu.ca:article/3568', 'oai:ojs.localhost:article/4515',
              'oai:localhost:123456789/7']:
        assert re.match(SHARED_ID_RX, i), i
    for i in ['oai:ojs2.journals.tmkarpinski.com:article/5', 'oai:doaj.org/article:60708c6f', 'oai:zenodo.org:4444441',
              'oai:www.research.unipd.it:11577/3172256']:
        assert not re.match(SHARED_ID_RX, i), i


def test_shared_id_takes_the_records_own_page():
    # the record's own page wins even though it is the newer one here; the older foreign page is never used
    assert pick('oai::article/1057', PAGES_1057, 'INFLUENCE OF FINANCIAL LEVERAGE RISK ON FIRM VALUE IN NIGERIA', [IJSSHR]) == ('13', '2')
    # same by host alone: a translated record title
    assert pick('oai::article/1057', PAGES_1057, 'PENGARUH RISIKO LEVERAGE KEUANGAN', [IJSSHR]) == ('13', '2')
    # only another journal's page under the id: no value
    assert pick('oai::article/1057', PAGES_1057[:1], 'INFLUENCE OF FINANCIAL LEVERAGE RISK ON FIRM VALUE IN NIGERIA', [IJSSHR]) is None


def test_unshared_id_takes_the_newest_page():
    pages = [('2026-07-14', '7', '1', 'Water-stable coatings', ['https://repo.example.org/x']),
             ('2026-11-01', '7', '2', 'Water-stable coatings.', [])]
    assert pick('oai:repo.example.org:123/4', pages, 'Something else entirely', ['https://other.example.com/']) == ('7', '2')


def test_title_test():
    # redalyc pages drop accented letters; a near title still counts
    assert same_title(title_key('Jos Murilo de Carvalho: cuatro libros fundamentales'),
                      title_key('José Murilo de Carvalho: cuatro libros fundamentales'))
    assert same_title(title_key('SAJAK DI DALAM AL-QUR&#039;AN: SEBUAH KONTROVERSI'), title_key("Sajak di dalam Al-Qur'an: sebuah kontroversi"))
    assert not same_title(title_key('Serving in professional tennis: Pressure or indifference?'),
                          title_key('Jurisprudència del Tribunal Constitucional. Primer semestre de 2016'))
    assert not same_title('', title_key('Anything'))


def test_shared_rule_in_cell():
    assert 'ARRAYS_OVERLAP(' in CELL and 'LEVENSHTEIN(' in CELL
    assert "COUNT(DISTINCT NULLIF(LEFT(title_key, 40), '')) > 1" in CELL
    assert 'WHERE l.work_id IS NOT NULL AND (c.rn = 1 OR c.shared)' in CELL


def work_value(records):
    """The cell's (volume, issue) for one work. records maps each repository record to the (vol, iss) of its newest
    pages: every page stamped with the record's newest fetched_at counts (Meta Tags Daily gives all pages of a run the
    run's time). A work takes a volume only when all those pages that state one (not a year) agree; a record's issue is
    the one its pages that state an issue agree on, and the work's issue needs every record to give the same one."""
    rows = []
    for pages in records.values():
        kept = [(volume(v), issue(i)) for v, i in pages if volume(v)]
        if kept:
            issues = {i for _, i in kept if i}
            rows.append(({v for v, _ in kept}, issues.pop() if len(issues) == 1 else None))
    vols = set().union(*(v for v, _ in rows)) if rows else set()
    if len(vols) != 1:
        return None
    issues = {i for _, i in rows}
    return vols.pop(), (issues.pop() if len(issues) == 1 and None not in issues else None)


def test_tied_pages_all_count():
    # 24 Nov 2026: ROW_NUMBER took one of a record's same-time pages at random, so ~3,000 works' values changed nightly
    assert 'RANK() OVER (PARTITION BY p.pmh_id ORDER BY p.fetched_at DESC) AS rn' in CELL
    assert 'RANK() OVER (PARTITION BY work_id, pmh_id ORDER BY fetched_at DESC) AS rn' in CELL
    assert 'ROW_NUMBER()' not in CELL
    assert 'COLLECT_SET(vol) AS vols, IF(SIZE(COLLECT_SET(iss)) = 1, GET(COLLECT_SET(iss), 0), NULL) AS iss' in CELL
    assert 'GROUP BY work_id, pmh_id' in CELL
    assert 'HAVING SIZE(ARRAY_DISTINCT(FLATTEN(COLLECT_LIST(r.vols)))) = 1' in CELL


def test_tied_pages_rule():
    # W7162995272: PubMed Central's page states issue 1, the record's other page of the same run states none
    assert work_value({'oai:pubmedcentral.nih.gov:13524915': [('16', '1'), ('16', None)]}) == ('16', '1')
    # W7174593: a bepress copy's page carries only bepress_citation_volume (no citation_volume) beside the journal's page
    assert work_value({'oai:openscholarship.wustl.edu:law_lawreview-4106': [('25', None), (None, None)]}) == ('25', None)
    # W4321256 (Pure): a year in place of the volume is left aside
    assert work_value({'oai:pure.eur.nl:openaire_cris_publications/fc59d040': [('2013', None), ('12', None)]}) == ('12', None)
    # two pages of one run that disagree on the volume: no volume
    assert work_value({'oai:repositorio.uan.edu.co:123456789/7372': [('10', None), ('22', None)]}) is None
    # ... nor when the disagreeing page belongs to another record of the work
    assert work_value({'a': [('7', '2')], 'b': [('8', '2')]}) is None
    # pages that disagree on the issue: the volume stays, no issue
    assert work_value({'r': [('25', '2'), ('25', '3')]}) == ('25', None)
    # one record gives no issue: no issue for the work (unchanged rule across records)
    assert work_value({'a': [('7', '2')], 'b': [('7', None)]}) == ('7', None)
    assert work_value({'a': [('7', '2')], 'b': [('7', '02')]}) == ('7', '2')
    assert work_value({'a': [(None, '2')]}) is None


def test_no_dollar_in_sql():
    # oxjob #1932: a job run blanks $1 / $name in notebook SQL
    assert '$' not in CELL


if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_'):
            fn()
            print('ok', name)
