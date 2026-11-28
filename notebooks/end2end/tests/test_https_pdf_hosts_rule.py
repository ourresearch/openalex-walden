#!/usr/bin/env python3
"""Offline checks for oxjob #2690: CreateWorksBase serves an http:// PDF link as https when its host is listed in
openalex.works.https_pdf_hosts. No warehouse, no network.

Run from any directory: python3 notebooks/end2end/tests/test_https_pdf_hosts_rule.py   (also collected by pytest)
"""
import json
import re
from pathlib import Path

NB = Path(__file__).resolve().parents[1] / 'CreateWorksBase.ipynb'

# The SQL exactly as the notebook must carry it (Python mirrors below; Java and Python regex agree on this subset).
HOST_SQL = "NULLIF(lower(regexp_extract(lm.pdf_url, '^(?i)http://([^/:?#@]+)(?::80)?(?:[/?#]|$)', 1)), '')"
REWRITE_SQL = ("regexp_replace(regexp_replace(pdf_url, '^(?i)http://', 'https://'), "
               "'(?<=^https://[^/:?#@]{1,253}):80(?=[/?#]|$)', '')")
WHEN_SQL = "WHEN https_pdf_host IS NOT NULL AND NOT COALESCE(array_contains(curated_fields, 'pdf_url'), FALSE)"


def host_key(url):
    """Python mirror of the hp join key: the lowercase host of an http:// URL with no port or :80, else '' (NULL in SQL,
    so it joins nothing)."""
    m = re.search(r'^(?i:http://)([^/:?#@]+)(?::80)?(?:[/?#]|$)', url)
    return m.group(1).lower() if m else ''


def rewrite(url):
    """Python mirror of the WHEN line's rewrite. Python's re refuses Java's bounded variable-width lookbehind, so the
    ':80' right after the host (before the path, query, fragment or end) is cut with a group instead: the same edit."""
    u = re.sub(r'^(?i:http://)', 'https://', url)
    m = re.match(r'^(https://[^/:?#@]{1,253}):80(?=[/?#]|$)', u)
    return m.group(1) + u[m.end():] if m else u


CASES = [
    # url, host key, served url
    ('http://jals.miard.org/index.php/jals/article/download/799/678', 'jals.miard.org',
     'https://jals.miard.org/index.php/jals/article/download/799/678'),        # ZD 28542, W7214930118
    ('http://koreascience.or.kr:80/article/JAKO200606140770215.pdf', 'koreascience.or.kr',
     'https://koreascience.or.kr/article/JAKO200606140770215.pdf'),             # explicit :80 goes
    ('HTTP://Journal.Example.ORG/a.pdf', 'journal.example.org', 'https://Journal.Example.ORG/a.pdf'),
    ('http://host.example', 'host.example', 'https://host.example'),
    ('http://host.example:80', 'host.example', 'https://host.example'),
    ('http://host.example:80?id=1', 'host.example', 'https://host.example?id=1'),
    ('http://host.example/path:80/x.pdf', 'host.example', 'https://host.example/path:80/x.pdf'),  # a path ':80' stays
    ('http://host.example:8080/x.pdf', '', None),                               # another port never switches
    ('http://user@host.example/x.pdf', '', None),                               # userinfo never switches
    ('https://host.example/x.pdf', '', None),                                   # already https
    ('ftp://host.example/x.pdf', '', None),
]


def test_python_mirror():
    for url, key, served in CASES:
        assert host_key(url) == key, (url, host_key(url))
        if served is not None:
            assert rewrite(url) == served, (url, rewrite(url))


def test_notebook_carries_the_rule():
    nb = json.loads(NB.read_text())
    sql = '\n'.join(''.join(c['source']) for c in nb['cells'])
    assert HOST_SQL in sql, 'hp join key changed: update HOST_SQL and host_key() together'
    assert REWRITE_SQL in sql, 'rewrite changed: update REWRITE_SQL and rewrite() together'
    assert WHEN_SQL in sql, 'a curated pdf_url must stay as curated'
    assert "SELECT DISTINCT host FROM openalex.works.https_pdf_hosts WHERE status = 'https' AND host <> ''" in sql
    assert 'https_pdf_host)' in sql, 'https_pdf_host must be in the outer EXCEPT list'
    # the job run blanks '$1' and friends in notebook SQL (oxjob #1932): the rule must use none
    assert not re.search(r'\$[0-9A-Za-z_{]', HOST_SQL + REWRITE_SQL)


if __name__ == '__main__':
    test_python_mirror()
    test_notebook_carries_the_rule()
    print('ok')
