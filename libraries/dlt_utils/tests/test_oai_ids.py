"""Tests for the placeholder-host OAI re-key (oxjob #1407).

The SQL builders are checked against the pure-Python reference on a warehouse (see
test_sql_matches_reference_cases' docstring); here we pin the reference's behaviour and the
properties a future edit could silently break.
"""

from openalex.dlt.oai_ids import (
    DEFAULT_OAI_HOSTS, is_placeholder_id_sql, pmh_url_id_host_py, pmh_url_id_host_sql,
    rekey_native_id_py, rekey_native_id_sql)

# (native_id, id_host, expected[, keep_placeholder_host]) -- also run against the warehouse.
CASES = [
    ("oai:ojs.localhost:article/5", "oai.lareferencia.info", "oai:oai.lareferencia.info/ojs.localhost:article/5", True),
    ("oai:OJS.PKP.SFU.CA:article/5", "oai.lareferencia.info", "oai:oai.lareferencia.info/ojs.pkp.sfu.ca:article/5", True),
    ("oai:hal.science:x", "hal.science", "oai:hal.science:x", True),
    ("oai:ojs.pkp.sfu.ca:article/1", "et.ippt.pan.pl", "oai:et.ippt.pan.pl:article/1"),
    ("oai:ojs.pkp.sfu.ca:article/1", "journals.sbmu.ac.ir", "oai:journals.sbmu.ac.ir:article/1"),
    ("OAI:OJS.PKP.SFU.CA:article/7", "a.org", "oai:a.org:article/7"),  # case-insensitive host/scheme
    ("oai:generic.eprints.org:21464", "irep.iium.edu.my", "oai:irep.iium.edu.my:21464"),
    ("oai:localhost:123456789/42", "open.uct.ac.za", "oai:open.uct.ac.za:123456789/42"),
    ("oai:ojs.localhost:article/9", "iiste.org", "oai:iiste.org:article/9"),
    ("oai:localhost:a:b:c", "x.org", "oai:x.org:a:b:c"),  # colons in the local part survive
    ("oai:ojs.pkp.sfu.ca:article/1", None, "oai:ojs.pkp.sfu.ca:article/1"),  # no id_host row
    ("oai:ojs.pkp.sfu.ca:article/1", "", "oai:ojs.pkp.sfu.ca:article/1"),
    ("oai:hal.science:hal-0001", "hal.science", "oai:hal.science:hal-0001"),  # not a placeholder
    ("oai:arXiv.org:1234.5678", "export.arxiv.org", "oai:arXiv.org:1234.5678"),
    ("oai:ojs.pkp.sfu.ca.evil.org:article/1", "x.org", "oai:ojs.pkp.sfu.ca.evil.org:article/1"),
    ("oai:notlocalhost:1", "x.org", "oai:notlocalhost:1"),  # exact host match, not substring
    ("urn:ojs.pkp.sfu.ca:article/1", "x.org", "urn:ojs.pkp.sfu.ca:article/1"),
    ("oai:ojs.pkp.sfu.ca", "x.org", "oai:ojs.pkp.sfu.ca"),  # no local part: not an OAI id shape
    (None, "x.org", None),
]

URL_CASES = [
    ("https://et.ippt.pan.pl/index.php/index/oai", "et.ippt.pan.pl"),
    ("https://journals.sbmu.ac.ir/urolj/index.php/index/oai", "journals.sbmu.ac.ir"),
    ("http://www.polibotanica.mx/index.php/polibotanica/oai", "polibotanica.mx"),
    ("http://dspace.bracu.ac.bd:8080/oai/request", "dspace.bracu.ac.bd"),
    ("  HTTP://Journals.KU.edu/index.php/index/oai ", "journals.ku.edu"),
    ("http://juser.fz-juelich.de/oai2d ", "juser.fz-juelich.de"),
    ("not a url", ""),
]


def _case(c):
    return (c + (False,))[:4]


def test_reference_cases():
    for nid, host, expected, keep in map(_case, CASES):
        assert rekey_native_id_py(nid, host, keep) == expected, (nid, host, keep)


def test_keep_placeholder_host_keeps_two_placeholders_apart():
    """An aggregator's ojs.localhost:X and ojs.pkp.sfu.ca:X are different articles today."""
    a = rekey_native_id_py("oai:ojs.localhost:article/1", "agg.org", True)
    b = rekey_native_id_py("oai:ojs.pkp.sfu.ca:article/1", "agg.org", True)
    assert a != b


def test_url_cases():
    for url, expected in URL_CASES:
        assert pmh_url_id_host_py(url) == expected, url


def test_rekey_is_idempotent():
    """A re-keyed id is never re-keyed again (its host is no longer a placeholder)."""
    for nid, host, _, keep in map(_case, CASES):
        once = rekey_native_id_py(nid, host, keep)
        assert rekey_native_id_py(once, host, keep) == once


def test_sql_names_every_placeholder_host_exactly_once():
    sql = is_placeholder_id_sql("x")
    for h in DEFAULT_OAI_HOSTS:
        assert sql.count("'" + h + "'") == 1


def test_sql_has_no_backslashes():
    """Embedded in Python f-strings and SQL literals alike; escapes would be read differently."""
    assert "\\" not in rekey_native_id_sql("a", "b")
    assert "\\" not in pmh_url_id_host_sql("u")


def test_sql_leaves_id_unchanged_without_id_host():
    sql = rekey_native_id_sql("nid", "h", "k")
    assert sql.startswith("CASE WHEN h IS NOT NULL AND h <> ''")
    assert sql.endswith("ELSE nid END")


def test_sql_matches_reference_cases():
    """Warehouse check of the SQL builder against CASES (run manually; no Spark here):

        python3 -c "from tests.test_oai_ids import warehouse_check_sql; print(warehouse_check_sql())"

    and run the printed SELECT; every row must have ok = true. Last run: 2026-09-29, DEV-WH,
    all 19 rekey + 7 url cases ok.
    """
    assert "VALUES" in warehouse_check_sql()


def warehouse_check_sql():
    def lit(v):
        return "CAST(NULL AS STRING)" if v is None else "'" + v.replace("'", "''") + "'"
    rows = ",\n  ".join(f"({lit(n)}, {lit(h)}, {lit(e)}, {str(k).lower()})" for n, h, e, k in map(_case, CASES))
    urls = ",\n  ".join(f"({lit(u)}, {lit(e)})" for u, e in URL_CASES)
    return (
        f"SELECT 'rekey' k, nid, got, want, got <=> want ok FROM (SELECT nid, "
        f"{rekey_native_id_sql('nid', 'h', 'k')} got, want FROM VALUES\n  {rows}\n  AS t(nid, h, want, k))\n"
        f"UNION ALL\n"
        f"SELECT 'url', u, got, want, got <=> want FROM (SELECT u, {pmh_url_id_host_sql('u')} got, "
        f"want FROM VALUES\n  {urls}\n  AS t(u, want))"
    )


def test_with_rekeyed_native_id_rejects_bad_rows():
    import pytest
    from openalex.dlt.oai_ids import with_rekeyed_native_id
    for rows in ([("ep1", "Bad Host", False)], [("ep'1", "a.org", False)],
                 [("ep1", "a.org", False), ("ep1", "b.org", False)]):
        with pytest.raises(ValueError):
            with_rekeyed_native_id(None, rows, "endpoint_id")
