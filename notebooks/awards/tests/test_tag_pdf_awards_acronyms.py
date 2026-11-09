"""TagPdfAwardsIncremental step 3b (oxjob #473): a funder matched only by acronyms that an unrelated active ROR organisation
abroad also uses is not inserted when no author is from the funder's country and one is from that organisation's.

The contract tests run anywhere. The behaviour test runs the notebook's own SQL on fixture views where a real Spark is
available: local pyspark + JVM, or Databricks Connect when DATABRICKS_CONFIG_PROFILE is set. Elsewhere it skips.
"""
import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SQL = (ROOT / 'notebooks/awards/TagPdfAwardsIncremental.sql').read_text()
CELLS = SQL.split('-- COMMAND ----------')
VIEW = 'CREATE OR REPLACE TEMP VIEW distrusted_acronym_matches AS'


def cell(needle):
    found = [c for c in CELLS if needle in c]
    assert len(found) == 1, (needle, len(found))
    return found[0]


def code(c):
    return '\n'.join(l for l in c.splitlines() if not l.strip().startswith('--'))


def test_step4_skips_the_distrusted_pairs():
    step4 = code(cell('INSERT INTO openalex.works.fulltext_work_funders'))
    assert re.search(r'LEFT ANTI JOIN distrusted_acronym_matches d\s+ON s\.work_id = d\.work_id AND s\.funder_id = d\.funder_id;',
                     step4)
    # the view is built before step 4 and step 5 still matches award numbers for every staged pair
    assert SQL.index(VIEW) < SQL.index('INSERT INTO openalex.works.fulltext_work_funders')
    assert 'distrusted_acronym_matches' not in cell('INSERT INTO openalex.pdf.grobid_award_matches')


def test_the_rule_is_data_not_a_list():
    view = code(cell(VIEW))
    # no funder ids, aliases or countries written into the rule: the twins come from ROR, the countries from authorships
    assert not re.search(r'F\d{6,}', view)
    literals = set(re.findall(r"'([^']*)'", view))
    assert literals <= {'https://ror.org/', 'active', 'acronym', '', 'F(\\\\d+)'}, literals
    tables = set(re.findall(r'\bopenalex\.\w+\.\w+', view))
    assert tables == {'openalex.institutions.ror_relationships', 'openalex.institutions.ror', 'openalex.funders.funders_api',
                      'openalex.pdf.funder_matches_staging', 'openalex.works.work_authorships'}


# ---------------------------------------------------------------- behaviour on fixture rows (needs a Spark)

@pytest.fixture(scope='module')
def spark():
    if os.environ.get('DATABRICKS_CONFIG_PROFILE'):
        dc = pytest.importorskip('databricks.connect')
        yield dc.DatabricksSession.builder.profile(os.environ['DATABRICKS_CONFIG_PROFILE']).serverless(True).getOrCreate()
        return
    pyspark = pytest.importorskip('pyspark')
    if not hasattr(pyspark, '__version__'):
        pytest.skip('pyspark stub, not real Spark')
    from pyspark.sql import SparkSession
    try:
        s = SparkSession.builder.master('local[1]').appName('acronym_twins').getOrCreate()
    except Exception as e:  # pragma: no cover - no JVM
        pytest.skip(f'no local Spark: {e}')
    yield s
    s.stop()


ES, BR, IN, DE, CN, IE, CH = 'es1', 'br1', 'in1', 'de1', 'cn1', 'ie1', 'ch1'
FUNDERS = [  # id, country, ror
    (1, 'ES', ES),   # Spain's MEC
    (2, 'US', 'nasa'),
    (3, 'DE', DE),   # DFG
    (4, None, 'esf'),  # an international funder: no country, never judged
    (5, 'IN', IN),   # India's MoE (mixed-case alias)
    (6, 'IE', IE),   # SFI
    (7, 'FR', None),  # a funder without a ROR record: twins still count
    (8, 'TW', 'tw1'),  # NSC
    (9, 'CH', CH),   # SNF
]
ROR = [  # id, status, acronyms, country
    (ES, 'active', ['MEC'], 'ES'),   # the funder's own record: not a twin
    (BR, 'active', ['MEC'], 'BR'),   # Brazil's Ministério da Educação
    ('cn2', 'active', ['DFG'], 'CN'),  # a DFG lookalike in DFG's own ROR family
    ('br2', 'active', ['ESF'], 'BR'),
    (CN, 'active', ['MOE'], 'CN'),
    ('us2', 'inactive', ['SFI'], 'US'),  # inactive records are not twins
    ('lb1', 'active', ['CNRS'], 'LB'),
    ('xx1', 'active', ['NSC'], None),    # a twin with no location is no twin
    ('gr1', 'active', ['SNF'], 'GR'),    # related to SNF's record, edge stored the other way round
]
RELATIONSHIPS = [(DE, 'cn2'), ('gr1', CH)]
MATCHES = [  # work, funder, name
    (101, 1, 'MEC'),                               # Brazilian authors only -> distrusted
    (102, 1, 'MEC'),                               # a Spanish author -> kept
    (103, 1, 'MEC'), (103, 1, 'Ministerio de Economía y Competitividad'),  # full name too -> kept
    (104, 1, 'MEC'),                               # no author countries -> kept
    (105, 1, 'MEC'),                               # authors from a non-twin country -> kept
    (106, 2, 'NASA'),                              # no ROR twin -> kept
    (107, 3, 'DFG'),                               # the twin is in DFG's ROR family -> kept
    (108, 4, 'ESF'),                               # funder has no country -> kept
    (109, 5, 'MoE'),                               # case-insensitive twin 'MOE' in China, Chinese authors -> distrusted
    (110, 6, 'SFI'),                               # twin inactive -> kept
    (111, 7, 'CNRS'),                              # funder has no ROR id, Lebanese authors -> distrusted
    (112, 8, 'NSC'),                               # the only twin has no country -> kept
    (113, 9, 'SNF'),                               # twin related to the funder (reverse edge) -> kept
]
AUTHORS = {101: [['BR'], ['BR', 'PT']], 102: [['BR'], ['ES']], 103: [['BR']], 104: [[]], 105: [['JP']],
           106: [['BR']], 107: [['CN']], 108: [['BR']], 109: [['CN']], 110: [['US']], 111: [['LB']], 112: [['SE']],
           113: [['GR']]}


def run_view(spark):
    from pyspark.sql.types import (ArrayType, LongType, StringType, StructField, StructType)
    views = {
        'openalex.institutions.ror_relationships': ('t_rel', spark.createDataFrame(
            RELATIONSHIPS, 'ror_id string, related_ror_id string')),
        'openalex.institutions.ror': ('t_ror', spark.createDataFrame(
            [(f'https://ror.org/{i}', st, [(['acronym'], a) for a in acr], [((cc,),)]) for i, st, acr, cc in ROR],
            StructType([StructField('id', StringType()), StructField('status', StringType()),
                        StructField('names', ArrayType(StructType([StructField('types', ArrayType(StringType())),
                                                                   StructField('value', StringType())]))),
                        StructField('locations', ArrayType(StructType([StructField('geonames_details', StructType([
                            StructField('country_code', StringType())]))])))]))),
        'openalex.funders.funders_api': ('t_fa', spark.createDataFrame(
            [(i, cc, (f'https://ror.org/{r}' if r else None,)) for i, cc, r in FUNDERS],
            StructType([StructField('id', LongType()), StructField('country_code', StringType()),
                        StructField('ids', StructType([StructField('ror', StringType())]))]))),
        'openalex.pdf.funder_matches_staging': ('t_stage', spark.createDataFrame(
            [(w, f'https://openalex.org/F{f}', n) for w, f, n in MATCHES], 'work_id bigint, funder_id string, funder_name string')),
        'openalex.works.work_authorships': ('t_wa', spark.createDataFrame(
            [(w, [(c,) for c in cs]) for w, cs in AUTHORS.items()],
            StructType([StructField('work_id', LongType()), StructField('authorships', ArrayType(StructType([
                StructField('countries', ArrayType(StringType()))])))]))),
    }
    query = cell(VIEW).split(VIEW, 1)[1].strip().rstrip(';')
    for table, (name, df) in views.items():  # ror_relationships first: openalex.institutions.ror is its prefix
        df.createOrReplaceTempView(name)
        query = query.replace(table, name)
    return {(r.work_id, r.funder_id) for r in spark.sql(query).collect()}


def test_distrusts_only_acronym_matches_with_a_twin_country_author_and_no_home_author(spark):
    assert run_view(spark) == {(101, 'https://openalex.org/F1'), (109, 'https://openalex.org/F5'), (111, 'https://openalex.org/F7')}
