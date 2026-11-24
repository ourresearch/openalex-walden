"""Execute the combined NIH projection, USD conversion and resolved loss baselines offline."""
import re
import types
import pytest
from test_award_flows import FakeC, ROOT
import award_keys
import award_usd
import funding_population as fp
import nih_fold


class DB:
    def __init__(self):
        self.db = pytest.importorskip('duckdb').connect()
        self.db.execute('CREATE MACRO get_json_object(x,p) AS json_extract_string(x,p)')
        self.r = ''; self.nih = True
        self.config = {'package_root': str(ROOT), 'bootstrap_counts': {}}
        self.counts = {}; self.checks = {}; self._last_counts = {}
    def require(self, value, name):
        if not value: raise RuntimeError(name)
    def has_extra(self, name): return False
    def sql(self, text, params=None):
        text = text.replace(' EXCEPT(', ' EXCLUDE(').replace('<=>',' IS NOT DISTINCT FROM ').replace('LEFT ANTI JOIN','ANTI JOIN')
        text = re.sub(r'LATERAL VIEW explode\((p\.\w+)\) e AS o', r', UNNEST(\1) AS e(o)', text)
        result = self.db.execute(text)
        return types.SimpleNamespace(columns=[d[0] for d in result.description], collect=lambda: [types.SimpleNamespace(**dict(zip([d[0] for d in result.description], row))) for row in result.fetchall()])
    def artifact(self, name, query, params=None):
        self.sql('CREATE OR REPLACE TABLE '+name+' AS '+query)
        return name
    def zero(self, name, query):
        self.checks[name] = not self.sql(query).collect()
        self.require(self.checks[name], name)
    def count(self, name, relation):
        n = self.sql('SELECT count(*) n FROM '+relation).collect()[0].n
        self.counts[name] = n; return n


def fixture():
    c = DB()
    c.sql("""CREATE TABLE previous_api_v AS SELECT i id,100.0 amount,'USD' currency,100.0 amount_usd,'usd_native' amount_usd_basis,
      {'is_research':true,'basis':'funder_default','tier':1} research_scope,
      [{'id':concat('I',i),'display_name':concat('Org',i),'ror':NULL::VARCHAR,'country_code':'US','roles':['lead'],
        'share':NULL::DOUBLE,'basis':'institution_name','asserted_by':'funder'}] recipients_full,
      [{'country_code':'US','share':1.0,'amount_usd':100.0,'basis':'funder_reported'}] recipient_countries_full,
      []::STRUCT(code VARCHAR,vocabulary VARCHAR,display_name VARCHAR,share DOUBLE)[] recipient_regions_full,
      concat('R01AA',i) funder_award_id,'Title' display_name,NULL::VARCHAR description,NULL::VARCHAR lead_investigator,
      NULL::VARCHAR co_lead_investigator,NULL::VARCHAR investigators,DATE '2020-01-01' start_date,DATE '2021-01-01' end_date,
      2020 start_year,2021 end_year,4320332161::BIGINT funder_id,TIMESTAMP '2026-10-01' updated_date,'nih_exporter' provenance
      FROM range(1,1001) t(i)""")
    c.sql("""CREATE TABLE resolve_candidate AS SELECT id owner_id,
      CASE WHEN id>980 THEN id-980 ELSE id END stable_id,'ACTIVE' status FROM previous_api_v""")
    c.sql("""CREATE TABLE migration_manifest_v AS SELECT id+980 loser_id,id target_id,'NIH_PROJECT_YEAR_FOLD' evidence_class,
      to_json({'primary_award_id':funder_award_id,'title_from':funder_award_id}) evidence_json FROM previous_api_v WHERE id<=20""")
    c.sql("""CREATE TABLE nih_survivors AS SELECT target_id stable_id,4320332161::BIGINT pinned_funder_id FROM migration_manifest_v""")
    c.sql("""CREATE TABLE bindings_final AS SELECT r.stable_id,cast(p.id AS VARCHAR) observation_key,true is_direct,10 priority,1 source_rank,p payload
      FROM previous_api_v p JOIN resolve_candidate r ON r.owner_id=p.id""")
    c.sql("""CREATE TABLE awards_candidate AS SELECT * EXCLUDE(amount_usd,amount_usd_basis,research_scope,recipients_full,
      recipient_countries_full,recipient_regions_full) FROM previous_api_v WHERE id<=980""")
    return c


def test_combined_nih_projection_conversion_and_all_funding_gates():
    c = fixture()
    nih_fold.project(c)  # production projection: default null policy on exactly the 20 targets
    assert c.sql('SELECT count(*) n FROM awards_candidate WHERE amount IS NULL').collect()[0].n == 20
    c.sql('CREATE TABLE awards_candidate_before_usd AS SELECT * FROM awards_candidate')
    c.sql('CREATE TABLE rates_daily_filled(currency VARCHAR,rate_date DATE,usd_per_unit DOUBLE)')
    c.sql('CREATE TABLE exchange_rates_annual_v(currency VARCHAR,rate_year INT,usd_per_unit DOUBLE)')
    capture = FakeC(extra=['exchange_rates','exchange_rates_annual']); capture.r = ''
    award_usd.run(capture)
    conversion = [q for name,q in capture.artifacts if name=='awards_candidate'][-1]
    c.artifact('awards_candidate', conversion)
    n = c.sql('SELECT count(*) n FROM awards_candidate WHERE amount_usd IS NOT NULL').collect()[0].n
    assert n == 960  # 20 losers gone and 20 survivor amounts explicitly nulled
    expected = {
        'usd': {'awards_with_amount_usd':960,'amount_usd_usd_native':960,'amount_usd_currency_USD':960},
        'research': {'awards_with_research_scope':980,'research_true':980},
        'orgs': {'awards_with_recipients':980,'awards_with_matched_recipients':980,'recipient_org_memberships':1000},
        'geography': {'awards_with_recipient_countries':980,'awards_with_recipient_regions':0,'recipient_country_memberships':980},
    }
    for kind, counts in expected.items():
        assert fp.expected(c,kind)[0] == counts
        for key, value in counts.items():
            fp.gate(c,kind,key,key,value,.01)
            if value:
                with pytest.raises(RuntimeError,match=key):
                    fp.gate(c,kind,key,key,value-30,.01)
    # Raw source receipts still use their original population. A fold is no waiver.
    c.config['bootstrap_counts']['recipients_rows_feed_organisation'] = 1000
    with pytest.raises(RuntimeError,match='SOURCE_LOSS'):
        award_keys.drop_gate(c,'SOURCE_LOSS','recipients_rows_feed_organisation',980,.01)


def test_missing_and_gone_are_not_discounted_as_folds():
    c = fixture(); c.nih = False
    c.sql("DELETE FROM resolve_candidate WHERE owner_id BETWEEN 100 AND 119")
    c.sql("UPDATE resolve_candidate SET status='GONE' WHERE owner_id BETWEEN 120 AND 139")
    assert fp.expected(c,'research')[0]['research_true'] == 980


def test_duplicate_org_membership_unions_roles_across_fold():
    c = fixture(); c.nih = False
    c.sql('UPDATE previous_api_v SET recipients_full=(SELECT recipients_full FROM previous_api_v WHERE id=1) WHERE id=981')
    assert fp.expected(c,'orgs')[0]['recipient_org_memberships'] == 999
    c._funding_population = {}
    c.sql("UPDATE previous_api_v SET recipients_full=from_json(replace(to_json(recipients_full),'lead','partner'),'[{\"id\":\"VARCHAR\",\"display_name\":\"VARCHAR\",\"ror\":\"VARCHAR\",\"country_code\":\"VARCHAR\",\"roles\":[\"VARCHAR\"],\"share\":\"DOUBLE\",\"basis\":\"VARCHAR\",\"asserted_by\":\"VARCHAR\"}]') WHERE id=981")
    assert fp.expected(c,'orgs')[0]['recipient_org_memberships'] == 999


def test_bootstrap_fold_requires_rows_and_rechecks_independent_scalar_baseline():
    c = fixture(); c.nih = False
    c.sql('CREATE TABLE funding_flows_baseline_v AS SELECT * FROM previous_api_v')
    c.sql('ALTER TABLE previous_api_v DROP COLUMN research_scope')
    with pytest.raises(RuntimeError,match='FUNDING_FOLD_BASELINE_REQUIRED'):
        fp.expected(c,'research')
    c.has_extra = lambda name: name=='funding_flows_baseline'
    c.config['bootstrap_counts'].update(research_true=1000,awards_with_research_scope=1000)
    fp.gate(c,'research','RESEARCH_TRUE_DROP','research_true',980,.01)
    assert c.counts['funding_before_research_true']==1000
    assert c.counts['funding_expected_research_true']==980
    c._funding_population={}
    c.sql("UPDATE funding_flows_baseline_v SET research_scope=NULL WHERE id BETWEEN 100 AND 140")
    with pytest.raises(RuntimeError,match='RESEARCH_TRUE_DROP_BOOTSTRAP'):
        fp.gate(c,'research','RESEARCH_TRUE_DROP','research_true',980,.01)


NIAID = 4320337355
OBS = [  # survivor, observation, direct, priority, number, provenance, amount, updated
    # project 10: three fiscal years, a supplement and a de-obligation, each counted once
    (10, 'y02', True, 3, '5R01AI000010-02', 'nih_exporter', 100000.0, '2026-11-01'),
    (10, 'y03', True, 3, '5R01AI000010-03', 'nih_exporter', 200000.0, '2026-11-01'),
    (10, 'y03_again', True, 3, '5R01AI000010-03', 'nih_exporter', 200000.0, '2026-10-01'),   # the same year bound twice: once
    (10, 's1', True, 3, '3R01AI000010-03S1', 'nih_exporter', 50000.0, '2026-11-01'),         # a supplement is its own award
    (10, 'y04', True, 3, '5R01AI000010-04', 'nih_exporter', -10000.0, '2026-11-01'),         # a de-obligation reduces it
    (10, 'cr', True, 1, 'R01AI000010', 'crossref_work', 777.0, '2026-11-01'),                # another source never adds
    (10, 'paper', False, 0, 'R01AI000010', 'crossref_work_funders', 555.0, '2026-11-01'),    # nor does a paper's mention
    # project 20: FY1998 has no published cost -> no amount (complete totals only, Rohan 11-19), counted as partial
    (20, 'p98', True, 3, '5R01CA000020-01', 'nih_exporter', None, '2026-11-01'),
    (20, 'p99', True, 3, '5R01CA000020-02', 'nih_exporter', 300000.0, '2026-11-01'),
    (20, 'p00', True, 3, '5R01CA000020-03', 'nih_exporter', 300000.0, '2026-11-01'),
    # project 70: NIH intramural (Z01); FY2006 has no cost (ExPORTER publishes intramural costs from FY2007) -> no amount
    (70, 'i06', True, 3, 'Z01AI000070-12', 'nih_exporter', None, '2026-11-01'),
    (70, 'i07', True, 3, 'Z01AI000070-13', 'nih_exporter', 250000.0, '2026-11-01'),
    # project 80: every year has a cost -> the sum; two years with the same (flat) budget are two awards, not one
    (80, 'f1', True, 3, '5R01DK000080-01', 'nih_exporter', 300000.0, '2026-11-01'),
    (80, 'f2', True, 3, '5R01DK000080-02', 'nih_exporter', 300000.0, '2026-11-01'),
    # project 90: began in April 1981 (before FY1985, NIH's first published year); every year we hold has a cost -> still no amount
    (90, 'e85', True, 3, '5P01HL000090-05', 'nih_exporter', 400000.0, '2026-11-01'),
    (90, 'e86', True, 3, '5P01HL000090-06', 'nih_exporter', 420000.0, '2026-11-01'),
    # project 95: intramural, no start date on any record, every year (FY2007 on) has a cost -> the sum (no start date is not "before")
    (95, 'z07', True, 3, 'ZIAAI000095-01', 'nih_exporter', 90000.0, '2026-11-01'),
    # project 30: no year with a cost -> no amount, never 0
    (30, 'n1', True, 3, '5R01NS000030-01', 'nih_exporter', None, '2026-11-01'),
    # project 40: years summing to zero -> no amount
    (40, 'z1', True, 3, '5R01HL000040-01', 'nih_exporter', 5.0, '2026-11-01'),
    (40, 'z2', True, 3, '5R01HL000040-02', 'nih_exporter', -5.0, '2026-11-01'),
    # 50 is no project survivor (a one-year record): its own amount stands
    (50, 'one', True, 3, '1R13AI000050-01', 'nih_exporter', 6000.0, '2026-11-01'),
    # 60 is a year of project 10 still served as its own record (not on the list, held, or arrived later): untouched, counted
    (60, 'y05', True, 3, '5R01AI000010-05', 'nih_exporter', 70000.0, '2026-11-01'),
]


def sum_fixture(policy):
    c = DB(); c.config['nih_fold'] = {'amount': policy}
    values = ','.join(f"({s},'{k}',{str(d).lower()},{p},'{n}','{pv}',{'NULL' if a is None else a},TIMESTAMP '{u}')" for s, k, d, p, n, pv, a, u in OBS)
    c.sql(f"""CREATE TABLE obs AS SELECT * REPLACE (amount::DOUBLE AS amount)
      FROM (VALUES {values}) t(stable_id,observation_key,is_direct,priority,number,provenance,amount,updated)""")
    c.sql("""CREATE TABLE bindings_final AS SELECT stable_id,observation_key,is_direct,priority,CASE WHEN is_direct THEN 1 ELSE 4 END source_rank,{'funder_award_id':number,'provenance':provenance,
      'display_name':'Title','description':NULL::VARCHAR,'lead_investigator':NULL::VARCHAR,'co_lead_investigator':NULL::VARCHAR,
      'investigators':NULL::VARCHAR,'start_date':CASE stable_id WHEN 90 THEN DATE '1981-04-01' WHEN 95 THEN NULL::DATE ELSE DATE '2010-07-01' END,'end_date':DATE '2015-06-30','amount':amount,'currency':'USD',
      'updated_date':updated} payload FROM obs""")
    c.sql("""CREATE TABLE migration_manifest_v AS SELECT s target_id,'NIH_PROJECT_YEAR_FOLD' evidence_class,
      to_json({'core':p,'primary_award_id':p,'title_from':t,'project_funder_id':4320337355}) evidence_json FROM (VALUES
      (10,'R01AI000010','5R01AI000010-03'),(20,'R01CA000020','5R01CA000020-02'),(30,'R01NS000030','5R01NS000030-01'),
      (40,'R01HL000040','5R01HL000040-02'),(70,'Z01AI000070','Z01AI000070-13'),(80,'R01DK000080','5R01DK000080-02'),
      (90,'P01HL000090','5P01HL000090-06'),(95,'ZIAAI000095','ZIAAI000095-01')) t(s,p,t)""")
    c.sql(f"CREATE TABLE nih_survivors AS SELECT target_id stable_id,{NIAID}::BIGINT pinned_funder_id FROM migration_manifest_v")
    # the elected document of each record: a survivor shows its newest year's own cost before the projection
    c.sql(f"""CREATE TABLE awards_candidate AS SELECT stable_id id,max_by(number,number) funder_award_id,'Title' display_name,
      NULL::VARCHAR description,NULL::VARCHAR lead_investigator,NULL::VARCHAR co_lead_investigator,NULL::VARCHAR investigators,
      DATE '2014-07-01' start_date,DATE '2015-06-30' end_date,2014 start_year,2015 end_year,max_by(amount,number) amount,'USD' currency,
      CASE WHEN stable_id IN (50,60) THEN 4320332161 ELSE {NIAID} END::BIGINT funder_id,'nih_exporter' provenance
      FROM obs WHERE provenance='nih_exporter' GROUP BY stable_id""")
    return c


def projected_usd(c):
    nih_fold.project(c)
    c.sql('CREATE TABLE awards_candidate_before_usd AS SELECT * FROM awards_candidate')
    c.sql('CREATE TABLE rates_daily_filled(currency VARCHAR,rate_date DATE,usd_per_unit DOUBLE)')
    c.sql('CREATE TABLE exchange_rates_annual_v(currency VARCHAR,rate_year INT,usd_per_unit DOUBLE)')
    capture = FakeC(extra=['exchange_rates','exchange_rates_annual']); capture.r = ''
    award_usd.run(capture)
    c.artifact('awards_candidate', [q for name,q in capture.artifacts if name=='awards_candidate'][-1])
    return {r.id: (r.funder_award_id, r.amount, r.amount_usd) for r in c.sql('SELECT * FROM awards_candidate').collect()}


def test_sum_policy_gives_each_project_its_exporter_years_once():
    c = sum_fixture('sum')
    got = projected_usd(c)
    assert got == {
        10: ('R01AI000010', 340000.0, 340000.0),   # 100k + 200k + supplement 50k - 10k; the twin year, Crossref and paper add nothing
        20: ('R01CA000020', None, None),           # partial (FY1998 has no cost): no amount, complete totals only
        70: ('Z01AI000070', None, None),           # intramural, FY2006 has no cost: no amount
        80: ('R01DK000080', 600000.0, 600000.0),   # complete; two flat-budget years both count
        90: ('P01HL000090', None, None),           # began before FY1985: years before NIH's first published one, no amount
        95: ('ZIAAI000095', 90000.0, 90000.0),     # no start date: not held back
        30: ('R01NS000030', None, None),           # no cost at all: no amount
        40: ('R01HL000040', None, None),           # sums to zero: no amount
        50: ('1R13AI000050-01', 6000.0, 6000.0),   # not a survivor: untouched
        60: ('5R01AI000010-05', 70000.0, 70000.0), # a year outside project 10's sum: untouched
    }, got
    assert c.counts['nih_amount_partial'] == 2         # 20 and 70: the totals option B withholds
    assert c.counts['nih_amount_before_fy1985'] == 1   # 90: complete for what NIH publishes, but it began before FY1985
    assert c.counts['nih_amount_twin_years'] == 1      # y03_again: the pick keeps one row per number and counts the other
    assert c.counts['nih_amount_years_outside'] == 1   # 60, not 50 (another project) and not the survivors themselves
    assert c.checks['NIH_PROJECTION_SAME_ROWS'] and c.checks['NIH_SURVIVOR_FUNDER_PUBLISHED']


def test_sum_policy_gives_a_total_only_when_every_bound_year_has_a_cost():
    """Rohan 11-19, option B: complete, partial, intramural before FY2007, zero-sum; a withheld total is counted, never shown short."""
    c = sum_fixture('sum')
    got = {k: v[1] for k, v in projected_usd(c).items()}
    assert got[10] == 340000.0 and got[80] == 600000.0   # complete projects keep their sum
    assert got[20] is None                                # partial: FY1998 (ExPORTER's FY1985-1999 files) has no cost
    assert got[70] is None                                # intramural: a year before FY2007 has no cost
    assert got[40] is None                                # zero-sum: no amount, never $0
    assert got[30] is None                                # no year with a cost
    span = {r.stable_id: (r.amount_sum, r.years_without_cost) for r in c.sql('SELECT * FROM nih_span').collect()}
    assert span[20] == (600000.0, 1) and span[70] == (250000.0, 1) and span[10][1] == 0 and span[80] == (600000.0, 0)
    assert c.counts['nih_amount_partial'] == 2


def test_null_policy_still_blanks_every_project_and_nothing_else():
    got = projected_usd(sum_fixture('null'))
    assert {k: v[1] for k, v in got.items()} == {10: None, 20: None, 30: None, 40: None, 50: 6000.0, 60: 70000.0, 70: None, 80: None,
                                                 90: None, 95: None}


def test_prod_config_sums_nih_project_amounts():
    import json
    c = DB(); c.config = json.loads((ROOT / 'config.prod.json').read_text())
    assert c.config['nih_fold'] == {'amount': 'sum'}             # Rohan 11-16 (Q2); the code default stays null
    assert nih_fold.settings(c)['amount'] == 'sum'


def test_held_pair_does_not_reduce_population_and_elected_amount_stays_convertible():
    c = fixture(); c.config['nih_fold']={'amount':'elected'}
    c.sql('UPDATE resolve_candidate SET stable_id=owner_id WHERE owner_id=981')
    assert fp.expected(c,'usd')[0]['awards_with_amount_usd']==981


def test_expected_population_cannot_shrink_to_match_a_broken_candidate():
    c = fixture(); c.nih = False
    c.sql('DROP TABLE awards_candidate')
    # Expected quantities are derived from yesterday and the checked resolver only.
    assert fp.expected(c,'orgs')[0]['awards_with_recipients']==980
    with pytest.raises(RuntimeError,match='RECIPIENT_ORG_COUNT_DROP'):
        fp.gate(c,'orgs','RECIPIENT_ORG_COUNT_DROP','awards_with_recipients',900,.01)


def test_geography_fold_reapplies_basis_priority_before_counting_memberships():
    c = fixture(); c.nih = False
    c.sql("""UPDATE previous_api_v SET recipient_countries_full=[{'country_code':'KE','share':1.0,
      'amount_usd':100.0,'basis':'organisation_location'}] WHERE id>980""")
    # Each target's funder-reported US location wins over the loser's org location.
    assert fp.expected(c,'geography')[0]['recipient_country_memberships']==980
