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
      [{'id':concat('I',i),'display_name':concat('Org',i),'ror':NULL::VARCHAR,'country_code':'US','role':'lead',
        'share':NULL::DOUBLE,'basis':'institution_name','asserted_by':'funder'}] recipients_full,
      [{'country_code':'US','share':1.0,'amount_usd':100.0,'basis':'funder_reported'}] recipient_countries_full,
      []::STRUCT(code VARCHAR,vocabulary VARCHAR,display_name VARCHAR,share DOUBLE)[] recipient_regions_full,
      concat('R01AA',i) funder_award_id,'Title' display_name,NULL::VARCHAR description,NULL::VARCHAR lead_investigator,
      NULL::VARCHAR co_lead_investigator,NULL::VARCHAR investigators,DATE '2020-01-01' start_date,DATE '2021-01-01' end_date,
      2020 start_year,2021 end_year,4320332161::BIGINT funder_id,TIMESTAMP '2026-10-01' updated_date
      FROM range(1,1001) t(i)""")
    c.sql("""CREATE TABLE resolve_candidate AS SELECT id owner_id,
      CASE WHEN id>980 THEN id-980 ELSE id END stable_id,'ACTIVE' status FROM previous_api_v""")
    c.sql("""CREATE TABLE migration_manifest_v AS SELECT id+980 loser_id,id target_id,'NIH_PROJECT_YEAR_FOLD' evidence_class,
      to_json({'primary_award_id':funder_award_id,'title_from':funder_award_id}) evidence_json FROM previous_api_v WHERE id<=20""")
    c.sql("""CREATE TABLE nih_survivors AS SELECT target_id stable_id,4320332161::BIGINT pinned_funder_id FROM migration_manifest_v""")
    c.sql("""CREATE TABLE bindings_final AS SELECT r.stable_id,cast(p.id AS VARCHAR) observation_key,true is_direct,10 priority,p payload
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


def test_duplicate_org_membership_collapses_but_distinct_role_does_not():
    c = fixture(); c.nih = False
    c.sql('UPDATE previous_api_v SET recipients_full=(SELECT recipients_full FROM previous_api_v WHERE id=1) WHERE id=981')
    assert fp.expected(c,'orgs')[0]['recipient_org_memberships'] == 999
    c._funding_population = {}
    c.sql("UPDATE previous_api_v SET recipients_full=from_json(replace(to_json(recipients_full),'lead','partner'),'[{\"id\":\"VARCHAR\",\"display_name\":\"VARCHAR\",\"ror\":\"VARCHAR\",\"country_code\":\"VARCHAR\",\"role\":\"VARCHAR\",\"share\":\"DOUBLE\",\"basis\":\"VARCHAR\",\"asserted_by\":\"VARCHAR\"}]') WHERE id=981")
    assert fp.expected(c,'orgs')[0]['recipient_org_memberships'] == 1000


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


def test_held_pair_does_not_reduce_population_and_elected_amount_stays_convertible():
    c = fixture(); c.config['nih_fold']={'amount':'elected'}
    c.sql('UPDATE resolve_candidate SET stable_id=owner_id WHERE owner_id=981')
    assert fp.expected(c,'usd')[0]['awards_with_amount_usd']==981
