"""Offline recipient matching cases execute the production selection/matching SQL in DuckDB."""
import json
import pytest
from test_award_flows import FakeC, ROOT, BASE
import award_recipients
import award_recipient_orgs
import create_api


def queries():
    c=FakeC(extra=['affiliation_answers']); c.r=''
    award_recipient_orgs.run(c)
    return dict(c.artifacts)


def matched(names, answers, institutions):
    db=pytest.importorskip('duckdb').connect()
    db.execute('CREATE TABLE recipients_resolved(award_id BIGINT,name VARCHAR,role VARCHAR,country_code VARCHAR,ror VARCHAR,share DOUBLE,basis VARCHAR,asserted_by VARCHAR,kind VARCHAR,dropped VARCHAR)')
    for row in names:
        db.execute("INSERT INTO recipients_resolved VALUES (1,?,?,?,?,?,'funder_reported','funder','organisation',NULL)",row)
    db.execute('CREATE TABLE affiliation_answers_v(raw_affiliation_string VARCHAR,institution_ids BIGINT[])')
    for row in answers: db.execute('INSERT INTO affiliation_answers_v VALUES (?,?)',row)
    db.execute('CREATE TABLE institutions_api_v(id BIGINT,display_name VARCHAR,ror VARCHAR,country_code VARCHAR)')
    for row in institutions: db.execute('INSERT INTO institutions_api_v VALUES (?,?,?,?)',row)
    q=queries()
    for name in ('recipient_org_selected','recipient_org_names','recipient_org_matches'):
        db.execute('CREATE TABLE '+name+' AS '+q[name].replace('explode(', 'unnest('))
    return db


def test_stored_empty_answer_is_final_even_when_exact_name_exists():
    db=matched([('University A','lead','GB',None,None)],[('University A',[])],[(1,'University A',None,'GB')])
    assert db.execute('SELECT * FROM recipient_org_matches').fetchall()==[]
    assert db.execute('SELECT name,share FROM recipient_org_selected').fetchall()==[('University A',None)]


def test_country_guard_rejects_foreign_twin_and_keeps_local_partner():
    db=matched([('Partner','implementing','KE',None,.25)],[('Partner',[1,2])],[(1,'Partner GB',None,'GB'),(2,'Partner KE',None,'KE')])
    assert db.execute('SELECT institution_id,match_basis FROM recipient_org_matches').fetchall()==[(2,'affiliation_matcher')]
    assert db.execute('SELECT role,share FROM recipient_org_selected').fetchall()==[('implementing',.25)]


def test_ambiguous_multi_institution_answer_stays_unmatched():
    db=matched([('Consortium','partner',None,None,None)],[('Consortium',[1,2])],[(1,'One',None,'GB'),(2,'Two',None,'KE')])
    assert db.execute('SELECT count(*) FROM recipient_org_matches').fetchone()==(0,)
    assert db.execute('SELECT count(*) FROM recipient_org_selected').fetchone()==(1,)


def test_explicit_ror_takes_precedence_and_country_still_applies():
    db=matched([('Local name','host','KE','https://ror.org/012345678',None)], [('Local name',[])], [(1,'Canonical','https://ror.org/012345678','KE')])
    assert db.execute('SELECT institution_id,match_basis FROM recipient_org_matches').fetchall()==[(1,'ror')]
    db=matched([('Local name','host','GB','https://ror.org/012345678',None)], [], [(1,'Canonical','https://ror.org/012345678','KE')])
    assert db.execute('SELECT count(*) FROM recipient_org_matches').fetchone()==(0,)


def test_unambiguous_exact_name_and_sovereign_territory_match():
    db=matched([('  UNIVERSITY A ','lead','US',None,None)],[],[(1,'University A','https://ror.org/012345678','PR')])
    assert db.execute('SELECT institution_id,match_basis FROM recipient_org_matches').fetchall()==[(1,'institution_name')]


def test_duplicate_exact_names_in_same_country_are_ambiguous():
    db=matched([('Institute','lead','US',None,None)],[],[(1,'Institute',None,'US'),(2,'Institute',None,'US')])
    assert db.execute('SELECT count(*) FROM recipient_org_matches').fetchone()==(0,)


def test_roles_survive_and_identical_source_rows_deduplicate():
    row=('Partner','partner','KE',None,None)
    db=matched([row,row,('Partner','implementing','KE',None,None)],[],[])
    assert db.execute('SELECT role FROM recipient_org_selected ORDER BY role').fetchall()==[('implementing',),('partner',)]


def test_geography_never_ranks_or_splits_organisation_rows():
    c=FakeC(extra=['award_recipients_raw']); award_recipients.run(c)
    q=dict(c.artifacts)
    assert "kind IN ('country','region')" in q['recipients_selected']
    assert 'institution_awarded' not in q['awards_candidate']
    assert 'slice(coalesce(o.recipients,array()),1,100) recipients' in q['awards_candidate']
    assert "'share',share" in q['recipient_org_arrays']
    assert 'sort_array(collect_set(' in q['recipient_org_arrays']
    assert 'coalesce(m.display_name,s.name)' in q['recipient_org_objects']
    assert 'RECIPIENT_ORG_CONFLICT' in c.checks
    assert 'RECIPIENT_ORG_MATCH_DROP' in c.checks


def test_hash_empty_orgs_preserves_replay8_and_nonempty_orgs_wrap_it():
    cols=set(create_api.FLOW_COLUMNS)|set(create_api.LINK_COLUMNS)
    old=create_api.hash_expression(BASE,cols)
    new=create_api.hash_expression(BASE,cols|{'recipients_full'})
    assert new==f"CASE WHEN coalesce(size(recipients_full),0)=0 THEN {old} ELSE xxhash64(concat_ws('|',CAST(({old}) AS STRING),to_json(recipients_full))) END"


def test_producer_contains_all_feeds_children_roles_and_no_landing_country_guess():
    sql=(ROOT/'sql/recipient_organisations_raw.sql').read_text()
    nb=json.loads((ROOT.parent/'CreateAwardRecipientsRaw.ipynb').read_text())
    assert sql in ''.join(nb['cells'][1]['source'])
    for table in ['usaid_awards_raw','cdc_awards_raw','dod_awards_raw','usda_awards_raw','idrc_raw','iati_raw','gates_raw','wellcome_raw']:
        assert 'openalex.awards.'+table in sql
    assert 'children_participating_orgs_json' in sql
    assert "WHEN '2' THEN 'partner' WHEN '4' THEN 'implementing'" in sql
    assert "WHERE o.role IN ('2','4')" in sql and "f.role='3'" in sql
    assert 'recipient_countries_json' not in sql and 'primary_place_of_performance' not in sql
    assert 'try_cast(o.percentage AS DOUBLE)/100.0' in sql


def test_missing_input_and_clear_cover_organisation_only_awards():
    c=FakeC(prev_cols=['id','recipients_full']); award_recipients.raw_input(c)
    assert 'RECIPIENT_ORG_INPUT_REQUIRED' in c.checks
    c=FakeC(config={'clear_funding_flows':True}); award_recipients.run(c)
    assert 'recipients_raw_empty' in dict(c.artifacts)
    assert 'CAST(NULL AS STRING) name' in award_recipients.EMPTY_RAW


def test_api_projects_orgs_without_altering_existing_institution_ctes():
    sql=(ROOT/'sql/api_payload.sql').read_text()
    assert 'oa.recipients,' in sql and 'oa.recipients_full' in sql
    assert 'recipient_org' not in sql.split('SELECT\n  oa.id',1)[0]


def test_known_wrong_stored_answer_is_rejected_without_dropping_organisation():
    db=matched([('NIRAS Sweden AB','partner','SE',None,None)], [('NIRAS Sweden AB',[4210162184])], [(4210162184,'Irras (Sweden)',None,'SE')])
    assert db.execute('SELECT count(*) FROM recipient_org_matches').fetchone()==(0,)
    assert db.execute('SELECT name FROM recipient_org_selected').fetchone()==('NIRAS Sweden AB',)


def test_producer_excludes_explicit_individuals_in_all_four_us_feeds():
    sql=(ROOT/'sql/recipient_organisations_raw.sql').read_text()
    assert sql.count("upper(trim(business_types_code))='P'")==4
    assert sql.count("IN ('INDIVIDUAL','INDIVIDUALS')")==4


def test_organisation_feed_loss_is_fatal_on_first_release_night():
    c=FakeC(extra=['award_recipients_raw'])
    c.config['bootstrap_counts']['recipients_rows_gates_foundation_organisation']=100
    with pytest.raises(RuntimeError,match='RECIPIENT_ROWS_DROP_gates_foundation_organisation: 100 -> 0'):
        award_recipients.run(c)


@pytest.mark.parametrize('key,gate',[
    ('awards_with_recipients','RECIPIENT_ORG_COUNT_DROP'),
    ('awards_with_matched_recipients','RECIPIENT_ORG_MATCH_DROP'),
    ('recipient_org_memberships','RECIPIENT_ORG_MEMBERSHIP_DROP'),
])
def test_organisation_served_loss_is_fatal(key,gate):
    c=FakeC(); c.config['bootstrap_counts'][key]=100
    with pytest.raises(RuntimeError,match=gate+': 100 -> 0'):
        award_recipient_orgs.check_counts(c,.01)


def grouped(db):
    # Execute production grouping, translating only aggregate spellings for DuckDB.
    q = queries()
    for name in ('recipient_org_objects', 'recipient_org_grouped'):
        sql = q[name].replace('<=>', ' IS NOT DISTINCT FROM ')
        sql = sql.replace('sort_array(collect_set(role))', 'list_sort(list(DISTINCT role))')
        db.execute('CREATE TABLE '+name+' AS '+sql)
    return db.execute('SELECT id,display_name,country_code,roles,share,basis FROM recipient_org_grouped ORDER BY country_code,basis,share').fetchall()


def test_served_roles_union_and_cross_country_unmatched_are_distinct():
    db=matched([('Partner','partner','KE',None,None),('Partner','implementing','KE',None,None),
                ('Partner','partner','GB',None,None)],[],[])
    assert grouped(db)==[(None,'Partner','GB',['partner'],None,'funder_reported'),
                         (None,'Partner','KE',['implementing','partner'],None,'funder_reported')]


def test_same_institution_two_match_methods_merge_to_strongest_basis():
    db=matched([('Alias','partner','KE',None,None),('Canonical','implementing','KE',None,None)],
               [('Alias',[1])],[(1,'Canonical',None,'KE')])
    assert grouped(db)==[('https://openalex.org/I1','Canonical','KE',['implementing','partner'],None,'affiliation_matcher')]


def test_distinct_share_assertions_never_sum_or_disappear():
    db=matched([('Partner','partner','KE',None,.25),('Partner','implementing','KE',None,.5)],[],[])
    assert [r[4] for r in grouped(db)]==[.25,.5]


def test_explicit_ror_evidence_wins_over_matcher_for_same_identity():
    db=matched([('Alias','partner','KE',None,None),('Canonical','implementing','KE','https://ror.org/012345678',None)],
               [('Alias',[1])],[(1,'Canonical','https://ror.org/012345678','KE')])
    assert grouped(db)==[('https://openalex.org/I1','Canonical','KE',['implementing','partner'],None,'ror')]
