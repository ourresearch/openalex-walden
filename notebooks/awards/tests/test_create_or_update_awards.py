"""Offline contract checks. Warehouse execution/Delta semantics are proved in BUILD-PLAINSQL.md."""
from pathlib import Path
import re
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
AWARDS = ROOT / 'notebooks/awards'
SQL = (AWARDS / 'CreateOrUpdateAwards.sql').read_text()
CELLS = SQL.split('-- COMMAND ----------')
SOURCES = [('nsf_award_search', 3), ('wellcome_trust', 22), ('cordis', 27),
           ('gates_foundation', 4), ('kaken', 10)]
COLUMNS = ('id display_name description funder_id funder_award_id amount currency funder '
           'funding_type funder_scheme provenance start_date end_date start_year end_year '
           'lead_investigator co_lead_investigator investigators landing_page_url doi '
           'works_api_url created_date updated_date priority').split()


def test_plain_sql_only():
    assert SQL.startswith('-- Databricks notebook source')
    assert not (AWARDS / 'CreateOrUpdateAwards.py').exists()
    assert not re.search(r'(?m)^\s*(import |from |def |for |%python|# MAGIC)', SQL)
    assert '{{' not in SQL and '${' not in SQL
    assert 'sources.csv and sources/*.sql are reference documentation only' in SQL
    assert SQL.count('MERGE INTO ') == 5
    assert 'UNION ALL' not in SQL and 'WHEN NOT MATCHED BY SOURCE' not in SQL
    assert not re.search(r'(?im)^\s*(DELETE|UPDATE|INSERT|RESTORE)\s', SQL)


def test_closed_parameter_guard():
    first = CELLS[0]
    assert "(:stage = 'prod' AND :raw_table = 'openalex.awards.openalex_awards_raw')" in first
    assert "OR (:stage = 'dev' AND :raw_table = 'openalex_dev.rohan_lab.gr1009_review_shadow_raw')" in first
    assert 'raise_error(' in first and 'must match an allowed production or review shadow pair' in first
    assert SQL.count('DESCRIBE HISTORY IDENTIFIER(:raw_table)') == 10
    assert 'DESCRIBE HISTORY openalex_dev.' not in SQL
    assert 'IDENTIFIER(session.awards_target)' in SQL


@pytest.mark.parametrize('source,priority', SOURCES)
def test_source_contract(source, priority):
    group = SQL[SQL.index(f'-- {source}: candidate mapping.'):]
    group = group[:group.index(f'SELECT to_json(session.{source}_result) AS result;')]
    assert "format => 'parquet'" in group
    assert '_metadata.file_path' in group and '_metadata.file_modification_time' in group
    assert 'ORDER BY _file_modified DESC, _file_path DESC LIMIT 1' in group
    assert f'WHERE _file_path = session.{source}_object' in group
    assert 'VERSION AS OF ' in group
    assert 'rows_in * 100 < rows_prev * 98 OR duplicate_keys > 0 OR invalid_rows > 0' in group
    assert group.count('GROUP BY ALL HAVING count(*) > 1') == 2
    assert f"WHERE session.{source}_result.state = 'written'" in group
    assert f'USING {source}_guarded_candidate c' in group
    assert f"ON t.provenance = '{source}' AND t.priority = {priority}" in group
    assert 'lower(t.funder_award_id) = lower(c.funder_award_id)' in group
    assert "WHEN MATCHED AND c._change = 'changed'" in group
    assert 'created_date = t.created_date' in group and 'funder = t.funder' in group
    updates = re.search(r'THEN UPDATE SET\s+(.*?)\s+WHEN NOT MATCHED', group, re.S).group(1)
    assert [assignment.split('=')[0].strip() for assignment in updates.split(',')] == COLUMNS
    insert = re.search(r'WHEN NOT MATCHED THEN INSERT \(([^)]+)\)', group).group(1)
    assert [c.strip() for c in insert.split(',')] == COLUMNS
    for name in ['display_name', 'description', 'funder_award_id', 'currency']:
        assert f'rtrim(i.{name}) <=> rtrim(b.{name})' in group
    assert 'rtrim(x3.asserted_by)' in group
    assert 'i.funder <=> b.funder' not in group
    assert 'CASE WHEN _exists THEN baseline_row.funder ELSE funder END AS funder' in group
    assert 'CASE WHEN _exists THEN baseline_row.created_date ELSE created_date END AS created_date' in group
    assert 'CASE WHEN _exists AND _unchanged THEN baseline_row.updated_date' in group
    assert f"array_append(session.awards_skipped, '{source}')" in group


def test_deferred_failure_and_frozen_result():
    assert SQL.count('DECLARE OR REPLACE VARIABLE ') == 15
    assert 'raise_error(' in CELLS[-1] and 'size(session.awards_skipped) > 0' in CELLS[-1]
    for source, _ in SOURCES:
        assert SQL.index(f'SET VAR {source}_result =') < SQL.index(f'USING {source}_guarded_candidate c')


def test_prod_job():
    job = yaml.safe_load((ROOT / 'jobs/create_or_update_awards.yaml').read_text())['resources']['jobs']['Create_Or_Update_Awards']
    assert job['name'] == 'Awards'
    assert job['schedule'] == {'quartz_cron_expression': '0 30 0 * * ?', 'timezone_id': 'UTC', 'pause_status': 'UNPAUSED'}
    task = job['tasks'][0]['notebook_task']
    assert task['notebook_path'] == 'notebooks/awards/CreateOrUpdateAwards'
    assert task['base_parameters'] == {'raw_table': 'openalex.awards.openalex_awards_raw', 'stage': 'prod'}
    assert job['max_concurrent_runs'] == 1 and job['email_notifications']['on_failure']

    assert job['tags']['stage'] == 'prod'
    assert job['environments'][0]['spec']['client'] == '2'
    assert 'new_cluster' not in job['tasks'][0] and 'existing_cluster_id' not in job['tasks'][0]


def test_production_source_prefixes():
    prefixes = re.findall(r"read_files\('([^']+)'", SQL)
    assert prefixes == [
        's3://openalex-ingest/awards/nsf/*.parquet',
        's3://openalex-ingest/awards/wellcome/*.parquet',
        's3://openalex-ingest/awards/european_commission/*.parquet',
        's3://openalex-ingest/awards/gates/*.parquet',
        's3://openalex-ingest/awards/kaken/*.parquet',
    ]
    assert 'dev-gr1009' not in SQL
