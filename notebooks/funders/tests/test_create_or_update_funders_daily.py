"""Offline contract checks for the Funders daily loop and the one-time seed (no warehouse access)."""
import json
import re
import sqlite3
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
NB = ROOT / 'notebooks/funders/CreateOrUpdateFunders.ipynb'
SEED_NB = ROOT / 'notebooks/funders/SeedFunders.ipynb'
CELLS = [''.join(c['source']) for c in json.loads(NB.read_text(encoding='utf-8'))['cells']]
SEED = [''.join(c['source']) for c in json.loads(SEED_NB.read_text(encoding='utf-8'))['cells']]
REGISTRY = ('funder', 'funder_id', 'funder_source_record', 'funder_decision', 'f_ids_ever_issued')
WRITE = re.compile(r"(?:MERGE\s+INTO|INSERT\s+INTO|INSERT\s+OVERWRITE|UPDATE|DELETE\s+FROM)\s+IDENTIFIER\(trim\(:prefix\)\s*\|\|\s*'(\w+)'\)")
LIMIT = "coalesce(try_cast(nullif(trim(:max_mints), '') AS BIGINT), 200)"


def cell(tag):
    hits = [i for i, s in enumerate(CELLS) if tag in s]
    assert len(hits) == 1, (tag, hits)
    return hits[0]


def registry_writes(s):
    return [t for t in WRITE.findall(s) if t in REGISTRY]


def test_mint_fuse_precedes_first_registry_write():
    first = min(i for i, s in enumerate(CELLS) if registry_writes(s))
    assert first == cell('-- promote_stage:') == cell('-- mint_fuse:')       # the one fuse opens the first writing cell
    s = CELLS[first]
    assert s.index('-- mint_fuse:') < s.index(LIMIT) < s.index('MERGE INTO')
    assert sum(LIMIT in c for c in CELLS) == 1, 'exactly one mint fuse'
    assert cell('-- never_issued') < first
    # Cells before the fuse write stage copies only; the stage is built before the first of them.
    stage = cell('-- fuse_stage:')
    assert stage < cell('-- Apply reviewed edits') < cell('-- upsert_sources') < cell('-- Future-only successor plan') < cell('-- decide') < first
    for i in range(stage, first):
        assert not registry_writes(CELLS[i]), i
    assert "'fuse_stage_funder')" in CELLS[cell('-- decide')]
    assert "'fuse_stage_funder_source_record')" in CELLS[cell('-- decide')]


def test_promote_rechecks_fuse_before_writing():
    s = CELLS[cell('-- promote_stage:')]
    assert s.index('-- mint_fuse:') < s.index('MERGE INTO')
    for t in ('funder_source_record', 'funder', 'funder_id'):
        assert f"USING IDENTIFIER(trim(:prefix) || 'fuse_stage_{t}')" in s
    assert "'ror_successor'" in s and "'ror_successor'" not in CELLS[cell('-- Future-only successor plan')]


def test_max_mints_zero_means_zero():
    assert LIMIT in CELLS[cell('-- promote_stage:')]
    assert not any('nullif(CAST(:max_mints' in s for s in CELLS)
    # Same expression in SQLite (try_cast -> CAST; cell 0 already rejects non-integers).
    expr = LIMIT.replace('try_cast', 'CAST').replace(':max_mints', '?').replace('BIGINT', 'INTEGER')
    db = sqlite3.connect(':memory:')
    got = {v: db.execute('SELECT ' + expr, (v,)).fetchone()[0] for v in ('0', '', ' ', None, '150', '200')}
    assert got == {'0': 0, '': 200, ' ': 200, None: 200, '150': 150, '200': 200}


def test_propagation_covers_every_unpropagated_merge():
    plan = cell('-- propagation_map')
    record = cell('-- propagation_record:')
    publish = cell('-- publish_compat')
    prop = [i for i, s in enumerate(CELLS) if s.startswith('%sql\n-- propagation_')]
    assert prop[0] == plan and prop[-1] == record and record == publish - 1
    for i in prop:
        assert "run_id=('fd' || trim(:run_id))" not in CELLS[i], i
        assert "LEFT ANTI JOIN IDENTIFIER(trim(:prefix) || 'funders_propagated')" in CELLS[i], i
    s = CELLS[plan]
    body = s[s.index('CREATE TABLE IF NOT EXISTS IDENTIFIER(trim(:prefix) || \'funders_propagation_\''):]
    assert "run_id = ('fd'" not in body and 'run_id=(' not in body
    assert "WHERE f.status IN ('merged','withdrawn')" in body
    assert "INSERT INTO IDENTIFIER(trim(:prefix) || 'funders_propagated')" in CELLS[record]
    # Ledger exists (baseline from the pinned pre-run registry) before any registry write.
    ledger = CELLS[cell('-- fuse_stage:')]
    assert "CREATE TABLE IF NOT EXISTS IDENTIFIER(trim(:prefix) || 'funders_propagated') AS" in ledger
    assert 'FROM registry_prev_funder' in ledger


def test_cell0_guard_unchanged():
    assert "(trim(:prefix)='openalex.funders.' AND trim(:allow_prod)=trim(:prefix))" in CELLS[0]
    assert "assert_true(try_cast(:max_mints AS BIGINT)>=0,'invalid mint fuse')" in CELLS[0]


def test_daily_yaml_is_production_unpaused():
    job = yaml.safe_load((ROOT / 'jobs/create_or_update_funders.yaml').read_text())['resources']['jobs']['Create_Or_Update_Funders']
    assert job['schedule'] == {'quartz_cron_expression': '0 45 21 * * ?', 'timezone_id': 'UTC', 'pause_status': 'UNPAUSED'}
    task = job['tasks'][0]['notebook_task']
    assert task['notebook_path'] == 'notebooks/funders/CreateOrUpdateFunders'
    p = task['base_parameters']
    assert set(p) == {'prefix', 'allow_prod', 'run_id', 'max_mints'}, p   # no mode, no seed parameters: the job runs only the daily loop
    assert p['prefix'] == p['allow_prod'] == 'openalex.funders.'
    assert p['max_mints'] == '280' and p['run_id'] == '{{job.run_id}}'
    assert job['tags'] == {'stage': 'prod', 'lane': 'funders'}


# --- NIH identity guard and name-lookup re-sign (funders-daily-on-resign, 10-14) ---------------------------------------
NIH_IDS = {4320332161, 4320337611, 4320337356, 4320337376, 4320337472, 4320337358, 4320337593, 4320337375, 4320337355,
           4320337362, 4320337363, 4320337364, 4320337365, 4320337357, 4320337361, 4320337354, 4320337346, 4320337359,
           4320337353, 4320337337, 4320337330, 4320337352, 4320337347, 4320337534, 4320337372, 4320337351, 4320337374,
           4320337350, 4320337338, 4320337348}   # grants-roadmap/nih-port/ship/s0b_funder_identity_post_switch.sql
SIGNATURE = ("concat(count(*), char(58), cast(sum(cast(xxhash64(funder_id,display_name,alternate_titles,ror_id,doi,crossref_id,"
             "merge_into_id) AS DECIMAL(38,0))) AS STRING))")
LOOKUP = ("IDENTIFIER(CASE WHEN trim(:prefix) = 'openalex.funders.' THEN 'openalex.funders.funder_name_lookup' "
          "ELSE trim(:prefix) || 'signed_name_lookup' END)")


def nih_list(s):
    m = re.search(r'explode\(array\(([0-9,]+)\)\) funder_id', s)
    return {int(x) for x in m.group(1).split(',')}


def test_nih_guard_runs_before_first_registry_write_and_writes_nothing():
    guard, pin, promote = cell('-- nih_identity_guard'), cell('-- name_lookup_resign_pin:'), cell('-- promote_stage:')
    assert cell('-- never_issued') < guard < pin < promote == guard + 2
    s = CELLS[guard]
    assert nih_list(s) == NIH_IDS and len(NIH_IDS) == 30
    assert not registry_writes(s) and not re.search(r'\b(MERGE|INSERT|UPDATE|DELETE)\b', s)
    for t in ('fuse_stage_funder', 'fuse_stage_funder_id', 'ror_successor_plan', 'funder_decision_tonight'):
        assert f"IDENTIFIER(trim(:prefix) || '{t}')" in s, t
    assert "action='AUTO_MERGE_ROR_SUCCESSOR'" in s and "action='ATTACH_ROR'" in s
    assert s.count('assert_true(') == 7


def test_nih_recheck_after_registry_writes_before_propagation():
    recheck = cell('-- nih_identity_recheck')
    last_write = max(i for i, s in enumerate(CELLS) if registry_writes(s))
    assert last_write < recheck == cell('-- propagation_map') - 1
    s = CELLS[recheck]
    assert 'registry_prev_funder' in s and 'nih_guard_ids' in s and not registry_writes(s)


def test_pin_reads_lookup_only_through_execute_immediate():
    s = CELLS[cell('-- name_lookup_resign_pin:')]
    assert not registry_writes(s)
    before_exec = s[:s.index('EXECUTE IMMEDIATE')]
    assert 'FROM ' + LOOKUP not in before_exec          # an absent lookup is never analyzed
    assert 'lookup_signature=pre_signature' in s


def switch_lookup_sql():
    rel = json.loads((ROOT / 'notebooks/funders/PublishFunderNameRelease.ipynb').read_text(encoding='utf-8'))['cells']
    src = next(''.join(c['source']) for c in rel if "'lookup_rows') AS" in ''.join(c['source']))
    head = "CREATE TABLE IDENTIFIER(:release_prefix || 'lookup_rows') AS\n"
    return src[src.index(head) + len(head):src.index(';\nEND IF;')]


def test_resign_reuses_the_switch_lookup_build_sql():
    s = CELLS[cell('-- resign_name_lookup:')]
    body = switch_lookup_sql()
    compat = "IDENTIFIER(trim(:prefix) || 'funders_compat')"
    for a, b in {
        "IDENTIFIER(:release_prefix || 'catalogue_after')": compat,
        "IDENTIFIER(:release_prefix || 'identifiers')": f"(SELECT 'ror' id_type,ror_id id_value,funder_id,funder_id origin_funder_id FROM {compat} WHERE ror_id IS NOT NULL)",
        "IDENTIFIER(:release_prefix || 'merges')": "(SELECT CAST(NULL AS BIGINT) loser_id,CAST(NULL AS BIGINT) survivor_id WHERE false)",
        "IDENTIFIER(:release_prefix || 'aliases')": "(SELECT CAST(NULL AS STRING) rule,CAST(NULL AS STRING) name_key,CAST(NULL AS BIGINT) funder_id WHERE false)",
    }.items():
        assert a in body
        body = body.replace(a, b)
    assert "CREATE OR REPLACE TABLE IDENTIFIER(trim(:prefix) || 'name_lookup_resign_rows') AS\n" + body + ';\n' in s
    # The switch defers every twin merge; the empty merge relation must stay in step with it.
    rel = (ROOT / 'notebooks/funders/release/integrated/044_name_stage.sql').read_text()
    assert 'SELECT CAST(NULL AS BIGINT) loser_id,CAST(NULL AS BIGINT) survivor_id WHERE false' in rel


def test_resign_follows_publish_compat_and_gates_before_replacing():
    compat, resign = cell('-- publish_compat'), cell('-- resign_name_lookup:')
    assert resign == compat + 1
    s = CELLS[resign]
    assert "IF (SELECT count(*) FROM IDENTIFIER(trim(:prefix) || 'name_lookup_resign_pin') WHERE run_id = ('fd' || trim(:run_id)) AND lookup_rows > 0) = 1 THEN" in s
    replace = s.index('CREATE OR REPLACE TABLE ' + LOOKUP)
    assert s.index('resign_gates_ok') < replace < s.index('funder_names_active')
    assert s.index('DEEP CLONE') < replace
    for msg in ('duplicate lookup key', 'not an unmerged catalogue funder', 'unchanged funder was altered or lost',
                'did not mint', 'more than one signature'):
        assert msg in s[:replace], msg
    assert "m.created_by='mint_ror'" in s and 'LEFT ANTI JOIN ' + LOOKUP + ' l ON l.rule=r.rule AND l.name_key=r.name_key' in s
    assert not registry_writes(s)


def test_resign_signs_exactly_as_the_readers_check():
    s = CELLS[cell('-- resign_name_lookup:')]
    assert SIGNATURE in s
    assert SIGNATURE.replace('char(58)', "':'") in s     # the signature written equals the one checked (char(58) = ':')
    for nb in ('CreateCrossrefWorkFunders', 'CreateDataCiteWorkFunders', 'CreateFunderReportedWorkFunders'):
        reader = ''.join(''.join(c['source']) for c in json.loads((ROOT / f'notebooks/awards/{nb}.ipynb').read_text())['cells'])
        assert 'WHERE catalogue_signature = (SELECT ' + SIGNATURE + ' FROM openalex.funders.funders)' in reader, nb
    assert "THEN 'openalex.funders.funders' ELSE trim(:prefix) || 'funders_compat' END" in s


# --- Daily/seed split (funders-daily-split, 10-15) --------------------------------------------------------------------------
PARAM = re.compile(r'(?<![?\w:]):([a-z_]+)\b')   # SQL named parameters (not (?:...) groups or URLs)
STATEMENT_START = re.compile(r'^(?:BEGIN|IF|END|DECLARE|SET|EXECUTE|FOR|SELECT|CREATE|INSERT|MERGE|UPDATE|DELETE|WITH|--)')


def test_daily_has_no_mode_and_no_seed():
    for i, s in enumerate(CELLS):
        assert ':mode' not in s and ':seed_source' not in s and ':seed_version' not in s, i
        assert 'registry_seed_funders' not in s and 'seed_v41' not in s.replace("'seed_v41_merged'", ''), i
        assert not re.search(r'\bCREATE TABLE IDENTIFIER\(', s), i          # the daily never creates the registry tables
    assert {p for s in CELLS for p in PARAM.findall(s)} == {'prefix', 'allow_prod', 'run_id', 'max_mints'}
    # one job, daily only: nothing in jobs/ runs the seed
    assert not [p.name for p in (ROOT / 'jobs').glob('*.yaml') if re.search(r'notebook_path:\s*\S*SeedFunders', p.read_text())]


def test_daily_dead_branches_stay_removed():
    text = '\n'.join(CELLS)
    for dead in ("'MINT_WITH_ROR'", "'MINT_ROR_DATACITE'", "'IGNORE_MINT_OFF'", "('ATTACH_ROR', 'ATTACH_ROR')", 'crossref_registry',
                 "ELSE 'fundref:' END", "LIKE 'REVIEW%'", "LIKE 'IGNORE%'", 'true AND', 'withdraw_unlinked', 'publish_names'):
        assert dead not in text, dead
    assert "SELECT 'ror' source, rid source_key, action," in CELLS[cell('-- decide')]   # why 'fundref:' keys cannot occur
    # dev harness, informational before-counts and the duplicate fuse are gone (round 2)
    assert "trim(:prefix) <> 'openalex.funders.'" not in text and "'funders_api" not in text and 'funder_name_twin_merges' not in text
    assert not [s for s in CELLS if s.startswith('%sql\n-- propagation_before_')]
    assert 'mint_fuse_recheck' not in text


def test_daily_collections_are_ordered():
    # Every collect_list/collect_set is array_sort-ed, except the switch's verbatim lookup SQL, which reads its two unsorted
    # sets only through size(targets)=1 and get(targets,0) (order-free).
    order_free = {cell('-- resign_name_lookup:')}
    for i, s in enumerate(CELLS):
        for m in re.finditer(r'(?i)collect_(?:list|set)\(', s):
            before = re.sub(r'\s+', '', s[max(0, m.start() - 60):m.start()].lower())
            if re.search(r'(array_sort|sort_array)\((flatten\(|transform\(filter\()?$', before):
                continue
            assert i in order_free, (i, s[m.start() - 60:m.start() + 60])
    for i in order_free:
        s = CELLS[i]
        assert 'get(targets,0) FROM normalized_names WHERE size(targets)=1' in s and 'get(targets,0) FROM twin_names WHERE size(targets)=1' in s


def test_seed_is_create_if_absent_and_fill_if_empty():
    assert SEED[0].count('assert_true(') == 4 and 'openalex.funders.' not in SEED[0].split('SELECT', 1)[1].split('seed_source')[0]   # dev prefixes only
    assert all(c.startswith('%sql') for c in SEED) and all(':mode' not in c for c in SEED)
    text = '\n'.join(SEED)
    assert not re.search(r'CREATE OR REPLACE TABLE|MERGE INTO|UPDATE IDENTIFIER|DELETE FROM|INSERT OVERWRITE', text)
    assert not re.search(r'CREATE TABLE IDENTIFIER', text)                  # every CREATE TABLE is IF NOT EXISTS
    inserts = 0
    for s in SEED:
        for m in re.finditer(r"INSERT INTO IDENTIFIER\(trim\(:prefix\) \|\| '(\w+)'\)", s):
            inserts += 1
            guard = f"IF (SELECT count(*) FROM IDENTIFIER(trim(:prefix) || '{m.group(1)}')) = 0 THEN"
            assert guard in s[:m.start()], m.group(1)
    assert inserts == 4
    # the seed's first catalogue is the daily's publish_compat SELECT, byte for byte
    daily_pub = CELLS[cell('-- publish_compat')]
    seed_pub = next(s for s in SEED if '-- publish_compat' in s)
    assert daily_pub[daily_pub.index('\nSELECT '):] == seed_pub[seed_pub.index('\nSELECT '):]
    assert "CREATE TABLE IF NOT EXISTS IDENTIFIER(trim(:prefix) || 'funders_compat') AS" in seed_pub


def test_prefix_guards():
    g = "SELECT assert_true((trim(:prefix)='openalex.funders.' AND trim(:allow_prod)=trim(:prefix)) OR\n (trim(:prefix) RLIKE '^openalex_dev[.]rohan_lab[.]ff1013_[A-Za-z0-9_]*_$' AND trim(:allow_prod)=''),'unapproved registry prefix'),"
    assert g in CELLS[0]
    # the seed never runs on prod: the prod registry was created by the switch packet
    assert "SELECT assert_true(trim(:prefix) RLIKE '^openalex_dev[.]rohan_lab[.]ff1013_[A-Za-z0-9_]*_$' AND trim(:allow_prod)=''," in SEED[0]
    assert not re.search(r'\btrue AND\b|\bAND true\b', '\n'.join(SEED))


def test_seed_checks_are_the_daily_checks_without_never_deleted():
    daily_chk, seed_chk = CELLS[cell('-- checks\n')], next(s for s in SEED if s.startswith('%sql\n-- checks'))
    never = ("  assert_true((SELECT count(*) FROM (SELECT funder_id FROM registry_prev_funder EXCEPT SELECT funder_id FROM IDENTIFIER(trim(:prefix) || 'funder'))) = 0,\n"
             "    'an issued F id disappeared') c_never_deleted,\n")
    assert never in daily_chk
    assert daily_chk[daily_chk.index('\nSELECT\n'):].replace(never, '') == seed_chk[seed_chk.index('\nSELECT\n'):]
    state = next(s for s in SEED if '-- seed_state:' in s)
    assert SEED.index(state) < min(i for i, s in enumerate(SEED) if 'INSERT INTO' in s)


def test_log_decisions_is_retry_safe():
    s = CELLS[cell('-- log_decisions')]
    assert 'WHERE NOT EXISTS' in s and 'd.run_id = n.run_id AND d.source = n.source AND d.source_key = n.source_key AND d.action = n.action' in s


def test_open_items_log_once_by_content():
    # unchanged held successors and open observations are logged once, not every run (keyed on content, not run_id)
    promote, observe = CELLS[cell('-- promote_stage:')], CELLS[cell('-- observe_open_items')]
    assert "ON d.source='ror_successor' AND d.source_key=concat(p.predecessor_rid,':',p.funder_id) AND d.action=p.action;" in promote
    assert "d.source='observation' AND d.source_key=c.source_key AND d.action=c.action;" in observe
    assert "d.run_id=('fd'" not in promote and "d.run_id=('fd'" not in observe


def test_propagation_keeps_backup_remap_and_residue_per_target():
    names = [s.split('\n')[1] for s in CELLS if s.startswith('%sql\n-- propagation_')]
    targets = [n[len('-- propagation_backup_'):] for n in names if n.startswith('-- propagation_backup_')]
    assert len(targets) == 11
    for t in targets:
        assert f'-- propagation_after_{t}' in names, t
        assert any(n in names for n in (f'-- propagation_remap_{t}', f'-- propagation_delete_{t}')), t


def test_seed_ranking_is_built_once():
    rank = next(s for s in SEED if '-- seed_ranking:' in s)
    assert rank.count('CREATE OR REPLACE TEMP VIEW') == 2 and 'shared_dec AS (' in rank
    fills = [s for s in SEED if 'INSERT INTO' in s]
    assert len(fills) == 4 and all('tier AS (' not in s and 'rorx AS (' not in s for s in fills)
    assert SEED.index(rank) < SEED.index(fills[0])
    assert "created_by LIKE 'mint%'" not in next(s for s in SEED if '-- create_ever_issued' in s)


def test_funders_api_roles_are_sorted():
    # funders-api-roles-sort (10-15): the content hash that drives funders_api.updated_date covers TO_JSON(roles), so the
    # roles array must not depend on COLLECT_LIST order. The daily has no copy of this build (prod API = CreateFundersAPI).
    api = ''.join(''.join(c['source']) for c in json.loads((ROOT / 'notebooks/funders/CreateFundersAPI.ipynb').read_text())['cells'])
    assert 'ARRAY_SORT(COLLECT_LIST(STRUCT(role, id, works_count))) AS roles' in api
    assert not any('-- funders_api_1' in s for s in CELLS)
