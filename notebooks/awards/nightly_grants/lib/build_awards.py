"""CreateAwards identity + projection, ported from the deployed stableid-r1-prod lib (build/deployed/lib/build_awards.py).
Only the release wrapper and dead paths are removed; see build/PORT-NOTES.md for each edit."""
from stable_award_ids import compact, generic_sql, ident, key_match, literal, norm_doi_sql, short

PUBLIC_DOI_CLASS='PUBLIC_DOI_EQUALS_TARGET_DOI'
# 10-01: a public paper-only grant (shell) retired into a public shell twin of the same funder whose number differs only in
# punctuation/case (e.g. 'HL-079457' vs 'HL079457'). Both are public, so the loser is allowed to be public.
PUBLIC_SHELL_CLASS='PUBLIC_SHELL_SAME_NUMBER'
PUBLIC_TWIN_CLASSES=(PUBLIC_DOI_CLASS,PUBLIC_SHELL_CLASS,'PUBLIC_GTR_LINEAGE_TWIN')
import uk_lineage
# a transition logged by a run that then failed before its swap (the only way a public id can already be moved in the registry)
def latest_unfinished(p):
    return f"""SELECT t.* FROM (SELECT * FROM state_transitions_v QUALIFY row_number() OVER (PARTITION BY stable_id ORDER BY recorded_at DESC)=1) t
      JOIN {p}award_nightly_runs n ON n.run_id=t.run_id AND n.status NOT LIKE 'SUCCEEDED%'"""


def tuple_sql(values):
    return '(' + ','.join(literal(v) for v in values) + ')'


def public_shell_filter(c,mm):
    """PUBLIC_SHELL_SAME_NUMBER, re-proved every night on the pending pairs: loser and target are both shells (no direct
    observation, SHELL in the plan), both served today, same canonical funder, and every observation of either carries ONE
    normalized number (letters+digits, lowercase) with >= 4 characters, a digit, and not weak for that funder; the target is
    not itself retiring by the rule. Inputs move daily (a feed can turn a shell into a funder record), so a pair that fails is
    HELD: not retired tonight, counted, re-proved next night. PUBLIC_SHELL_HELD_FUSE stops the run if more than
    max(100, 1%) of the pending pairs fail (a bad list, not drift). Returns the pending relation with held pairs removed."""
    r=c.r; cls=literal(PUBLIC_SHELL_CLASS)
    c.artifact('public_shell_pairs',f"SELECT loser_id,target_id FROM {mm} WHERE evidence_class={cls}")
    c.artifact('public_shell_members',f"""SELECT loser_id,loser_id member_id FROM {r}public_shell_pairs
      UNION SELECT loser_id,target_id FROM {r}public_shell_pairs""")
    c.artifact('direct_ids',f"SELECT DISTINCT stable_id FROM {r}resolved_observations WHERE is_direct")
    c.artifact('public_shell_numbers',f"""SELECT x.loser_id,count(DISTINCT o.nk) keys,min(o.nk) nk,bool_or(coalesce(n.is_weak,true)) any_weak
      FROM {r}public_shell_members x JOIN {r}resolved_observations o ON o.stable_id=x.member_id
      LEFT JOIN {r}normalization_lookup n ON n.funder_id<=>o.funder_id AND n.award_key<=>o.award_key AND n.is_direct=o.is_direct
      GROUP BY x.loser_id""")
    c.artifact('public_shell_held',f"""SELECT DISTINCT loser_id,reason FROM (
        SELECT x.loser_id,'NOT_BOTH_SHELL' reason FROM {r}public_shell_members x LEFT JOIN {r}ownership_plan o ON o.stable_id=x.member_id
          LEFT JOIN {r}direct_ids d ON d.stable_id=x.member_id WHERE NOT COALESCE(o.entity_kind='SHELL',false) OR d.stable_id IS NOT NULL
        UNION ALL SELECT x.loser_id,'NOT_SERVED' FROM {r}public_shell_members x LEFT ANTI JOIN awards_v a ON a.id=x.member_id
        UNION ALL SELECT m.loser_id,'NUMBER' FROM {r}public_shell_pairs m LEFT JOIN {r}public_shell_numbers n USING(loser_id)
          WHERE NOT COALESCE(n.keys=1 AND length(n.nk)>=4 AND n.nk RLIKE '[0-9]' AND NOT n.any_weak,false)
        UNION ALL SELECT m.loser_id,'FUNDER' FROM {r}public_shell_pairs m LEFT JOIN {r}ownership_plan a ON a.stable_id=m.loser_id
          LEFT JOIN {r}ownership_plan b ON b.stable_id=m.target_id WHERE a.funder_id IS NULL OR NOT(a.funder_id<=>b.funder_id)
        UNION ALL SELECT m.loser_id,'TARGET_RETIRING' FROM {r}public_shell_pairs m JOIN {r}retirements_rule t ON t.old_id=m.target_id)""")
    held=c.count('public_shell_held',r+'public_shell_held'); pairs=c.count('public_shell_pairs',r+'public_shell_pairs')
    for row in c.sql(f"SELECT reason,count(DISTINCT loser_id) n FROM {r}public_shell_held GROUP BY 1").collect():
        c.counts['public_shell_held_'+row.reason.lower()]=int(row.n)
    c.zero('PUBLIC_SHELL_HELD_FUSE',f"SELECT count(DISTINCT loser_id) n FROM {r}public_shell_held HAVING n>greatest(100,0.01*{pairs})")
    return c.artifact('migration_proven',f"SELECT m.* FROM {mm} m LEFT ANTI JOIN {r}public_shell_held h ON h.loser_id=m.loser_id")


def prepare(c):
    p,r = c.p,c.r
    c.artifact('entities_before',f'SELECT * FROM {p}award_entities')
    c.artifact('keys_before',f'SELECT * FROM {p}award_keys')
    c.zero('UNIQUE_ENTITY',f'SELECT stable_id FROM {r}entities_before GROUP BY stable_id HAVING count(*)<>1')
    c.zero('UNIQUE_BIRTH',f'SELECT birth_key FROM {r}entities_before GROUP BY birth_key HAVING count(*)<>1')
    keycols='key_type,staging_id,funder_id,award_key,namespace,source_record_id'
    c.zero('UNIQUE_KEY',f'SELECT {keycols} FROM {r}keys_before GROUP BY {keycols} HAVING count(*)<>1')
    c.zero('KEY_OWNER_EXISTS',f'SELECT k.* FROM {r}keys_before k LEFT ANTI JOIN {r}entities_before e ON e.stable_id=k.stable_id')
    c.graph(r+'entities_before','resolve_before')
    c.zero('UNIQUE_FUNDER', 'SELECT funder_id FROM funders_v GROUP BY funder_id HAVING count(*)<>1')
    c.artifact('funder_nodes',"SELECT funder_id stable_id,merge_into_id redirect_to,CASE WHEN merge_into_id IS NULL THEN 'ACTIVE' ELSE 'REDIRECTED' END status FROM funders_v")
    c.graph(r+'funder_nodes','funder_resolve')
    c.artifact('funder_canon',f'SELECT owner_id funder_id,stable_id canonical_funder_id FROM {r}funder_resolve')
    crdoi=norm_doi_sql('record_doi'); crurl=norm_doi_sql('record_url'); dcdoi=norm_doi_sql('record_id')
    c.zero('CR_RECORD_LINEAGE',f"SELECT * FROM crossref_records_v WHERE record_type <> 'grant' OR {crdoi} IS NULL OR NOT ({crdoi} <=> {crurl})")
    c.zero('DC_RECORD_LINEAGE',f"SELECT * FROM datacite_records_v WHERE resource_type <> 'Award' OR {dcdoi} IS NULL")
    c.artifact('cr_records',f'SELECT DISTINCT {crdoi} record_doi FROM crossref_records_v')
    c.artifact('dc_records',f'SELECT DISTINCT {dcdoi} record_doi FROM datacite_records_v')
    for ns,prov,producer,record in [('crossref_grant','crossref_work','crossref', 'cr'),('datacite_award','datacite','datacite','dc')]:
        ndoi=norm_doi_sql('x.doi')
        c.zero(ns+'_PRODUCER_LINEAGE',f"SELECT x.* FROM {producer}_v x LEFT ANTI JOIN {r}{record}_records s ON {ndoi}=s.record_doi")
        c.zero(ns+'_RAW_LINEAGE',f"SELECT x.* FROM raw_v x LEFT ANTI JOIN {producer}_v y ON x.id=y.id AND x.funder_id <=> y.funder_id AND lower(x.funder_award_id) <=> lower(y.funder_award_id) AND {ndoi}={norm_doi_sql('y.doi')} WHERE x.provenance='{prov}' AND x.priority=1")
    rawdoi=norm_doi_sql('x.doi')
    c.artifact('raw_observations',f"""WITH classified AS (
      SELECT x.*,
        CASE WHEN x.provenance='crossref_work' AND x.priority=1 THEN 'crossref_grant'
             WHEN x.provenance='datacite' AND x.priority=1 THEN 'datacite_award' END namespace,
        CASE WHEN x.priority=1 AND x.provenance IN ('crossref_work','datacite') THEN {rawdoi} END source_record_id,
        struct(x.*) payload FROM raw_v x)
      SELECT to_json(named_struct('adapter','raw','staging_id',id,'funder_id',funder_id,
        'award_key',lower(funder_award_id),'namespace',namespace,'native_id',source_record_id),map('ignoreNullFields','false')) observation_key,
        id staging_id,funder_id source_funder_id,lower(funder_award_id) award_key,namespace,source_record_id,
        (namespace IS NOT NULL OR priority>=3) is_direct,priority,payload,
        coalesce(namespace,'raw') family FROM classified""")
    raw_schema=c.spark.table('raw_v').schema.simpleString()
    # FROM_JSON fills absent GTR metadata with typed nulls. No fabricated payload.
    c.artifact('gtr_observations',f"""SELECT
      to_json(named_struct('adapter','gtr','native_id',cast(id AS STRING),
        'staging_id',abs(xxhash64(id)) % 9000000000,'funder_id',funder_id,
        'award_key',lower(funder_award_id)),map('ignoreNullFields','false')) observation_key,
      abs(xxhash64(id)) % 9000000000 staging_id,funder_id source_funder_id,
      lower(funder_award_id) award_key,'gtr' namespace,cast(id AS STRING) source_record_id,
      true is_direct,-1 priority,
      from_json(to_json(named_struct('id',abs(xxhash64(id)) % 9000000000,
        'funder_id',funder_id,'funder_award_id',funder_award_id,'funder',funder,
        'provenance','gtr_legacy','created_date',created_date)),{literal(raw_schema)}) payload,
      'gtr' family FROM gtr_v WHERE work_id IS NOT NULL""")
    c.artifact('observations',f"""SELECT * FROM (
      SELECT * FROM {r}raw_observations UNION ALL SELECT * FROM {r}gtr_observations)
      QUALIFY row_number() OVER (PARTITION BY observation_key ORDER BY is_direct DESC,priority DESC,
        payload.updated_date DESC NULLS LAST,to_json(payload))=1""")
    c.zero('INVALID_OBSERVATION',f"SELECT * FROM {r}observations WHERE staging_id IS NULL OR is_direct IS NULL OR (namespace IS NOT NULL AND source_record_id IS NULL)")
    c.zero('MISSING_FUNDER',f'SELECT o.* FROM {r}observations o LEFT ANTI JOIN {r}funder_canon f ON f.funder_id=o.source_funder_id WHERE o.source_funder_id IS NOT NULL')
    c.family_receipts()   # per-family completeness vs last successful run (replaces hard-coded CaptureD0 receipts)
    c.zero('SOURCE_RECEIPTS_COMPLETE',"SELECT * FROM receipts_v WHERE success IS NOT TRUE OR parser_ok IS NOT TRUE OR coverage_complete IS NOT TRUE")
    c.zero('RECEIPT_FAMILY_UNIQUE','SELECT family FROM receipts_v GROUP BY family HAVING COUNT(*)<>1')
    c.zero('ADAPTER_RECEIPT_MISSING',f'SELECT DISTINCT coalesce(o.payload.provenance,o.family) FROM {r}observations o LEFT ANTI JOIN receipts_v s ON coalesce(o.payload.provenance,o.family)=s.family')
    c.artifact('lookup',f"""SELECT o.*,kn.stable_id native_original_owner,ks.stable_id staging_original_owner,
      rn.stable_id native_hit,rs.stable_id staging_hit,coalesce(rn.stable_id,rs.stable_id) existing_id
      FROM {r}observations o LEFT JOIN {r}keys_before kn ON kn.key_type='NATIVE'
        AND kn.namespace=o.namespace AND kn.source_record_id=o.source_record_id
      LEFT JOIN {r}keys_before ks ON ks.key_type='STAGING' AND ks.staging_id=o.staging_id
        AND ks.funder_id <=> o.source_funder_id AND ks.award_key <=> o.award_key
      LEFT JOIN {r}resolve_before rn ON rn.owner_id=kn.stable_id
      LEFT JOIN {r}resolve_before rs ON rs.owner_id=ks.stable_id""")
    c.zero('NATIVE_STAGING_DISAGREE',f'SELECT * FROM {r}lookup WHERE native_hit IS NOT NULL AND staging_hit IS NOT NULL AND native_hit<>staging_hit')
    # op14 (Q5 risk 2): a native-keyed observation whose only hit is a STAGING key owned by a REDIRECTED entity would inherit the
    # redirect's equivalence without native proof; block it.
    c.zero('NATIVE_EVIDENCE_THROUGH_REDIRECT',f'SELECT * FROM {r}lookup WHERE namespace IS NOT NULL AND native_hit IS NULL AND staging_original_owner IS NOT NULL AND staging_original_owner<>staging_hit')
    c.zero('UNRESOLVED_PRESENT_KEY',f'SELECT * FROM {r}lookup WHERE (native_original_owner IS NOT NULL AND native_hit IS NULL) OR (staging_original_owner IS NOT NULL AND staging_hit IS NULL)')
    # Current multiple triples for one native record require explicit old -> new evidence.
    c.artifact('native_multitriples',f"""SELECT namespace,source_record_id FROM {r}observations
      WHERE namespace IS NOT NULL GROUP BY namespace,source_record_id
      HAVING count(DISTINCT to_json(named_struct('s',staging_id,'f',source_funder_id,'a',award_key),map('ignoreNullFields','false')))>1""")
    c.zero('NATIVE_MULTITRIPLE_UNPROVEN',f"""SELECT o.* FROM {r}observations o JOIN {r}native_multitriples n
      USING(namespace,source_record_id) LEFT ANTI JOIN continuity_v m
      ON m.new_observation_key=o.observation_key AND m.namespace=o.namespace AND m.source_record_id=o.source_record_id
      AND m.producer_code_sha IS NOT NULL AND m.evidence_uri IS NOT NULL""")


def allocate(c):
    p,r=c.p,c.r
    c.artifact('observation_keys',f"""SELECT observation_key,'STAGING' key_type,
      to_json(named_struct('v',2,'type','STAGING','staging_id',staging_id,'funder_id',source_funder_id,'award_key',award_key),map('ignoreNullFields','false')) key_json
      FROM {r}observations UNION ALL SELECT observation_key,'NATIVE',
      to_json(named_struct('v',2,'type','NATIVE','namespace',namespace,'source_record_id',source_record_id),map('ignoreNullFields','false'))
      FROM {r}observations WHERE namespace IS NOT NULL""")
    labels=c.artifact('labels_0',f'SELECT observation_key,observation_key component_key FROM {r}observations')
    for i in range(100):
        new=c.artifact(f'labels_{i+1}',f"""WITH minima AS (
          SELECT k.key_json,min(l.component_key) component_key FROM {r}observation_keys k
          JOIN {labels} l USING(observation_key) GROUP BY k.key_json)
          SELECT k.observation_key,min(m.component_key) component_key
          FROM {r}observation_keys k JOIN minima m USING(key_json) GROUP BY k.observation_key""")
        changes=c.sql(f'SELECT 1 FROM {new} n JOIN {labels} p USING(observation_key) WHERE n.component_key<>p.component_key LIMIT 1').collect()
        labels=new
        if not changes: break
    else: raise RuntimeError('COMPONENT_DEPTH_FUSE')
    c.artifact('component_members',f'SELECT * FROM {labels}')
    c.zero('COMPONENT_OWNER_CONFLICT',f'SELECT component_key FROM {r}component_members m JOIN {r}lookup l USING(observation_key) GROUP BY component_key HAVING count(DISTINCT existing_id)>1')
    c.zero('COMPONENT_FUNDER_CONFLICT',f"""SELECT component_key FROM {r}component_members m JOIN {r}observations o USING(observation_key)
      LEFT JOIN {r}funder_canon f ON o.source_funder_id=f.funder_id GROUP BY component_key
      HAVING count(DISTINCT to_json(named_struct('f',f.canonical_funder_id),map('ignoreNullFields','false')))>1""")
    c.artifact('components',f"""WITH roots AS (
      SELECT m.component_key,coalesce(min(CASE WHEN k.key_type='NATIVE' THEN k.key_json END),min(k.key_json)) birth_key
      FROM {r}component_members m JOIN {r}observation_keys k USING(observation_key) GROUP BY m.component_key),
    info AS (SELECT m.component_key,max(l.existing_id) existing_id,max(f.canonical_funder_id) funder_id,
      min(l.award_key) award_key,min(l.payload.doi) doi,
      CASE WHEN bool_or(l.is_direct) THEN 'DIRECT' ELSE 'SHELL' END entity_kind,
      bool_or(e.staging_id IS NOT NULL AND NOT (e.funder_id <=> l.source_funder_id AND e.award_key <=> l.award_key)) collision
      FROM {r}component_members m JOIN {r}lookup l USING(observation_key)
      LEFT JOIN {r}funder_canon f ON f.funder_id=l.source_funder_id
      LEFT JOIN {r}keys_before e ON e.key_type='STAGING' AND e.staging_id=l.staging_id
      GROUP BY m.component_key)
    SELECT roots.*,info.* EXCEPT(component_key),CASE WHEN collision THEN 'COLLISION_RECOVERY' ELSE 'NEW' END origin
    FROM roots JOIN info USING(component_key)""")
    c.artifact('allocations',f"""SELECT DISTINCT birth_key,sha2(birth_key,256) birth_sha256,
      funder_id,award_key,doi,entity_kind,origin FROM {r}components WHERE existing_id IS NULL""")
    c.zero('ALLOCATION_FUSE',f'SELECT count(*) n FROM {r}allocations a LEFT ANTI JOIN {p}award_entities e ON e.birth_key=a.birth_key HAVING n>500000')
    c.write(f"""MERGE INTO {p}award_entities e USING {r}allocations a ON e.birth_key=a.birth_key
      WHEN NOT MATCHED THEN INSERT (birth_key,birth_sha256,funder_id,award_key,doi,entity_kind,origin,status,redirect_to,first_seen,created_run,updated_at)
      VALUES(a.birth_key,a.birth_sha256,a.funder_id,a.award_key,a.doi,a.entity_kind,a.origin,'ACTIVE',NULL,CAST(:ts AS TIMESTAMP),:rid,CAST(:ts AS TIMESTAMP))""")
    c.zero('ALLOCATION_BOUNDARY',f'SELECT e.* FROM {p}award_entities e JOIN {r}allocations a USING(birth_key) WHERE e.stable_id<9000000000')
    c.zero('UNIQUE_ALLOCATED_BIRTH',f'SELECT birth_key FROM {p}award_entities GROUP BY birth_key HAVING count(*)<>1')
    c.zero('UNIQUE_ALLOCATED_ID',f'SELECT stable_id FROM {p}award_entities GROUP BY stable_id HAVING count(*)<>1')
    c.artifact('component_ids',f'SELECT c.component_key,coalesce(c.existing_id,e.stable_id) stable_id FROM {r}components c LEFT JOIN {p}award_entities e ON c.birth_key=e.birth_key')
    c.artifact('bindings',f'SELECT o.*,i.stable_id FROM {r}observations o JOIN {r}component_members m USING(observation_key) JOIN {r}component_ids i USING(component_key)')
    c.zero('ALL_OBSERVATIONS_BOUND',f'SELECT observation_key FROM {r}observations EXCEPT SELECT observation_key FROM {r}bindings WHERE stable_id IS NOT NULL')
    c.zero('ONE_BINDING_PER_OBSERVATION',f'SELECT observation_key FROM {r}bindings GROUP BY observation_key HAVING count(*)<>1')
    c.artifact('new_keys',f"""SELECT DISTINCT 'STAGING' key_type,staging_id,source_funder_id funder_id,
      award_key,CAST(NULL AS STRING) namespace,CAST(NULL AS STRING) source_record_id,stable_id FROM {r}bindings
      UNION ALL SELECT DISTINCT 'NATIVE',CAST(NULL AS BIGINT),CAST(NULL AS BIGINT),CAST(NULL AS STRING),namespace,source_record_id,stable_id
      FROM {r}bindings WHERE namespace IS NOT NULL""")
    c.zero('NEW_KEYS_UNIQUE_OWNER',f'SELECT key_type,staging_id,funder_id,award_key,namespace,source_record_id FROM {r}new_keys GROUP BY ALL HAVING count(DISTINCT stable_id)<>1')
    c.zero('PERMANENT_OWNER_CONFLICT',f'SELECT n.* FROM {r}new_keys n JOIN {r}keys_before k ON {key_match("n","k")} JOIN {r}resolve_before b ON b.owner_id=k.stable_id WHERE n.stable_id<>b.stable_id')
    c.write(f"""MERGE INTO {p}award_keys k USING {r}new_keys n ON {key_match('k','n')}
      WHEN NOT MATCHED THEN INSERT (key_type,staging_id,funder_id,award_key,namespace,source_record_id,stable_id,kind,created_run,created_at)
      VALUES(n.key_type,n.staging_id,n.funder_id,n.award_key,n.namespace,n.source_record_id,n.stable_id,'OBSERVATION',:rid,CAST(:ts AS TIMESTAMP))""")
    c.zero('KEY_OWNER_UNCHANGED',f'SELECT k.* FROM {r}keys_before k LEFT ANTI JOIN {p}award_keys n ON {key_match("k","n")} AND k.stable_id=n.stable_id')
    c.artifact('entities_allocated',f'SELECT * FROM {p}award_entities')


def plan(c):
    p,r=c.p,c.r
    c.artifact('ownership_plan',f"""SELECT b.stable_id,min(f.canonical_funder_id) funder_id,
      CASE WHEN bool_or(b.is_direct) THEN 'DIRECT' ELSE max(e.entity_kind) END entity_kind
      FROM {r}bindings b JOIN {r}entities_allocated e USING(stable_id)
      LEFT JOIN {r}funder_canon f ON f.funder_id=b.source_funder_id GROUP BY b.stable_id""")
    c.zero('ENTITY_FUNDER_AMBIGUOUS',f"""SELECT b.stable_id FROM {r}bindings b LEFT JOIN {r}funder_canon f
      ON f.funder_id=b.source_funder_id GROUP BY b.stable_id HAVING count(DISTINCT to_json(named_struct('f',f.canonical_funder_id),map('ignoreNullFields','false')))>1""")
    # A native hit proves the current observation's continuity. Other direct changes need row proof.
    c.artifact('ownership_changes',f"""SELECT b.observation_key,b.stable_id,e.funder_id old_funder_id,f.canonical_funder_id new_funder_id,
      b.namespace,b.source_record_id,(b.is_direct OR e.entity_kind='DIRECT') is_direct,l.native_hit
      FROM {r}bindings b JOIN {r}entities_before e USING(stable_id) JOIN {r}lookup l USING(observation_key)
      LEFT JOIN {r}funder_canon f ON f.funder_id=b.source_funder_id WHERE NOT (e.funder_id <=> f.canonical_funder_id)""")
    c.zero('DIRECT_CORRECTION_UNPROVEN',f"""SELECT o.* FROM {r}ownership_changes o
      LEFT JOIN {r}funder_canon fc ON fc.funder_id=o.old_funder_id
      LEFT ANTI JOIN continuity_v m ON m.new_observation_key=o.observation_key AND m.stable_id=o.stable_id
      AND m.old_funder_id <=> o.old_funder_id AND m.new_funder_id <=> o.new_funder_id
      AND m.old_observation_key IS NOT NULL AND m.evidence_uri IS NOT NULL AND m.producer_code_sha IS NOT NULL
      WHERE o.is_direct AND o.native_hit IS NULL AND NOT(fc.canonical_funder_id <=> o.new_funder_id)""")
    # D0 normalization is a pinned lookup computed from captured function definitions.
    lookup = 'SELECT DISTINCT canonical_funder_id AS funder_id, award_key, is_direct, sharp_key, sharp_eligible, is_weak FROM normalization_v'
    if True:   # normalization_work is always derived by the runtime
        lookup += ' UNION SELECT funder_id,award_key,is_direct,sharp_key,sharp_eligible,is_weak FROM normalization_work_v'
    c.artifact('normalization_lookup',lookup)
    c.zero('NORMALIZATION_UNIQUE',f'SELECT funder_id,award_key,is_direct FROM {r}normalization_lookup GROUP BY ALL HAVING count(*)<>1')
    c.zero('NORMALIZATION_COVERAGE',f"""SELECT b.observation_key FROM {r}bindings b JOIN {r}ownership_plan e USING(stable_id)
      LEFT ANTI JOIN {r}normalization_lookup n ON n.funder_id <=> e.funder_id AND n.award_key <=> b.award_key AND n.is_direct=b.is_direct""")
    # op16: NARROW. The wide raw `payload` struct must not travel through these joins (17.8M rows x 24 nested columns hung d01/d01m
    # and exceeded 10 min even on the SQL warehouse). Payload is fetched once, by observation_key, only for elected metadata rows.
    # op21 (F21): TWO STEPS. When one statement joined bindings, ownership_plan and normalization_lookup, the optimizer paired
    # ownership_plan with normalization_lookup on funder_id ALONE (the only key those two share) before touching bindings: a per-funder
    # cross product (d01m2: 4.5 TB spilled, 41 CPU-hours in 38 min, cancelled). Materializing bindings x ownership_plan first leaves the
    # lookup join a single two-table equi-join on the full (funder_id, award_key, is_direct) key. Rule: a join predicate must never
    # reference two different earlier relations; materialize the pair instead.
    c.artifact('bound_observations',f"""SELECT b.observation_key,b.stable_id,b.staging_id,b.source_funder_id,b.award_key,b.namespace,
      b.source_record_id,b.is_direct,b.priority,b.family,e.funder_id
      FROM (SELECT observation_key,stable_id,staging_id,source_funder_id,award_key,namespace,source_record_id,is_direct,priority,family
              FROM {r}bindings) b JOIN {r}ownership_plan e USING(stable_id)""")
    c.zero('BOUND_OBSERVATIONS_COMPLETE',f'SELECT b.observation_key FROM {r}bindings b LEFT ANTI JOIN {r}bound_observations o USING(observation_key)')
    c.artifact('resolved_observations',f"""SELECT b.observation_key,b.stable_id,b.staging_id,b.source_funder_id,b.award_key,b.namespace,
      b.source_record_id,b.is_direct,b.priority,b.family,b.funder_id,{generic_sql('b.award_key')} nk,
      CASE WHEN n.sharp_eligible AND (b.is_direct OR NOT n.is_weak) THEN n.sharp_key END sk
      FROM {r}bound_observations b
      JOIN {r}normalization_lookup n ON n.funder_id <=> b.funder_id AND n.award_key <=> b.award_key AND n.is_direct=b.is_direct""")
    c.artifact('shell_targets',f"""WITH direct AS (SELECT DISTINCT stable_id,funder_id,nk,sk FROM {r}resolved_observations WHERE is_direct),
      shells AS (SELECT * FROM {r}resolved_observations WHERE NOT is_direct),
      generic AS (SELECT s.observation_key,count(DISTINCT d.stable_id) n,min(d.stable_id) target_id FROM shells s
        JOIN direct d ON s.funder_id=d.funder_id AND s.nk=d.nk WHERE s.nk IS NOT NULL GROUP BY s.observation_key),
      sharp AS (SELECT s.observation_key,count(DISTINCT d.stable_id) n,min(d.stable_id) target_id FROM shells s
        JOIN direct d ON s.funder_id=d.funder_id AND s.sk=d.sk WHERE s.sk IS NOT NULL GROUP BY s.observation_key)
      SELECT s.observation_key,s.stable_id shell_id,
        CASE WHEN coalesce(g.n,0)=1 THEN g.target_id WHEN coalesce(g.n,0)=0 AND coalesce(h.n,0)=1 THEN h.target_id END proposed_target,
        coalesce(g.n,0) generic_targets,coalesce(h.n,0) sharp_targets
      FROM shells s LEFT JOIN generic g USING(observation_key) LEFT JOIN sharp h USING(observation_key)""")
    c.artifact('retirements_rule',f"""SELECT s.shell_id old_id,min(s.proposed_target) canonical_id
      FROM {r}shell_targets s JOIN {r}entities_allocated e ON e.stable_id=s.shell_id
      LEFT ANTI JOIN {r}resolved_observations d ON d.stable_id=s.shell_id AND d.is_direct
      WHERE e.entity_kind='SHELL' AND e.status<>'REDIRECTED'
      GROUP BY s.shell_id HAVING count(*)=count(s.proposed_target) AND count(DISTINCT s.proposed_target)=1 AND min(s.proposed_target)<>s.shell_id""")
    # op14: manifest-driven retirements (migration release). A frozen, pinned manifest of proven-duplicate pairs is applied through
    # the SAME retirement path (journaled REDIRECTED transitions; keys never rebound; losers must be ACTIVE entities that were never
    # public, i.e. allocated >= 9e9). Rule retirements and manifest retirements are unioned into `retirements`.
    if c.has_extra('migration_manifest'):
        mm='migration_manifest_v'
        c.zero('MIGRATION_FIELDS_NOT_NULL',f'SELECT * FROM {mm} WHERE loser_id IS NULL OR target_id IS NULL OR evidence_class IS NULL OR loser_id=target_id')
        c.zero('MIGRATION_UNIQUE',f'SELECT loser_id FROM {mm} GROUP BY loser_id HAVING count(DISTINCT target_id)<>1')
        c.zero('MIGRATION_ACYCLIC',f'SELECT m.* FROM {mm} m JOIN {mm} t ON t.loser_id=m.target_id')
        # op24: three proof classes. EXACT_FUNDER_NUMBER_SINGLE_TARGET_AGREEING: shell loser -> DIRECT target (original rule).
        # DOI_EQUALS_TARGET_DOI: native-born (crossref_grant) loser whose every NATIVE key is the DOI the seed target already carries.
        # SHELL_TO_SERVED_SHELL_SAME_NUMBER: shell loser -> target that is served today (pinned awards) but has no direct observation.
        DOI_CLASS,SHELL_CLASS='DOI_EQUALS_TARGET_DOI','SHELL_TO_SERVED_SHELL_SAME_NUMBER'
        doi_classes=f"({literal(DOI_CLASS)},{literal(PUBLIC_DOI_CLASS)})"
        c.zero('MIGRATION_EVIDENCE_CLASS',f"SELECT * FROM {mm} WHERE NOT COALESCE(evidence_class IN ('EXACT_FUNDER_NUMBER_SINGLE_TARGET_AGREEING',{literal(DOI_CLASS)},{literal(SHELL_CLASS)}) OR evidence_class IN {tuple_sql(PUBLIC_TWIN_CLASSES)},false)")
        # op23: the manifest is idempotent. A pair whose loser is ALREADY REDIRECTED (by an earlier release, accepted or dev-abandoned after
        # APPLYING) is 'applied': it must already resolve to the manifest target (else MIGRATION_APPLIED_MISMATCH) and is excluded from the
        # loser/target gates and from this release's retirements. Only 'pending' pairs are retired here.
        c.artifact('migration_applied',f"SELECT m.*,e.redirect_to FROM {mm} m JOIN {r}entities_before e ON e.stable_id=m.loser_id WHERE e.status='REDIRECTED'")
        c.zero('MIGRATION_APPLIED_MISMATCH',f"SELECT a.* FROM {r}migration_applied a LEFT ANTI JOIN {r}resolve_before x ON x.owner_id=a.loser_id AND x.stable_id=a.target_id AND x.status='ACTIVE'")
        c.artifact('migration_pending',f"SELECT m.* FROM {mm} m LEFT ANTI JOIN {r}migration_applied a ON a.loser_id=m.loser_id")
        mm=f'{r}migration_pending'
        mm=public_shell_filter(c,mm)   # 10-01: held pairs never reach the retirement gates
        mm=uk_lineage.filter(c,mm)
        # loser: allocated by a registry release (origin NEW, id >= 9e9), never published, an ACTIVE entity, bound in this release, no NATIVE key
        c.zero('MIGRATION_LOSER_NEVER_PUBLIC',f"""SELECT m.* FROM {mm} m LEFT JOIN {r}entities_allocated e ON e.stable_id=m.loser_id
          WHERE m.evidence_class NOT IN {tuple_sql(PUBLIC_TWIN_CLASSES)} AND (e.stable_id IS NULL OR m.loser_id<9000000000 OR NOT COALESCE(e.origin='NEW',false) OR e.first_published_release IS NOT NULL OR e.first_published_at IS NOT NULL)""")
        c.zero('MIGRATION_LOSER_ACTIVE_ENTITY',f"SELECT m.* FROM {mm} m LEFT ANTI JOIN {r}entities_allocated e ON e.stable_id=m.loser_id AND e.status='ACTIVE'")
        c.zero('MIGRATION_LOSER_BOUND',f"SELECT m.* FROM {mm} m LEFT ANTI JOIN {r}bindings b ON b.stable_id=m.loser_id")
        c.zero('MIGRATION_LOSER_NO_NATIVE',f"SELECT m.* FROM {mm} m JOIN {p}award_keys k ON k.stable_id=m.loser_id AND k.key_type='NATIVE' WHERE m.evidence_class NOT IN ({literal(DOI_CLASS)},{literal(PUBLIC_DOI_CLASS)},{literal(uk_lineage.LINEAGE_TWIN_CLASS)})")
        # DOI class proof: loser born NATIVE in crossref_grant, origin NEW; target carries a doi; EVERY native key of the loser normalizes to that doi.
        c.zero('MIGRATION_DOI_PROOF',f"""SELECT m.* FROM {mm} m JOIN {r}entities_allocated l ON l.stable_id=m.loser_id JOIN {r}entities_allocated t ON t.stable_id=m.target_id
          WHERE m.evidence_class IN {doi_classes} AND NOT COALESCE((l.origin='NEW' OR (m.evidence_class={literal(PUBLIC_DOI_CLASS)} AND l.origin='COLLISION_RECOVERY')) AND l.birth_key:type='NATIVE' AND l.birth_key:namespace='crossref_grant' AND t.doi IS NOT NULL
            AND {norm_doi_sql('l.birth_key:source_record_id')}={norm_doi_sql('t.doi')},false)""")
        c.zero('MIGRATION_DOI_KEYS',f"""SELECT m.loser_id,k.namespace,k.source_record_id FROM {mm} m JOIN {p}award_keys k ON k.stable_id=m.loser_id AND k.key_type='NATIVE'
          JOIN {r}entities_allocated t ON t.stable_id=m.target_id WHERE m.evidence_class IN {doi_classes}
            AND NOT COALESCE(k.namespace='crossref_grant' AND {norm_doi_sql('k.source_record_id')}={norm_doi_sql('t.doi')},false)""")
        c.zero('MIGRATION_PUBLIC_DOI_ONE_NATIVE',f"""SELECT m.* FROM {mm} m WHERE m.evidence_class={literal(PUBLIC_DOI_CLASS)}
          AND (SELECT count(*) FROM {p}award_keys k WHERE k.stable_id=m.loser_id AND k.key_type='NATIVE')<>1""")
        c.zero('MIGRATION_PUBLIC_DOI_NO_PAPER_LINKS',f"""SELECT m.* FROM {mm} m JOIN {c.outputs['work_awards']} w
          ON w.award.id=concat('https://openalex.org/G',m.loser_id) WHERE m.evidence_class={literal(PUBLIC_DOI_CLASS)}""")
        # target: independently ACTIVE, bound, with a direct observation, same canonical funder (both sides must be present in the plan)
        c.zero('MIGRATION_TARGET_ACTIVE',f"SELECT m.* FROM {mm} m LEFT ANTI JOIN {r}entities_allocated e ON e.stable_id=m.target_id AND e.status='ACTIVE'")
        c.zero('MIGRATION_TARGET_BOUND',f"SELECT m.* FROM {mm} m LEFT ANTI JOIN {r}bindings b ON b.stable_id=m.target_id")
        c.zero('MIGRATION_TARGET_DIRECT',f"SELECT m.* FROM {mm} m LEFT ANTI JOIN {r}resolved_observations b ON b.stable_id=m.target_id AND b.is_direct WHERE m.evidence_class NOT IN ({literal(SHELL_CLASS)},{literal(PUBLIC_SHELL_CLASS)})")
        # shell-class proof: loser is a SHELL, target is served today (pinned awards relation) and is not retired by the rule in this release.
        c.zero('MIGRATION_SHELL_TARGET_SERVED',f"""SELECT m.* FROM {mm} m LEFT JOIN awards_v a ON a.id=m.target_id LEFT JOIN {r}ownership_plan o ON o.stable_id=m.loser_id
          WHERE m.evidence_class={literal(SHELL_CLASS)} AND NOT COALESCE(a.id IS NOT NULL AND o.entity_kind='SHELL',false)""")
        c.zero('MIGRATION_SHELL_TARGET_RETIRED',f"SELECT m.* FROM {mm} m JOIN {r}retirements_rule t ON t.old_id=m.target_id WHERE m.evidence_class={literal(SHELL_CLASS)}")
        c.zero('MIGRATION_SAME_FUNDER',f"""SELECT m.* FROM {mm} m LEFT JOIN {r}ownership_plan a ON a.stable_id=m.loser_id LEFT JOIN {r}ownership_plan b ON b.stable_id=m.target_id
          WHERE a.stable_id IS NULL OR b.stable_id IS NULL OR a.funder_id IS NULL OR NOT (a.funder_id <=> b.funder_id)""")
        c.zero('MIGRATION_CONFLICT_WITH_RULE',f"SELECT m.* FROM {mm} m JOIN {r}retirements_rule t ON t.old_id=m.loser_id WHERE t.canonical_id<>m.target_id")
        c.artifact('retirements_manifest',f"SELECT DISTINCT loser_id old_id,target_id canonical_id,evidence_class FROM {mm}")
        # provenance kept separately; the operational set is deduplicated on (old_id, canonical_id)
        c.artifact('retirement_sources',f"SELECT old_id,canonical_id,'RULE' source FROM {r}retirements_rule UNION ALL SELECT old_id,canonical_id,'MANIFEST' FROM {r}retirements_manifest")
        c.artifact('retirements',f"SELECT DISTINCT old_id,canonical_id FROM {r}retirement_sources")
    else:
        c.artifact('retirement_sources',f"SELECT old_id,canonical_id,'RULE' source FROM {r}retirements_rule")
        c.artifact('retirements',f"SELECT DISTINCT old_id,canonical_id FROM {r}retirement_sources")
    # composition: no retirement may target another retiring entity (bindings_final applies exactly one hop)
    c.zero('RETIREMENT_CHAIN',f'SELECT r.* FROM {r}retirements r JOIN {r}retirements t ON t.old_id=r.canonical_id')
    c.zero('RETIREMENT_UNIQUE',f'SELECT old_id FROM {r}retirements GROUP BY old_id HAVING count(DISTINCT canonical_id)<>1')
    c.zero('RETIREMENT_SAME_FUNDER',f'SELECT t.* FROM {r}retirements t JOIN {r}ownership_plan a ON a.stable_id=t.old_id JOIN {r}ownership_plan b ON b.stable_id=t.canonical_id WHERE a.funder_id IS NULL OR a.funder_id<>b.funder_id')
    # op24: a manifest pair of the shell class is the one sanctioned retirement into a non-direct (served) target.
    shell_exempt=(f" LEFT ANTI JOIN {r}retirements_manifest s ON s.old_id=t.old_id AND s.canonical_id=t.canonical_id AND s.evidence_class IN ('SHELL_TO_SERVED_SHELL_SAME_NUMBER',{literal(PUBLIC_SHELL_CLASS)})"
                  if c.has_extra('migration_manifest') else '')
    c.zero('RETIREMENT_DIRECT_TARGET',f"SELECT t.* FROM {r}retirements t LEFT ANTI JOIN {r}resolved_observations b ON b.stable_id=t.canonical_id AND b.is_direct"+shell_exempt)
    c.artifact('bindings_final',f"SELECT b.* EXCEPT(stable_id),b.stable_id original_terminal_id,coalesce(t.canonical_id,b.stable_id) stable_id FROM {r}bindings b LEFT JOIN {r}retirements t ON t.old_id=b.stable_id")
    c.artifact('projected_ids',f'SELECT DISTINCT stable_id FROM {r}bindings_final')
    # First release from seed may not withdraw: seed source_evidence lacks receipt families.
    c.artifact('withdrawal_evidence',f"""WITH previous AS (SELECT stable_id,collect_set(family) families FROM previous_bindings GROUP BY stable_id),
      receipt_check AS (SELECT b.stable_id,count(*) expected,count(s.family) covered,
        count_if(s.success AND s.parser_ok AND s.coverage_complete) complete
        FROM (SELECT DISTINCT stable_id,family FROM previous_bindings) b LEFT JOIN receipts_v s USING(family) GROUP BY b.stable_id),
      current AS (SELECT stable_id,count(*) resolving_observations FROM {r}bindings_final GROUP BY stable_id)
      SELECT p.stable_id,coalesce(x.resolving_observations,0) resolving_observations,
        coalesce(q.expected,0) evidence_families,coalesce(q.covered,0) covered_families,coalesce(q.complete,0) complete_families,
        (coalesce(x.resolving_observations,0)=0 AND q.expected>0 AND q.expected=q.covered AND q.expected=q.complete) withdrawal_proven
      FROM last_state p LEFT JOIN receipt_check q USING(stable_id) LEFT JOIN current x USING(stable_id)""")
    # op23: an id that was ACTIVE in the last accepted state but is ALREADY REDIRECTED in the registry (a dev release abandoned after
    # APPLYING, marker DEV_MOCK_UNPUBLISHED) is an INHERITED redirect: the registry transition is journaled in that release's retirements
    # artifact, so the disposition is REDIRECTED (to the resolved terminal), never RENUMBER. Unexplained inherited redirects still fail.
    c.artifact('inherited_redirects',f"""SELECT p.stable_id,x.stable_id canonical_id FROM last_state p JOIN {r}entities_before e USING(stable_id)
      JOIN {r}resolve_before x ON x.owner_id=p.stable_id WHERE p.status='ACTIVE' AND e.status='REDIRECTED' AND x.status='ACTIVE'""")
    c.zero('INHERITED_REDIRECT_TERMINAL',f"""SELECT p.stable_id FROM last_state p JOIN {r}entities_before e USING(stable_id)
      LEFT ANTI JOIN {r}inherited_redirects i USING(stable_id) WHERE p.status='ACTIVE' AND e.status='REDIRECTED'""")
    c.zero('INHERITED_REDIRECT_EXPLAINED',f"""SELECT i.stable_id FROM {r}inherited_redirects i JOIN {r}entities_before e USING(stable_id)
      LEFT ANTI JOIN ({latest_unfinished(p)}) t ON t.stable_id=i.stable_id AND t.status='REDIRECTED' AND t.redirect_to<=>e.redirect_to""")
    c.artifact('inherited_gone',f"""SELECT p.stable_id FROM last_state p JOIN {r}entities_before e USING(stable_id)
      WHERE p.status='ACTIVE' AND e.status='GONE'""")
    c.zero('INHERITED_GONE_EXPLAINED',f"SELECT g.stable_id FROM {r}inherited_gone g LEFT ANTI JOIN ({latest_unfinished(p)}) t ON t.stable_id=g.stable_id AND t.status='GONE'")
    c.artifact('dispositions',f"""SELECT p.stable_id,
      CASE WHEN n.stable_id IS NOT NULL THEN 'PRESENT' WHEN t.old_id IS NOT NULL THEN 'REDIRECTED' WHEN i.stable_id IS NOT NULL THEN 'REDIRECTED'
        WHEN w.withdrawal_proven THEN 'GONE' WHEN ig.stable_id IS NOT NULL AND coalesce(w.resolving_observations,0)=0 THEN 'GONE' ELSE 'RENUMBER' END disposition,
      coalesce(t.canonical_id,i.canonical_id) canonical_id,w.resolving_observations FROM last_state p
      LEFT JOIN {r}projected_ids n USING(stable_id) LEFT JOIN {r}retirements t ON t.old_id=p.stable_id
      LEFT JOIN {r}inherited_redirects i USING(stable_id) LEFT JOIN {r}inherited_gone ig USING(stable_id)
      LEFT JOIN {r}withdrawal_evidence w ON w.stable_id=p.stable_id WHERE p.status='ACTIVE'""")
    c.zero('REVERSE_GATE_RENUMBER',f"SELECT * FROM {r}dispositions WHERE disposition='RENUMBER'")
    c.zero('GONE_FUSE',f"SELECT count(*) n FROM {r}dispositions WHERE disposition='GONE' HAVING n>{int(c.config.get('gone_fuse',5000))}")
    c.artifact('state_changes',f"""SELECT old_id stable_id,'REDIRECTED' status,canonical_id redirect_to FROM {r}retirements
      UNION ALL SELECT stable_id,'GONE',CAST(NULL AS BIGINT) FROM {r}dispositions WHERE disposition='GONE'
      UNION ALL SELECT DISTINCT b.stable_id,'ACTIVE',CAST(NULL AS BIGINT) FROM {r}bindings_final b
        JOIN {r}entities_allocated e USING(stable_id) WHERE e.status='GONE'
      UNION ALL SELECT l.stable_id,'GONE',CAST(NULL AS BIGINT) FROM last_state l
        LEFT ANTI JOIN {r}projected_ids n USING(stable_id) LEFT ANTI JOIN {r}retirements t ON t.old_id=l.stable_id
        WHERE l.status='UNPUBLISHED'""")
    c.zero('STATE_CHANGE_UNIQUE',f'SELECT stable_id FROM {r}state_changes GROUP BY stable_id HAVING count(*)<>1')
    c.artifact('state_unflattened',f"""SELECT e.* EXCEPT(status,redirect_to,funder_id,entity_kind),
      coalesce(s.status,e.status) status,CASE WHEN s.stable_id IS NOT NULL THEN s.redirect_to ELSE e.redirect_to END redirect_to,
      CASE WHEN o.stable_id IS NOT NULL THEN o.funder_id ELSE e.funder_id END funder_id,
      coalesce(o.entity_kind,e.entity_kind) entity_kind FROM {r}entities_allocated e
      LEFT JOIN {r}state_changes s USING(stable_id) LEFT JOIN {r}ownership_plan o USING(stable_id)""")
    c.graph(r+'state_unflattened','resolve_candidate')
    c.artifact('state_candidate',f"SELECT e.* EXCEPT(redirect_to),CASE WHEN e.status='REDIRECTED' THEN x.stable_id ELSE e.redirect_to END redirect_to FROM {r}state_unflattened e JOIN {r}resolve_candidate x ON x.owner_id=e.stable_id")
    c.artifact('redirects_candidate',f"SELECT stable_id old_id,redirect_to canonical_id FROM {r}state_candidate WHERE status='REDIRECTED'")
    c.artifact('entity_updates',f'SELECT * FROM {r}state_candidate')
    c.artifact('keys_candidate',f'SELECT * FROM {p}award_keys')
    c.zero('NATIVE_CONTINUITY',f"""SELECT k.namespace,k.source_record_id FROM {r}keys_before k JOIN {r}resolve_before b ON b.owner_id=k.stable_id
      JOIN {r}resolve_candidate n ON n.owner_id=k.stable_id LEFT JOIN {r}retirements t ON t.old_id=b.stable_id
      WHERE k.key_type='NATIVE' AND n.stable_id<>coalesce(t.canonical_id,b.stable_id)""")


def resolve_embedded_links(c):
    """op28 (F27): resolve every award id embedded in the pinned works. Route ENTITY = the id is a registry owner (resolve_candidate, redirects
    followed). Route STAGING_KEY = the id is a LEGACY award id born after the seed = the raw staging id of a row whose STAGING key the registry
    already owns (allocated by this or an earlier release); resolve through that key's owner, then through the redirect graph. Anything else is
    unresolved and fails UNRESOLVED_EMBEDDED_ID. Prod FORBIDS the staging route: works published by the registry embed registry ids only, and
    the first prod release must pin works from the seed's input day — fail closed rather than guess (cutover packet same-day rule)."""
    r=c.r
    c.artifact('embedded_staging_owner',f"""SELECT staging_id old_id,min(stable_id) stable_id,count(DISTINCT stable_id) owners
      FROM {r}keys_candidate WHERE key_type='STAGING' AND staging_id IS NOT NULL GROUP BY staging_id""")
    c.zero('EMBEDDED_STAGING_ROUTE_AMBIGUOUS',f"""SELECT l.old_id,k.owners FROM (SELECT DISTINCT old_id FROM {r}embedded_links) l
      LEFT JOIN {r}resolve_candidate e ON e.owner_id=l.old_id JOIN {r}embedded_staging_owner k ON k.old_id=l.old_id
      WHERE e.owner_id IS NULL AND k.owners<>1""")
    c.artifact('embedded_links_resolved',f"""SELECT l.work_id,l.old_id,coalesce(e.stable_id,x.stable_id) stable_id,coalesce(e.status,x.status) status,
      CASE WHEN e.owner_id IS NOT NULL THEN 'ENTITY' WHEN x.owner_id IS NOT NULL THEN 'STAGING_KEY' END route
      FROM {r}embedded_links l LEFT JOIN {r}resolve_candidate e ON e.owner_id=l.old_id
      LEFT JOIN {r}embedded_staging_owner k ON e.owner_id IS NULL AND k.old_id=l.old_id AND k.owners=1
      LEFT JOIN {r}resolve_candidate x ON x.owner_id=k.stable_id""")
    c.zero('UNRESOLVED_EMBEDDED_ID',f'SELECT * FROM {r}embedded_links_resolved WHERE stable_id IS NULL')
    c.artifact('embedded_resolution_report',f"SELECT route,count(*) links,count(DISTINCT work_id) works,count(DISTINCT old_id) old_ids FROM {r}embedded_links_resolved GROUP BY route")
    if True:
        c.zero('EMBEDDED_STAGING_ROUTE_FORBIDDEN_IN_PROD',f"SELECT * FROM {r}embedded_links_resolved WHERE route='STAGING_KEY'")


def merge_doi_metadata(c):
    """Carry approved pair provenance forward; rederive donor fields from current native observations."""
    r=c.r
    sources=[]
    sources.append("SELECT loser_id,target_id FROM merge_doi_pairs_v")
    if c.has_extra('migration_manifest') and c.sql(f"SELECT 1 FROM migration_manifest_v WHERE evidence_class={literal(PUBLIC_DOI_CLASS)} LIMIT 1").collect():
        sources.append(f"SELECT DISTINCT loser_id,target_id FROM migration_manifest_v WHERE evidence_class={literal(PUBLIC_DOI_CLASS)}")
    if not sources:
        return r+'award_metadata'
    c.artifact('merge_doi_pairs',' UNION '.join(sources))
    # Native keys retain the original loser owner even after bindings resolve to the target.
    c.artifact('merge_doi_donors',f"""SELECT p.loser_id,x.stable_id target_id,k.namespace,k.source_record_id
      FROM {r}merge_doi_pairs p JOIN {r}keys_candidate k ON k.stable_id=p.loser_id AND k.key_type='NATIVE'
      JOIN {r}resolve_candidate x ON x.owner_id=p.target_id WHERE x.status='ACTIVE'""")
    c.artifact('merge_doi_payload',f"""SELECT d.loser_id,d.target_id,b.payload.start_date,b.payload.end_date,b.payload.description
      FROM {r}merge_doi_donors d JOIN {r}bindings b ON b.namespace=d.namespace AND b.source_record_id=d.source_record_id
      QUALIFY row_number() OVER(PARTITION BY d.target_id ORDER BY b.is_direct DESC,b.priority DESC,b.payload.updated_date DESC NULLS LAST,b.observation_key,d.loser_id)=1""")
    c.artifact('merge_doi_start_conflicts',f"""SELECT d.*,m.payload.start_year target_start_year FROM {r}merge_doi_payload d
      JOIN {r}award_metadata m ON m.stable_id=d.target_id WHERE m.payload.start_date IS NULL AND d.start_date IS NOT NULL
        AND m.payload.start_year IS NOT NULL AND year(d.start_date)<>m.payload.start_year""")
    c.artifact('merge_doi_fills',f"""SELECT d.loser_id,d.target_id,
      CASE WHEN m.payload.start_date IS NULL AND (m.payload.start_year IS NULL OR year(d.start_date)=m.payload.start_year) THEN d.start_date END start_date,
      CASE WHEN m.payload.end_date IS NULL THEN d.end_date END end_date,
      CASE WHEN m.payload.description IS NULL THEN d.description END description
      FROM {r}merge_doi_payload d JOIN {r}award_metadata m ON m.stable_id=d.target_id""")
    c.artifact('merge_doi_fill_report',f"""SELECT count(start_date) start_date_fills,count(end_date) end_date_fills,count(description) description_fills,
      (SELECT count(*) FROM {r}merge_doi_start_conflicts) start_year_conflicts FROM {r}merge_doi_fills""")
    return c.artifact('award_metadata_filled',f"""SELECT m.* EXCEPT(payload),struct(m.payload.* EXCEPT(start_date,end_date,description),
      coalesce(m.payload.start_date,d.start_date) start_date,coalesce(m.payload.end_date,d.end_date) end_date,
      coalesce(m.payload.description,d.description) description) payload
      FROM {r}award_metadata m LEFT JOIN {r}merge_doi_fills d ON d.target_id=m.stable_id""")


def project(c):
    p,r=c.p,c.r
    # op16: elect on narrow columns (+ payload.updated_date fetched as one narrow column), then attach the full payload only to winners.
    c.artifact('award_metadata_election',f"""SELECT o.observation_key,o.stable_id FROM {r}resolved_observations o
      JOIN (SELECT observation_key,payload.updated_date updated_date FROM {r}bindings) u USING(observation_key)
      LEFT ANTI JOIN {r}retirements t ON t.old_id=o.stable_id
      QUALIFY row_number() OVER(PARTITION BY o.stable_id ORDER BY o.is_direct DESC,o.priority DESC,u.updated_date DESC NULLS LAST,o.observation_key)=1""")
    c.zero('METADATA_ELECTION_UNIQUE',f'SELECT stable_id FROM {r}award_metadata_election GROUP BY stable_id HAVING count(*)<>1')
    c.artifact('award_metadata',f"""SELECT o.*,b.payload FROM {r}resolved_observations o JOIN {r}award_metadata_election w USING(observation_key,stable_id)
      JOIN {r}bindings b USING(observation_key)""")
    c.zero('RETIRED_LOSER_ELECTED',f'SELECT a.stable_id FROM {r}award_metadata a JOIN {r}retirements t ON t.old_id=a.stable_id')  # op14 (Q5 risk 3)
    metadata=merge_doi_metadata(c)
    c.artifact('embedded_links',"SELECT w.id work_id,try_cast(regexp_replace(a.id,'^https://openalex[.]org/G','') AS BIGINT) old_id FROM works_v w LATERAL VIEW explode(w.awards) x AS a")
    resolve_embedded_links(c)
    c.artifact('award_aggregates',f"""SELECT l.stable_id,
      transform(sort_array(collect_set(l.work_id)),x->concat('https://openalex.org/W',x)) funded_outputs,
      count(DISTINCT l.work_id) funded_outputs_count FROM {r}embedded_links_resolved l
      WHERE l.status='ACTIVE' GROUP BY l.stable_id""")
    c.zero('TOPICS_UNIQUE','SELECT award_id FROM topics_v GROUP BY award_id HAVING count(*)<>1')
    c.artifact('awards_candidate',f"""SELECT m.payload.* EXCEPT(id,funder_id,funder,start_year,end_year,works_api_url,priority),
      m.stable_id id,e.funder_id,
      CASE WHEN e.funder_id IS NULL THEN m.payload.funder ELSE named_struct(
        'id',concat('https://openalex.org/F',e.funder_id),'display_name',f.display_name,'ror_id',f.ror_id,'doi',f.doi) END funder,
      CASE WHEN m.payload.start_year>year(CAST(:ts AS TIMESTAMP))+1 THEN NULL ELSE m.payload.start_year END start_year,
      CASE WHEN m.payload.start_year>year(CAST(:ts AS TIMESTAMP))+1 THEN NULL ELSE m.payload.end_year END end_year,
      concat('https://api.openalex.org/works?filter=awards.id:G',m.stable_id) works_api_url,
      slice(coalesce(a.funded_outputs,array()),1,100) funded_outputs,coalesce(a.funded_outputs_count,0) funded_outputs_count,
      t.topics,t.topics[0] primary_topic,:rid release_id
      FROM {metadata} m JOIN {r}state_candidate e USING(stable_id)
      LEFT JOIN funders_v f ON f.funder_id=e.funder_id LEFT JOIN {r}award_aggregates a USING(stable_id)
      LEFT JOIN topics_v t ON t.award_id=m.stable_id WHERE e.status='ACTIVE'""")
    c.artifact('aliases_candidate',f"""SELECT k.staging_id old_id,k.funder_id,k.award_key old_funder_award_id,
      k.stable_id old_stable_id,x.stable_id canonical_stable_id,x.stable_id canonical_id,
      a.funder_id canonical_funder_id,lower(a.funder_award_id) canonical_funder_award_id,
      CAST(:ts AS TIMESTAMP) created_date,:rid release_id
      FROM {r}keys_candidate k JOIN {r}resolve_candidate x ON x.owner_id=k.stable_id
      JOIN {r}awards_candidate a ON a.id=x.stable_id
      WHERE k.key_type='STAGING' AND (k.stable_id<>x.stable_id OR k.kind='ALIAS')""")
    uk_lineage.winner_metadata(c)
    c.zero('CANDIDATE_UNIQUE',f'SELECT id FROM {r}awards_candidate GROUP BY id HAVING count(*)<>1 OR id IS NULL')
    c.zero('CANDIDATE_NONEMPTY',f'SELECT count(*) n FROM {r}awards_candidate HAVING n=0')
    c.zero('PROJECTED_SET_EQUAL',f'(SELECT id FROM {r}awards_candidate EXCEPT SELECT stable_id FROM {r}projected_ids) UNION ALL (SELECT stable_id FROM {r}projected_ids EXCEPT SELECT id FROM {r}awards_candidate)')
    c.zero('CANDIDATE_ACTIVE',f"SELECT a.id FROM {r}awards_candidate a LEFT ANTI JOIN {r}state_candidate e ON e.stable_id=a.id AND e.status='ACTIVE'")
    c.zero('ACTIVE_HAS_DOCUMENT',f"SELECT e.stable_id FROM {r}state_candidate e LEFT ANTI JOIN {r}awards_candidate a ON e.stable_id=a.id WHERE e.status='ACTIVE'")
    c.zero('RETIREMENT_HAS_TARGET_DOCUMENT',f'SELECT t.* FROM {r}retirements t LEFT ANTI JOIN {r}awards_candidate a ON a.id=t.canonical_id')
    c.zero('STABLE_URL_MISMATCH',f"SELECT id FROM {r}awards_candidate WHERE works_api_url<>concat('https://api.openalex.org/works?filter=awards.id:G',id)")
    c.zero('OBSERVATION_CONTINUITY',f"""SELECT b.observation_key,b.stable_id,n.stable_id FROM previous_bindings b
      JOIN {r}bindings_final n USING(observation_key) JOIN {r}resolve_candidate x ON x.owner_id=b.stable_id
      WHERE n.stable_id<>x.stable_id""")


def apply(c):
    """Registry state transitions; called only after every candidate gate (awards, links, API, search staging) passed."""
    p,r=c.p,c.r
    cols=['status','redirect_to','funder_id','entity_kind']
    same=lambda a,b:' AND '.join(f'{a}.{v} <=> {b}.{v}' for v in cols)
    c.zero('THIRD_STATE_CONFLICT',f'SELECT e.stable_id FROM {p}award_entities e JOIN {r}entities_allocated b USING(stable_id) JOIN {r}entity_updates u USING(stable_id) WHERE NOT ({same("e","b")}) AND NOT ({same("e","u")})')
    c.write(f"""INSERT INTO {p}award_state_transitions (run_id,stable_id,old_status,status,redirect_to,recorded_at)
      SELECT :rid,u.stable_id,b.status,u.status,u.redirect_to,CAST(:ts AS TIMESTAMP) FROM {r}entity_updates u JOIN {r}entities_allocated b USING(stable_id)
      WHERE NOT(b.status<=>u.status) OR NOT(b.redirect_to<=>u.redirect_to)""")
    c.write(f"""MERGE INTO {p}award_entities e USING {r}entity_updates u ON e.stable_id=u.stable_id
      WHEN MATCHED AND NOT ({same("e","u")}) THEN UPDATE SET e.status=u.status,e.redirect_to=u.redirect_to,e.funder_id=u.funder_id,
        e.entity_kind=u.entity_kind,e.updated_at=CAST(:ts AS TIMESTAMP)""")


def run(c):
    prepare(c); allocate(c); plan(c); project(c)


def mark_published(c):
    p,r=c.p,c.r
    c.write(f"""UPDATE {p}award_entities SET first_published_release=:rid,first_published_at=CAST(:ts AS TIMESTAMP)
      WHERE first_published_release IS NULL AND stable_id IN (SELECT id FROM {r}awards_candidate)""")
