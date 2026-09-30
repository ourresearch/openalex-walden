"""Ported from deployed stableid-r1-prod lib/work_awards.py (+ merged-work move, 09-29). Pinned WorkAwards legs; no numeric-only hash joins. op26 (F25, Rohan 09-15 'match today'): the incumbent LINKING election —
legacy WorkAwards elects ONE award per (funder, key, regime): g regime ORDER BY (display_name IS NOT NULL) DESC, id ASC; s regime
ORDER BY is_registry DESC, (display_name IS NOT NULL) DESC, end_year DESC NULLS LAST, id ASC ("families elect the newest year's registry
record", Kyle 2026-08-03); awards_al = keys of COLLAPSED shells (award_id_aliases) → canonical, at pref 3 below own keys (g 1, s 2).
Release 1 reproduces it on its ACTIVE candidates (links only; identity, keys, redirects, allocations untouched): own keys first, the
pinned legacy aliases + release aliases third, and a SHELL whose key legacy only knew through an alias (a collapsed shell, minted here as
its own entity) last — so papers keep linking to the family's newest-year record while the shell stays an ACTIVE public entity."""
DEPOSITED = "('crossref_work_funders','crossref_work.grants','crossref_work','europepmc_work_funders','datacite_work_funders')"   # legacy is_registry = provenance NOT IN
from stable_award_ids import generic_sql

# source view, work field, number field, explode, priority, sharp eligibility, registry mode
LEGS = [('work_funder','paper_id','award_ids',True,1,False,False),
        ('grobid','paper_id','funder_award_id',False,2,False,False),
        ('crossref_work_funders','work_id','award_ids',True,4,True,False),
        ('nwo_work_funders','work_id','award_ids',True,3,False,False),
        ('anr_work_funders','work_id','award_ids',True,3,False,False),
        ('datacite_work_funders','work_id','award_ids',True,4,True,False),
        ('europepmc_work_funders','work_id','award_ids',True,4,True,False),
        ('kaken_work_funders','work_id','award_ids',True,3,False,False),
        ('relink','work_id','target_award_id',False,4,True,True)]


def leg_sql():
    parts=[]
    for view,work,number,explode,priority,sharp,registry in LEGS:
        funder='target_funder_id' if view=='relink' else 'funder_id'
        expr='number' if explode else 'x.'+number
        tail=f'LATERAL VIEW explode(x.{number}) n AS number' if explode else ''
        parts.append(f"SELECT '{view}' source,x.{work} work_id,x.{funder} source_funder_id,lower({expr}) award_key,{priority} priority,{str(sharp).lower()} sharp_eligible,{str(registry).lower()} is_direct FROM {view}_v x {tail}")
    return '\nUNION ALL\n'.join(parts)


def run(c):
    c.guard(); p,r=c.p,c.r
    c.artifact('work_observations',f"SELECT DISTINCT * FROM ({leg_sql()})")
    c.zero('WORK_FUNDER_COVERAGE',f'SELECT w.* FROM {r}work_observations w LEFT ANTI JOIN {r}funder_canon f ON f.funder_id=w.source_funder_id WHERE w.source_funder_id IS NOT NULL')
    c.zero('WORK_NORMALIZATION_COVERAGE',f"""SELECT w.* FROM {r}work_observations w LEFT JOIN {r}funder_canon f
      ON f.funder_id=w.source_funder_id LEFT ANTI JOIN {r}normalization_lookup n
      ON n.funder_id <=> f.canonical_funder_id AND n.award_key <=> w.award_key AND n.is_direct=w.is_direct""")
    c.artifact('work_normalized',f"""SELECT w.*,f.canonical_funder_id funder_id,{generic_sql('w.award_key')} nk,
      CASE WHEN w.sharp_eligible AND n.sharp_eligible AND (w.is_direct OR NOT n.is_weak) THEN n.sharp_key END sk
      FROM {r}work_observations w LEFT JOIN {r}funder_canon f ON f.funder_id=w.source_funder_id
      JOIN {r}normalization_lookup n ON n.funder_id <=> f.canonical_funder_id AND n.award_key <=> w.award_key AND n.is_direct=w.is_direct""")
    # Alias evidence is admitted only after validating all three permanent STAGING fields.
    c.artifact('work_aliases',f"""SELECT a.*,x.stable_id target_id FROM {r}aliases_candidate a
      JOIN {r}keys_candidate k ON k.key_type='STAGING' AND k.staging_id=a.old_id
        AND k.funder_id <=> a.funder_id AND k.award_key <=> a.old_funder_award_id
      JOIN {r}resolve_candidate x ON x.owner_id=k.stable_id
      WHERE x.stable_id=a.canonical_stable_id AND x.status='ACTIVE'""")
    c.zero('ALIAS_FULL_TRIPLE_MISSING',f'SELECT old_id,funder_id,old_funder_award_id FROM {r}aliases_candidate EXCEPT SELECT old_id,funder_id,old_funder_award_id FROM {r}work_aliases')
    # Citation-side number-only records do not carry the hash. Use the recorded
    # observation mapping only if exactly one full triple and terminal is possible.
    c.artifact('work_exact_map',f"""SELECT k.funder_id source_funder_id,k.award_key,min(x.stable_id) stable_id
      FROM {r}keys_candidate k JOIN {r}resolve_candidate x ON x.owner_id=k.stable_id
      JOIN {r}awards_candidate a ON a.id=x.stable_id WHERE k.key_type='STAGING' AND x.status='ACTIVE'
      GROUP BY k.funder_id,k.award_key HAVING count(DISTINCT k.staging_id)=1 AND count(DISTINCT x.stable_id)=1""")
    # op26: legacy award_id_aliases (pinned input) as LINKING keys: the collapsed spelling → its canonical, resolved through the registry.
    c.artifact('legacy_alias_targets',f"""SELECT DISTINCT al.old_id,al.funder_id alias_funder_id,f.canonical_funder_id funder_id,lower(al.old_funder_award_id) award_key,x.stable_id,al.canonical_id
      FROM aliases_v al LEFT JOIN {r}funder_canon f ON f.funder_id=al.funder_id
      JOIN {r}resolve_candidate x ON x.owner_id=al.canonical_id WHERE x.status='ACTIVE' AND al.old_funder_award_id IS NOT NULL""")
    # legacy eligibility (awards_al joins awards_base ON id AND funder_id, and awards_base excludes NULL numbers): the resolved canonical must
    # be served under the SAME canonical funder with a funder_award_id; everything else is recorded, not linked.
    c.artifact('legacy_aliases',f"""SELECT DISTINCT t.old_id,t.funder_id,t.award_key,t.stable_id FROM {r}legacy_alias_targets t
      JOIN {r}awards_candidate d ON d.id=t.stable_id WHERE d.funder_id <=> t.funder_id AND d.funder_award_id IS NOT NULL""")
    c.artifact('legacy_alias_unresolved',f"""SELECT al.*,'NOT_ACTIVE' reason FROM aliases_v al LEFT ANTI JOIN {r}resolve_candidate x ON x.owner_id=al.canonical_id AND x.status='ACTIVE'
      UNION ALL SELECT al.*,'INELIGIBLE_FUNDER_OR_NUMBER' FROM aliases_v al JOIN {r}legacy_alias_targets t ON t.old_id=al.old_id AND t.alias_funder_id=al.funder_id AND t.award_key=lower(al.old_funder_award_id)
      LEFT ANTI JOIN {r}legacy_aliases l ON l.old_id=t.old_id AND l.funder_id <=> t.funder_id AND l.award_key <=> t.award_key""")
    # (F25 rounds 2–4) collapse evidence = the exact legacy triple: the observation whose STAGING id IS the alias's old_id, under the
    # alias's canonical funder with the collapsed spelling. Legacy never served that record, so its keys are reachable ONLY through the
    # alias: the observation is EXCLUDED from own-key candidates (any entity kind — legacy collapsed by priority, not by shell-ness);
    # an independently served spelling of the same entity keeps its own keys.
    c.artifact('collapsed_observations',f"""SELECT DISTINCT o.observation_key,o.stable_id FROM {r}resolved_observations o
      JOIN {r}legacy_aliases l ON l.old_id=o.staging_id AND l.funder_id <=> o.funder_id AND l.award_key <=> o.award_key""")
    # op26: every candidate carries its legacy tier (pref) and the legacy election fields; the election happens in work_key_map.
    # (F25 round 5) legacy derives a served award's SHARP key by the DOCUMENT's provenance (deposited set → 'deposited' mode with the weak
    # filter; anything else → 'registry' mode), not by the observation's identity mode. linking_sharp re-derives it from the normalization
    # lookup in that mode; where the lookup has no row in that mode (measured 09-15 on d01m8: ~4% of observations, modes disagree) the
    # identity-mode key is kept as a documented fallback and counted in linking_sharp_coverage.
    c.artifact('linking_sharp_input',f"""SELECT DISTINCT x.stable_id,o.funder_id,o.award_key,o.sk identity_sk,
        CASE WHEN d.provenance IN {DEPOSITED} THEN false ELSE true END registry_mode
      FROM {r}resolved_observations o LEFT JOIN {r}collapsed_observations co ON co.observation_key=o.observation_key
      JOIN {r}resolve_candidate x ON x.owner_id=o.stable_id JOIN {r}awards_candidate d ON d.id=x.stable_id
      WHERE co.observation_key IS NULL AND x.status='ACTIVE'""")
    c.artifact('linking_sharp',f"""SELECT l.stable_id,l.funder_id,l.award_key,l.registry_mode,
        CASE WHEN n.award_key IS NOT NULL THEN CASE WHEN l.registry_mode OR (n.sharp_eligible AND NOT n.is_weak) THEN n.sharp_key END ELSE l.identity_sk END sk,
        CASE WHEN n.award_key IS NOT NULL THEN 'legacy_mode' ELSE 'identity_fallback' END sk_source
      FROM {r}linking_sharp_input l LEFT JOIN {r}normalization_lookup n
        ON n.funder_id <=> l.funder_id AND n.award_key <=> l.award_key AND n.is_direct = l.registry_mode""")
    # grain = DISTINCT (entity, canonical funder, spelling) rows of linking_sharp_input
    c.artifact('linking_sharp_coverage',f"SELECT sk_source,registry_mode,count(*) spellings,count(sk) with_sharp FROM {r}linking_sharp GROUP BY 1,2")
    c.artifact('work_key_candidates',f"""WITH obs AS (
      SELECT o.* FROM {r}resolved_observations o LEFT JOIN {r}collapsed_observations co ON co.observation_key=o.observation_key
      WHERE co.observation_key IS NULL),
    own AS (
      SELECT DISTINCT x.stable_id,o.funder_id,o.nk,'g' regime,1 tier
      FROM obs o JOIN {r}resolve_candidate x ON x.owner_id=o.stable_id WHERE x.status='ACTIVE' AND o.nk IS NOT NULL
      UNION SELECT DISTINCT stable_id,funder_id,sk,'s',1 FROM {r}linking_sharp WHERE sk IS NOT NULL),
    alias AS (
      SELECT DISTINCT a.target_id stable_id,f.canonical_funder_id funder_id,{generic_sql('a.old_funder_award_id')} nk,'g' regime
      FROM {r}work_aliases a LEFT JOIN {r}funder_canon f ON f.funder_id=a.funder_id
      UNION SELECT DISTINCT a.target_id,f.canonical_funder_id,n.sharp_key,'s'
      FROM {r}work_aliases a LEFT JOIN {r}funder_canon f ON f.funder_id=a.funder_id
      JOIN {r}normalization_lookup n ON n.funder_id <=> f.canonical_funder_id AND n.award_key <=> a.old_funder_award_id
        AND NOT n.is_direct WHERE n.sharp_eligible AND NOT n.is_weak AND n.sharp_key IS NOT NULL AND n.sharp_key <> {generic_sql('a.old_funder_award_id')}
      UNION SELECT DISTINCT l.stable_id,l.funder_id,{generic_sql('l.award_key')},'g' FROM {r}legacy_aliases l
      UNION SELECT DISTINCT l.stable_id,l.funder_id,n.sharp_key,'s' FROM {r}legacy_aliases l
      JOIN {r}normalization_lookup n ON n.funder_id <=> l.funder_id AND n.award_key <=> l.award_key AND NOT n.is_direct
      WHERE n.sharp_eligible AND NOT n.is_weak AND n.sharp_key IS NOT NULL AND n.sharp_key <> {generic_sql('l.award_key')}),
    cand AS (SELECT stable_id,funder_id,nk,regime,tier FROM own UNION ALL SELECT stable_id,funder_id,nk,regime,3 FROM alias)
    SELECT c.stable_id,c.funder_id,c.nk,c.regime,c.tier pref,
      CASE WHEN d.provenance IN {DEPOSITED} THEN false ELSE true END is_registry,e.entity_kind='DIRECT' is_direct,d.display_name IS NOT NULL has_name,d.end_year
    FROM cand c JOIN {r}state_candidate e ON e.stable_id=c.stable_id LEFT JOIN {r}awards_candidate d ON d.id=c.stable_id""")
    c.zero('WORK_KEY_CANDIDATE_KIND_MISSING',f"SELECT * FROM {r}work_key_candidates WHERE is_direct IS NULL OR is_registry IS NULL")
    # op26: ONE elected award per (funder, key, regime): legacy tier first (own g/s before aliases; alias-covered shells last), then the
    # legacy order — g: (display_name IS NOT NULL) DESC, id ASC; s: is_registry DESC, name DESC, end_year DESC NULLS LAST, id ASC.
    c.artifact('work_key_map',f"""SELECT funder_id,nk,regime,stable_id,pref elected_pref,is_registry,targets FROM (
      SELECT funder_id,nk,regime,stable_id,pref,is_registry,count(*) OVER (PARTITION BY funder_id,nk,regime) targets,
        row_number() OVER (PARTITION BY funder_id,nk,regime ORDER BY pref ASC,
          CASE WHEN regime='s' THEN is_registry END DESC NULLS LAST, has_name DESC, CASE WHEN regime='s' THEN end_year END DESC NULLS LAST, stable_id ASC) rn
      FROM (SELECT DISTINCT funder_id,nk,regime,stable_id,pref,is_registry,has_name,end_year FROM {r}work_key_candidates)) WHERE rn=1""")
    c.zero('WORK_ELECTION_UNIQUE',f'SELECT funder_id,nk,regime FROM {r}work_key_map GROUP BY 1,2,3 HAVING count(*)<>1')
    # op26 precedence = legacy: the generic election first, then the sharp election; no exact-spelling step (legacy has none).
    c.artifact('work_matches',f"""SELECT w.*,coalesce(g.stable_id,s.stable_id) stable_id,
      CASE WHEN g.stable_id IS NOT NULL THEN 'g' WHEN s.stable_id IS NOT NULL THEN 's' END match_route,
      CASE WHEN g.stable_id IS NOT NULL THEN g.elected_pref ELSE s.elected_pref END elected_pref,
      coalesce(g.targets,0) generic_targets,coalesce(s.targets,0) sharp_targets
      FROM {r}work_normalized w
      LEFT JOIN {r}work_key_map g ON g.regime='g' AND g.funder_id=w.funder_id AND g.nk=w.nk
      LEFT JOIN {r}work_key_map s ON s.regime='s' AND s.funder_id=w.funder_id AND s.nk=w.sk""")
    c.artifact('work_election_report',f"""SELECT match_route,elected_pref,generic_targets>1 generic_tie,sharp_targets>1 sharp_tie,count(*) observations,count(DISTINCT work_id) works
      FROM {r}work_matches WHERE stable_id IS NOT NULL GROUP BY 1,2,3,4""")
    c.artifact('gtr_work_matches',f"""SELECT DISTINCT g.work_id,x.stable_id,3 priority FROM gtr_v g
      JOIN {r}keys_candidate k ON k.key_type='NATIVE' AND k.namespace='gtr' AND k.source_record_id=cast(g.id AS STRING)
      JOIN {r}resolve_candidate x ON x.owner_id=k.stable_id AND x.status='ACTIVE' WHERE g.work_id IS NOT NULL""")
    c.zero('GTR_NATIVE_UNRESOLVED',f"""SELECT g.id,g.work_id FROM gtr_v g LEFT ANTI JOIN (
      SELECT k.source_record_id FROM {r}keys_candidate k JOIN {r}resolve_candidate x ON x.owner_id=k.stable_id
      JOIN {r}awards_candidate a ON a.id=x.stable_id WHERE k.key_type='NATIVE' AND k.namespace='gtr' AND x.status='ACTIVE') x
      ON x.source_record_id=cast(g.id AS STRING) WHERE g.work_id IS NOT NULL""")
    c.artifact('work_unmatched',f"SELECT *,CASE WHEN generic_targets>1 OR sharp_targets>1 THEN 'AMBIGUOUS' ELSE 'NO_CANDIDATE' END disposition FROM {r}work_matches WHERE stable_id IS NULL")
    c.artifact('work_links',f"""SELECT work_id,stable_id,priority FROM {r}work_matches WHERE stable_id IS NOT NULL
      UNION ALL SELECT work_id,stable_id,priority FROM {r}gtr_work_matches""")
    survivors=work_survivors(c)
    move=bool(c.config.get('move_merged_work_links',True))
    # NEW (09-29): links on merged-away works move to the surviving work, before the one final (work, award) dedupe.
    c.artifact('work_links_moved',f"""SELECT coalesce(m.survivor_work_id,l.work_id) work_id,l.work_id original_work_id,l.stable_id,l.priority
      FROM {r}work_links l LEFT JOIN {survivors} m ON {'true' if move else 'false'} AND m.loser_work_id=l.work_id""")
    c.artifact('work_awards_candidate',f"""WITH links AS (SELECT work_id,stable_id,priority FROM {r}work_links_moved)
      SELECT l.work_id,named_struct('id',concat('https://openalex.org/G',a.id),'display_name',a.display_name,
        'funder_award_id',a.funder_award_id,'funder_id',a.funder.id,'funder_display_name',a.funder.display_name) award,:rid release_id
      FROM links l JOIN {r}awards_candidate a ON a.id=l.stable_id
      QUALIFY row_number() OVER(PARTITION BY l.work_id,a.id ORDER BY l.priority)=1""")
    c.zero('WORK_MATCH_HAS_DOCUMENT',f"SELECT DISTINCT stable_id FROM {r}work_matches WHERE stable_id IS NOT NULL EXCEPT SELECT id FROM {r}awards_candidate")
    c.zero('WORK_EMITTED_ACTIVE',f"SELECT w.* FROM {r}work_awards_candidate w LEFT ANTI JOIN {r}resolve_candidate e ON w.award.id=concat('https://openalex.org/G',e.owner_id) AND e.status='ACTIVE'")
    c.zero('WORK_HAS_DOCUMENT',f"SELECT w.* FROM {r}work_awards_candidate w LEFT ANTI JOIN {r}awards_candidate a ON w.award.id=concat('https://openalex.org/G',a.id)")
    c.zero('WORK_DUPLICATE',f'SELECT work_id,award.id FROM {r}work_awards_candidate GROUP BY work_id,award.id HAVING count(*)<>1')
    c.zero('WORK_RELEASE_MISMATCH',f'SELECT * FROM {r}work_awards_candidate WHERE release_id<>:rid OR release_id IS NULL')
    c.artifact('work_merge_report',f"""SELECT count_if(work_id<>original_work_id) moved_links,count(DISTINCT CASE WHEN work_id<>original_work_id THEN original_work_id END) moved_from_works,
      (SELECT count(*) FROM {r}work_links l JOIN {r}work_merge_unresolved u ON u.loser_work_id=l.work_id) unresolved_links,
      (SELECT count(*) FROM {r}work_links l JOIN {r}work_merge_unserved u ON u.loser_work_id=l.work_id) survivor_not_served_links,
      (SELECT count(*) FROM {r}work_merge_edges WHERE winners>1) ambiguous_losers FROM {r}work_links_moved""")
    for k,v in c.sql(f'SELECT * FROM {r}work_merge_report').collect()[0].asDict().items():
        c.counts['work_merge_'+k]=int(v)
    c.count('work_awards',r+'work_awards_candidate')


def work_survivors(c, max_hops=100):
    """loser work -> terminal surviving work, from openalex.works.merged_work_ids (walden MergeIdenticalKeyWorks record_merges).
    A loser with several winners takes the most recent merge (ties: lowest winner id). Chains are followed; a chain that does not end
    within max_hops (a cycle) is skipped and counted, never guessed. A link moves only if the survivor is a served work."""
    r=c.r
    c.artifact('work_merge_edges',"""SELECT loser_work_id,max_by(winner_work_id,struct(merged_at,-winner_work_id)) winner_work_id,
      count(DISTINCT winner_work_id) winners FROM merged_work_ids_v WHERE loser_work_id<>winner_work_id GROUP BY loser_work_id""")
    cur=c.artifact('work_merge_walk_0',f'SELECT loser_work_id,winner_work_id survivor_work_id,false done FROM {r}work_merge_edges')
    for hop in range(1,max_hops+1):
        cur=c.artifact(f'work_merge_walk_{hop % 2}',f"""SELECT w.loser_work_id,coalesce(e.winner_work_id,w.survivor_work_id) survivor_work_id,
          w.done OR e.loser_work_id IS NULL done FROM {cur} w LEFT JOIN {r}work_merge_edges e ON NOT w.done AND e.loser_work_id=w.survivor_work_id""")
        if not c.sql(f'SELECT 1 FROM {cur} WHERE NOT done LIMIT 1').collect():
            break
    c.artifact('work_merge_unresolved',f'SELECT loser_work_id FROM {cur} WHERE NOT done')
    c.artifact('work_merge_unserved',f"""SELECT w.loser_work_id,w.survivor_work_id FROM {cur} w LEFT ANTI JOIN (SELECT id FROM works_v) s ON s.id=w.survivor_work_id WHERE w.done""")
    return c.artifact('work_survivors',f"""SELECT w.loser_work_id,w.survivor_work_id FROM {cur} w JOIN (SELECT id FROM works_v) s ON s.id=w.survivor_work_id
      WHERE w.done AND w.survivor_work_id<>w.loser_work_id""")
