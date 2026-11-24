"""A funder's own record and the same grant's registered DOI: one rule (Kyle + Rohan, 19 Nov 2026, decision 4).

Rule: the funder's own data (rank 1, award_source_rank.py) overwrites its grant DOI (rank 2), and the merged grant keeps all
links: the registered DOI, the paper links and the lower record's id (as a redirect). A grant shows up in three shapes:

1. SAME NUMBER (oxjob #2486: FCT, ANR, AHA, ~36.6K grants on 20 Nov). Feed and grant DOI write the same funder + number, so
   they are one grant already (one raw id, one STAGING key). The feed wins the election; feeds such as fct, anr_opendata and
   aha_report_builder carry no DOI, so the funder's registered DOI went unserved. keep_grant_doi() serves it.
2. SAME DOI, OTHER NUMBER (UCOP, ~7.9K; CFI once its DataCite awards are linked, oxjob #2487). UCOP's DataCite copy carries the
   DOI suffix as its number ('g90006'), the feed the grant number ('10AB-1100') and the DOI, so they never share a key and two
   grants are served. doi_twins() retires the grant-DOI grant into the funder's own grant, every night, by rule: the funder's
   id stays, the DOI grant's id redirects to it, its paper links follow the redirect (link_conservation() checks it).
3. OTHER SPELLING (class PUBLIC_REGISTRY_FEED_TWIN: Wellcome, DOE/OSTI, JSMF; 11-10). Feed and Crossref deposit write the
   number differently ('076576/Z/05/Z' vs '076576') and the feed carries no DOI, so only the registry norm key pairs them. A
   frozen, reviewed manifest retired the FEED id into the Crossref id (already applied: the ids served are Crossref's). From the
   next night the feed observation binds to that id and wins the election on rank, so the record shown is the funder's; the
   DOI is kept by keep_grant_doi() like shape 1, and fill() carries the Crossref lead ORCID. New pairs of this shape still go
   through the manifest, because a number spelling is weaker evidence than a shared DOI.

Shape 3, manifest proof (copied from uk_lineage's PUBLIC_GTR_LINEAGE_TWIN), re-run every night for pending pairs. A pair failing
it is HELD for the night (counted), never a stop; REGISTRY_TWIN_HELD_FUSE stops the run above max(100, 1%) held:
- yesterday's state: both DIRECT entities of one funder; loser served from a funder feed with no NATIVE key; target served
  from crossref_work with a crossref_grant NATIVE key; equal non-null registry norm keys; no ancestor redirected into the loser;
- tonight's state: both bound and in the plan with one funder; the target has a direct observation; no shell retires into
  the loser tonight (RETIREMENT_CHAIN); exactly two direct records of the funder carry that norm key (still one-to-one).
Measured on awards_api v1383 (2026-11-09): Wellcome 18,925 grants (18,095 one-to-one), DOE/OSTI 759 (56), JSMF 2. FWF's
openaire_fwf twins are an aggregator copy (rank 3), not a grant DOI, and are left alone (Rohan 11-05).
"""
import award_source_rank
import list_legs
from stable_award_ids import literal, norm_doi_sql

REGISTRY_TWIN_CLASS = 'PUBLIC_REGISTRY_FEED_TWIN'
DOI_TWIN_SOURCE = 'DOI_TWIN'          # retirement_sources.source of shape 2
DOI_TWIN_FUSE = 20000                 # night one: UCOP ~7.9K; config "doi_twin_fuse" overrides (a bug, not a size limit)
FAM = "regexp_replace(lower({}.family_name),'[^a-z]','')"


def enabled(c):
    return c.has_extra('migration_manifest') and bool(c.sql(
        f"SELECT 1 FROM migration_manifest_v WHERE evidence_class={literal(REGISTRY_TWIN_CLASS)} LIMIT 1").collect())


def filter(c, mm):
    """Runs right after uk_lineage.filter; returns mm without held registry-twin pairs."""
    if not enabled(c):
        return mm
    p, r = c.p, c.r
    cls = literal(REGISTRY_TWIN_CLASS)
    nk = lambda a: f"openalex.awards.award_norm_key({a}.funder_id,{a}.funder_award_id,'registry')"
    c.artifact('rt_pairs', f"SELECT loser_id,target_id FROM {mm} WHERE evidence_class={cls}")
    c.zero('REGISTRY_TWIN_TARGET_UNIQUE', f"SELECT target_id FROM {r}rt_pairs GROUP BY target_id HAVING count(DISTINCT loser_id)<>1")
    c.artifact('rt_keys', f"""SELECT m.loser_id,m.target_id,ta.funder_id,{nk('ta')} nk FROM {r}rt_pairs m JOIN awards_v ta ON ta.id=m.target_id""")
    # tonight's direct records of those funders that carry a pair's norm key (one-to-one re-proved every night)
    # award_norm_key is a SQL UDF with a subquery inside: Spark allows it in projections and filters, not in a join condition
    c.artifact('rt_key_owners', f"""WITH o AS (SELECT DISTINCT stable_id,funder_id,openalex.awards.award_norm_key(funder_id,award_key,'registry') nk
        FROM {r}resolved_observations WHERE is_direct AND funder_id IN (SELECT DISTINCT funder_id FROM {r}rt_keys))
      SELECT k.loser_id,count(DISTINCT o.stable_id) n FROM {r}rt_keys k JOIN o ON o.funder_id=k.funder_id AND o.nk=k.nk
      GROUP BY k.loser_id""")
    c.artifact('rt_held', f"""SELECT DISTINCT loser_id,reason FROM (
        SELECT m.loser_id,'PROOF' reason FROM {r}rt_pairs m
          LEFT JOIN {r}entities_allocated l ON l.stable_id=m.loser_id LEFT JOIN {r}entities_allocated t ON t.stable_id=m.target_id
          LEFT JOIN awards_v la ON la.id=m.loser_id LEFT JOIN awards_v ta ON ta.id=m.target_id
          WHERE NOT COALESCE(l.entity_kind='DIRECT' AND t.entity_kind='DIRECT'
            AND l.funder_id <=> t.funder_id AND la.funder_id <=> ta.funder_id
            AND la.provenance NOT IN ('crossref_work','datacite') AND ta.provenance='crossref_work'
            AND {nk('la')}={nk('ta')},false)
        UNION ALL SELECT m.loser_id,'LOSER_NATIVE' FROM {r}rt_pairs m JOIN {p}award_keys k ON k.stable_id=m.loser_id AND k.key_type='NATIVE'
        UNION ALL SELECT m.loser_id,'TARGET_NATIVE' FROM {r}rt_pairs m LEFT JOIN {p}award_keys k ON k.stable_id=m.target_id
            AND k.key_type='NATIVE' AND k.namespace='crossref_grant' GROUP BY m.loser_id HAVING count(k.stable_id)=0
        UNION ALL SELECT m.loser_id,'HAS_ANCESTOR' FROM {r}rt_pairs m JOIN {r}entities_allocated x ON x.redirect_to=m.loser_id AND x.status='REDIRECTED'
        UNION ALL SELECT m.loser_id,'LOSER_RULE_TARGET' FROM {r}rt_pairs m JOIN {r}retirements_rule t ON t.canonical_id=m.loser_id
        UNION ALL SELECT m.loser_id,'NOT_BOUND' FROM {r}rt_pairs m WHERE NOT EXISTS (SELECT 1 FROM {r}bindings b WHERE b.stable_id=m.loser_id)
            OR NOT EXISTS (SELECT 1 FROM {r}bindings b WHERE b.stable_id=m.target_id)
        UNION ALL SELECT m.loser_id,'TARGET_NOT_DIRECT' FROM {r}rt_pairs m LEFT ANTI JOIN {r}resolved_observations o ON o.stable_id=m.target_id AND o.is_direct
        UNION ALL SELECT m.loser_id,'PLAN_FUNDER' FROM {r}rt_pairs m LEFT JOIN {r}ownership_plan a ON a.stable_id=m.loser_id
            LEFT JOIN {r}ownership_plan b ON b.stable_id=m.target_id WHERE a.funder_id IS NULL OR NOT (a.funder_id <=> b.funder_id)
        UNION ALL SELECT k.loser_id,'NOT_ONE_TO_ONE' FROM {r}rt_keys k LEFT JOIN {r}rt_key_owners o USING(loser_id) WHERE coalesce(o.n,0)<>2)""")
    pairs = c.count('rt_pairs', r + 'rt_pairs')
    c.count('rt_held', r + 'rt_held')
    for row in c.sql(f"SELECT reason,count(DISTINCT loser_id) n FROM {r}rt_held GROUP BY 1").collect():
        c.counts['rt_held_' + row.reason.lower()] = int(row.n)
    c.zero('REGISTRY_TWIN_HELD_FUSE', f"SELECT count(DISTINCT loser_id) n FROM {r}rt_held HAVING n>greatest(100,0.01*{pairs})")
    return c.artifact('migration_proven_rt', f"SELECT m.* FROM {mm} m LEFT ANTI JOIN {r}rt_held h ON h.loser_id=m.loser_id")


def doi_twins(c, manifest):
    """Shape 2: tonight's (old_id, canonical_id) retirements of a grant-DOI grant into the funder's own grant with its DOI.
    Called by build_awards.plan() after the shell rule and the manifest (manifest: retirements_manifest exists).
    A pair: the loser's best observation is a grant DOI (rank 2: no funder's-own observation binds to it); the target has a
    funder's-own observation (rank 1) whose DOI is the loser's NATIVE DOI; one canonical funder; one-to-one by DOI.
    HELD (counted, not retired, tried again next night): not one-to-one (until the inputs change); the target retires tonight
    by the manifest; the loser retires tonight by the manifest; something retires into the loser tonight (RETIREMENT_CHAIN;
    from the next night that shell's redirect is simply re-pointed through the loser to the target); the loser is a
    migration-manifest target (an applied pair must keep resolving to it: MIGRATION_APPLIED_MISMATCH).
    DOI_TWIN_FUSE stops the night above config doi_twin_fuse pairs."""
    r = c.r
    ndoi = norm_doi_sql('b.payload.doi')
    c.artifact('dt_sides', f"""WITH best AS (SELECT stable_id,min(source_rank) best FROM {r}bindings GROUP BY stable_id)
      SELECT DISTINCT 'L' side,b.stable_id,b.source_record_id doi FROM {r}bindings b JOIN best x USING(stable_id)
        WHERE b.source_rank=2 AND x.best=2 AND b.source_record_id IS NOT NULL
      UNION SELECT DISTINCT 'T',b.stable_id,{ndoi} FROM {r}bindings b WHERE b.source_rank=1 AND {ndoi} IS NOT NULL""")
    c.artifact('dt_candidates', f"""SELECT l.stable_id loser_id,t.stable_id target_id,l.doi FROM {r}dt_sides l
      JOIN {r}dt_sides t ON t.side='T' AND t.doi=l.doi AND t.stable_id<>l.stable_id
      JOIN {r}ownership_plan a ON a.stable_id=l.stable_id JOIN {r}ownership_plan b ON b.stable_id=t.stable_id
      WHERE l.side='L' AND a.funder_id IS NOT NULL AND a.funder_id=b.funder_id""")
    retiring = f"SELECT old_id,canonical_id FROM {r}retirements_rule" + (
        f" UNION ALL SELECT old_id,canonical_id FROM {r}retirements_manifest" if manifest else "")
    c.artifact('dt_held', f"""WITH x AS (SELECT DISTINCT loser_id,target_id FROM {r}dt_candidates), t AS ({retiring})
      SELECT DISTINCT loser_id,reason FROM (
        SELECT loser_id,'NOT_ONE_TO_ONE' reason FROM x WHERE loser_id IN (SELECT loser_id FROM x GROUP BY 1 HAVING count(*)>1)
          OR target_id IN (SELECT target_id FROM x GROUP BY 1 HAVING count(*)>1)
        UNION ALL SELECT x.loser_id,'TARGET_RETIRING' FROM x JOIN t ON t.old_id=x.target_id
        UNION ALL SELECT x.loser_id,'LOSER_RETIRING' FROM x JOIN t ON t.old_id=x.loser_id
        UNION ALL SELECT x.loser_id,'LOSER_IS_TARGET' FROM x JOIN t ON t.canonical_id=x.loser_id
        {"UNION ALL SELECT x.loser_id,'MANIFEST_TARGET' FROM x JOIN migration_manifest_v m ON m.target_id=x.loser_id" if manifest else ""})""")
    pairs = c.artifact('dt_pairs', f"""SELECT DISTINCT d.loser_id old_id,d.target_id canonical_id FROM {r}dt_candidates d
      LEFT ANTI JOIN {r}dt_held h ON h.loser_id=d.loser_id""")
    c.count('doi_twin_pairs', pairs)
    c.count('doi_twin_held', f"(SELECT DISTINCT loser_id FROM {r}dt_held) h")
    for row in c.sql(f"SELECT reason,count(DISTINCT loser_id) n FROM {r}dt_held GROUP BY 1").collect():
        c.counts['doi_twin_held_' + row.reason.lower()] = int(row.n)
    c.zero('DOI_TWIN_FUSE', f"SELECT count(*) n FROM {pairs} HAVING n>{int(c.config.get('doi_twin_fuse', DOI_TWIN_FUSE))}")
    return pairs


def keep_grant_doi(c, metadata):
    """Shapes 1 and 3: the served grant keeps the DOI registered for it. A grant whose served record has no DOI serves the DOI
    of its best grant-DOI observation (rank 2) in bindings_final (so a loser retired tonight donates to its target). Not when
    another grant of the same or a higher rank already serves that DOI, or two grants would be given it. A lower-ranked holder
    (on 23 Nov: 669 mention-only grants whose paper deposit carried the grant DOI) doesn't block it: rank decides who serves a
    DOI, and both then serve it, as main already did for 205 Wellcome grants. Counts: doi_kept (fills), doi_kept_with_lower_holder
    (of them), doi_kept_served_elsewhere and doi_kept_shared (skipped)."""
    r = c.r
    c.artifact('doi_keep_donors', f"""SELECT b.stable_id,b.payload.doi doi,b.source_record_id d FROM {r}bindings_final b
      WHERE b.source_rank=2 AND b.source_record_id IS NOT NULL
      QUALIFY row_number() OVER(PARTITION BY b.stable_id ORDER BY {award_source_rank.order_sql('b')},b.payload.updated_date DESC NULLS LAST,b.observation_key)=1""")
    c.artifact('doi_keep_wanted', f"""SELECT d.* FROM {r}doi_keep_donors d JOIN {metadata} m ON m.stable_id=d.stable_id WHERE m.payload.doi IS NULL""")
    c.artifact('doi_keep', f"""WITH best AS (SELECT stable_id,min(source_rank) best FROM {r}bindings_final GROUP BY stable_id),
        served AS (SELECT DISTINCT {norm_doi_sql('m.payload.doi')} d,m.stable_id,x.best FROM {metadata} m JOIN best x USING(stable_id)
          WHERE m.payload.doi IS NOT NULL),
        elsewhere AS (SELECT w.stable_id,bool_or(s.best<=x.best) blocks FROM {r}doi_keep_wanted w JOIN best x USING(stable_id)
          JOIN served s ON s.d=w.d AND s.stable_id<>w.stable_id GROUP BY w.stable_id),
        shared AS (SELECT d FROM {r}doi_keep_wanted GROUP BY d HAVING count(DISTINCT stable_id)>1)
      SELECT w.*,CASE WHEN e.blocks THEN 'SERVED_ELSEWHERE' WHEN s.d IS NOT NULL THEN 'SHARED' END skipped,
        coalesce(NOT e.blocks,false) lower_holder
      FROM {r}doi_keep_wanted w LEFT JOIN elsewhere e ON e.stable_id=w.stable_id LEFT JOIN shared s ON s.d=w.d""")
    for row in c.sql(f"""SELECT count_if(skipped IS NULL) doi_kept,count_if(skipped IS NULL AND lower_holder) doi_kept_with_lower_holder,
        count_if(skipped='SERVED_ELSEWHERE') doi_kept_served_elsewhere,count_if(skipped='SHARED') doi_kept_shared FROM {r}doi_keep""").collect():
        c.counts.update({k: int(v or 0) for k, v in row.asDict().items()})
    fields = c.spark.table(metadata).schema['payload'].dataType.fieldNames()
    cols = ",".join("coalesce(m.payload.doi,k.doi) AS doi" if f == 'doi' else f"m.payload.`{f}` AS `{f}`" for f in fields)
    return c.artifact('award_metadata_doi_kept', f"""SELECT m.* EXCEPT(payload),struct({cols}) payload
      FROM {metadata} m LEFT JOIN (SELECT stable_id,doi FROM {r}doi_keep WHERE skipped IS NULL) k ON k.stable_id=m.stable_id""")


def fill(c, metadata):
    """Shape 3 pairs (pending or applied), target resolved to tonight's id: the Crossref lead ORCID when the served lead has
    none and both name the same non-empty family name. The DOI is kept by keep_grant_doi() for every grant. Field order is kept."""
    if not enabled(c):
        return metadata
    r = c.r
    cls = literal(REGISTRY_TWIN_CLASS)
    c.artifact('rt_donors', f"""SELECT x.stable_id target_id,b.payload.lead_investigator li
      FROM (SELECT DISTINCT target_id FROM migration_manifest_v WHERE evidence_class={cls}) m
      JOIN {r}resolve_candidate x ON x.owner_id=m.target_id AND x.status='ACTIVE'
      JOIN {r}bindings_final b ON b.stable_id=x.stable_id AND b.namespace='crossref_grant'
      QUALIFY row_number() OVER(PARTITION BY x.stable_id ORDER BY {award_source_rank.order_sql('b')},b.payload.updated_date DESC NULLS LAST,b.observation_key)=1""")
    ml, dl = FAM.format('m.payload.lead_investigator'), FAM.format('d.li')
    orcid_ok = (f"d.target_id IS NOT NULL AND m.payload.lead_investigator.orcid IS NULL AND d.li.orcid IS NOT NULL"
                f" AND length({ml})>0 AND {ml}={dl}")
    c.artifact('rt_fill_report', f"""SELECT count(*) targets,count_if({orcid_ok}) orcid_fills FROM {metadata} m JOIN {r}rt_donors d ON d.target_id=m.stable_id""")
    for row in c.sql(f"SELECT * FROM {r}rt_fill_report").collect():
        c.counts.update({'rt_fill_' + k: int(v or 0) for k, v in row.asDict().items()})
    li = ("CASE WHEN " + orcid_ok + " THEN named_struct('given_name',m.payload.lead_investigator.given_name,"
          "'family_name',m.payload.lead_investigator.family_name,'orcid',d.li.orcid,'role_start',m.payload.lead_investigator.role_start,"
          "'affiliation',m.payload.lead_investigator.affiliation) ELSE m.payload.lead_investigator END")
    fields = c.spark.table(metadata).schema['payload'].dataType.fieldNames()
    cols = ",".join(li + " AS lead_investigator" if f == 'lead_investigator' else f"m.payload.`{f}` AS `{f}`" for f in fields)
    return c.artifact('award_metadata_rt_filled', f"""SELECT m.* EXCEPT(payload),struct({cols}) payload
      FROM {metadata} m LEFT JOIN {r}rt_donors d ON d.target_id=m.stable_id""")


def link_conservation(c):
    """Link gates for tonight's twin retirements, shape 3 (manifest) and shape 2 (doi_twins): uk_lineage.link_conservation's
    contract minus its GtR-native leg. No candidate link on a loser; every link a loser had moves to its target (unless a
    pinned SUPPRESS row removes it); every link the target had stays (unless its producer is gone or SUPPRESSed)."""
    r = c.r
    if not enabled(c) and not c.counts.get('doi_twin_pairs'):
        return
    live = c.outputs['work_awards']
    # yesterday's served links (on surviving works) and tonight's candidate links: shared by both shapes
    c.artifact('twin_links_before', f"""SELECT DISTINCT coalesce(sv.survivor_work_id,w.work_id) work_id,w.award.id award_id FROM {live} w
      LEFT JOIN {r}work_survivors sv ON sv.loser_work_id=w.work_id""")
    c.artifact('twin_candidate_edges', f"SELECT DISTINCT work_id,award.id award_id FROM {r}work_awards_candidate")
    if enabled(c):
        c.artifact('rt_applied', f"SELECT old_id loser_id,canonical_id target_id FROM {r}retirements_manifest WHERE evidence_class={literal(REGISTRY_TWIN_CLASS)}")
        _conserve(c, 'rt', 'REGISTRY_TWIN', strict=True)
    c.artifact('dt_applied', f"SELECT DISTINCT old_id loser_id,canonical_id target_id FROM {r}retirement_sources WHERE source={literal(DOI_TWIN_SOURCE)}")
    _conserve(c, 'dt', 'DOI_TWIN', strict=False)


def _conserve(c, x, gate, strict):
    """strict (shape 3, a one-off manifest night): every link a loser had moves to its target. Not strict (shape 2, a rule
    that runs every night): like nih_fold, only a link tonight's producers still make must survive; a paper whose
    acknowledgement vanished upstream tonight is an explained removal (counted), so one such paper can't stop every night."""
    r = c.r
    G = "concat('https://openalex.org/G',{})"
    c.artifact(x + '_links_before', f"""SELECT b.* FROM {r}twin_links_before b JOIN (SELECT {G.format('loser_id')} a FROM {r}{x}_applied
      UNION SELECT {G.format('target_id')} FROM {r}{x}_applied) m ON m.a=b.award_id""")
    c.artifact(x + '_candidate_edges', f"""SELECT b.* FROM {r}twin_candidate_edges b JOIN (SELECT {G.format('loser_id')} a FROM {r}{x}_applied
      UNION SELECT {G.format('target_id')} FROM {r}{x}_applied) m ON m.a=b.award_id""")
    c.artifact(x + '_loser_edges', f"""SELECT b.* FROM {r}{x}_candidate_edges b JOIN {r}{x}_applied m ON b.award_id={G.format('m.loser_id')}""")
    c.artifact(x + '_mapped_expected', f"""SELECT DISTINCT b.work_id,{G.format('m.target_id')} award_id
      FROM {r}{x}_links_before b JOIN {r}{x}_applied m ON b.award_id={G.format('m.loser_id')}""")
    c.artifact(x + '_missing_mapped', f"""SELECT * FROM {r}{x}_mapped_expected EXCEPT SELECT * FROM {r}{x}_candidate_edges
      EXCEPT SELECT * FROM ({list_legs.removed_pairs_sql(c)})""")   # a pinned SUPPRESS row is an explained removal
    c.artifact(x + '_missing_target', f"""SELECT b.* FROM {r}{x}_links_before b JOIN {r}{x}_applied m ON b.award_id={G.format('m.target_id')}
      EXCEPT SELECT * FROM {r}{x}_candidate_edges""")
    c.artifact(x + '_link_producers', f"""SELECT work_id,{G.format('stable_id')} award_id FROM {r}work_matches WHERE stable_id IS NOT NULL
      UNION ALL SELECT work_id,award_id FROM ({list_legs.added_pairs_sql(c)})
      UNION ALL SELECT work_id,{G.format('stable_id')} FROM {r}work_links_moved WHERE work_id<>original_work_id""")
    c.artifact(x + '_supported_missing_target', f"""SELECT e.* FROM {r}{x}_missing_target e LEFT SEMI JOIN {r}{x}_link_producers p ON p.work_id=e.work_id AND p.award_id=e.award_id
      LEFT ANTI JOIN ({list_legs.removed_pairs_sql(c)}) s ON s.work_id=e.work_id AND s.award_id=e.award_id""")
    c.zero(gate + '_NO_LOSER_LINKS', f"SELECT * FROM {r}{x}_loser_edges")
    if strict:
        c.zero(gate + '_MAPPED_LINKS_PRESERVED', f"SELECT * FROM {r}{x}_missing_mapped")
    else:
        c.zero(gate + '_MAPPED_LINKS_PRESERVED', f"""SELECT m.* FROM {r}{x}_missing_mapped m
          LEFT SEMI JOIN {r}{x}_link_producers p ON p.work_id=m.work_id AND p.award_id=m.award_id""")
        c.counts[x + '_explained_removals'] = int(c.sql(f"""SELECT count(*) n FROM {r}{x}_missing_mapped m
          LEFT ANTI JOIN {r}{x}_link_producers p ON p.work_id=m.work_id AND p.award_id=m.award_id""").collect()[0].n)
    c.zero(gate + '_TARGET_LINKS_PRESERVED', f"SELECT * FROM {r}{x}_supported_missing_target")
    row = c.sql(f"""WITH marks AS (
        SELECT m.loser_id,b.work_id,1 l,0 t FROM {r}{x}_applied m JOIN {r}{x}_links_before b ON b.award_id={G.format('m.loser_id')}
        UNION ALL SELECT m.loser_id,b.work_id,0,1 FROM {r}{x}_applied m JOIN {r}{x}_links_before b ON b.award_id={G.format('m.target_id')}),
      e AS (SELECT loser_id,work_id,max(l) l,max(t) t FROM marks GROUP BY 1,2)
      SELECT (SELECT count(*) FROM {r}{x}_applied) pairs,count_if(l=1 AND t=0) moved,count_if(l=1 AND t=1) deduplicated,count(*) final_links FROM e""").collect()[0]
    for k in ('pairs', 'moved', 'deduplicated', 'final_links'):
        c.counts[x + '_applied_' + k] = int(row[k])
