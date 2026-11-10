"""Funder-feed records twinned by the funder's own Crossref grant deposits (class PUBLIC_REGISTRY_FEED_TWIN), 11-10 draft v2.

Why they exist: an award's identity is its raw id, abs(xxhash64(funder:lower(number))) % 9e9, plus the number itself
(build_awards.raw_observations). A funder feed and the funder's Crossref deposits that write the same grant differently
('076576/Z/05/Z' vs '076576') never share a key, so the Nightly serves two awards for one grant. award_norm_key (registry side)
maps both spellings to one key, but only the paper side uses it. Measured on awards_api v1383 (2026-11-09): Wellcome 18,925
grants (18,095 one-to-one), DOE/OSTI 759 (56), JSMF 2. FWF's openaire_fwf twins are left alone (Rohan 11-05).

A published feed record retires into the unique served Crossref record of the same grant (frozen, reviewed manifest).
Proof, re-run every night for pending pairs (shape copied from uk_lineage's PUBLIC_GTR_LINEAGE_TWIN). A pair failing it is
HELD for the night (counted), never a stop; REGISTRY_TWIN_HELD_FUSE stops the run above max(100, 1%) held:
- yesterday's state: both DIRECT entities of one funder; loser served from a funder feed with no NATIVE key; target served
  from crossref_work with a crossref_grant NATIVE key; equal non-null registry norm keys; no ancestor redirected into the loser;
- tonight's state: both bound and in the plan with one funder; the target has a direct observation; no shell retires into
  the loser tonight (RETIREMENT_CHAIN); exactly two direct records of the funder carry that norm key (still one-to-one).

Served row: the night a pair retires, the election skips the loser's observations, so the target keeps its Crossref row.
From the next night the feed observation binds to the target through the redirect and wins on priority (feed rows outrank
crossref_work), so the served row becomes the feed's (people, amount, institutions). fill() then carries the Crossref grant
DOI, and the Crossref lead ORCID when the feed's lead has none and both name the same non-empty family name, for every
applied or pending pair of this class. The payload keeps its field order.
"""
import list_legs
from stable_award_ids import literal

REGISTRY_TWIN_CLASS = 'PUBLIC_REGISTRY_FEED_TWIN'
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


def fill(c, metadata):
    """Every pair of the class (pending or applied), target resolved to tonight's id: carry the Crossref grant DOI, and the
    Crossref lead ORCID when the served lead has none and both name the same non-empty family name. Field order is kept."""
    if not enabled(c):
        return metadata
    r = c.r
    cls = literal(REGISTRY_TWIN_CLASS)
    c.artifact('rt_donors', f"""SELECT x.stable_id target_id,b.payload.doi doi,b.payload.lead_investigator li
      FROM (SELECT DISTINCT target_id FROM migration_manifest_v WHERE evidence_class={cls}) m
      JOIN {r}resolve_candidate x ON x.owner_id=m.target_id AND x.status='ACTIVE'
      JOIN {r}bindings_final b ON b.stable_id=x.stable_id AND b.namespace='crossref_grant'
      QUALIFY row_number() OVER(PARTITION BY x.stable_id ORDER BY b.priority DESC,b.payload.updated_date DESC NULLS LAST,b.observation_key)=1""")
    ml, dl = FAM.format('m.payload.lead_investigator'), FAM.format('d.li')
    orcid_ok = (f"d.target_id IS NOT NULL AND m.payload.lead_investigator.orcid IS NULL AND d.li.orcid IS NOT NULL"
                f" AND length({ml})>0 AND {ml}={dl}")
    c.artifact('rt_fill_report', f"""SELECT count(*) targets,count_if(m.payload.doi IS NULL AND d.doi IS NOT NULL) doi_fills,
      count_if({orcid_ok}) orcid_fills FROM {metadata} m JOIN {r}rt_donors d ON d.target_id=m.stable_id""")
    for row in c.sql(f"SELECT * FROM {r}rt_fill_report").collect():
        c.counts.update({'rt_fill_' + k: int(v or 0) for k, v in row.asDict().items()})
    li = ("CASE WHEN " + orcid_ok + " THEN named_struct('given_name',m.payload.lead_investigator.given_name,"
          "'family_name',m.payload.lead_investigator.family_name,'orcid',d.li.orcid,'role_start',m.payload.lead_investigator.role_start,"
          "'affiliation',m.payload.lead_investigator.affiliation) ELSE m.payload.lead_investigator END")
    fields = c.spark.table(metadata).schema['payload'].dataType.fieldNames()
    cols = ",".join({'doi': "coalesce(m.payload.doi,d.doi) AS doi", 'lead_investigator': li + " AS lead_investigator"}
                    .get(f, f"m.payload.`{f}` AS `{f}`") for f in fields)
    return c.artifact('award_metadata_rt_filled', f"""SELECT m.* EXCEPT(payload),struct({cols}) payload
      FROM {metadata} m LEFT JOIN {r}rt_donors d ON d.target_id=m.stable_id""")


def link_conservation(c):
    """Link gates for tonight's registry-twin retirements (uk_lineage.link_conservation's contract minus its GtR-native leg)."""
    if not enabled(c):
        return
    r = c.r
    live = c.outputs['work_awards']
    G = "concat('https://openalex.org/G',{})"
    c.artifact('rt_applied', f"SELECT old_id loser_id,canonical_id target_id FROM {r}retirements_manifest WHERE evidence_class={literal(REGISTRY_TWIN_CLASS)}")
    c.artifact('rt_links_before', f"""SELECT DISTINCT coalesce(sv.survivor_work_id,w.work_id) work_id,w.award.id award_id FROM {live} w
      LEFT JOIN {r}work_survivors sv ON sv.loser_work_id=w.work_id""")
    c.artifact('rt_candidate_edges', f"SELECT DISTINCT work_id,award.id award_id FROM {r}work_awards_candidate")
    c.artifact('rt_loser_edges', f"""SELECT b.* FROM {r}rt_candidate_edges b JOIN {r}rt_applied m ON b.award_id={G.format('m.loser_id')}""")
    c.artifact('rt_mapped_expected', f"""SELECT DISTINCT b.work_id,{G.format('m.target_id')} award_id
      FROM {r}rt_links_before b JOIN {r}rt_applied m ON b.award_id={G.format('m.loser_id')}""")
    c.artifact('rt_missing_mapped', f"""SELECT * FROM {r}rt_mapped_expected EXCEPT SELECT * FROM {r}rt_candidate_edges
      EXCEPT SELECT * FROM ({list_legs.removed_pairs_sql(c)})""")   # a pinned SUPPRESS row is an explained removal
    c.artifact('rt_missing_target', f"""SELECT b.* FROM {r}rt_links_before b JOIN {r}rt_applied m ON b.award_id={G.format('m.target_id')}
      EXCEPT SELECT * FROM {r}rt_candidate_edges""")
    c.artifact('rt_link_producers', f"""SELECT work_id,{G.format('stable_id')} award_id FROM {r}work_matches WHERE stable_id IS NOT NULL
      UNION ALL SELECT work_id,award_id FROM ({list_legs.added_pairs_sql(c)})
      UNION ALL SELECT work_id,{G.format('stable_id')} FROM {r}work_links_moved WHERE work_id<>original_work_id""")
    c.artifact('rt_supported_missing_target', f"""SELECT e.* FROM {r}rt_missing_target e LEFT SEMI JOIN {r}rt_link_producers p ON p.work_id=e.work_id AND p.award_id=e.award_id
      LEFT ANTI JOIN ({list_legs.removed_pairs_sql(c)}) s ON s.work_id=e.work_id AND s.award_id=e.award_id""")
    c.zero('REGISTRY_TWIN_NO_LOSER_LINKS', f"SELECT * FROM {r}rt_loser_edges")
    c.zero('REGISTRY_TWIN_MAPPED_LINKS_PRESERVED', f"SELECT * FROM {r}rt_missing_mapped")
    c.zero('REGISTRY_TWIN_TARGET_LINKS_PRESERVED', f"SELECT * FROM {r}rt_supported_missing_target")
    row = c.sql(f"""WITH marks AS (
        SELECT m.loser_id,b.work_id,1 l,0 t FROM {r}rt_applied m JOIN {r}rt_links_before b ON b.award_id={G.format('m.loser_id')}
        UNION ALL SELECT m.loser_id,b.work_id,0,1 FROM {r}rt_applied m JOIN {r}rt_links_before b ON b.award_id={G.format('m.target_id')}),
      e AS (SELECT loser_id,work_id,max(l) l,max(t) t FROM marks GROUP BY 1,2)
      SELECT (SELECT count(*) FROM {r}rt_applied) pairs,count_if(l=1 AND t=0) moved,count_if(l=1 AND t=1) deduplicated,count(*) final_links FROM e""").collect()[0]
    for k in ('pairs', 'moved', 'deduplicated', 'final_links'):
        c.counts['rt_applied_' + k] = int(row[k])
